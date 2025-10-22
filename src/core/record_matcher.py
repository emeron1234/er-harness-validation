"""
Record Matching Module
Handles matching and mismatching record identification
"""

from typing import List, Tuple
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.functions import when, concat, lit, split, regexp_replace


def map_match_mismatch_records(
    source_dataframe: DataFrame, 
    target_dataframe: DataFrame, 
    drop_columns: List[str]
) -> Tuple[DataFrame, DataFrame]:
    """
    Identify Match/Mismatch comparison results of ER Output vs Truth File.
    
    Performs two types of comparison:
    1. 1:1 matching of hash columns
    2. Column-by-column comparison with special handling for name fields
    
    Args:
        source_dataframe: Source dataframe (ER output) with _eo suffix
        target_dataframe: Target dataframe (truth file) with _tf suffix
        drop_columns: List of column names to drop from result
        
    Returns:
        Tuple of (match_df, mismatch_df)
        - match_df: Records where key_column & hash_columns match
        - mismatch_df: Records where only key_column matches
    """
    # Hash-based comparison
    hash_df = (
        source_dataframe.join(
            target_dataframe, 
            source_dataframe.src_tgt_key_columns_eo == target_dataframe.src_tgt_key_columns_tf, 
            how="outer"
        ).withColumn(
            "validation_status", 
            when(
                source_dataframe.src_tgt_md5_hash_column_eo == target_dataframe.src_tgt_md5_hash_column_tf, 
                'Match'
            ).otherwise('Mismatch')
        )
    ).drop(*drop_columns)

    # Re-initialize with original df
    hash_df_compare = hash_df

    # Columns to exclude from comparison
    exclude_col = [
        "validation_status", 
        "human_review_tf", 
        "src_tgt_md5_hash_column_eo", 
        "src_tgt_md5_hash_column_tf", 
        "match_status_er_eo", 
        "match_status_manual_tf", 
        "error_matrix_eo", 
        "error_matrix_tf"
    ]

    # Column-level comparison
    for col in hash_df.columns:
        if col not in exclude_col: 
            if "_eo" in col:
                # To prevent iteration of same column of _tf
                target_col = col.replace("_eo", "_tf")

                if col in ["source_first_name_eo", "source_last_name_eo"]:
                    # Token-based matching for compound names
                    # Example: "Kirby-Jones" → ["Kirby", "Jones"]
                    df_clean = hash_df_compare \
                        .withColumn(
                            f"{col}_token",
                            split(regexp_replace(F.col(col), "[^a-z]", " "), "\\s+")
                        ) \
                        .withColumn(
                            f"{target_col}_token",
                            split(regexp_replace(F.col(target_col), "[^a-z]", " "), "\\s+")
                        )

                    # Check for common tokens
                    hash_df_compare = df_clean.withColumn(
                        f"pattern_{col}",
                        when(
                            F.expr(f"size(array_intersect({col}_token, {target_col}_token)) > 0"), 
                            '1'
                        ).otherwise('0')
                    ).drop(f"{col}_token", f"{target_col}_token")
                else:
                    # Direct 1:1 comparison
                    hash_df_compare = hash_df_compare.withColumn(
                        f"pattern_{col}", 
                        when(
                            (hash_df[col].isNull() & hash_df[target_col].isNull()) | 
                            (hash_df[col] == hash_df[target_col]), 
                            '1'
                        ).otherwise('0')
                    )

    # Concatenate all pattern columns to create mismatch code
    concat_col = [
        hash_df_compare[f"{i}"] 
        for i in hash_df_compare.columns 
        if "pattern_" in i and "validation_status" not in i
    ]

    compared_column = hash_df_compare \
        .withColumn("mismatches_code", concat(*concat_col, lit(""))) \
        .withColumn("extra_or_missing_flag", lit(None)) \
        .drop(*concat_col)

    # Filter between match & mismatch records
    match_df = compared_column.where(compared_column.validation_status == "Match")
    mismatch_df = compared_column.where(
        (compared_column.validation_status == "Mismatch") & 
        (compared_column.source_primary_key_eo.isNotNull()) & 
        (compared_column.source_primary_key_tf.isNotNull())
    )
    
    return match_df, mismatch_df


def map_extra_records(
    source_df: DataFrame,
    target_df: DataFrame,
    key_column_source: str,
    key_column_target: str,
    repopulate_columns: List[str],
    drop_columns: List[str],
) -> DataFrame:
    """
    Identify records that exist in source but not in target.
    
    Args:
        source_df: Source dataframe
        target_df: Target dataframe
        key_column_source: Key column name in source
        key_column_target: Key column name in target
        repopulate_columns: Columns to populate with None
        drop_columns: Columns to drop from result
        
    Returns:
        DataFrame with extra records marked as Mismatch
    """
    # Find extra records in source dataframe
    extra_record_df = (
        source_df.join(
            target_df,
            source_df[key_column_source] == target_df[key_column_target],
            how="left_anti",
        )
    ).withColumn(
        "validation_status",
        when(
            source_df[key_column_source].isNotNull(), 
            "Mismatch"
        ).otherwise("Match"),
    )

    # Repopulate all target_dataframe columns as None
    for col in repopulate_columns:
        extra_record_df = extra_record_df.withColumn(col, lit(None))

    extra_record_df = (
        extra_record_df.withColumn(
            "mismatches_code",
            when(
                extra_record_df.validation_status == "Mismatch", 
                "000000000000000000000"
            ).otherwise(None),
        )
        .withColumn("validation_status", lit("Mismatch"))
        .drop(*drop_columns)
    )
    
    extra_record_df = extra_record_df.withColumn(
        "extra_or_missing_flag",
        when(
            extra_record_df.source_primary_key_tf.isNull(), 
            False
        ).otherwise(True),
    )
    
    return extra_record_df
