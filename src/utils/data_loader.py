"""
Data Loader Module
Handles loading and preparing data for validation
"""

from pyspark.sql import DataFrame, functions as F
from pyspark.shell import spark
from typing import Dict, List


def load_er_dataframe(
    table_name: str,
    selected_columns: List[str],
    er_matching_rules: Dict[str, any]
) -> DataFrame:
    """
    Load and process ER (Entity Resolution) output dataframe.
    
    Args:
        table_name: Fully qualified table name in Spark
        selected_columns: List of column names to select
        er_matching_rules: Dictionary containing matching rules and thresholds
        
    Returns:
        DataFrame with match_status_er column added
    """
    columns_str = ", ".join(selected_columns)
    
    # Build match status logic based on rules
    er_df = spark.sql(f"""
        SELECT {columns_str}, 
        CASE 
            WHEN er_rank = 1 AND
            (
                (
                    er_score >= {er_matching_rules['er_score_threshold']} AND
                    suffix_score > 0 
                ) OR
                (
                    er_score > {er_matching_rules['er_score_min']} AND
                    first_name_score >= {er_matching_rules['first_name_score']} AND
                    last_name_score >= {er_matching_rules['last_name_score']} AND
                    middle_name_score >= {er_matching_rules['middle_name_score']} AND
                    city_score >= {er_matching_rules['city_score']} AND
                    address_line_score > {er_matching_rules['address_line_score']} AND
                    state_score >= {er_matching_rules['state_score']} AND
                    postal_code_score >= {er_matching_rules['postal_code_score']} AND
                    suffix_score > 0
                ) 
            )
            THEN 'Matched' 
            ELSE 'NOT Matched'    
        END AS match_status_er   
        FROM {table_name}
    """)
    
    return er_df


def load_truth_dataframe(
    table_name: str,
    selected_columns: List[str],
    er_matching_rules: Dict[str, any],
    exception_pairs: List[tuple] = None
) -> DataFrame:
    """
    Load and process truth/ground truth dataframe.
    
    Args:
        table_name: Fully qualified table name in Spark
        selected_columns: List of column names to select
        er_matching_rules: Dictionary containing matching rules and thresholds
        exception_pairs: List of tuples with (source_pk, target_pk) to handle specially
        
    Returns:
        DataFrame with match_status_manual and human_review columns
    """
    columns_str = ", ".join(selected_columns)
    
    # Build exception condition if provided
    exception_condition = ""
    if exception_pairs:
        conditions = []
        for source_pk, target_pk in exception_pairs:
            conditions.append(
                f"(source_primary_key = '{source_pk}' AND target_primary_key = '{target_pk}')"
            )
        exception_condition = f"""
        WHEN {' OR '.join(conditions)}
        THEN 
            CASE 
                WHEN er_rank = 1 AND
                    (
                        (er_score >= {er_matching_rules['er_score_threshold']} AND suffix_score > 0)
                        OR
                        (
                            er_score > {er_matching_rules['er_score_min']} AND
                            first_name_score >= {er_matching_rules['first_name_score']} AND
                            last_name_score >= {er_matching_rules['last_name_score']} AND
                            middle_name_score >= {er_matching_rules['middle_name_score']} AND
                            city_score >= {er_matching_rules['city_score']} AND
                            address_line_score > {er_matching_rules['address_line_score']} AND
                            state_score >= {er_matching_rules['state_score']} AND
                            postal_code_score >= {er_matching_rules['postal_code_score']} AND
                            suffix_score > 0
                        )
                    )
                THEN 'NOT Matched'
                ELSE 'Matched'
            END
        """
    
    truth_df = spark.sql(f"""
        SELECT {columns_str}, 
        CASE 
            {exception_condition}
            
            WHEN er_rank = 1 AND
                (
                    (er_score >= {er_matching_rules['er_score_threshold']} AND suffix_score > 0)
                    OR
                    (
                        er_score > {er_matching_rules['er_score_min']} AND
                        first_name_score >= {er_matching_rules['first_name_score']} AND
                        last_name_score >= {er_matching_rules['last_name_score']} AND
                        middle_name_score >= {er_matching_rules['middle_name_score']} AND
                        city_score >= {er_matching_rules['city_score']} AND
                        address_line_score > {er_matching_rules['address_line_score']} AND
                        state_score >= {er_matching_rules['state_score']} AND
                        postal_code_score >= {er_matching_rules['postal_code_score']} AND
                        suffix_score > 0
                    )
                )
            THEN 'Matched'
            ELSE 'NOT Matched'
        END AS match_status_manual,
        human_review 
        FROM {table_name}
    """)
    
    return truth_df


def create_error_matrix(
    er_df: DataFrame,
    truth_df: DataFrame
) -> DataFrame:
    """
    Create error matrix (TP, TN, FP, FN) by comparing ER output with truth.
    
    Args:
        er_df: ER output dataframe with match_status_er
        truth_df: Truth dataframe with match_status_manual
        
    Returns:
        DataFrame with error_matrix column
    """
    er_df_alias = er_df.alias("er")
    truth_df_alias = truth_df.alias("truth")
    
    result_df = er_df_alias \
        .join(
            truth_df_alias, 
            on=["source_primary_key", "target_primary_key"], 
            how="left"
        ) \
        .withColumn(
            "error_matrix",
            F.when(
                (F.col("er.match_status_er") == "Matched") & 
                (F.col("truth.match_status_manual") == "Matched"), 
                "TP"
            )
            .when(
                (F.col("er.match_status_er") == "NOT Matched") & 
                (F.col("truth.match_status_manual") == "NOT Matched"), 
                "TN"
            )
            .when(
                (F.col("er.match_status_er") == "Matched") & 
                (F.col("truth.match_status_manual") == "NOT Matched"), 
                "FP"
            )
            .when(
                (F.col("er.match_status_er") == "NOT Matched") & 
                (F.col("truth.match_status_manual") == "Matched"), 
                "FN"
            )
        ) \
        .select(*[F.col(f"er.{col}") for col in er_df.columns], "error_matrix") \
        .dropDuplicates(["source_primary_key", "target_primary_key"])
    
    return result_df


def prepare_dataframes(
    source_df: DataFrame,
    target_df: DataFrame
) -> tuple:
    """
    Prepare source and target dataframes for validation.
    
    Args:
        source_df: Source dataframe to prepare
        target_df: Target dataframe to prepare
        
    Returns:
        Tuple of (prepared_source_df, prepared_target_df)
    """
    # Create temporary views
    source_df.createOrReplaceTempView("source_data_view")
    target_df.createOrReplaceTempView("target_data_view")

    # Fill empty values with 'null'
    prepared_source = source_df.fillna("null").replace("", "null")
    prepared_target = target_df.fillna("null").replace("", "null")
    
    return prepared_source, prepared_target
