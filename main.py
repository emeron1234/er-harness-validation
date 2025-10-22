"""
Main ER Harness Validation Script
Orchestrates the complete validation workflow
"""

from datetime import datetime
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import col, lit, concat_ws, udf
from pyspark.sql.types import StructType, StructField, ArrayType, StringType, IntegerType

# Import project modules
from src.core.md5_generator import generate_md5_concatenate_columns
from src.core.record_matcher import map_match_mismatch_records, map_extra_records
from src.core.validation_analyzer import get_mismatching_columns, get_status_mismatch_type
from src.utils.data_loader import (
    load_er_dataframe, 
    load_truth_dataframe, 
    create_error_matrix, 
    prepare_dataframes
)
from src.utils.data_transformer import rename_columns
from config.config import (
    SELECTED_COLUMNS,
    LOOKUP_LIBRARY,
    ER_MATCHING_RULES,
    KEY_COLUMNS,
    DROP_COLUMNS,
    EXCEPTION_PAIRS,
    DATABASE_CONFIG,
    OUTPUT_COLUMNS
)


class ERHarnessValidator:
    """Main validation orchestrator for ER Harness"""
    
    def __init__(self, spark_session=None):
        """
        Initialize the validator.
        
        Args:
            spark_session: Spark session (optional, will use default if not provided)
        """
        self.spark = spark_session
        self.batch_id = datetime.utcnow().strftime("%Y%m%d%H%M%S")
        self.lookup_lib = LOOKUP_LIBRARY
        
    def load_data(self):
        """Load ER output and truth data"""
        print("Loading data...")
        
        # Load ER dataframe
        er_df = load_er_dataframe(
            table_name=DATABASE_CONFIG["er_output_table"],
            selected_columns=SELECTED_COLUMNS,
            er_matching_rules=ER_MATCHING_RULES
        )
        
        # Load truth dataframe
        truth_df = load_truth_dataframe(
            table_name=DATABASE_CONFIG["truth_table"],
            selected_columns=SELECTED_COLUMNS,
            er_matching_rules=ER_MATCHING_RULES,
            exception_pairs=EXCEPTION_PAIRS
        )
        
        # Create error matrix
        source_df = create_error_matrix(er_df, truth_df)
        target_df = truth_df
        
        # Prepare dataframes
        self.source_dataframe, self.target_dataframe = prepare_dataframes(
            source_df, 
            target_df
        )
        
        print(f"Loaded {self.source_dataframe.count()} source records")
        print(f"Loaded {self.target_dataframe.count()} target records")
        
    def generate_hashes(self):
        """Generate MD5 hashes for source and target dataframes"""
        print("Generating MD5 hashes...")
        
        # Generate MD5 for source (ER output)
        source_with_hash = generate_md5_concatenate_columns(
            key_column=KEY_COLUMNS, 
            view_or_table="source_data_view"
        )
        source_with_hash = (
            source_with_hash
            .withColumnRenamed("key_columns", "src_tgt_key_columns")
            .withColumnRenamed("md5_hash_concat_column", "src_tgt_md5_hash_column")
        )
        
        # Rename all Source columns to _eo (ER output)
        for column in source_with_hash.columns:
            source_with_hash = source_with_hash.withColumnRenamed(
                column, 
                f"{column}_eo"
            )
        
        # Generate MD5 for target (truth file)
        target_with_hash = generate_md5_concatenate_columns(
            key_column=KEY_COLUMNS, 
            view_or_table="target_data_view"
        )
        target_with_hash = (
            target_with_hash
            .withColumnRenamed("key_columns", "src_tgt_key_columns")
            .withColumnRenamed("md5_hash_concat_column", "src_tgt_md5_hash_column")
        )
        
        # Rename all Target columns to _tf (truth file)
        for column in target_with_hash.columns:
            target_with_hash = target_with_hash.withColumnRenamed(
                column, 
                f"{column}_tf"
            )
        
        self.source_with_hash = source_with_hash
        self.target_with_hash = target_with_hash
        
    def identify_matches_and_mismatches(self):
        """Identify match and mismatch records"""
        print("Identifying matches and mismatches...")
        
        # Match & Mismatch records
        match_df, mismatch_df = map_match_mismatch_records(
            self.source_with_hash, 
            self.target_with_hash, 
            drop_columns=DROP_COLUMNS
        )
        
        print(f"Found {match_df.count()} matching records")
        print(f"Found {mismatch_df.count()} mismatching records")
        
        self.match_df = match_df
        self.mismatch_df = mismatch_df
        
    def identify_extra_records(self):
        """Identify extra records from either ER Output or Truth Table"""
        print("Identifying extra records...")
        
        # Extra records in source (ER Output)
        source_extra_record_df = map_extra_records(
            self.source_with_hash,
            self.target_with_hash,
            key_column_source="src_tgt_key_columns_eo",
            key_column_target="src_tgt_key_columns_tf",
            repopulate_columns=self.target_with_hash.columns,
            drop_columns=["src_tgt_key_columns_eo", "src_tgt_key_columns_tf"],
        )
        
        # Extra records in target (Truth File)
        target_extra_record_df = map_extra_records(
            self.target_with_hash,
            self.source_with_hash,
            key_column_source="src_tgt_key_columns_tf",
            key_column_target="src_tgt_key_columns_eo",
            repopulate_columns=self.source_with_hash.columns,
            drop_columns=["src_tgt_key_columns_eo", "src_tgt_key_columns_tf"],
        )
        
        print(f"Found {source_extra_record_df.count()} extra records in ER Output")
        print(f"Found {target_extra_record_df.count()} missing records from ER Output")
        
        self.source_extra_df = source_extra_record_df
        self.target_extra_df = target_extra_record_df
        
    def analyze_mismatches(self):
        """Analyze mismatch patterns and create final output"""
        print("Analyzing mismatch patterns...")
        
        # Union all dataframes
        union_df = (
            self.match_df
            .unionByName(self.mismatch_df)
            .unionByName(self.source_extra_df)
            .unionByName(self.target_extra_df)
            .distinct()
        )
        
        # Define UDF schema for get_mismatching_columns
        deviation_schema = StructType([
            StructField("mismatches_columns", ArrayType(StringType()), True),
            StructField("mismatch_deviation", IntegerType(), True)
        ])
        
        # Register UDF for mismatching columns
        get_mismatching_columns_udf = udf(
            lambda x: get_mismatching_columns(x, self.lookup_lib), 
            deviation_schema
        )
        
        # Apply UDF to get mismatching columns
        mismatching_col = union_df.withColumn(
            "result", 
            get_mismatching_columns_udf(union_df.mismatches_code)
        )
        mismatching_col = mismatching_col \
            .withColumn(
                "mismatches_columns", 
                concat_ws(',', mismatching_col.result.mismatches_columns)
            ) \
            .withColumn(
                "mismatch_deviation", 
                mismatching_col.result.mismatch_deviation
            ) \
            .drop("result")
        
        # Define UDF schema for get_status_mismatch_type
        mismatch_type_schema = StructType([
            StructField("mismatch", StringType(), True),
            StructField("mismatch_type", StringType(), True)
        ])
        
        # Register UDF for mismatch type
        get_status_mismatch_type_udf = udf(
            lambda x, y: get_status_mismatch_type(x, y, self.lookup_lib), 
            mismatch_type_schema
        )
        
        # Apply UDF to get mismatch status and type
        status_mismatch = mismatching_col.withColumn(
            "result", 
            get_status_mismatch_type_udf(
                mismatching_col.mismatches_code, 
                mismatching_col.extra_or_missing_flag
            )
        )
        status_mismatch = status_mismatch \
            .withColumn(
                "mismatch", 
                concat_ws(',', status_mismatch.result.mismatch)
            ) \
            .withColumn(
                "mismatch_type", 
                status_mismatch.result.mismatch_type
            ) \
            .withColumnRenamed("human_review_tf", "human_review") \
            .drop("result", "mismatch")
        
        # Clean up columns
        columns_to_drop = [
            col for col in status_mismatch.columns 
            if col.endswith('_tf') and col != 'match_status_manual_tf'
        ]
        cleaned_df = status_mismatch.drop(*columns_to_drop)
        
        # Rename columns and add batch_id
        rename_col_df = rename_columns(cleaned_df)
        final_df = rename_col_df.withColumn("batch_id", lit(self.batch_id))
        
        self.final_df = final_df
        
    def run_validation(self):
        """Execute the complete validation workflow"""
        print("=" * 60)
        print("Starting ER Harness Validation")
        print("=" * 60)
        
        self.load_data()
        self.generate_hashes()
        self.identify_matches_and_mismatches()
        self.identify_extra_records()
        self.analyze_mismatches()
        
        print("=" * 60)
        print("Validation Complete!")
        print("=" * 60)
        
        return self.final_df
    
    def display_results(self):
        """Display validation results"""
        if hasattr(self, 'final_df'):
            print("\nValidation Results:")
            self.final_df.select(*OUTPUT_COLUMNS).display()
        else:
            print("No results to display. Run validation first.")
    
    def save_results(self, output_path, format="parquet"):
        """
        Save validation results to storage.
        
        Args:
            output_path: Path to save results
            format: Output format (parquet, csv, delta, etc.)
        """
        if hasattr(self, 'final_df'):
            print(f"Saving results to {output_path}...")
            self.final_df.write.mode("overwrite").format(format).save(output_path)
            print("Results saved successfully!")
        else:
            print("No results to save. Run validation first.")


def main():
    """Main entry point for the validation script"""
    # Initialize validator
    validator = ERHarnessValidator()
    
    # Run validation
    final_df = validator.run_validation()
    
    # Display results
    validator.display_results()
    
    # Optional: Save results
    # validator.save_results("/path/to/output")
    
    return final_df


if __name__ == "__main__":
    main()
