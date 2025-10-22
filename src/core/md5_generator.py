"""
MD5 Hash Generation Module
Generates MD5 hashes for data comparison
"""

from typing import Text
from pyspark.sql import DataFrame
from pyspark.shell import spark


def generate_md5_concatenate_columns(
    key_column: Text, 
    view_or_table: Text
) -> DataFrame:
    """
    Generate MD5 hashes for concatenated columns.
    
    Generates:
    - key_columns: Join of source_primary_key & target_primary_key
    - concat_columns: Join of all columns
    - MD5 hashing of all concat_columns
    
    Args:
        key_column: Comma-separated string of key column names
        view_or_table: Name of the Spark view or table to process
        
    Returns:
        DataFrame with added key_columns and md5_hash_concat_column
        
    Raises:
        Exception: If an error occurs during MD5 generation
    """
    try:
        # Get all columns except specific ones
        all_columns = spark.sql(
            f"SELECT * FROM {view_or_table}"
        ).drop(
            'human_review', 
            'match_status_er', 
            'match_status_manual', 
            'error_matrix'
        ).columns
        
        key_columns = key_column.split(",")

        # Join both key_columns (source_primary_key, target_primary_key)
        all_columns_string = "\n||'-'||".join(
            [f"nvl(cast({x} as string), '')" for x in all_columns]
        )
        key_columns_string = "\n||'-'||".join(
            [f"nvl(cast({x} as string), '')" for x in key_columns]
        )
        
        query = f"""
            SELECT *, 
                {key_columns_string} as key_columns,
                md5({all_columns_string}) as md5_hash_concat_column
            FROM {view_or_table}
        """

        df = spark.sql(query)        
        return df

    except Exception as e:
        print(f"An unexpected error occurred while generating MD5 concatenation: {e}")
        return None
