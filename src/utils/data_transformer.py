"""
Data Transformation Utilities
Helper functions for data transformation operations
"""

from pyspark.sql import DataFrame


def rename_columns(df: DataFrame) -> DataFrame:
    """
    Remove _tf and _eo suffixes from column names.
    
    Args:
        df: DataFrame with suffixed column names
        
    Returns:
        DataFrame with cleaned column names
    """
    for col in df.columns:
        if '_tf' in col:
            new_col = col.replace('_tf', '')
            df = df.withColumnRenamed(col, new_col)
        else:
            new_col = col.replace('_eo', '')
            df = df.withColumnRenamed(col, new_col)
    return df
