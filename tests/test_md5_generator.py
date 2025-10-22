"""
Unit tests for MD5 Generator module
"""

import pytest
from pyspark.sql import SparkSession
from src.core.md5_generator import generate_md5_concatenate_columns


@pytest.fixture(scope="module")
def spark():
    """Create a Spark session for testing"""
    return SparkSession.builder \
        .master("local[1]") \
        .appName("TestMD5Generator") \
        .getOrCreate()


def test_generate_md5_concatenate_columns(spark):
    """Test MD5 hash generation"""
    # Create test data
    test_data = [
        ("pk1", "pk2", "John", "Doe"),
        ("pk3", "pk4", "Jane", "Smith"),
    ]
    
    df = spark.createDataFrame(
        test_data, 
        ["source_primary_key", "target_primary_key", "first_name", "last_name"]
    )
    
    # Create temp view
    df.createOrReplaceTempView("test_view")
    
    # Generate MD5
    result_df = generate_md5_concatenate_columns(
        key_column="source_primary_key,target_primary_key",
        view_or_table="test_view"
    )
    
    # Assertions
    assert result_df is not None
    assert "key_columns" in result_df.columns
    assert "md5_hash_concat_column" in result_df.columns
    assert result_df.count() == 2


def test_generate_md5_with_null_values(spark):
    """Test MD5 generation with null values"""
    test_data = [
        ("pk1", "pk2", "John", None),
        ("pk3", None, "Jane", "Smith"),
    ]
    
    df = spark.createDataFrame(
        test_data, 
        ["source_primary_key", "target_primary_key", "first_name", "last_name"]
    )
    
    df.createOrReplaceTempView("test_view_null")
    
    result_df = generate_md5_concatenate_columns(
        key_column="source_primary_key,target_primary_key",
        view_or_table="test_view_null"
    )
    
    assert result_df is not None
    assert result_df.count() == 2
