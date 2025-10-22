"""
Unit tests for Validation Analyzer module
"""

import pytest
from src.core.validation_analyzer import get_mismatching_columns, get_status_mismatch_type


def test_get_mismatching_columns():
    """Test mismatch column identification"""
    binary_string = "101010"
    lookup_lib = ["col1", "col2", "col3", "col4", "col5", "col6"]
    
    unmatching_cols, deviation = get_mismatching_columns(binary_string, lookup_lib)
    
    assert len(unmatching_cols) == 3
    assert "col2" in unmatching_cols
    assert "col4" in unmatching_cols
    assert "col6" in unmatching_cols
    assert deviation == 50  # 3 out of 6 = 50%


def test_get_mismatching_columns_all_match():
    """Test when all columns match"""
    binary_string = "111111"
    lookup_lib = ["col1", "col2", "col3", "col4", "col5", "col6"]
    
    unmatching_cols, deviation = get_mismatching_columns(binary_string, lookup_lib)
    
    assert len(unmatching_cols) == 0
    assert deviation == 0


def test_get_status_mismatch_type_match():
    """Test mismatch type for matching records"""
    binary_string = "111111"
    lookup_lib = ["col1", "col2", "target_entity_id", "col4", "col5", "col6"]
    
    status, mismatch_type = get_status_mismatch_type(
        binary_string, 
        None, 
        lookup_lib
    )
    
    assert status == "Match"
    assert mismatch_type == "NA"


def test_get_status_mismatch_type_we_pid_only():
    """Test mismatch type for WE_PID only mismatch"""
    binary_string = "110111"
    lookup_lib = ["col1", "col2", "target_entity_id", "col4", "col5", "col6"]
    
    status, mismatch_type = get_status_mismatch_type(
        binary_string, 
        None, 
        lookup_lib
    )
    
    assert status == "Mismatch"
    assert mismatch_type == "Only WE_PID mismatch"


def test_get_status_mismatch_type_extra_record():
    """Test mismatch type for extra records"""
    binary_string = "000000"
    lookup_lib = ["col1", "col2", "target_entity_id", "col4", "col5", "col6"]
    
    status, mismatch_type = get_status_mismatch_type(
        binary_string, 
        False,  # Extra record in ER Output
        lookup_lib
    )
    
    assert status == "Mismatch"
    assert mismatch_type == "Extra Record in ER Output"


def test_get_status_mismatch_type_missing_record():
    """Test mismatch type for missing records"""
    binary_string = "000000"
    lookup_lib = ["col1", "col2", "target_entity_id", "col4", "col5", "col6"]
    
    status, mismatch_type = get_status_mismatch_type(
        binary_string, 
        True,  # Record missing from ER Output
        lookup_lib
    )
    
    assert status == "Mismatch"
    assert mismatch_type == "Record Missing from ER Output"
