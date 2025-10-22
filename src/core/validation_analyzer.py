"""
Validation Analysis Module
Analyzes mismatch patterns and categorizes validation results
"""

from typing import List, Tuple, Dict, Optional


def get_mismatching_columns(
    binary_string: str, 
    lookup_lib: List[str]
) -> Tuple[List[str], int]:
    """
    Identify which columns correspond to 0 bits in the mismatch string.
    
    Args:
        binary_string: Binary string where 0 indicates mismatch
        lookup_lib: List of column names corresponding to positions
        
    Returns:
        Tuple of (unmatching_columns, deviation_percentage)
        - unmatching_columns: List of column names with mismatches
        - deviation_percentage: Percentage of columns that mismatched
    """
    # Convert binary string to list of integers
    binary_list = [int(bit) for bit in binary_string]

    # Find corresponding values from lookup dictionary where the value is 0
    unmatching_columns = [
        lookup_lib[i] 
        for i, j in enumerate(binary_list) 
        if j == 0
    ]
    
    # Calculate the deviation and convert to percentage
    deviation = (len(unmatching_columns) / len(lookup_lib)) * 100
    # Converting float to int for percentage
    deviation = int(deviation)

    return unmatching_columns, deviation


def get_status_mismatch_type(
    binary_string: str, 
    extra_or_missing_flag: Optional[bool], 
    lookup_lib: List[str]
) -> Tuple[Optional[str], Optional[str]]:
    """
    Read the mismatch binary code and convert to readable description.
    
    Args:
        binary_string: Binary string where 0 indicates mismatch
        extra_or_missing_flag: Boolean flag
            - False: 'Extra Record in ER Output'
            - True: 'Record Missing from ER Output'
            - None: Normal record comparison
        lookup_lib: List of column names corresponding to positions
        
    Returns:
        Tuple of (final_status, mismatch_type)
        - final_status: 'Match' or 'Mismatch'
        - mismatch_type: Detailed mismatch category
    """
    try:
        binary_list = list(binary_string)  

        # Determine overall status
        if '0' in binary_list:
            final_status = 'Mismatch'
        else:
            final_status = 'Match'

        # Check if target_entity_id (WE_PID) is in the mismatch
        try:
            indexing = lookup_lib.index('target_entity_id')
            mismatch = binary_list[indexing]
        except (ValueError, IndexError):
            mismatch = '1'  # Default to matched if not found

        mismatch_col = binary_list.count('0')

        # Categorize mismatch type
        if final_status == 'Match' and extra_or_missing_flag is None:
            get_mismatch_type = 'NA'
            
        elif mismatch == '0' and final_status == 'Mismatch' and \
             extra_or_missing_flag is None and mismatch_col == 1:
            get_mismatch_type = 'Only WE_PID mismatch'
            
        elif mismatch == '0' and final_status == 'Mismatch' and \
             extra_or_missing_flag is None and mismatch_col > 1:
            get_mismatch_type = 'WE_PID and other column mismatch'
            
        elif mismatch == '1' and final_status == 'Mismatch' and \
             extra_or_missing_flag is None and mismatch_col >= 1:
            get_mismatch_type = 'Other columns mismatch'
            
        elif final_status == 'Mismatch' and extra_or_missing_flag is False:
            get_mismatch_type = 'Extra Record in ER Output'
            
        elif final_status == 'Mismatch' and extra_or_missing_flag is True:
            get_mismatch_type = 'Record Missing from ER Output'
            
        else:
            get_mismatch_type = 'Unknown Error'

        return final_status, get_mismatch_type
        
    except Exception:
        return (None, None)
