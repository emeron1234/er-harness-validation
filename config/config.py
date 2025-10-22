"""
Configuration file for ER Harness Validation
"""

# Column Configuration
SELECTED_COLUMNS = [
    "source_primary_key",
    "target_primary_key",
    "source_first_name",
    "target_first_name",
    "source_last_name",
    "target_last_name",
    "source_address_line",
    "target_address_line",
    "source_city",
    "target_city",
    "source_middle_name",
    "target_middle_name",
    "source_postal_code",
    "target_postal_code",
    "source_state",
    "target_state",
    "source_suffix",
    "target_suffix",
    "target_entity_id",
    "er_score",
    "er_rank",
    "first_name_score",
    "last_name_score",
    "middle_name_score",
    "city_score",
    "address_line_score",
    "state_score",
    "postal_code_score",
    "suffix_score",
]

# Lookup library for column comparison
LOOKUP_LIBRARY = [
    "source_primary_key",
    "target_primary_key",
    "source_first_name",
    "target_first_name",
    "source_last_name",
    "target_last_name",
    "source_address_line",
    "target_address_line",
    "source_city",
    "target_city",
    "source_middle_name",
    "target_middle_name",
    "source_postal_code",
    "target_postal_code",
    "source_state",
    "target_state",
    "source_suffix",
    "target_suffix",
    "target_entity_id",
    "er_score",
    "er_rank",
    "match_status_er",
    "match_status_manual",
    "error_matrix"
]

# ER Matching Rules and Thresholds
ER_MATCHING_RULES = {
    "er_score_threshold": 0.8,
    "er_score_min": 0.6,
    "first_name_score": 0.12,
    "last_name_score": 0.12,
    "middle_name_score": 0.02,
    "city_score": 0.08,
    "address_line_score": 0.14,
    "state_score": 0.05,
    "postal_code_score": 0.11,
}

# Key columns for MD5 generation
KEY_COLUMNS = "source_primary_key, target_primary_key"

# Columns to drop during validation
DROP_COLUMNS = [
    "src_tgt_key_columns_tf",
    "src_tgt_key_columns_eo"
]

# Exception pairs for testing (source_primary_key, target_primary_key)
EXCEPTION_PAIRS = [
    ('4f81ac70-491d-4d07-86c1-1026563c0458', 'd9a4bd4e-3b15-42dc-a9c9-c5590ae5895c'),
    ('9644f1a1-9140-4b1d-aeba-61c1ff11068f', '0922ddad-651b-4d62-9f54-8bf005d40da3'),
    ('0e32e4f7-da08-4817-8281-0ba70d2767e9', '890d5f8e-df8f-4d08-9c91-5701d1b13e63'),
]

# Database Configuration
DATABASE_CONFIG = {
    "er_output_table": "altrata_data_lake_poc_dev.synthetic_er_harness.er_test_pam_matching_pairs_sorted_by_overall_score",
    "truth_table": "altrata_data_lake_poc_dev.synthetic_er_harness.er_test_truth",
}

# Output Configuration
OUTPUT_COLUMNS = [
    "source_primary_key", 
    "target_primary_key", 
    "target_entity_id", 
    "human_review", 
    "match_status_er", 
    "match_status_manual", 
    "error_matrix",
    "mismatches_code", 
    "mismatches_columns", 
    "mismatch_deviation", 
    "mismatch_type", 
    "validation_status",
    "batch_id"
]
