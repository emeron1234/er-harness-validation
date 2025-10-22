# API Reference

## Core Modules

### md5_generator.py

#### `generate_md5_concatenate_columns(key_column, view_or_table)`

Generate MD5 hashes for concatenated columns.

**Parameters:**
- `key_column` (str): Comma-separated string of key column names
- `view_or_table` (str): Name of the Spark view or table to process

**Returns:**
- DataFrame with added `key_columns` and `md5_hash_concat_column`

**Example:**
```python
from src.core.md5_generator import generate_md5_concatenate_columns

df = generate_md5_concatenate_columns(
    key_column="source_pk, target_pk",
    view_or_table="my_table"
)
```

---

### record_matcher.py

#### `map_match_mismatch_records(source_dataframe, target_dataframe, drop_columns)`

Identify Match/Mismatch comparison results.

**Parameters:**
- `source_dataframe` (DataFrame): Source dataframe with _eo suffix
- `target_dataframe` (DataFrame): Target dataframe with _tf suffix
- `drop_columns` (List[str]): Column names to drop from result

**Returns:**
- Tuple[DataFrame, DataFrame]: (match_df, mismatch_df)

**Example:**
```python
from src.core.record_matcher import map_match_mismatch_records

match_df, mismatch_df = map_match_mismatch_records(
    source_df, target_df, 
    drop_columns=['temp_col']
)
```

#### `map_extra_records(source_df, target_df, key_column_source, key_column_target, repopulate_columns, drop_columns)`

Identify records that exist in source but not in target.

**Parameters:**
- `source_df` (DataFrame): Source dataframe
- `target_df` (DataFrame): Target dataframe
- `key_column_source` (str): Key column name in source
- `key_column_target` (str): Key column name in target
- `repopulate_columns` (List[str]): Columns to populate with None
- `drop_columns` (List[str]): Columns to drop from result

**Returns:**
- DataFrame with extra records marked as Mismatch

---

### validation_analyzer.py

#### `get_mismatching_columns(binary_string, lookup_lib)`

Identify which columns have mismatches based on binary code.

**Parameters:**
- `binary_string` (str): Binary string where 0 indicates mismatch
- `lookup_lib` (List[str]): List of column names corresponding to positions

**Returns:**
- Tuple[List[str], int]: (unmatching_columns, deviation_percentage)

**Example:**
```python
from src.core.validation_analyzer import get_mismatching_columns

columns, deviation = get_mismatching_columns(
    "101010",
    ["col1", "col2", "col3", "col4", "col5", "col6"]
)
```

#### `get_status_mismatch_type(binary_string, extra_or_missing_flag, lookup_lib)`

Convert mismatch binary code to readable description.

**Parameters:**
- `binary_string` (str): Binary string where 0 indicates mismatch
- `extra_or_missing_flag` (Optional[bool]): Flag for extra/missing records
- `lookup_lib` (List[str]): List of column names

**Returns:**
- Tuple[Optional[str], Optional[str]]: (final_status, mismatch_type)

---

## Utility Modules

### data_loader.py

#### `load_er_dataframe(table_name, selected_columns, er_matching_rules)`

Load and process ER output dataframe.

**Parameters:**
- `table_name` (str): Fully qualified table name
- `selected_columns` (List[str]): Columns to select
- `er_matching_rules` (Dict): Matching rules and thresholds

**Returns:**
- DataFrame with match_status_er column

#### `load_truth_dataframe(table_name, selected_columns, er_matching_rules, exception_pairs)`

Load and process truth dataframe.

**Parameters:**
- `table_name` (str): Fully qualified table name
- `selected_columns` (List[str]): Columns to select
- `er_matching_rules` (Dict): Matching rules and thresholds
- `exception_pairs` (List[tuple], optional): Special test cases

**Returns:**
- DataFrame with match_status_manual column

#### `create_error_matrix(er_df, truth_df)`

Create error matrix (TP, TN, FP, FN).

**Parameters:**
- `er_df` (DataFrame): ER output dataframe
- `truth_df` (DataFrame): Truth dataframe

**Returns:**
- DataFrame with error_matrix column

---

### data_transformer.py

#### `rename_columns(df)`

Remove _tf and _eo suffixes from column names.

**Parameters:**
- `df` (DataFrame): DataFrame with suffixed column names

**Returns:**
- DataFrame with cleaned column names

---

## Main Orchestrator

### ERHarnessValidator Class

Main validation orchestrator.

#### `__init__(spark_session=None)`

Initialize the validator.

#### `run_validation()`

Execute the complete validation workflow.

**Returns:**
- DataFrame: Final validation results

#### `display_results()`

Display validation results in tabular format.

#### `save_results(output_path, format='parquet')`

Save validation results to storage.

**Parameters:**
- `output_path` (str): Path to save results
- `format` (str): Output format (parquet, csv, delta, etc.)

**Example:**
```python
from main import ERHarnessValidator

validator = ERHarnessValidator()
results = validator.run_validation()
validator.display_results()
validator.save_results("/output/path", format="delta")
```
