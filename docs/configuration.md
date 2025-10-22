# Configuration Guide

## Overview

The `config/config.py` file contains all configurable parameters for the ER Harness Validation framework.

## Configuration Sections

### 1. Column Configuration

Defines which columns to select from the source tables:

```python
SELECTED_COLUMNS = [
    "source_primary_key",
    "target_primary_key",
    # ... other columns
]
```

### 2. Lookup Library

Defines the ordered list of columns used for binary mismatch code generation:

```python
LOOKUP_LIBRARY = [
    "source_primary_key",
    "target_primary_key",
    # ... other columns
]
```

Each position in this list corresponds to a bit position in the mismatch code.

### 3. ER Matching Rules

Score thresholds for determining matches:

```python
ER_MATCHING_RULES = {
    "er_score_threshold": 0.8,  # High confidence threshold
    "er_score_min": 0.6,         # Minimum score for consideration
    "first_name_score": 0.12,    # Minimum first name similarity
    # ... other score thresholds
}
```

### 4. Key Columns

Columns used for generating MD5 hashes:

```python
KEY_COLUMNS = "source_primary_key, target_primary_key"
```

### 5. Database Configuration

Table locations in your data lake:

```python
DATABASE_CONFIG = {
    "er_output_table": "catalog.schema.er_output_table",
    "truth_table": "catalog.schema.truth_table",
}
```

### 6. Exception Pairs

Special test cases where the logic should be inverted:

```python
EXCEPTION_PAIRS = [
    ('source_pk_1', 'target_pk_1'),
    ('source_pk_2', 'target_pk_2'),
]
```

## Customization Tips

1. **Adjusting Thresholds**: Modify `ER_MATCHING_RULES` based on your data quality requirements
2. **Adding Columns**: Update both `SELECTED_COLUMNS` and `LOOKUP_LIBRARY` when adding new fields
3. **Changing Tables**: Update `DATABASE_CONFIG` to point to your specific tables

## Example Custom Configuration

```python
# config/config_custom.py
from config.config import *

# Override specific settings
ER_MATCHING_RULES['er_score_threshold'] = 0.85
DATABASE_CONFIG['er_output_table'] = 'my_catalog.my_schema.my_table'
```
