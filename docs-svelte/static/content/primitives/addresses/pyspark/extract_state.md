---
title: extract_state
primitive: Address Primitives
category: pyspark
---

# `extract_state()`

Extract and standardize state to 2-letter abbreviation.

Handles both full state names and abbreviations, case-insensitive.
Returns standardized 2-letter uppercase abbreviation.

Args:
    col: Column containing address text with state information
    custom_states: Optional dict mapping full state names to abbreviations
                  e.g., {"ONTARIO": "ON", "QUEBEC": "QC"}

Returns:
    Column with 2-letter state abbreviation or empty string if not found

Example:
    # Direct usage
    df.select(addresses.extract_state(F.col("address")))

    # With custom states (e.g., Canadian provinces)
    canadian_provinces = {"ONTARIO": "ON", "QUEBEC": "QC", "BRITISH COLUMBIA": "BC"}
    df.select(addresses.extract_state(F.col("address"), custom_states=canadian_provinces))

## Parameters

col, custom_states

## Usage

```python
from datacompose import clean_addresses

@clean_addresses.compose()
def pipeline(df):
    return df.extract_state("column_name")
```

## Example

```python
# Import the library
from datacompose import clean_addresses
from pyspark.sql import SparkSession

# Create Spark session
spark = SparkSession.builder.appName("DataCompose").getOrCreate()

# Create a sample DataFrame
df = spark.createDataFrame([
    ("example_data_1",),
    ("example_data_2",),
], ["input_column"])

# Apply the transformation
@clean_addresses.compose()
def transform_pipeline(df):
    return df.extract_state("input_column")

result_df = transform_pipeline(df)
result_df.show()
```

## Source

Found in: `transformers/text/clean_addresses/pyspark/pyspark_primitives.py`

---
[← Back to Address Primitives](/primitives/addresses)
