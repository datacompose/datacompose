---
title: standardize_street_prefix
primitive: Address Primitives
category: pyspark
---

# `standardize_street_prefix()`

Standardize street directional prefixes to abbreviated form.

Converts all variations to standard USPS abbreviations:
North/N/N. → N, South/S/S. → S, etc.

Args:
    col: Column containing street prefix
    custom_mappings: Optional dict of custom prefix mappings (case insensitive)

Returns:
    Column with standardized prefix (always abbreviated per USPS standards)

Example:
    df.select(addresses.standardize_street_prefix(F.col("prefix")))
    # "North" -> "N"
    # "south" -> "S"
    # "NorthEast" -> "NE"

## Parameters

col, custom_mappings

## Usage

```python
from datacompose import clean_addresses

@clean_addresses.compose()
def pipeline(df):
    return df.standardize_street_prefix("column_name")
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
    return df.standardize_street_prefix("input_column")

result_df = transform_pipeline(df)
result_df.show()
```

## Source

Found in: `transformers/text/clean_addresses/pyspark/pyspark_primitives.py`

---
[← Back to Address Primitives](/primitives/addresses)
