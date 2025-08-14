---
title: standardize_country
primitive: Address Primitives
category: pyspark
---

# `standardize_country()`

Standardize country name to consistent format.

Converts various country representations to standard names.

Args:
    col: Column containing country name or abbreviation
    custom_mappings: Optional dict of custom country mappings

Returns:
    Column with standardized country name

Example:
    df.select(addresses.standardize_country(F.col("country")))
    # "US" -> "USA"
    # "U.K." -> "United Kingdom"
    # "Deutschland" -> "Germany"

## Parameters

col, custom_mappings

## Usage

```python
from datacompose import clean_addresses

@clean_addresses.compose()
def pipeline(df):
    return df.standardize_country("column_name")
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
    return df.standardize_country("input_column")

result_df = transform_pipeline(df)
result_df.show()
```

## Source

Found in: `transformers/text/clean_addresses/pyspark/pyspark_primitives.py`

---
[← Back to Address Primitives](/primitives/addresses)
