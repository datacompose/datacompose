---
title: extract_full_street
primitive: Address Primitives
category: pyspark
---

# `extract_full_street()`

Extract complete street address (number + prefix + name + suffix).

Extracts everything before apartment/suite and city/state/zip.

Args:
    col: Column containing address text

Returns:
    Column with extracted street address or empty string

Example:
    df.select(addresses.extract_full_street(F.col("address")))
    # "123 N Main St, Apt 4B, New York, NY" -> "123 N Main St"

## Parameters

col

## Usage

```python
from datacompose import clean_addresses

@clean_addresses.compose()
def pipeline(df):
    return df.extract_full_street("column_name")
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
    return df.extract_full_street("input_column")

result_df = transform_pipeline(df)
result_df.show()
```

## Source

Found in: `transformers/text/clean_addresses/pyspark/pyspark_primitives.py`

---
[← Back to Address Primitives](/primitives/addresses)
