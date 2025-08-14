---
title: extract_building
primitive: Address Primitives
category: pyspark
---

# `extract_building()`

Extract building name or identifier from address.

Extracts building information like:
Building A, Tower 2, Complex B, Block C, etc.

Args:
    col: Column containing address text

Returns:
    Column with extracted building identifier or empty string

Example:
    df.select(addresses.extract_building(F.col("address")))
    # "123 Main St, Building A" -> "A"
    # "456 Oak Ave, Tower 2" -> "2"
    # "789 Elm St, Complex North" -> "North"

## Parameters

col

## Usage

```python
from datacompose import clean_addresses

@clean_addresses.compose()
def pipeline(df):
    return df.extract_building("column_name")
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
    return df.extract_building("input_column")

result_df = transform_pipeline(df)
result_df.show()
```

## Source

Found in: `transformers/text/clean_addresses/pyspark/pyspark_primitives.py`

---
[← Back to Address Primitives](/primitives/addresses)
