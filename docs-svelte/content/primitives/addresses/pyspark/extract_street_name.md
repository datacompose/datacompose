---
title: extract_street_name
primitive: Address Primitives
category: pyspark
---

# `extract_street_name()`

Extract street name from address.

Extracts the main street name, excluding number, prefix, and suffix.

Args:
    col: Column containing address text

Returns:
    Column with extracted street name or empty string

Example:
    df.select(addresses.extract_street_name(F.col("address")))
    # "123 N Main Street" -> "Main"
    # "456 Oak Avenue" -> "Oak"
    # "789 Martin Luther King Jr Blvd" -> "Martin Luther King Jr"

## Parameters

col

## Usage

```python
from datacompose import clean_addresses

@clean_addresses.compose()
def pipeline(df):
    return df.extract_street_name("column_name")
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
    return df.extract_street_name("input_column")

result_df = transform_pipeline(df)
result_df.show()
```

## Source

Found in: `transformers/text/clean_addresses/pyspark/pyspark_primitives.py`

---
[← Back to Address Primitives](/primitives/addresses)
