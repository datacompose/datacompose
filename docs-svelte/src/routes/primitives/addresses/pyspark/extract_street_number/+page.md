---
title: extract_street_number
primitive: Address Primitives
category: pyspark
---

# `extract_street_number()`

Extract street/house number from address.

Extracts the numeric portion at the beginning of an address.
Handles various formats: 123, 123A, 123-125, etc.

Args:
    col: Column containing address text

Returns:
    Column with extracted street number or empty string

Example:
    df.select(addresses.extract_street_number(F.col("address")))
    # "123 Main St" -> "123"
    # "123A Oak Ave" -> "123A"
    # "123-125 Elm St" -> "123-125"

## Parameters

col

## Usage

```python
from datacompose import clean_addresses

@clean_addresses.compose()
def pipeline(df):
    return df.extract_street_number("column_name")
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
    return df.extract_street_number("input_column")

result_df = transform_pipeline(df)
result_df.show()
```

## Source

Found in: `transformers/text/clean_addresses/pyspark/pyspark_primitives.py`

---
[← Back to Address Primitives](/primitives/addresses)
