---
title: extract_apartment_number
primitive: Address Primitives
category: pyspark
---

# `extract_apartment_number()`

Extract apartment/unit number from address.

Extracts apartment, suite, unit, or room numbers including:
Apt 5B, Suite 200, Unit 12, #4A, Rm 101, etc.

Args:
    col: Column containing address text

Returns:
    Column with extracted apartment/unit number or empty string

Example:
    df.select(addresses.extract_apartment_number(F.col("address")))
    # "123 Main St Apt 5B" -> "5B"
    # "456 Oak Ave Suite 200" -> "200"
    # "789 Elm St #4A" -> "4A"

## Parameters

col

## Usage

```python
from datacompose import clean_addresses

@clean_addresses.compose()
def pipeline(df):
    return df.extract_apartment_number("column_name")
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
    return df.extract_apartment_number("input_column")

result_df = transform_pipeline(df)
result_df.show()
```

## Source

Found in: `transformers/text/clean_addresses/pyspark/pyspark_primitives.py`

---
[← Back to Address Primitives](/primitives/addresses)
