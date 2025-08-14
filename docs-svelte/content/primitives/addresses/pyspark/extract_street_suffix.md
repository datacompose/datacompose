---
title: extract_street_suffix
primitive: Address Primitives
category: pyspark
---

# `extract_street_suffix()`

Extract street type/suffix from address.

Extracts street type like Street, Avenue, Road, Boulevard, etc.

Args:
    col: Column containing address text

Returns:
    Column with extracted street suffix or empty string

Example:
    df.select(addresses.extract_street_suffix(F.col("address")))
    # "123 Main Street" -> "Street"
    # "456 Oak Ave" -> "Ave"
    # "789 Elm Boulevard" -> "Boulevard"

## Parameters

col

## Usage

```python
from datacompose import clean_addresses

@clean_addresses.compose()
def pipeline(df):
    return df.extract_street_suffix("column_name")
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
    return df.extract_street_suffix("input_column")

result_df = transform_pipeline(df)
result_df.show()
```

## Source

Found in: `transformers/text/clean_addresses/pyspark/pyspark_primitives.py`

---
[← Back to Address Primitives](/primitives/addresses)
