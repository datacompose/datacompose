---
title: extract_po_box
primitive: Address Primitives
category: pyspark
---

# `extract_po_box()`

Extract PO Box number from address.

Extracts PO Box, P.O. Box, POB, Post Office Box numbers.
Handles various formats including with/without periods and spaces.

Args:
    col: Column containing address text

Returns:
    Column with extracted PO Box number or empty string

Example:
    df.select(addresses.extract_po_box(F.col("address")))
    # "PO Box 123" -> "123"
    # "P.O. Box 456" -> "456"
    # "POB 789" -> "789"
    # "Post Office Box 1011" -> "1011"

## Parameters

col

## Usage

```python
from datacompose import clean_addresses

@clean_addresses.compose()
def pipeline(df):
    return df.extract_po_box("column_name")
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
    return df.extract_po_box("input_column")

result_df = transform_pipeline(df)
result_df.show()
```

## Source

Found in: `transformers/text/clean_addresses/pyspark/pyspark_primitives.py`

---
[← Back to Address Primitives](/primitives/addresses)
