---
title: standardize_po_box
primitive: Address Primitives
category: pyspark
---

# `standardize_po_box()`

Standardize PO Box format to consistent representation.

Converts various PO Box formats to standard "PO Box XXXX" format.

Args:
    col: Column containing PO Box text

Returns:
    Column with standardized PO Box format

Example:
    df.select(addresses.standardize_po_box(F.col("po_box")))
    # "P.O. Box 123" -> "PO Box 123"
    # "POB 456" -> "PO Box 456"
    # "Post Office Box 789" -> "PO Box 789"
    # "123 Main St" -> "123 Main St" (no change if no PO Box)

## Parameters

col

## Usage

```python
from datacompose import clean_addresses

@clean_addresses.compose()
def pipeline(df):
    return df.standardize_po_box("column_name")
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
    return df.standardize_po_box("input_column")

result_df = transform_pipeline(df)
result_df.show()
```

## Source

Found in: `transformers/text/clean_addresses/pyspark/pyspark_primitives.py`

---
[← Back to Address Primitives](/primitives/addresses)
