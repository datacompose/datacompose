---
title: is_po_box_only
primitive: Address Primitives
category: pyspark
---

# `is_po_box_only()`

Check if address is ONLY a PO Box (no street address).

Args:
    col: Column containing address text

Returns:
    Column with boolean indicating if address is PO Box only

Example:
    df.select(addresses.is_po_box_only(F.col("address")))
    # "PO Box 123" -> True
    # "123 Main St, PO Box 456" -> False
    # "PO Box 789, New York, NY" -> True

## Parameters

col

## Usage

```python
from datacompose import clean_addresses

@clean_addresses.compose()
def pipeline(df):
    return df.is_po_box_only("column_name")
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
    return df.is_po_box_only("input_column")

result_df = transform_pipeline(df)
result_df.show()
```

## Source

Found in: `transformers/text/clean_addresses/pyspark/pyspark_primitives.py`

---
[← Back to Address Primitives](/primitives/addresses)
