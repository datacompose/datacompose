---
title: remove_po_box
primitive: Address Primitives
category: pyspark
---

# `remove_po_box()`

Remove PO Box from address.

Removes PO Box information while preserving other address components.

Args:
    col: Column containing address text

Returns:
    Column with PO Box removed

Example:
    df.select(addresses.remove_po_box(F.col("address")))
    # "123 Main St, PO Box 456" -> "123 Main St"
    # "PO Box 789, New York, NY" -> "New York, NY"

## Parameters

col

## Usage

```python
from datacompose import clean_addresses

@clean_addresses.compose()
def pipeline(df):
    return df.remove_po_box("column_name")
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
    return df.remove_po_box("input_column")

result_df = transform_pipeline(df)
result_df.show()
```

## Source

Found in: `transformers/text/clean_addresses/pyspark/pyspark_primitives.py`

---
[← Back to Address Primitives](/primitives/addresses)
