---
title: extract_floor
primitive: Address Primitives
category: pyspark
---

# `extract_floor()`

Extract floor number from address.

Extracts floor information like:
5th Floor, Floor 2, Fl 3, Level 4, etc.

Args:
    col: Column containing address text

Returns:
    Column with extracted floor number or empty string

Example:
    df.select(addresses.extract_floor(F.col("address")))
    # "123 Main St, 5th Floor" -> "5"
    # "456 Oak Ave, Floor 2" -> "2"
    # "789 Elm St, Level 3" -> "3"

## Parameters

col

## Usage

```python
from datacompose import clean_addresses

@clean_addresses.compose()
def pipeline(df):
    return df.extract_floor("column_name")
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
    return df.extract_floor("input_column")

result_df = transform_pipeline(df)
result_df.show()
```

## Source

Found in: `transformers/text/clean_addresses/pyspark/pyspark_primitives.py`

---
[← Back to Address Primitives](/primitives/addresses)
