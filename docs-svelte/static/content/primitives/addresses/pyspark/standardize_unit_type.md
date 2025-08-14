---
title: standardize_unit_type
primitive: Address Primitives
category: pyspark
---

# `standardize_unit_type()`

Standardize unit type to common abbreviations.

Converts all variations to standard abbreviations:
Apartment/Apt. → Apt, Suite → Ste, Room → Rm, etc.

Args:
    col: Column containing unit type
    custom_mappings: Optional dict of custom unit type mappings

Returns:
    Column with standardized unit type

Example:
    df.select(addresses.standardize_unit_type(F.col("unit_type")))
    # "Apartment" -> "Apt"
    # "Suite" -> "Ste"
    # "Room" -> "Rm"

## Parameters

col, custom_mappings

## Usage

```python
from datacompose import clean_addresses

@clean_addresses.compose()
def pipeline(df):
    return df.standardize_unit_type("column_name")
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
    return df.standardize_unit_type("input_column")

result_df = transform_pipeline(df)
result_df.show()
```

## Source

Found in: `transformers/text/clean_addresses/pyspark/pyspark_primitives.py`

---
[← Back to Address Primitives](/primitives/addresses)
