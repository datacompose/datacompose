---
title: extract_unit_type
primitive: Address Primitives
category: pyspark
---

# `extract_unit_type()`

Extract the type of unit (Apt, Suite, Unit, etc.) from address.

Args:
    col: Column containing address text

Returns:
    Column with unit type or empty string

Example:
    df.select(addresses.extract_unit_type(F.col("address")))
    # "123 Main St Apt 5B" -> "Apt"
    # "456 Oak Ave Suite 200" -> "Suite"
    # "789 Elm St Unit 12" -> "Unit"

## Parameters

col

## Usage

```python
from datacompose import clean_addresses

@clean_addresses.compose()
def pipeline(df):
    return df.extract_unit_type("column_name")
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
    return df.extract_unit_type("input_column")

result_df = transform_pipeline(df)
result_df.show()
```

## Source

Found in: `transformers/text/clean_addresses/pyspark/pyspark_primitives.py`

---
[← Back to Address Primitives](/primitives/addresses)
