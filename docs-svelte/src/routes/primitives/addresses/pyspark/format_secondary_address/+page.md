---
title: format_secondary_address
primitive: Address Primitives
category: pyspark
---

# `format_secondary_address()`

Format unit type and number into standard secondary address.

Note: This is a helper function, not registered with addresses primitive.
Use it directly with two columns.

Args:
    unit_type: Column containing unit type (Apt, Suite, etc.)
    unit_number: Column containing unit number (5B, 200, etc.)

Returns:
    Column with formatted secondary address

Example:
    from datacompose.transformers.text.clean_addresses.pyspark.pyspark_udf import format_secondary_address
    df.select(format_secondary_address(F.lit("Apartment"), F.lit("5B")))
    # -> "Apt 5B"

## Parameters

unit_type, unit_number

## Usage

```python
from datacompose import clean_addresses

@clean_addresses.compose()
def pipeline(df):
    return df.format_secondary_address("column_name")
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
    return df.format_secondary_address("input_column")

result_df = transform_pipeline(df)
result_df.show()
```

## Source

Found in: `transformers/text/clean_addresses/pyspark/pyspark_primitives.py`

---
[← Back to Address Primitives](/primitives/addresses)
