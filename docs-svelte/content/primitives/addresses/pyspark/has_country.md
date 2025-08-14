---
title: has_country
primitive: Address Primitives
category: pyspark
---

# `has_country()`

Check if address contains country information.

Args:
    col: Column containing address text

Returns:
    Column with boolean indicating presence of country

Example:
    df.select(addresses.has_country(F.col("address")))
    # "123 Main St, USA" -> True
    # "456 Oak Ave" -> False

## Parameters

col

## Usage

```python
from datacompose import clean_addresses

@clean_addresses.compose()
def pipeline(df):
    return df.has_country("column_name")
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
    return df.has_country("input_column")

result_df = transform_pipeline(df)
result_df.show()
```

## Source

Found in: `transformers/text/clean_addresses/pyspark/pyspark_primitives.py`

---
[← Back to Address Primitives](/primitives/addresses)
