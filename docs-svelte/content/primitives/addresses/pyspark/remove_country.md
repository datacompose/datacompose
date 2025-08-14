---
title: remove_country
primitive: Address Primitives
category: pyspark
---

# `remove_country()`

Remove country from address.

Removes country information from the end of addresses.

Args:
    col: Column containing address text

Returns:
    Column with country removed

Example:
    df.select(addresses.remove_country(F.col("address")))
    # "123 Main St, New York, USA" -> "123 Main St, New York"
    # "456 Oak Ave, Toronto, Canada" -> "456 Oak Ave, Toronto"

## Parameters

col

## Usage

```python
from datacompose import clean_addresses

@clean_addresses.compose()
def pipeline(df):
    return df.remove_country("column_name")
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
    return df.remove_country("input_column")

result_df = transform_pipeline(df)
result_df.show()
```

## Source

Found in: `transformers/text/clean_addresses/pyspark/pyspark_primitives.py`

---
[← Back to Address Primitives](/primitives/addresses)
