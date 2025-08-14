---
title: remove_secondary_address
primitive: Address Primitives
category: pyspark
---

# `remove_secondary_address()`

Remove apartment/suite/unit information from address.

Removes secondary address components to get clean street address.

Args:
    col: Column containing address text

Returns:
    Column with secondary address removed

Example:
    df.select(addresses.remove_secondary_address(F.col("address")))
    # "123 Main St Apt 5B" -> "123 Main St"
    # "456 Oak Ave, Suite 200" -> "456 Oak Ave"

## Parameters

col

## Usage

```python
from datacompose import clean_addresses

@clean_addresses.compose()
def pipeline(df):
    return df.remove_secondary_address("column_name")
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
    return df.remove_secondary_address("input_column")

result_df = transform_pipeline(df)
result_df.show()
```

## Source

Found in: `transformers/text/clean_addresses/pyspark/pyspark_primitives.py`

---
[← Back to Address Primitives](/primitives/addresses)
