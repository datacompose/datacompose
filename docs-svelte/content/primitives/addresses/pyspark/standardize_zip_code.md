---
title: standardize_zip_code
primitive: Address Primitives
category: pyspark
---

# `standardize_zip_code()`

Standardize ZIP code format.

- Removes extra spaces
- Ensures proper dash placement for ZIP+4
- Returns empty string for invalid formats

Args:
    col (Column): Column containing ZIP codes to standardize

Returns:
    Column: Standardized ZIP code or empty string if invalid

## Parameters

col

## Usage

```python
from datacompose import clean_addresses

@clean_addresses.compose()
def pipeline(df):
    return df.standardize_zip_code("column_name")
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
    return df.standardize_zip_code("input_column")

result_df = transform_pipeline(df)
result_df.show()
```

## Source

Found in: `transformers/text/clean_addresses/pyspark/pyspark_primitives.py`

---
[← Back to Address Primitives](/primitives/addresses)
