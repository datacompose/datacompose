---
title: extract_city
primitive: Address Primitives
category: pyspark
---

# `extract_city()`

Extract city name from US address text.

Extracts city by finding text before state abbreviation or ZIP code.
Handles various formats including comma-separated and multi-word cities.

Args:
    col: Column containing address text
    custom_cities: Optional list of custom city names to recognize (case-insensitive)

Returns:
    Column with extracted city name or empty string if not found

Example:
    # Direct usage
    df.select(addresses.extract_city(F.col("address")))

    # With custom cities
    df.select(addresses.extract_city(F.col("address"), custom_cities=["Reading", "Mobile"]))

    # Pre-configured
    extract_city_custom = addresses.extract_city(custom_cities=["Reading", "Mobile"])
    df.select(extract_city_custom(F.col("address")))

## Parameters

col, custom_cities

## Usage

```python
from datacompose import clean_addresses

@clean_addresses.compose()
def pipeline(df):
    return df.extract_city("column_name")
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
    return df.extract_city("input_column")

result_df = transform_pipeline(df)
result_df.show()
```

## Source

Found in: `transformers/text/clean_addresses/pyspark/pyspark_primitives.py`

---
[← Back to Address Primitives](/primitives/addresses)
