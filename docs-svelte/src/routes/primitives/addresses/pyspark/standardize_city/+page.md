---
title: standardize_city
primitive: Address Primitives
category: pyspark
---

# `standardize_city()`

Standardize city name formatting.

- Trims whitespace
- Normalizes internal spacing
- Applies title case (with special handling for common patterns)
- Optionally applies custom city name mappings

Args:
    col: Column containing city names to standardize
    custom_mappings: Optional dict for city name corrections/standardization
                    e.g., {"ST LOUIS": "St. Louis", "NEWYORK": "New York"}

Returns:
    Column with standardized city names

Example:
    # Basic standardization
    df.select(addresses.standardize_city(F.col("city")))

    # With custom mappings for common variations
    city_mappings = {
        "NYC": "New York",
        "LA": "Los Angeles",
        "SF": "San Francisco",
        "STLOUIS": "St. Louis"
    }
    df.select(addresses.standardize_city(F.col("city"), custom_mappings=city_mappings))

## Parameters

col, custom_mappings

## Usage

```python
from datacompose import clean_addresses

@clean_addresses.compose()
def pipeline(df):
    return df.standardize_city("column_name")
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
    return df.standardize_city("input_column")

result_df = transform_pipeline(df)
result_df.show()
```

## Source

Found in: `transformers/text/clean_addresses/pyspark/pyspark_primitives.py`

---
[← Back to Address Primitives](/primitives/addresses)
