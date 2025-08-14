---
title: validate_city
primitive: Address Primitives
category: pyspark
---

# `validate_city()`

Validate if a city name appears valid.

Validates:
- Not empty/null
- Within reasonable length bounds
- Contains valid characters (letters, spaces, hyphens, apostrophes, periods)
- Optionally: matches a list of known cities

Args:
    col: Column containing city names to validate
    known_cities: Optional list of valid city names to check against
    min_length: Minimum valid city name length (default 2)
    max_length: Maximum valid city name length (default 50)

Returns:
    Boolean column indicating if city name is valid

Example:
    # Basic validation
    df.select(addresses.validate_city(F.col("city")))

    # Validate against known cities
    us_cities = ["New York", "Los Angeles", "Chicago", ...]
    df.select(addresses.validate_city(F.col("city"), known_cities=us_cities))

## Parameters

col, known_cities, min_length, max_length

## Usage

```python
from datacompose import clean_addresses

@clean_addresses.compose()
def pipeline(df):
    return df.validate_city("column_name")
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
    return df.validate_city("input_column")

result_df = transform_pipeline(df)
result_df.show()
```

## Source

Found in: `transformers/text/clean_addresses/pyspark/pyspark_primitives.py`

---
[← Back to Address Primitives](/primitives/addresses)
