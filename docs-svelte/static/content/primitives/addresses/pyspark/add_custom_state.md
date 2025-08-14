---
title: add_custom_state
primitive: Address Primitives
category: pyspark
---

# `add_custom_state()`

Add a custom state or region to the state mappings.

This allows extending the address parser to handle non-US states/provinces.

Args:
    full_name: Full name of the state/province (e.g., "ONTARIO")
    abbreviation: Two-letter abbreviation (e.g., "ON")

Example:
    # Add Canadian provinces
    add_custom_state("ONTARIO", "ON")
    add_custom_state("QUEBEC", "QC")
    add_custom_state("BRITISH COLUMBIA", "BC")

## Parameters

full_name, abbreviation

## Usage

```python
from datacompose import clean_addresses

@clean_addresses.compose()
def pipeline(df):
    return df.add_custom_state("column_name")
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
    return df.add_custom_state("input_column")

result_df = transform_pipeline(df)
result_df.show()
```

## Source

Found in: `transformers/text/clean_addresses/pyspark/pyspark_primitives.py`

---
[← Back to Address Primitives](/primitives/addresses)
