---
title: extract_secondary_address
primitive: Address Primitives
category: pyspark
---

# `extract_secondary_address()`

Extract complete secondary address information (unit type + number).

Combines unit type and number into standard format:
"Apt 5B", "Ste 200", "Unit 12", etc.

Args:
    col: Column containing address text

Returns:
    Column with complete secondary address or empty string

Example:
    df.select(addresses.extract_secondary_address(F.col("address")))
    # "123 Main St Apt 5B" -> "Apt 5B"
    # "456 Oak Ave, Suite 200" -> "Suite 200"
    # "789 Elm St" -> ""

## Parameters

col

## Usage

```python
from datacompose import clean_addresses

@clean_addresses.compose()
def pipeline(df):
    return df.extract_secondary_address("column_name")
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
    return df.extract_secondary_address("input_column")

result_df = transform_pipeline(df)
result_df.show()
```

## Source

Found in: `transformers/text/clean_addresses/pyspark/pyspark_primitives.py`

---
[← Back to Address Primitives](/primitives/addresses)
