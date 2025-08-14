---
title: extract_private_mailbox
primitive: Address Primitives
category: pyspark
---

# `extract_private_mailbox()`

Extract private mailbox (PMB) number from address.

Extracts PMB or Private Mail Box numbers, commonly used with
commercial mail receiving agencies (like UPS Store).

Args:
    col: Column containing address text

Returns:
    Column with extracted PMB number or empty string

Example:
    df.select(addresses.extract_private_mailbox(F.col("address")))
    # "123 Main St PMB 456" -> "456"
    # "789 Oak Ave #101 PMB 12" -> "12"

## Parameters

col

## Usage

```python
from datacompose import clean_addresses

@clean_addresses.compose()
def pipeline(df):
    return df.extract_private_mailbox("column_name")
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
    return df.extract_private_mailbox("input_column")

result_df = transform_pipeline(df)
result_df.show()
```

## Source

Found in: `transformers/text/clean_addresses/pyspark/pyspark_primitives.py`

---
[← Back to Address Primitives](/primitives/addresses)
