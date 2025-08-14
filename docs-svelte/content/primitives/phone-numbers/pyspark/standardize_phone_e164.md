---
title: standardize_phone_e164
primitive: Phone Number Primitives
category: pyspark
---

# `standardize_phone_e164()`

Standardize phone number with cleaning and E.164 formatting.

Args:
    col: Column containing phone number
    
Returns:
    Column with standardized phone number in E.164 format

## Parameters

col

## Usage

```python
from datacompose import clean_phone_numbers

@clean_phone_numbers.compose()
def pipeline(df):
    return df.standardize_phone_e164("column_name")
```

## Example

```python
# Import the library
from datacompose import clean_phone_numbers
from pyspark.sql import SparkSession

# Create Spark session
spark = SparkSession.builder.appName("DataCompose").getOrCreate()

# Create a sample DataFrame
df = spark.createDataFrame([
    ("example_data_1",),
    ("example_data_2",),
], ["input_column"])

# Apply the transformation
@clean_phone_numbers.compose()
def transform_pipeline(df):
    return df.standardize_phone_e164("input_column")

result_df = transform_pipeline(df)
result_df.show()
```

## Source

Found in: `transformers/text/clean_phone_numbers/pyspark/pyspark_primitives.py`

---
[← Back to Phone Number Primitives](/primitives/phone-numbers)
