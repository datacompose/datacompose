---
title: extract_subscriber
primitive: Phone Number Primitives
category: pyspark
---

# `extract_subscriber()`

Extract subscriber number (last 4 digits) from NANP phone number.

Args:
    col: Column containing phone number
    
Returns:
    Column with subscriber number or empty string

## Parameters

col

## Usage

```python
from datacompose import clean_phone_numbers

@clean_phone_numbers.compose()
def pipeline(df):
    return df.extract_subscriber("column_name")
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
    return df.extract_subscriber("input_column")

result_df = transform_pipeline(df)
result_df.show()
```

## Source

Found in: `transformers/text/clean_phone_numbers/pyspark/pyspark_primitives.py`

---
[← Back to Phone Number Primitives](/primitives/phone-numbers)
