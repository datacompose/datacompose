---
title: convert_letters_to_numbers
primitive: Phone Number Primitives
category: pyspark
---

# `convert_letters_to_numbers()`

Convert phone letters to numbers (e.g., 1-800-FLOWERS to 1-800-3569377).

Args:
    col: Column containing phone number with letters
    
Returns:
    Column with letters converted to numbers

## Parameters

col

## Usage

```python
from datacompose import clean_phone_numbers

@clean_phone_numbers.compose()
def pipeline(df):
    return df.convert_letters_to_numbers("column_name")
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
    return df.convert_letters_to_numbers("input_column")

result_df = transform_pipeline(df)
result_df.show()
```

## Source

Found in: `transformers/text/clean_phone_numbers/pyspark/pyspark_primitives.py`

---
[← Back to Phone Number Primitives](/primitives/phone-numbers)
