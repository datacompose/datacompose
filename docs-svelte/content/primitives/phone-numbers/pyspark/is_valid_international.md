---
title: is_valid_international
primitive: Phone Number Primitives
category: pyspark
---

# `is_valid_international()`

Check if phone number could be valid international format.

Args:
    col: Column containing phone number
    min_length: Minimum digits for international number
    max_length: Maximum digits for international number
    
Returns:
    Column with boolean indicating potential international validity

## Parameters

col, min_length, max_length

## Usage

```python
from datacompose import clean_phone_numbers

@clean_phone_numbers.compose()
def pipeline(df):
    return df.is_valid_international("column_name")
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
    return df.is_valid_international("input_column")

result_df = transform_pipeline(df)
result_df.show()
```

## Source

Found in: `transformers/text/clean_phone_numbers/pyspark/pyspark_primitives.py`

---
[← Back to Phone Number Primitives](/primitives/phone-numbers)
