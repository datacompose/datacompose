---
title: is_valid_username
primitive: Email Primitives
category: pyspark
---

# `is_valid_username()`

Check if email username part is valid.

Args:
    col: Column containing email address
    min_length: Minimum length for valid username (default 1)
    max_length: Maximum length for valid username (default 64 per RFC)

Returns:
    Column with boolean indicating username validity

## Parameters

col, min_length, max_length

## Usage

```python
from datacompose import clean_emails

@clean_emails.compose()
def pipeline(df):
    return df.is_valid_username("column_name")
```

## Example

```python
# Import the library
from datacompose import clean_emails
from pyspark.sql import SparkSession

# Create Spark session
spark = SparkSession.builder.appName("DataCompose").getOrCreate()

# Create a sample DataFrame
df = spark.createDataFrame([
    ("example_data_1",),
    ("example_data_2",),
], ["input_column"])

# Apply the transformation
@clean_emails.compose()
def transform_pipeline(df):
    return df.is_valid_username("input_column")

result_df = transform_pipeline(df)
result_df.show()
```

## Source

Found in: `transformers/text/clean_emails/pyspark/pyspark_primitives.py`

---
[← Back to Email Primitives](/primitives/emails)
