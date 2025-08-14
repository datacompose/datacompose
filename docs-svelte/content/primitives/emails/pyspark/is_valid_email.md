---
title: is_valid_email
primitive: Email Primitives
category: pyspark
---

# `is_valid_email()`

Check if email address has valid format.

Args:
    col: Column containing email address
    min_length: Minimum length for valid email
    max_length: Maximum length for valid email

Returns:
    Column with boolean indicating validity

## Parameters

col, min_length, max_length

## Usage

```python
from datacompose import clean_emails

@clean_emails.compose()
def pipeline(df):
    return df.is_valid_email("column_name")
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
    return df.is_valid_email("input_column")

result_df = transform_pipeline(df)
result_df.show()
```

## Source

Found in: `transformers/text/clean_emails/pyspark/pyspark_primitives.py`

---
[← Back to Email Primitives](/primitives/emails)
