---
title: extract_name_from_email
primitive: Email Primitives
category: pyspark
---

# `extract_name_from_email()`

Attempt to extract person's name from email username.
E.g., john.smith@example.com -> "John Smith"

Args:
    col: Column containing email address

Returns:
    Column with extracted name or empty string

## Parameters

col

## Usage

```python
from datacompose import clean_emails

@clean_emails.compose()
def pipeline(df):
    return df.extract_name_from_email("column_name")
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
    return df.extract_name_from_email("input_column")

result_df = transform_pipeline(df)
result_df.show()
```

## Source

Found in: `transformers/text/clean_emails/pyspark/pyspark_primitives.py`

---
[← Back to Email Primitives](/primitives/emails)
