---
title: extract_email
primitive: Email Primitives
category: pyspark
---

# `extract_email()`

Extract first valid email address from text.

Args:
    col: Column containing text with potential email addresses

Returns:
    Column with extracted email address or empty string

## Parameters

col

## Usage

```python
from datacompose import clean_emails

@clean_emails.compose()
def pipeline(df):
    return df.extract_email("column_name")
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
    return df.extract_email("input_column")

result_df = transform_pipeline(df)
result_df.show()
```

## Source

Found in: `transformers/text/clean_emails/pyspark/pyspark_primitives.py`

---
[← Back to Email Primitives](/primitives/emails)
