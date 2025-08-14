---
title: extract_domain
primitive: Email Primitives
category: pyspark
---

# `extract_domain()`

Extract domain from email address.

Args:
    col: Column containing email address

Returns:
    Column with domain part or empty string

## Parameters

col

## Usage

```python
from datacompose import clean_emails

@clean_emails.compose()
def pipeline(df):
    return df.extract_domain("column_name")
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
    return df.extract_domain("input_column")

result_df = transform_pipeline(df)
result_df.show()
```

## Source

Found in: `transformers/text/clean_emails/pyspark/pyspark_primitives.py`

---
[← Back to Email Primitives](/primitives/emails)
