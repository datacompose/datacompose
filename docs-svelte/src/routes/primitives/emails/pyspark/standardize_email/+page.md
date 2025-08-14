---
title: standardize_email
primitive: Email Primitives
category: pyspark
---

# `standardize_email()`

Apply standard email cleaning and normalization.

Args:
    col: Column containing email address
    lowercase: Convert to lowercase
    remove_dots_gmail: Remove dots from Gmail addresses
    remove_plus: Remove plus addressing
    fix_typos: Fix common domain typos

Returns:
    Column with standardized email

## Parameters

col, lowercase, remove_dots_gmail, remove_plus, fix_typos

## Usage

```python
from datacompose import clean_emails

@clean_emails.compose()
def pipeline(df):
    return df.standardize_email("column_name")
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
    return df.standardize_email("input_column")

result_df = transform_pipeline(df)
result_df.show()
```

## Source

Found in: `transformers/text/clean_emails/pyspark/pyspark_primitives.py`

---
[← Back to Email Primitives](/primitives/emails)
