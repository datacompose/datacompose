---
title: mask_email
primitive: Email Primitives
category: pyspark
---

# `mask_email()`

Mask email address for privacy (e.g., joh***@gm***.com).

Args:
    col: Column containing email address
    mask_char: Character to use for masking
    keep_chars: Number of characters to keep at start

Returns:
    Column with masked email

## Parameters

col, mask_char, keep_chars

## Usage

```python
from datacompose import clean_emails

@clean_emails.compose()
def pipeline(df):
    return df.mask_email("column_name")
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
    return df.mask_email("input_column")

result_df = transform_pipeline(df)
result_df.show()
```

## Source

Found in: `transformers/text/clean_emails/pyspark/pyspark_primitives.py`

---
[← Back to Email Primitives](/primitives/emails)
