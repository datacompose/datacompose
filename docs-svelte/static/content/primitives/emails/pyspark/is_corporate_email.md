---
title: is_corporate_email
primitive: Email Primitives
category: pyspark
---

# `is_corporate_email()`

Check if email appears to be from a corporate domain (not free email provider).

Args:
    col: Column containing email address
    free_providers: List of free email provider domains to check against

Returns:
    Column with boolean indicating if email is corporate

Examples:
    # Use default free provider list
    df.withColumn("is_corp", emails.is_corporate_email(F.col("email")))

    # Add custom free providers to check
    custom_free = ["company-internal.com", "contractor-email.com"]
    df.withColumn("is_corp", emails.is_corporate_email(F.col("email"), custom_free))

## Parameters

col, free_providers

## Usage

```python
from datacompose import clean_emails

@clean_emails.compose()
def pipeline(df):
    return df.is_corporate_email("column_name")
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
    return df.is_corporate_email("input_column")

result_df = transform_pipeline(df)
result_df.show()
```

## Source

Found in: `transformers/text/clean_emails/pyspark/pyspark_primitives.py`

---
[← Back to Email Primitives](/primitives/emails)
