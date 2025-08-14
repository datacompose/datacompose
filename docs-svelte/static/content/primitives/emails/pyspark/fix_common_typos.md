---
title: fix_common_typos
primitive: Email Primitives
category: pyspark
---

# `fix_common_typos()`

Fix common domain typos in email addresses.

Args:
    col: Column containing email address
    custom_mappings: Additional domain mappings to apply (extends DOMAIN_TYPO_MAPPINGS)
    custom_tld_mappings: Additional TLD mappings to apply (extends TLD_TYPO_MAPPINGS)

Returns:
    Column with typos fixed

Examples:
    # Use default typo fixes
    df.withColumn("fixed", emails.fix_common_typos(F.col("email")))

    # Add custom domain typo mappings
    custom_domains = {
        "company.con": "company.com",
        "mycompany.co": "mycompany.com",
        "gmai.com": "gmail.com"  # Override default mapping
    }
    df.withColumn("fixed", emails.fix_common_typos(F.col("email"), custom_domains))

    # Add custom TLD mappings
    custom_tlds = {
        ".coom": ".com",
        ".nett": ".net"
    }
    df.withColumn("fixed", emails.fix_common_typos(
        F.col("email"),
        custom_tld_mappings=custom_tlds
    ))

## Parameters

col, custom_mappings, custom_tld_mappings

## Usage

```python
from datacompose import clean_emails

@clean_emails.compose()
def pipeline(df):
    return df.fix_common_typos("column_name")
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
    return df.fix_common_typos("input_column")

result_df = transform_pipeline(df)
result_df.show()
```

## Source

Found in: `transformers/text/clean_emails/pyspark/pyspark_primitives.py`

---
[← Back to Email Primitives](/primitives/emails)
