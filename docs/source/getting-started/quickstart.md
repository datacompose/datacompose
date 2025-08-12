# Quickstart

Get started with Datacompose in 5 minutes!

## 1. Initialize Your Project

```bash
datacompose init my-cleaning-project
cd my-cleaning-project
```

## 2. List Available Specs

Datacompose comes with built-in specifications:

```bash
datacompose list specs
```

Output:

```
Available specs:
  - clean_emails: Email address cleaning and validation
  - clean_phone_numbers: Phone number standardization
  - clean_addresses: Address normalization
  - clean_job_titles: Job title standardization
  - clean_datetimes: Date/time parsing and formatting
```

## 3. Generate Your First UDF

Let's generate an email cleaning UDF for Spark:

```bash
datacompose add clean_emails --target spark
```

This creates:

```
build/
└── spark/
    └── email_cleaner_udf.py
```

## 4. Use the Generated UDF

```python
from pyspark.sql import SparkSession
from build.spark.email_cleaner_udf import email_cleaner_udf

# Initialize Spark
spark = SparkSession.builder \
    .appName("EmailCleaning") \
    .getOrCreate()

# Create sample data
data = [
    ("  john@GMIAL.com  ",),  # Typo + whitespace + uppercase
    ("jane@yahooo.com",),      # Typo
    ("bob@gmail.com",),         # Clean
]

df = spark.createDataFrame(data, ["dirty_email"])

# Apply the UDF
cleaned_df = df.withColumn("clean_email", email_cleaner_udf(df.dirty_email))

cleaned_df.show()
# +------------------+----------------+
# |      dirty_email |    clean_email |
# +------------------+----------------+
# |  john@GMIAL.com  |john@gmail.com  |
# |  jane@yahooo.com |jane@yahoo.com  |
# |    bob@gmail.com |bob@gmail.com   |
# +------------------+----------------+
```

## 5. Create a Custom Spec

Create your own cleaning specification:

```yaml
# my_custom_spec.yaml
name: clean_company_names
description: Standardize company names
type: text
flags:
  strip_whitespace: true
  uppercase: true
typo_map:
  "AMAZN": "AMAZON"
  "MSFT": "MICROSOFT"
  "GOOGL": "GOOGLE"
patterns:
  - pattern: "\\s+(INC|LLC|LTD|CORP)\\.?$"
    replacement: ""
```

## Next Steps

- Learn about [creating custom specifications](../user-guide/specifications.md)
- Explore [different target platforms](../user-guide/targets.md)
- See more [examples](../examples/index.md)
