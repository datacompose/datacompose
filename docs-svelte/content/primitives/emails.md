---
title: Email Primitives
description: Comprehensive email cleaning, validation, and transformation functions for PySpark DataFrames
---

# Email Primitives

The email primitives library provides powerful functions for cleaning, validating, and transforming email addresses in PySpark DataFrames at scale.

## Installation

```bash
pip install datacompose-email-primitives
```

## Import

```python
from datacompose import clean_emails
```

## Core Functions

### clean_emails
Remove whitespace and standardize email format.

```python
@clean_emails.compose()
def clean_pipeline(df):
    return df.clean_emails("email_column")
```

### validate_emails
Check if email addresses follow valid format patterns.

```python
@clean_emails.compose()
def validation_pipeline(df):
    return df.validate_emails("email_column")
```

### extract_email_domains
Extract the domain portion from email addresses.

```python
@clean_emails.compose()
def domain_extraction(df):
    return df.extract_email_domains("email", "domain_column")
```

## Pipeline Examples

### Basic Email Cleaning
Clean and validate email addresses in a single pipeline:

```python
from datacompose import clean_emails

@clean_emails.compose()
def clean_email_pipeline(df):
    return (df
        .clean_emails("email")
        .validate_emails("email")
        .remove_invalid_emails("email")
    )

# Apply to your DataFrame
cleaned_df = clean_email_pipeline(spark_df)
```

### Domain Analysis
Extract and analyze email domains:

```python
@clean_emails.compose()
def domain_analysis(df):
    return (df
        .extract_email_domains("email", "domain")
        .get_email_providers("email", "provider")
        .flag_disposable_emails("email", "is_disposable")
    )
```

### Corporate Email Filtering
Filter and validate corporate email addresses:

```python
@clean_emails.compose()
def corporate_emails(df):
    return (df
        .clean_emails("email")
        .filter_corporate_emails("email")
        .extract_company_from_email("email", "company")
        .validate_corporate_domains("email", "is_valid_corp")
    )
```

## Complete Method Reference

| Method | Description |
|--------|-------------|
| `clean_emails()` | Remove whitespace and standardize format |
| `validate_emails()` | Check if email format is valid |
| `extract_email_domains()` | Extract domain from email address |
| `remove_invalid_emails()` | Filter out invalid email addresses |
| `normalize_emails()` | Convert to lowercase and trim |
| `get_email_providers()` | Identify email service provider |
| `flag_disposable_emails()` | Mark temporary email addresses |
| `filter_corporate_emails()` | Keep only business emails |
| `extract_username()` | Get username part of email |
| `validate_mx_records()` | Check domain MX records |
| `dedupe_emails()` | Remove duplicate email addresses |
| `mask_emails()` | Partially obscure email for privacy |

## Custom Primitives

Extend the library with your own email validation logic:

```python
from pyspark.sql import functions as F
from datacompose import clean_emails

@clean_emails.register_primitive
def validate_company_emails(df, email_col, company_domain):
    """Validate emails belong to specific company domain"""
    return df.filter(
        F.col(email_col).endswith(f"@{company_domain}")
    )

# Use in pipeline
@clean_emails.compose()
def company_email_pipeline(df):
    return (df
        .clean_emails("email")
        .validate_company_emails("email", "acme.com")
    )
```

## Advanced Examples

### Email Validation with Custom Rules

```python
@clean_emails.register_primitive
def validate_email_length(df, email_col, max_length=254):
    """Validate email length per RFC standards"""
    return df.filter(F.length(F.col(email_col)) <= max_length)

@clean_emails.register_primitive
def validate_special_chars(df, email_col):
    """Check for allowed special characters"""
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return df.withColumn(
        f"{email_col}_valid_chars",
        F.regexp_extract(F.col(email_col), pattern, 0) != ""
    )
```

### Bulk Email Processing

```python
@clean_emails.compose()
def bulk_email_processing(df):
    return (df
        .clean_emails("email")
        .validate_emails("email")
        .extract_email_domains("email", "domain")
        .withColumn("email_hash", F.sha2(F.col("email"), 256))
        .dedupe_emails("email")
        .cache()  # Cache for performance
    )
```

### Email Provider Analysis

```python
@clean_emails.compose()
def provider_analysis(df):
    return (df
        .get_email_providers("email", "provider")
        .groupBy("provider")
        .agg(
            F.count("*").alias("count"),
            F.countDistinct("email").alias("unique_emails")
        )
        .orderBy(F.desc("count"))
    )
```