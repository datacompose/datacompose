---
title: Email Primitives
description: Email cleaning, validation, and transformation functions
generated: true
---

# Email Primitives

Email cleaning, validation, and transformation functions

## Import

```python
from datacompose import clean_emails
```

## Available Methods

### Pyspark Primitives

#### `extract_email()`

Extract first valid email address from text.

**Parameters:** col

#### `extract_all_emails()`

Extract all email addresses from text as an array.

**Parameters:** col

#### `extract_username()`

Extract username (local part) from email address.

**Parameters:** col

#### `extract_domain()`

Extract domain from email address.

**Parameters:** col

#### `extract_domain_name()`

Extract domain name without TLD from email address.

**Parameters:** col

#### `extract_tld()`

Extract top-level domain from email address.

**Parameters:** col

#### `is_valid_email()`

Check if email address has valid format.

**Parameters:** col, min_length, max_length

#### `is_valid_username()`

Check if email username part is valid.

**Parameters:** col, min_length, max_length

#### `is_valid_domain()`

Check if email domain part is valid.

**Parameters:** col

#### `has_plus_addressing()`

Check if email uses plus addressing (e.g., user+tag@gmail.com).

**Parameters:** col

#### `is_disposable_email()`

Check if email is from a disposable email service.

**Parameters:** col, disposable_domains

#### `is_corporate_email()`

Check if email appears to be from a corporate domain (not free email provider).

**Parameters:** col, free_providers

#### `remove_whitespace()`

Remove all whitespace from email address.

**Parameters:** col

#### `lowercase_email()`

Convert entire email address to lowercase.

**Parameters:** col

#### `lowercase_domain()`

Convert only domain part to lowercase, preserve username case.

**Parameters:** col

#### `remove_plus_addressing()`

Remove plus addressing from email (e.g., user+tag@gmail.com -> user@gmail.com).

**Parameters:** col

#### `remove_dots_from_gmail()`

Remove dots from Gmail addresses (Gmail ignores dots in usernames).

**Parameters:** col

#### `fix_common_typos()`

Fix common domain typos in email addresses.

**Parameters:** col, custom_mappings, custom_tld_mappings

#### `standardize_email()`

Apply standard email cleaning and normalization.

**Parameters:** col, lowercase, remove_dots_gmail, remove_plus, fix_typos

#### `normalize_gmail()`

Normalize Gmail addresses (remove dots, plus addressing, lowercase).

**Parameters:** col

#### `get_canonical_email()`

Get canonical form of email address for deduplication.

**Parameters:** col

#### `extract_name_from_email()`

Attempt to extract person's name from email username.

**Parameters:** col

#### `get_email_provider()`

Get email provider name from domain.

**Parameters:** col

#### `mask_email()`

Mask email address for privacy (e.g., joh***@gm***.com).

**Parameters:** col, mask_char, keep_chars

#### `filter_valid_emails()`

Return email only if valid, otherwise return null.

**Parameters:** col

#### `filter_corporate_emails()`

Return email only if corporate, otherwise return null.

**Parameters:** col

#### `filter_non_disposable_emails()`

Return email only if not disposable, otherwise return null.

**Parameters:** col



## Example Pipeline

```python
from datacompose import clean_emails

@clean_emails.compose()
def process_pipeline(df):
    return (df
        .extract_email(column_name)
        .extract_all_emails(column_name)
        .extract_username(column_name)
    )

# Apply to your DataFrame
result_df = process_pipeline(spark_df)
```

## Using the @compose Decorator

The `@compose` decorator allows you to chain multiple operations efficiently:

```python
@clean_emails.compose()
def my_pipeline(df):
    return df.extract_email("column_name")
```

## Custom Primitives

You can extend this library with custom primitives:

```python
from pyspark.sql import functions as F
from datacompose import clean_emails

@clean_emails.register_primitive
def custom_transformation(df, column_name):
    """Your custom transformation logic"""
    return df.withColumn(
        f"{column_name}_transformed",
        F.col(column_name)  # Your logic here
    )
```

---
*This documentation was automatically generated from the source code.*


## Detailed Method Documentation

For detailed documentation of each method, see:

- [PySpark Methods →](./emails/pyspark/) - All 27 available PySpark methods

