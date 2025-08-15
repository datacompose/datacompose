---
title: PYSPARK Implementation
description: Emails transformers for PYSPARK
---

# PYSPARK Implementation

## Installation

```bash
pip install datacompose[pyspark]
```

## Usage

```python
from datacompose.transformers.text.emails import emails

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('DataCompose').getOrCreate()
df = spark.read.parquet('data.parquet')

# Using the @emails.compose() decorator
@emails.compose()
def clean_data(df):
    return df

cleaned_df = clean_data(df)
```

## Available Methods

Total methods: **27**

#### `extract_all_emails(col: Column)`

Extract all email addresses from text as an array.

Args:
    col: Column containing text with potential email addresses

Returns:
    Column with array of email addresses

```python
df = df.extract_all_emails(col)
```

#### `extract_domain(col: Column)`

Extract domain from email address.

Args:
    col: Column containing email address

Returns:
    Column with domain part or empty string

```python
df = df.extract_domain(col)
```

#### `extract_domain_name(col: Column)`

Extract domain name without TLD from email address.

Args:
    col: Column containing email address

Returns:
    Column with domain name (e.g., "gmail" from "user@gmail.com")

```python
df = df.extract_domain_name(col)
```

#### `extract_email(col: Column)`

Extract first valid email address from text.

Args:
    col: Column containing text with potential email addresses

Returns:
    Column with extracted email address or empty string

```python
df = df.extract_email(col)
```

#### `extract_name_from_email(col: Column)`

Attempt to extract person's name from email username.
E.g., john.smith@example.com -> "John Smith"

Args:
    col: Column containing email address

Returns:
    Column with extracted name or empty string

```python
df = df.extract_name_from_email(col)
```

#### `extract_tld(col: Column)`

Extract top-level domain from email address.

Args:
    col: Column containing email address

Returns:
    Column with TLD (e.g., "com", "co.uk")

```python
df = df.extract_tld(col)
```

#### `extract_username(col: Column)`

Extract username (local part) from email address.

Args:
    col: Column containing email address

Returns:
    Column with username part or empty string

```python
df = df.extract_username(col)
```

#### `filter_corporate_emails(col: Column)`

Return email only if corporate, otherwise return null.

Args:
    col: Column containing email address

Returns:
    Column with corporate email or null

```python
df = df.filter_corporate_emails(col)
```

#### `filter_non_disposable_emails(col: Column)`

Return email only if not disposable, otherwise return null.

Args:
    col: Column containing email address

Returns:
    Column with non-disposable email or null

```python
df = df.filter_non_disposable_emails(col)
```

#### `filter_valid_emails(col: Column)`

Return email only if valid, otherwise return null.

Args:
    col: Column containing email address

Returns:
    Column with valid email or null

```python
df = df.filter_valid_emails(col)
```

#### `fix_common_typos(col: Column, custom_mappings: Optional[Dict[str, str]] = None, custom_tld_mappings: Optional[Dict[str, str]] = None)`

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

```python
df = df.fix_common_typos(col, custom_mappings, custom_tld_mappings)
```

#### `get_canonical_email(col: Column)`

Get canonical form of email address for deduplication.
Applies maximum normalization.

Args:
    col: Column containing email address

Returns:
    Column with canonical email form

```python
df = df.get_canonical_email(col)
```

#### `get_email_provider(col: Column)`

Get email provider name from domain.

Args:
    col: Column containing email address

Returns:
    Column with provider name

```python
df = df.get_email_provider(col)
```

#### `has_plus_addressing(col: Column)`

Check if email uses plus addressing (e.g., user+tag@gmail.com).

Args:
    col: Column containing email address

Returns:
    Column with boolean indicating plus addressing usage

```python
df = df.has_plus_addressing(col)
```

#### `is_corporate_email(col: Column, free_providers: Optional[List[str]] = None)`

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

```python
df = df.is_corporate_email(col, free_providers)
```

#### `is_disposable_email(col: Column, disposable_domains: Optional[List[str]] = None)`

Check if email is from a disposable email service.

Args:
    col: Column containing email address
    disposable_domains: List of disposable domains to check against

Returns:
    Column with boolean indicating if email is disposable

```python
df = df.is_disposable_email(col, disposable_domains)
```

#### `is_valid_domain(col: Column)`

Check if email domain part is valid.

Args:
    col: Column containing email address

Returns:
    Column with boolean indicating domain validity

```python
df = df.is_valid_domain(col)
```

#### `is_valid_email(col: Column, min_length: int = 6, max_length: int = 254)`

Check if email address has valid format.

Args:
    col: Column containing email address
    min_length: Minimum length for valid email
    max_length: Maximum length for valid email

Returns:
    Column with boolean indicating validity

```python
df = df.is_valid_email(col, min_length, max_length)
```

#### `is_valid_username(col: Column, min_length: int = 1, max_length: int = 64)`

Check if email username part is valid.

Args:
    col: Column containing email address
    min_length: Minimum length for valid username (default 1)
    max_length: Maximum length for valid username (default 64 per RFC)

Returns:
    Column with boolean indicating username validity

```python
df = df.is_valid_username(col, min_length, max_length)
```

#### `lowercase_domain(col: Column)`

Convert only domain part to lowercase, preserve username case.

Args:
    col: Column containing email address

Returns:
    Column with domain lowercased

```python
df = df.lowercase_domain(col)
```

#### `lowercase_email(col: Column)`

Convert entire email address to lowercase.

Args:
    col: Column containing email address

Returns:
    Column with lowercased email

```python
df = df.lowercase_email(col)
```

#### `mask_email(col: Column, mask_char: str = '*', keep_chars: int = 3)`

Mask email address for privacy (e.g., joh***@gm***.com).

Args:
    col: Column containing email address
    mask_char: Character to use for masking
    keep_chars: Number of characters to keep at start

Returns:
    Column with masked email

```python
df = df.mask_email(col, mask_char, keep_chars)
```

#### `normalize_gmail(col: Column)`

Normalize Gmail addresses (remove dots, plus addressing, lowercase).

Args:
    col: Column containing email address

Returns:
    Column with normalized Gmail address

```python
df = df.normalize_gmail(col)
```

#### `remove_dots_from_gmail(col: Column)`

Remove dots from Gmail addresses (Gmail ignores dots in usernames).

Args:
    col: Column containing email address

Returns:
    Column with dots removed from Gmail usernames

```python
df = df.remove_dots_from_gmail(col)
```

#### `remove_plus_addressing(col: Column)`

Remove plus addressing from email (e.g., user+tag@gmail.com -> user@gmail.com).

Args:
    col: Column containing email address

Returns:
    Column with plus addressing removed

```python
df = df.remove_plus_addressing(col)
```

#### `remove_whitespace(col: Column)`

Remove all whitespace from email address.

Args:
    col: Column containing email address

Returns:
    Column with whitespace removed

```python
df = df.remove_whitespace(col)
```

#### `standardize_email(col: Column, lowercase: bool = True, remove_dots_gmail: bool = True, remove_plus: bool = False, fix_typos: bool = True)`

Apply standard email cleaning and normalization.

Args:
    col: Column containing email address
    lowercase: Convert to lowercase
    remove_dots_gmail: Remove dots from Gmail addresses
    remove_plus: Remove plus addressing
    fix_typos: Fix common domain typos

Returns:
    Column with standardized email

```python
df = df.standardize_email(col, lowercase, remove_dots_gmail, remove_plus, fix_typos)
```
