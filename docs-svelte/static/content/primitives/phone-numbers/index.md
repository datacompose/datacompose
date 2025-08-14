---
title: Phone Number Primitives
description: Phone number validation, formatting, and analysis
generated: true
---

# Phone Number Primitives

Phone number validation, formatting, and analysis

## Import

```python
from datacompose import clean_phone_numbers
```

## Available Methods

### Pyspark Primitives

#### `extract_phone_from_text()`

Extract first phone number from text using regex patterns.

**Parameters:** col

#### `extract_all_phones_from_text()`

Extract all phone numbers from text as an array.

**Parameters:** col

#### `extract_digits()`

Extract only digits from phone number string.

**Parameters:** col

#### `extract_extension()`

Extract extension from phone number if present.

**Parameters:** col

#### `extract_country_code()`

Extract country code from phone number.

**Parameters:** col

#### `extract_area_code()`

Extract area code from NANP phone number.

**Parameters:** col

#### `extract_exchange()`

Extract exchange (first 3 digits of local number) from NANP phone number.

**Parameters:** col

#### `extract_subscriber()`

Extract subscriber number (last 4 digits) from NANP phone number.

**Parameters:** col

#### `extract_local_number()`

Extract local number (exchange + subscriber) from NANP phone number.

**Parameters:** col

#### `is_valid_nanp()`

Check if phone number is valid NANP format (North American Numbering Plan).

**Parameters:** col

#### `is_valid_international()`

Check if phone number could be valid international format.

**Parameters:** col, min_length, max_length

#### `is_valid_phone()`

Check if phone number is valid (NANP or international).

**Parameters:** col

#### `is_toll_free()`

Check if phone number is toll-free (800, 888, 877, 866, 855, 844, 833).

**Parameters:** col

#### `is_premium_rate()`

Check if phone number is premium rate (900).

**Parameters:** col

#### `has_extension()`

Check if phone number has an extension.

**Parameters:** col

#### `remove_non_digits()`

Remove all non-digit characters from phone number.

**Parameters:** col

#### `remove_extension()`

Remove extension from phone number.

**Parameters:** col

#### `convert_letters_to_numbers()`

Convert phone letters to numbers (e.g., 1-800-FLOWERS to 1-800-3569377).

**Parameters:** col

#### `normalize_separators()`

Normalize various separator styles to hyphens.

**Parameters:** col

#### `add_country_code()`

Add country code "1" if not present (for NANP numbers).

**Parameters:** col

#### `format_nanp()`

Format NANP phone number in standard hyphen format (XXX-XXX-XXXX).

**Parameters:** col

#### `format_nanp_paren()`

Format NANP phone number with parentheses ((XXX) XXX-XXXX).

**Parameters:** col

#### `format_nanp_dot()`

Format NANP phone number with dots (XXX.XXX.XXXX).

**Parameters:** col

#### `format_nanp_space()`

Format NANP phone number with spaces (XXX XXX XXXX).

**Parameters:** col

#### `format_international()`

Format international phone number with country code.

**Parameters:** col

#### `format_e164()`

Format phone number in E.164 format (+CCAAANNNNNNN) with default country code 1.

**Parameters:** col

#### `standardize_phone()`

Standardize phone number with cleaning and NANP formatting.

**Parameters:** col

#### `standardize_phone_e164()`

Standardize phone number with cleaning and E.164 formatting.

**Parameters:** col

#### `standardize_phone_digits()`

Standardize phone number and return digits only.

**Parameters:** col

#### `clean_phone()`

Clean and validate phone number, returning null for invalid numbers.

**Parameters:** col

#### `get_phone_type()`

Get phone number type (toll-free, premium, standard, international).

**Parameters:** col

#### `get_region_from_area_code()`

Get geographic region from area code (simplified - would need lookup table).

**Parameters:** col

#### `mask_phone()`

Mask phone number for privacy keeping last 4 digits (e.g., ***-***-1234).

**Parameters:** col

#### `filter_valid_phones()`

Return phone number only if valid, otherwise return null.

**Parameters:** col

#### `filter_nanp_phones()`

Return phone number only if valid NANP, otherwise return null.

**Parameters:** col

#### `filter_toll_free_phones()`

Return phone number only if toll-free, otherwise return null.

**Parameters:** col



## Example Pipeline

```python
from datacompose import clean_phone_numbers

@clean_phone_numbers.compose()
def process_pipeline(df):
    return (df
        .extract_phone_from_text(column_name)
        .extract_all_phones_from_text(column_name)
        .extract_digits(column_name)
    )

# Apply to your DataFrame
result_df = process_pipeline(spark_df)
```

## Using the @compose Decorator

The `@compose` decorator allows you to chain multiple operations efficiently:

```python
@clean_phone_numbers.compose()
def my_pipeline(df):
    return df.extract_phone_from_text("column_name")
```

## Custom Primitives

You can extend this library with custom primitives:

```python
from pyspark.sql import functions as F
from datacompose import clean_phone_numbers

@clean_phone_numbers.register_primitive
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

- [PySpark Methods →](./phone-numbers/pyspark/) - All 36 available PySpark methods

