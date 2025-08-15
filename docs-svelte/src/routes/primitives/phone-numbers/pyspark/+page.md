---
title: PYSPARK Implementation
description: Phone Numbers transformers for PYSPARK
---

# PYSPARK Implementation

## Installation

```bash
pip install datacompose[pyspark]
```

## Usage

```python
from datacompose.transformers.text.phones import phones

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('DataCompose').getOrCreate()
df = spark.read.parquet('data.parquet')

# Using the @phones.compose() decorator
@phones.compose()
def clean_data(df):
    return df

cleaned_df = clean_data(df)
```

## Available Methods

Total methods: **36**

#### `add_country_code(col: Column)`

Add country code "1" if not present (for NANP numbers).

Args:
    col: Column containing phone number
    
Returns:
    Column with country code added if needed

```python
df = df.add_country_code(col)
```

#### `clean_phone(col: Column)`

Clean and validate phone number, returning null for invalid numbers.

Args:
    col: Column containing phone number
    
Returns:
    Column with cleaned phone number or null

```python
df = df.clean_phone(col)
```

#### `convert_letters_to_numbers(col: Column)`

Convert phone letters to numbers (e.g., 1-800-FLOWERS to 1-800-3569377).

Args:
    col: Column containing phone number with letters
    
Returns:
    Column with letters converted to numbers

```python
df = df.convert_letters_to_numbers(col)
```

#### `extract_all_phones_from_text(col: Column)`

Extract all phone numbers from text as an array.

Args:
    col: Column containing text with potential phone numbers
    
Returns:
    Column with array of phone numbers

```python
df = df.extract_all_phones_from_text(col)
```

#### `extract_area_code(col: Column)`

Extract area code from NANP phone number.

Args:
    col: Column containing phone number
    
Returns:
    Column with area code or empty string

```python
df = df.extract_area_code(col)
```

#### `extract_country_code(col: Column)`

Extract country code from phone number.

Args:
    col: Column containing phone number
    
Returns:
    Column with country code or empty string

```python
df = df.extract_country_code(col)
```

#### `extract_digits(col: Column)`

Extract only digits from phone number string.

Args:
    col: Column containing phone number
    
Returns:
    Column with only digits

```python
df = df.extract_digits(col)
```

#### `extract_exchange(col: Column)`

Extract exchange (first 3 digits of local number) from NANP phone number.

Args:
    col: Column containing phone number
    
Returns:
    Column with exchange or empty string

```python
df = df.extract_exchange(col)
```

#### `extract_extension(col: Column)`

Extract extension from phone number if present.

Args:
    col: Column containing phone number
    
Returns:
    Column with extension or empty string

```python
df = df.extract_extension(col)
```

#### `extract_local_number(col: Column)`

Extract local number (exchange + subscriber) from NANP phone number.

Args:
    col: Column containing phone number
    
Returns:
    Column with 7-digit local number or empty string

```python
df = df.extract_local_number(col)
```

#### `extract_phone_from_text(col: Column)`

Extract first phone number from text using regex patterns.

Args:
    col: Column containing text with potential phone numbers
    
Returns:
    Column with extracted phone number or empty string

```python
df = df.extract_phone_from_text(col)
```

#### `extract_subscriber(col: Column)`

Extract subscriber number (last 4 digits) from NANP phone number.

Args:
    col: Column containing phone number
    
Returns:
    Column with subscriber number or empty string

```python
df = df.extract_subscriber(col)
```

#### `filter_nanp_phones(col: Column)`

Return phone number only if valid NANP, otherwise return null.

Args:
    col: Column containing phone number
    
Returns:
    Column with NANP phone or null

```python
df = df.filter_nanp_phones(col)
```

#### `filter_toll_free_phones(col: Column)`

Return phone number only if toll-free, otherwise return null.

Args:
    col: Column containing phone number
    
Returns:
    Column with toll-free phone or null

```python
df = df.filter_toll_free_phones(col)
```

#### `filter_valid_phones(col: Column)`

Return phone number only if valid, otherwise return null.

Args:
    col: Column containing phone number
    
Returns:
    Column with valid phone or null

```python
df = df.filter_valid_phones(col)
```

#### `format_e164(col: Column)`

Format phone number in E.164 format (+CCAAANNNNNNN) with default country code 1.

Args:
    col: Column containing phone number
    
Returns:
    Column with E.164 formatted number

```python
df = df.format_e164(col)
```

#### `format_international(col: Column)`

Format international phone number with country code.

Args:
    col: Column containing phone number
    
Returns:
    Column with formatted international number

```python
df = df.format_international(col)
```

#### `format_nanp(col: Column)`

Format NANP phone number in standard hyphen format (XXX-XXX-XXXX).

Args:
    col: Column containing phone number
    
Returns:
    Column with formatted phone number

```python
df = df.format_nanp(col)
```

#### `format_nanp_dot(col: Column)`

Format NANP phone number with dots (XXX.XXX.XXXX).

Args:
    col: Column containing phone number
    
Returns:
    Column with formatted phone number

```python
df = df.format_nanp_dot(col)
```

#### `format_nanp_paren(col: Column)`

Format NANP phone number with parentheses ((XXX) XXX-XXXX).

Args:
    col: Column containing phone number
    
Returns:
    Column with formatted phone number

```python
df = df.format_nanp_paren(col)
```

#### `format_nanp_space(col: Column)`

Format NANP phone number with spaces (XXX XXX XXXX).

Args:
    col: Column containing phone number
    
Returns:
    Column with formatted phone number

```python
df = df.format_nanp_space(col)
```

#### `get_phone_type(col: Column)`

Get phone number type (toll-free, premium, standard, international).

Args:
    col: Column containing phone number
    
Returns:
    Column with phone type

```python
df = df.get_phone_type(col)
```

#### `get_region_from_area_code(col: Column)`

Get geographic region from area code (simplified - would need lookup table).

Args:
    col: Column containing phone number
    
Returns:
    Column with region or empty string

```python
df = df.get_region_from_area_code(col)
```

#### `has_extension(col: Column)`

Check if phone number has an extension.

Args:
    col: Column containing phone number
    
Returns:
    Column with boolean indicating presence of extension

```python
df = df.has_extension(col)
```

#### `is_premium_rate(col: Column)`

Check if phone number is premium rate (900).

Args:
    col: Column containing phone number
    
Returns:
    Column with boolean indicating if premium rate

```python
df = df.is_premium_rate(col)
```

#### `is_toll_free(col: Column)`

Check if phone number is toll-free (800, 888, 877, 866, 855, 844, 833).

Args:
    col: Column containing phone number
    
Returns:
    Column with boolean indicating if toll-free

```python
df = df.is_toll_free(col)
```

#### `is_valid_international(col: Column, min_length: int = 7, max_length: int = 15)`

Check if phone number could be valid international format.

Args:
    col: Column containing phone number
    min_length: Minimum digits for international number
    max_length: Maximum digits for international number
    
Returns:
    Column with boolean indicating potential international validity

```python
df = df.is_valid_international(col, min_length, max_length)
```

#### `is_valid_nanp(col: Column)`

Check if phone number is valid NANP format (North American Numbering Plan).

Args:
    col: Column containing phone number
    
Returns:
    Column with boolean indicating NANP validity

```python
df = df.is_valid_nanp(col)
```

#### `is_valid_phone(col: Column)`

Check if phone number is valid (NANP or international).

Args:
    col: Column containing phone number
    
Returns:
    Column with boolean indicating validity

```python
df = df.is_valid_phone(col)
```

#### `mask_phone(col: Column)`

Mask phone number for privacy keeping last 4 digits (e.g., ***-***-1234).

Args:
    col: Column containing phone number
    
Returns:
    Column with masked phone number

```python
df = df.mask_phone(col)
```

#### `normalize_separators(col: Column)`

Normalize various separator styles to hyphens.
Removes parentheses and replaces dots, spaces with hyphens.

Args:
    col: Column containing phone number
    
Returns:
    Column with normalized separators

```python
df = df.normalize_separators(col)
```

#### `remove_extension(col: Column)`

Remove extension from phone number.

Args:
    col: Column containing phone number
    
Returns:
    Column with extension removed

```python
df = df.remove_extension(col)
```

#### `remove_non_digits(col: Column)`

Remove all non-digit characters from phone number.

Args:
    col: Column containing phone number
    
Returns:
    Column with only digits

```python
df = df.remove_non_digits(col)
```

#### `standardize_phone(col: Column)`

Standardize phone number with cleaning and NANP formatting.

Args:
    col: Column containing phone number
    
Returns:
    Column with standardized phone number in NANP format

```python
df = df.standardize_phone(col)
```

#### `standardize_phone_digits(col: Column)`

Standardize phone number and return digits only.

Args:
    col: Column containing phone number
    
Returns:
    Column with digits only

```python
df = df.standardize_phone_digits(col)
```

#### `standardize_phone_e164(col: Column)`

Standardize phone number with cleaning and E.164 formatting.

Args:
    col: Column containing phone number
    
Returns:
    Column with standardized phone number in E.164 format

```python
df = df.standardize_phone_e164(col)
```
