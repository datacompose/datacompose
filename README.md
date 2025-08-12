# Datacompose

A powerful data transformation framework for building reusable, composable data cleaning pipelines in PySpark.

## Overview

Datacompose provides a declarative way to build data transformation pipelines using composable primitives. It generates optimized, standalone PySpark code that can be deployed without runtime dependencies.

## Key Features

- **Composable Primitives**: Build complex transformations from simple, reusable functions
- **Smart Partial Application**: Configure transformations with parameters for reuse
- **Pipeline Compilation**: Convert declarative pipeline definitions into optimized Spark operations
- **Code Generation**: Generate standalone PySpark code with embedded dependencies
- **Comprehensive Libraries**: Pre-built primitives for emails, addresses, and phone numbers
- **Conditional Logic**: Support for if/else branching in pipelines
- **Type-Safe Operations**: All transformations maintain Spark column type safety

## Installation

```bash
pip install datacompose
```

## Quick Start

### 1. Initialize a Project

```bash
datacompose init
```

This creates a `datacompose.json` configuration file with default settings.

### 2. Generate Transformation Code

```bash
# Generate email cleaning primitives
datacompose add clean_emails --target pyspark

# Generate address standardization primitives  
datacompose add clean_addresses --target pyspark

# Generate phone number validation primitives
datacompose add clean_phone_numbers --target pyspark
```

### 3. Use the Generated Code

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Import the generated primitives
from build.pyspark.clean_emails.email_primitives import emails

# Create Spark session
spark = SparkSession.builder.appName("DataCleaning").getOrCreate()

# Load your data
df = spark.read.csv("data.csv", header=True)

# Apply email transformations
cleaned_df = df.withColumn(
    "email_clean",
    emails.standardize_email(F.col("email"))
).withColumn(
    "email_domain",
    emails.extract_domain(F.col("email_clean"))
).withColumn(
    "is_valid",
    emails.is_valid_email(F.col("email_clean"))
)

# Filter to valid emails only
valid_emails = cleaned_df.filter(F.col("is_valid"))
```

## Core Concepts

### PrimitiveRegistry

A container for organizing related transformation functions:

```python
from datacompose.operators.primitives import PrimitiveRegistry

# Create a registry for text operations
text = PrimitiveRegistry("text")

# Register transformation functions
@text.register()
def lowercase(col):
    return F.lower(col)

@text.register()
def remove_spaces(col):
    return F.regexp_replace(col, r'\s+', '')

# Use the transformations
df = df.withColumn("clean_text", text.lowercase(F.col("input")))
```

### SmartPrimitive

Enables partial application of transformations:

```python
@text.register()
def trim(col, chars=' '):
    return F.trim(col, chars)

# Direct usage
df = df.withColumn("trimmed", text.trim(F.col("input")))

# Pre-configured usage
trim_tabs = text.trim(chars='\t')
df = df.withColumn("no_tabs", trim_tabs(F.col("input")))
```

### Pipeline Composition

Build complex pipelines from simple primitives:

```python
@text.compose(text=text)
def clean_pipeline():
    text.trim()
    text.lowercase()
    text.remove_spaces()

# Apply the entire pipeline
df = df.withColumn("cleaned", clean_pipeline(F.col("input")))
```

### Conditional Pipelines

Add conditional logic to your transformations:

```python
@text.register(is_conditional=True)
def is_valid_length(col):
    return F.length(col) > 5

@text.register()
def truncate(col):
    return F.substring(col, 1, 5)

@text.compose(text=text)
def smart_truncate():
    if text.is_valid_length():
        text.truncate()
```

## Available Primitives

### Email Primitives

```python
from build.pyspark.clean_emails.email_primitives import emails

# Validation
emails.is_valid_email(col)
emails.is_business_email(col)
emails.is_disposable_email(col)

# Extraction
emails.extract_domain(col)
emails.extract_username(col)
emails.extract_tld(col)

# Standardization
emails.standardize_email(col)
emails.normalize_gmail(col)
emails.fix_common_typos(col)

# Filtering
emails.filter_valid_emails(col)
emails.filter_business_emails(col)
```

### Address Primitives

```python
from build.pyspark.clean_addresses.address_primitives import addresses

# Extraction
addresses.extract_street_number(col)
addresses.extract_street_name(col)
addresses.extract_city(col)
addresses.extract_state(col)
addresses.extract_zip_code(col)

# Standardization
addresses.standardize_state(col)
addresses.standardize_street_suffix(col)
addresses.standardize_direction(col)

# Validation
addresses.is_valid_zip_code(col)
addresses.is_valid_state(col)
addresses.is_po_box(col)
```

### Phone Number Primitives

```python
from build.pyspark.clean_phone_numbers.phone_primitives import phones

# Validation
phones.is_valid_nanp(col)
phones.is_valid_international(col)
phones.is_toll_free(col)

# Extraction
phones.extract_country_code(col)
phones.extract_area_code(col)
phones.extract_exchange(col)
phones.extract_subscriber(col)

# Formatting
phones.format_nanp(col)
phones.format_e164(col)
phones.format_international(col)

# Standardization
phones.standardize_phone(col)
phones.clean_phone(col)
```

## Advanced Usage

### Creating Custom Primitives

```python
from datacompose.operators.primitives import PrimitiveRegistry

# Create your own registry
custom = PrimitiveRegistry("custom")

@custom.register()
def remove_special_chars(col):
    return F.regexp_replace(col, r'[^a-zA-Z0-9\s]', '')

@custom.register()
def capitalize_words(col):
    return F.initcap(col)

@custom.register(is_conditional=True)
def contains_numbers(col):
    return col.rlike(r'\d+')

# Create a pipeline with your custom primitives
@custom.compose(custom=custom)
def clean_text():
    custom.remove_special_chars()
    if custom.contains_numbers():
        custom.capitalize_words()
```

### Working with Parameters

```python
@custom.register()
def pad_string(col, length=10, fill_char='0'):
    return F.lpad(col, length, fill_char)

# Use with different parameters
df = df.withColumn("padded_10", custom.pad_string(F.col("id")))
df = df.withColumn("padded_5", custom.pad_string(length=5)(F.col("id")))
df = df.withColumn("padded_x", custom.pad_string(length=8, fill_char='X')(F.col("id")))
```

### Combining Multiple Registries

```python
from build.pyspark.clean_emails.email_primitives import emails
from build.pyspark.clean_phones.phone_primitives import phones

# Create a combined validation pipeline
validation = PrimitiveRegistry("validation")

@validation.compose(emails=emails, phones=phones)
def validate_contact_info():
    # Check email
    if emails.is_valid_email():
        emails.standardize_email()
    
    # Check phone
    if phones.is_valid_phone():
        phones.standardize_phone()
```

## CLI Commands

### Initialize a Project
```bash
datacompose init [--yes]
```

### Add Transformers
```bash
datacompose add <transformer> [--target TARGET] [--output OUTPUT] [--verbose]

# Examples
datacompose add clean_emails --target pyspark
datacompose add clean_addresses --target pyspark --output ./custom/path
datacompose add clean_phone_numbers --target pyspark --verbose
```

### List Available Transformers
```bash
datacompose list transformers
datacompose list generators
```

## Project Structure

After running `datacompose add`, your project will have the following structure:

```
project/
├── datacompose.json                    # Configuration file
├── build/
│   └── pyspark/
│       ├── clean_emails/
│       │   ├── email_primitives.py     # Generated email primitives
│       │   └── utils/
│       │       └── primitives.py       # Core framework (embedded)
│       ├── clean_addresses/
│       │   ├── address_primitives.py
│       │   └── utils/
│       │       └── primitives.py
│       └── clean_phone_numbers/
│           ├── phone_primitives.py
│           └── utils/
│               └── primitives.py
```

## Configuration

The `datacompose.json` file configures default settings:

```json
{
  "version": "1.0.0",
  "targets": {
    "pyspark": {
      "output": "./build/pyspark",
      "generator": "SparkPandasUDFGenerator"
    }
  },
  "templates": {
    "directory": "src/transformers/templates"
  }
}
```



## Performance Considerations

- Primitives are designed to be efficient Spark operations
- Pipelines are compiled to minimize intermediate columns
- Conditional logic uses Spark's `when/otherwise` for vectorized operations
- Generated code has no runtime dependencies beyond PySpark

## Philosophy & Inspiration

Datacompose is inspired by [shadcn-svelte](https://www.shadcn-svelte.com/) and [huntabyte](https://github.com/huntabyte)'s approach to component libraries. Just as shadcn-svelte provides "copy and paste" components rather than npm packages, Datacompose generates data transformation code that becomes part of YOUR codebase.

**Why we believe in this approach:**

- **You Own Your Code**: No external dependencies to manage or worry about breaking changes
- **Full Transparency**: Every transformation is readable, debuggable PySpark code you can understand
- **Customization First**: Need to adjust transformation? Just edit the code
- **Learn by Reading**: The generated code serves as documentation and learning material

This is NOT a traditional library - it's a code generator that gives you production-ready data transformation primitives that you can modify to fit your exact needs.


## License

MIT License - see LICENSE file for details