# Quick Start

This guide will help you get started with Datacompose in just a few minutes.

## Step 1: Initialize Your Project

First, create a new directory for your project and initialize Datacompose:

```bash
mkdir my-data-project
cd my-data-project
datacompose init
```

This creates a `datacompose.json` configuration file with default settings:

```json
{
  "version": "1.0.0",
  "targets": {
    "pyspark": {
      "output": "./build/pyspark",
      "generator": "SparkPandasUDFGenerator"
    }
  }
}
```

## Step 2: Generate Transformation Primitives

Now, let's generate some useful data transformation primitives. Datacompose comes with pre-built transformations for common data cleaning tasks:

### Email Cleaning

```bash
datacompose add clean_emails --target pyspark
```

This generates email validation and standardization functions in `build/pyspark/clean_emails/`.

### Address Standardization

```bash
datacompose add clean_addresses --target pyspark
```

This generates address parsing and standardization functions in `build/pyspark/clean_addresses/`.

### Phone Number Validation

```bash
datacompose add clean_phone_numbers --target pyspark
```

This generates phone number validation and formatting functions in `build/pyspark/clean_phone_numbers/`.

## Step 3: Use the Generated Code

Let's create a simple PySpark script to clean some customer data:

```python title="clean_customer_data.py"
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Import the generated primitives
from build.pyspark.clean_emails.email_primitives import emails
from build.pyspark.clean_phones.phone_primitives import phones
from build.pyspark.clean_addresses.address_primitives import addresses

# Create Spark session
spark = SparkSession.builder \
    .appName("CustomerDataCleaning") \
    .getOrCreate()

# Sample data
data = [
    ("john.doe@GMAIL.COM", "123-456-7890", "123 Main St, New York, NY 10001"),
    ("jane.smith@company.com", "(555) 987-6543", "456 Oak Ave Suite 200, Los Angeles, CA 90001"),
    ("invalid@email", "not-a-phone", "Bad Address")
]

df = spark.createDataFrame(data, ["email", "phone", "address"])

# Clean emails
df_cleaned = df.withColumn(
    "email_clean",
    emails.standardize_email(F.col("email"))
).withColumn(
    "email_valid",
    emails.is_valid_email(F.col("email_clean"))
).withColumn(
    "email_domain",
    emails.extract_domain(F.col("email_clean"))
)

# Clean phone numbers
df_cleaned = df_cleaned.withColumn(
    "phone_clean",
    phones.standardize_phone(F.col("phone"))
).withColumn(
    "phone_valid",
    phones.is_valid_nanp(F.col("phone_clean"))
).withColumn(
    "phone_formatted",
    phones.format_nanp(F.col("phone_clean"))
)

# Extract address components
df_cleaned = df_cleaned.withColumn(
    "street",
    addresses.extract_street_name(F.col("address"))
).withColumn(
    "city",
    addresses.extract_city(F.col("address"))
).withColumn(
    "state",
    addresses.extract_state(F.col("address"))
).withColumn(
    "zip_code",
    addresses.extract_zip_code(F.col("address"))
)

# Show results
df_cleaned.show(truncate=False)

# Filter to valid records only
valid_records = df_cleaned.filter(
    F.col("email_valid") & F.col("phone_valid")
)

print(f"Valid records: {valid_records.count()}")
```

## Step 4: Run Your Pipeline

Execute your data cleaning pipeline:

```bash
python clean_customer_data.py
```

## What's Next?

Now that you've seen the basics, explore more advanced features:

### Create Custom Pipelines

Build complex transformation pipelines by composing primitives:

```python
from datacompose.operators.primitives import PrimitiveRegistry

# Create a custom registry
custom = PrimitiveRegistry("custom")

@custom.compose(emails=emails, phones=phones)
def clean_contact_info():
    # Clean email
    emails.standardize_email()
    if emails.is_valid_email():
        emails.extract_domain()
    
    # Clean phone
    phones.standardize_phone()
    if phones.is_valid_nanp():
        phones.format_nanp()

# Apply the pipeline
df = df.withColumn("contact_clean", clean_contact_info(F.struct("email", "phone")))
```

### Use Partial Application

Pre-configure transformations with specific parameters:

```python
# Create a trimmer that removes tabs
trim_tabs = text.trim(chars='\t')

# Use it multiple times
df = df.withColumn("no_tabs_1", trim_tabs(F.col("field1")))
df = df.withColumn("no_tabs_2", trim_tabs(F.col("field2")))
```

### Explore More

- Learn about [Core Concepts](../user-guide/core-concepts.md)
- Explore [all available primitives](../api/index.md)
- See [real-world examples](../examples/index.md)
- Create [custom primitives](../user-guide/custom-primitives.md)

## Getting Help

- Check the [User Guide](../user-guide/index.md) for detailed documentation
- Browse [Examples](../examples/index.md) for common use cases
- Report issues on [GitHub](https://github.com/datacompose/datacompose/issues)