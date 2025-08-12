# Examples

Practical examples and recipes for using Datacompose.

```{toctree}
:maxdepth: 2

email-cleaning
phone-normalization
address-standardization
custom-specs
```

## Quick Examples

### Email Cleaning
```python
from build.spark.email_cleaner import clean_email

# Clean email addresses in a DataFrame
cleaned_df = clean_email(df, "dirty_email", "clean_email")
```

### Phone Number Normalization
```python
from build.spark.phone_cleaner import clean_phone

# Normalize phone numbers
cleaned_df = clean_phone(df, "dirty_phone", "clean_phone")
```

### Address Standardization
```python
from build.spark.address_cleaner import clean_address, validate_address

# Clean and standardize addresses
cleaned_df = clean_address(df, "raw_address", "cleaned_address")

# Validate addresses
validated_df = validate_address(cleaned_df, "cleaned_address")
```

### Chaining Multiple Cleaners
```python
# Clean multiple fields in a pipeline
result_df = (df
    .transform(clean_email, "email_col", "clean_email")
    .transform(clean_phone, "phone_col", "clean_phone")
    .transform(clean_address, "address_col", "clean_address")
)