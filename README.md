# DataCompose

PySpark transformations you can actually own and modify. No black boxes.

## Before vs After

```python
# Before: Regex nightmare for addresses
df = df.withColumn("state_clean",
    F.when(F.col("address").rlike(".*\\b(NY|N\\.Y\\.|New York|NewYork|Newyork)\\b.*"), "NY")
    .when(F.col("address").rlike(".*\\b(CA|Cal\\.|Calif\\.|California)\\b.*"), "CA")
    .when(F.col("address").rlike(".*\\b(IL|Ill\\.|Illinois|Illinios)\\b.*"), "IL")
    .when(F.upper(F.col("address")).contains("NEW YORK"), "NY")
    .when(F.regexp_extract(F.col("address"), ",\\s*([A-Z]{2})\\s+\\d{5}", 1) == "NY", "NY")
    .when(F.regexp_extract(F.col("address"), "\\s+([A-Z]{2})\\s*$", 1) == "NY", "NY")
    # ... handle "N.Y 10001" vs "NY, 10001" vs "New York 10001"
    # ... handle misspellings like "Californai" or "Illnois"  
    # ... 50 more states × 10 variations each
)

# After: One line
from builders.transformers.addresses import addresses
df = df.withColumn("state", addresses.standardize_state(F.col("address")))
```

## Installation

```bash
pip install datacompose
```

## How It Works

```bash
# Copy transformers into YOUR repo
datacompose add phones
datacompose add addresses
datacompose add emails
```

```python
# Use them like any Python module - this is your code now
from transformers.pyspark.addresses import addresses

df = (df
    .withColumn("street_number", addresses.extract_street_number(F.col("address")))
    .withColumn("street_name", addresses.extract_street_name(F.col("address")))
    .withColumn("city", addresses.extract_city(F.col("address")))
    .withColumn("state", addresses.standardize_state(F.col("address")))
    .withColumn("zip", addresses.extract_zip_code(F.col("address")))
)

# Result:
+----------------------------------------+-------------+------------+-----------+-----+-------+
|address                                 |street_number|street_name |city       |state|zip    |
+----------------------------------------+-------------+------------+-----------+-----+-------+
|123 Main St, New York, NY 10001        |123          |Main        |New York   |NY   |10001  |
|456 Oak Ave Apt 5B, Los Angeles, CA 90001|456        |Oak         |Los Angeles|CA   |90001  |
|789 Pine Blvd, Chicago, IL 60601       |789          |Pine        |Chicago    |IL   |60601  |
+----------------------------------------+-------------+------------+-----------+-----+-------+
```

The code lives in your repo. Modify it. Delete what you don't need. No external dependencies.

## Why Copy-to-Own?

- **Your data is weird** - Phone numbers with "ask for Bob"? We can't predict that. You can fix it.
- **No breaking changes** - Library updates can't break your pipeline at 2 AM
- **Actually debuggable** - Stack traces point to YOUR code, not site-packages
- **No dependency hell** - It's just PySpark. If Spark runs, this runs.

## Available Transformers

**Phones** - Standardize formats, extract from text, validate, handle extensions  
**Addresses** - Parse components, standardize states, validate zips, detect PO boxes  
**Emails** - Validate, extract domains, fix typos (gmial→gmail), standardize

More coming based on what you need.

## Real Example

```python
# Messy customer data
df = spark.createDataFrame([
    ("(555) 123-4567 ext 89", "john.doe@gmial.com", "123 Main St Apt 4B"),
    ("555.987.6543", "JANE@COMPANY.COM", "456 Oak Ave, NY, NY 10001")
])

# Clean it
clean_df = (df
    .withColumn("phone", phones.standardize_phone(F.col("phone")))
    .withColumn("email", emails.fix_common_typos(F.col("email")))
    .withColumn("street", addresses.extract_street_address(F.col("address")))
)
```

## The Philosophy

```
█████████████ 60% - Already clean
████████ 30% - Common patterns (formatting, typos)
██ 8% - Edge cases (weird but fixable)
▌ 2% - Complete chaos (that's what interns are for)
```

We handle the 38% with patterns. You handle the 2% chaos.

## Documentation

Full docs at [datacompose.io](https://datacompose.io)

## Key Features

- **Zero dependencies** - Just PySpark code that runs anywhere Spark runs
- **Fully modifiable** - It's in your repo. Change whatever you need
- **Battle-tested patterns** - Built from real production data cleaning challenges  
- **Composable functions** - Chain simple operations into complex pipelines
- **No breaking changes** - You control when and how to update

## License

MIT - It's your code now.

---

*Inspired by [shadcn/ui](https://ui.shadcn.com/) and [Svelte](https://svelte.dev/)'s approach to components - copy, don't install.*
