# Primitives Guide

Primitives are the fundamental building blocks of Datacompose - reusable transformation functions that operate on Spark DataFrame columns. This guide provides an overview of all available primitive libraries and how to use them effectively.

## Available Primitive Libraries

Datacompose provides three comprehensive primitive libraries out of the box:

<div class="grid cards" markdown>

-   :material-email:{ .lg .middle } **[Email Primitives](../api/email-primitives.md)**

    ---
    
    Complete email validation, extraction, and standardization
    
    - RFC 5322 compliant validation
    - Domain and username extraction
    - Gmail normalization
    - Typo correction
    - Business vs personal classification

-   :material-home:{ .lg .middle } **[Address Primitives](../api/address-primitives.md)**

    ---
    
    Full address parsing and standardization
    
    - Component extraction (street, city, state, ZIP)
    - USPS standardization
    - PO Box handling
    - International support
    - Geocoding preparation

-   :material-phone:{ .lg .middle } **[Phone Primitives](../api/phone-primitives.md)**

    ---
    
    Phone number validation and formatting
    
    - NANP validation (US/Canada)
    - International E.164 formatting
    - Toll-free detection
    - Area code extraction
    - Multiple format support

</div>

## Quick Start with Primitives

### 1. Generate Primitives

First, generate the primitives you need:

```bash
# Generate all three primitive libraries
datacompose add clean_emails --target pyspark
datacompose add clean_addresses --target pyspark
datacompose add clean_phone_numbers --target pyspark
```

### 2. Import and Use

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Import generated primitives
from build.clean_emails.email_primitives import emails
from build.clean_addresses.address_primitives import addresses
from build.clean_phone_numbers.phone_primitives import phones

# Create Spark session
spark = SparkSession.builder.appName("DataCleaning").getOrCreate()

# Load your data
df = spark.read.csv("customer_data.csv", header=True)

# Apply transformations
df_clean = df.withColumn(
    "email_valid", 
    emails.is_valid_email(F.col("email"))
).withColumn(
    "phone_formatted",
    phones.format_nanp(F.col("phone"))
).withColumn(
    "state_abbr",
    addresses.standardize_state(addresses.extract_state(F.col("address")))
)
```

## Common Use Cases

### Data Quality Assessment

Quickly assess the quality of your data:

```python
# Email quality check
email_quality = df.agg(
    F.count("*").alias("total_records"),
    F.sum(emails.is_valid_email(F.col("email")).cast("int")).alias("valid_emails"),
    F.sum(emails.is_business_email(F.col("email")).cast("int")).alias("business_emails"),
    F.sum(emails.is_disposable_email(F.col("email")).cast("int")).alias("disposable_emails")
).collect()[0]

print(f"Email Quality Report:")
print(f"  Valid: {email_quality['valid_emails']}/{email_quality['total_records']}")
print(f"  Business: {email_quality['business_emails']}")
print(f"  Disposable: {email_quality['disposable_emails']}")

# Phone quality check
phone_quality = df.agg(
    F.sum(phones.is_valid_nanp(F.col("phone")).cast("int")).alias("valid_phones"),
    F.sum(phones.is_toll_free(F.col("phone")).cast("int")).alias("toll_free"),
    F.sum(phones.is_mobile(F.col("phone")).cast("int")).alias("mobile_likely")
).collect()[0]

# Address completeness check
address_quality = df.agg(
    F.sum(addresses.is_complete_address(F.col("address")).cast("int")).alias("complete"),
    F.sum(addresses.is_po_box(F.col("address")).cast("int")).alias("po_boxes"),
    F.countDistinct(addresses.extract_state(F.col("address"))).alias("unique_states")
).collect()[0]
```

### Data Standardization

Standardize all contact information:

```python
df_standardized = df.withColumn(
    # Standardize emails
    "email_clean",
    F.when(
        emails.is_valid_email(F.col("email")),
        emails.standardize_email(F.col("email"))
    ).otherwise(None)
).withColumn(
    # Format phone numbers
    "phone_clean",
    F.when(
        phones.is_valid_nanp(F.col("phone")),
        phones.format_e164(phones.clean_phone(F.col("phone")), country_code="1")
    ).otherwise(None)
).withColumn(
    # Standardize addresses
    "address_clean",
    addresses.standardize_address(F.col("address"))
)
```

### Data Enrichment

Extract additional information from existing data:

```python
df_enriched = df.withColumn(
    # Email insights
    "email_provider",
    emails.get_email_provider(emails.extract_domain(F.col("email")))
).withColumn(
    "email_username",
    emails.extract_username(F.col("email"))
).withColumn(
    # Phone insights
    "area_code",
    phones.extract_area_code(F.col("phone"))
).withColumn(
    "phone_type",
    phones.get_phone_type(F.col("phone"))
).withColumn(
    # Address components
    "city",
    addresses.extract_city(F.col("address"))
).withColumn(
    "state",
    addresses.extract_state(F.col("address"))
).withColumn(
    "zip_code",
    addresses.extract_zip_code(F.col("address"))
)
```

### Deduplication

Remove duplicate records based on standardized values:

```python
# Create deduplication keys
df_dedup = df.withColumn(
    "email_key",
    F.when(
        F.col("email").contains("gmail"),
        emails.normalize_gmail(F.col("email"))
    ).otherwise(
        emails.standardize_email(F.col("email"))
    )
).withColumn(
    "phone_key",
    phones.extract_digits_only(F.col("phone"))
).withColumn(
    "address_key",
    F.upper(
        F.concat_ws("|",
            addresses.extract_street_number(F.col("address")),
            addresses.extract_zip_code(F.col("address"))
        )
    )
)

# Remove duplicates based on any matching key
df_unique = df_dedup.dropDuplicates(["email_key", "phone_key", "address_key"])
```

## Creating Custom Primitives

While Datacompose provides comprehensive primitives for emails, addresses, and phones, you can create your own domain-specific primitives:

```python
from datacompose.operators.primitives import PrimitiveRegistry

# Create a registry for product data
products = PrimitiveRegistry("products")

@products.register()
def extract_sku(col):
    """Extract SKU from product description"""
    return F.regexp_extract(col, r'SKU[:#\s]*([A-Z0-9]+)', 1)

@products.register()
def normalize_product_name(col):
    """Standardize product names"""
    return F.initcap(
        F.regexp_replace(
            F.trim(col),
            r'\s+', ' '
        )
    )

@products.register()
def extract_price(col):
    """Extract price from text"""
    return F.regexp_extract(col, r'\$?(\d+\.?\d*)', 1).cast("decimal(10,2)")

@products.register(is_conditional=True)
def is_in_stock(col):
    """Check if product is in stock"""
    return ~F.lower(col).contains("out of stock")

# Use custom primitives
df = df.withColumn("sku", products.extract_sku(F.col("description")))
df = df.withColumn("price", products.extract_price(F.col("price_text")))
df = df.filter(products.is_in_stock(F.col("status")))
```

## Best Practices

### 1. Validate Before Processing

Always validate data before applying transformations:

```python
# Good practice
df = df.withColumn(
    "email_processed",
    F.when(
        emails.is_valid_email(F.col("email")),
        emails.standardize_email(F.col("email"))
    ).otherwise(None)
)

# Avoid processing invalid data
# df = df.withColumn("email_processed", emails.standardize_email(F.col("email")))
```

### 2. Chain Operations Efficiently

Combine multiple operations in a single pass:

```python
# Efficient - single pass
df = df.withColumn(
    "contact_quality",
    F.struct(
        emails.is_valid_email(F.col("email")).alias("email_valid"),
        phones.is_valid_nanp(F.col("phone")).alias("phone_valid"),
        addresses.is_complete_address(F.col("address")).alias("address_complete")
    )
)

# Less efficient - multiple passes
df = df.withColumn("email_valid", emails.is_valid_email(F.col("email")))
df = df.withColumn("phone_valid", phones.is_valid_nanp(F.col("phone")))
df = df.withColumn("address_complete", addresses.is_complete_address(F.col("address")))
```

### 3. Handle Nulls Appropriately

Primitives handle nulls gracefully, but be explicit when needed:

```python
df = df.withColumn(
    "email_domain",
    F.when(
        F.col("email").isNotNull(),
        emails.extract_domain(F.col("email"))
    ).otherwise(F.lit("unknown"))
)
```

### 4. Use Specific Functions

Use the most specific function for your needs:

```python
# Specific validation (faster)
is_valid = phones.is_valid_nanp(F.col("phone"))

# Generic validation (slower, but handles international)
is_valid = phones.is_valid_phone(F.col("phone"))
```

## Performance Tips

1. **Cache DataFrames** when applying multiple primitive operations:
   ```python
   df.cache()
   # Apply multiple transformations
   df_processed = df.withColumn(...).withColumn(...).withColumn(...)
   ```

2. **Partition appropriately** for large datasets:
   ```python
   df.repartition(100, F.col("state"))
   # Process by state
   ```

3. **Use broadcast joins** for lookup data:
   ```python
   area_codes_df = spark.read.csv("area_codes.csv")
   df = df.join(F.broadcast(area_codes_df), ["area_code"], "left")
   ```

## Combining All Three Libraries

Here's a complete example using all three primitive libraries together:

```python
from build.clean_emails.email_primitives import emails
from build.clean_addresses.address_primitives import addresses
from build.clean_phone_numbers.phone_primitives import phones
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Initialize Spark
spark = SparkSession.builder.appName("DataCleaning").getOrCreate()

# Load customer data
df = spark.read.csv("customers.csv", header=True)

# Method 1: Simple combined cleaning
def simple_clean_all(df):
    """
    Basic cleaning of all contact fields
    """
    return (
        df
        # Email cleaning
        .withColumn(
            "email_clean",
            emails.standardize_email(
                emails.fix_common_typos(F.col("email"))
            )
        )
        .withColumn("email_valid", emails.is_valid_email(F.col("email_clean")))
        
        # Phone cleaning  
        .withColumn(
            "phone_clean",
            phones.standardize_phone(F.col("phone"))
        )
        .withColumn("phone_valid", phones.is_valid_nanp(F.col("phone_clean")))
        
        # Address cleaning
        .withColumn(
            "address_clean",
            addresses.standardize_address(F.col("address"))
        )
        .withColumn("address_complete", addresses.is_complete_address(F.col("address_clean")))
    )

# Method 2: Production Customer Data Pipeline
class CustomerDataPipeline:
    """
    Complete customer data processing pipeline
    """
    
    def __init__(self, df):
        self.df = df
        self.emails = emails
        self.phones = phones
        self.addresses = addresses
    
    def process(self):
        """Run full processing pipeline"""
        self.df = (
            self.df
            .transform(self.clean_emails)
            .transform(self.clean_phones)
            .transform(self.clean_addresses)
            .transform(self.validate_all)
            .transform(self.enrich_data)
            .transform(self.score_quality)
            .transform(self.deduplicate)
        )
        return self.df
    
    def clean_emails(self, df):
        """Email cleaning and validation"""
        return (
            df
            # Fix typos first
            .withColumn(
                "email_fixed",
                self.emails.fix_common_typos(F.col("email"))
            )
            # Standardize
            .withColumn(
                "email_std",
                self.emails.standardize_email(F.col("email_fixed"))
            )
            # Gmail normalization
            .withColumn(
                "email_clean",
                F.when(
                    self.emails.extract_domain(F.col("email_std")).isin("gmail.com", "googlemail.com"),
                    self.emails.normalize_gmail(F.col("email_std"))
                ).otherwise(F.col("email_std"))
            )
            # Validation flags
            .withColumn("email_is_valid", self.emails.is_valid_email(F.col("email_clean")))
            .withColumn("email_is_business", self.emails.is_business_email(F.col("email_clean")))
            .withColumn("email_is_disposable", self.emails.is_disposable_email(F.col("email_clean")))
            # Extract components
            .withColumn("email_domain", self.emails.extract_domain(F.col("email_clean")))
            .withColumn("email_username", self.emails.extract_username(F.col("email_clean")))
        )
    
    def clean_phones(self, df):
        """Phone cleaning and formatting"""
        return (
            df
            # Basic cleaning
            .withColumn(
                "phone_digits",
                self.phones.extract_digits_only(F.col("phone"))
            )
            # Standardize
            .withColumn(
                "phone_std",
                self.phones.standardize_phone(F.col("phone"))
            )
            # Validation flags
            .withColumn("phone_is_valid", self.phones.is_valid_phone(F.col("phone_std")))
            .withColumn("phone_is_nanp", self.phones.is_valid_nanp(F.col("phone_std")))
            .withColumn("phone_is_toll_free", self.phones.is_toll_free(F.col("phone_std")))
            .withColumn("phone_is_mobile", self.phones.is_mobile(F.col("phone_std")))
            # Format for different uses
            .withColumn(
                "phone_formatted",
                F.when(
                    F.col("phone_is_nanp"),
                    self.phones.format_nanp(F.col("phone_std"))
                )
            )
            .withColumn(
                "phone_e164",
                F.when(
                    F.col("phone_is_nanp"),
                    self.phones.format_e164(F.col("phone_std"), country_code="1")
                )
            )
            # Extract components
            .withColumn("phone_area_code", self.phones.extract_area_code(F.col("phone_std")))
        )
    
    def clean_addresses(self, df):
        """Address parsing and standardization"""
        return (
            df
            # Standardize
            .withColumn(
                "address_std",
                self.addresses.standardize_address(F.col("address"))
            )
            # Check type
            .withColumn("address_is_po_box", self.addresses.is_po_box(F.col("address_std")))
            .withColumn("address_is_complete", self.addresses.is_complete_address(F.col("address_std")))
            # Extract all components
            .withColumn("street_number", self.addresses.extract_street_number(F.col("address_std")))
            .withColumn("street_name", self.addresses.extract_street_name(F.col("address_std")))
            .withColumn("city", self.addresses.extract_city(F.col("address_std")))
            .withColumn("state_raw", self.addresses.extract_state(F.col("address_std")))
            .withColumn("state", self.addresses.standardize_state(F.col("state_raw")))
            .withColumn("zip_code", self.addresses.extract_zip_code(F.col("address_std")))
            # Validate components
            .withColumn("zip_is_valid", self.addresses.is_valid_zip_code(F.col("zip_code")))
            .withColumn("state_is_valid", self.addresses.is_valid_state(F.col("state")))
        )
    
    def validate_all(self, df):
        """Combined validation"""
        return (
            df
            .withColumn(
                "contact_valid",
                F.struct(
                    F.col("email_is_valid").alias("email"),
                    F.col("phone_is_valid").alias("phone"),
                    F.col("address_is_complete").alias("address")
                )
            )
            .withColumn(
                "all_contacts_valid",
                F.col("email_is_valid") & 
                F.col("phone_is_valid") & 
                F.col("address_is_complete")
            )
        )
    
    def enrich_data(self, df):
        """Add enrichment data"""
        return (
            df
            # Email enrichment
            .withColumn(
                "email_provider",
                self.emails.get_email_provider(F.col("email_domain"))
            )
            # Phone enrichment
            .withColumn(
                "phone_timezone",
                F.when(
                    F.col("phone_is_nanp"),
                    self.phones.get_timezone(F.col("phone_std"))
                )
            )
            .withColumn(
                "phone_geographic_area",
                F.when(
                    F.col("phone_area_code").isNotNull(),
                    self.phones.get_geographic_area(F.col("phone_area_code"))
                )
            )
            # Address enrichment
            .withColumn(
                "state_full_name",
                self.addresses.get_state_name(F.col("state"))
            )
            .withColumn(
                "address_timezone",
                self.addresses.get_timezone(F.col("state"))
            )
        )
    
    def score_quality(self, df):
        """Calculate data quality scores"""
        return (
            df
            # Email quality score
            .withColumn(
                "email_quality",
                F.when(~F.col("email_is_valid"), 0)
                .when(F.col("email_is_disposable"), 0.3)
                .when(F.col("email_is_business"), 1.0)
                .otherwise(0.7)
            )
            # Phone quality score
            .withColumn(
                "phone_quality",
                F.when(~F.col("phone_is_valid"), 0)
                .when(F.col("phone_is_toll_free"), 0.5)
                .when(F.col("phone_is_mobile"), 1.0)
                .when(F.col("phone_is_nanp"), 0.8)
                .otherwise(0.3)
            )
            # Address quality score
            .withColumn(
                "address_quality",
                F.when(~F.col("address_is_complete"), 0)
                .when(F.col("address_is_po_box"), 0.7)
                .when(
                    F.col("zip_is_valid") & F.col("state_is_valid"),
                    1.0
                )
                .otherwise(0.5)
            )
            # Overall quality score
            .withColumn(
                "overall_quality",
                (F.col("email_quality") + F.col("phone_quality") + F.col("address_quality")) / 3
            )
            # Quality tier
            .withColumn(
                "quality_tier",
                F.when(F.col("overall_quality") >= 0.9, "A")
                .when(F.col("overall_quality") >= 0.7, "B")
                .when(F.col("overall_quality") >= 0.5, "C")
                .when(F.col("overall_quality") >= 0.3, "D")
                .otherwise("F")
            )
        )
    
    def deduplicate(self, df):
        """Deduplicate based on cleaned values"""
        return (
            df
            # Create deduplication keys
            .withColumn(
                "dedup_key",
                F.md5(
                    F.concat_ws("|",
                        F.coalesce(F.col("email_clean"), F.lit("")),
                        F.coalesce(F.col("phone_e164"), F.col("phone_digits"), F.lit("")),
                        F.coalesce(
                            F.concat_ws("-", F.col("street_number"), F.col("zip_code")),
                            F.lit("")
                        )
                    )
                )
            )
            # Keep best quality record for each key
            .withColumn(
                "row_priority",
                F.row_number().over(
                    Window.partitionBy("dedup_key")
                    .orderBy(F.desc("overall_quality"), F.desc("all_contacts_valid"))
                )
            )
            .filter(F.col("row_priority") == 1)
            .drop("row_priority")
        )
    
    def get_summary(self):
        """Generate summary statistics"""
        total = self.df.count()
        
        summary = {
            "total_records": total,
            "quality_distribution": (
                self.df.groupBy("quality_tier")
                .count()
                .orderBy("quality_tier")
                .collect()
            ),
            "validation_stats": self.df.agg(
                F.avg("email_is_valid").alias("email_valid_pct"),
                F.avg("phone_is_valid").alias("phone_valid_pct"),
                F.avg("address_is_complete").alias("address_complete_pct"),
                F.avg("overall_quality").alias("avg_quality_score")
            ).collect()[0],
            "contact_types": {
                "business_emails": self.df.filter(F.col("email_is_business")).count(),
                "mobile_phones": self.df.filter(F.col("phone_is_mobile")).count(),
                "po_boxes": self.df.filter(F.col("address_is_po_box")).count()
            }
        }
        
        return summary

# Method 3: Data Quality Assessment Pipeline
class DataQualityAssessment:
    """
    Assess and report on data quality using all primitives
    """
    
    def __init__(self, df):
        self.df = df
        self.emails = emails
        self.phones = phones
        self.addresses = addresses
    
    def assess(self):
        """Run quality assessment"""
        return {
            "email_quality": self.assess_emails(),
            "phone_quality": self.assess_phones(),
            "address_quality": self.assess_addresses(),
            "overall_quality": self.assess_overall()
        }
    
    def assess_emails(self):
        """Assess email data quality"""
        return self.df.agg(
            F.count("email").alias("total"),
            F.sum(self.emails.is_valid_email(F.col("email")).cast("int")).alias("valid"),
            F.sum(self.emails.is_business_email(F.col("email")).cast("int")).alias("business"),
            F.sum(self.emails.is_disposable_email(F.col("email")).cast("int")).alias("disposable"),
            F.countDistinct(self.emails.extract_domain(F.col("email"))).alias("unique_domains")
        ).collect()[0].asDict()
    
    def assess_phones(self):
        """Assess phone data quality"""
        return self.df.agg(
            F.count("phone").alias("total"),
            F.sum(self.phones.is_valid_nanp(F.col("phone")).cast("int")).alias("valid_nanp"),
            F.sum(self.phones.is_toll_free(F.col("phone")).cast("int")).alias("toll_free"),
            F.sum(self.phones.is_mobile(F.col("phone")).cast("int")).alias("mobile"),
            F.countDistinct(self.phones.extract_area_code(F.col("phone"))).alias("unique_area_codes")
        ).collect()[0].asDict()
    
    def assess_addresses(self):
        """Assess address data quality"""
        return self.df.agg(
            F.count("address").alias("total"),
            F.sum(self.addresses.is_complete_address(F.col("address")).cast("int")).alias("complete"),
            F.sum(self.addresses.is_po_box(F.col("address")).cast("int")).alias("po_boxes"),
            F.sum(self.addresses.is_valid_zip_code(
                self.addresses.extract_zip_code(F.col("address"))
            ).cast("int")).alias("valid_zips"),
            F.countDistinct(self.addresses.extract_state(F.col("address"))).alias("unique_states")
        ).collect()[0].asDict()
    
    def assess_overall(self):
        """Overall quality metrics"""
        return {
            "total_records": self.df.count(),
            "complete_records": self.df.filter(
                self.emails.is_valid_email(F.col("email")) &
                self.phones.is_valid_phone(F.col("phone")) &
                self.addresses.is_complete_address(F.col("address"))
            ).count(),
            "missing_data": self.df.filter(
                F.col("email").isNull() |
                F.col("phone").isNull() |
                F.col("address").isNull()
            ).count()
        }

# Usage Examples
# Simple cleaning
df_clean = simple_clean_all(df)

# Full pipeline
pipeline = CustomerDataPipeline(df)
df_processed = pipeline.process()
summary = pipeline.get_summary()
print(f"Average quality score: {summary['validation_stats']['avg_quality_score']:.2%}")

# Quality assessment
assessment = DataQualityAssessment(df)
quality_report = assessment.assess()
print(f"Email validity: {quality_report['email_quality']['valid'] / quality_report['email_quality']['total']:.2%}")
print(f"Phone validity: {quality_report['phone_quality']['valid_nanp'] / quality_report['phone_quality']['total']:.2%}")
print(f"Address completeness: {quality_report['address_quality']['complete'] / quality_report['address_quality']['total']:.2%}")

# Export high-quality records
df_high_quality = df_processed.filter(F.col("quality_tier").isin("A", "B"))
df_high_quality.write.parquet("clean_customers.parquet")

# Comprehensive customer data cleaning
def clean_customer_data(df):
    """
    Clean and standardize all customer contact information
    """
    return df.withColumn(
        # Email processing
        "email_clean",
        F.when(
            emails.is_valid_email(F.col("email")) & 
            ~emails.is_disposable_email(F.col("email")),
            emails.standardize_email(F.col("email"))
        )
    ).withColumn(
        "email_domain",
        emails.extract_domain(F.col("email_clean"))
    ).withColumn(
        # Phone processing
        "phone_clean",
        F.when(
            phones.is_valid_nanp(F.col("phone")) &
            ~phones.is_premium_rate(F.col("phone")),
            phones.format_e164(F.col("phone"), country_code="1")
        )
    ).withColumn(
        "phone_type",
        phones.get_phone_type(F.col("phone_clean"))
    ).withColumn(
        # Address processing
        "address_clean",
        addresses.standardize_address(F.col("address"))
    ).withColumn(
        "address_components",
        F.struct(
            addresses.extract_street_number(F.col("address")).alias("number"),
            addresses.extract_street_name(F.col("address")).alias("street"),
            addresses.extract_city(F.col("address")).alias("city"),
            addresses.standardize_state(
                addresses.extract_state(F.col("address"))
            ).alias("state"),
            addresses.extract_zip_code(F.col("address")).alias("zip")
        )
    ).withColumn(
        # Overall data quality score
        "quality_score",
        (
            emails.is_valid_email(F.col("email")).cast("int") +
            phones.is_valid_nanp(F.col("phone")).cast("int") +
            addresses.is_complete_address(F.col("address")).cast("int")
        ) / 3.0
    )

# Apply the cleaning function
df_clean = clean_customer_data(df)

# Filter high-quality records
df_high_quality = df_clean.filter(F.col("quality_score") >= 0.66)
```

## Next Steps

- Explore detailed documentation for each primitive library:
  - [Email Primitives Reference](../api/email-primitives.md)
  - [Address Primitives Reference](../api/address-primitives.md)
  - [Phone Primitives Reference](../api/phone-primitives.md)
- Learn about [Building Pipelines](pipelines.md) with primitives
- Understand [Core Concepts](core-concepts.md) behind primitives
- See [Examples](../examples/index.md) of real-world usage