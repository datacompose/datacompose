# Email Primitives

Comprehensive email validation, extraction, and transformation primitives for PySpark.

## Installation

Generate the email primitives for your project:

```bash
datacompose add clean_emails --target pyspark
```

## Import

```python
from build.clean_emails.email_primitives import emails
from pyspark.sql import functions as F
```

## Validation Functions

### `is_valid_email`

Validates email addresses according to RFC 5322 standards.

```python
df = df.withColumn("is_valid", emails.is_valid_email(F.col("email")))
```

**Validates:**
- Proper format with @ symbol
- Valid local part (before @)
- Valid domain part (after @)
- No consecutive dots
- No leading/trailing dots
- Valid characters only

**Examples:**
```python
# Valid emails
"user@example.com"          # ✅
"user.name@example.co.uk"   # ✅
"user+tag@example.com"      # ✅

# Invalid emails
"@example.com"              # ❌ Missing local part
"user@"                     # ❌ Missing domain
"user..name@example.com"    # ❌ Consecutive dots
"user@.example.com"         # ❌ Leading dot in domain
```

### `is_business_email`

Checks if an email belongs to a business domain (not free email providers).

```python
df = df.withColumn("is_business", emails.is_business_email(F.col("email")))
```

**Excludes common free providers:**
- gmail.com, yahoo.com, hotmail.com
- outlook.com, aol.com, icloud.com
- And many more...

### `is_disposable_email`

Detects temporary/disposable email addresses.

```python
df = df.withColumn("is_disposable", emails.is_disposable_email(F.col("email")))
```

**Detects domains like:**
- 10minutemail.com
- tempmail.com
- guerrillamail.com
- mailinator.com

### `has_valid_mx`

Checks if the email domain could potentially receive emails (has MX records).

```python
df = df.withColumn("has_mx", emails.has_valid_mx(F.col("email")))
```

## Extraction Functions

### `extract_domain`

Extracts the domain portion of an email address.

```python
df = df.withColumn("domain", emails.extract_domain(F.col("email")))

# Input: "user@example.com"
# Output: "example.com"
```

### `extract_username`

Extracts the username (local part) of an email address.

```python
df = df.withColumn("username", emails.extract_username(F.col("email")))

# Input: "john.doe+filter@example.com"
# Output: "john.doe+filter"
```

### `extract_tld`

Extracts the top-level domain from an email address.

```python
df = df.withColumn("tld", emails.extract_tld(F.col("email")))

# Input: "user@example.co.uk"
# Output: "co.uk"
```

### `extract_subdomain`

Extracts subdomain if present.

```python
df = df.withColumn("subdomain", emails.extract_subdomain(F.col("email")))

# Input: "user@mail.example.com"
# Output: "mail"
```

### `extract_base_username`

Extracts username without plus addressing or dots (Gmail-style).

```python
df = df.withColumn("base_user", emails.extract_base_username(F.col("email")))

# Input: "john.doe+newsletter@gmail.com"
# Output: "johndoe"
```

## Standardization Functions

### `standardize_email`

Applies standard email normalization rules.

```python
df = df.withColumn("email_clean", emails.standardize_email(F.col("email")))
```

**Performs:**
- Converts to lowercase
- Trims whitespace
- Removes quotes from local part
- Normalizes dots in Gmail addresses

**Examples:**
```python
"John.Doe@EXAMPLE.COM"     → "john.doe@example.com"
"  user@example.com  "     → "user@example.com"
'"user"@example.com'       → "user@example.com"
```

### `normalize_gmail`

Applies Gmail-specific normalization rules.

```python
df = df.withColumn("gmail_normalized", emails.normalize_gmail(F.col("email")))
```

**Gmail rules:**
- Removes dots from username
- Removes everything after + in username
- Converts googlemail.com to gmail.com

**Examples:**
```python
"john.doe+spam@gmail.com"        → "johndoe@gmail.com"
"j.o.h.n@googlemail.com"         → "john@gmail.com"
```

### `fix_common_typos`

Corrects common email domain typos.

```python
df = df.withColumn("email_fixed", emails.fix_common_typos(F.col("email")))
```

**Fixes typos like:**
```python
"user@gmial.com"          → "user@gmail.com"
"user@yahooo.com"         → "user@yahoo.com"
"user@hotnail.com"        → "user@hotmail.com"
"user@outlok.com"         → "user@outlook.com"
```

### `remove_plus_addressing`

Removes plus addressing from email addresses.

```python
df = df.withColumn("email_no_plus", emails.remove_plus_addressing(F.col("email")))

# Input: "user+newsletter@example.com"
# Output: "user@example.com"
```

## Filtering Functions

### `filter_valid_emails`

Returns only valid email addresses, null for invalid ones.

```python
df = df.withColumn("valid_only", emails.filter_valid_emails(F.col("email")))
```

### `filter_business_emails`

Returns only business email addresses.

```python
df = df.withColumn("business_only", emails.filter_business_emails(F.col("email")))
```

### `filter_personal_emails`

Returns only personal (free provider) email addresses.

```python
df = df.withColumn("personal_only", emails.filter_personal_emails(F.col("email")))
```

## Analysis Functions

### `get_email_provider`

Identifies the email service provider.

```python
df = df.withColumn("provider", emails.get_email_provider(F.col("email")))

# Input: "user@gmail.com"
# Output: "Google"

# Input: "user@company.com"
# Output: "Other"
```

### `categorize_email`

Categorizes email into types.

```python
df = df.withColumn("category", emails.categorize_email(F.col("email")))
```

**Categories:**
- "personal" - Free email providers
- "business" - Corporate domains
- "disposable" - Temporary email services
- "invalid" - Malformed addresses

### `get_domain_parts`

Splits domain into components.

```python
df = df.withColumn("parts", emails.get_domain_parts(F.col("email")))

# Input: "user@mail.example.co.uk"
# Output: {"subdomain": "mail", "domain": "example", "tld": "co.uk"}
```

## Pipeline Examples Using @compose

The `@compose` decorator allows you to build powerful transformation pipelines by chaining primitives together. These composed functions must be defined at the module level (top level of your Python file).

### Complete Email Cleaning Pipelines

```python
from build.clean_emails.email_primitives import emails
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ============================================
# COMPOSED PIPELINE FUNCTIONS (Module Level)
# ============================================

@emails.compose(emails=emails)
def basic_email_cleaning():
    """Basic email cleaning pipeline"""
    emails.standardize_email()
    emails.fix_common_typos()
    if emails.is_valid_email():
        emails.extract_domain()

@emails.compose(emails=emails)
def validate_and_standardize():
    """Validate and standardize emails"""
    # First fix common typos
    emails.fix_common_typos()
    
    # Then standardize
    emails.standardize_email()
    
    # Check validity
    if emails.is_valid_email():
        # For valid emails, normalize Gmail
        if emails.extract_domain() == "gmail.com":
            emails.normalize_gmail()
    else:
        # Return null for invalid
        return F.lit(None)

@emails.compose(emails=emails)
def business_email_pipeline():
    """Pipeline for business email processing"""
    emails.standardize_email()
    emails.fix_common_typos()
    
    if emails.is_valid_email():
        if emails.is_business_email():
            # Process business emails
            emails.extract_domain()
            emails.extract_username()
            return F.lit("BUSINESS_VERIFIED")
        elif emails.is_disposable_email():
            return F.lit("DISPOSABLE_BLOCKED")
        else:
            # Personal email
            if emails.extract_domain() in ["gmail.com", "googlemail.com"]:
                emails.normalize_gmail()
            return F.lit("PERSONAL")
    else:
        return F.lit("INVALID")

@emails.compose(emails=emails)
def email_deduplication_key():
    """Generate deduplication key for emails"""
    emails.standardize_email()
    
    # Special handling for Gmail
    if emails.extract_domain() in ["gmail.com", "googlemail.com"]:
        emails.normalize_gmail()
        # Remove dots and everything after +
        emails.extract_base_username()
    
    # Return standardized email as key
    emails.standardize_email()

@emails.compose(emails=emails)
def email_quality_assessment():
    """Assess email quality and return score"""
    # Initial standardization
    emails.standardize_email()
    emails.fix_common_typos()
    
    # Check various quality factors
    if not emails.is_valid_email():
        return F.lit(0.0)
    elif emails.is_disposable_email():
        return F.lit(0.2)
    elif emails.has_valid_mx():
        if emails.is_business_email():
            return F.lit(1.0)
        else:
            return F.lit(0.8)
    else:
        return F.lit(0.5)

@emails.compose(emails=emails)
def extract_email_metadata():
    """Extract all metadata from email"""
    emails.standardize_email()
    
    if emails.is_valid_email():
        # Extract all components
        username = emails.extract_username()
        domain = emails.extract_domain()
        tld = emails.extract_tld()
        provider = emails.get_email_provider()
        
        # Return as struct
        return F.struct(
            F.lit(username).alias("username"),
            F.lit(domain).alias("domain"),
            F.lit(tld).alias("tld"),
            F.lit(provider).alias("provider")
        )
    else:
        return F.lit(None)

@emails.compose(emails=emails)
def marketing_email_filter():
    """Filter emails suitable for marketing"""
    emails.standardize_email()
    emails.fix_common_typos()
    
    # Must be valid
    if not emails.is_valid_email():
        return F.lit(False)
    
    # Must not be disposable
    if emails.is_disposable_email():
        return F.lit(False)
    
    # Must have valid MX records
    if not emails.has_valid_mx():
        return F.lit(False)
    
    # Prefer business emails
    if emails.is_business_email():
        return F.lit(True)
    
    # Personal emails are okay too
    return F.lit(True)

@emails.compose(emails=emails)
def email_anonymization():
    """Anonymize email while preserving domain"""
    if emails.is_valid_email():
        domain = emails.extract_domain()
        # Return anonymized email
        return F.concat(F.lit("user****@"), F.lit(domain))
    else:
        return F.lit("invalid@unknown.com")

# ============================================
# USAGE IN APPLICATION
# ============================================

def process_customer_emails(spark):
    """Main processing function using composed pipelines"""
    
    # Load data
    df = spark.read.csv("customer_emails.csv", header=True)
    
    # Apply basic cleaning
    df = df.withColumn(
        "email_clean",
        basic_email_cleaning(F.col("email"))
    )
    
    # Validate and standardize
    df = df.withColumn(
        "email_validated",
        validate_and_standardize(F.col("email"))
    )
    
    # Categorize emails
    df = df.withColumn(
        "email_category",
        business_email_pipeline(F.col("email"))
    )
    
    # Generate deduplication keys
    df = df.withColumn(
        "dedup_key",
        email_deduplication_key(F.col("email"))
    )
    
    # Assess quality
    df = df.withColumn(
        "quality_score",
        email_quality_assessment(F.col("email"))
    )
    
    # Extract metadata
    df = df.withColumn(
        "email_metadata",
        extract_email_metadata(F.col("email"))
    )
    
    # Filter for marketing
    df = df.withColumn(
        "marketing_eligible",
        marketing_email_filter(F.col("email"))
    )
    
    # Anonymize if needed
    df = df.withColumn(
        "email_anonymized",
        email_anonymization(F.col("email"))
    )
    
    return df

# ============================================
# ADVANCED COMPOSED PIPELINES
# ============================================

@emails.compose(emails=emails)
def email_enrichment_pipeline():
    """Comprehensive email enrichment"""
    # Standardize first
    emails.standardize_email()
    emails.fix_common_typos()
    
    if emails.is_valid_email():
        # Extract base components
        username = emails.extract_username()
        domain = emails.extract_domain()
        
        # Determine email type
        if emails.is_business_email():
            email_type = "business"
        elif emails.is_disposable_email():
            email_type = "disposable"
        else:
            email_type = "personal"
        
        # Get provider info
        provider = emails.get_email_provider()
        
        # Check special cases
        if domain in ["gmail.com", "googlemail.com"]:
            emails.normalize_gmail()
            base_username = emails.extract_base_username()
        else:
            base_username = username
        
        # Return enriched data
        return F.struct(
            F.lit(username).alias("username"),
            F.lit(base_username).alias("base_username"),
            F.lit(domain).alias("domain"),
            F.lit(provider).alias("provider"),
            F.lit(email_type).alias("type"),
            emails.has_valid_mx().alias("has_mx")
        )
    else:
        return F.lit(None)

@emails.compose(emails=emails)
def email_data_quality_report():
    """Generate detailed quality report for email"""
    # Check all quality aspects
    is_valid = emails.is_valid_email()
    is_business = emails.is_business_email() if is_valid else False
    is_disposable = emails.is_disposable_email() if is_valid else False
    has_mx = emails.has_valid_mx() if is_valid else False
    
    # Calculate quality score
    if not is_valid:
        score = 0
        status = "invalid"
    elif is_disposable:
        score = 20
        status = "disposable"
    elif not has_mx:
        score = 40
        status = "no_mx_records"
    elif is_business:
        score = 100
        status = "verified_business"
    else:
        score = 80
        status = "verified_personal"
    
    return F.struct(
        F.lit(is_valid).alias("is_valid"),
        F.lit(is_business).alias("is_business"),
        F.lit(is_disposable).alias("is_disposable"),
        F.lit(has_mx).alias("has_mx_records"),
        F.lit(score).alias("quality_score"),
        F.lit(status).alias("status")
    )

# ============================================
# USAGE EXAMPLE
# ============================================

if __name__ == "__main__":
    # Initialize Spark
    spark = SparkSession.builder.appName("EmailProcessing").getOrCreate()
    
    # Process emails
    df_processed = process_customer_emails(spark)
    
    # Show results
    df_processed.select(
        "email",
        "email_category",
        "quality_score",
        "marketing_eligible",
        "email_metadata.domain",
        "email_metadata.provider"
    ).show(20, truncate=False)
    
    # Deduplicate using composed key
    df_unique = df_processed.dropDuplicates(["dedup_key"])
    
    # Filter high-quality emails for marketing
    df_marketing = df_processed.filter(
        (F.col("marketing_eligible") == True) & 
        (F.col("quality_score") >= 0.8)
    )
    
    print(f"Total emails: {df_processed.count()}")
    print(f"Unique emails: {df_unique.count()}")
    print(f"Marketing eligible: {df_marketing.count()}")

# Method 1: Step-by-step pipeline (without @compose)
def clean_emails_step_by_step(df):
    """
    Clean emails with detailed step-by-step transformations
    """
    return (
        df
        # Step 1: Basic standardization
        .withColumn("email_lower", F.lower(F.trim(F.col("email"))))
        
        # Step 2: Fix common typos first
        .withColumn("email_fixed", emails.fix_common_typos(F.col("email_lower")))
        
        # Step 3: Validate the fixed email
        .withColumn("is_valid", emails.is_valid_email(F.col("email_fixed")))
        
        # Step 4: Standardize valid emails
        .withColumn(
            "email_standardized",
            F.when(
                F.col("is_valid"),
                emails.standardize_email(F.col("email_fixed"))
            ).otherwise(None)
        )
        
        # Step 5: Extract domain for analysis
        .withColumn(
            "domain",
            F.when(
                F.col("is_valid"),
                emails.extract_domain(F.col("email_standardized"))
            )
        )
        
        # Step 6: Check if business email
        .withColumn(
            "is_business",
            F.when(
                F.col("is_valid"),
                emails.is_business_email(F.col("email_standardized"))
            ).otherwise(False)
        )
        
        # Step 7: Check for disposable emails
        .withColumn(
            "is_disposable",
            F.when(
                F.col("is_valid"),
                emails.is_disposable_email(F.col("email_standardized"))
            ).otherwise(False)
        )
        
        # Step 8: Gmail-specific normalization
        .withColumn(
            "email_final",
            F.when(
                F.col("domain").isin("gmail.com", "googlemail.com"),
                emails.normalize_gmail(F.col("email_standardized"))
            ).otherwise(F.col("email_standardized"))
        )
        
        # Step 9: Create quality score
        .withColumn(
            "email_quality_score",
            F.when(~F.col("is_valid"), 0)
            .when(F.col("is_disposable"), 1)
            .when(F.col("is_business"), 3)
            .otherwise(2)
        )
        
        # Step 10: Add metadata
        .withColumn("email_provider", emails.get_email_provider(F.col("domain")))
        .withColumn("username", emails.extract_username(F.col("email_final")))
    )

# Method 2: Composed pipeline function
@emails.compose(emails=emails)
def advanced_email_pipeline():
    """
    Advanced email cleaning pipeline using composition
    """
    # Initial cleaning
    emails.standardize_email()
    emails.fix_common_typos()
    
    # Validation check
    if emails.is_valid_email():
        # Process valid emails
        emails.extract_domain()
        emails.extract_username()
        
        # Handle Gmail specifically
        if emails.is_gmail():
            emails.normalize_gmail()
        
        # Check email type
        if emails.is_disposable_email():
            return F.lit("BLOCKED_DISPOSABLE")
        elif emails.is_business_email():
            return F.lit("BUSINESS")
        else:
            return F.lit("PERSONAL")
    else:
        return F.lit("INVALID")

# Method 3: Complete production pipeline
class EmailCleaningPipeline:
    """
    Production-ready email cleaning pipeline
    """
    
    def __init__(self, df):
        self.df = df
        self.emails = emails
    
    def clean(self):
        """Main cleaning pipeline"""
        self.df = (
            self.df
            .transform(self.standardize_emails)
            .transform(self.validate_emails)
            .transform(self.extract_components)
            .transform(self.categorize_emails)
            .transform(self.deduplicate_emails)
        )
        return self.df
    
    def standardize_emails(self, df):
        """Standardization step"""
        return df.withColumn(
            "email_clean",
            self.emails.standardize_email(
                self.emails.fix_common_typos(F.col("email"))
            )
        )
    
    def validate_emails(self, df):
        """Validation step"""
        return df.withColumn(
            "validation",
            F.struct(
                self.emails.is_valid_email(F.col("email_clean")).alias("is_valid"),
                self.emails.is_disposable_email(F.col("email_clean")).alias("is_disposable"),
                self.emails.is_business_email(F.col("email_clean")).alias("is_business"),
                self.emails.has_valid_mx(F.col("email_clean")).alias("has_mx")
            )
        )
    
    def extract_components(self, df):
        """Component extraction step"""
        return df.withColumn(
            "email_parts",
            F.struct(
                self.emails.extract_username(F.col("email_clean")).alias("username"),
                self.emails.extract_domain(F.col("email_clean")).alias("domain"),
                self.emails.extract_tld(F.col("email_clean")).alias("tld"),
                self.emails.get_email_provider(F.col("email_clean")).alias("provider")
            )
        )
    
    def categorize_emails(self, df):
        """Categorization step"""
        return df.withColumn(
            "email_category",
            F.when(~F.col("validation.is_valid"), "invalid")
            .when(F.col("validation.is_disposable"), "disposable")
            .when(F.col("validation.is_business"), "business")
            .when(F.col("email_parts.domain").isin("gmail.com", "yahoo.com", "hotmail.com"), "personal_major")
            .otherwise("personal_other")
        )
    
    def deduplicate_emails(self, df):
        """Deduplication step"""
        return df.withColumn(
            "email_normalized",
            F.when(
                F.col("email_parts.domain").isin("gmail.com", "googlemail.com"),
                self.emails.normalize_gmail(F.col("email_clean"))
            ).otherwise(F.col("email_clean"))
        ).dropDuplicates(["email_normalized"])
    
    def get_statistics(self):
        """Generate statistics about the cleaned data"""
        return self.df.groupBy("email_category").agg(
            F.count("*").alias("count"),
            F.countDistinct("email_parts.domain").alias("unique_domains"),
            F.avg(F.length("email_clean")).alias("avg_email_length")
        )

# Usage
pipeline = EmailCleaningPipeline(df)
cleaned_df = pipeline.clean()
stats = pipeline.get_statistics()
stats.show()
```

### Business Email Validation

```python
# Filter and validate business emails only
df_business = df.filter(
    emails.is_valid_email(F.col("email")) & 
    emails.is_business_email(F.col("email"))
).withColumn(
    "domain", 
    emails.extract_domain(F.col("email"))
).withColumn(
    "company", 
    F.split(F.col("domain"), "\\.").getItem(0)
)
```

### Email Deduplication

```python
# Deduplicate emails considering Gmail normalization
df_dedup = df.withColumn(
    "email_key",
    F.when(
        F.col("email").contains("gmail") | F.col("email").contains("googlemail"),
        emails.normalize_gmail(F.col("email"))
    ).otherwise(
        emails.standardize_email(F.col("email"))
    )
).dropDuplicates(["email_key"])
```

### Data Quality Report

```python
# Generate email data quality metrics
quality_report = df.agg(
    F.count("email").alias("total_emails"),
    F.sum(emails.is_valid_email(F.col("email")).cast("int")).alias("valid_count"),
    F.sum(emails.is_business_email(F.col("email")).cast("int")).alias("business_count"),
    F.sum(emails.is_disposable_email(F.col("email")).cast("int")).alias("disposable_count"),
    F.countDistinct(emails.extract_domain(F.col("email"))).alias("unique_domains")
)
```

## Advanced: Creating Custom Email Primitives

You can extend the email primitives library with your own custom functions tailored to your specific business needs.

### Setting Up Custom Email Primitives

```python
from build.clean_emails.email_primitives import emails
from pyspark.sql import functions as F
import re

# ============================================
# CUSTOM EMAIL PRIMITIVES
# ============================================

# Add custom validation functions
@emails.register()
def is_corporate_email(col, company_domain):
    """Check if email belongs to a specific company"""
    return F.lower(emails.extract_domain(col)) == F.lower(F.lit(company_domain))

@emails.register()
def is_internal_email(col):
    """Check if email is from internal domains"""
    internal_domains = ["mycompany.com", "subsidiary.com", "internal.org"]
    domain = emails.extract_domain(col)
    return domain.isin(internal_domains)

@emails.register()
def is_partner_email(col):
    """Check if email is from partner organizations"""
    partner_domains = [
        "partner1.com", "partner2.org", "vendor.com",
        "supplier.net", "consultant.io"
    ]
    domain = emails.extract_domain(col)
    return domain.isin(partner_domains)

@emails.register()
def is_competitor_email(col):
    """Flag emails from competitor domains"""
    competitor_domains = [
        "competitor1.com", "rival-company.com", 
        "competing-firm.org"
    ]
    domain = emails.extract_domain(col)
    return domain.isin(competitor_domains)

# Add custom extraction functions
@emails.register()
def extract_department_from_email(col):
    """Extract department from email pattern (e.g., sales.john@company.com)"""
    username = emails.extract_username(col)
    # Look for department prefix
    return F.when(
        username.contains("."),
        F.split(username, "\\.").getItem(0)
    ).when(
        username.contains("-"),
        F.split(username, "-").getItem(0)
    ).when(
        username.contains("_"),
        F.split(username, "_").getItem(0)
    )

@emails.register()
def extract_email_language_hint(col):
    """Guess language from email TLD and patterns"""
    tld = emails.extract_tld(col)
    return F.when(tld == "fr", "French")\
        .when(tld == "de", "German")\
        .when(tld == "es", "Spanish")\
        .when(tld == "jp", "Japanese")\
        .when(tld == "cn", "Chinese")\
        .when(tld.isin("uk", "gb"), "British English")\
        .when(tld.isin("com", "org", "net"), "English")\
        .otherwise("Unknown")

@emails.register()
def extract_email_vintage(col):
    """Categorize email by provider generation"""
    domain = emails.extract_domain(col)
    return F.when(
        domain.isin("aol.com", "hotmail.com", "yahoo.com"),
        "legacy"
    ).when(
        domain.isin("gmail.com", "outlook.com", "icloud.com"),
        "modern"
    ).when(
        domain.isin("protonmail.com", "tutanota.com", "fastmail.com"),
        "privacy-focused"
    ).when(
        domain.isin("hey.com", "superhuman.com"),
        "next-gen"
    ).otherwise("standard")

# Add custom standardization functions
@emails.register()
def standardize_corporate_email(col, company_domain):
    """Standardize emails to corporate format"""
    if emails.is_valid_email(col):
        username = emails.extract_username(col)
        # Convert to firstname.lastname format
        clean_username = F.regexp_replace(username, r'[^a-zA-Z0-9.]', '.')
        return F.lower(F.concat(clean_username, F.lit("@"), F.lit(company_domain)))
    return None

@emails.register()
def normalize_unicode_email(col):
    """Handle unicode characters in email addresses"""
    # Remove non-ASCII characters from username
    return F.regexp_replace(col, r'[^\x00-\x7F]+', '')

@emails.register()
def fix_mobile_carrier_emails(col):
    """Fix common mobile carrier email formats"""
    carrier_domains = {
        "txt.att.net": "mms.att.net",
        "vtext.com": "vzwpix.com",  # Verizon
        "tmomail.net": "T-Mobile",
        "messaging.sprintpcs.com": "pm.sprint.com"
    }
    
    domain = emails.extract_domain(col)
    username = emails.extract_username(col)
    
    for old_domain, new_domain in carrier_domains.items():
        if domain == old_domain:
            return F.concat(username, F.lit("@"), F.lit(new_domain))
    return col

# Add custom filtering functions
@emails.register()
def filter_suspicious_emails(col):
    """Filter potentially suspicious email patterns"""
    username = emails.extract_username(col)
    
    suspicious_patterns = [
        F.length(username) > 30,  # Very long usernames
        username.rlike(r'^[0-9]{8,}'),  # Starts with many numbers
        username.rlike(r'(temp|test|fake|dummy)'),  # Test accounts
        username.rlike(r'[0-9]{4,}$'),  # Ends with many numbers
        username.contains("..."),  # Multiple dots
    ]
    
    is_suspicious = F.lit(False)
    for pattern in suspicious_patterns:
        is_suspicious = is_suspicious | pattern
    
    return F.when(~is_suspicious, col).otherwise(None)

@emails.register()
def filter_role_based_emails(col):
    """Filter role-based email addresses"""
    role_prefixes = [
        "admin", "info", "contact", "support", "sales",
        "marketing", "noreply", "no-reply", "donotreply",
        "webmaster", "postmaster", "abuse", "help"
    ]
    
    username = F.lower(emails.extract_username(col))
    is_role = F.lit(False)
    
    for prefix in role_prefixes:
        is_role = is_role | username.startswith(prefix)
    
    return F.when(~is_role, col).otherwise(None)

# Add custom analysis functions
@emails.register()
def calculate_email_complexity_score(col):
    """Calculate complexity score for email addresses"""
    username = emails.extract_username(col)
    
    score = (
        F.when(username.rlike(r'[A-Z]'), 1).otherwise(0) +  # Has uppercase
        F.when(username.rlike(r'[0-9]'), 1).otherwise(0) +  # Has numbers
        F.when(username.rlike(r'[._-]'), 1).otherwise(0) +  # Has separators
        F.when(F.length(username) > 8, 1).otherwise(0) +    # Good length
        F.when(~username.rlike(r'^[0-9]'), 1).otherwise(0)  # Doesn't start with number
    )
    
    return score / 5.0  # Normalize to 0-1

@emails.register()
def guess_email_creation_year(col):
    """Estimate when email was likely created based on patterns"""
    domain = emails.extract_domain(col)
    username = emails.extract_username(col)
    
    # Look for year patterns in username
    year_match = F.regexp_extract(username, r'(19[7-9][0-9]|20[0-2][0-9])', 1)
    
    return F.when(
        year_match.isNotNull(),
        year_match
    ).when(
        domain == "gmail.com",
        F.when(F.length(username) <= 6, "2004-2007")  # Short Gmail = early adopter
         .when(username.contains("."), "2008-2012")
         .otherwise("2013+")
    ).when(
        domain == "hotmail.com",
        "1996-2005"  # Old school
    ).otherwise(None)

# ============================================
# ADVANCED COMPOSED PIPELINES WITH CUSTOM PRIMITIVES
# ============================================

@emails.compose(emails=emails)
def corporate_email_security_check():
    """Security check for corporate emails"""
    emails.standardize_email()
    
    if not emails.is_valid_email():
        return F.lit("INVALID")
    
    # Check if from competitor
    if emails.is_competitor_email():
        return F.lit("COMPETITOR_BLOCKED")
    
    # Check if internal
    if emails.is_internal_email():
        return F.lit("INTERNAL_TRUSTED")
    
    # Check if partner
    if emails.is_partner_email():
        return F.lit("PARTNER_VERIFIED")
    
    # Check if suspicious
    if not emails.filter_suspicious_emails():
        return F.lit("SUSPICIOUS_FLAGGED")
    
    # Check if role-based
    if not emails.filter_role_based_emails():
        return F.lit("ROLE_BASED")
    
    return F.lit("EXTERNAL_ALLOWED")

@emails.compose(emails=emails)
def email_intelligence_gathering():
    """Gather intelligence about email"""
    emails.standardize_email()
    
    if emails.is_valid_email():
        return F.struct(
            emails.extract_department_from_email().alias("department"),
            emails.extract_email_language_hint().alias("language"),
            emails.extract_email_vintage().alias("vintage"),
            emails.calculate_email_complexity_score().alias("complexity"),
            emails.guess_email_creation_year().alias("created_year"),
            emails.is_internal_email().alias("is_internal"),
            emails.is_partner_email().alias("is_partner"),
            emails.is_competitor_email().alias("is_competitor")
        )
    return None

@emails.compose(emails=emails)
def email_migration_pipeline():
    """Pipeline for email system migration"""
    # Standardize first
    emails.standardize_email()
    emails.fix_common_typos()
    
    # Handle mobile carrier emails
    emails.fix_mobile_carrier_emails()
    
    # Normalize unicode
    emails.normalize_unicode_email()
    
    # Check if needs corporate standardization
    if emails.is_internal_email():
        emails.standardize_corporate_email(company_domain="newcompany.com")
    
    # Validate final result
    if emails.is_valid_email():
        return emails.standardize_email()
    else:
        return None

# ============================================
# USAGE EXAMPLE WITH CUSTOM PRIMITIVES
# ============================================

def process_corporate_emails(spark):
    """Process emails with custom business logic"""
    
    df = spark.read.csv("corporate_emails.csv", header=True)
    
    # Apply security checks
    df = df.withColumn(
        "security_status",
        corporate_email_security_check(F.col("email"))
    )
    
    # Gather intelligence
    df = df.withColumn(
        "email_intel",
        email_intelligence_gathering(F.col("email"))
    )
    
    # Filter based on business rules
    df = df.withColumn(
        "email_filtered",
        emails.filter_suspicious_emails(
            emails.filter_role_based_emails(F.col("email"))
        )
    )
    
    # Check if corporate email
    df = df.withColumn(
        "is_our_company",
        emails.is_corporate_email(F.col("email"), "mycompany.com")
    )
    
    # Calculate complexity
    df = df.withColumn(
        "complexity_score",
        emails.calculate_email_complexity_score(F.col("email"))
    )
    
    # Categorize by vintage
    df = df.withColumn(
        "email_generation",
        emails.extract_email_vintage(F.col("email"))
    )
    
    # Extract department if internal
    df = df.withColumn(
        "department",
        F.when(
            F.col("is_our_company"),
            emails.extract_department_from_email(F.col("email"))
        )
    )
    
    return df

if __name__ == "__main__":
    spark = SparkSession.builder.appName("CustomEmailProcessing").getOrCreate()
    
    df_processed = process_corporate_emails(spark)
    
    # Analyze results
    df_processed.groupBy("security_status").count().show()
    df_processed.groupBy("email_generation").count().show()
    
    # Export high-complexity internal emails
    df_complex_internal = df_processed.filter(
        (F.col("is_our_company") == True) &
        (F.col("complexity_score") >= 0.8)
    )
    
    print(f"Complex internal emails: {df_complex_internal.count()}")
```

## Performance Considerations

1. **Use filter_valid_emails for bulk operations** - More efficient than is_valid_email + filter
2. **Cache intermediate results** when applying multiple transformations
3. **Standardize before deduplication** to catch variations
4. **Use columnar operations** - All primitives are optimized for Spark's columnar processing

## Common Patterns

### Email Validation with Fallback

```python
df = df.withColumn(
    "email_final",
    F.coalesce(
        emails.filter_valid_emails(
            emails.fix_common_typos(F.col("email"))
        ),
        emails.filter_valid_emails(
            emails.standardize_email(F.col("email_backup"))
        ),
        F.lit("no-email@unknown.com")
    )
)
```

### Conditional Processing

```python
df = df.withColumn(
    "email_processed",
    F.when(
        emails.is_disposable_email(F.col("email")),
        F.lit(None)  # Reject disposable emails
    ).when(
        emails.is_business_email(F.col("email")),
        emails.standardize_email(F.col("email"))  # Keep business emails
    ).otherwise(
        emails.normalize_gmail(F.col("email"))  # Normalize personal emails
    )
)
```

### Email Domain Analytics

```python
domain_stats = df.groupBy(
    emails.extract_domain(F.col("email")).alias("domain")
).agg(
    F.count("*").alias("user_count"),
    F.avg(F.length("email")).alias("avg_email_length"),
    F.first(emails.get_email_provider(F.col("email"))).alias("provider")
).orderBy(F.desc("user_count"))
```