# Phone Number Primitives

Comprehensive phone number validation, formatting, and standardization primitives for PySpark.

## Installation

Generate the phone number primitives for your project:

```bash
datacompose add clean_phone_numbers --target pyspark
```

## Import

```python
from build.clean_phone_numbers.phone_primitives import phones
from pyspark.sql import functions as F
```

## Validation Functions

### `is_valid_phone`

General phone number validation for any format.

```python
df = df.withColumn("is_valid", phones.is_valid_phone(F.col("phone")))
```

**Validates:**
- Minimum 7 digits (for local numbers)
- Maximum 15 digits (ITU-T E.164 standard)
- Valid characters only (digits, spaces, hyphens, parentheses, plus sign)

### `is_valid_nanp`

Validates North American Numbering Plan (US, Canada, Caribbean).

```python
df = df.withColumn("is_valid_nanp", phones.is_valid_nanp(F.col("phone")))
```

**NANP Rules:**
- 10 digits total (3-digit area code + 7-digit local number)
- Area code cannot start with 0 or 1
- Exchange code cannot start with 0 or 1
- Valid formats: (XXX) XXX-XXXX, XXX-XXX-XXXX, XXXXXXXXXX

**Examples:**
```python
"(555) 123-4567"        # ✅ Valid
"555-123-4567"          # ✅ Valid
"15551234567"           # ✅ Valid with country code
"123-4567"              # ❌ Missing area code
"(055) 123-4567"        # ❌ Invalid area code (starts with 0)
```

### `is_valid_international`

Validates international phone numbers (E.164 format).

```python
df = df.withColumn("is_valid_intl", phones.is_valid_international(F.col("phone")))
```

**E.164 Rules:**
- Starts with + and country code
- Maximum 15 digits total
- No formatting characters in strict mode

**Examples:**
```python
"+1-555-123-4567"       # ✅ US/Canada
"+44 20 7123 4567"      # ✅ UK
"+86 138 0000 0000"     # ✅ China
"+33 1 23 45 67 89"     # ✅ France
"555-123-4567"          # ❌ Missing country code
```

### `is_toll_free`

Checks if number is toll-free (US/Canada).

```python
df = df.withColumn("is_toll_free", phones.is_toll_free(F.col("phone")))
```

**Toll-free area codes:**
- 800, 833, 844, 855, 866, 877, 888

### `is_premium_rate`

Checks if number is premium rate (900 numbers).

```python
df = df.withColumn("is_premium", phones.is_premium_rate(F.col("phone")))
```

### `is_mobile`

Attempts to identify mobile numbers (US-based heuristics).

```python
df = df.withColumn("is_mobile", phones.is_mobile(F.col("phone")))
```

**Note:** Mobile detection is approximate and based on known mobile prefixes.

### `is_voip`

Attempts to identify VoIP numbers.

```python
df = df.withColumn("is_voip", phones.is_voip(F.col("phone")))
```

## Extraction Functions

### `extract_country_code`

Extracts the country calling code.

```python
df = df.withColumn("country_code", phones.extract_country_code(F.col("phone")))

# Input: "+1-555-123-4567"
# Output: "1"

# Input: "+44 20 7123 4567"
# Output: "44"
```

### `extract_area_code`

Extracts the area code (NANP numbers).

```python
df = df.withColumn("area_code", phones.extract_area_code(F.col("phone")))

# Input: "(555) 123-4567"
# Output: "555"

# Input: "555-123-4567"
# Output: "555"
```

### `extract_exchange`

Extracts the exchange code (first 3 digits of local number).

```python
df = df.withColumn("exchange", phones.extract_exchange(F.col("phone")))

# Input: "(555) 123-4567"
# Output: "123"
```

### `extract_subscriber`

Extracts the subscriber number (last 4 digits).

```python
df = df.withColumn("subscriber", phones.extract_subscriber(F.col("phone")))

# Input: "(555) 123-4567"
# Output: "4567"
```

### `extract_extension`

Extracts extension if present.

```python
df = df.withColumn("extension", phones.extract_extension(F.col("phone")))

# Input: "555-123-4567 ext 890"
# Output: "890"

# Input: "555-123-4567 x123"
# Output: "123"
```

### `extract_digits_only`

Removes all non-digit characters.

```python
df = df.withColumn("digits", phones.extract_digits_only(F.col("phone")))

# Input: "(555) 123-4567"
# Output: "5551234567"

# Input: "+1-555-123-4567"
# Output: "15551234567"
```

## Formatting Functions

### `format_nanp`

Formats NANP numbers in standard format.

```python
df = df.withColumn("formatted", phones.format_nanp(F.col("phone")))

# Input: "5551234567"
# Output: "(555) 123-4567"
```

### `format_e164`

Formats numbers in E.164 international format.

```python
df = df.withColumn("e164", phones.format_e164(F.col("phone"), country_code="1"))

# Input: "5551234567"
# Output: "+15551234567"

# Input: "(555) 123-4567"
# Output: "+15551234567"
```

### `format_international`

Formats with international dialing prefix.

```python
df = df.withColumn("intl", phones.format_international(F.col("phone")))

# Input: "5551234567" (US)
# Output: "+1 555 123 4567"

# Input: "442071234567" (UK)
# Output: "+44 20 7123 4567"
```

### `format_national`

Formats in national format (without country code).

```python
df = df.withColumn("national", phones.format_national(F.col("phone")))

# Input: "+15551234567"
# Output: "(555) 123-4567"

# Input: "15551234567"
# Output: "(555) 123-4567"
```

### `format_rfc3966`

Formats as tel: URI (RFC 3966).

```python
df = df.withColumn("tel_uri", phones.format_rfc3966(F.col("phone")))

# Input: "555-123-4567"
# Output: "tel:+1-555-123-4567"

# Input: "555-123-4567 ext 123"
# Output: "tel:+1-555-123-4567;ext=123"
```

### `format_dots`

Formats with dots as separators.

```python
df = df.withColumn("dots", phones.format_dots(F.col("phone")))

# Input: "5551234567"
# Output: "555.123.4567"
```

### `format_hyphens`

Formats with hyphens only.

```python
df = df.withColumn("hyphens", phones.format_hyphens(F.col("phone")))

# Input: "5551234567"
# Output: "555-123-4567"
```

## Standardization Functions

### `standardize_phone`

Applies comprehensive phone number standardization.

```python
df = df.withColumn("phone_clean", phones.standardize_phone(F.col("phone")))
```

**Performs:**
- Removes invalid characters
- Normalizes formatting
- Adds country code if missing (US/Canada assumed)
- Validates result

**Examples:**
```python
"(555) 123-4567"        → "+15551234567"
"555.123.4567"          → "+15551234567"
"1-555-123-4567"        → "+15551234567"
"555-1234" (local)      → null (invalid)
```

### `clean_phone`

Basic cleaning without validation.

```python
df = df.withColumn("phone_clean", phones.clean_phone(F.col("phone")))
```

**Performs:**
- Removes spaces, parentheses, hyphens
- Keeps only digits and plus sign
- Does not validate

### `normalize_international`

Normalizes international numbers to E.164.

```python
df = df.withColumn("normalized", phones.normalize_international(F.col("phone")))

# "001-555-123-4567"      → "+15551234567"
# "011 44 20 7123 4567"   → "+442071234567"
```

### `remove_country_code`

Removes country code from international numbers.

```python
df = df.withColumn("local", phones.remove_country_code(F.col("phone")))

# "+1-555-123-4567"       → "5551234567"
# "+44 20 7123 4567"      → "2071234567"
```

## Analysis Functions

### `get_phone_type`

Categorizes phone number type.

```python
df = df.withColumn("type", phones.get_phone_type(F.col("phone")))
```

**Returns:**
- "toll_free" - Toll-free numbers
- "premium" - Premium rate numbers
- "mobile" - Mobile numbers (approximate)
- "landline" - Landline numbers (approximate)
- "voip" - VoIP numbers (approximate)
- "unknown" - Cannot determine

### `get_carrier_info`

Attempts to identify carrier (requires lookup data).

```python
df = df.withColumn("carrier", phones.get_carrier_info(F.col("phone")))
```

**Note:** This requires additional carrier data to be effective.

### `get_geographic_area`

Gets geographic area for area code (US/Canada).

```python
df = df.withColumn("area", phones.get_geographic_area(F.col("area_code")))

# "212" → "New York, NY"
# "415" → "San Francisco, CA"
# "416" → "Toronto, ON"
```

### `get_timezone`

Estimates timezone based on area code.

```python
df = df.withColumn("timezone", phones.get_timezone(F.col("phone")))

# "212-xxx-xxxx" → "America/New_York"
# "415-xxx-xxxx" → "America/Los_Angeles"
```

## Comparison Functions

### `phones_match`

Checks if two phone numbers are the same (ignoring formatting).

```python
df = df.withColumn(
    "is_same",
    phones.phones_match(F.col("phone1"), F.col("phone2"))
)

# "(555) 123-4567" vs "555-123-4567" → True
# "+1-555-123-4567" vs "5551234567" → True
```

### `is_possible_number`

Checks if number could be valid for its region.

```python
df = df.withColumn("is_possible", phones.is_possible_number(F.col("phone")))
```

## Pipeline Examples Using @compose

The `@compose` decorator allows you to build powerful transformation pipelines by chaining primitives together. These composed functions must be defined at the module level (top level of your Python file).

### Complete Phone Cleaning Pipelines

```python
from build.clean_phone_numbers.phone_primitives import phones
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ============================================
# COMPOSED PIPELINE FUNCTIONS (Module Level)
# ============================================

@phones.compose(phones=phones)
def basic_phone_cleaning():
    """Basic phone number cleaning"""
    phones.clean_phone()
    if phones.is_valid_phone():
        phones.standardize_phone()
        if phones.is_valid_nanp():
            phones.format_nanp()

@phones.compose(phones=phones)
def validate_and_format_phone():
    """Validate and format phone numbers"""
    # Clean first
    phones.clean_phone()
    
    # Check validity
    if not phones.is_valid_phone():
        return F.lit(None)
    
    # Check for premium numbers
    if phones.is_premium_rate():
        return F.lit("PREMIUM_BLOCKED")
    
    # Check for toll-free
    if phones.is_toll_free():
        phones.format_nanp()
        return F.concat(F.lit("TOLL_FREE:"), phones.format_nanp())
    
    # Format based on type
    if phones.is_valid_international():
        return phones.format_e164()
    elif phones.is_valid_nanp():
        return phones.format_e164(country_code="1")
    else:
        return phones.standardize_phone()

@phones.compose(phones=phones)
def extract_phone_components():
    """Extract all phone number components"""
    phones.clean_phone()
    
    if not phones.is_valid_phone():
        return F.lit(None)
    
    # Extract components
    country = phones.extract_country_code()
    area = phones.extract_area_code()
    exchange = phones.extract_exchange()
    subscriber = phones.extract_subscriber()
    extension = phones.extract_extension()
    
    return F.struct(
        F.lit(country).alias("country_code"),
        F.lit(area).alias("area_code"),
        F.lit(exchange).alias("exchange"),
        F.lit(subscriber).alias("subscriber"),
        F.lit(extension).alias("extension")
    )

@phones.compose(phones=phones)
def mobile_phone_pipeline():
    """Process mobile phone numbers"""
    phones.clean_phone()
    phones.standardize_phone()
    
    if not phones.is_valid_nanp():
        return F.lit("NOT_NANP")
    
    # Check if mobile
    if phones.is_mobile():
        # Format for SMS
        formatted = phones.format_e164(country_code="1")
        return F.struct(
            F.lit(True).alias("is_mobile"),
            F.lit(formatted).alias("sms_number"),
            F.lit("SMS_CAPABLE").alias("status")
        )
    elif phones.is_voip():
        return F.struct(
            F.lit(False).alias("is_mobile"),
            F.lit(None).alias("sms_number"),
            F.lit("VOIP_UNCERTAIN").alias("status")
        )
    else:
        return F.struct(
            F.lit(False).alias("is_mobile"),
            F.lit(None).alias("sms_number"),
            F.lit("LANDLINE_LIKELY").alias("status")
        )

@phones.compose(phones=phones)
def phone_deduplication_key():
    """Generate deduplication key for phones"""
    phones.clean_phone()
    
    # Extract just digits
    digits = phones.extract_digits_only()
    
    # For NANP, use last 10 digits
    if phones.is_valid_nanp():
        # Remove country code if present
        if F.length(digits) == 11 and digits.startswith("1"):
            return F.substring(digits, 2, 10)
        elif F.length(digits) == 10:
            return digits
    
    # For international, use E.164
    if phones.is_valid_international():
        return phones.format_e164()
    
    # Default to digits
    return digits

@phones.compose(phones=phones)
def phone_quality_score():
    """Calculate phone quality score"""
    phones.clean_phone()
    
    # Invalid phone
    if not phones.is_valid_phone():
        return F.lit(0.0)
    
    # Premium rate - low quality
    if phones.is_premium_rate():
        return F.lit(0.1)
    
    # Toll free - medium quality
    if phones.is_toll_free():
        return F.lit(0.5)
    
    # Check for mobile (high quality for most use cases)
    if phones.is_valid_nanp():
        if phones.is_mobile():
            return F.lit(1.0)
        else:
            return F.lit(0.8)  # Landline
    
    # International
    if phones.is_valid_international():
        return F.lit(0.7)
    
    # Valid but unknown type
    return F.lit(0.4)

@phones.compose(phones=phones)
def format_for_display():
    """Format phone for user display"""
    phones.clean_phone()
    
    if not phones.is_valid_phone():
        return F.lit("Invalid")
    
    # Format based on type
    if phones.is_valid_nanp():
        # Check for extension
        ext = phones.extract_extension()
        formatted = phones.format_nanp()
        
        if ext:
            return F.concat(formatted, F.lit(" ext "), F.lit(ext))
        else:
            return formatted
    elif phones.is_valid_international():
        return phones.format_international()
    else:
        return phones.standardize_phone()

@phones.compose(phones=phones)
def sms_eligibility_check():
    """Check if phone is eligible for SMS"""
    phones.clean_phone()
    phones.standardize_phone()
    
    # Must be valid
    if not phones.is_valid_phone():
        return F.struct(
            F.lit(False).alias("eligible"),
            F.lit("INVALID_NUMBER").alias("reason")
        )
    
    # Must be NANP for US SMS
    if not phones.is_valid_nanp():
        return F.struct(
            F.lit(False).alias("eligible"),
            F.lit("NOT_US_NUMBER").alias("reason")
        )
    
    # Cannot be toll-free
    if phones.is_toll_free():
        return F.struct(
            F.lit(False).alias("eligible"),
            F.lit("TOLL_FREE_NO_SMS").alias("reason")
        )
    
    # Cannot be premium
    if phones.is_premium_rate():
        return F.struct(
            F.lit(False).alias("eligible"),
            F.lit("PREMIUM_BLOCKED").alias("reason")
        )
    
    # Check if mobile
    if phones.is_mobile():
        return F.struct(
            F.lit(True).alias("eligible"),
            F.lit("MOBILE_VERIFIED").alias("reason")
        )
    else:
        # Landline - may not support SMS
        return F.struct(
            F.lit(False).alias("eligible"),
            F.lit("LANDLINE_NO_SMS").alias("reason")
        )

@phones.compose(phones=phones)
def international_dialing_format():
    """Format for international dialing from US"""
    phones.clean_phone()
    
    if not phones.is_valid_phone():
        return F.lit(None)
    
    # Check if US/Canada number
    if phones.is_valid_nanp():
        # No international prefix needed
        return phones.format_nanp()
    
    # International number - add US exit code
    if phones.is_valid_international():
        formatted = phones.format_international()
        return F.concat(F.lit("011 "), formatted)
    
    return phones.standardize_phone()

# ============================================
# ADVANCED COMPOSED PIPELINES  
# ============================================

@phones.compose(phones=phones)
def phone_enrichment_pipeline():
    """Comprehensive phone enrichment"""
    phones.clean_phone()
    
    if not phones.is_valid_phone():
        return F.lit(None)
    
    # Standardize
    phones.standardize_phone()
    
    # Determine type
    if phones.is_toll_free():
        phone_type = "toll_free"
    elif phones.is_premium_rate():
        phone_type = "premium"
    elif phones.is_mobile():
        phone_type = "mobile"
    elif phones.is_voip():
        phone_type = "voip"
    else:
        phone_type = "landline"
    
    # Extract components
    components = extract_phone_components()
    
    # Get geographic info for NANP
    if phones.is_valid_nanp():
        area_code = phones.extract_area_code()
        geographic_area = phones.get_geographic_area()
        timezone = phones.get_timezone()
    else:
        geographic_area = None
        timezone = None
    
    # Format in multiple ways
    formats = F.struct(
        phones.format_e164().alias("e164"),
        phones.format_national().alias("national"),
        phones.format_international().alias("international"),
        phones.format_rfc3966().alias("tel_uri")
    )
    
    return F.struct(
        F.lit(phone_type).alias("type"),
        components.alias("components"),
        F.lit(geographic_area).alias("geographic_area"),
        F.lit(timezone).alias("timezone"),
        formats.alias("formats"),
        phones.is_possible_number().alias("is_possible")
    )

@phones.compose(phones=phones)
def call_routing_pipeline():
    """Determine call routing priority"""
    phones.clean_phone()
    phones.standardize_phone()
    
    if not phones.is_valid_phone():
        return F.struct(
            F.lit(0).alias("priority"),
            F.lit("INVALID").alias("route"),
            F.lit(False).alias("callable")
        )
    
    # Premium numbers - do not call
    if phones.is_premium_rate():
        return F.struct(
            F.lit(0).alias("priority"),
            F.lit("BLOCKED").alias("route"),
            F.lit(False).alias("callable")
        )
    
    # Toll-free - low priority
    if phones.is_toll_free():
        return F.struct(
            F.lit(1).alias("priority"),
            F.lit("TOLL_FREE").alias("route"),
            F.lit(True).alias("callable")
        )
    
    # Mobile - high priority
    if phones.is_mobile():
        return F.struct(
            F.lit(3).alias("priority"),
            F.lit("MOBILE").alias("route"),
            F.lit(True).alias("callable")
        )
    
    # Landline - medium priority
    if phones.is_valid_nanp():
        return F.struct(
            F.lit(2).alias("priority"),
            F.lit("LANDLINE").alias("route"),
            F.lit(True).alias("callable")
        )
    
    # International - depends on business rules
    if phones.is_valid_international():
        return F.struct(
            F.lit(1).alias("priority"),
            F.lit("INTERNATIONAL").alias("route"),
            F.lit(True).alias("callable")
        )
    
    return F.struct(
        F.lit(0).alias("priority"),
        F.lit("UNKNOWN").alias("route"),
        F.lit(False).alias("callable")
    )

@phones.compose(phones=phones)
def phone_anonymization():
    """Anonymize phone while preserving area code"""
    phones.clean_phone()
    
    if not phones.is_valid_phone():
        return F.lit("XXX-XXX-XXXX")
    
    if phones.is_valid_nanp():
        area = phones.extract_area_code()
        return F.concat(F.lit("("), F.lit(area), F.lit(") XXX-XXXX"))
    else:
        # International - keep country code
        country = phones.extract_country_code()
        return F.concat(F.lit("+"), F.lit(country), F.lit(" XXXX-XXXX"))

# ============================================
# USAGE IN APPLICATION
# ============================================

def process_customer_phones(spark):
    """Main processing function using composed pipelines"""
    
    # Load data
    df = spark.read.csv("customer_phones.csv", header=True)
    
    # Basic cleaning
    df = df.withColumn(
        "phone_clean",
        basic_phone_cleaning(F.col("phone"))
    )
    
    # Validate and format
    df = df.withColumn(
        "phone_formatted",
        validate_and_format_phone(F.col("phone"))
    )
    
    # Extract components
    df = df.withColumn(
        "phone_components",
        extract_phone_components(F.col("phone"))
    )
    
    # Process mobile phones
    df = df.withColumn(
        "mobile_info",
        mobile_phone_pipeline(F.col("phone"))
    )
    
    # Generate dedup key
    df = df.withColumn(
        "dedup_key",
        phone_deduplication_key(F.col("phone"))
    )
    
    # Calculate quality score
    df = df.withColumn(
        "quality_score",
        phone_quality_score(F.col("phone"))
    )
    
    # Format for display
    df = df.withColumn(
        "display_format",
        format_for_display(F.col("phone"))
    )
    
    # Check SMS eligibility
    df = df.withColumn(
        "sms_check",
        sms_eligibility_check(F.col("phone"))
    )
    
    # International dialing
    df = df.withColumn(
        "intl_dial_format",
        international_dialing_format(F.col("phone"))
    )
    
    # Full enrichment
    df = df.withColumn(
        "phone_enriched",
        phone_enrichment_pipeline(F.col("phone"))
    )
    
    # Call routing
    df = df.withColumn(
        "call_routing",
        call_routing_pipeline(F.col("phone"))
    )
    
    # Anonymize for privacy
    df = df.withColumn(
        "phone_anonymized",
        phone_anonymization(F.col("phone"))
    )
    
    return df

# ============================================
# USAGE EXAMPLE
# ============================================

if __name__ == "__main__":
    # Initialize Spark
    spark = SparkSession.builder.appName("PhoneProcessing").getOrCreate()
    
    # Process phones
    df_processed = process_customer_phones(spark)
    
    # Show results
    df_processed.select(
        "phone",
        "phone_formatted",
        "quality_score",
        "mobile_info.status",
        "sms_check.eligible",
        "call_routing.priority",
        "phone_enriched.type",
        "phone_enriched.timezone"
    ).show(20, truncate=False)
    
    # Deduplicate
    df_unique = df_processed.dropDuplicates(["dedup_key"])
    
    # Filter SMS-capable phones
    df_sms = df_processed.filter(
        F.col("sms_check.eligible") == True
    )
    
    # Filter high-priority call targets
    df_calls = df_processed.filter(
        (F.col("call_routing.callable") == True) &
        (F.col("call_routing.priority") >= 2)
    )
    
    # Export mobile numbers for SMS campaign
    df_sms_campaign = df_processed.filter(
        F.col("mobile_info.status") == "SMS_CAPABLE"
    ).select(
        "customer_id",
        F.col("mobile_info.sms_number").alias("sms_number"),
        "phone_enriched.timezone"
    )
    
    print(f"Total phones: {df_processed.count()}")
    print(f"Unique phones: {df_unique.count()}")
    print(f"SMS capable: {df_sms.count()}")
    print(f"High priority calls: {df_calls.count()}")

# Method 1: Comprehensive step-by-step pipeline (without @compose)
def clean_phones_step_by_step(df):
    """
    Complete phone cleaning with detailed transformations
    """
    return (
        df
        # Step 1: Extract digits only for initial processing
        .withColumn("phone_digits", phones.extract_digits_only(F.col("phone")))
        
        # Step 2: Determine phone length and potential country
        .withColumn("digit_count", F.length(F.col("phone_digits")))
        
        # Step 3: Check for country code
        .withColumn(
            "has_country_code",
            (F.col("phone").startswith("+")) | 
            (F.col("digit_count") > 10)
        )
        .withColumn(
            "country_code",
            F.when(
                F.col("has_country_code"),
                phones.extract_country_code(F.col("phone"))
            ).otherwise("1")  # Default to US/Canada
        )
        
        # Step 4: Validate different formats
        .withColumn("is_valid_nanp", phones.is_valid_nanp(F.col("phone")))
        .withColumn("is_valid_intl", phones.is_valid_international(F.col("phone")))
        .withColumn("is_valid_general", phones.is_valid_phone(F.col("phone")))
        
        # Step 5: Check special number types
        .withColumn("is_toll_free", phones.is_toll_free(F.col("phone")))
        .withColumn("is_premium", phones.is_premium_rate(F.col("phone")))
        .withColumn("is_mobile_likely", phones.is_mobile(F.col("phone")))
        .withColumn("is_voip_likely", phones.is_voip(F.col("phone")))
        
        # Step 6: Extract components for NANP numbers
        .withColumn(
            "area_code",
            F.when(
                F.col("is_valid_nanp"),
                phones.extract_area_code(F.col("phone"))
            )
        )
        .withColumn(
            "exchange",
            F.when(
                F.col("is_valid_nanp"),
                phones.extract_exchange(F.col("phone"))
            )
        )
        .withColumn(
            "subscriber",
            F.when(
                F.col("is_valid_nanp"),
                phones.extract_subscriber(F.col("phone"))
            )
        )
        
        # Step 7: Extract extension if present
        .withColumn("extension", phones.extract_extension(F.col("phone")))
        
        # Step 8: Standardize valid phones
        .withColumn(
            "phone_standardized",
            F.when(
                F.col("is_valid_general"),
                phones.standardize_phone(F.col("phone"))
            ).otherwise(None)
        )
        
        # Step 9: Format in multiple ways
        .withColumn(
            "formatted_nanp",
            F.when(
                F.col("is_valid_nanp"),
                phones.format_nanp(F.col("phone"))
            )
        )
        .withColumn(
            "formatted_e164",
            F.when(
                F.col("is_valid_nanp"),
                phones.format_e164(F.col("phone"), country_code="1")
            ).when(
                F.col("is_valid_intl"),
                phones.format_e164(F.col("phone"))
            )
        )
        .withColumn(
            "formatted_intl",
            F.when(
                F.col("is_valid_general"),
                phones.format_international(F.col("phone"))
            )
        )
        
        # Step 10: Get phone metadata
        .withColumn("phone_type", phones.get_phone_type(F.col("phone")))
        .withColumn(
            "geographic_area",
            F.when(
                F.col("area_code").isNotNull(),
                phones.get_geographic_area(F.col("area_code"))
            )
        )
        .withColumn(
            "timezone",
            F.when(
                F.col("is_valid_nanp"),
                phones.get_timezone(F.col("phone"))
            )
        )
        
        # Step 11: Create quality score
        .withColumn(
            "phone_quality_score",
            F.when(~F.col("is_valid_general"), 0)
            .when(F.col("is_premium"), 0.2)
            .when(F.col("is_toll_free"), 0.5)
            .when(F.col("is_valid_intl") & ~F.col("is_valid_nanp"), 0.7)
            .when(F.col("is_valid_nanp") & F.col("is_mobile_likely"), 1.0)
            .when(F.col("is_valid_nanp"), 0.9)
            .otherwise(0.3)
        )
    )

# Method 2: Production Phone Processing Pipeline
class PhoneProcessingPipeline:
    """
    Production-ready phone number processing pipeline
    """
    
    def __init__(self, df, default_country="US"):
        self.df = df
        self.phones = phones
        self.default_country = default_country
    
    def process(self):
        """Main processing pipeline"""
        self.df = (
            self.df
            .transform(self.clean_phones)
            .transform(self.validate_phones)
            .transform(self.extract_components)
            .transform(self.format_phones)
            .transform(self.enrich_phones)
            .transform(self.categorize_phones)
            .transform(self.deduplicate_phones)
        )
        return self.df
    
    def clean_phones(self, df):
        """Initial cleaning step"""
        return (
            df
            # Remove common prefixes
            .withColumn(
                "phone_cleaned",
                F.regexp_replace(
                    F.col("phone"),
                    r"^(tel:|phone:|mobile:|cell:)",
                    ""
                )
            )
            # Basic cleaning
            .withColumn(
                "phone_basic_clean",
                self.phones.clean_phone(F.col("phone_cleaned"))
            )
            # Extract just digits for analysis
            .withColumn(
                "digits_only",
                self.phones.extract_digits_only(F.col("phone_cleaned"))
            )
            .withColumn(
                "digit_count",
                F.length(F.col("digits_only"))
            )
        )
    
    def validate_phones(self, df):
        """Validation step"""
        return (
            df
            .withColumn("validation", F.struct(
                self.phones.is_valid_phone(F.col("phone_basic_clean")).alias("is_valid"),
                self.phones.is_valid_nanp(F.col("phone_basic_clean")).alias("is_nanp"),
                self.phones.is_valid_international(F.col("phone_basic_clean")).alias("is_intl"),
                self.phones.is_toll_free(F.col("phone_basic_clean")).alias("is_toll_free"),
                self.phones.is_premium_rate(F.col("phone_basic_clean")).alias("is_premium"),
                self.phones.is_possible_number(F.col("phone_basic_clean")).alias("is_possible")
            ))
            # Determine best validation status
            .withColumn(
                "validation_status",
                F.when(~F.col("validation.is_valid"), "invalid")
                .when(F.col("validation.is_premium"), "premium_blocked")
                .when(F.col("validation.is_toll_free"), "toll_free")
                .when(F.col("validation.is_nanp"), "valid_nanp")
                .when(F.col("validation.is_intl"), "valid_international")
                .when(F.col("validation.is_possible"), "possibly_valid")
                .otherwise("unknown")
            )
        )
    
    def extract_components(self, df):
        """Component extraction step"""
        return (
            df
            .withColumn("components", F.struct(
                F.when(
                    F.col("validation.is_intl") | (F.col("digit_count") > 10),
                    self.phones.extract_country_code(F.col("phone_basic_clean"))
                ).otherwise("1").alias("country_code"),
                
                F.when(
                    F.col("validation.is_nanp"),
                    self.phones.extract_area_code(F.col("phone_basic_clean"))
                ).alias("area_code"),
                
                F.when(
                    F.col("validation.is_nanp"),
                    self.phones.extract_exchange(F.col("phone_basic_clean"))
                ).alias("exchange"),
                
                F.when(
                    F.col("validation.is_nanp"),
                    self.phones.extract_subscriber(F.col("phone_basic_clean"))
                ).alias("subscriber"),
                
                self.phones.extract_extension(F.col("phone_cleaned")).alias("extension")
            ))
        )
    
    def format_phones(self, df):
        """Formatting step - multiple formats for different uses"""
        return (
            df
            # E.164 format for systems
            .withColumn(
                "format_e164",
                F.when(
                    F.col("validation.is_nanp"),
                    self.phones.format_e164(
                        F.col("phone_basic_clean"),
                        country_code="1"
                    )
                ).when(
                    F.col("validation.is_intl"),
                    self.phones.format_e164(F.col("phone_basic_clean"))
                )
            )
            
            # National format for display
            .withColumn(
                "format_national",
                F.when(
                    F.col("validation.is_nanp"),
                    self.phones.format_nanp(F.col("phone_basic_clean"))
                ).when(
                    F.col("validation.is_intl"),
                    self.phones.format_national(F.col("phone_basic_clean"))
                )
            )
            
            # International format for global display
            .withColumn(
                "format_international",
                F.when(
                    F.col("validation.is_valid"),
                    self.phones.format_international(F.col("phone_basic_clean"))
                )
            )
            
            # RFC 3966 tel: URI format
            .withColumn(
                "format_tel_uri",
                F.when(
                    F.col("validation.is_valid"),
                    self.phones.format_rfc3966(F.col("phone_basic_clean"))
                )
            )
            
            # Clickable format for web
            .withColumn(
                "format_clickable",
                F.when(
                    F.col("validation.is_valid"),
                    F.concat(
                        F.lit("<a href='"),
                        self.phones.format_rfc3966(F.col("phone_basic_clean")),
                        F.lit("'>"),
                        F.col("format_national"),
                        F.lit("</a>")
                    )
                )
            )
        )
    
    def enrich_phones(self, df):
        """Enrich with additional data"""
        return (
            df
            .withColumn("enrichment", F.struct(
                self.phones.get_phone_type(F.col("phone_basic_clean")).alias("type"),
                
                F.when(
                    F.col("components.area_code").isNotNull(),
                    self.phones.get_geographic_area(F.col("components.area_code"))
                ).alias("geographic_area"),
                
                F.when(
                    F.col("validation.is_nanp"),
                    self.phones.get_timezone(F.col("phone_basic_clean"))
                ).alias("timezone"),
                
                self.phones.is_mobile(F.col("phone_basic_clean")).alias("is_mobile_likely"),
                self.phones.is_voip(F.col("phone_basic_clean")).alias("is_voip_likely")
            ))
        )
    
    def categorize_phones(self, df):
        """Categorize phones for business logic"""
        return (
            df
            .withColumn(
                "category",
                F.when(~F.col("validation.is_valid"), "invalid")
                .when(F.col("validation.is_premium"), "premium")
                .when(F.col("validation.is_toll_free"), "toll_free")
                .when(F.col("enrichment.is_mobile_likely"), "mobile")
                .when(F.col("enrichment.is_voip_likely"), "voip")
                .when(F.col("validation.is_nanp"), "landline_likely")
                .when(F.col("validation.is_intl"), "international")
                .otherwise("unknown")
            )
            
            # SMS capability flag
            .withColumn(
                "sms_capable",
                F.col("enrichment.is_mobile_likely") & 
                F.col("validation.is_nanp") &
                ~F.col("validation.is_toll_free")
            )
            
            # Call priority
            .withColumn(
                "call_priority",
                F.when(~F.col("validation.is_valid"), 0)
                .when(F.col("validation.is_premium"), 0)
                .when(F.col("enrichment.is_mobile_likely"), 3)
                .when(F.col("validation.is_toll_free"), 1)
                .when(F.col("validation.is_nanp"), 2)
                .otherwise(1)
            )
        )
    
    def deduplicate_phones(self, df):
        """Deduplicate based on E.164 format"""
        return (
            df
            .withColumn(
                "dedup_key",
                F.coalesce(
                    F.col("format_e164"),
                    F.col("digits_only")
                )
            )
            # Keep the best version of each phone number
            .withColumn(
                "row_priority",
                F.row_number().over(
                    Window.partitionBy("dedup_key")
                    .orderBy(F.desc("call_priority"), F.desc("validation.is_valid"))
                )
            )
            .filter(F.col("row_priority") == 1)
            .drop("row_priority")
        )
    
    def get_statistics(self):
        """Generate statistics about processed phones"""
        return {
            "total": self.df.count(),
            "by_category": self.df.groupBy("category").count().collect(),
            "by_validation": self.df.groupBy("validation_status").count().collect(),
            "sms_capable": self.df.filter(F.col("sms_capable")).count(),
            "area_code_distribution": (
                self.df
                .groupBy("components.area_code")
                .count()
                .orderBy(F.desc("count"))
                .limit(10)
                .collect()
            )
        }

# Method 3: Phone matching and verification pipeline
class PhoneMatchingPipeline:
    """
    Pipeline for matching phones across datasets
    """
    
    def __init__(self, df):
        self.df = df
        self.phones = phones
    
    def prepare_for_matching(self):
        """Prepare phones for matching"""
        return (
            self.df
            # Create multiple match keys
            .withColumn(
                "match_e164",
                F.when(
                    self.phones.is_valid_phone(F.col("phone")),
                    self.phones.format_e164(
                        self.phones.standardize_phone(F.col("phone")),
                        country_code="1"
                    )
                )
            )
            .withColumn(
                "match_digits",
                self.phones.extract_digits_only(F.col("phone"))
            )
            .withColumn(
                "match_last_10",
                F.substring(
                    self.phones.extract_digits_only(F.col("phone")),
                    -10, 10
                )
            )
            .withColumn(
                "match_last_7",
                F.substring(
                    self.phones.extract_digits_only(F.col("phone")),
                    -7, 7
                )
            )
        )
    
    def find_duplicates(self):
        """Find duplicate phone numbers"""
        df_prepared = self.prepare_for_matching()
        
        # Find exact duplicates (same E.164)
        exact_dupes = (
            df_prepared
            .groupBy("match_e164")
            .agg(F.count("*").alias("count"), F.collect_list("id").alias("ids"))
            .filter(F.col("count") > 1)
        )
        
        # Find potential duplicates (same last 10 digits)
        potential_dupes = (
            df_prepared
            .groupBy("match_last_10")
            .agg(F.count("*").alias("count"), F.collect_list("id").alias("ids"))
            .filter(F.col("count") > 1)
        )
        
        return exact_dupes, potential_dupes

# Usage examples
# Basic pipeline
df_cleaned = clean_phones_step_by_step(df)

# Advanced pipeline
pipeline = PhoneProcessingPipeline(df)
df_processed = pipeline.process()
stats = pipeline.get_statistics()

# Matching pipeline
matcher = PhoneMatchingPipeline(df)
exact_dupes, potential_dupes = matcher.find_duplicates()

# Display results
df_processed.select(
    "phone",
    "validation_status",
    "category",
    "format_national",
    "format_e164",
    "sms_capable",
    "enrichment.timezone"
).show(20, truncate=False)
```

### Phone Deduplication

```python
# Deduplicate phone numbers regardless of format
df_dedup = df.withColumn(
    "phone_key",
    phones.extract_digits_only(phones.standardize_phone(F.col("phone")))
).dropDuplicates(["phone_key"])
```

### Contact Validation

```python
# Validate and categorize contact numbers
df_contacts = df.withColumn(
    "phone_status",
    F.when(
        phones.is_toll_free(F.col("phone")),
        F.lit("toll_free")
    ).when(
        phones.is_premium_rate(F.col("phone")),
        F.lit("premium_blocked")
    ).when(
        phones.is_valid_nanp(F.col("phone")),
        F.lit("valid_us")
    ).when(
        phones.is_valid_international(F.col("phone")),
        F.lit("valid_international")
    ).otherwise(
        F.lit("invalid")
    )
).filter(F.col("phone_status") != "premium_blocked")
```

### SMS Campaign Preparation

```python
# Prepare phone numbers for SMS campaign
df_sms = df.filter(
    phones.is_valid_nanp(F.col("phone")) &
    ~phones.is_toll_free(F.col("phone")) &
    ~phones.is_premium_rate(F.col("phone"))
).withColumn(
    "sms_number",
    phones.format_e164(phones.clean_phone(F.col("phone")), country_code="1")
).withColumn(
    "is_mobile_likely",
    phones.is_mobile(F.col("phone"))
).select("customer_id", "sms_number", "is_mobile_likely")
```

### International Dialing

```python
# Format numbers for international dialing from US
df_intl = df.withColumn(
    "dial_string",
    F.when(
        phones.is_valid_nanp(F.col("phone")),
        phones.format_national(F.col("phone"))  # No intl prefix needed
    ).otherwise(
        F.concat(F.lit("011 "), phones.format_international(F.col("phone")))
    )
)
```

## Data Quality Reporting

```python
# Generate phone data quality metrics
quality_report = df.agg(
    F.count("phone").alias("total_phones"),
    F.sum(phones.is_valid_phone(F.col("phone")).cast("int")).alias("valid_count"),
    F.sum(phones.is_valid_nanp(F.col("phone")).cast("int")).alias("nanp_count"),
    F.sum(phones.is_valid_international(F.col("phone")).cast("int")).alias("intl_count"),
    F.sum(phones.is_toll_free(F.col("phone")).cast("int")).alias("toll_free_count"),
    F.sum(phones.is_mobile(F.col("phone")).cast("int")).alias("mobile_count"),
    F.countDistinct(phones.extract_area_code(F.col("phone"))).alias("unique_area_codes")
)
```

## Advanced: Creating Custom Phone Primitives

You can extend the phone primitives library with your own custom functions tailored to your specific business needs.

### Setting Up Custom Phone Primitives

```python
from build.clean_phone_numbers.phone_primitives import phones
from pyspark.sql import functions as F
import re

# ============================================
# CUSTOM PHONE PRIMITIVES
# ============================================

# Add custom validation functions
@phones.register()
def is_vanity_number(col):
    """Check if phone number is a vanity number (contains letters)"""
    return F.regexp_extract(col, r'[A-Za-z]{3,}', 0).isNotNull()

@phones.register()
def is_short_code(col):
    """Check if number is a short code (3-6 digits)"""
    digits = phones.extract_digits_only(col)
    length = F.length(digits)
    return (length >= 3) & (length <= 6)

@phones.register()
def is_emergency_number(col):
    """Check if number is an emergency service"""
    emergency_numbers = ["911", "112", "999", "000", "110", "119"]
    digits = phones.extract_digits_only(col)
    return digits.isin(emergency_numbers)

@phones.register()
def is_premium_short_code(col):
    """Check if number is a premium SMS short code"""
    if phones.is_short_code(col):
        digits = phones.extract_digits_only(col)
        # Premium short codes often start with certain prefixes
        return digits.rlike(r'^(900|976|809|909)')
    return F.lit(False)

@phones.register()
def is_conference_number(col):
    """Check if number is a conference call number"""
    conf_patterns = [
        r'(866|877|888).*?(MEET|CONF|CALL)',  # Vanity conference numbers
        r'(\+1)?\s*?(605|712|218)\s*?\d{3}\s*?\d{4}'  # Common conference bridges
    ]
    
    is_conf = F.lit(False)
    for pattern in conf_patterns:
        is_conf = is_conf | F.upper(col).rlike(pattern)
    return is_conf

@phones.register()
def is_internal_extension(col):
    """Check if number is an internal extension"""
    digits = phones.extract_digits_only(col)
    # Internal extensions are typically 3-5 digits
    return (F.length(digits) >= 3) & (F.length(digits) <= 5) & ~col.contains("+")

# Add custom extraction functions
@phones.register()
def extract_vanity_letters(col):
    """Extract vanity letters from phone number"""
    return F.regexp_extract(col, r'([A-Za-z]{3,})', 1)

@phones.register()
def extract_carrier_from_area_code(col):
    """Guess carrier from area code patterns"""
    area_code = phones.extract_area_code(col)
    
    # Simplified carrier mapping (in reality, this would be more complex)
    carrier_map = {
        "312": "AT&T",
        "415": "Verizon",
        "917": "T-Mobile",
        "646": "Sprint",
        # Add more mappings
    }
    
    # Create case statement
    carrier = F.lit("Unknown")
    for code, provider in carrier_map.items():
        carrier = F.when(area_code == code, provider).otherwise(carrier)
    
    return carrier

@phones.register()
def extract_country_name(col):
    """Extract country name from international number"""
    country_code = phones.extract_country_code(col)
    
    country_map = {
        "1": "USA/Canada",
        "44": "United Kingdom",
        "33": "France",
        "49": "Germany",
        "81": "Japan",
        "86": "China",
        "91": "India",
        "61": "Australia",
        "7": "Russia",
        "52": "Mexico"
    }
    
    country = F.lit("Unknown")
    for code, name in country_map.items():
        country = F.when(country_code == code, name).otherwise(country)
    
    return country

@phones.register()
def extract_time_zone_offset(col):
    """Get timezone offset from phone number"""
    if phones.is_valid_nanp(col):
        area_code = phones.extract_area_code(col)
        
        # Simplified timezone mapping
        tz_map = {
            # Eastern
            "212": "-5", "347": "-5", "646": "-5", "917": "-5",
            # Central
            "312": "-6", "773": "-6", "214": "-6", "469": "-6",
            # Mountain
            "303": "-7", "720": "-7", "602": "-7",
            # Pacific
            "213": "-8", "310": "-8", "415": "-8", "650": "-8"
        }
        
        offset = F.lit(None)
        for code, tz in tz_map.items():
            offset = F.when(area_code == code, tz).otherwise(offset)
        
        return offset
    return None

# Add custom formatting functions
@phones.register()
def format_vanity_number(col):
    """Format vanity numbers properly"""
    if phones.is_vanity_number(col):
        # Convert letters to numbers for actual dialing
        letter_to_num = {
            'ABC': '2', 'DEF': '3', 'GHI': '4', 'JKL': '5',
            'MNO': '6', 'PQRS': '7', 'TUV': '8', 'WXYZ': '9'
        }
        
        result = col
        for letters, num in letter_to_num.items():
            for letter in letters:
                result = F.regexp_replace(result, letter, num)
                result = F.regexp_replace(result, letter.lower(), num)
        
        return result
    return col

@phones.register()
def format_with_country_flag(col):
    """Add country flag emoji to international numbers"""
    country_code = phones.extract_country_code(col)
    
    flag_map = {
        "1": "🇺🇸",
        "44": "🇬🇧",
        "33": "🇫🇷",
        "49": "🇩🇪",
        "81": "🇯🇵",
        "86": "🇨🇳",
        "91": "🇮🇳",
        "61": "🇦🇺"
    }
    
    formatted = phones.format_international(col)
    for code, flag in flag_map.items():
        if country_code == code:
            return F.concat(F.lit(flag), F.lit(" "), formatted)
    
    return formatted

@phones.register()
def format_for_whatsapp(col):
    """Format phone for WhatsApp link"""
    if phones.is_valid_phone(col):
        # WhatsApp uses E.164 without the +
        e164 = phones.format_e164(col)
        digits = F.regexp_replace(e164, r'[^0-9]', '')
        return F.concat(F.lit("https://wa.me/"), digits)
    return None

@phones.register()
def format_for_sip(col):
    """Format phone for SIP URI"""
    if phones.is_valid_phone(col):
        e164 = phones.format_e164(col)
        return F.concat(F.lit("sip:"), e164, F.lit("@voip.company.com"))
    return None

# Add custom analysis functions
@phones.register()
def calculate_call_cost_estimate(col, rate_per_minute=0.10):
    """Estimate call cost based on number type"""
    # Check number type and assign rates
    if phones.is_toll_free(col):
        return F.lit(0.0)  # Free
    elif phones.is_premium_rate(col):
        return F.lit(2.99)  # Premium per minute
    elif phones.is_valid_international(col) and not phones.is_valid_nanp(col):
        return F.lit(0.50)  # International rate
    elif phones.is_valid_nanp(col):
        if phones.is_mobile(col):
            return F.lit(0.15)  # Mobile rate
        else:
            return F.lit(rate_per_minute)  # Standard rate
    else:
        return None

@phones.register()
def estimate_response_rate(col):
    """Estimate response rate for marketing"""
    # Based on phone type, estimate likelihood of response
    if phones.is_mobile(col):
        base_rate = 0.15  # 15% for mobile
    elif phones.is_toll_free(col):
        base_rate = 0.05  # 5% for toll-free
    elif phones.is_voip(col):
        base_rate = 0.08  # 8% for VoIP
    else:
        base_rate = 0.10  # 10% for landline
    
    # Adjust based on time zone (simplified)
    if phones.is_valid_nanp(col):
        area_code = phones.extract_area_code(col)
        # West coast area codes might have different response rates
        if area_code in ["415", "650", "408", "310", "213"]:
            base_rate *= 1.2  # 20% higher in tech hubs
    
    return F.lit(base_rate)

@phones.register()
def calculate_phone_age_estimate(col):
    """Estimate how old a phone number might be"""
    if phones.is_valid_nanp(col):
        area_code = phones.extract_area_code(col)
        
        # Newer area codes (overlay codes)
        new_codes = ["332", "646", "347", "929", "628", "424", "747"]
        old_codes = ["212", "213", "312", "415", "202", "404"]
        
        if area_code in new_codes:
            return F.lit("recent")  # Last 10 years
        elif area_code in old_codes:
            return F.lit("established")  # 20+ years
        else:
            return F.lit("standard")  # 10-20 years
    return None

@phones.register()
def suggest_best_contact_time(col):
    """Suggest best time to call based on timezone"""
    if phones.is_valid_nanp(col):
        timezone = phones.get_timezone(col)
        
        # Map timezone to best call times
        best_times = {
            "America/New_York": "10am-12pm EST",
            "America/Chicago": "11am-1pm CST",
            "America/Denver": "12pm-2pm MST",
            "America/Los_Angeles": "1pm-3pm PST"
        }
        
        for tz, time in best_times.items():
            if timezone == tz:
                return F.lit(time)
    
    return F.lit("10am-4pm local time")

# ============================================
# ADVANCED COMPOSED PIPELINES WITH CUSTOM PRIMITIVES
# ============================================

@phones.compose(phones=phones)
def marketing_phone_analysis():
    """Analyze phone for marketing campaigns"""
    phones.clean_phone()
    
    if not phones.is_valid_phone():
        return F.lit(None)
    
    # Skip emergency and premium numbers
    if phones.is_emergency_number() or phones.is_premium_rate():
        return F.struct(
            F.lit(False).alias("contactable"),
            F.lit("restricted").alias("reason"),
            F.lit(0.0).alias("response_rate")
        )
    
    # Get all analytics
    response_rate = phones.estimate_response_rate()
    call_cost = phones.calculate_call_cost_estimate()
    best_time = phones.suggest_best_contact_time()
    phone_age = phones.calculate_phone_age_estimate()
    
    # Determine if good for marketing
    if phones.is_mobile():
        contactable = True
        channel = "SMS_and_CALL"
    elif phones.is_toll_free():
        contactable = False
        channel = "DO_NOT_CALL"
    else:
        contactable = True
        channel = "CALL_ONLY"
    
    return F.struct(
        F.lit(contactable).alias("contactable"),
        F.lit(channel).alias("channel"),
        response_rate.alias("response_rate"),
        call_cost.alias("cost_per_minute"),
        best_time.alias("best_time"),
        phone_age.alias("number_age")
    )

@phones.compose(phones=phones)
def communication_channel_router():
    """Route to best communication channel"""
    phones.clean_phone()
    phones.standardize_phone()
    
    # Check various phone types
    if phones.is_vanity_number():
        # Convert vanity to dialable
        phones.format_vanity_number()
        channel = "VOICE_IVR"  # Likely business with IVR
    elif phones.is_conference_number():
        channel = "CONFERENCE"
    elif phones.is_short_code():
        if phones.is_premium_short_code():
            channel = "PREMIUM_SMS"
        else:
            channel = "SMS_SHORT"
    elif phones.is_internal_extension():
        channel = "INTERNAL_PBX"
    elif phones.is_mobile():
        # Mobile supports multiple channels
        whatsapp = phones.format_for_whatsapp()
        channel = "OMNICHANNEL"  # SMS, WhatsApp, Call
    elif phones.is_voip():
        sip_uri = phones.format_for_sip()
        channel = "VOIP_SIP"
    else:
        channel = "TRADITIONAL_VOICE"
    
    return F.struct(
        F.lit(channel).alias("primary_channel"),
        phones.format_for_whatsapp().alias("whatsapp_link"),
        phones.format_for_sip().alias("sip_uri"),
        phones.format_rfc3966().alias("tel_uri")
    )

@phones.compose(phones=phones)
def international_calling_preparation():
    """Prepare phone for international calling"""
    phones.clean_phone()
    
    if not phones.is_valid_phone():
        return None
    
    # Get country information
    country_code = phones.extract_country_code()
    country_name = phones.extract_country_name()
    
    # Format with country flag
    formatted_display = phones.format_with_country_flag()
    
    # Get timezone offset
    tz_offset = phones.extract_time_zone_offset()
    
    # Calculate calling cost
    call_cost = phones.calculate_call_cost_estimate()
    
    # Format for different systems
    formats = F.struct(
        phones.format_e164().alias("e164"),
        phones.format_international().alias("international"),
        phones.international_dialing_format().alias("from_us"),
        formatted_display.alias("display_with_flag")
    )
    
    return F.struct(
        F.lit(country_code).alias("country_code"),
        F.lit(country_name).alias("country"),
        F.lit(tz_offset).alias("timezone_offset"),
        call_cost.alias("cost_per_minute"),
        formats.alias("formats")
    )

@phones.compose(phones=phones)
def phone_reputation_check():
    """Check phone number reputation"""
    phones.clean_phone()
    
    reputation_score = 100  # Start with perfect score
    flags = []
    
    # Check various risk factors
    if phones.is_premium_rate():
        reputation_score -= 50
        flags.append("premium_rate")
    
    if phones.is_voip():
        reputation_score -= 10
        flags.append("voip")
    
    if phones.is_short_code():
        reputation_score -= 20
        flags.append("short_code")
    
    # Check if recently created (new area code)
    age = phones.calculate_phone_age_estimate()
    if age == "recent":
        reputation_score -= 15
        flags.append("recently_created")
    
    # Mobile numbers generally more trustworthy for 2FA
    if phones.is_mobile():
        reputation_score += 10
        flags.append("mobile_verified")
    
    # Determine risk level
    if reputation_score >= 80:
        risk = "low"
    elif reputation_score >= 50:
        risk = "medium"
    else:
        risk = "high"
    
    return F.struct(
        F.lit(reputation_score).alias("score"),
        F.lit(risk).alias("risk_level"),
        F.array(*[F.lit(f) for f in flags]).alias("flags")
    )

# ============================================
# USAGE EXAMPLE WITH CUSTOM PRIMITIVES
# ============================================

def process_marketing_phones(spark):
    """Process phones for marketing campaign"""
    
    df = spark.read.csv("marketing_phones.csv", header=True)
    
    # Analyze for marketing
    df = df.withColumn(
        "marketing_analysis",
        marketing_phone_analysis(F.col("phone"))
    )
    
    # Determine communication channels
    df = df.withColumn(
        "channels",
        communication_channel_router(F.col("phone"))
    )
    
    # Prepare international numbers
    df = df.withColumn(
        "international_info",
        international_calling_preparation(F.col("phone"))
    )
    
    # Check reputation
    df = df.withColumn(
        "reputation",
        phone_reputation_check(F.col("phone"))
    )
    
    # Check special types
    df = df.withColumn("is_vanity", phones.is_vanity_number(F.col("phone")))
    df = df.withColumn("is_conference", phones.is_conference_number(F.col("phone")))
    df = df.withColumn("is_short_code", phones.is_short_code(F.col("phone")))
    
    # Extract vanity letters if present
    df = df.withColumn(
        "vanity_text",
        F.when(
            F.col("is_vanity"),
            phones.extract_vanity_letters(F.col("phone"))
        )
    )
    
    # Format for different platforms
    df = df.withColumn(
        "whatsapp_link",
        phones.format_for_whatsapp(F.col("phone"))
    )
    
    df = df.withColumn(
        "sip_uri",
        phones.format_for_sip(F.col("phone"))
    )
    
    # Calculate costs
    df = df.withColumn(
        "call_cost_per_min",
        phones.calculate_call_cost_estimate(F.col("phone"))
    )
    
    # Get best contact times
    df = df.withColumn(
        "best_call_time",
        phones.suggest_best_contact_time(F.col("phone"))
    )
    
    return df

if __name__ == "__main__":
    spark = SparkSession.builder.appName("CustomPhoneProcessing").getOrCreate()
    
    df_processed = process_marketing_phones(spark)
    
    # Filter contactable phones
    df_contactable = df_processed.filter(
        F.col("marketing_analysis.contactable") == True
    )
    
    # Segment by channel
    channel_counts = df_processed.groupBy(
        "channels.primary_channel"
    ).count().show()
    
    # Find high-value targets (mobile, low cost, high response rate)
    df_high_value = df_processed.filter(
        (F.col("marketing_analysis.response_rate") > 0.12) &
        (F.col("marketing_analysis.cost_per_minute") < 0.20) &
        (F.col("reputation.risk_level") == "low")
    )
    
    print(f"Total phones: {df_processed.count()}")
    print(f"Contactable: {df_contactable.count()}")
    print(f"High value targets: {df_high_value.count()}")
    print(f"Vanity numbers: {df_processed.filter(F.col('is_vanity')).count()}")
```

## Performance Considerations

1. **Extract digits once** - Store cleaned digits for repeated operations
2. **Validate before formatting** - Avoid formatting invalid numbers
3. **Use specific validators** - `is_valid_nanp` is faster than generic `is_valid_phone`
4. **Cache standardized results** - Standardization can be expensive

## Common Patterns

### Multiple Phone Number Handling

```python
# Handle primary and alternate phones
df = df.withColumn(
    "best_phone",
    F.coalesce(
        phones.standardize_phone(F.col("mobile")),
        phones.standardize_phone(F.col("home")),
        phones.standardize_phone(F.col("work"))
    )
).withColumn(
    "phone_type",
    F.when(phones.standardize_phone(F.col("mobile")).isNotNull(), "mobile")
    .when(phones.standardize_phone(F.col("home")).isNotNull(), "home")
    .when(phones.standardize_phone(F.col("work")).isNotNull(), "work")
    .otherwise("none")
)
```

### Phone Number Masking

```python
# Mask phone numbers for privacy
df = df.withColumn(
    "phone_masked",
    F.when(
        phones.is_valid_nanp(F.col("phone")),
        F.concat(
            F.lit("(XXX) XXX-"),
            phones.extract_subscriber(F.col("phone")).substr(-4, 4)
        )
    ).otherwise(F.lit("INVALID"))
)
```

### International Support

```python
# Handle mixed domestic and international numbers
df = df.withColumn(
    "formatted_phone",
    F.when(
        F.col("country") == "US",
        phones.format_nanp(phones.clean_phone(F.col("phone")))
    ).when(
        F.col("country") == "UK",
        phones.format_international(
            F.concat(F.lit("44"), phones.remove_country_code(F.col("phone")))
        )
    ).otherwise(
        phones.format_international(F.col("phone"))
    )
)
```