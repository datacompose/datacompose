# Address Primitives

Comprehensive address parsing, validation, and standardization primitives for PySpark.

## Installation

Generate the address primitives for your project:

```bash
datacompose add clean_addresses --target pyspark
```

## Import

```python
from build.clean_addresses.address_primitives import addresses
from pyspark.sql import functions as F
```

## Extraction Functions

### Street Components

#### `extract_street_number`

Extracts the street number from an address.

```python
df = df.withColumn("street_num", addresses.extract_street_number(F.col("address")))

# Input: "123 Main Street"
# Output: "123"

# Input: "456-458 Oak Avenue"
# Output: "456-458"
```

#### `extract_street_name`

Extracts the street name without number or suffix.

```python
df = df.withColumn("street", addresses.extract_street_name(F.col("address")))

# Input: "123 Main Street"
# Output: "Main"

# Input: "456 Martin Luther King Jr Blvd"
# Output: "Martin Luther King Jr"
```

#### `extract_street_suffix`

Extracts the street type suffix.

```python
df = df.withColumn("suffix", addresses.extract_street_suffix(F.col("address")))

# Input: "123 Main Street"
# Output: "Street"

# Input: "456 Oak Ave"
# Output: "Ave"
```

#### `extract_street_direction`

Extracts directional components (N, S, E, W, NE, SW, etc.).

```python
df = df.withColumn("direction", addresses.extract_street_direction(F.col("address")))

# Input: "123 N Main Street"
# Output: "N"

# Input: "456 Oak Avenue NW"
# Output: "NW"
```

### Location Components

#### `extract_city`

Extracts the city name from an address.

```python
df = df.withColumn("city", addresses.extract_city(F.col("address")))

# Input: "123 Main St, New York, NY 10001"
# Output: "New York"

# Input: "456 Oak Ave, Los Angeles, CA"
# Output: "Los Angeles"
```

#### `extract_state`

Extracts the state abbreviation or full name.

```python
df = df.withColumn("state", addresses.extract_state(F.col("address")))

# Input: "123 Main St, New York, NY 10001"
# Output: "NY"

# Input: "456 Oak Ave, California"
# Output: "California"
```

#### `extract_zip_code`

Extracts ZIP code (5-digit or ZIP+4 format).

```python
df = df.withColumn("zip", addresses.extract_zip_code(F.col("address")))

# Input: "123 Main St, New York, NY 10001"
# Output: "10001"

# Input: "456 Oak Ave, Los Angeles, CA 90001-1234"
# Output: "90001-1234"
```

#### `extract_country`

Extracts country name if present.

```python
df = df.withColumn("country", addresses.extract_country(F.col("address")))

# Input: "123 Main St, Toronto, ON, Canada"
# Output: "Canada"

# Input: "456 High Street, London, UK"
# Output: "UK"
```

### Unit/Suite Extraction

#### `extract_unit_number`

Extracts apartment, suite, or unit numbers.

```python
df = df.withColumn("unit", addresses.extract_unit_number(F.col("address")))

# Input: "123 Main St Apt 4B"
# Output: "4B"

# Input: "456 Oak Ave Suite 200"
# Output: "200"

# Input: "789 Elm St Unit 15"
# Output: "15"
```

#### `extract_floor`

Extracts floor information.

```python
df = df.withColumn("floor", addresses.extract_floor(F.col("address")))

# Input: "123 Main St, 5th Floor"
# Output: "5"

# Input: "456 Oak Ave Floor 2"
# Output: "2"
```

#### `extract_building`

Extracts building name or number.

```python
df = df.withColumn("building", addresses.extract_building(F.col("address")))

# Input: "Empire State Building, 350 5th Ave"
# Output: "Empire State Building"

# Input: "Building A, 123 Main St"
# Output: "Building A"
```

### Special Address Types

#### `extract_po_box`

Extracts PO Box numbers.

```python
df = df.withColumn("po_box", addresses.extract_po_box(F.col("address")))

# Input: "PO Box 123"
# Output: "123"

# Input: "P.O. Box 45678"
# Output: "45678"
```

#### `extract_rural_route`

Extracts rural route information.

```python
df = df.withColumn("rural_route", addresses.extract_rural_route(F.col("address")))

# Input: "RR 2 Box 123"
# Output: "RR 2 Box 123"

# Input: "Rural Route 5"
# Output: "Rural Route 5"
```

## Standardization Functions

### `standardize_address`

Applies comprehensive address standardization.

```python
df = df.withColumn("address_clean", addresses.standardize_address(F.col("address")))
```

**Performs:**
- Normalizes abbreviations
- Standardizes punctuation
- Fixes spacing issues
- Capitalizes properly

**Examples:**
```python
"123 main st."          → "123 Main Street"
"456 oak ave,apt 2"     → "456 Oak Avenue, Apt 2"
"po box 789"            → "PO Box 789"
```

### `standardize_state`

Converts state names to standard two-letter abbreviations.

```python
df = df.withColumn("state_abbr", addresses.standardize_state(F.col("state")))

# Input: "California"     → "CA"
# Input: "New York"       → "NY"
# Input: "texas"          → "TX"
```

### `standardize_street_suffix`

Normalizes street type suffixes.

```python
df = df.withColumn("suffix_std", addresses.standardize_street_suffix(F.col("suffix")))

# Input: "St"             → "Street"
# Input: "Ave"            → "Avenue"
# Input: "Blvd"           → "Boulevard"
# Input: "Ln"             → "Lane"
```

### `standardize_direction`

Normalizes directional abbreviations.

```python
df = df.withColumn("dir_std", addresses.standardize_direction(F.col("direction")))

# Input: "N"              → "North"
# Input: "SW"             → "Southwest"
# Input: "E."             → "East"
```

### `expand_abbreviations`

Expands common address abbreviations.

```python
df = df.withColumn("expanded", addresses.expand_abbreviations(F.col("address")))

# "123 MLK Jr Blvd"       → "123 Martin Luther King Junior Boulevard"
# "456 St. James Pl"      → "456 Saint James Place"
# "789 Mt. Vernon Ave"    → "789 Mount Vernon Avenue"
```

### `abbreviate_components`

Abbreviates address components for consistency.

```python
df = df.withColumn("abbreviated", addresses.abbreviate_components(F.col("address")))

# "123 North Main Street"     → "123 N Main St"
# "456 Southwest Oak Avenue"  → "456 SW Oak Ave"
```

## Validation Functions

### `is_valid_zip_code`

Validates ZIP code format.

```python
df = df.withColumn("valid_zip", addresses.is_valid_zip_code(F.col("zip")))
```

**Validates:**
- 5-digit format (12345)
- ZIP+4 format (12345-6789)

### `is_valid_state`

Validates state abbreviations or names.

```python
df = df.withColumn("valid_state", addresses.is_valid_state(F.col("state")))
```

**Accepts:**
- Two-letter abbreviations (NY, CA, TX)
- Full state names (New York, California, Texas)

### `is_po_box`

Checks if address is a PO Box.

```python
df = df.withColumn("is_po_box", addresses.is_po_box(F.col("address")))

# "PO Box 123"            → True
# "123 Main Street"       → False
```

### `is_complete_address`

Checks if address has all required components.

```python
df = df.withColumn("is_complete", addresses.is_complete_address(F.col("address")))
```

**Requires:**
- Street number or PO Box
- Street name (for non-PO Box)
- City
- State
- ZIP code

### `is_residential`

Attempts to identify residential addresses.

```python
df = df.withColumn("is_residential", addresses.is_residential(F.col("address")))
```

**Indicators:**
- Contains "Apt", "Unit", "Apartment"
- Residential street suffixes (Court, Circle, Place)
- No business indicators

### `is_business`

Attempts to identify business addresses.

```python
df = df.withColumn("is_business", addresses.is_business(F.col("address")))
```

**Indicators:**
- Contains "Suite", "Floor", "Building"
- Business keywords (Office, Industrial, Corporate)

## Parsing Functions

### `parse_address`

Parses address into structured components.

```python
df = df.withColumn("parsed", addresses.parse_address(F.col("address")))

# Input: "123 Main St Apt 4B, New York, NY 10001"
# Output: {
#   "street_number": "123",
#   "street_name": "Main",
#   "street_suffix": "St",
#   "unit": "4B",
#   "city": "New York",
#   "state": "NY",
#   "zip": "10001"
# }
```

### `split_address_lines`

Splits address into standard lines.

```python
df = df.withColumn("lines", addresses.split_address_lines(F.col("address")))

# Input: "123 Main St Apt 4B, New York, NY 10001"
# Output: {
#   "line1": "123 Main St Apt 4B",
#   "line2": "",
#   "city": "New York",
#   "state": "NY",
#   "zip": "10001"
# }
```

## Formatting Functions

### `format_address`

Formats address according to USPS standards.

```python
df = df.withColumn("formatted", addresses.format_address(
    street=F.col("street"),
    city=F.col("city"),
    state=F.col("state"),
    zip=F.col("zip")
))
```

### `format_mailing_address`

Creates properly formatted mailing address.

```python
df = df.withColumn("mailing", addresses.format_mailing_address(F.col("address")))

# Output format:
# 123 MAIN ST APT 4B
# NEW YORK NY 10001
```

### `format_international`

Formats address for international mail.

```python
df = df.withColumn("intl_format", addresses.format_international(
    F.col("address"),
    F.col("country")
))
```

## Geographic Functions

### `get_state_name`

Converts state abbreviation to full name.

```python
df = df.withColumn("state_full", addresses.get_state_name(F.col("state_abbr")))

# "NY" → "New York"
# "CA" → "California"
```

### `get_state_abbreviation`

Converts state name to abbreviation.

```python
df = df.withColumn("state_abbr", addresses.get_state_abbreviation(F.col("state_name")))

# "New York" → "NY"
# "California" → "CA"
```

### `get_timezone`

Estimates timezone based on state.

```python
df = df.withColumn("timezone", addresses.get_timezone(F.col("state")))

# "NY" → "America/New_York"
# "CA" → "America/Los_Angeles"
```

## Pipeline Examples Using @compose

The `@compose` decorator allows you to build powerful transformation pipelines by chaining primitives together. These composed functions must be defined at the module level (top level of your Python file).

### Complete Address Cleaning Pipelines

```python
from build.clean_addresses.address_primitives import addresses
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ============================================
# COMPOSED PIPELINE FUNCTIONS (Module Level)
# ============================================

@addresses.compose(addresses=addresses)
def basic_address_cleaning():
    """Basic address standardization"""
    addresses.standardize_address()
    if addresses.is_complete_address():
        addresses.extract_city()
        addresses.extract_state()
        addresses.extract_zip_code()

@addresses.compose(addresses=addresses)
def parse_and_validate_address():
    """Parse address and validate components"""
    # Standardize first
    addresses.standardize_address()
    
    # Check if it's a PO Box
    if addresses.is_po_box():
        addresses.extract_po_box()
        addresses.extract_zip_code()
        return F.lit("PO_BOX")
    
    # Check if complete
    if addresses.is_complete_address():
        # Extract all components
        addresses.extract_street_number()
        addresses.extract_street_name()
        addresses.extract_city()
        addresses.extract_state()
        addresses.extract_zip_code()
        
        # Validate components
        if addresses.is_valid_zip_code() and addresses.is_valid_state():
            return F.lit("VALID_COMPLETE")
        else:
            return F.lit("INVALID_COMPONENTS")
    else:
        return F.lit("INCOMPLETE")

@addresses.compose(addresses=addresses)
def extract_address_components():
    """Extract all address components into a struct"""
    addresses.standardize_address()
    
    # Extract street components
    street_num = addresses.extract_street_number()
    street_name = addresses.extract_street_name()
    street_suffix = addresses.extract_street_suffix()
    street_dir = addresses.extract_street_direction()
    
    # Extract location
    city = addresses.extract_city()
    state = addresses.extract_state()
    zip_code = addresses.extract_zip_code()
    
    # Extract unit info
    unit = addresses.extract_unit_number()
    
    # Standardize state
    state_std = addresses.standardize_state()
    
    return F.struct(
        F.lit(street_num).alias("street_number"),
        F.lit(street_name).alias("street_name"),
        F.lit(street_suffix).alias("street_suffix"),
        F.lit(street_dir).alias("direction"),
        F.lit(unit).alias("unit"),
        F.lit(city).alias("city"),
        F.lit(state_std).alias("state"),
        F.lit(zip_code).alias("zip")
    )

@addresses.compose(addresses=addresses)
def residential_address_pipeline():
    """Process residential addresses"""
    addresses.standardize_address()
    
    if addresses.is_po_box():
        return F.lit("PO_BOX_NOT_RESIDENTIAL")
    
    if addresses.is_residential():
        # Extract residential components
        addresses.extract_street_number()
        addresses.extract_street_name()
        addresses.extract_unit_number()
        addresses.extract_city()
        addresses.standardize_state()
        addresses.extract_zip_code()
        
        if addresses.is_complete_address():
            return F.lit("RESIDENTIAL_VALID")
        else:
            return F.lit("RESIDENTIAL_INCOMPLETE")
    elif addresses.is_business():
        return F.lit("BUSINESS_ADDRESS")
    else:
        return F.lit("UNKNOWN_TYPE")

@addresses.compose(addresses=addresses)
def address_deduplication_key():
    """Generate deduplication key for addresses"""
    addresses.standardize_address()
    
    # Extract key components
    street_num = addresses.extract_street_number()
    street_name = addresses.extract_street_name()
    unit = addresses.extract_unit_number()
    zip_code = addresses.extract_zip_code()
    
    # Combine into key
    if street_num and zip_code:
        if unit:
            return F.concat_ws("_", 
                F.lit(street_num),
                F.lit(street_name),
                F.lit(unit),
                F.lit(zip_code)
            )
        else:
            return F.concat_ws("_",
                F.lit(street_num),
                F.lit(street_name),
                F.lit(zip_code)
            )
    else:
        return F.lit(None)

@addresses.compose(addresses=addresses)
def geocoding_preparation():
    """Prepare address for geocoding service"""
    addresses.standardize_address()
    
    # Must not be PO Box
    if addresses.is_po_box():
        return F.lit(None)
    
    # Must be complete
    if not addresses.is_complete_address():
        return F.lit(None)
    
    # Extract and format components
    street_num = addresses.extract_street_number()
    street_name = addresses.extract_street_name()
    street_suffix = addresses.standardize_street_suffix()
    city = addresses.extract_city()
    state = addresses.standardize_state()
    zip_code = addresses.extract_zip_code()
    
    # Format for geocoding
    return F.concat_ws(", ",
        F.concat_ws(" ", F.lit(street_num), F.lit(street_name), F.lit(street_suffix)),
        F.lit(city),
        F.concat_ws(" ", F.lit(state), F.lit(zip_code))
    )

@addresses.compose(addresses=addresses)
def mailing_address_formatter():
    """Format address for mailing"""
    addresses.standardize_address()
    addresses.expand_abbreviations()
    
    # Check if PO Box
    if addresses.is_po_box():
        po_box = addresses.extract_po_box()
        city = addresses.extract_city()
        state = addresses.standardize_state()
        zip_code = addresses.extract_zip_code()
        
        return F.upper(F.concat_ws("\n",
            F.concat(F.lit("PO BOX "), F.lit(po_box)),
            F.concat_ws(" ", F.lit(city), F.lit(state), F.lit(zip_code))
        ))
    else:
        # Format regular address
        return addresses.format_mailing_address()

@addresses.compose(addresses=addresses)
def address_quality_score():
    """Calculate address quality score"""
    addresses.standardize_address()
    
    # Start with base score
    score = 0
    
    # Check completeness (40 points)
    if addresses.is_complete_address():
        score = 40
    
    # Check components (30 points)
    if addresses.extract_street_number():
        score += 10
    if addresses.extract_city():
        score += 10
    if addresses.extract_state():
        score += 10
    
    # Check validity (30 points)
    if addresses.is_valid_zip_code():
        score += 15
    if addresses.is_valid_state():
        score += 15
    
    return F.lit(score / 100.0)

@addresses.compose(addresses=addresses)
def international_address_handler():
    """Handle international addresses"""
    addresses.standardize_address()
    
    # Check for country
    country = addresses.extract_country()
    
    if country:
        # International address
        if country in ["USA", "US", "United States"]:
            # US address - full processing
            addresses.standardize_state()
            addresses.extract_zip_code()
            return addresses.format_mailing_address()
        else:
            # Other countries - basic formatting
            return addresses.format_international()
    else:
        # Assume US if no country specified
        addresses.standardize_state()
        return addresses.format_mailing_address()

# ============================================
# ADVANCED COMPOSED PIPELINES
# ============================================

@addresses.compose(addresses=addresses)
def address_enrichment_pipeline():
    """Comprehensive address enrichment"""
    addresses.standardize_address()
    
    # Determine address type
    if addresses.is_po_box():
        addr_type = "po_box"
    elif addresses.is_residential():
        addr_type = "residential"
    elif addresses.is_business():
        addr_type = "business"
    else:
        addr_type = "unknown"
    
    # Extract all components
    components = extract_address_components()
    
    # Get geographic info
    state = addresses.extract_state()
    if state:
        state_full = addresses.get_state_name()
        timezone = addresses.get_timezone()
    else:
        state_full = None
        timezone = None
    
    # Check validity
    is_complete = addresses.is_complete_address()
    valid_zip = addresses.is_valid_zip_code()
    valid_state = addresses.is_valid_state()
    
    return F.struct(
        F.lit(addr_type).alias("type"),
        components.alias("components"),
        F.lit(state_full).alias("state_full_name"),
        F.lit(timezone).alias("timezone"),
        F.lit(is_complete).alias("is_complete"),
        F.lit(valid_zip).alias("valid_zip"),
        F.lit(valid_state).alias("valid_state")
    )

@addresses.compose(addresses=addresses)
def address_comparison_key():
    """Generate key for address comparison/matching"""
    addresses.standardize_address()
    
    # Extract key components
    street_num = addresses.extract_street_number()
    street_name = addresses.extract_street_name()
    zip5 = F.substring(addresses.extract_zip_code(), 1, 5)  # First 5 digits of ZIP
    
    # Normalize for comparison
    if street_num and street_name and zip5:
        # Remove common words and normalize
        street_normalized = F.upper(F.regexp_replace(street_name, r"\b(STREET|ST|AVENUE|AVE|ROAD|RD|DRIVE|DR)\b", ""))
        
        return F.concat_ws("|",
            F.lit(street_num),
            street_normalized,
            F.lit(zip5)
        )
    else:
        return F.lit(None)

@addresses.compose(addresses=addresses)
def shipping_address_validation():
    """Validate address for shipping"""
    addresses.standardize_address()
    
    # Cannot ship to PO Box for most carriers
    if addresses.is_po_box():
        return F.struct(
            F.lit(False).alias("shippable"),
            F.lit("PO_BOX_NOT_SHIPPABLE").alias("reason")
        )
    
    # Must be complete
    if not addresses.is_complete_address():
        return F.struct(
            F.lit(False).alias("shippable"),
            F.lit("INCOMPLETE_ADDRESS").alias("reason")
        )
    
    # Validate components
    if not addresses.is_valid_zip_code():
        return F.struct(
            F.lit(False).alias("shippable"),
            F.lit("INVALID_ZIP_CODE").alias("reason")
        )
    
    if not addresses.is_valid_state():
        return F.struct(
            F.lit(False).alias("shippable"),
            F.lit("INVALID_STATE").alias("reason")
        )
    
    # All checks passed
    return F.struct(
        F.lit(True).alias("shippable"),
        F.lit("VALID").alias("reason")
    )

# ============================================
# USAGE IN APPLICATION
# ============================================

def process_customer_addresses(spark):
    """Main processing function using composed pipelines"""
    
    # Load data
    df = spark.read.csv("customer_addresses.csv", header=True)
    
    # Basic cleaning
    df = df.withColumn(
        "address_clean",
        basic_address_cleaning(F.col("address"))
    )
    
    # Parse and validate
    df = df.withColumn(
        "validation_status",
        parse_and_validate_address(F.col("address"))
    )
    
    # Extract components
    df = df.withColumn(
        "address_components",
        extract_address_components(F.col("address"))
    )
    
    # Process residential addresses
    df = df.withColumn(
        "residential_status",
        residential_address_pipeline(F.col("address"))
    )
    
    # Generate dedup key
    df = df.withColumn(
        "dedup_key",
        address_deduplication_key(F.col("address"))
    )
    
    # Prepare for geocoding
    df = df.withColumn(
        "geocoding_address",
        geocoding_preparation(F.col("address"))
    )
    
    # Format for mailing
    df = df.withColumn(
        "mailing_format",
        mailing_address_formatter(F.col("address"))
    )
    
    # Calculate quality score
    df = df.withColumn(
        "quality_score",
        address_quality_score(F.col("address"))
    )
    
    # Validate for shipping
    df = df.withColumn(
        "shipping_validation",
        shipping_address_validation(F.col("address"))
    )
    
    # Full enrichment
    df = df.withColumn(
        "address_enriched",
        address_enrichment_pipeline(F.col("address"))
    )
    
    return df

# ============================================
# USAGE EXAMPLE
# ============================================

if __name__ == "__main__":
    # Initialize Spark
    spark = SparkSession.builder.appName("AddressProcessing").getOrCreate()
    
    # Process addresses
    df_processed = process_customer_addresses(spark)
    
    # Show results
    df_processed.select(
        "address",
        "validation_status",
        "quality_score",
        "shipping_validation.shippable",
        "address_components.city",
        "address_components.state",
        "address_components.zip"
    ).show(20, truncate=False)
    
    # Deduplicate
    df_unique = df_processed.dropDuplicates(["dedup_key"])
    
    # Filter shippable addresses
    df_shippable = df_processed.filter(
        F.col("shipping_validation.shippable") == True
    )
    
    # High quality addresses for geocoding
    df_geocoding = df_processed.filter(
        (F.col("geocoding_address").isNotNull()) &
        (F.col("quality_score") >= 0.8)
    )
    
    print(f"Total addresses: {df_processed.count()}")
    print(f"Unique addresses: {df_unique.count()}")
    print(f"Shippable addresses: {df_shippable.count()}")
    print(f"Geocoding ready: {df_geocoding.count()}")

# Method 1: Comprehensive step-by-step pipeline (without @compose)
def clean_addresses_step_by_step(df):
    """
    Complete address cleaning with all components extracted
    """
    return (
        df
        # Step 1: Initial standardization
        .withColumn("address_upper", F.upper(F.trim(F.col("address"))))
        
        # Step 2: Check address type
        .withColumn("is_po_box", addresses.is_po_box(F.col("address_upper")))
        .withColumn("is_complete", addresses.is_complete_address(F.col("address_upper")))
        
        # Step 3: Standardize the full address
        .withColumn("address_std", addresses.standardize_address(F.col("address_upper")))
        
        # Step 4: Extract street components
        .withColumn("street_number", addresses.extract_street_number(F.col("address_std")))
        .withColumn("street_name", addresses.extract_street_name(F.col("address_std")))
        .withColumn("street_suffix", addresses.extract_street_suffix(F.col("address_std")))
        .withColumn("street_direction", addresses.extract_street_direction(F.col("address_std")))
        
        # Step 5: Extract unit/suite information
        .withColumn("unit_number", addresses.extract_unit_number(F.col("address_std")))
        .withColumn("floor", addresses.extract_floor(F.col("address_std")))
        
        # Step 6: Extract location components
        .withColumn("city", addresses.extract_city(F.col("address_std")))
        .withColumn("state_raw", addresses.extract_state(F.col("address_std")))
        .withColumn("zip_code", addresses.extract_zip_code(F.col("address_std")))
        
        # Step 7: Standardize extracted components
        .withColumn("state", addresses.standardize_state(F.col("state_raw")))
        .withColumn(
            "street_suffix_std",
            addresses.standardize_street_suffix(F.col("street_suffix"))
        )
        .withColumn(
            "direction_std",
            addresses.standardize_direction(F.col("street_direction"))
        )
        
        # Step 8: Handle PO Boxes specially
        .withColumn(
            "po_box_number",
            F.when(
                F.col("is_po_box"),
                addresses.extract_po_box(F.col("address_std"))
            )
        )
        
        # Step 9: Validate components
        .withColumn("valid_zip", addresses.is_valid_zip_code(F.col("zip_code")))
        .withColumn("valid_state", addresses.is_valid_state(F.col("state")))
        
        # Step 10: Format for output
        .withColumn(
            "formatted_address",
            addresses.format_mailing_address(F.col("address_std"))
        )
        
        # Step 11: Create address quality score
        .withColumn(
            "address_quality_score",
            (
                F.col("is_complete").cast("int") * 3 +
                F.col("valid_zip").cast("int") * 2 +
                F.col("valid_state").cast("int") * 2 +
                F.when(F.col("street_number").isNotNull(), 1).otherwise(0) +
                F.when(F.col("city").isNotNull(), 1).otherwise(0)
            ) / 9.0
        )
    )

# Method 2: Production Address Processing Pipeline
class AddressProcessingPipeline:
    """
    Production-ready address processing pipeline
    """
    
    def __init__(self, df):
        self.df = df
        self.addresses = addresses
    
    def process(self):
        """Main processing pipeline"""
        self.df = (
            self.df
            .transform(self.parse_addresses)
            .transform(self.standardize_components)
            .transform(self.validate_addresses)
            .transform(self.enrich_addresses)
            .transform(self.format_addresses)
            .transform(self.deduplicate_addresses)
        )
        return self.df
    
    def parse_addresses(self, df):
        """Parse address into components"""
        return (
            df
            # Use the parse_address function to get all components at once
            .withColumn("parsed", self.addresses.parse_address(F.col("address")))
            
            # Extract individual components from parsed struct
            .withColumn("street_parts", F.struct(
                self.addresses.extract_street_number(F.col("address")).alias("number"),
                self.addresses.extract_street_name(F.col("address")).alias("name"),
                self.addresses.extract_street_suffix(F.col("address")).alias("suffix"),
                self.addresses.extract_street_direction(F.col("address")).alias("direction")
            ))
            
            .withColumn("location_parts", F.struct(
                self.addresses.extract_city(F.col("address")).alias("city"),
                self.addresses.extract_state(F.col("address")).alias("state"),
                self.addresses.extract_zip_code(F.col("address")).alias("zip"),
                self.addresses.extract_country(F.col("address")).alias("country")
            ))
            
            .withColumn("unit_parts", F.struct(
                self.addresses.extract_unit_number(F.col("address")).alias("unit"),
                self.addresses.extract_floor(F.col("address")).alias("floor"),
                self.addresses.extract_building(F.col("address")).alias("building")
            ))
            
            # Check for special address types
            .withColumn("address_type",
                F.when(self.addresses.is_po_box(F.col("address")), "po_box")
                .when(self.addresses.is_residential(F.col("address")), "residential")
                .when(self.addresses.is_business(F.col("address")), "business")
                .otherwise("unknown")
            )
        )
    
    def standardize_components(self, df):
        """Standardize all components"""
        return (
            df
            .withColumn("street_parts_std", F.struct(
                F.col("street_parts.number").alias("number"),
                F.col("street_parts.name").alias("name"),
                self.addresses.standardize_street_suffix(
                    F.col("street_parts.suffix")
                ).alias("suffix"),
                self.addresses.standardize_direction(
                    F.col("street_parts.direction")
                ).alias("direction")
            ))
            
            .withColumn("location_parts_std", F.struct(
                F.col("location_parts.city").alias("city"),
                self.addresses.standardize_state(
                    F.col("location_parts.state")
                ).alias("state"),
                F.col("location_parts.zip").alias("zip"),
                F.col("location_parts.country").alias("country")
            ))
            
            # Expand common abbreviations
            .withColumn(
                "address_expanded",
                self.addresses.expand_abbreviations(F.col("address"))
            )
        )
    
    def validate_addresses(self, df):
        """Validate address components"""
        return (
            df
            .withColumn("validation", F.struct(
                self.addresses.is_complete_address(F.col("address")).alias("is_complete"),
                self.addresses.is_valid_zip_code(
                    F.col("location_parts_std.zip")
                ).alias("valid_zip"),
                self.addresses.is_valid_state(
                    F.col("location_parts_std.state")
                ).alias("valid_state"),
                self.addresses.is_po_box(F.col("address")).alias("is_po_box")
            ))
            
            # Calculate validation score
            .withColumn("validation_score",
                F.when(F.col("validation.is_complete"), 1.0)
                .when(
                    F.col("validation.valid_zip") & F.col("validation.valid_state"),
                    0.75
                )
                .when(
                    F.col("validation.valid_zip") | F.col("validation.valid_state"),
                    0.5
                )
                .otherwise(0.25)
            )
        )
    
    def enrich_addresses(self, df):
        """Enrich with additional data"""
        return (
            df
            # Add geographic information
            .withColumn(
                "geo_info",
                F.struct(
                    self.addresses.get_state_name(
                        F.col("location_parts_std.state")
                    ).alias("state_full"),
                    self.addresses.get_timezone(
                        F.col("location_parts_std.state")
                    ).alias("timezone")
                )
            )
            
            # Determine if address needs geocoding
            .withColumn(
                "geocoding_ready",
                F.col("validation.is_complete") & 
                ~F.col("validation.is_po_box")
            )
        )
    
    def format_addresses(self, df):
        """Format addresses for different uses"""
        return (
            df
            # USPS mailing format
            .withColumn(
                "mailing_address",
                self.addresses.format_mailing_address(F.col("address"))
            )
            
            # Standard format
            .withColumn(
                "formatted_address",
                self.addresses.format_address(
                    street=F.concat_ws(" ",
                        F.col("street_parts_std.number"),
                        F.col("street_parts_std.direction"),
                        F.col("street_parts_std.name"),
                        F.col("street_parts_std.suffix")
                    ),
                    city=F.col("location_parts_std.city"),
                    state=F.col("location_parts_std.state"),
                    zip=F.col("location_parts_std.zip")
                )
            )
            
            # International format if needed
            .withColumn(
                "intl_address",
                F.when(
                    F.col("location_parts.country").isNotNull(),
                    self.addresses.format_international(
                        F.col("address"),
                        F.col("location_parts.country")
                    )
                )
            )
        )
    
    def deduplicate_addresses(self, df):
        """Deduplicate based on standardized components"""
        return (
            df
            .withColumn(
                "address_key",
                F.upper(F.concat_ws("|",
                    F.coalesce(F.col("street_parts_std.number"), F.lit("")),
                    F.coalesce(F.col("street_parts_std.name"), F.lit("")),
                    F.coalesce(F.col("location_parts_std.zip"), F.lit("")),
                    F.coalesce(F.col("unit_parts.unit"), F.lit(""))
                ))
            )
            .dropDuplicates(["address_key"])
        )
    
    def get_statistics(self):
        """Generate statistics about processed addresses"""
        return {
            "total": self.df.count(),
            "by_type": self.df.groupBy("address_type").count().collect(),
            "by_state": self.df.groupBy("location_parts_std.state").count().collect(),
            "validation_stats": self.df.agg(
                F.avg("validation_score").alias("avg_validation_score"),
                F.sum(F.col("validation.is_complete").cast("int")).alias("complete_count"),
                F.sum(F.col("validation.is_po_box").cast("int")).alias("po_box_count")
            ).collect()[0]
        }

# Method 3: Address matching and deduplication pipeline
class AddressMatchingPipeline:
    """
    Pipeline for matching and deduplicating addresses
    """
    
    def __init__(self, df1, df2):
        self.df1 = df1
        self.df2 = df2
        self.addresses = addresses
    
    def standardize_for_matching(self, df, suffix=""):
        """Standardize addresses for matching"""
        return (
            df
            .withColumn(f"std_number{suffix}",
                self.addresses.extract_street_number(F.col("address"))
            )
            .withColumn(f"std_street{suffix}",
                F.upper(self.addresses.extract_street_name(F.col("address")))
            )
            .withColumn(f"std_zip{suffix}",
                F.substring(self.addresses.extract_zip_code(F.col("address")), 1, 5)
            )
            .withColumn(f"std_unit{suffix}",
                F.upper(self.addresses.extract_unit_number(F.col("address")))
            )
            # Create match key
            .withColumn(f"match_key{suffix}",
                F.concat_ws("_",
                    F.col(f"std_number{suffix}"),
                    F.col(f"std_street{suffix}"),
                    F.col(f"std_zip{suffix}")
                )
            )
        )
    
    def find_matches(self):
        """Find matching addresses between two datasets"""
        df1_std = self.standardize_for_matching(self.df1, "_1")
        df2_std = self.standardize_for_matching(self.df2, "_2")
        
        # Exact matches
        exact_matches = df1_std.join(
            df2_std,
            F.col("match_key_1") == F.col("match_key_2"),
            "inner"
        )
        
        # Fuzzy matches (same street and zip, different unit)
        fuzzy_matches = df1_std.join(
            df2_std,
            (F.col("std_number_1") == F.col("std_number_2")) &
            (F.col("std_street_1") == F.col("std_street_2")) &
            (F.col("std_zip_1") == F.col("std_zip_2")) &
            (F.col("std_unit_1") != F.col("std_unit_2")),
            "inner"
        )
        
        return exact_matches, fuzzy_matches

# Usage examples
# Basic pipeline
df_cleaned = clean_addresses_step_by_step(df)

# Advanced pipeline
pipeline = AddressProcessingPipeline(df)
df_processed = pipeline.process()
stats = pipeline.get_statistics()

# Matching pipeline
matcher = AddressMatchingPipeline(df1, df2)
exact, fuzzy = matcher.find_matches()
```

### Address Deduplication

```python
# Standardize addresses for deduplication
df_dedup = df.withColumn(
    "address_key",
    F.concat_ws("|",
        addresses.standardize_address(F.col("address")),
        addresses.standardize_state(addresses.extract_state(F.col("address"))),
        addresses.extract_zip_code(F.col("address"))
    )
).dropDuplicates(["address_key"])
```

### Address Validation Report

```python
# Generate address quality metrics
quality_report = df.agg(
    F.count("address").alias("total_addresses"),
    F.sum(addresses.is_complete_address(F.col("address")).cast("int")).alias("complete_count"),
    F.sum(addresses.is_po_box(F.col("address")).cast("int")).alias("po_box_count"),
    F.sum(addresses.is_valid_zip_code(
        addresses.extract_zip_code(F.col("address"))
    ).cast("int")).alias("valid_zip_count"),
    F.countDistinct(addresses.extract_state(F.col("address"))).alias("unique_states")
)
```

### Geocoding Preparation

```python
# Prepare addresses for geocoding service
df_geocode = df.withColumn(
    "geocode_address",
    F.concat_ws(", ",
        addresses.extract_street_number(F.col("address")),
        addresses.extract_street_name(F.col("address")),
        addresses.standardize_street_suffix(
            addresses.extract_street_suffix(F.col("address"))
        ),
        addresses.extract_city(F.col("address")),
        addresses.standardize_state(
            addresses.extract_state(F.col("address"))
        ),
        addresses.extract_zip_code(F.col("address"))
    )
).filter(addresses.is_complete_address(F.col("address")))
```

## Advanced: Creating Custom Address Primitives

You can extend the address primitives library with your own custom functions tailored to your specific business needs.

### Setting Up Custom Address Primitives

```python
from build.clean_addresses.address_primitives import addresses
from pyspark.sql import functions as F
import re

# ============================================
# CUSTOM ADDRESS PRIMITIVES
# ============================================

# Add custom validation functions
@addresses.register()
def is_military_address(col):
    """Check if address is military (APO/FPO/DPO)"""
    upper_addr = F.upper(col)
    return (
        upper_addr.contains("APO") |
        upper_addr.contains("FPO") |
        upper_addr.contains("DPO")
    )

@addresses.register()
def is_campus_address(col):
    """Check if address is a university/campus address"""
    campus_indicators = [
        "DORM", "DORMITORY", "RESIDENCE HALL", "STUDENT HOUSING",
        "CAMPUS", "UNIVERSITY", "COLLEGE", "HALL"
    ]
    upper_addr = F.upper(col)
    is_campus = F.lit(False)
    for indicator in campus_indicators:
        is_campus = is_campus | upper_addr.contains(indicator)
    return is_campus

@addresses.register()
def is_commercial_address(col):
    """Check if address appears commercial"""
    commercial_indicators = [
        "INDUSTRIAL", "BUSINESS PARK", "CORPORATE", "PLAZA",
        "TOWER", "BUILDING", "OFFICE", "WAREHOUSE", "FACILITY"
    ]
    upper_addr = F.upper(col)
    is_commercial = F.lit(False)
    for indicator in commercial_indicators:
        is_commercial = is_commercial | upper_addr.contains(indicator)
    return is_commercial

@addresses.register()
def is_rural_address(col):
    """Check if address is rural"""
    return (
        addresses.extract_rural_route(col).isNotNull() |
        F.upper(col).contains("RURAL") |
        F.upper(col).contains("RR ") |
        F.upper(col).contains("HC ")  # Highway Contract
    )

@addresses.register()
def is_international_address(col):
    """Check if address appears to be international"""
    country = addresses.extract_country(col)
    return (
        country.isNotNull() &
        ~country.isin("USA", "US", "United States", "America")
    )

# Add custom extraction functions
@addresses.register()
def extract_cross_street(col):
    """Extract cross street information"""
    patterns = [
        r'(?:between|btwn|btw)\s+([^&]+)\s+(?:and|&)\s+([^,]+)',
        r'(?:corner of|cor)\s+([^&]+)\s+(?:and|&)\s+([^,]+)',
        r'([^&]+)\s+&\s+([^,]+)\s+(?:intersection|inter)'
    ]
    
    for pattern in patterns:
        match = F.regexp_extract(col, pattern, 1)
        if match.isNotNull():
            return match
    return None

@addresses.register()
def extract_landmark(col):
    """Extract landmark references from address"""
    patterns = [
        r'(?:near|by|close to|next to|across from|behind|in front of)\s+([^,]+)',
        r'(?:inside|within|at)\s+(.*?mall|.*?center|.*?station|.*?airport)'
    ]
    
    for pattern in patterns:
        match = F.regexp_extract(F.lower(col), pattern, 1)
        if match.isNotNull():
            return F.initcap(match)
    return None

@addresses.register()
def extract_delivery_instructions(col):
    """Extract delivery instructions from address"""
    patterns = [
        r'(?:deliver to|leave at|ring|buzz)\s+(.+)',
        r'(?:gate code|access code|entry code)[:\s]*([0-9#*]+)',
        r'(?:back door|front door|side door|garage)'
    ]
    
    for pattern in patterns:
        match = F.regexp_extract(F.lower(col), pattern, 0)
        if match.isNotNull():
            return match
    return None

@addresses.register()
def extract_county(col):
    """Extract county information if present"""
    return F.regexp_extract(
        col,
        r'([A-Za-z\s]+)\s+County',
        1
    )

@addresses.register()
def extract_neighborhood(col):
    """Extract neighborhood/district information"""
    # Common neighborhood indicators
    patterns = [
        r'(?:in|@)\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*?)\s+(?:neighborhood|district|area)',
        r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*?)\s+(?:Heights|Hills|Park|Square|Village)'
    ]
    
    for pattern in patterns:
        match = F.regexp_extract(col, pattern, 1)
        if match.isNotNull():
            return match
    return None

# Add custom standardization functions
@addresses.register()
def standardize_military_address(col):
    """Standardize military addresses"""
    if addresses.is_military_address(col):
        # Standardize military state codes
        return F.regexp_replace(
            F.upper(col),
            r'\b(APO|FPO|DPO)\b',
            lambda m: m.group(0)
        )
    return col

@addresses.register()
def standardize_rural_address(col):
    """Standardize rural route addresses"""
    # Standardize RR format
    standardized = F.regexp_replace(
        F.upper(col),
        r'\b(?:RURAL ROUTE|RR|R\.R\.)\s*([0-9]+)',
        'RR $1'
    )
    # Standardize HC format
    standardized = F.regexp_replace(
        standardized,
        r'\b(?:HIGHWAY CONTRACT|HC|H\.C\.)\s*([0-9]+)',
        'HC $1'
    )
    return standardized

@addresses.register()
def normalize_building_names(col):
    """Normalize common building name patterns"""
    building_map = {
        r'\bBLDG\b': 'BUILDING',
        r'\bTWR\b': 'TOWER',
        r'\bCTR\b': 'CENTER',
        r'\bPLZ\b': 'PLAZA',
        r'\bCT\b': 'COURT',
        r'\bSQ\b': 'SQUARE'
    }
    
    result = F.upper(col)
    for pattern, replacement in building_map.items():
        result = F.regexp_replace(result, pattern, replacement)
    return result

@addresses.register()
def add_plus4_zip(col, plus4):
    """Add ZIP+4 to addresses"""
    current_zip = addresses.extract_zip_code(col)
    if F.length(current_zip) == 5:
        return F.regexp_replace(
            col,
            current_zip,
            F.concat(current_zip, F.lit("-"), plus4)
        )
    return col

# Add custom formatting functions
@addresses.register()
def format_for_label(col, name=None, company=None):
    """Format address for shipping label"""
    lines = []
    
    if name:
        lines.append(F.upper(name))
    if company:
        lines.append(F.upper(company))
    
    # Parse address components
    street = F.concat_ws(" ",
        addresses.extract_street_number(col),
        addresses.extract_street_direction(col),
        addresses.extract_street_name(col),
        addresses.extract_street_suffix(col)
    )
    
    unit = addresses.extract_unit_number(col)
    if unit:
        street = F.concat(street, F.lit(" "), unit)
    
    lines.append(F.upper(street))
    
    # City, State ZIP line
    city_state_zip = F.concat_ws(" ",
        F.upper(addresses.extract_city(col)),
        F.upper(addresses.standardize_state(addresses.extract_state(col))),
        addresses.extract_zip_code(col)
    )
    lines.append(city_state_zip)
    
    return F.concat_ws("\n", *lines)

@addresses.register()
def format_one_line(col):
    """Format address as single line"""
    return F.regexp_replace(
        addresses.standardize_address(col),
        r'[\n\r]+',
        ', '
    )

# Add custom analysis functions
@addresses.register()
def calculate_address_specificity_score(col):
    """Calculate how specific/complete an address is"""
    score = 0
    
    # Check components (each worth points)
    components = [
        (addresses.extract_street_number(col), 20),
        (addresses.extract_street_name(col), 20),
        (addresses.extract_city(col), 15),
        (addresses.extract_state(col), 10),
        (addresses.extract_zip_code(col), 15),
        (addresses.extract_unit_number(col), 10),
        (addresses.extract_street_suffix(col), 5),
        (addresses.extract_street_direction(col), 5)
    ]
    
    total = 0
    for component, points in components:
        if component.isNotNull():
            total += points
    
    return total / 100.0

@addresses.register()
def estimate_delivery_difficulty(col):
    """Estimate delivery difficulty based on address type"""
    # Start with base difficulty
    difficulty = F.lit("standard")
    
    # Check various factors
    if addresses.is_po_box(col):
        difficulty = F.lit("po_box")
    elif addresses.is_military_address(col):
        difficulty = F.lit("military")
    elif addresses.is_rural_address(col):
        difficulty = F.lit("rural")
    elif addresses.extract_unit_number(col).isNotNull():
        difficulty = F.lit("apartment")
    elif addresses.is_commercial_address(col):
        difficulty = F.lit("commercial")
    
    return difficulty

@addresses.register()
def suggest_address_improvements(col):
    """Suggest improvements for incomplete addresses"""
    suggestions = []
    
    if addresses.extract_street_number(col).isNull():
        suggestions.append("Missing street number")
    if addresses.extract_city(col).isNull():
        suggestions.append("Missing city")
    if addresses.extract_state(col).isNull():
        suggestions.append("Missing state")
    if addresses.extract_zip_code(col).isNull():
        suggestions.append("Missing ZIP code")
    if not addresses.is_valid_zip_code(addresses.extract_zip_code(col)):
        suggestions.append("Invalid ZIP code format")
    
    return F.array(*[F.lit(s) for s in suggestions]) if suggestions else F.array()

# ============================================
# ADVANCED COMPOSED PIPELINES WITH CUSTOM PRIMITIVES
# ============================================

@addresses.compose(addresses=addresses)
def delivery_routing_pipeline():
    """Determine delivery routing based on address type"""
    addresses.standardize_address()
    
    # Military addresses
    if addresses.is_military_address():
        addresses.standardize_military_address()
        return F.struct(
            F.lit("military").alias("route"),
            F.lit("USPS").alias("carrier"),
            F.lit(5).alias("days_estimate")
        )
    
    # PO Boxes
    if addresses.is_po_box():
        return F.struct(
            F.lit("po_box").alias("route"),
            F.lit("USPS").alias("carrier"),
            F.lit(2).alias("days_estimate")
        )
    
    # Rural addresses
    if addresses.is_rural_address():
        addresses.standardize_rural_address()
        return F.struct(
            F.lit("rural").alias("route"),
            F.lit("USPS_or_UPS").alias("carrier"),
            F.lit(4).alias("days_estimate")
        )
    
    # Commercial addresses
    if addresses.is_commercial_address():
        return F.struct(
            F.lit("commercial").alias("route"),
            F.lit("Any").alias("carrier"),
            F.lit(1).alias("days_estimate")
        )
    
    # Standard residential
    return F.struct(
        F.lit("residential").alias("route"),
        F.lit("Any").alias("carrier"),
        F.lit(2).alias("days_estimate")
    )

@addresses.compose(addresses=addresses)
def address_intelligence_extraction():
    """Extract all intelligence from address"""
    addresses.standardize_address()
    
    return F.struct(
        addresses.extract_cross_street().alias("cross_street"),
        addresses.extract_landmark().alias("landmark"),
        addresses.extract_delivery_instructions().alias("delivery_notes"),
        addresses.extract_county().alias("county"),
        addresses.extract_neighborhood().alias("neighborhood"),
        addresses.is_military_address().alias("is_military"),
        addresses.is_campus_address().alias("is_campus"),
        addresses.is_commercial_address().alias("is_commercial"),
        addresses.is_rural_address().alias("is_rural"),
        addresses.calculate_address_specificity_score().alias("specificity_score"),
        addresses.estimate_delivery_difficulty().alias("delivery_difficulty")
    )

@addresses.compose(addresses=addresses)
def address_enhancement_pipeline():
    """Enhance address with all available data"""
    # Standardize first
    addresses.standardize_address()
    addresses.normalize_building_names()
    
    # Handle special types
    if addresses.is_military_address():
        addresses.standardize_military_address()
    elif addresses.is_rural_address():
        addresses.standardize_rural_address()
    
    # Extract all components
    components = addresses.extract_address_components()
    
    # Get intelligence
    intel = address_intelligence_extraction()
    
    # Format options
    formats = F.struct(
        addresses.format_mailing_address().alias("mailing"),
        addresses.format_one_line().alias("one_line"),
        addresses.format_for_label().alias("label")
    )
    
    # Get suggestions
    suggestions = addresses.suggest_address_improvements()
    
    return F.struct(
        components.alias("components"),
        intel.alias("intelligence"),
        formats.alias("formats"),
        suggestions.alias("improvements_needed")
    )

# ============================================
# USAGE EXAMPLE WITH CUSTOM PRIMITIVES
# ============================================

def process_delivery_addresses(spark):
    """Process addresses for delivery optimization"""
    
    df = spark.read.csv("delivery_addresses.csv", header=True)
    
    # Determine routing
    df = df.withColumn(
        "routing",
        delivery_routing_pipeline(F.col("address"))
    )
    
    # Extract intelligence
    df = df.withColumn(
        "address_intel",
        address_intelligence_extraction(F.col("address"))
    )
    
    # Full enhancement
    df = df.withColumn(
        "enhanced",
        address_enhancement_pipeline(F.col("address"))
    )
    
    # Check special types
    df = df.withColumn("is_military", addresses.is_military_address(F.col("address")))
    df = df.withColumn("is_campus", addresses.is_campus_address(F.col("address")))
    df = df.withColumn("is_rural", addresses.is_rural_address(F.col("address")))
    df = df.withColumn("is_commercial", addresses.is_commercial_address(F.col("address")))
    
    # Calculate scores
    df = df.withColumn(
        "specificity_score",
        addresses.calculate_address_specificity_score(F.col("address"))
    )
    
    # Estimate difficulty
    df = df.withColumn(
        "delivery_difficulty",
        addresses.estimate_delivery_difficulty(F.col("address"))
    )
    
    # Format for labels
    df = df.withColumn(
        "shipping_label",
        addresses.format_for_label(
            F.col("address"),
            name=F.col("customer_name"),
            company=F.col("company_name")
        )
    )
    
    return df

if __name__ == "__main__":
    spark = SparkSession.builder.appName("CustomAddressProcessing").getOrCreate()
    
    df_processed = process_delivery_addresses(spark)
    
    # Analyze routing distribution
    df_processed.groupBy("routing.route", "routing.carrier").count().show()
    
    # Find difficult deliveries
    df_difficult = df_processed.filter(
        F.col("delivery_difficulty").isin("military", "rural", "po_box")
    )
    
    # Export enhanced addresses
    df_enhanced = df_processed.select(
        "address",
        "enhanced.components",
        "enhanced.intelligence",
        "enhanced.formats.label",
        "routing"
    )
    
    print(f"Difficult deliveries: {df_difficult.count()}")
    print(f"Military addresses: {df_processed.filter(F.col('is_military')).count()}")
    print(f"Rural addresses: {df_processed.filter(F.col('is_rural')).count()}")
```

## Performance Considerations

1. **Parse once, use many times** - Store parsed components rather than re-parsing
2. **Use specific extractors** - More efficient than full parsing when you need specific components
3. **Standardize before matching** - Improves deduplication accuracy
4. **Cache validation results** - Validation functions can be expensive on large datasets

## Common Patterns

### Address Completion

```python
# Fill missing components from multiple sources
df = df.withColumn(
    "complete_address",
    F.struct(
        F.coalesce(
            addresses.extract_street_number(F.col("address")),
            F.col("street_num_backup")
        ).alias("number"),
        F.coalesce(
            addresses.extract_city(F.col("address")),
            F.col("city_backup")
        ).alias("city"),
        F.coalesce(
            addresses.standardize_state(addresses.extract_state(F.col("address"))),
            addresses.standardize_state(F.col("state_backup"))
        ).alias("state")
    )
)
```

### Multi-line Address Handling

```python
# Handle addresses that may be split across multiple lines
df = df.withColumn(
    "full_address",
    F.concat_ws(" ",
        F.col("address_line_1"),
        F.col("address_line_2"),
        F.col("city_state_zip")
    )
).withColumn(
    "parsed",
    addresses.parse_address(F.col("full_address"))
)
```