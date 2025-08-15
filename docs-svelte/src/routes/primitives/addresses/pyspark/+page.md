---
title: PYSPARK Implementation
description: Addresses transformers for PYSPARK
---

# PYSPARK Implementation

## Installation

```bash
pip install datacompose[pyspark]
```

## Usage

```python
from datacompose.transformers.text.addresses import addresses

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('DataCompose').getOrCreate()
df = spark.read.parquet('data.parquet')

# Using the @addresses.compose() decorator
@addresses.compose()
def clean_data(df):
    return df

cleaned_df = clean_data(df)
```

## Available Methods

Total methods: **43**

#### `add_custom_city(city_name: str)`

Add a custom city name to improve city extraction.

This is useful for cities that might be ambiguous or hard to extract.

Args:
    city_name: Name of the city to add

Example:
    # Add cities that might be confused with other words
    add_custom_city("Reading")  # Could be confused with the verb
    add_custom_city("Mobile")   # Could be confused with the adjective

```python
df = df.add_custom_city(city_name)
```

#### `add_custom_state(full_name: str, abbreviation: str)`

Add a custom state or region to the state mappings.

This allows extending the address parser to handle non-US states/provinces.

Args:
    full_name: Full name of the state/province (e.g., "ONTARIO")
    abbreviation: Two-letter abbreviation (e.g., "ON")

Example:
    # Add Canadian provinces
    add_custom_state("ONTARIO", "ON")
    add_custom_state("QUEBEC", "QC")
    add_custom_state("BRITISH COLUMBIA", "BC")

```python
df = df.add_custom_state(full_name, abbreviation)
```

#### `extract_apartment_number(col: Column)`

Extract apartment/unit number from address.

Extracts apartment, suite, unit, or room numbers including:
Apt 5B, Suite 200, Unit 12, #4A, Rm 101, etc.

Args:
    col: Column containing address text

Returns:
    Column with extracted apartment/unit number or empty string

Example:
    df.select(addresses.extract_apartment_number(F.col("address")))
    # "123 Main St Apt 5B" -> "5B"
    # "456 Oak Ave Suite 200" -> "200"
    # "789 Elm St #4A" -> "4A"

```python
df = df.extract_apartment_number(col)
```

#### `extract_building(col: Column)`

Extract building name or identifier from address.

Extracts building information like:
Building A, Tower 2, Complex B, Block C, etc.

Args:
    col: Column containing address text

Returns:
    Column with extracted building identifier or empty string

Example:
    df.select(addresses.extract_building(F.col("address")))
    # "123 Main St, Building A" -> "A"
    # "456 Oak Ave, Tower 2" -> "2"
    # "789 Elm St, Complex North" -> "North"

```python
df = df.extract_building(col)
```

#### `extract_city(col: Column, custom_cities: Optional[List] = None)`

Extract city name from US address text.

Extracts city by finding text before state abbreviation or ZIP code.
Handles various formats including comma-separated and multi-word cities.

Args:
    col: Column containing address text
    custom_cities: Optional list of custom city names to recognize (case-insensitive)

Returns:
    Column with extracted city name or empty string if not found

Example:
    # Direct usage
    df.select(addresses.extract_city(F.col("address")))

    # With custom cities
    df.select(addresses.extract_city(F.col("address"), custom_cities=["Reading", "Mobile"]))

    # Pre-configured
    extract_city_custom = addresses.extract_city(custom_cities=["Reading", "Mobile"])
    df.select(extract_city_custom(F.col("address")))

```python
df = df.extract_city(col, custom_cities)
```

#### `extract_country(col: Column)`

Extract country from address.

Extracts country names from addresses, handling common variations
and abbreviations. Returns standardized country name.

Args:
    col: Column containing address text with potential country

Returns:
    Column with extracted country name or empty string

Example:
    df.select(addresses.extract_country(F.col("address")))
    # "123 Main St, New York, USA" -> "USA"
    # "456 Oak Ave, Toronto, Canada" -> "Canada"
    # "789 Elm St, London, UK" -> "United Kingdom"

```python
df = df.extract_country(col)
```

#### `extract_floor(col: Column)`

Extract floor number from address.

Extracts floor information like:
5th Floor, Floor 2, Fl 3, Level 4, etc.

Args:
    col: Column containing address text

Returns:
    Column with extracted floor number or empty string

Example:
    df.select(addresses.extract_floor(F.col("address")))
    # "123 Main St, 5th Floor" -> "5"
    # "456 Oak Ave, Floor 2" -> "2"
    # "789 Elm St, Level 3" -> "3"

```python
df = df.extract_floor(col)
```

#### `extract_full_street(col: Column)`

Extract complete street address (number + prefix + name + suffix).

Extracts everything before apartment/suite and city/state/zip.

Args:
    col: Column containing address text

Returns:
    Column with extracted street address or empty string

Example:
    df.select(addresses.extract_full_street(F.col("address")))
    # "123 N Main St, Apt 4B, New York, NY" -> "123 N Main St"

```python
df = df.extract_full_street(col)
```

#### `extract_po_box(col: Column)`

Extract PO Box number from address.

Extracts PO Box, P.O. Box, POB, Post Office Box numbers.
Handles various formats including with/without periods and spaces.

Args:
    col: Column containing address text

Returns:
    Column with extracted PO Box number or empty string

Example:
    df.select(addresses.extract_po_box(F.col("address")))
    # "PO Box 123" -> "123"
    # "P.O. Box 456" -> "456"
    # "POB 789" -> "789"
    # "Post Office Box 1011" -> "1011"

```python
df = df.extract_po_box(col)
```

#### `extract_private_mailbox(col: Column)`

Extract private mailbox (PMB) number from address.

Extracts PMB or Private Mail Box numbers, commonly used with
commercial mail receiving agencies (like UPS Store).

Args:
    col: Column containing address text

Returns:
    Column with extracted PMB number or empty string

Example:
    df.select(addresses.extract_private_mailbox(F.col("address")))
    # "123 Main St PMB 456" -> "456"
    # "789 Oak Ave #101 PMB 12" -> "12"

```python
df = df.extract_private_mailbox(col)
```

#### `extract_secondary_address(col: Column)`

Extract complete secondary address information (unit type + number).

Combines unit type and number into standard format:
"Apt 5B", "Ste 200", "Unit 12", etc.

Args:
    col: Column containing address text

Returns:
    Column with complete secondary address or empty string

Example:
    df.select(addresses.extract_secondary_address(F.col("address")))
    # "123 Main St Apt 5B" -> "Apt 5B"
    # "456 Oak Ave, Suite 200" -> "Suite 200"
    # "789 Elm St" -> ""

```python
df = df.extract_secondary_address(col)
```

#### `extract_state(col: Column, custom_states: Optional[Dict] = None)`

Extract and standardize state to 2-letter abbreviation.

Handles both full state names and abbreviations, case-insensitive.
Returns standardized 2-letter uppercase abbreviation.

Args:
    col: Column containing address text with state information
    custom_states: Optional dict mapping full state names to abbreviations
                  e.g., {"ONTARIO": "ON", "QUEBEC": "QC"}

Returns:
    Column with 2-letter state abbreviation or empty string if not found

Example:
    # Direct usage
    df.select(addresses.extract_state(F.col("address")))

    # With custom states (e.g., Canadian provinces)
    canadian_provinces = {"ONTARIO": "ON", "QUEBEC": "QC", "BRITISH COLUMBIA": "BC"}
    df.select(addresses.extract_state(F.col("address"), custom_states=canadian_provinces))

```python
df = df.extract_state(col, custom_states)
```

#### `extract_street_name(col: Column)`

Extract street name from address.

Extracts the main street name, excluding number, prefix, and suffix.

Args:
    col: Column containing address text

Returns:
    Column with extracted street name or empty string

Example:
    df.select(addresses.extract_street_name(F.col("address")))
    # "123 N Main Street" -> "Main"
    # "456 Oak Avenue" -> "Oak"
    # "789 Martin Luther King Jr Blvd" -> "Martin Luther King Jr"

```python
df = df.extract_street_name(col)
```

#### `extract_street_number(col: Column)`

Extract street/house number from address.

Extracts the numeric portion at the beginning of an address.
Handles various formats: 123, 123A, 123-125, etc.

Args:
    col: Column containing address text

Returns:
    Column with extracted street number or empty string

Example:
    df.select(addresses.extract_street_number(F.col("address")))
    # "123 Main St" -> "123"
    # "123A Oak Ave" -> "123A"
    # "123-125 Elm St" -> "123-125"

```python
df = df.extract_street_number(col)
```

#### `extract_street_prefix(col: Column)`

Extract directional prefix from street address.

Extracts directional prefixes like N, S, E, W, NE, NW, SE, SW.

Args:
    col: Column containing address text

Returns:
    Column with extracted street prefix or empty string

Example:
    df.select(addresses.extract_street_prefix(F.col("address")))
    # "123 N Main St" -> "N"
    # "456 South Oak Ave" -> "South"

```python
df = df.extract_street_prefix(col)
```

#### `extract_street_suffix(col: Column)`

Extract street type/suffix from address.

Extracts street type like Street, Avenue, Road, Boulevard, etc.

Args:
    col: Column containing address text

Returns:
    Column with extracted street suffix or empty string

Example:
    df.select(addresses.extract_street_suffix(F.col("address")))
    # "123 Main Street" -> "Street"
    # "456 Oak Ave" -> "Ave"
    # "789 Elm Boulevard" -> "Boulevard"

```python
df = df.extract_street_suffix(col)
```

#### `extract_unit_type(col: Column)`

Extract the type of unit (Apt, Suite, Unit, etc.) from address.

Args:
    col: Column containing address text

Returns:
    Column with unit type or empty string

Example:
    df.select(addresses.extract_unit_type(F.col("address")))
    # "123 Main St Apt 5B" -> "Apt"
    # "456 Oak Ave Suite 200" -> "Suite"
    # "789 Elm St Unit 12" -> "Unit"

```python
df = df.extract_unit_type(col)
```

#### `extract_zip_code(col: Column)`

Extract US ZIP code (5-digit or ZIP+4 format) from text.

Returns empty string for null/invalid inputs.

```python
df = df.extract_zip_code(col)
```

#### `format_secondary_address(unit_type: Column, unit_number: Column)`

Format unit type and number into standard secondary address.

Note: This is a helper function, not registered with addresses primitive.
Use it directly with two columns.

Args:
    unit_type: Column containing unit type (Apt, Suite, etc.)
    unit_number: Column containing unit number (5B, 200, etc.)

Returns:
    Column with formatted secondary address

Example:
    from datacompose.transformers.text.clean_addresses.pyspark.pyspark_udf import format_secondary_address
    df.select(format_secondary_address(F.lit("Apartment"), F.lit("5B")))
    # -> "Apt 5B"

```python
df = df.format_secondary_address(unit_type, unit_number)
```

#### `get_state_name(col: Column)`

Convert state abbreviation to full name.

Args:
    col: Column containing 2-letter state abbreviations

Returns:
    Column with full state names (title case) or empty string if invalid

```python
df = df.get_state_name(col)
```

#### `get_zip_code_type(col: Column)`

Determine the type of ZIP code.

Args:
    col (Column): Column containing ZIP codes

Returns:
    Column: String column with values: "standard", "plus4", "invalid", or "empty"

```python
df = df.get_zip_code_type(col)
```

#### `has_apartment(col: Column)`

Check if address contains apartment/unit information.

Args:
    col: Column containing address text

Returns:
    Column with boolean indicating presence of apartment/unit

Example:
    df.select(addresses.has_apartment(F.col("address")))
    # "123 Main St Apt 5B" -> True
    # "456 Oak Ave" -> False

```python
df = df.has_apartment(col)
```

#### `has_country(col: Column)`

Check if address contains country information.

Args:
    col: Column containing address text

Returns:
    Column with boolean indicating presence of country

Example:
    df.select(addresses.has_country(F.col("address")))
    # "123 Main St, USA" -> True
    # "456 Oak Ave" -> False

```python
df = df.has_country(col)
```

#### `has_po_box(col: Column)`

Check if address contains PO Box.

Args:
    col: Column containing address text

Returns:
    Column with boolean indicating presence of PO Box

Example:
    df.select(addresses.has_po_box(F.col("address")))
    # "PO Box 123" -> True
    # "123 Main St" -> False

```python
df = df.has_po_box(col)
```

#### `is_po_box_only(col: Column)`

Check if address is ONLY a PO Box (no street address).

Args:
    col: Column containing address text

Returns:
    Column with boolean indicating if address is PO Box only

Example:
    df.select(addresses.is_po_box_only(F.col("address")))
    # "PO Box 123" -> True
    # "123 Main St, PO Box 456" -> False
    # "PO Box 789, New York, NY" -> True

```python
df = df.is_po_box_only(col)
```

#### `is_valid_zip_code(col: Column)`

Alias for validate_zip_code for consistency.

Args:
    col (Column): Column containing ZIP codes to validate

Returns:
    Column: Boolean column indicating if ZIP code is valid

```python
df = df.is_valid_zip_code(col)
```

#### `remove_country(col: Column)`

Remove country from address.

Removes country information from the end of addresses.

Args:
    col: Column containing address text

Returns:
    Column with country removed

Example:
    df.select(addresses.remove_country(F.col("address")))
    # "123 Main St, New York, USA" -> "123 Main St, New York"
    # "456 Oak Ave, Toronto, Canada" -> "456 Oak Ave, Toronto"

```python
df = df.remove_country(col)
```

#### `remove_custom_city(city_name: str)`

Remove a custom city from the set.

Args:
    city_name: Name of the city to remove

```python
df = df.remove_custom_city(city_name)
```

#### `remove_custom_state(identifier: str)`

Remove a custom state from the mappings.

Args:
    identifier: Either the full name or abbreviation of the state to remove

```python
df = df.remove_custom_state(identifier)
```

#### `remove_po_box(col: Column)`

Remove PO Box from address.

Removes PO Box information while preserving other address components.

Args:
    col: Column containing address text

Returns:
    Column with PO Box removed

Example:
    df.select(addresses.remove_po_box(F.col("address")))
    # "123 Main St, PO Box 456" -> "123 Main St"
    # "PO Box 789, New York, NY" -> "New York, NY"

```python
df = df.remove_po_box(col)
```

#### `remove_secondary_address(col: Column)`

Remove apartment/suite/unit information from address.

Removes secondary address components to get clean street address.

Args:
    col: Column containing address text

Returns:
    Column with secondary address removed

Example:
    df.select(addresses.remove_secondary_address(F.col("address")))
    # "123 Main St Apt 5B" -> "123 Main St"
    # "456 Oak Ave, Suite 200" -> "456 Oak Ave"

```python
df = df.remove_secondary_address(col)
```

#### `split_zip_code(col: Column)`

Split ZIP+4 code into base and extension components.

Args:
    col (Column): Column containing ZIP codes

Returns:
    Column: Struct with 'base' and 'extension' fields

```python
df = df.split_zip_code(col)
```

#### `standardize_city(col: Column, custom_mappings: Optional[Dict] = None)`

Standardize city name formatting.

- Trims whitespace
- Normalizes internal spacing
- Applies title case (with special handling for common patterns)
- Optionally applies custom city name mappings

Args:
    col: Column containing city names to standardize
    custom_mappings: Optional dict for city name corrections/standardization
                    e.g., {"ST LOUIS": "St. Louis", "NEWYORK": "New York"}

Returns:
    Column with standardized city names

Example:
    # Basic standardization
    df.select(addresses.standardize_city(F.col("city")))

    # With custom mappings for common variations
    city_mappings = {
        "NYC": "New York",
        "LA": "Los Angeles",
        "SF": "San Francisco",
        "STLOUIS": "St. Louis"
    }
    df.select(addresses.standardize_city(F.col("city"), custom_mappings=city_mappings))

```python
df = df.standardize_city(col, custom_mappings)
```

#### `standardize_country(col: Column, custom_mappings: Optional[dict] = None)`

Standardize country name to consistent format.

Converts various country representations to standard names.

Args:
    col: Column containing country name or abbreviation
    custom_mappings: Optional dict of custom country mappings

Returns:
    Column with standardized country name

Example:
    df.select(addresses.standardize_country(F.col("country")))
    # "US" -> "USA"
    # "U.K." -> "United Kingdom"
    # "Deutschland" -> "Germany"

```python
df = df.standardize_country(col, custom_mappings)
```

#### `standardize_po_box(col: Column)`

Standardize PO Box format to consistent representation.

Converts various PO Box formats to standard "PO Box XXXX" format.

Args:
    col: Column containing PO Box text

Returns:
    Column with standardized PO Box format

Example:
    df.select(addresses.standardize_po_box(F.col("po_box")))
    # "P.O. Box 123" -> "PO Box 123"
    # "POB 456" -> "PO Box 456"
    # "Post Office Box 789" -> "PO Box 789"
    # "123 Main St" -> "123 Main St" (no change if no PO Box)

```python
df = df.standardize_po_box(col)
```

#### `standardize_state(col: Column)`

Convert state to standard 2-letter format.

Converts full names to abbreviations and ensures uppercase.

Args:
    col: Column containing state names or abbreviations

Returns:
    Column with standardized 2-letter state codes

```python
df = df.standardize_state(col)
```

#### `standardize_street_prefix(col: Column, custom_mappings: Optional[Dict[str, str]] = None)`

Standardize street directional prefixes to abbreviated form.

Converts all variations to standard USPS abbreviations:
North/N/N. → N, South/S/S. → S, etc.

Args:
    col: Column containing street prefix
    custom_mappings: Optional dict of custom prefix mappings (case insensitive)

Returns:
    Column with standardized prefix (always abbreviated per USPS standards)

Example:
    df.select(addresses.standardize_street_prefix(F.col("prefix")))
    # "North" -> "N"
    # "south" -> "S"
    # "NorthEast" -> "NE"

```python
df = df.standardize_street_prefix(col, custom_mappings)
```

#### `standardize_street_suffix(col: Column, custom_mappings: Optional[Dict[str, str]] = None)`

Standardize street type/suffix to USPS abbreviated form.

Converts all variations to standard USPS abbreviations per the config:
Street/St/St. → St, Avenue/Ave/Av → Ave, Boulevard → Blvd, etc.

Args:
    col: Column containing street suffix
    custom_mappings: Optional dict of custom suffix mappings (case insensitive)

Returns:
    Column with standardized suffix (always abbreviated per USPS standards)

Example:
    df.select(addresses.standardize_street_suffix(F.col("suffix")))
    # "Street" -> "St"
    # "avenue" -> "Ave"
    # "BOULEVARD" -> "Blvd"

```python
df = df.standardize_street_suffix(col, custom_mappings)
```

#### `standardize_unit_type(col: Column, custom_mappings: Optional[Dict[str, str]] = None)`

Standardize unit type to common abbreviations.

Converts all variations to standard abbreviations:
Apartment/Apt. → Apt, Suite → Ste, Room → Rm, etc.

Args:
    col: Column containing unit type
    custom_mappings: Optional dict of custom unit type mappings

Returns:
    Column with standardized unit type

Example:
    df.select(addresses.standardize_unit_type(F.col("unit_type")))
    # "Apartment" -> "Apt"
    # "Suite" -> "Ste"
    # "Room" -> "Rm"

```python
df = df.standardize_unit_type(col, custom_mappings)
```

#### `standardize_zip_code(col: Column)`

Standardize ZIP code format.

- Removes extra spaces
- Ensures proper dash placement for ZIP+4
- Returns empty string for invalid formats

Args:
    col (Column): Column containing ZIP codes to standardize

Returns:
    Column: Standardized ZIP code or empty string if invalid

```python
df = df.standardize_zip_code(col)
```

#### `validate_city(col: Column, known_cities: Optional[List] = None, min_length: int = 2, max_length: int = 50)`

Validate if a city name appears valid.

Validates:
- Not empty/null
- Within reasonable length bounds
- Contains valid characters (letters, spaces, hyphens, apostrophes, periods)
- Optionally: matches a list of known cities

Args:
    col: Column containing city names to validate
    known_cities: Optional list of valid city names to check against
    min_length: Minimum valid city name length (default 2)
    max_length: Maximum valid city name length (default 50)

Returns:
    Boolean column indicating if city name is valid

Example:
    # Basic validation
    df.select(addresses.validate_city(F.col("city")))

    # Validate against known cities
    us_cities = ["New York", "Los Angeles", "Chicago", ...]
    df.select(addresses.validate_city(F.col("city"), known_cities=us_cities))

```python
df = df.validate_city(col, known_cities, min_length, max_length)
```

#### `validate_state(col: Column)`

Validate if state code is a valid US state abbreviation.

Checks against list of valid US state abbreviations including territories.

Args:
    col: Column containing state codes to validate

Returns:
    Boolean column indicating if state code is valid

```python
df = df.validate_state(col)
```

#### `validate_zip_code(col: Column)`

Validate if a ZIP code is in correct US format.

Validates:
- 5-digit format (e.g., "12345")
- ZIP+4 format (e.g., "12345-6789")
- Not all zeros (except "00000" which is technically valid)
- Within valid range (00001-99999 for base ZIP)

Args:
    col (Column): Column containing ZIP codes to validate

Returns:
    Column: Boolean column indicating if ZIP code is valid

```python
df = df.validate_zip_code(col)
```
