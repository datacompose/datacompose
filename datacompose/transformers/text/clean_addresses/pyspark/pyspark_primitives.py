import re
from typing import TYPE_CHECKING, Dict, List, Optional

if TYPE_CHECKING:
    # For type checkers only - these imports are always available during type checking
    from pyspark.sql import Column
    from pyspark.sql import functions as F
else:
    # At runtime, handle missing PySpark gracefully
    try:
        from pyspark.sql import Column
        from pyspark.sql import functions as F
    except ImportError:
        # PySpark is not installed - functions will fail at runtime if called
        pass

try:
    # Try local utils import first (for generated code)
    from utils.primitives import PrimitiveRegistry 
except ImportError:
    # Fall back to installed datacompose package
    from datacompose.operators.primitives import PrimitiveRegistry

addresses = PrimitiveRegistry("addresses")

# US State mappings - comprehensive list including territories
# These are mutable to allow extension
US_STATES = {
    "ALABAMA": "AL",
    "ALASKA": "AK",
    "ARIZONA": "AZ",
    "ARKANSAS": "AR",
    "CALIFORNIA": "CA",
    "COLORADO": "CO",
    "CONNECTICUT": "CT",
    "DELAWARE": "DE",
    "FLORIDA": "FL",
    "GEORGIA": "GA",
    "HAWAII": "HI",
    "IDAHO": "ID",
    "ILLINOIS": "IL",
    "INDIANA": "IN",
    "IOWA": "IA",
    "KANSAS": "KS",
    "KENTUCKY": "KY",
    "LOUISIANA": "LA",
    "MAINE": "ME",
    "MARYLAND": "MD",
    "MASSACHUSETTS": "MA",
    "MICHIGAN": "MI",
    "MINNESOTA": "MN",
    "MISSISSIPPI": "MS",
    "MISSOURI": "MO",
    "MONTANA": "MT",
    "NEBRASKA": "NE",
    "NEVADA": "NV",
    "NEW HAMPSHIRE": "NH",
    "NEW JERSEY": "NJ",
    "NEW MEXICO": "NM",
    "NEW YORK": "NY",
    "NORTH CAROLINA": "NC",
    "NORTH DAKOTA": "ND",
    "OHIO": "OH",
    "OKLAHOMA": "OK",
    "OREGON": "OR",
    "PENNSYLVANIA": "PA",
    "RHODE ISLAND": "RI",
    "SOUTH CAROLINA": "SC",
    "SOUTH DAKOTA": "SD",
    "TENNESSEE": "TN",
    "TEXAS": "TX",
    "UTAH": "UT",
    "VERMONT": "VT",
    "VIRGINIA": "VA",
    "WASHINGTON": "WA",
    "WEST VIRGINIA": "WV",
    "WISCONSIN": "WI",
    "WYOMING": "WY",
    # US Territories and DC
    "DISTRICT OF COLUMBIA": "DC",
    "PUERTO RICO": "PR",
    "VIRGIN ISLANDS": "VI",
    "GUAM": "GU",
    "AMERICAN SAMOA": "AS",
    "NORTHERN MARIANA ISLANDS": "MP",
}

# Reverse mapping: abbreviation to full name
STATE_ABBREV = {
    "AL": "ALABAMA",
    "AK": "ALASKA",
    "AZ": "ARIZONA",
    "AR": "ARKANSAS",
    "CA": "CALIFORNIA",
    "CO": "COLORADO",
    "CT": "CONNECTICUT",
    "DE": "DELAWARE",
    "FL": "FLORIDA",
    "GA": "GEORGIA",
    "HI": "HAWAII",
    "ID": "IDAHO",
    "IL": "ILLINOIS",
    "IN": "INDIANA",
    "IA": "IOWA",
    "KS": "KANSAS",
    "KY": "KENTUCKY",
    "LA": "LOUISIANA",
    "ME": "MAINE",
    "MD": "MARYLAND",
    "MA": "MASSACHUSETTS",
    "MI": "MICHIGAN",
    "MN": "MINNESOTA",
    "MS": "MISSISSIPPI",
    "MO": "MISSOURI",
    "MT": "MONTANA",
    "NE": "NEBRASKA",
    "NV": "NEVADA",
    "NH": "NEW HAMPSHIRE",
    "NJ": "NEW JERSEY",
    "NM": "NEW MEXICO",
    "NY": "NEW YORK",
    "NC": "NORTH CAROLINA",
    "ND": "NORTH DAKOTA",
    "OH": "OHIO",
    "OK": "OKLAHOMA",
    "OR": "OREGON",
    "PA": "PENNSYLVANIA",
    "RI": "RHODE ISLAND",
    "SC": "SOUTH CAROLINA",
    "SD": "SOUTH DAKOTA",
    "TN": "TENNESSEE",
    "TX": "TEXAS",
    "UT": "UTAH",
    "VT": "VERMONT",
    "VA": "VIRGINIA",
    "WA": "WASHINGTON",
    "WV": "WEST VIRGINIA",
    "WI": "WISCONSIN",
    "WY": "WYOMING",
    # US Territories and DC
    "DC": "DISTRICT OF COLUMBIA",
    "PR": "PUERTO RICO",
    "VI": "VIRGIN ISLANDS",
    "GU": "GUAM",
    "AS": "AMERICAN SAMOA",
    "MP": "NORTHERN MARIANA ISLANDS",
}

# Custom cities that users want to recognize
# Users can add to this list for better city extraction
CUSTOM_CITIES = set()


def add_custom_state(full_name: str, abbreviation: str) -> None:
    """Add a custom state or region to the state mappings.

    This allows extending the address parser to handle non-US states/provinces.

    Args:
        full_name: Full name of the state/province (e.g., "ONTARIO")
        abbreviation: Two-letter abbreviation (e.g., "ON")

    Example:
        # Add Canadian provinces
        add_custom_state("ONTARIO", "ON")
        add_custom_state("QUEBEC", "QC")
        add_custom_state("BRITISH COLUMBIA", "BC")
    """
    full_name_upper = full_name.upper()
    abbrev_upper = abbreviation.upper()

    US_STATES[full_name_upper] = abbrev_upper
    STATE_ABBREV[abbrev_upper] = full_name_upper


def add_custom_city(city_name: str) -> None:
    """Add a custom city name to improve city extraction.

    This is useful for cities that might be ambiguous or hard to extract.

    Args:
        city_name: Name of the city to add

    Example:
        # Add cities that might be confused with other words
        add_custom_city("Reading")  # Could be confused with the verb
        add_custom_city("Mobile")   # Could be confused with the adjective
    """
    CUSTOM_CITIES.add(city_name.upper())


def remove_custom_state(identifier: str) -> None:
    """Remove a custom state from the mappings.

    Args:
        identifier: Either the full name or abbreviation of the state to remove
    """
    identifier_upper = identifier.upper()

    # Check if it's an abbreviation
    if identifier_upper in STATE_ABBREV:
        full_name = STATE_ABBREV[identifier_upper]
        del STATE_ABBREV[identifier_upper]
        if full_name in US_STATES:
            del US_STATES[full_name]
    # Check if it's a full name
    elif identifier_upper in US_STATES:
        abbrev = US_STATES[identifier_upper]
        del US_STATES[identifier_upper]
        if abbrev in STATE_ABBREV:
            del STATE_ABBREV[abbrev]


def remove_custom_city(city_name: str) -> None:
    """Remove a custom city from the set.

    Args:
        city_name: Name of the city to remove
    """
    CUSTOM_CITIES.discard(city_name.upper())


@addresses.register()
def extract_street_number(col: Column) -> Column:
    """Extract street/house number from address.

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
    """
    # Pattern to match house/building numbers at the start (after optional whitespace)
    # Matches: 123, 123A, 123-125, 123½, etc.
    pattern = r"^\s*(\d+[\w\-/]*)\b"
    result = F.regexp_extract(col, pattern, 1)
    # Return empty string for null results
    return F.when(result.isNull() | (col.isNull()), F.lit("")).otherwise(result)


@addresses.register()
def extract_street_prefix(col: Column) -> Column:
    """Extract directional prefix from street address.

    Extracts directional prefixes like N, S, E, W, NE, NW, SE, SW.

    Args:
        col: Column containing address text

    Returns:
        Column with extracted street prefix or empty string

    Example:
        df.select(addresses.extract_street_prefix(F.col("address")))
        # "123 N Main St" -> "N"
        # "456 South Oak Ave" -> "South"
    """
    # Handle nulls
    col = F.when(col.isNull(), F.lit("")).otherwise(col)

    # Remove house number first (after trimming leading whitespace)
    without_number = F.regexp_replace(col, r"^\s*\d+[\w\-/]*\s*", "")

    # Pattern for directional prefixes - case insensitive
    # Capture the prefix including optional period
    prefix_pattern = r"^(?i)(North|South|East|West|Northeast|Northwest|Southeast|Southwest|N\.?|S\.?|E\.?|W\.?|NE\.?|NW\.?|SE\.?|SW\.?)\b"

    result = F.regexp_extract(without_number, prefix_pattern, 1)
    return F.when(result.isNull(), F.lit("")).otherwise(result)


@addresses.register()
def extract_street_name(col: Column) -> Column:
    """Extract street name from address.

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
    """
    # Handle nulls
    col = F.when(col.isNull(), F.lit("")).otherwise(col)

    # Common street suffixes to identify end of street name
    # Using abbreviated forms from the YAML config
    suffixes = [
        "Street",
        "St",
        "Avenue",
        "Ave",
        "Road",
        "Rd",
        "Boulevard",
        "Blvd",
        "Drive",
        "Dr",
        "Lane",
        "Ln",
        "Court",
        "Ct",
        "Place",
        "Pl",
        "Circle",
        "Cir",
        "Trail",
        "Trl",
        "Parkway",
        "Pkwy",
        "Highway",
        "Hwy",
        "Way",
        "Terrace",
        "Ter",
        "Plaza",
        "Plz",
        "Square",
        "Sq",
        "Loop",
        "Crescent",
        "Cres",
    ]

    # Remove house number only if followed by more text (not just a suffix)
    # This preserves numbered streets like "5th Avenue" while removing "123 Main St"
    # Check if we have a pattern like "number word suffix" vs just "number suffix"
    # Trim leading whitespace first
    trimmed_col = F.trim(col)
    without_number = F.when(
        # If it's just a numbered street (e.g., "5th Avenue", "1st Street")
        trimmed_col.rlike(r"^(?i)\d+(?:st|nd|rd|th)\s+(?:" + "|".join(suffixes) + r")$"),
        trimmed_col  # Keep as is - it's a numbered street name
    ).otherwise(
        # Otherwise remove the house number
        F.regexp_replace(trimmed_col, r"^\d+[\w\-/]*\s+", "")
    )

    # Remove directional prefix - case insensitive
    # Include full directional words and abbreviations
    prefix_pattern = (
        r"^(?i)(?:North|South|East|West|Northeast|Northwest|Southeast|Southwest|N\.?|S\.?|E\.?|W\.?|NE\.?|NW\.?|SE\.?|SW\.?)\s+"
    )
    without_prefix = F.regexp_replace(without_number, prefix_pattern, "")

    # Extract everything before the street suffix - case insensitive
    suffix_pattern = r"^(?i)(.+?)\s+(?:" + "|".join(suffixes) + r")\b"
    street_name = F.regexp_extract(without_prefix, suffix_pattern, 1)

    # If no suffix found, try to extract before comma or end
    street_name = F.when(street_name != "", street_name).otherwise(
        F.regexp_extract(without_prefix, r"^([^,]+?)(?:\s*,|\s*$)", 1)
    )

    return F.trim(street_name)


@addresses.register()
def extract_street_suffix(col: Column) -> Column:
    """Extract street type/suffix from address.

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
    """
    # Handle nulls
    col = F.when(col.isNull(), F.lit("")).otherwise(col)

    # Comprehensive list of street suffixes (both full and abbreviated)
    suffixes = [
        "Street",
        "St",
        "Avenue",
        "Ave",
        "Road",
        "Rd",
        "Boulevard",
        "Blvd",
        "Drive",
        "Dr",
        "Lane",
        "Ln",
        "Court",
        "Ct",
        "Place",
        "Pl",
        "Circle",
        "Cir",
        "Trail",
        "Trl",
        "Parkway",
        "Pkwy",
        "Highway",
        "Hwy",
        "Way",
        "Terrace",
        "Ter",
        "Plaza",
        "Plz",
        "Square",
        "Sq",
        "Loop",
        "Crescent",
        "Cres",
        "Alley",
        "Aly",
    ]

    # Build pattern to match the LAST suffix in the string
    # This handles cases like "St. James Place" where we want "Place" not "St"
    suffix_pattern = r"\b(" + "|".join(suffixes) + r")\b(?!.*\b(?:" + "|".join(suffixes) + r")\b)"
    
    # Extract the last matching suffix - case insensitive
    suffix_pattern_ci = r"(?i)" + suffix_pattern
    result = F.regexp_extract(col, suffix_pattern_ci, 1)
    return F.when(result.isNull(), F.lit("")).otherwise(result)


@addresses.register()
def extract_full_street(col: Column) -> Column:
    """Extract complete street address (number + prefix + name + suffix).

    Extracts everything before apartment/suite and city/state/zip.

    Args:
        col: Column containing address text

    Returns:
        Column with extracted street address or empty string

    Example:
        df.select(addresses.extract_full_street(F.col("address")))
        # "123 N Main St, Apt 4B, New York, NY" -> "123 N Main St"
    """
    # Handle nulls
    col = F.when(col.isNull(), F.lit("")).otherwise(col)

    # Remove apartment/suite information - case insensitive
    apt_pattern = r"\s*,?\s*(?i)(?:Apt|Apartment|Unit|Suite|Ste|#)\s*[\w\-]+\b"
    without_apt = F.regexp_replace(col, apt_pattern, "")

    # Extract everything before the first comma (usually street part)
    street = F.regexp_extract(without_apt, r"^([^,]+)", 1)

    # If no comma, try to extract before city/state pattern
    # Look for pattern like "Street City" or "Street State ZIP"
    street = F.when(
        street == "",
        F.regexp_extract(
            col, r"^(.+?)\s+[A-Z][a-z]+(?:\s+[A-Z][a-z]+)*\s*,?\s*[A-Z]{2}\s+\d{5}", 1
        ),
    ).otherwise(street)

    return F.trim(street)


@addresses.register()
def standardize_street_prefix(
    col: Column, custom_mappings: Optional[Dict[str, str]] = None
) -> Column:
    """Standardize street directional prefixes to abbreviated form.

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
    """
    # Mapping based on YAML config prefixes (lines 806-814)
    prefix_map = {
        "NORTH": "N",
        "N.": "N",
        "N": "N",
        "SOUTH": "S",
        "S.": "S",
        "S": "S",
        "EAST": "E",
        "E.": "E",
        "E": "E",
        "WEST": "W",
        "W.": "W",
        "W": "W",
        "NORTHEAST": "NE",
        "NE.": "NE",
        "NE": "NE",
        "NORTHWEST": "NW",
        "NW.": "NW",
        "NW": "NW",
        "SOUTHEAST": "SE",
        "SE.": "SE",
        "SE": "SE",
        "SOUTHWEST": "SW",
        "SW.": "SW",
        "SW": "SW",
    }

    # Convert to uppercase for matching
    upper_col = F.upper(F.trim(col))

    # Apply custom mappings first if provided
    result = col
    if custom_mappings:
        for original, standard in custom_mappings.items():
            result = F.when(
                upper_col == F.upper(F.lit(original)), F.lit(standard)
            ).otherwise(result)
        return result

    # Apply default mapping
    result = F.lit("")
    for original, standard in prefix_map.items():
        result = F.when(upper_col == original, F.lit(standard)).otherwise(result)

    return result


@addresses.register()
def standardize_street_suffix(
    col: Column, custom_mappings: Optional[Dict[str, str]] = None
) -> Column:
    """Standardize street type/suffix to USPS abbreviated form.

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
    """
    # Based on YAML config suffixes mapping (lines 824-965)
    # This is a subset of the most common ones
    suffix_map = {
        "STREET": "St",
        "ST": "St",
        "ST.": "St",
        "STR": "St",
        "AVENUE": "Ave",
        "AVE": "Ave",
        "AVE.": "Ave",
        "AV": "Ave",
        "AVEN": "Ave",
        "ROAD": "Rd",
        "RD": "Rd",
        "RD.": "Rd",
        "BOULEVARD": "Blvd",
        "BLVD": "Blvd",
        "BLVD.": "Blvd",
        "BOUL": "Blvd",
        "DRIVE": "Dr",
        "DR": "Dr",
        "DR.": "Dr",
        "DRV": "Dr",
        "DRIV": "Dr",
        "LANE": "Ln",
        "LN": "Ln",
        "LN.": "Ln",
        "COURT": "Ct",
        "CT": "Ct",
        "CT.": "Ct",
        "CRT": "Ct",
        "PLACE": "Pl",
        "PL": "Pl",
        "PL.": "Pl",
        "PLC": "Pl",
        "CIRCLE": "Cir",
        "CIR": "Cir",
        "CIR.": "Cir",
        "CIRC": "Cir",
        "TRAIL": "Trl",
        "TRL": "Trl",
        "TRL.": "Trl",
        "TR": "Trl",
        "PARKWAY": "Pkwy",
        "PKWY": "Pkwy",
        "PKY": "Pkwy",
        "PWAY": "Pkwy",
        "HIGHWAY": "Hwy",
        "HWY": "Hwy",
        "HWY.": "Hwy",
        "HIWAY": "Hwy",
        "WAY": "Way",
        "WY": "Way",
        "TERRACE": "Ter",
        "TER": "Ter",
        "TER.": "Ter",
        "TERR": "Ter",
        "PLAZA": "Plz",
        "PLZ": "Plz",
        "PLZ.": "Plz",
        "PLZA": "Plz",
        "SQUARE": "Sq",
        "SQ": "Sq",
        "SQ.": "Sq",
        "SQR": "Sq",
        "LOOP": "Loop",
        "LP": "Loop",
        "CRESCENT": "Cres",
        "CRES": "Cres",
        "CRES.": "Cres",
        "CRSC": "Cres",
        "ALLEY": "Aly",
        "ALY": "Aly",
        "ALY.": "Aly",
        "ALLY": "Aly",
    }

    # Handle nulls - return empty string for null input
    if col is None:
        return F.lit("")
    col = F.when(col.isNull(), F.lit("")).otherwise(col)
    
    # Convert to uppercase for matching
    upper_col = F.upper(F.trim(col))

    # Start with the original column
    result = col
    
    # Apply custom mappings first if provided (they take precedence)
    if custom_mappings:
        for original, standard in custom_mappings.items():
            result = F.when(
                upper_col == F.upper(F.lit(original)), F.lit(standard)
            ).otherwise(result)
    
    # Then apply standard mappings for anything not already mapped
    # Need to check if result has changed to avoid overwriting custom mappings
    for original, standard in suffix_map.items():
        # Only apply if not already mapped by custom mappings
        if custom_mappings and original.upper() in [k.upper() for k in custom_mappings.keys()]:
            continue
        result = F.when(upper_col == original, F.lit(standard)).otherwise(result)

    return result


@addresses.register()
def extract_apartment_number(col: Column) -> Column:
    """Extract apartment/unit number from address.

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
    """
    # Handle nulls
    col = F.when(col.isNull(), F.lit("")).otherwise(col)

    # Patterns for different unit types - case insensitive
    # Matches: Apt, Apartment, Suite, Ste, Unit, Room, Rm, # followed by alphanumeric
    # Updated to handle fractions (1/2, 3½), decimals (12.5), parentheses and other special cases
    apt_pattern = r"(?i)(?:Apt\.?|Apartment|Suite|Ste\.?|Unit|Room|Rm\.?|#)\s*(\(?[A-Z0-9\-/½¼¾\.]+\)?)"

    result = F.regexp_extract(col, apt_pattern, 1)

    # If no unit type found, check for trailing numbers (e.g., "123 Main St 456")
    if_no_result = F.when(
        result == "", F.regexp_extract(col, r"\s+(\d+[A-Z]?)\s*$", 1)
    ).otherwise(result)

    return F.when(if_no_result.isNull(), F.lit("")).otherwise(if_no_result)


@addresses.register()
def extract_floor(col: Column) -> Column:
    """Extract floor number from address.

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
    """
    # Handle nulls
    col = F.when(col.isNull(), F.lit("")).otherwise(col)

    # Pattern for floor information - case insensitive
    # Matches: 1st Floor, 2nd Floor, 3rd Floor, 4th-99th Floor, Floor 1, Fl. 2, Level 3
    # Updated to handle abbreviated forms like "31st Fl"
    floor_pattern = r"(?i)(?:(\d+)(?:st|nd|rd|th)?\s*(?:Floor|Fl\.?)|Floor\s*(\d+)|Fl\.?\s*(\d+)|Level\s*(\d+))"

    # Extract from any of the capture groups
    floor1 = F.regexp_extract(col, floor_pattern, 1)
    floor2 = F.regexp_extract(col, floor_pattern, 2)
    floor3 = F.regexp_extract(col, floor_pattern, 3)
    floor4 = F.regexp_extract(col, floor_pattern, 4)

    # Return the first non-empty match
    result = F.when(floor1 != "", floor1).otherwise(
        F.when(floor2 != "", floor2).otherwise(
            F.when(floor3 != "", floor3).otherwise(floor4)
        )
    )

    return F.when(result.isNull() | (result == ""), F.lit("")).otherwise(result)


@addresses.register()
def extract_building(col: Column) -> Column:
    """Extract building name or identifier from address.

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
    """
    # Handle nulls
    col = F.when(col.isNull(), F.lit("")).otherwise(col)

    # Pattern for building information - case insensitive
    # Matches: Building A, Bldg 2, Tower B, Complex 3, Block C, Wing D, Blg B
    # Updated to handle multi-word names but stop at commas or other building indicators
    building_pattern = r"(?i)(?:Building|Bldg\.?|Blg|Tower|Complex|Block|Wing)\s+([A-Z0-9]+(?:\s+[A-Z0-9]+)?)"

    # Stop capturing if we hit another building indicator (Floor, Suite, etc.)
    result_raw = F.regexp_extract(col, building_pattern, 1)

    # Clean up - remove anything after Floor, Suite, Apt, etc.
    result = F.regexp_replace(
        result_raw,
        r"(?i)\s+(?:Floor|Fl\.?|Suite|Ste\.?|Apt\.?|Apartment|Unit|Room|Rm\.?).*",
        "",
    )

    return F.when(result.isNull() | (result == ""), F.lit("")).otherwise(result)


@addresses.register()
def extract_unit_type(col: Column) -> Column:
    """Extract the type of unit (Apt, Suite, Unit, etc.) from address.

    Args:
        col: Column containing address text

    Returns:
        Column with unit type or empty string

    Example:
        df.select(addresses.extract_unit_type(F.col("address")))
        # "123 Main St Apt 5B" -> "Apt"
        # "456 Oak Ave Suite 200" -> "Suite"
        # "789 Elm St Unit 12" -> "Unit"
    """
    # Handle nulls
    col = F.when(col.isNull(), F.lit("")).otherwise(col)

    # Pattern to extract unit type - case insensitive
    unit_type_pattern = r"(?i)(Apt\.?|Apartment|Suite|Ste\.?|Unit|Room|Rm\.?|#)"

    result = F.regexp_extract(col, unit_type_pattern, 1)

    # Clean up the result (remove periods, standardize case)
    result = F.when(
        result != "", F.initcap(F.regexp_replace(result, r"\.", ""))
    ).otherwise("")

    return F.when(result.isNull(), F.lit("")).otherwise(result)


@addresses.register()
def standardize_unit_type(
    col: Column, custom_mappings: Optional[Dict[str, str]] = None
) -> Column:
    """Standardize unit type to common abbreviations.

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
    """
    # Handle nulls
    col = F.when(col.isNull(), F.lit("")).otherwise(col)

    # Standard mappings for unit types
    unit_map = {
        "APARTMENT": "Apt",
        "APT.": "Apt",
        "APT": "Apt",
        "SUITE": "Ste",
        "STE.": "Ste",
        "STE": "Ste",
        "UNIT": "Unit",
        "ROOM": "Rm",
        "RM.": "Rm",
        "RM": "Rm",
        "FLOOR": "Fl",
        "FL.": "Fl",
        "FL": "Fl",
        "BUILDING": "Bldg",
        "BLDG.": "Bldg",
        "BLDG": "Bldg",
        "#": "#",
        "NUMBER": "#",
        "NO.": "#",
        "NO": "#",
    }

    # Convert to uppercase for matching
    upper_col = F.upper(F.trim(col))

    # Apply custom mappings first if provided
    result = col
    if custom_mappings:
        for original, standard in custom_mappings.items():
            result = F.when(
                upper_col == F.upper(F.lit(original)), F.lit(standard)
            ).otherwise(result)

    # Then apply standard mappings for anything not custom mapped
    for original, standard in unit_map.items():
        result = F.when(upper_col == original, F.lit(standard)).otherwise(result)

    return result


@addresses.register()
def extract_secondary_address(col: Column) -> Column:
    """Extract complete secondary address information (unit type + number).

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
    """
    # Handle nulls
    col = F.when(col.isNull(), F.lit("")).otherwise(col)

    # Pattern to extract complete secondary address - case insensitive
    secondary_pattern = (
        r"(?i)((?:Apt\.?|Apartment|Suite|Ste\.?|Unit|Room|Rm\.?|#)\s*[A-Z0-9\-]+)"
    )

    result = F.regexp_extract(col, secondary_pattern, 1)
    return F.when(result.isNull(), F.lit("")).otherwise(result)


@addresses.register()
def has_apartment(col: Column) -> Column:
    """Check if address contains apartment/unit information.

    Args:
        col: Column containing address text

    Returns:
        Column with boolean indicating presence of apartment/unit

    Example:
        df.select(addresses.has_apartment(F.col("address")))
        # "123 Main St Apt 5B" -> True
        # "456 Oak Ave" -> False
    """
    # Handle nulls
    col = F.when(col.isNull(), F.lit("")).otherwise(col)

    # Check for apartment/unit patterns
    apt_pattern = (
        r"(?i)(?:Apt\.?|Apartment|Suite|Ste\.?|Unit|Room|Rm\.?|#)\s*[A-Z0-9\-]+"
    )

    # Return boolean
    return F.when(F.regexp_extract(col, apt_pattern, 0) != "", F.lit(True)).otherwise(
        F.lit(False)
    )


@addresses.register()
def remove_secondary_address(col: Column) -> Column:
    """Remove apartment/suite/unit information from address.

    Removes secondary address components to get clean street address.

    Args:
        col: Column containing address text

    Returns:
        Column with secondary address removed

    Example:
        df.select(addresses.remove_secondary_address(F.col("address")))
        # "123 Main St Apt 5B" -> "123 Main St"
        # "456 Oak Ave, Suite 200" -> "456 Oak Ave"
    """
    # Handle nulls
    col = F.when(col.isNull(), F.lit("")).otherwise(col)

    # Pattern to match secondary address components - case insensitive
    # Include optional comma and spaces before
    secondary_pattern = (
        r",?\s*(?i)(?:Apt\.?|Apartment|Suite|Ste\.?|Unit|Room|Rm\.?|#)\s*[A-Z0-9\-]+\b"
    )

    # Remove the pattern and clean up extra spaces
    result = F.regexp_replace(col, secondary_pattern, "")
    result = F.regexp_replace(result, r"\s+", " ")  # Clean multiple spaces
    result = F.trim(result)

    return result


def format_secondary_address(unit_type: Column, unit_number: Column) -> Column:
    """Format unit type and number into standard secondary address.

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
    """
    # Standardize the unit type first
    std_type = standardize_unit_type(unit_type)

    # Combine type and number, handling nulls
    result = F.when(
        (std_type.isNotNull() & (std_type != ""))
        & (unit_number.isNotNull() & (unit_number != "")),
        F.concat_ws(" ", std_type, unit_number),
    ).otherwise(F.lit(""))

    return result


@addresses.register()
def extract_zip_code(col: Column) -> Column:  # type: ignore
    """Extract US ZIP code (5-digit or ZIP+4 format) from text.

    Returns empty string for null/invalid inputs.
    """
    extracted = F.regexp_extract(col, r"\b(\d{5}(?:-\d{4})?)\b", 1)
    # Return empty string instead of null for consistency
    return F.when(extracted.isNull(), F.lit("")).otherwise(extracted)


@addresses.register()
def validate_zip_code(col: Column) -> Column:
    """Validate if a ZIP code is in correct US format.

    Validates:
    - 5-digit format (e.g., "12345")
    - ZIP+4 format (e.g., "12345-6789")
    - Not all zeros (except "00000" which is technically valid)
    - Within valid range (00001-99999 for base ZIP)

    Args:
        col (Column): Column containing ZIP codes to validate

    Returns:
        Column: Boolean column indicating if ZIP code is valid
    """
    # Check if the column matches valid ZIP code pattern
    is_valid_format = F.regexp_extract(col, r"^(\d{5}(?:-\d{4})?)$", 1) != ""

    # Additional validation: not empty/null
    is_not_empty = (col.isNotNull()) & (F.trim(col) != "")

    # Combined validation
    return is_valid_format & is_not_empty


@addresses.register()
def is_valid_zip_code(col: Column) -> "Column":
    """Alias for validate_zip_code for consistency.

    Args:
        col (Column): Column containing ZIP codes to validate

    Returns:
        Column: Boolean column indicating if ZIP code is valid
    """
    return validate_zip_code(col)


@addresses.register()
def standardize_zip_code(col: Column):
    """Standardize ZIP code format.

    - Removes extra spaces
    - Ensures proper dash placement for ZIP+4
    - Returns empty string for invalid formats

    Args:
        col (Column): Column containing ZIP codes to standardize

    Returns:
        Column: Standardized ZIP code or empty string if invalid
    """
    # First extract the ZIP code
    extracted = extract_zip_code(col)

    # Then validate it
    is_valid = validate_zip_code(extracted)

    # Return standardized version or empty string
    return F.when(is_valid, extracted).otherwise(F.lit(""))


@addresses.register()
def get_zip_code_type(col: Column):
    """Determine the type of ZIP code.

    Args:
        col (Column): Column containing ZIP codes

    Returns:
        Column: String column with values: "standard", "plus4", "invalid", or "empty"
    """
    # Check patterns
    is_standard = F.regexp_extract(col, r"^(\d{5})$", 1) != ""
    is_plus4 = F.regexp_extract(col, r"^(\d{5}-\d{4})$", 1) != ""
    is_empty = (col.isNull()) | (F.trim(col) == "")

    return (
        F.when(is_plus4, F.lit("plus4"))
        .when(is_standard, F.lit("standard"))
        .when(is_empty, F.lit("empty"))
        .otherwise(F.lit("invalid"))
    )


@addresses.register()
def split_zip_code(col: Column):
    """Split ZIP+4 code into base and extension components.

    Args:
        col (Column): Column containing ZIP codes

    Returns:
        Column: Struct with 'base' and 'extension' fields
    """
    # Extract base ZIP (first 5 digits)
    base_zip = F.regexp_extract(col, r"^(\d{5})", 1)

    # Extract extension (4 digits after dash, if present)
    extension = F.regexp_extract(col, r"^\d{5}-(\d{4})$", 1)

    # Return as struct
    return F.struct(
        base_zip.alias("base"),
        F.when(extension != "", extension).otherwise(F.lit(None)).alias("extension"),
    )


@addresses.register()
def extract_city(col: Column, custom_cities: Optional[List] = None) -> Column:
    """Extract city name from US address text.

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
    """
    # For city extraction, match both abbreviations and full state names
    # But prioritize abbreviations to avoid false matches
    state_abbrevs_only = list(STATE_ABBREV.keys())
    # Add common full state names for city extraction
    common_full_states = [
        "California",
        "New York",
        "Texas",
        "Florida",
        "Pennsylvania",
        "Illinois",
        "Ohio",
        "Georgia",
        "North Carolina",
        "Michigan",
        "New Jersey",
        "Virginia",
        "Washington",
        "Massachusetts",
        "Arizona",
        "Tennessee",
        "Indiana",
        "Missouri",
        "Maryland",
        "Wisconsin",
        "Colorado",
        "Minnesota",
        "South Carolina",
        "Alabama",
        "Louisiana",
        "Kentucky",
        "Oregon",
        "Oklahoma",
        "Connecticut",
        "Utah",
        "Iowa",
        "Nevada",
        "Arkansas",
        "Mississippi",
        "Kansas",
        "New Mexico",
        "Nebraska",
        "Idaho",
        "West Virginia",
        "Hawaii",
        "New Hampshire",
        "Maine",
        "Montana",
        "Rhode Island",
        "Delaware",
        "South Dakota",
        "North Dakota",
        "Alaska",
        "Vermont",
        "Wyoming",
        "District of Columbia",
        "Puerto Rico",
    ]

    # Combine abbreviations and full names for pattern
    all_state_patterns = state_abbrevs_only + [s.upper() for s in common_full_states]

    # Check for custom cities if provided
    custom_city_result = F.lit("")

    # Use provided custom_cities parameter, or fall back to module-level CUSTOM_CITIES
    cities_to_check = (
        custom_cities if custom_cities is not None else list(CUSTOM_CITIES)
    )

    if cities_to_check:
        # Create a single regex pattern for all custom cities
        # Sort by length (longest first) to match multi-word cities first
        sorted_custom_cities = sorted(cities_to_check, key=len, reverse=True)
        # Ensure cities are strings and uppercase for comparison
        sorted_custom_cities = [str(city).upper() for city in sorted_custom_cities]
        # Build pattern with all custom cities as alternatives
        custom_pattern = (
            r"(?i)\b(?:"
            + "|".join(re.escape(city) for city in sorted_custom_cities)
            + r")\b"
        )
        custom_city_result = F.regexp_extract(col, custom_pattern, 0)

    # Pattern to extract city before a proper state
    # First pattern: try to match city that comes after a comma and before state
    # "anything, City, State" - captures "City"
    city_after_comma_pattern = (
        r"(?i),\s*([^,]+?)\s*,\s*(?:" + "|".join(all_state_patterns) + r")\b"
    )

    # Second pattern: match city at start before state (no street address)
    # "City, State" or "City State ZIP"
    city_at_start_pattern = (
        r"(?i)^([^,]+?)(?:\s*,\s*(?:" + "|".join(all_state_patterns) + r")\b|"
        r"\s+(?:" + "|".join(state_abbrevs_only) + r")\s+\d{5})"
    )

    # Try to extract city using both patterns - prefer after comma (more specific)
    city_after_comma = F.regexp_extract(col, city_after_comma_pattern, 1)
    city_at_start = F.regexp_extract(col, city_at_start_pattern, 1)
    city = F.when(city_after_comma != "", city_after_comma).otherwise(city_at_start)

    # If no state found, try to extract before ZIP code only
    city_from_zip = F.regexp_extract(col, r"^(.+?)\s*(?:,\s*)?\d{5}(?:-\d{4})?\s*$", 1)

    # Use custom city if found, otherwise use regular extraction
    result = F.when(custom_city_result != "", F.initcap(custom_city_result)).otherwise(
        F.coalesce(city, city_from_zip, F.lit(""))
    )
    result = F.trim(F.regexp_replace(result, r"[,\s]+$", ""))

    # Handle case where we might have captured too much (e.g., street info)
    # If result contains common street suffixes, try to extract just the city part
    street_indicators = [
        "Street",
        "St",
        "Avenue",
        "Ave",
        "Road",
        "Rd",
        "Boulevard",
        "Blvd",
        "Drive",
        "Dr",
        "Lane",
        "Ln",
        "Court",
        "Ct",
        "Place",
        "Pl",
    ]
    street_pattern = r"(?i)\b(?:" + "|".join(street_indicators) + r")\b.*?,\s*(.+)$"

    # If we find street indicators, extract what comes after the last comma
    city_after_street = F.regexp_extract(result, street_pattern, 1)

    return F.when(city_after_street != "", city_after_street).otherwise(result)


@addresses.register()
def extract_state(col: Column, custom_states: Optional[Dict] = None) -> Column:
    """Extract and standardize state to 2-letter abbreviation.

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
    """
    # Build combined state mappings
    states_map = US_STATES.copy()
    abbrev_map = STATE_ABBREV.copy()

    # Add custom states if provided
    if custom_states:
        for full_name, abbrev in custom_states.items():
            full_name_upper = str(full_name).upper()
            abbrev_upper = str(abbrev).upper()
            states_map[full_name_upper] = abbrev_upper
            abbrev_map[abbrev_upper] = full_name_upper

    # Create comprehensive state pattern
    all_states = list(states_map.keys()) + list(abbrev_map.keys())

    # Pattern to match state names/abbreviations
    # Look for states that appear before ZIP/postal code or at end of string
    # Support both US ZIP codes (12345) and Canadian postal codes (A1B 2C3)
    state_pattern = (
        r"(?i)\b("
        + "|".join(all_states)
        + r")\b(?:\s+(?:\d{5}(?:-\d{4})?|[A-Z]\d[A-Z]\s*\d[A-Z]\d))?(?:\s*$)"
    )

    # Extract the state (case-insensitive)
    extracted = F.upper(F.regexp_extract(col, state_pattern, 1))

    # Check if it's already a valid abbreviation (including custom ones)
    is_abbrev = extracted.isin(list(abbrev_map.keys()))

    # If it's an abbreviation, return it; otherwise check if it's a full name
    result = F.when(is_abbrev, extracted)

    # Map full state names to abbreviations (including custom ones)
    for full_name, abbrev in states_map.items():
        result = result.when(extracted == full_name, F.lit(abbrev))

    # Default to empty string if no match
    result = result.otherwise(F.lit(""))

    return result


@addresses.register()
def validate_city(
    col: Column,
    known_cities: Optional[List] = None,
    min_length: int = 2,
    max_length: int = 50,
) -> Column:
    """Validate if a city name appears valid.

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
    """
    # Clean the input
    cleaned = F.trim(col)

    # Basic validation: not empty
    not_empty = (cleaned.isNotNull()) & (cleaned != "")

    # Length validation
    length_valid = (F.length(cleaned) >= min_length) & (F.length(cleaned) <= max_length)

    # Character validation: letters, spaces, hyphens, apostrophes, periods, and numbers
    # Allow: St. Louis, O'Fallon, Winston-Salem, 29 Palms, etc.
    char_pattern = r"^[A-Za-z0-9\s\-'.]+$"
    chars_valid = F.regexp_extract(cleaned, char_pattern, 0) != ""

    # Combine basic validations
    basic_valid = not_empty & length_valid & chars_valid

    # If known cities provided, check against them
    if known_cities:
        # Normalize for comparison
        cleaned_upper = F.upper(cleaned)
        known_cities_upper = [str(city).upper() for city in known_cities]
        in_known_list = cleaned_upper.isin(known_cities_upper)
        return basic_valid & in_known_list

    return basic_valid


@addresses.register()
def validate_state(col: Column) -> Column:
    """Validate if state code is a valid US state abbreviation.

    Checks against list of valid US state abbreviations including territories.

    Args:
        col: Column containing state codes to validate

    Returns:
        Boolean column indicating if state code is valid
    """
    # Convert to uppercase for comparison
    upper_col = F.upper(F.trim(col))

    # Check if it's a valid abbreviation
    valid_abbrevs = list(STATE_ABBREV.keys())

    # Also check if it's a valid full state name
    valid_full_names = list(US_STATES.keys())

    return (upper_col.isin(valid_abbrevs)) | (upper_col.isin(valid_full_names))


@addresses.register()
def standardize_city(col: Column, custom_mappings: Optional[Dict] = None) -> Column:
    """Standardize city name formatting.

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
    """
    # Clean and normalize whitespace
    result = F.trim(F.regexp_replace(col, r"\s+", " "))

    # Apply custom mappings if provided
    mapped = F.lit(None)
    if custom_mappings:
        # Normalize mappings to uppercase for comparison
        normalized_mappings = {str(k).upper(): v for k, v in custom_mappings.items()}

        # Start with the original result
        upper_result = F.upper(result)

        # Apply each mapping
        for original, replacement in normalized_mappings.items():
            mapped = F.when(upper_result == original, F.lit(replacement)).otherwise(
                mapped
            )

    # If a mapping was applied, use it; otherwise apply standard formatting
    result = F.when(mapped.isNotNull(), mapped).otherwise(
        # Apply intelligent title case
        F.initcap(result)
    )

    # Fix common patterns that initcap doesn't handle well
    # Only apply these if we didn't use a custom mapping
    result = F.when(
        mapped.isNull(),
        F.regexp_replace(
            F.regexp_replace(
                F.regexp_replace(result, r"\bSt\b", "St."), r"\bFt\b", "Ft."
            ),
            r"\bMt\b",
            "Mt.",
        ),
    ).otherwise(result)

    return result


@addresses.register()
def standardize_state(col: Column) -> Column:
    """Convert state to standard 2-letter format.

    Converts full names to abbreviations and ensures uppercase.

    Args:
        col: Column containing state names or abbreviations

    Returns:
        Column with standardized 2-letter state codes
    """
    # Use extract_state which already does the standardization
    return extract_state(col)


@addresses.register()
def get_state_name(col: Column) -> Column:
    """Convert state abbreviation to full name.

    Args:
        col: Column containing 2-letter state abbreviations

    Returns:
        Column with full state names (title case) or empty string if invalid
    """
    # Convert to uppercase for lookup
    upper_col = F.upper(F.trim(col))

    # Start with empty string as default
    result = F.lit("")

    # Map each abbreviation to its full name
    for abbrev, full_name in STATE_ABBREV.items():
        result = F.when(upper_col == abbrev, F.lit(full_name.title())).otherwise(result)

    return result


# Common country names and their variations
COUNTRIES = {
    # North America
    "USA": [
        "USA",
        "US",
        "U.S.A.",
        "U.S.",
        "United States",
        "United States of America",
        "America",
    ],
    "Canada": ["Canada", "CA", "CAN"],
    "Mexico": ["Mexico", "MX", "MEX"],
    # Europe
    "United Kingdom": [
        "UK",
        "U.K.",
        "United Kingdom",
        "Great Britain",
        "GB",
        "GBR",
        "England",
    ],
    "Germany": ["Germany", "DE", "DEU", "Deutschland"],
    "France": ["France", "FR", "FRA"],
    "Italy": ["Italy", "IT", "ITA", "Italia"],
    "Spain": ["Spain", "ES", "ESP", "España"],
    "Netherlands": ["Netherlands", "NL", "NLD", "Holland"],
    "Belgium": ["Belgium", "BE", "BEL"],
    "Switzerland": ["Switzerland", "CH", "CHE", "Swiss"],
    "Austria": ["Austria", "AT", "AUT"],
    "Poland": ["Poland", "PL", "POL"],
    "Sweden": ["Sweden", "SE", "SWE"],
    "Norway": ["Norway", "NO", "NOR"],
    "Denmark": ["Denmark", "DK", "DNK"],
    "Finland": ["Finland", "FI", "FIN"],
    "Ireland": ["Ireland", "IE", "IRL"],
    "Portugal": ["Portugal", "PT", "PRT"],
    "Greece": ["Greece", "GR", "GRC"],
    # Asia
    "China": ["China", "CN", "CHN", "PRC", "People's Republic of China"],
    "Japan": ["Japan", "JP", "JPN"],
    "India": ["India", "IN", "IND"],
    "South Korea": ["South Korea", "Korea", "KR", "KOR", "Republic of Korea"],
    "Singapore": ["Singapore", "SG", "SGP"],
    "Thailand": ["Thailand", "TH", "THA"],
    "Malaysia": ["Malaysia", "MY", "MYS"],
    "Indonesia": ["Indonesia", "ID", "IDN"],
    "Philippines": ["Philippines", "PH", "PHL"],
    "Vietnam": ["Vietnam", "VN", "VNM"],
    # Oceania
    "Australia": ["Australia", "AU", "AUS"],
    "New Zealand": ["New Zealand", "NZ", "NZL"],
    # South America
    "Brazil": ["Brazil", "BR", "BRA", "Brasil"],
    "Argentina": ["Argentina", "AR", "ARG"],
    "Chile": ["Chile", "CL", "CHL"],
    "Colombia": ["Colombia", "CO", "COL"],
    "Peru": ["Peru", "PE", "PER"],
    # Middle East
    "Israel": ["Israel", "IL", "ISR"],
    "Saudi Arabia": ["Saudi Arabia", "SA", "SAU", "KSA"],
    "UAE": ["UAE", "United Arab Emirates", "AE", "ARE"],
    # Africa
    "South Africa": ["South Africa", "ZA", "ZAF", "RSA"],
    "Egypt": ["Egypt", "EG", "EGY"],
    "Nigeria": ["Nigeria", "NG", "NGA"],
    "Kenya": ["Kenya", "KE", "KEN"],
}

# Create reverse mapping for quick lookups
COUNTRY_LOOKUP = {}
for standard_name, variations in COUNTRIES.items():
    for variation in variations:
        COUNTRY_LOOKUP[variation.upper()] = standard_name


@addresses.register()
def extract_country(col: Column) -> Column:
    """Extract country from address.

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
    """
    # Handle nulls
    col = F.when(col.isNull(), F.lit("")).otherwise(col)

    # Start with empty result
    result = F.lit("")

    # Check for country at the end of the address (most common)
    # Sort variations by length (longest first) to avoid partial matches
    sorted_variations = sorted(
        COUNTRY_LOOKUP.items(), key=lambda x: len(x[0]), reverse=True
    )

    # Pattern to match country at the end, possibly after comma
    for variation, standard in sorted_variations:
        # Check if the address ends with this country variation
        # Use word boundary to avoid partial matches
        pattern = rf"(?:,\s*)?\b{re.escape(variation)}\.?\s*$"
        result = F.when(F.upper(col).rlike(pattern), F.lit(standard)).otherwise(result)

    return result


@addresses.register()
def has_country(col: Column) -> Column:
    """Check if address contains country information.

    Args:
        col: Column containing address text

    Returns:
        Column with boolean indicating presence of country

    Example:
        df.select(addresses.has_country(F.col("address")))
        # "123 Main St, USA" -> True
        # "456 Oak Ave" -> False
    """
    return extract_country(col) != ""


@addresses.register()
def remove_country(col: Column) -> Column:
    """Remove country from address.

    Removes country information from the end of addresses.

    Args:
        col: Column containing address text

    Returns:
        Column with country removed

    Example:
        df.select(addresses.remove_country(F.col("address")))
        # "123 Main St, New York, USA" -> "123 Main St, New York"
        # "456 Oak Ave, Toronto, Canada" -> "456 Oak Ave, Toronto"
    """
    # Handle nulls
    col = F.when(col.isNull(), F.lit("")).otherwise(col)

    result = col

    # Sort variations by length (longest first) to avoid partial matches
    sorted_variations = sorted(COUNTRY_LOOKUP.keys(), key=len, reverse=True)

    # Remove each country variation
    for variation in sorted_variations:
        # Pattern to match country at the end with optional comma and spaces
        # Note: PySpark's regexp_replace uses Java regex, which has different syntax
        # Escape the variation for regex
        escaped = re.escape(variation)
        # Build pattern for case-insensitive matching at end of string
        pattern = f"(?i),?\\s*{escaped}\\.?\\s*$"
        result = F.regexp_replace(result, pattern, "")

    # Clean up any trailing commas or spaces
    result = F.regexp_replace(result, r",?\s*$", "")

    return result


@addresses.register()
def standardize_country(col: Column, custom_mappings: Optional[dict] = None) -> Column:
    """Standardize country name to consistent format.

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
    """
    # Handle nulls
    col = F.when(col.isNull(), F.lit("")).otherwise(col)

    # Clean and normalize
    upper_col = F.upper(F.trim(col))

    # Apply custom mappings first if provided
    result = col
    if custom_mappings:
        for original, standard in custom_mappings.items():
            result = F.when(
                upper_col == F.upper(F.lit(original)), F.lit(standard)
            ).otherwise(result)

    # Then apply standard mappings
    for variation, standard in COUNTRY_LOOKUP.items():
        result = F.when(upper_col == variation, F.lit(standard)).otherwise(result)

    return result


@addresses.register()
def extract_po_box(col: Column) -> Column:
    """Extract PO Box number from address.

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
    """
    # Handle nulls
    col = F.when(col.isNull(), F.lit("")).otherwise(col)

    # Pattern to match various PO Box formats
    # Matches: PO Box, P.O. Box, POB, Post Office Box, etc.
    # Captures the box number (numeric, alphanumeric, or with dashes and special chars)
    # POB must be followed by space and start with number or #
    po_box_pattern = r"(?i)(?:P\.?\s?O\.?\s?Box|POB(?=\s+[#0-9])|Post\s+Office\s+Box)\s+(#?[A-Z0-9\-/]+)"

    result = F.regexp_extract(col, po_box_pattern, 1)
    return F.when(result.isNull(), F.lit("")).otherwise(result)


@addresses.register()
def has_po_box(col: Column) -> Column:
    """Check if address contains PO Box.

    Args:
        col: Column containing address text

    Returns:
        Column with boolean indicating presence of PO Box

    Example:
        df.select(addresses.has_po_box(F.col("address")))
        # "PO Box 123" -> True
        # "123 Main St" -> False
    """
    return extract_po_box(col) != ""


@addresses.register()
def is_po_box_only(col: Column) -> Column:
    """Check if address is ONLY a PO Box (no street address).

    Args:
        col: Column containing address text

    Returns:
        Column with boolean indicating if address is PO Box only

    Example:
        df.select(addresses.is_po_box_only(F.col("address")))
        # "PO Box 123" -> True
        # "123 Main St, PO Box 456" -> False
        # "PO Box 789, New York, NY" -> True
    """
    # Handle nulls
    col = F.when(col.isNull(), F.lit("")).otherwise(col)

    # Check if it has a PO Box
    has_box = has_po_box(col)

    # Check if it has a street number (indicating a street address)
    # Pattern to detect street numbers at the beginning
    street_pattern = r"^\d+\s+[A-Za-z]"
    has_street = F.regexp_extract(col, street_pattern, 0) != ""

    # It's PO Box only if it has a PO Box but no street address
    return has_box & ~has_street


@addresses.register()
def remove_po_box(col: Column) -> Column:
    """Remove PO Box from address.

    Removes PO Box information while preserving other address components.

    Args:
        col: Column containing address text

    Returns:
        Column with PO Box removed

    Example:
        df.select(addresses.remove_po_box(F.col("address")))
        # "123 Main St, PO Box 456" -> "123 Main St"
        # "PO Box 789, New York, NY" -> "New York, NY"
    """
    # Handle nulls
    col = F.when(col.isNull(), F.lit("")).otherwise(col)

    # Pattern to match various PO Box formats with optional comma
    po_box_pattern = r"(?i),?\s*(?:P\.?\s?O\.?\s?Box|POB(?=\s+[#0-9])|Post\s+Office\s+Box)\s+(#?[A-Z0-9\-/]+)\s*,?"

    # Remove the PO Box
    result = F.regexp_replace(col, po_box_pattern, ",")

    # Clean up any leading/trailing commas or spaces
    result = F.regexp_replace(result, r"^\s*,\s*", "")  # Leading comma
    result = F.regexp_replace(result, r",?\s*$", "")  # Trailing comma/space
    result = F.regexp_replace(result, r",\s*,+", ",")  # Multiple commas to single
    result = F.regexp_replace(result, r"\s+", " ")  # Multiple spaces to single

    return F.trim(result)


@addresses.register()
def standardize_po_box(col: Column) -> Column:
    """Standardize PO Box format to consistent representation.

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
    """
    # Handle nulls
    col = F.when(col.isNull(), F.lit("")).otherwise(col)

    # Extract the PO Box number
    box_number = extract_po_box(col)

    # If we found a PO Box, replace it with standard format
    result = F.when(
        box_number != "",
        F.regexp_replace(
            col,
            r"(?i)(?:P\.?\s?O\.?\s?Box|POB(?=\s+[#0-9])|Post\s+Office\s+Box)\s+(#?[A-Z0-9\-/]+)",
            F.concat(F.lit("PO Box "), box_number),
        ),
    ).otherwise(col)

    return result


@addresses.register()
def extract_private_mailbox(col: Column) -> Column:
    """Extract private mailbox (PMB) number from address.

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
    """
    # Handle nulls
    col = F.when(col.isNull(), F.lit("")).otherwise(col)

    # Pattern to match PMB (Private Mail Box)
    pmb_pattern = r"(?i)(?:PMB|Private\s+Mail\s+Box)\s+([A-Z0-9\-]+)"

    result = F.regexp_extract(col, pmb_pattern, 1)
    return F.when(result.isNull(), F.lit("")).otherwise(result)
