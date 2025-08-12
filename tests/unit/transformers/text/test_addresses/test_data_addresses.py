"""
Test data for address cleaning and ZIP code extraction tests.
"""

# Valid ZIP code formats
VALID_ZIP_CODES = [
    ("12345", "12345"),  # Standard 5-digit
    ("00000", "00000"),  # All zeros (valid)
    ("99999", "99999"),  # All nines (valid)
    ("01234", "01234"),  # Leading zero
    ("90210", "90210"),  # Beverly Hills
    ("10001", "10001"),  # NYC
    ("12345-6789", "12345-6789"),  # ZIP+4 format
    ("00501-1234", "00501-1234"),  # Holtsville, NY (lowest ZIP)
    ("99950-9999", "99950-9999"),  # Ketchikan, AK (highest ZIP)
    ("12345-0000", "12345-0000"),  # ZIP+4 with all zeros extension
    ("12345-0001", "12345-0001"),  # ZIP+4 with minimal extension
]

# Invalid ZIP code formats
INVALID_ZIP_CODES = [
    ("1234", ""),  # Too short
    ("123", ""),  # Way too short
    ("1", ""),  # Single digit
    ("123456", ""),  # Too long without dash
    ("1234567890", ""),  # Way too long
    ("12345-", "12345"),  # Dash but no extension (extracts base)
    ("12345-1", "12345"),  # Extension too short
    ("12345-12", "12345"),  # Extension too short
    ("12345-123", "12345"),  # Extension too short
    ("12345-12345", "12345"),  # Extension too long
    ("12345-678", "12345"),  # Invalid extension length
    ("12345-67890", "12345"),  # Extension too long
    ("12345--6789", "12345"),  # Double dash
    ("12345 6789", "12345"),  # Space instead of dash
    ("12345.6789", "12345"),  # Dot instead of dash
    ("12345_6789", ""),  # Underscore instead of dash (no word boundary)
    ("abcde", ""),  # All letters
    ("ABCDE", ""),  # All uppercase letters
    ("12a45", ""),  # Letters mixed in
    ("123-45", ""),  # Dash in wrong position
    ("", ""),  # Empty string
    (None, ""),  # Null value
    ("     ", ""),  # Only spaces
    ("\t\n", ""),  # Only whitespace
]

# ZIP codes embedded in text
ZIP_CODES_IN_TEXT = [
    # Simple addresses
    ("123 Main St, New York, NY 10001", "10001"),
    ("456 Oak Ave, Los Angeles, CA 90001-1234", "90001-1234"),
    ("PO Box 123, Dallas, TX 75201", "75201"),
    # Multiple ZIP codes (should extract first)
    ("Ship from 10001 to 90210", "10001"),
    ("Between 12345 and 67890 areas", "12345"),
    # ZIP codes with various surrounding characters
    ("ZIP: 12345", "12345"),
    ("(12345)", "12345"),
    ("[12345]", "12345"),
    ("zip=12345", "12345"),
    ("12345.", "12345"),
    ("12345,", "12345"),
    ("12345;", "12345"),
    ("12345!", "12345"),
    ("12345?", "12345"),
    ('"12345"', "12345"),
    ("'12345'", "12345"),
    # ZIP codes at different positions
    ("12345 at the beginning", "12345"),
    ("At the end 12345", "12345"),
    ("In the 12345 middle", "12345"),
    # No word boundaries (should not match)
    ("abc12345def", ""),
    ("12345678", ""),
    ("x12345", ""),
    ("12345y", ""),
    # Complex real-world addresses
    ("789 Elm Street Suite 200, Chicago, IL 60601 USA", "60601"),
    ("1234 Park Blvd Unit A, Seattle, WA 98101-0001", "98101-0001"),
    ("Send to: John Doe, 555 Tech Way, San Jose CA 95110", "95110"),
    # International formats mixed with US ZIP
    ("10 Downing St, London SW1A 2AA, forward to 12345 USA", "12345"),
    (
        "Paris 75008 France, originally from 90210 CA",
        "75008",
    ),  # French matches US format
    # Messy formatting
    ("  12345  ", "12345"),
    ("\n\n12345\n\n", "12345"),
    ("   Multiple   spaces   12345   here   ", "12345"),
    ("12345\r\n", "12345"),
    # Edge cases
    ("The year 12345 BC", "12345"),  # Number that looks like ZIP
    ("Product code 12345-6789", "12345-6789"),  # Looks like ZIP+4
    ("Phone: 12345", "12345"),
    ("Order #12345", "12345"),
    # No ZIP codes
    ("No postal code here", ""),
    ("Just text without numbers", ""),
    ("Numbers like 123 but not a ZIP", ""),
]

# Special ZIP code cases
SPECIAL_CASES = [
    # Military ZIP codes (valid)
    ("09001", "09001"),  # APO Europe
    ("34001", "34001"),  # APO Americas
    ("96201", "96201"),  # APO Pacific
    # US Territories
    ("00501", "00501"),  # Holtsville, NY (IRS)
    ("00601", "00601"),  # Puerto Rico
    ("00801", "00801"),  # US Virgin Islands
    ("96799", "96799"),  # American Samoa
    ("96910", "96910"),  # Guam
    ("96950", "96950"),  # Northern Mariana Islands
    # Special format variations
    ("ZIP 12345", "12345"),
    ("ZIP: 12345", "12345"),
    ("Zip Code 12345", "12345"),
    ("ZIPCODE 12345", "12345"),
    ("Postal Code: 12345", "12345"),
    ("PC 12345", "12345"),
    # With country codes
    ("12345 USA", "12345"),
    ("12345 US", "12345"),
    ("USA 12345", "12345"),
    ("United States 12345", "12345"),
    # Database/CSV formats
    ("'12345'", "12345"),
    ('"12345"', "12345"),
    ("`12345`", "12345"),
    # URL encoded
    ("zip%3D12345", ""),  # No word boundary around the number
    ("zip%20 12345", "12345"),  # Space creates word boundary
    # HTML/XML
    ("<zip>12345</zip>", "12345"),
    ("&lt;12345&gt;", "12345"),
    # JSON format
    ('{"zip":"12345"}', "12345"),
    ('{"postal_code":"12345-6789"}', "12345-6789"),
]

# International postal codes
INTERNATIONAL_POSTAL_CODES = [
    # UK - should NOT match
    ("SW1A 2AA", ""),
    ("EC1A 1BB", ""),
    ("W1A 0AX", ""),
    # Canada - should NOT match
    ("M5V 3A8", ""),
    ("K1A 0B1", ""),
    ("V6B 4Y8", ""),
    # Some international codes that match US format
    ("75008", "75008"),  # Paris, France (matches US format)
    ("28001", "28001"),  # Madrid, Spain (matches US format)
    ("10115", "10115"),  # Berlin, Germany (matches US format)
    # Japanese - should NOT match (wrong dash position)
    ("100-0001", ""),
    ("123-4567", ""),
    # Mixed formats
    ("12345 UK", "12345"),  # US ZIP with UK label
    ("US 12345 forwarding", "12345"),
]

# ZIP codes at boundaries
BOUNDARY_ZIP_CODES = [
    # Boundary values
    ("00000", "00000"),  # Minimum possible
    ("99999", "99999"),  # Maximum possible
    ("00000-0000", "00000-0000"),  # Minimum ZIP+4
    ("99999-9999", "99999-9999"),  # Maximum ZIP+4
    # Just outside boundaries (still valid as 5-digit numbers)
    ("00001", "00001"),
    ("99998", "99998"),
    # Real boundary ZIP codes
    ("00501", "00501"),  # Lowest assigned (Holtsville, NY)
    ("99950", "99950"),  # Highest assigned (Ketchikan, AK)
]

# Unicode and special characters
UNICODE_SPECIAL_CHARS = [
    # Unicode characters
    ("📍 12345", "12345"),
    ("ZIP→12345", "12345"),
    ("「12345」", "12345"),  # Japanese brackets
    ("【12345】", "12345"),  # Chinese brackets
    ("«12345»", "12345"),  # French quotes
    ('„12345"', "12345"),  # German quotes - left quote different from right
    # Special characters
    ("ZIP•12345", "12345"),
    ("12345†", "12345"),
    ("★12345★", "12345"),
    ("◆12345◆", "12345"),
    # Emoji mixed
    ("🏠 Address: 12345 🇺🇸", "12345"),
    ("📮 90210-1234 ✉️", "90210-1234"),
    # Zero-width characters
    ("12​345", ""),  # Zero-width space in middle (breaks the number)
    ("‌12345‌", "12345"),  # Zero-width non-joiner
    # Right-to-left text
    ("מיקוד 12345", "12345"),  # Hebrew
    ("الرمز البريدي 12345", "12345"),  # Arabic
]

# Test data for null handling
NULL_HANDLING = [
    (None, ""),
    ("", ""),
    ("12345", "12345"),
    (None, ""),
    ("90210", "90210"),
]

# Test data for city and state extraction
CITY_STATE_TEST_DATA = [
    # Standard formats with comma
    ("New York, NY 10001", "New York", "NY"),
    ("Los Angeles, CA 90001", "Los Angeles", "CA"),
    ("Chicago, IL 60601", "Chicago", "IL"),
    ("Houston, TX 77001", "Houston", "TX"),
    ("Phoenix, AZ 85001", "Phoenix", "AZ"),
    # Multi-word cities
    ("San Francisco, CA 94102", "San Francisco", "CA"),
    ("Salt Lake City, UT 84101", "Salt Lake City", "UT"),
    ("New York City, NY 10001", "New York City", "NY"),
    ("Las Vegas, NV 89101", "Las Vegas", "NV"),
    ("San Diego, CA 92101", "San Diego", "CA"),
    # Without comma
    ("Boston MA 02134", "Boston", "MA"),
    ("Seattle WA 98101", "Seattle", "WA"),
    ("Denver CO 80202", "Denver", "CO"),
    ("Miami FL 33101", "Miami", "FL"),
    # Full state names
    ("Philadelphia, Pennsylvania 19103", "Philadelphia", "PA"),
    ("Dallas, Texas 75201", "Dallas", "TX"),
    ("Atlanta, Georgia 30303", "Atlanta", "GA"),
    ("Portland, Oregon 97201", "Portland", "OR"),
    # With full address
    ("123 Main St, New York, NY 10001", "New York", "NY"),
    ("456 Oak Ave, Los Angeles, CA 90001", "Los Angeles", "CA"),
    ("789 Elm St Suite 200, Chicago, IL 60601", "Chicago", "IL"),
    # Case variations
    ("new york, ny 10001", "new york", "NY"),
    ("LOS ANGELES, CALIFORNIA 90001", "LOS ANGELES", "CA"),
    ("ChIcAgO, iL 60601", "ChIcAgO", "IL"),
    # Edge cases
    ("Washington, DC 20001", "Washington", "DC"),
    ("Washington DC 20001", "Washington", "DC"),
    ("Puerto Rico, PR 00901", "Puerto Rico", "PR"),
    # Just city and state (no ZIP)
    ("Boston, MA", "Boston", "MA"),
    ("Seattle, Washington", "Seattle", "WA"),
    ("Miami FL", "Miami", "FL"),
    # Territories
    ("San Juan, PR 00901", "San Juan", "PR"),
    ("Charlotte Amalie, VI 00801", "Charlotte Amalie", "VI"),
    ("Tamuning, GU 96913", "Tamuning", "GU"),
    # Invalid/missing
    ("12345", "", ""),
    ("No State Here", "", ""),
    ("", "", ""),
    (None, "", ""),
]

# State validation test data
STATE_VALIDATION_DATA = [
    # Valid abbreviations
    ("NY", True, "NY"),
    ("CA", True, "CA"),
    ("TX", True, "TX"),
    ("FL", True, "FL"),
    # Valid full names
    ("New York", True, "NY"),
    ("California", True, "CA"),
    ("Texas", True, "TX"),
    ("Florida", True, "FL"),
    # Case variations
    ("ny", True, "NY"),
    ("california", True, "CA"),
    ("TEXAS", True, "TX"),
    ("fLoRiDa", True, "FL"),
    # Territories
    ("DC", True, "DC"),
    ("PR", True, "PR"),
    ("VI", True, "VI"),
    ("District of Columbia", True, "DC"),
    ("Puerto Rico", True, "PR"),
    # Invalid
    ("XX", False, ""),
    ("ABC", False, ""),
    ("United States", False, ""),
    ("England", False, ""),
    ("12", False, ""),
    ("", False, ""),
    (None, False, ""),
]


def generate_performance_test_data(n=10000):
    """Generate large dataset for performance testing."""
    import random

    patterns = [
        lambda: f"{random.randint(10000, 99999)}",  # Valid 5-digit
        lambda: f"{random.randint(10000, 99999)}-{random.randint(1000, 9999)}",  # Valid ZIP+4
        lambda: f"Address {random.randint(10000, 99999)} Street",  # ZIP in text
        lambda: f"{random.randint(100, 9999)}",  # Invalid (too short)
        lambda: "No ZIP here",  # No ZIP
        lambda: None,  # Null
    ]

    data = []
    for i in range(n):
        pattern = random.choice(patterns)
        value = pattern()
        data.append((str(i), value))

    return data


# ========================================================================
# ADDRESS TEST DATA FROM test_clean_addresses.py
# ========================================================================

# Full address test data with components
ADDRESS_TEST_DATA = [
    # (street_number, street_name, unit, city, state, zip, country, full_address)
    (
        "123",
        "Main St",
        "Apt 4B",
        "New York",
        "NY",
        "10001",
        "USA",
        "123 Main St Apt 4B, New York, NY 10001",
    ),
    (
        "456",
        "Oak Avenue",
        None,
        "Los Angeles",
        "CA",
        "90001",
        "USA",
        "456 Oak Avenue, Los Angeles, CA 90001",
    ),
    (
        "789",
        "Elm Street",
        "Suite 200",
        "Chicago",
        "IL",
        "60601",
        "USA",
        "789 Elm Street Suite 200, Chicago, IL 60601",
    ),
    (
        "",
        "Broadway",
        None,
        "San Francisco",
        "CA",
        "94102",
        "USA",
        "Broadway, San Francisco, CA 94102",
    ),
    (
        "1234",
        "Park Blvd",
        "Unit A",
        "Seattle",
        "WA",
        "98101",
        "USA",
        "1234 Park Blvd Unit A, Seattle, WA 98101",
    ),
    (None, None, None, None, None, None, None, None),  # All nulls
    (
        "100",
        "First Ave",
        "",
        "Boston",
        "MA",
        "02134",
        "USA",
        "100 First Ave, Boston, MA 02134",
    ),
    (
        "42",
        "MLK Jr Blvd",
        "Apt 1",
        "Atlanta",
        "GA",
        "30303",
        "USA",
        "42 MLK Jr Blvd Apt 1, Atlanta, GA 30303",
    ),
    (
        "999",
        "Market St.",
        "#5",
        "Philadelphia",
        "PA",
        "19103",
        "USA",
        "999 Market St. #5, Philadelphia, PA 19103",
    ),
    (
        "777",
        "5th Avenue",
        "Floor 10",
        "New York",
        "NY",
        "10010",
        "USA",
        "777 5th Avenue Floor 10, New York, NY 10010",
    ),
    # International addresses
    (
        "10",
        "Downing Street",
        None,
        "London",
        None,
        "SW1A 2AA",
        "UK",
        "10 Downing Street, London SW1A 2AA, UK",
    ),
    (
        "1",
        "Champs-Élysées",
        None,
        "Paris",
        None,
        "75008",
        "France",
        "1 Champs-Élysées, Paris 75008, France",
    ),
    # Messy data
    (
        "  123  ",
        "  MAIN STREET  ",
        "apt 4b",
        "new york",
        "ny",
        "10001",
        "usa",
        "  123   MAIN STREET   apt 4b, new york, ny 10001  ",
    ),
    (
        "456",
        "oak ave.",
        "STE 100",
        "L.A.",
        "calif",
        "90001-1234",
        "United States",
        "456 oak ave. STE 100, L.A., calif 90001-1234",
    ),
    # PO Boxes
    (
        None,
        "PO Box 123",
        None,
        "Dallas",
        "TX",
        "75201",
        "USA",
        "PO Box 123, Dallas, TX 75201",
    ),
    (
        None,
        "P.O. BOX 456",
        None,
        "Houston",
        "TX",
        "77001",
        "USA",
        "P.O. BOX 456, Houston, TX 77001",
    ),
    # Abbreviations to expand
    (
        "123",
        "N Main St",
        None,
        "Phoenix",
        "AZ",
        "85001",
        "USA",
        "123 N Main St, Phoenix, AZ 85001",
    ),
    (
        "456",
        "S Broadway Ave",
        "Ste 5",
        "Denver",
        "CO",
        "80202",
        "USA",
        "456 S Broadway Ave Ste 5, Denver, CO 80202",
    ),
    (
        "789",
        "E 42nd St",
        None,
        "New York",
        "NY",
        "10017",
        "USA",
        "789 E 42nd St, New York, NY 10017",
    ),
    (
        "321",
        "W Sunset Blvd",
        None,
        "Los Angeles",
        "CA",
        "90028",
        "USA",
        "321 W Sunset Blvd, Los Angeles, CA 90028",
    ),
]

ADDRESS_TEST_DATA_COLUMNS = [
    "street_number",
    "street_name",
    "unit",
    "city",
    "state",
    "zip_code",
    "country",
    "full_address",
]

# Messy address data for edge case testing
MESSY_ADDRESS_DATA = [
    # Various formatting issues
    ("123 Main St, Apt 4B New York NY 10001",),  # Missing commas
    ("456 oak avenue los angeles california 90001",),  # All lowercase
    ("789 ELM STREET SUITE 200 CHICAGO IL 60601",),  # All uppercase
    ("   1234    Park     Blvd    Unit   A   Seattle   WA   98101   ",),  # Extra spaces
    ("100 First Ave.,Boston,MA,02134",),  # Inconsistent comma usage
    ("42 MLK Jr. Boulevard, Apartment #1, Atlanta Georgia 30303",),  # Mixed formats
    ("999 Market Street #5 Philadelphia Pennsylvania 19103-1234",),  # Extended zip
    ("777 Fifth Ave Floor 10 NYC NY 10010",),  # City abbreviation
    ("10 Main St\nApt 4B\nNew York, NY 10001",),  # Newlines
    ("123 Main St\tApt 4B\tNew York\tNY\t10001",),  # Tabs
    # Missing components
    ("Main Street, New York, NY",),  # No number or zip
    ("123, NY 10001",),  # Missing street and city
    ("Apt 4B, 10001",),  # Only unit and zip
    # International formats
    ("10 Downing Street London SW1A 2AA United Kingdom",),
    ("1 Rue de la Paix 75002 Paris France",),
    # Special cases
    ("PO Box 123 Dallas TX 75201",),
    ("P.O.Box 456, Houston, Texas, 77001",),
    ("Rural Route 1 Box 123 Somewhere KS 67890",),
    # Nulls and empty
    (None,),
    ("",),
    ("   ",),
]

# Common address abbreviations to expand
ADDRESS_ABBREVIATIONS = {
    # Directional
    "N": "North",
    "S": "South",
    "E": "East",
    "W": "West",
    "NE": "Northeast",
    "NW": "Northwest",
    "SE": "Southeast",
    "SW": "Southwest",
    # Street types
    "St": "Street",
    "St.": "Street",
    "Ave": "Avenue",
    "Ave.": "Avenue",
    "Blvd": "Boulevard",
    "Blvd.": "Boulevard",
    "Rd": "Road",
    "Rd.": "Road",
    "Dr": "Drive",
    "Dr.": "Drive",
    "Ct": "Court",
    "Ct.": "Court",
    "Pl": "Place",
    "Pl.": "Place",
    "Ln": "Lane",
    "Ln.": "Lane",
    "Pkwy": "Parkway",
    "Hwy": "Highway",
    # Unit types
    "Apt": "Apartment",
    "Apt.": "Apartment",
    "Ste": "Suite",
    "Ste.": "Suite",
    "Fl": "Floor",
    "Fl.": "Floor",
    "Rm": "Room",
    "Rm.": "Room",
    # State abbreviations (partial list)
    "CA": "California",
    "NY": "New York",
    "TX": "Texas",
    "FL": "Florida",
    "IL": "Illinois",
    "PA": "Pennsylvania",
    "OH": "Ohio",
    "GA": "Georgia",
    "NC": "North Carolina",
    "MI": "Michigan",
}

# Regex patterns for parsing addresses
ADDRESS_PATTERNS = {
    "street_number": r"^\d+",
    "zip_code": r"\b\d{5}(?:-\d{4})?\b",
    "state": r"\b[A-Z]{2}\b",
    "po_box": r"(?:P\.?O\.?\s*Box|Post\s*Office\s*Box)\s*\d+",
    "unit": r"(?:Apt|Apartment|Suite|Ste|Unit|#)\s*[\w\d]+",
    "direction": r"\b(?:N|S|E|W|NE|NW|SE|SW|North|South|East|West)\b",
}
