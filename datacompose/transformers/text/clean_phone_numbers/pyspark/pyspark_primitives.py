import re
from typing import TYPE_CHECKING, Dict, Optional

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

phones = PrimitiveRegistry("phones")

# Phone keypad mapping for letter to number conversion
PHONE_KEYPAD_MAPPING = {
    "A": "2", "B": "2", "C": "2",
    "D": "3", "E": "3", "F": "3",
    "G": "4", "H": "4", "I": "4",
    "J": "5", "K": "5", "L": "5",
    "M": "6", "N": "6", "O": "6",
    "P": "7", "Q": "7", "R": "7", "S": "7",
    "T": "8", "U": "8", "V": "8",
    "W": "9", "X": "9", "Y": "9", "Z": "9",
}


# ============================================================================
# Core Phone Number Extraction Functions
# ============================================================================


@phones.register()
def extract_phone_from_text(col: Column) -> Column:
    """
    Extract first phone number from text using regex patterns.
    
    Args:
        col: Column containing text with potential phone numbers
        
    Returns:
        Column with extracted phone number or empty string
    """
    # Comprehensive phone pattern that matches various formats
    # Handles: +1-555-123-4567, (555) 123-4567, 555.123.4567, 555-123-4567, etc.
    phone_pattern = r"(\+?\d{1,3}[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}(\s*(ext|x)\.?\s*\d+)?"
    
    return F.when(col.isNull(), F.lit("")).otherwise(
        F.regexp_extract(col, phone_pattern, 0)
    )


@phones.register()
def extract_all_phones_from_text(col: Column) -> Column:
    """
    Extract all phone numbers from text as an array.
    
    Args:
        col: Column containing text with potential phone numbers
        
    Returns:
        Column with array of phone numbers
    """
    # For simplicity, we'll return an array with just the first phone found
    # A proper implementation would require more complex regex or UDF
    # This is a limitation of Spark SQL's regex capabilities
    first_phone = extract_phone_from_text(col)
    
    # Return array with single element or empty array
    return F.when(
        first_phone != "",
        F.array(first_phone)
    ).otherwise(F.array())


@phones.register()
def extract_digits(col: Column) -> Column:
    """
    Extract only digits from phone number string.
    
    Args:
        col: Column containing phone number
        
    Returns:
        Column with only digits
    """
    return F.when(col.isNull(), F.lit("")).otherwise(
        F.regexp_replace(col, r"[^\d]", "")
    )


@phones.register()
def extract_extension(col: Column) -> Column:
    """
    Extract extension from phone number if present.
    
    Args:
        col: Column containing phone number
        
    Returns:
        Column with extension or empty string
    """
    return F.when(col.isNull(), F.lit("")).otherwise(
        F.when(
            col.rlike(r"ext\.?\s*(\d+)"),
            F.regexp_extract(col, r"ext\.?\s*(\d+)", 1)
        ).otherwise("")
    )


@phones.register()
def extract_country_code(col: Column) -> Column:
    """
    Extract country code from phone number.
    
    Args:
        col: Column containing phone number
        
    Returns:
        Column with country code or empty string
    """
    digits = extract_digits(col)
    
    # Check for explicit country code with + prefix
    has_plus = col.contains("+")
    
    return F.when(col.isNull(), F.lit("")).otherwise(
        F.when(
            # Explicit country code with +
            has_plus & col.rlike(r"^\+(\d{1,3})"),
            F.regexp_extract(col, r"^\+(\d{1,3})", 1)
        ).when(
            # NANP with leading 1 (11 digits total)
            (F.length(digits) == 11) & digits.startswith("1"),
            F.lit("1")
        ).otherwise("")
    )


@phones.register()
def extract_area_code(col: Column) -> Column:
    """
    Extract area code from NANP phone number.
    
    Args:
        col: Column containing phone number
        
    Returns:
        Column with area code or empty string
    """
    digits = extract_digits(col)
    
    return F.when(col.isNull(), F.lit("")).otherwise(
        F.when(
            F.length(digits) == 11,
            F.substring(digits, 2, 3)  # Skip country code
        ).when(
            F.length(digits) == 10,
            F.substring(digits, 1, 3)
        ).otherwise("")
    )


@phones.register()
def extract_exchange(col: Column) -> Column:
    """
    Extract exchange (first 3 digits of local number) from NANP phone number.
    
    Args:
        col: Column containing phone number
        
    Returns:
        Column with exchange or empty string
    """
    digits = extract_digits(col)
    
    return F.when(col.isNull(), F.lit("")).otherwise(
        F.when(
            F.length(digits) == 11,
            F.substring(digits, 5, 3)
        ).when(
            F.length(digits) == 10,
            F.substring(digits, 4, 3)
        ).otherwise("")
    )


@phones.register()
def extract_subscriber(col: Column) -> Column:
    """
    Extract subscriber number (last 4 digits) from NANP phone number.
    
    Args:
        col: Column containing phone number
        
    Returns:
        Column with subscriber number or empty string
    """
    digits = extract_digits(col)
    
    return F.when(col.isNull(), F.lit("")).otherwise(
        F.when(
            F.length(digits) == 11,
            F.substring(digits, 8, 4)
        ).when(
            F.length(digits) == 10,
            F.substring(digits, 7, 4)
        ).otherwise("")
    )


@phones.register()
def extract_local_number(col: Column) -> Column:
    """
    Extract local number (exchange + subscriber) from NANP phone number.
    
    Args:
        col: Column containing phone number
        
    Returns:
        Column with 7-digit local number or empty string
    """
    exchange = extract_exchange(col)
    subscriber = extract_subscriber(col)
    
    return F.when(
        (exchange != "") & (subscriber != ""),
        F.concat(exchange, subscriber)
    ).otherwise("")


# ============================================================================
# Phone Number Validation Functions
# ============================================================================


@phones.register()
def is_valid_nanp(col: Column) -> Column:
    """
    Check if phone number is valid NANP format (North American Numbering Plan).
    
    Args:
        col: Column containing phone number
        
    Returns:
        Column with boolean indicating NANP validity
    """
    digits = extract_digits(col)
    area_code = extract_area_code(col)
    exchange = extract_exchange(col)
    subscriber = extract_subscriber(col)
    
    return F.when(col.isNull(), F.lit(False)).otherwise(
        (F.length(digits).isin([10, 11])) &
        # Area code: 2-9 for first digit, 0-9 for second, 0-9 for third
        (area_code.rlike(r"^[2-9]\d{2}$")) &
        # Exchange: 2-9 for first digit (historically, now 1-9 is valid)
        (exchange.rlike(r"^[1-9]\d{2}$")) &
        # Subscriber: any 4 digits
        (subscriber.rlike(r"^\d{4}$")) &
        # If 11 digits, must start with 1
        ((F.length(digits) == 10) | (digits.startswith("1")))
    )


@phones.register()
def is_valid_international(col: Column, min_length: int = 7, max_length: int = 15) -> Column:
    """
    Check if phone number could be valid international format.
    
    Args:
        col: Column containing phone number
        min_length: Minimum digits for international number
        max_length: Maximum digits for international number
        
    Returns:
        Column with boolean indicating potential international validity
    """
    digits = extract_digits(col)
    
    return F.when(col.isNull(), F.lit(False)).otherwise(
        (F.length(digits) >= min_length) & 
        (F.length(digits) <= max_length) &
        digits.rlike(r"^\d+$")
    )


@phones.register()
def is_valid_phone(col: Column) -> Column:
    """
    Check if phone number is valid (NANP or international).
    
    Args:
        col: Column containing phone number
        
    Returns:
        Column with boolean indicating validity
    """
    return is_valid_nanp(col) | is_valid_international(col)


@phones.register()
def is_toll_free(col: Column) -> Column:
    """
    Check if phone number is toll-free (800, 888, 877, 866, 855, 844, 833).
    
    Args:
        col: Column containing phone number
        
    Returns:
        Column with boolean indicating if toll-free
    """
    area_code = extract_area_code(col)
    
    toll_free_codes = ["800", "888", "877", "866", "855", "844", "833"]
    
    return F.when(col.isNull(), F.lit(False)).otherwise(
        area_code.isin(toll_free_codes)
    )


@phones.register()
def is_premium_rate(col: Column) -> Column:
    """
    Check if phone number is premium rate (900).
    
    Args:
        col: Column containing phone number
        
    Returns:
        Column with boolean indicating if premium rate
    """
    area_code = extract_area_code(col)
    
    return F.when(col.isNull(), F.lit(False)).otherwise(
        area_code == "900"
    )


@phones.register()
def has_extension(col: Column) -> Column:
    """
    Check if phone number has an extension.
    
    Args:
        col: Column containing phone number
        
    Returns:
        Column with boolean indicating presence of extension
    """
    return F.when(col.isNull(), F.lit(False)).otherwise(
        col.rlike(r"ext\.?\s*\d+")
    )


# ============================================================================
# Phone Number Cleaning Functions
# ============================================================================


@phones.register()
def remove_non_digits(col: Column) -> Column:
    """
    Remove all non-digit characters from phone number.
    
    Args:
        col: Column containing phone number
        
    Returns:
        Column with only digits
    """
    return extract_digits(col)


@phones.register()
def remove_extension(col: Column) -> Column:
    """
    Remove extension from phone number.
    
    Args:
        col: Column containing phone number
        
    Returns:
        Column with extension removed
    """
    return F.when(col.isNull(), F.lit("")).otherwise(
        F.regexp_replace(col, r"ext\.?\s*\d+", "")
    )


@phones.register()
def convert_letters_to_numbers(col: Column) -> Column:
    """
    Convert phone letters to numbers (e.g., 1-800-FLOWERS to 1-800-3569377).
    
    Args:
        col: Column containing phone number with letters
        
    Returns:
        Column with letters converted to numbers
    """
    result = col
    
    # Apply each letter-to-number mapping
    for letter, number in PHONE_KEYPAD_MAPPING.items():
        result = F.regexp_replace(result, letter, number)
        result = F.regexp_replace(result, letter.lower(), number)
    
    return F.when(col.isNull(), F.lit("")).otherwise(result)


@phones.register()
def normalize_separators(col: Column) -> Column:
    """
    Normalize various separator styles to hyphens.
    Removes parentheses and replaces dots, spaces with hyphens.
    
    Args:
        col: Column containing phone number
        
    Returns:
        Column with normalized separators
    """
    # First remove parentheses and replace with space to maintain separation
    result = F.regexp_replace(col, r"\(", "")
    result = F.regexp_replace(result, r"\)", " ")
    # Then replace any sequence of spaces or dots with hyphen
    result = F.regexp_replace(result, r"[\s\.]+", "-")
    # Collapse multiple hyphens into one
    result = F.regexp_replace(result, r"-+", "-")
    # Remove leading/trailing hyphens
    result = F.regexp_replace(result, r"^-+|-+$", "")
    
    return F.when(col.isNull(), F.lit("")).otherwise(result)


@phones.register()
def add_country_code(col: Column) -> Column:
    """
    Add country code "1" if not present (for NANP numbers).
    
    Args:
        col: Column containing phone number
        
    Returns:
        Column with country code added if needed
    """
    digits = extract_digits(col)
    
    return F.when(col.isNull(), col).otherwise(
        F.when(
            (F.length(digits) == 10) & is_valid_nanp(col),
            F.concat(F.lit("1"), digits)
        ).otherwise(digits)
    )


# ============================================================================
# Phone Number Formatting Functions
# ============================================================================


@phones.register()
def format_nanp(col: Column) -> Column:
    """
    Format NANP phone number in standard hyphen format (XXX-XXX-XXXX).
    
    Args:
        col: Column containing phone number
        
    Returns:
        Column with formatted phone number
    """
    # Remove extension for validation but preserve it
    extension = extract_extension(col)
    phone_no_ext = remove_extension(col)
    
    area_code = extract_area_code(phone_no_ext)
    exchange = extract_exchange(phone_no_ext)
    subscriber = extract_subscriber(phone_no_ext)
    
    base_format = F.concat(
        area_code, F.lit("-"),
        exchange, F.lit("-"),
        subscriber
    )
    
    # Add extension if present
    formatted = F.when(
        (extension != ""),
        F.concat(base_format, F.lit(" ext. "), extension)
    ).otherwise(base_format)
    
    return F.when(
        is_valid_nanp(phone_no_ext),
        formatted
    ).otherwise(F.lit(""))


@phones.register()
def format_nanp_paren(col: Column) -> Column:
    """
    Format NANP phone number with parentheses ((XXX) XXX-XXXX).
    
    Args:
        col: Column containing phone number
        
    Returns:
        Column with formatted phone number
    """
    # Remove extension for validation but preserve it
    extension = extract_extension(col)
    phone_no_ext = remove_extension(col)
    
    area_code = extract_area_code(phone_no_ext)
    exchange = extract_exchange(phone_no_ext)
    subscriber = extract_subscriber(phone_no_ext)
    
    base_format = F.concat(
        F.lit("("), area_code, F.lit(") "),
        exchange, F.lit("-"), subscriber
    )
    
    # Add extension if present
    formatted = F.when(
        (extension != ""),
        F.concat(base_format, F.lit(" ext. "), extension)
    ).otherwise(base_format)
    
    return F.when(
        is_valid_nanp(phone_no_ext),
        formatted
    ).otherwise(F.lit(""))


@phones.register()
def format_nanp_dot(col: Column) -> Column:
    """
    Format NANP phone number with dots (XXX.XXX.XXXX).
    
    Args:
        col: Column containing phone number
        
    Returns:
        Column with formatted phone number
    """
    # Remove extension for validation but preserve it
    extension = extract_extension(col)
    phone_no_ext = remove_extension(col)
    
    area_code = extract_area_code(phone_no_ext)
    exchange = extract_exchange(phone_no_ext)
    subscriber = extract_subscriber(phone_no_ext)
    
    base_format = F.concat(
        area_code, F.lit("."),
        exchange, F.lit("."),
        subscriber
    )
    
    # Add extension if present
    formatted = F.when(
        (extension != ""),
        F.concat(base_format, F.lit(" ext. "), extension)
    ).otherwise(base_format)
    
    return F.when(
        is_valid_nanp(phone_no_ext),
        formatted
    ).otherwise(F.lit(""))


@phones.register()
def format_nanp_space(col: Column) -> Column:
    """
    Format NANP phone number with spaces (XXX XXX XXXX).
    
    Args:
        col: Column containing phone number
        
    Returns:
        Column with formatted phone number
    """
    # Remove extension for validation but preserve it
    extension = extract_extension(col)
    phone_no_ext = remove_extension(col)
    
    area_code = extract_area_code(phone_no_ext)
    exchange = extract_exchange(phone_no_ext)
    subscriber = extract_subscriber(phone_no_ext)
    
    base_format = F.concat(
        area_code, F.lit(" "),
        exchange, F.lit(" "),
        subscriber
    )
    
    # Add extension if present
    formatted = F.when(
        (extension != ""),
        F.concat(base_format, F.lit(" ext. "), extension)
    ).otherwise(base_format)
    
    return F.when(
        is_valid_nanp(phone_no_ext),
        formatted
    ).otherwise(F.lit(""))


@phones.register()
def format_international(col: Column) -> Column:
    """
    Format international phone number with country code.
    
    Args:
        col: Column containing phone number
        
    Returns:
        Column with formatted international number
    """
    country_code = extract_country_code(col)
    digits = extract_digits(col)
    
    # For international numbers, if we have a country code, remove it from the beginning
    # Use F.substring with proper column references
    cc_length = F.length(country_code)
    remaining_digits = F.when(
        (country_code != "") & (cc_length > 0) & digits.startswith(country_code),
        F.substring(digits, cc_length + 1, 999)
    ).otherwise(digits)
    
    return F.when(
        is_valid_international(col) & (country_code != ""),
        F.concat(F.lit("+"), country_code, F.lit(" "), remaining_digits)
    ).when(
        is_valid_international(col),
        digits
    ).otherwise(F.lit(""))


@phones.register()
def format_e164(col: Column) -> Column:
    """
    Format phone number in E.164 format (+CCAAANNNNNNN) with default country code 1.
    
    Args:
        col: Column containing phone number
        
    Returns:
        Column with E.164 formatted number
    """
    digits = extract_digits(col)
    country_code = extract_country_code(col)
    
    # Check if it's a valid NANP number first
    is_nanp = is_valid_nanp(col)
    
    # Use default country code "1" if not present and number is 10 digits NANP
    final_country = F.when(
        (country_code == "") & (F.length(digits) == 10) & is_nanp,
        F.lit("1")
    ).otherwise(country_code)
    
    # Build E.164 format - only for valid phones
    return F.when(
        is_valid_phone(col),
        F.when(
            (F.length(digits) == 10) & is_nanp,
            F.concat(F.lit("+"), F.lit("1"), digits)
        ).when(
            (F.length(digits) == 11) & digits.startswith("1") & is_nanp,
            F.concat(F.lit("+"), digits)
        ).when(
            (country_code != "") & is_valid_international(col),
            F.concat(F.lit("+"), digits)  # digits already includes country code
        ).otherwise(F.lit(""))
    ).otherwise(F.lit(""))


# ============================================================================
# Phone Number Standardization Functions
# ============================================================================


@phones.register()
def standardize_phone(col: Column) -> Column:
    """
    Standardize phone number with cleaning and NANP formatting.
    
    Args:
        col: Column containing phone number
        
    Returns:
        Column with standardized phone number in NANP format
    """
    # Clean and convert letters in a simpler way
    cleaned = convert_letters_to_numbers(col)
    
    # Extract extension first
    extension = extract_extension(cleaned)
    phone_no_ext = remove_extension(cleaned) 
    
    # Get digits and check validity
    digits = extract_digits(phone_no_ext)
    
    # Simple NANP formatting for valid 10 or 11 digit numbers
    result = F.when(
        F.length(digits) == 10,
        F.concat(
            F.substring(digits, 1, 3), F.lit("-"),
            F.substring(digits, 4, 3), F.lit("-"),
            F.substring(digits, 7, 4)
        )
    ).when(
        F.length(digits) == 11,
        F.concat(
            F.substring(digits, 2, 3), F.lit("-"),
            F.substring(digits, 5, 3), F.lit("-"),
            F.substring(digits, 8, 4)
        )
    ).otherwise(F.lit(""))
    
    # Add extension back if present
    final_result = F.when(
        (extension != "") & (result != ""),
        F.concat(result, F.lit(" ext. "), extension)
    ).otherwise(result)
    
    return final_result


@phones.register()
def standardize_phone_e164(col: Column) -> Column:
    """
    Standardize phone number with cleaning and E.164 formatting.
    
    Args:
        col: Column containing phone number
        
    Returns:
        Column with standardized phone number in E.164 format
    """
    # Clean and convert letters
    cleaned = convert_letters_to_numbers(col)
    
    # Format as E.164
    result = format_e164(cleaned)
    
    # Only return valid phone numbers
    return F.when(is_valid_phone(cleaned), result).otherwise(F.lit(""))


@phones.register()
def standardize_phone_digits(col: Column) -> Column:
    """
    Standardize phone number and return digits only.
    
    Args:
        col: Column containing phone number
        
    Returns:
        Column with digits only
    """
    # Clean and convert letters
    cleaned = convert_letters_to_numbers(col)
    
    # Get digits only
    result = extract_digits(cleaned)
    
    # Only return valid phone numbers
    return F.when(is_valid_phone(cleaned), result).otherwise(F.lit(""))


@phones.register()
def clean_phone(col: Column) -> Column:
    """
    Clean and validate phone number, returning null for invalid numbers.
    
    Args:
        col: Column containing phone number
        
    Returns:
        Column with cleaned phone number or null
    """
    # Simple implementation to avoid deep nesting
    cleaned = convert_letters_to_numbers(col)
    digits = extract_digits(cleaned)
    
    # Simple validation and formatting
    result = F.when(
        F.length(digits) == 10,
        F.concat(
            F.substring(digits, 1, 3), F.lit("-"),
            F.substring(digits, 4, 3), F.lit("-"),
            F.substring(digits, 7, 4)
        )
    ).when(
        F.length(digits) == 11,
        F.concat(
            F.substring(digits, 2, 3), F.lit("-"),
            F.substring(digits, 5, 3), F.lit("-"),
            F.substring(digits, 8, 4)
        )
    ).otherwise(F.lit(None))
    
    return result


# ============================================================================
# Phone Number Information Functions
# ============================================================================


@phones.register()
def get_phone_type(col: Column) -> Column:
    """
    Get phone number type (toll-free, premium, standard, international).
    
    Args:
        col: Column containing phone number
        
    Returns:
        Column with phone type
    """
    return F.when(col.isNull() | (col == ""), F.lit("unknown")).otherwise(
        F.when(is_toll_free(col), F.lit("toll-free"))
        .when(is_premium_rate(col), F.lit("premium"))
        .when(is_valid_nanp(col), F.lit("standard"))
        .when(is_valid_international(col), F.lit("international"))
        .otherwise(F.lit("invalid"))
    )


@phones.register()
def get_region_from_area_code(col: Column) -> Column:
    """
    Get geographic region from area code (simplified - would need lookup table).
    
    Args:
        col: Column containing phone number
        
    Returns:
        Column with region or empty string
    """
    area_code = extract_area_code(col)
    
    # This is a simplified example - in practice you'd use a lookup table
    # Just showing structure for major area codes
    return F.when(area_code == "212", F.lit("New York, NY")).\
        when(area_code == "213", F.lit("Los Angeles, CA")).\
        when(area_code == "312", F.lit("Chicago, IL")).\
        when(area_code == "415", F.lit("San Francisco, CA")).\
        when(area_code == "202", F.lit("Washington, DC")).\
        when(area_code.isin(["800", "888", "877", "866", "855", "844", "833"]), 
             F.lit("Toll-Free")).\
        when(area_code == "900", F.lit("Premium")).\
        otherwise(F.lit(""))


@phones.register()
def mask_phone(col: Column) -> Column:
    """
    Mask phone number for privacy keeping last 4 digits (e.g., ***-***-1234).
    
    Args:
        col: Column containing phone number
        
    Returns:
        Column with masked phone number
    """
    subscriber = extract_subscriber(col)
    
    # Mask area code and exchange, keep last 4 digits
    masked = F.when(
        is_valid_nanp(col),
        F.concat(
            F.lit("***"), F.lit("-"),
            F.lit("***"), F.lit("-"),
            subscriber
        )
    ).otherwise(col)
    
    return F.when(col.isNull() | (col == ""), F.lit(None)).otherwise(masked)


# ============================================================================
# Phone Number Filtering Functions
# ============================================================================


@phones.register()
def filter_valid_phones(col: Column) -> Column:
    """
    Return phone number only if valid, otherwise return null.
    
    Args:
        col: Column containing phone number
        
    Returns:
        Column with valid phone or null
    """
    return F.when(is_valid_phone(col), col).otherwise(F.lit(None))


@phones.register()
def filter_nanp_phones(col: Column) -> Column:
    """
    Return phone number only if valid NANP, otherwise return null.
    
    Args:
        col: Column containing phone number
        
    Returns:
        Column with NANP phone or null
    """
    return F.when(is_valid_nanp(col), col).otherwise(F.lit(None))


@phones.register()
def filter_toll_free_phones(col: Column) -> Column:
    """
    Return phone number only if toll-free, otherwise return null.
    
    Args:
        col: Column containing phone number
        
    Returns:
        Column with toll-free phone or null
    """
    return F.when(is_toll_free(col), col).otherwise(F.lit(None))