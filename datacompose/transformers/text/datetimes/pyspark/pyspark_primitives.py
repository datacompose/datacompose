import re
from typing import TYPE_CHECKING, Dict, List, Optional

if TYPE_CHECKING:
    from pyspark.sql import Column
    from pyspark.sql import functions as F
else:
    try:
        from pyspark.sql import Column
        from pyspark.sql import functions as F
    except ImportError:
        pass

try:
    # Try local utils import first (for generated code)
    from utils.primitives import PrimitiveRegistry  # type: ignore
except ImportError:
    # Fall back to installed datacompose package
    from datacompose.operators.primitives import PrimitiveRegistry

datetimes = PrimitiveRegistry("datetimes")


# ============================================================================
# Core Standardization Functions
# ============================================================================


@datetimes.register()
def standardize_iso(col: Column) -> Column:
    """
    Convert datetimes strings to ISO 8601 format (YYYY-MM-DD HH:MM:SS).
    Attempts to parse common formats and standardize them.

    Examples:
        "01/15/2024" -> "2024-01-15 00:00:00"
        "2024-Jan-15" -> "2024-01-15 00:00:00"
        "15-01-2024 14:30" -> "2024-01-15 14:30:00"
    """
    # Try multiple formats in order using coalesce
    # try_to_timestamp returns null if format doesn't match or date is invalid
    # This handles ambiguous cases like "01/15/2024" vs "15/01/2024" because
    # invalid dates (like month 15) will return null

    parsed = F.coalesce(
        # ISO formats (highest priority - unambiguous)
        F.try_to_timestamp(col, F.lit("yyyy-MM-dd'T'HH:mm:ss")),
        F.try_to_timestamp(col, F.lit("yyyy-MM-dd HH:mm:ss")),
        F.try_to_timestamp(col, F.lit("yyyy-MM-dd'T'HH:mm")),
        F.try_to_timestamp(col, F.lit("yyyy-MM-dd HH:mm")),
        F.try_to_timestamp(col, F.lit("yyyy-MM-dd")),

        # US formats with time and AM/PM
        F.try_to_timestamp(col, F.lit("MM/dd/yyyy h:mm a")),
        F.try_to_timestamp(col, F.lit("M/d/yyyy h:mm a")),
        F.try_to_timestamp(col, F.lit("MM/dd/yyyy hh:mm a")),

        # US formats with 24-hour time
        F.try_to_timestamp(col, F.lit("MM/dd/yyyy HH:mm:ss")),
        F.try_to_timestamp(col, F.lit("MM/dd/yyyy HH:mm")),
        F.try_to_timestamp(col, F.lit("M/d/yyyy HH:mm")),

        # US formats without time
        F.try_to_timestamp(col, F.lit("MM/dd/yyyy")),
        F.try_to_timestamp(col, F.lit("M/d/yyyy")),

        # Named month formats with time and AM/PM
        F.try_to_timestamp(col, F.lit("MMMM d, yyyy h:mm a")),
        F.try_to_timestamp(col, F.lit("MMM d, yyyy h:mm a")),
        F.try_to_timestamp(col, F.lit("MMMM d, yyyy hh:mm a")),
        F.try_to_timestamp(col, F.lit("MMM d, yyyy hh:mm a")),

        # Named month formats without time
        F.try_to_timestamp(col, F.lit("MMMM d, yyyy")),
        F.try_to_timestamp(col, F.lit("MMM d, yyyy")),
        F.try_to_timestamp(col, F.lit("MMMM dd, yyyy")),
        F.try_to_timestamp(col, F.lit("MMM dd, yyyy")),
        F.try_to_timestamp(col, F.lit("d-MMM-yyyy")),
        F.try_to_timestamp(col, F.lit("dd-MMM-yyyy")),
        F.try_to_timestamp(col, F.lit("d MMMM yyyy")),
        F.try_to_timestamp(col, F.lit("dd MMMM yyyy")),

        # EU formats (after US formats due to ambiguity)
        F.try_to_timestamp(col, F.lit("dd/MM/yyyy")),
        F.try_to_timestamp(col, F.lit("d/M/yyyy")),
        F.try_to_timestamp(col, F.lit("dd.MM.yyyy")),
        F.try_to_timestamp(col, F.lit("d.M.yyyy")),
    )

    # Format as ISO string "yyyy-MM-dd HH:mm:ss"
    # Returns null if parsed is null
    return F.date_format(parsed, "yyyy-MM-dd HH:mm:ss")


@datetimes.register()
def standardize_date(col: Column) -> Column:
    """
    Extract and standardize just the date portion to YYYY-MM-DD format.

    Examples:
        "2024-01-15 14:30:00" -> "2024-01-15"
        "01/15/2024" -> "2024-01-15"
    """
    # First standardize to ISO format, then extract just the date part
    iso_datetime = standardize_iso(col)
    # Extract date portion (YYYY-MM-DD) from the datetime
    return F.when(
        iso_datetime.isNotNull(),
        F.substring(iso_datetime, 1, 10)
    ).otherwise(None)


@datetimes.register()
def standardize_time(col: Column) -> Column:
    """
    Extract and standardize just the time portion to HH:MM:SS format.

    Examples:
        "2024-01-15 14:30:00" -> "14:30:00"
        "2:30 PM" -> "14:30:00"
    """
    # First standardize to ISO format, then extract just the time part
    iso_datetime = standardize_iso(col)
    # Extract time portion (HH:MM:SS) from the datetime
    return F.when(
        iso_datetime.isNotNull(),
        F.substring(iso_datetime, 12, 8)
    ).otherwise(None)


# ============================================================================
# Format Detection and Parsing
# ============================================================================


@datetimes.register()
def detect_format(col: Column) -> Column:
    """
    Detect the datetimes format of a string.
    Returns format string that can be used with to_timestamp().

    Examples:
        "2024-01-15" -> "yyyy-MM-dd"
        "01/15/2024" -> "MM/dd/yyyy"
        "15-Jan-2024" -> "dd-MMM-yyyy"
    """
    # TODO: Implement format detection logic
    return F.lit("unknown")


@datetimes.register()
def parse_flexible(col: Column) -> Column:
    """
    Parse datetimes strings with multiple possible formats.
    Tries common formats in order of likelihood.

    Handles formats like:
        - ISO: "2024-01-15", "2024-01-15T14:30:00"
        - US: "01/15/2024", "1/15/24"
        - EU: "15/01/2024", "15.01.2024"
        - Named months: "Jan 15, 2024", "15-Jan-2024"
    """
    # TODO: Implement flexible parsing with multiple format attempts
    return col


@datetimes.register()
def parse_natural_language(
    col: Column, reference_date: Optional[Column] = None
) -> Column:
    """
    Parse natural language date expressions.

    Examples:
        "yesterday" -> (current_date - 1 day)
        "last Monday" -> (most recent Monday)
        "next month" -> (current_date + 1 month)
        "3 days ago" -> (current_date - 3 days)
        "end of year" -> "YYYY-12-31"
    """
    # TODO: Implement natural language parsing
    return col


# ============================================================================
# Validation Functions
# ============================================================================


@datetimes.register()
def is_valid_date(col: Column) -> Column:
    """
    Check if a string represents a valid date.
    Returns true if parseable as date, false otherwise.

    Validates:
        - Month is 1-12
        - Day is valid for the month (handles leap years)
        - Year is reasonable (e.g., 1900-2100)
    """
    # A date is valid if standardize_iso can parse it successfully
    standardized = standardize_iso(col)
    return F.when(
        col.isNull() | (F.trim(col) == ""),
        F.lit(False)
    ).otherwise(
        standardized.isNotNull()
    )


@datetimes.register()
def is_valid_datetimes(col: Column) -> Column:
    """
    Check if a string represents a valid datetimes.
    More comprehensive than is_valid_date, includes time validation.
    """
    # Use same logic as is_valid_date since standardize_iso handles both
    return is_valid_date(col)


@datetimes.register()
def is_business_day(col: Column) -> Column:
    """
    Check if a date falls on a business day (Monday-Friday).
    """
    # First standardize, then check if dayofweek is 2-6 (Mon-Fri in PySpark)
    # In PySpark: 1=Sunday, 2=Monday, ..., 7=Saturday
    standardized = standardize_iso(col)
    return F.when(
        standardized.isNotNull(),
        F.dayofweek(F.to_timestamp(standardized, "yyyy-MM-dd HH:mm:ss")).isin([2, 3, 4, 5, 6])
    ).otherwise(F.lit(False))


@datetimes.register()
def is_future_date(col: Column, reference_date: Optional[Column] = None) -> Column:
    """
    Check if a date is in the future.
    Uses current_date() if reference_date not provided.
    """
    standardized = standardize_iso(col)
    ref = F.current_date() if reference_date is None else reference_date

    return F.when(
        standardized.isNotNull(),
        F.to_date(F.to_timestamp(standardized, "yyyy-MM-dd HH:mm:ss")) > ref
    ).otherwise(F.lit(False))


@datetimes.register()
def is_past_date(col: Column, reference_date: Optional[Column] = None) -> Column:
    """
    Check if a date is in the past.
    Uses current_date() if reference_date not provided.
    """
    standardized = standardize_iso(col)
    ref = F.current_date() if reference_date is None else reference_date

    return F.when(
        standardized.isNotNull(),
        F.to_date(F.to_timestamp(standardized, "yyyy-MM-dd HH:mm:ss")) < ref
    ).otherwise(F.lit(False))


# ============================================================================
# Component Extraction Functions
# ============================================================================


@datetimes.register()
def extract_year(col: Column) -> Column:
    """
    Extract year from datetimes string.

    Examples:
        "2024-01-15" -> 2024
        "01/15/2024" -> 2024
    """
    # First standardize, then convert to timestamp and extract year
    standardized = standardize_iso(col)
    return F.when(
        standardized.isNotNull(),
        F.year(F.to_timestamp(standardized, "yyyy-MM-dd HH:mm:ss"))
    ).otherwise(None)


@datetimes.register()
def extract_month(col: Column) -> Column:
    """
    Extract month from datetimes string (1-12).

    Examples:
        "2024-01-15" -> 1
        "Jan 15, 2024" -> 1
    """
    # First standardize, then convert to timestamp and extract month
    standardized = standardize_iso(col)
    return F.when(
        standardized.isNotNull(),
        F.month(F.to_timestamp(standardized, "yyyy-MM-dd HH:mm:ss"))
    ).otherwise(None)


@datetimes.register()
def extract_day(col: Column) -> Column:
    """
    Extract day from datetimes string (1-31).

    Examples:
        "2024-01-15" -> 15
        "15/01/2024" -> 15
    """
    # First standardize, then convert to timestamp and extract day
    standardized = standardize_iso(col)
    return F.when(
        standardized.isNotNull(),
        F.dayofmonth(F.to_timestamp(standardized, "yyyy-MM-dd HH:mm:ss"))
    ).otherwise(None)


@datetimes.register()
def extract_quarter(col: Column) -> Column:
    """
    Extract quarter from datetimes string (1-4).

    Examples:
        "2024-01-15" -> 1
        "2024-07-01" -> 3
    """
    # First standardize, then convert to timestamp and extract quarter
    standardized = standardize_iso(col)
    return F.when(
        standardized.isNotNull(),
        F.quarter(F.to_timestamp(standardized, "yyyy-MM-dd HH:mm:ss"))
    ).otherwise(None)


@datetimes.register()
def extract_week_of_year(col: Column) -> Column:
    """
    Extract week number from datetimes string (1-53).
    """
    # First standardize, then convert to timestamp and extract week
    standardized = standardize_iso(col)
    return F.when(
        standardized.isNotNull(),
        F.weekofyear(F.to_timestamp(standardized, "yyyy-MM-dd HH:mm:ss"))
    ).otherwise(None)


@datetimes.register()
def extract_day_of_week(col: Column) -> Column:
    """
    Extract day of week from datetimes string.
    Returns string name (Monday, Tuesday, etc).
    """
    # First standardize, then convert to timestamp and extract day name
    standardized = standardize_iso(col)
    return F.when(
        standardized.isNotNull(),
        F.date_format(F.to_timestamp(standardized, "yyyy-MM-dd HH:mm:ss"), "EEEE")
    ).otherwise(None)


# ============================================================================
# Timezone Functions
# ============================================================================


@datetimes.register()
def normalize_timezone(col: Column, target_tz: str = "UTC") -> Column:
    """
    Convert datetimes to specified timezone.
    Default converts to UTC.

    Examples:
        "2024-01-15 14:30:00 EST" -> "2024-01-15 19:30:00 UTC"
    """
    # TODO: Implement timezone normalization
    return col


@datetimes.register()
def add_timezone(col: Column, timezone: str) -> Column:
    """
    Add timezone information to naive datetimes.
    """
    # TODO: Implement timezone addition
    return col


@datetimes.register()
def remove_timezone(col: Column) -> Column:
    """
    Remove timezone information, keeping local time.
    """
    # TODO: Implement timezone removal
    return col


# ============================================================================
# Date Math Functions
# ============================================================================


@datetimes.register()
def add_days(col: Column, days: int) -> Column:
    """
    Add or subtract days from a date.

    Examples:
        ("2024-01-15", 5) -> "2024-01-20"
        ("2024-01-15", -5) -> "2024-01-10"
    """
    # First standardize to get consistent format
    standardized = standardize_iso(col)

    # Convert to date, add days, then format back to string
    return F.when(
        standardized.isNotNull(),
        F.date_format(
            F.date_add(F.to_date(F.to_timestamp(standardized, "yyyy-MM-dd HH:mm:ss")), days),
            "yyyy-MM-dd HH:mm:ss"
        )
    ).otherwise(None)


@datetimes.register()
def add_months(col: Column, months: int) -> Column:
    """
    Add or subtract months from a date.
    Handles month-end edge cases properly.
    """
    # First standardize to get consistent format
    standardized = standardize_iso(col)

    # Convert to date, add months, then format back to string
    return F.when(
        standardized.isNotNull(),
        F.date_format(
            F.add_months(F.to_date(F.to_timestamp(standardized, "yyyy-MM-dd HH:mm:ss")), months),
            "yyyy-MM-dd HH:mm:ss"
        )
    ).otherwise(None)


@datetimes.register()
def date_diff(col1: Column, col2: Column, unit: str = "days") -> Column:
    """
    Calculate difference between two dates.
    Units: days, months, years, hours, minutes, seconds
    """
    # Standardize both dates
    standardized1 = standardize_iso(col1)
    standardized2 = standardize_iso(col2)

    # Convert to timestamps
    ts1 = F.to_timestamp(standardized1, "yyyy-MM-dd HH:mm:ss")
    ts2 = F.to_timestamp(standardized2, "yyyy-MM-dd HH:mm:ss")

    # Calculate difference based on unit
    # For now, implement days - can extend to other units later
    return F.when(
        standardized1.isNotNull() & standardized2.isNotNull(),
        F.datediff(ts1, ts2)
    ).otherwise(None)


@datetimes.register()
def business_days_between(col1: Column, col2: Column) -> Column:
    """
    Calculate number of business days between two dates.
    Excludes weekends (and optionally holidays).
    """
    # TODO: Implement business days calculation
    return F.lit(0)


# ============================================================================
# Formatting Functions
# ============================================================================


@datetimes.register()
def format_date(col: Column, format_string: str) -> Column:
    """
    Format date according to specified pattern.

    Format patterns:
        - "MM/dd/yyyy" -> "01/15/2024"
        - "dd-MMM-yyyy" -> "15-Jan-2024"
        - "EEEE, MMMM d, yyyy" -> "Monday, January 15, 2024"
    """
    # TODO: Implement custom formatting
    return col


@datetimes.register()
def to_unix_timestamp(col: Column) -> Column:
    """
    Convert datetimes to Unix timestamp (seconds since epoch).
    """
    # TODO: Implement Unix timestamp conversion
    return F.lit(None)


@datetimes.register()
def from_unix_timestamp(col: Column) -> Column:
    """
    Convert Unix timestamp to datetimes string.
    """
    # TODO: Implement Unix timestamp parsing
    return col


# ============================================================================
# Special Date Functions
# ============================================================================


@datetimes.register()
def start_of_month(col: Column) -> Column:
    """
    Get the first day of the month for a given date.

    Examples:
        "2024-01-15" -> "2024-01-01"
    """
    # Standardize, then use trunc to get first day of month
    standardized = standardize_iso(col)

    return F.when(
        standardized.isNotNull(),
        F.date_format(
            F.trunc(F.to_timestamp(standardized, "yyyy-MM-dd HH:mm:ss"), "month"),
            "yyyy-MM-dd HH:mm:ss"
        )
    ).otherwise(None)


@datetimes.register()
def end_of_month(col: Column) -> Column:
    """
    Get the last day of the month for a given date.

    Examples:
        "2024-01-15" -> "2024-01-31"
        "2024-02-10" -> "2024-02-29" (leap year)
    """
    # Standardize, then use last_day to get last day of month
    standardized = standardize_iso(col)

    return F.when(
        standardized.isNotNull(),
        F.date_format(
            F.last_day(F.to_timestamp(standardized, "yyyy-MM-dd HH:mm:ss")),
            "yyyy-MM-dd HH:mm:ss"
        )
    ).otherwise(None)


@datetimes.register()
def start_of_quarter(col: Column) -> Column:
    """
    Get the first day of the quarter for a given date.
    """
    # Standardize first
    standardized = standardize_iso(col)
    ts = F.to_timestamp(standardized, "yyyy-MM-dd HH:mm:ss")

    # Get quarter (1-4) and year
    quarter = F.quarter(ts)
    year = F.year(ts)

    # Calculate first month of quarter: (quarter - 1) * 3 + 1
    first_month = (quarter - 1) * 3 + 1

    # Build the first day of quarter date
    return F.when(
        standardized.isNotNull(),
        F.concat(
            F.lpad(year.cast("string"), 4, "0"),
            F.lit("-"),
            F.lpad(first_month.cast("string"), 2, "0"),
            F.lit("-01 00:00:00")
        )
    ).otherwise(None)


@datetimes.register()
def end_of_quarter(col: Column) -> Column:
    """
    Get the last day of the quarter for a given date.
    """
    # Standardize first
    standardized = standardize_iso(col)
    ts = F.to_timestamp(standardized, "yyyy-MM-dd HH:mm:ss")

    # Get quarter (1-4) and year
    quarter = F.quarter(ts)
    year = F.year(ts)

    # Calculate last month of quarter: quarter * 3
    last_month = quarter * 3

    # Build a date with the last month and day 1, then get last_day
    temp_date = F.to_date(
        F.concat(
            F.lpad(year.cast("string"), 4, "0"),
            F.lit("-"),
            F.lpad(last_month.cast("string"), 2, "0"),
            F.lit("-01")
        )
    )

    return F.when(
        standardized.isNotNull(),
        F.date_format(F.last_day(temp_date), "yyyy-MM-dd HH:mm:ss")
    ).otherwise(None)


@datetimes.register()
def fiscal_year(col: Column, fiscal_start_month: int = 10) -> Column:
    """
    Get fiscal year for a date.
    Default assumes October 1 fiscal year start (US government).
    """
    # Standardize first
    standardized = standardize_iso(col)
    ts = F.to_timestamp(standardized, "yyyy-MM-dd HH:mm:ss")

    # Get calendar year and month
    year = F.year(ts)
    month = F.month(ts)

    # If month >= fiscal_start_month, fiscal year is year + 1
    # Otherwise, fiscal year is same as calendar year
    return F.when(
        standardized.isNotNull(),
        F.when(month >= fiscal_start_month, year + 1).otherwise(year)
    ).otherwise(None)


# ============================================================================
# Age and Duration Functions
# ============================================================================


@datetimes.register()
def calculate_age(col: Column, reference_date: Optional[Column] = None) -> Column:
    """
    Calculate age in years from a birthdate.
    Uses current_date() if reference_date not provided.
    """
    # Standardize birthdate
    standardized_birth = standardize_iso(col)
    birth_ts = F.to_timestamp(standardized_birth, "yyyy-MM-dd HH:mm:ss")

    # Get reference date
    if reference_date is None:
        ref_ts = F.current_timestamp()
    else:
        standardized_ref = standardize_iso(reference_date)
        ref_ts = F.to_timestamp(standardized_ref, "yyyy-MM-dd HH:mm:ss")

    # Calculate years difference
    return F.when(
        standardized_birth.isNotNull(),
        F.floor(F.months_between(ref_ts, birth_ts) / 12)
    ).otherwise(None)


@datetimes.register()
def format_duration(seconds_col: Column) -> Column:
    """
    Format duration in seconds to human-readable string.

    Examples:
        90 -> "1 minute 30 seconds"
        3661 -> "1 hour 1 minute 1 second"
        86400 -> "1 day"
    """
    # TODO: Implement duration formatting
    return seconds_col
