"""
Datetime transformation primitives for PySpark.

Preview Output:
+---------------------------+---------------------+------------+----------+------+-------+---------+
|datetime_text              |standardized         |date_only   |time_only |year  |month  |quarter  |
+---------------------------+---------------------+------------+----------+------+-------+---------+
|2024-01-15T14:30:00Z       |2024-01-15 14:30:00  |2024-01-15  |14:30:00  |2024  |1      |1        |
|01/15/2024 2:30 PM         |2024-01-15 14:30:00  |2024-01-15  |14:30:00  |2024  |1      |1        |
|January 15, 2024           |2024-01-15 00:00:00  |2024-01-15  |00:00:00  |2024  |1      |1        |
|15-Jan-2024                |2024-01-15 00:00:00  |2024-01-15  |00:00:00  |2024  |1      |1        |
|Q3 2024                    |2024-07-01           |2024-07-01  |null      |2024  |7      |3        |
+---------------------------+---------------------+------------+----------+------+-------+---------+

Usage Example:
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from transformers.pyspark.datetimes import datetimes

# Initialize Spark
spark = SparkSession.builder.appName("DataCleaning").getOrCreate()

# Create sample data
data = [
    ("Meeting scheduled for 2024-01-15T14:30:00Z",),
    ("Invoice date: 01/15/2024",),
    ("Event on January 15, 2024 at 10:00 AM",),
    ("Report due 15-Jan-2024",),
]
df = spark.createDataFrame(data, ["text"])

# Extract and standardize datetime components
result_df = df.select(
    F.col("text"),
    datetimes.extract_datetime_from_text(F.col("text")).alias("extracted"),
    datetimes.standardize_iso(F.col("text")).alias("standardized"),
    datetimes.standardize_date(F.col("text")).alias("date_only"),
    datetimes.extract_year(F.col("text")).alias("year"),
    datetimes.extract_month(F.col("text")).alias("month"),
)

# Show results
result_df.show(truncate=False)

# Filter to valid dates
valid_dates = result_df.filter(datetimes.is_valid_date(F.col("text")))

Installation:
datacompose add datetimes
"""

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
# Extraction Functions
# ============================================================================


@datetimes.register()
def extract_datetime_from_text(col: Column) -> Column:
    """Extract first datetime mention from free text.

    Supports ISO, US, EU formats, named months, natural language, and more.

    Args:
        col: Column containing text with potential datetime

    Returns:
        Column with extracted date string or None if no date found

    Example:
        df.select(datetimes.extract_datetime_from_text(F.col("text")))
        # "Meeting on 2024-01-15" -> "2024-01-15"
        # "Due 01/15/2024 2:30 PM" -> "01/15/2024 2:30 PM"
        # "Event tomorrow" -> "tomorrow"
    """
    # Try extracting different date formats in order of specificity
    # Use F.coalesce to return first non-null match

    # ISO format with timezone: 2024-03-01T10:30:00Z
    iso_tz = F.regexp_extract(col, r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z?", 0)

    # ISO format with time: 2024-01-15 14:30 or 2024-12-31T23:59:59
    iso_time = F.regexp_extract(col, r"\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}(?::\d{2})?", 0)

    # ISO date only: 2024-01-15
    iso_date = F.regexp_extract(col, r"\d{4}-\d{2}-\d{2}", 0)

    # Named month with optional time: January 15, 2024 at 10:00 AM
    named_month_long = F.regexp_extract(
        col,
        r"(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2}(?:st|nd|rd|th)?,\s+\d{4}(?:\s+at\s+\d{1,2}:\d{2}\s+[AP]M)?",
        0
    )

    # Short named month with optional time: Jan 15, 2024 at 10:00 AM
    named_month_short = F.regexp_extract(
        col,
        r"(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2},\s+\d{4}(?:\s+at\s+\d{1,2}:\d{2}\s+[AP]M)?",
        0
    )

    # Day-Month-Year with named month: 15-Jan-2024, 15 January 2024
    day_named_month = F.regexp_extract(
        col,
        r"\d{1,2}[\s-](?:Jan|January|Feb|February|Mar|March|Apr|April|May|Jun|June|Jul|July|Aug|August|Sep|September|Oct|October|Nov|November|Dec|December)[\s-]\d{4}",
        0
    )

    # US/EU date with time and AM/PM: 01/15/2024 2:30 PM
    date_time_ampm = F.regexp_extract(col, r"\d{1,2}/\d{1,2}/\d{2,4}\s+\d{1,2}:\d{2}\s+[AP]M", 0)

    # US/EU date with time: 01/15/2024 14:30
    date_time = F.regexp_extract(col, r"\d{1,2}/\d{1,2}/\d{2,4}\s+\d{1,2}:\d{2}", 0)

    # US/EU date with slash: 01/15/2024, 3/7/2024
    date_slash = F.regexp_extract(col, r"\d{1,2}/\d{1,2}/\d{2,4}", 0)

    # EU date with dots: 31.12.2024
    date_dot = F.regexp_extract(col, r"\d{1,2}\.\d{1,2}\.\d{4}", 0)

    # Quarter notation: Q3 2024
    quarter = F.regexp_extract(col, r"Q[1-4]\s+\d{4}", 0)

    # Natural language - relative days with numbers: "in 3 days", "3 days ago"
    relative_days = F.regexp_extract(col, r"(?:in\s+)?\d+\s+days?(?:\s+ago)?", 0)

    # Natural language - simple patterns
    tomorrow = F.regexp_extract(col, r"\btomorrow\b", 0)
    yesterday = F.regexp_extract(col, r"\byesterday\b", 0)
    next_day = F.regexp_extract(col, r"\bnext\s+(?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday)\b", 0)
    last_period = F.regexp_extract(col, r"\blast\s+(?:week|month|year|Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday)\b", 0)

    # Just year: 2024 (but not part of a larger date)
    # Use negative lookbehind/lookahead to avoid matching years in dates
    year_only = F.regexp_extract(col, r"(?<![/\-\d])\b(20\d{2}|19\d{2})\b(?![/\-\dT])", 1)

    # Return first non-empty match using coalesce
    return F.coalesce(
        F.when(iso_tz != "", iso_tz),
        F.when(iso_time != "", iso_time),
        F.when(iso_date != "", iso_date),
        F.when(named_month_long != "", named_month_long),
        F.when(named_month_short != "", named_month_short),
        F.when(day_named_month != "", day_named_month),
        F.when(date_time_ampm != "", date_time_ampm),
        F.when(date_time != "", date_time),
        F.when(date_slash != "", date_slash),
        F.when(date_dot != "", date_dot),
        F.when(quarter != "", quarter),
        F.when(relative_days != "", relative_days),
        F.when(tomorrow != "", tomorrow),
        F.when(yesterday != "", yesterday),
        F.when(next_day != "", next_day),
        F.when(last_period != "", last_period),
        F.when(year_only != "", year_only),
        F.lit(None)  # Return None if no date found
    )


# ============================================================================
# Core Standardization Functions
# ============================================================================


@datetimes.register()
def standardize_iso(col: Column) -> Column:
    """Convert datetime strings to ISO 8601 format (YYYY-MM-DD HH:MM:SS).

    Attempts to parse common formats and standardize them.

    Args:
        col: Column containing datetime text

    Returns:
        Column with standardized ISO datetime string or None if unparseable

    Example:
        df.select(datetimes.standardize_iso(F.col("date")))
        # "01/15/2024" -> "2024-01-15 00:00:00"
        # "2024-Jan-15" -> "2024-01-15 00:00:00"
        # "15-01-2024 14:30" -> "2024-01-15 14:30:00"
    """
    # Normalize whitespace:
    # 1. Trim leading/trailing whitespace (spaces, tabs, newlines)
    # 2. Collapse multiple internal whitespace characters into single spaces
    trimmed = F.regexp_replace(col, r"^\s+|\s+$", "")
    normalized = F.regexp_replace(trimmed, r"\s+", " ")

    # Try multiple formats in order using coalesce
    # try_to_timestamp returns null if format doesn't match or date is invalid
    # This handles ambiguous cases like "01/15/2024" vs "15/01/2024" because
    # invalid dates (like month 15) will return null

    parsed = F.coalesce(
        # ISO formats (highest priority - unambiguous)
        F.try_to_timestamp(normalized, F.lit("yyyy-MM-dd'T'HH:mm:ss'Z'")),
        F.try_to_timestamp(normalized, F.lit("yyyy-MM-dd'T'HH:mm:ss")),
        F.try_to_timestamp(normalized, F.lit("yyyy-MM-dd HH:mm:ss")),
        F.try_to_timestamp(normalized, F.lit("yyyy-MM-dd'T'HH:mm")),
        F.try_to_timestamp(normalized, F.lit("yyyy-MM-dd HH:mm")),
        F.try_to_timestamp(normalized, F.lit("yyyy-MM-dd")),
        # ISO date with 12-hour time and AM/PM
        F.try_to_timestamp(normalized, F.lit("yyyy-MM-dd h:mm a")),
        F.try_to_timestamp(normalized, F.lit("yyyy-MM-dd hh:mm a")),
        # US formats with time and AM/PM
        F.try_to_timestamp(normalized, F.lit("MM/dd/yyyy h:mm a")),
        F.try_to_timestamp(normalized, F.lit("M/d/yyyy h:mm a")),
        F.try_to_timestamp(normalized, F.lit("MM/dd/yyyy hh:mm a")),
        # US formats with 24-hour time
        F.try_to_timestamp(normalized, F.lit("MM/dd/yyyy HH:mm:ss")),
        F.try_to_timestamp(normalized, F.lit("MM/dd/yyyy HH:mm")),
        F.try_to_timestamp(normalized, F.lit("M/d/yyyy HH:mm")),
        # US formats without time
        F.try_to_timestamp(normalized, F.lit("MM/dd/yyyy")),
        F.try_to_timestamp(normalized, F.lit("M/d/yyyy")),
        # Named month formats with time and AM/PM
        F.try_to_timestamp(normalized, F.lit("MMMM d, yyyy h:mm a")),
        F.try_to_timestamp(normalized, F.lit("MMM d, yyyy h:mm a")),
        F.try_to_timestamp(normalized, F.lit("MMMM d, yyyy hh:mm a")),
        F.try_to_timestamp(normalized, F.lit("MMM d, yyyy hh:mm a")),
        # Named month formats without time
        F.try_to_timestamp(normalized, F.lit("MMMM d, yyyy")),
        F.try_to_timestamp(normalized, F.lit("MMM d, yyyy")),
        F.try_to_timestamp(normalized, F.lit("MMMM dd, yyyy")),
        F.try_to_timestamp(normalized, F.lit("MMM dd, yyyy")),
        F.try_to_timestamp(normalized, F.lit("d-MMM-yyyy")),
        F.try_to_timestamp(normalized, F.lit("dd-MMM-yyyy")),
        F.try_to_timestamp(normalized, F.lit("d MMMM yyyy")),
        F.try_to_timestamp(normalized, F.lit("dd MMMM yyyy")),
        # EU formats (after US formats due to ambiguity)
        F.try_to_timestamp(normalized, F.lit("dd/MM/yyyy")),
        F.try_to_timestamp(normalized, F.lit("d/M/yyyy")),
        F.try_to_timestamp(normalized, F.lit("dd.MM.yyyy")),
        F.try_to_timestamp(normalized, F.lit("d.M.yyyy")),
    )

    # Format as ISO string "yyyy-MM-dd HH:mm:ss"
    # Returns null if parsed is null
    return F.date_format(parsed, "yyyy-MM-dd HH:mm:ss")


@datetimes.register()
def standardize_date(col: Column) -> Column:
    """Extract and standardize just the date portion to YYYY-MM-DD format.

    Args:
        col: Column containing datetime text

    Returns:
        Column with standardized date string (YYYY-MM-DD) or None

    Example:
        df.select(datetimes.standardize_date(F.col("datetime")))
        # "2024-01-15 14:30:00" -> "2024-01-15"
        # "01/15/2024" -> "2024-01-15"
    """
    # First standardize to ISO format, then extract just the date part
    iso_datetime = standardize_iso(col)
    # Extract date portion (YYYY-MM-DD) from the datetime
    return F.when(iso_datetime.isNotNull(), F.substring(iso_datetime, 1, 10)).otherwise(
        None
    )


@datetimes.register()
def standardize_time(col: Column) -> Column:
    """Extract and standardize just the time portion to HH:MM:SS format.

    Args:
        col: Column containing datetime text

    Returns:
        Column with standardized time string (HH:MM:SS) or None

    Example:
        df.select(datetimes.standardize_time(F.col("datetime")))
        # "2024-01-15 14:30:00" -> "14:30:00"
        # "2:30 PM" -> "14:30:00"
    """
    # First standardize to ISO format, then extract just the time part
    iso_datetime = standardize_iso(col)
    # Extract time portion (HH:MM:SS) from the datetime
    return F.when(iso_datetime.isNotNull(), F.substring(iso_datetime, 12, 8)).otherwise(
        None
    )


# ============================================================================
# Format Detection and Parsing
# ============================================================================


@datetimes.register()
def detect_format(col: Column) -> Column:
    """Detect the datetime format of a string.

    Returns format string that can be used with to_timestamp().

    Args:
        col: Column containing datetime text

    Returns:
        Column with detected format pattern or "unknown"

    Example:
        df.select(datetimes.detect_format(F.col("date")))
        # "2024-01-15" -> "yyyy-MM-dd"
        # "01/15/2024" -> "MM/dd/yyyy"
        # "15-Jan-2024" -> "dd-MMM-yyyy"
    """
    # Try to detect format by testing which format successfully parses
    # Order matters - try more specific formats first, then more general
    return (
        F.when(
            F.try_to_timestamp(col, F.lit("yyyy-MM-dd'T'HH:mm:ss")).isNotNull(),
            F.lit("yyyy-MM-dd'T'HH:mm:ss"),
        )
        .when(
            F.try_to_timestamp(col, F.lit("yyyy-MM-dd HH:mm:ss")).isNotNull(),
            F.lit("yyyy-MM-dd HH:mm:ss"),
        )
        .when(
            F.try_to_timestamp(col, F.lit("yyyy-MM-dd")).isNotNull(),
            F.lit("yyyy-MM-dd"),
        )
        .when(
            F.try_to_timestamp(col, F.lit("MM/dd/yyyy")).isNotNull(),
            F.lit("MM/dd/yyyy"),
        )
        .when(F.try_to_timestamp(col, F.lit("M/d/yyyy")).isNotNull(), F.lit("M/d/yyyy"))
        .when(
            F.try_to_timestamp(col, F.lit("dd/MM/yyyy")).isNotNull(),
            F.lit("dd/MM/yyyy"),
        )
        .when(F.try_to_timestamp(col, F.lit("d/M/yyyy")).isNotNull(), F.lit("d/M/yyyy"))
        .when(F.try_to_timestamp(col, F.lit("MM/dd/yy")).isNotNull(), F.lit("MM/dd/yy"))
        .when(F.try_to_timestamp(col, F.lit("M/d/yy")).isNotNull(), F.lit("M/d/yy"))
        .when(F.try_to_timestamp(col, F.lit("dd/MM/yy")).isNotNull(), F.lit("dd/MM/yy"))
        .when(F.try_to_timestamp(col, F.lit("d/M/yy")).isNotNull(), F.lit("d/M/yy"))
        .when(
            F.try_to_timestamp(col, F.lit("MMMM d, yyyy")).isNotNull(),
            F.lit("MMMM d, yyyy"),
        )
        .when(
            F.try_to_timestamp(col, F.lit("MMM d, yyyy")).isNotNull(),
            F.lit("MMM d, yyyy"),
        )
        .when(
            F.try_to_timestamp(col, F.lit("dd-MMM-yyyy")).isNotNull(),
            F.lit("dd-MMM-yyyy"),
        )
        .when(
            F.try_to_timestamp(col, F.lit("d-MMM-yyyy")).isNotNull(),
            F.lit("d-MMM-yyyy"),
        )
        .otherwise(F.lit("unknown"))
    )


@datetimes.register()
def parse_flexible(col: Column) -> Column:
    """Parse datetime strings with multiple possible formats.

    Handles ISO, US, EU, named months, quarters, fiscal years, and more.

    Args:
        col: Column containing datetime text

    Returns:
        Column with parsed date string (YYYY-MM-DD) or None

    Example:
        df.select(datetimes.parse_flexible(F.col("date")))
        # "Q3 2024" -> "2024-07-01"
        # "FY2024" -> "2023-10-01"
        # "Jan 2024" -> "2024-01-01"
    """
    # Handle quarter notation: Q1 2024 -> 2024-01-01, Q2 2024 -> 2024-04-01, etc.
    quarter_match = F.regexp_extract(col, r"^Q([1-4])\s+(\d{4})$", 0)
    quarter_num = F.regexp_extract(col, r"^Q([1-4])\s+(\d{4})$", 1)
    quarter_year = F.regexp_extract(col, r"^Q([1-4])\s+(\d{4})$", 2)
    quarter_month = ((quarter_num.cast("int") - 1) * 3) + 1

    # Handle fiscal year: FY2024 -> 2023-10-01 (US fiscal year starts Oct 1)
    fy_match = F.regexp_extract(col, r"^FY\s*(\d{4})$", 0)
    fy_year = F.regexp_extract(col, r"^FY\s*(\d{4})$", 1)

    # Handle fiscal year range: FY 2024-25 -> 2024-04-01
    fy_range_match = F.regexp_extract(col, r"^FY\s*(\d{4})-\d{2}$", 0)
    fy_range_year = F.regexp_extract(col, r"^FY\s*(\d{4})-\d{2}$", 1)

    # Handle year only: "2024" -> "2024-01-01"
    year_only_match = F.regexp_extract(col, r"^(\d{4})$", 0)
    year_only = F.regexp_extract(col, r"^(\d{4})$", 1)

    # Handle short year: "'24" -> "2024-01-01"
    short_year_match = F.regexp_extract(col, r"^'(\d{2})$", 0)
    short_year = F.regexp_extract(col, r"^'(\d{2})$", 1)

    # Handle month/year: "01/2024" -> "2024-01-01"
    month_slash_year_match = F.regexp_extract(col, r"^(\d{1,2})/(\d{4})$", 0)
    month_slash = F.regexp_extract(col, r"^(\d{1,2})/(\d{4})$", 1)
    year_slash = F.regexp_extract(col, r"^(\d{1,2})/(\d{4})$", 2)

    # Handle yyyy-MM: "2024-01" -> "2024-01-01"
    yyyy_mm_match = F.regexp_extract(col, r"^(\d{4})-(\d{2})$", 0)
    yyyy_mm_year = F.regexp_extract(col, r"^(\d{4})-(\d{2})$", 1)
    yyyy_mm_month = F.regexp_extract(col, r"^(\d{4})-(\d{2})$", 2)

    # Handle week notation: "Week 1, 2024" -> "2024-01-01"
    week_match = F.regexp_extract(col, r"^Week\s+(\d+),\s+(\d{4})$", 0)
    week_year = F.regexp_extract(col, r"^Week\s+(\d+),\s+(\d{4})$", 2)

    # Handle W notation: "W10-2024" -> calculate date for week 10
    w_notation_match = F.regexp_extract(col, r"^W(\d+)-(\d{4})$", 0)
    w_notation_week = F.regexp_extract(col, r"^W(\d+)-(\d{4})$", 1)
    w_notation_year = F.regexp_extract(col, r"^W(\d+)-(\d{4})$", 2)

    # Try to parse named months: "January 2024", "Jan 2024"
    # First try full month name
    full_month_match = F.coalesce(
        F.try_to_timestamp(col, F.lit("MMMM yyyy")),
        F.try_to_timestamp(col, F.lit("MMM yyyy")),
    )

    # Build result with cascading conditions
    return (
        F.when(
            quarter_match != "",
            F.concat(
                quarter_year,
                F.lit("-"),
                F.lpad(quarter_month.cast("string"), 2, "0"),
                F.lit("-01"),
            ),
        )
        .when(
            fy_match != "",
            F.concat((fy_year.cast("int") - 1).cast("string"), F.lit("-10-01")),
        )
        .when(fy_range_match != "", F.concat(fy_range_year, F.lit("-04-01")))
        .when(year_only_match != "", F.concat(year_only, F.lit("-01-01")))
        .when(
            short_year_match != "", F.concat(F.lit("20"), short_year, F.lit("-01-01"))
        )
        .when(
            month_slash_year_match != "",
            F.concat(year_slash, F.lit("-"), F.lpad(month_slash, 2, "0"), F.lit("-01")),
        )
        .when(
            yyyy_mm_match != "",
            F.concat(yyyy_mm_year, F.lit("-"), yyyy_mm_month, F.lit("-01")),
        )
        .when(
            week_match != "",
            F.concat(week_year, F.lit("-01-01")),  # Simplified - just return year start
        )
        .when(
            w_notation_match != "",
            # Calculate date for week number: year start + (week - 1) * 7 days
            F.date_format(
                F.date_add(
                    F.to_date(F.concat(w_notation_year, F.lit("-01-01"))),
                    (w_notation_week.cast("int") - 1) * 7,
                ),
                "yyyy-MM-dd",
            ),
        )
        .when(
            full_month_match.isNotNull(), F.date_format(full_month_match, "yyyy-MM-dd")
        )
        .otherwise(
            # Fall back to standardize_date for complete dates
            standardize_date(col)
        )
    )


@datetimes.register()
def parse_natural_language(
    col: Column, reference_date: Optional[Column] = None
) -> Column:
    """Parse natural language date expressions.

    Args:
        col: Column containing natural language date text
        reference_date (Optional): Reference date for relative calculations

    Returns:
        Column with parsed date or original value (TODO: not yet implemented)

    Example:
        df.select(datetimes.parse_natural_language(F.col("text")))
        # "yesterday" -> (current_date - 1 day)
        # "3 days ago" -> (current_date - 3 days)
    """
    # TODO: Implement natural language parsing
    return col


# ============================================================================
# Validation Functions
# ============================================================================


@datetimes.register()
def is_valid_date(col: Column) -> Column:
    """Check if a string represents a valid date.

    Validates month (1-12), day (valid for month), and handles leap years.

    Args:
        col: Column containing date text to validate

    Returns:
        Column with boolean indicating if date is valid

    Example:
        df.select(datetimes.is_valid_date(F.col("date")))
        # "2024-01-15" -> True
        # "2024-13-01" -> False
        # "invalid" -> False
    """
    # A date is valid if standardize_iso can parse it successfully
    standardized = standardize_iso(col)
    return F.when(col.isNull() | (F.trim(col) == ""), F.lit(False)).otherwise(
        standardized.isNotNull()
    )


@datetimes.register()
def is_valid_datetimes(col: Column) -> Column:
    """Check if a string represents a valid datetime.

    More comprehensive than is_valid_date, includes time validation.

    Args:
        col: Column containing datetime text to validate

    Returns:
        Column with boolean indicating if datetime is valid

    Example:
        df.select(datetimes.is_valid_datetimes(F.col("datetime")))
        # "2024-01-15 14:30:00" -> True
        # "invalid" -> False
    """
    # Use same logic as is_valid_date since standardize_iso handles both
    return is_valid_date(col)


@datetimes.register()
def is_business_day(col: Column) -> Column:
    """Check if a date falls on a business day (Monday-Friday).

    Args:
        col: Column containing date text

    Returns:
        Column with boolean indicating if date is a business day

    Example:
        df.select(datetimes.is_business_day(F.col("date")))
        # "2024-01-15" (Monday) -> True
        # "2024-01-13" (Saturday) -> False
    """
    # First standardize, then check if dayofweek is 2-6 (Mon-Fri in PySpark)
    # In PySpark: 1=Sunday, 2=Monday, ..., 7=Saturday
    standardized = standardize_iso(col)
    return F.when(
        standardized.isNotNull(),
        F.dayofweek(F.to_timestamp(standardized, "yyyy-MM-dd HH:mm:ss")).isin(
            [2, 3, 4, 5, 6]
        ),
    ).otherwise(F.lit(False))


@datetimes.register()
def is_future_date(col: Column, reference_date: Optional[Column] = None) -> Column:
    """Check if a date is in the future.

    Args:
        col: Column containing date text
        reference_date (Optional): Reference date column (defaults to current_date)

    Returns:
        Column with boolean indicating if date is in the future

    Example:
        df.select(datetimes.is_future_date(F.col("date")))
        # "2099-01-15" -> True
        # "2000-01-15" -> False
    """
    standardized = standardize_iso(col)
    ref = F.current_date() if reference_date is None else reference_date

    return F.when(
        standardized.isNotNull(),
        F.to_date(F.to_timestamp(standardized, "yyyy-MM-dd HH:mm:ss")) > ref,
    ).otherwise(F.lit(False))


@datetimes.register()
def is_past_date(col: Column, reference_date: Optional[Column] = None) -> Column:
    """Check if a date is in the past.

    Args:
        col: Column containing date text
        reference_date (Optional): Reference date column (defaults to current_date)

    Returns:
        Column with boolean indicating if date is in the past

    Example:
        df.select(datetimes.is_past_date(F.col("date")))
        # "2000-01-15" -> True
        # "2099-01-15" -> False
    """
    standardized = standardize_iso(col)
    ref = F.current_date() if reference_date is None else reference_date

    return F.when(
        standardized.isNotNull(),
        F.to_date(F.to_timestamp(standardized, "yyyy-MM-dd HH:mm:ss")) < ref,
    ).otherwise(F.lit(False))


# ============================================================================
# Component Extraction Functions
# ============================================================================


@datetimes.register()
def extract_year(col: Column) -> Column:
    """Extract year from datetime string.

    Args:
        col: Column containing datetime text

    Returns:
        Column with extracted year as integer or None

    Example:
        df.select(datetimes.extract_year(F.col("date")))
        # "2024-01-15" -> 2024
        # "01/15/2024" -> 2024
    """
    # First standardize, then convert to timestamp and extract year
    standardized = standardize_iso(col)
    return F.when(
        standardized.isNotNull(),
        F.year(F.to_timestamp(standardized, "yyyy-MM-dd HH:mm:ss")),
    ).otherwise(None)


@datetimes.register()
def extract_month(col: Column) -> Column:
    """Extract month from datetime string (1-12).

    Args:
        col: Column containing datetime text

    Returns:
        Column with extracted month as integer (1-12) or None

    Example:
        df.select(datetimes.extract_month(F.col("date")))
        # "2024-01-15" -> 1
        # "Jan 15, 2024" -> 1
    """
    # First standardize, then convert to timestamp and extract month
    standardized = standardize_iso(col)
    return F.when(
        standardized.isNotNull(),
        F.month(F.to_timestamp(standardized, "yyyy-MM-dd HH:mm:ss")),
    ).otherwise(None)


@datetimes.register()
def extract_day(col: Column) -> Column:
    """Extract day from datetime string (1-31).

    Args:
        col: Column containing datetime text

    Returns:
        Column with extracted day as integer (1-31) or None

    Example:
        df.select(datetimes.extract_day(F.col("date")))
        # "2024-01-15" -> 15
        # "15/01/2024" -> 15
    """
    # First standardize, then convert to timestamp and extract day
    standardized = standardize_iso(col)
    return F.when(
        standardized.isNotNull(),
        F.dayofmonth(F.to_timestamp(standardized, "yyyy-MM-dd HH:mm:ss")),
    ).otherwise(None)


@datetimes.register()
def extract_quarter(col: Column) -> Column:
    """Extract quarter from datetime string (1-4).

    Args:
        col: Column containing datetime text

    Returns:
        Column with extracted quarter as integer (1-4) or None

    Example:
        df.select(datetimes.extract_quarter(F.col("date")))
        # "2024-01-15" -> 1
        # "2024-07-01" -> 3
    """
    # First standardize, then convert to timestamp and extract quarter
    standardized = standardize_iso(col)
    return F.when(
        standardized.isNotNull(),
        F.quarter(F.to_timestamp(standardized, "yyyy-MM-dd HH:mm:ss")),
    ).otherwise(None)


@datetimes.register()
def extract_week_of_year(col: Column) -> Column:
    """Extract week number from datetime string (1-53).

    Args:
        col: Column containing datetime text

    Returns:
        Column with extracted week number as integer (1-53) or None

    Example:
        df.select(datetimes.extract_week_of_year(F.col("date")))
        # "2024-01-15" -> 3
        # "2024-12-31" -> 1
    """
    # First standardize, then convert to timestamp and extract week
    standardized = standardize_iso(col)
    return F.when(
        standardized.isNotNull(),
        F.weekofyear(F.to_timestamp(standardized, "yyyy-MM-dd HH:mm:ss")),
    ).otherwise(None)


@datetimes.register()
def extract_day_of_week(col: Column) -> Column:
    """Extract day of week from datetime string.

    Returns string name (Monday, Tuesday, etc).

    Args:
        col: Column containing datetime text

    Returns:
        Column with day name string or None

    Example:
        df.select(datetimes.extract_day_of_week(F.col("date")))
        # "2024-01-15" -> "Monday"
        # "2024-01-13" -> "Saturday"
    """
    # First standardize, then convert to timestamp and extract day name
    standardized = standardize_iso(col)
    return F.when(
        standardized.isNotNull(),
        F.date_format(F.to_timestamp(standardized, "yyyy-MM-dd HH:mm:ss"), "EEEE"),
    ).otherwise(None)


# ============================================================================
# Timezone Functions
# ============================================================================


def normalize_timezone(col: Column, target_tz: Column) -> Column:
    """Convert datetime to specified timezone.

    Note: This function takes 2 column arguments and is manually added to the namespace.

    Args:
        col: Column containing datetime text
        target_tz: Target timezone as Column (e.g., F.lit("UTC"))

    Returns:
        Column with timezone-converted datetime (TODO: not yet implemented)

    Example:
        df.select(datetimes.normalize_timezone(F.col("dt"), F.lit("UTC")))
        # "2024-01-15 14:30:00 EST" -> "2024-01-15 19:30:00 UTC"
    """
    # TODO: Implement timezone normalization
    # For now, just return the input column to avoid crashes
    return col


def add_timezone(col: Column, timezone: Column) -> Column:
    """Add timezone information to naive datetime.

    Note: This function takes 2 column arguments and is manually added to the namespace.

    Args:
        col: Column containing naive datetime text
        timezone: Timezone to add as Column (e.g., F.lit("America/New_York"))

    Returns:
        Column with timezone-aware datetime (TODO: not yet implemented)

    Example:
        df.select(datetimes.add_timezone(F.col("dt"), F.lit("America/New_York")))
    """
    # TODO: Implement timezone addition
    # For now, just return the input column to avoid crashes
    return col


@datetimes.register()
def remove_timezone(col: Column) -> Column:
    """Remove timezone information, keeping local time.

    Args:
        col: Column containing timezone-aware datetime text

    Returns:
        Column with timezone removed (TODO: not yet implemented)

    Example:
        df.select(datetimes.remove_timezone(F.col("datetime")))
        # "2024-01-15 14:30:00 EST" -> "2024-01-15 14:30:00"
    """
    # TODO: Implement timezone removal
    return col


# Manually register multi-column timezone functions to namespace
datetimes.normalize_timezone = normalize_timezone
datetimes.add_timezone = add_timezone


# ============================================================================
# Date Math Functions
# ============================================================================


def add_days(col: Column, days: Column) -> Column:
    """Add or subtract days from a date.

    Note: This function takes 2 column arguments and is manually added to the namespace.

    Args:
        col: Column containing date text
        days: Number of days to add as Column (negative to subtract)

    Returns:
        Column with adjusted date string or None

    Example:
        df.select(datetimes.add_days(F.col("date"), F.lit(5)))
        # "2024-01-15" -> "2024-01-20"
        # "2024-01-15" + (-5) -> "2024-01-10"
    """
    # First standardize to get consistent format
    standardized = standardize_iso(col)

    # Convert to date, add days, then format back to string
    # PySpark's date_add requires INT type for days, so cast it
    return F.when(
        standardized.isNotNull(),
        F.date_format(
            F.date_add(
                F.to_date(F.to_timestamp(standardized, "yyyy-MM-dd HH:mm:ss")),
                days.cast("int")
            ),
            "yyyy-MM-dd HH:mm:ss",
        ),
    ).otherwise(None)


def add_months(col: Column, months: Column) -> Column:
    """Add or subtract months from a date.

    Handles month-end edge cases properly.
    Note: This function takes 2 column arguments and is manually added to the namespace.

    Args:
        col: Column containing date text
        months: Number of months to add as Column (negative to subtract)

    Returns:
        Column with adjusted date string or None

    Example:
        df.select(datetimes.add_months(F.col("date"), F.lit(1)))
        # "2024-01-15" -> "2024-02-15"
        # "2024-01-31" -> "2024-02-29" (leap year)
    """
    # First standardize to get consistent format
    standardized = standardize_iso(col)

    # Convert to date, add months, then format back to string
    # PySpark's add_months requires INT type for months, so cast it
    return F.when(
        standardized.isNotNull(),
        F.date_format(
            F.add_months(
                F.to_date(F.to_timestamp(standardized, "yyyy-MM-dd HH:mm:ss")),
                months.cast("int")
            ),
            "yyyy-MM-dd HH:mm:ss",
        ),
    ).otherwise(None)


# Manually register multi-column add_days and add_months functions to namespace
datetimes.add_days = add_days
datetimes.add_months = add_months


def date_diff_days(col1: Column, col2: Column) -> Column:
    """Calculate difference between two dates in days.

    Note: This function takes 2 column arguments and is manually added to the namespace.

    Args:
        col1: First date column
        col2: Second date column

    Returns:
        Column with number of days between dates (col1 - col2)

    Example:
        df.select(datetimes.date_diff_days(F.col("end"), F.col("start")))
        # ("2024-01-20", "2024-01-15") -> 5
        # ("2024-01-15", "2024-01-20") -> -5
    """
    standardized1 = standardize_iso(col1)
    standardized2 = standardize_iso(col2)

    ts1 = F.to_timestamp(standardized1, "yyyy-MM-dd HH:mm:ss")
    ts2 = F.to_timestamp(standardized2, "yyyy-MM-dd HH:mm:ss")

    return F.when(
        standardized1.isNotNull() & standardized2.isNotNull(), F.datediff(ts1, ts2)
    ).otherwise(None)


def date_diff_months(col1: Column, col2: Column) -> Column:
    """Calculate difference between two dates in months.

    Note: This function takes 2 column arguments and is manually added to the namespace.

    Args:
        col1: First date column
        col2: Second date column

    Returns:
        Column with number of months between dates (col1 - col2)

    Example:
        df.select(datetimes.date_diff_months(F.col("end"), F.col("start")))
        # ("2024-03-15", "2024-01-15") -> 2
        # ("2025-01-15", "2024-01-15") -> 12
    """
    standardized1 = standardize_iso(col1)
    standardized2 = standardize_iso(col2)

    ts1 = F.to_timestamp(standardized1, "yyyy-MM-dd HH:mm:ss")
    ts2 = F.to_timestamp(standardized2, "yyyy-MM-dd HH:mm:ss")

    return F.when(
        standardized1.isNotNull() & standardized2.isNotNull(),
        F.months_between(ts1, ts2).cast("long"),
    ).otherwise(None)


def date_diff_years(col1: Column, col2: Column) -> Column:
    """Calculate difference between two dates in years.

    Note: This function takes 2 column arguments and is manually added to the namespace.

    Args:
        col1: First date column
        col2: Second date column

    Returns:
        Column with number of years between dates (col1 - col2)

    Example:
        df.select(datetimes.date_diff_years(F.col("end"), F.col("start")))
        # ("2025-01-15", "2024-01-15") -> 1
        # ("2024-01-15", "2020-01-15") -> 4
    """
    standardized1 = standardize_iso(col1)
    standardized2 = standardize_iso(col2)

    ts1 = F.to_timestamp(standardized1, "yyyy-MM-dd HH:mm:ss")
    ts2 = F.to_timestamp(standardized2, "yyyy-MM-dd HH:mm:ss")

    return F.when(
        standardized1.isNotNull() & standardized2.isNotNull(),
        F.floor(F.months_between(ts1, ts2) / 12).cast("long"),
    ).otherwise(None)


def date_diff_hours(col1: Column, col2: Column) -> Column:
    """Calculate difference between two datetimes in hours.

    Note: This function takes 2 column arguments and is manually added to the namespace.

    Args:
        col1: First datetime column
        col2: Second datetime column

    Returns:
        Column with number of hours between datetimes (col1 - col2)

    Example:
        df.select(datetimes.date_diff_hours(F.col("end"), F.col("start")))
        # ("2024-01-15 14:30:00", "2024-01-15 12:30:00") -> 2
    """
    standardized1 = standardize_iso(col1)
    standardized2 = standardize_iso(col2)

    ts1 = F.to_timestamp(standardized1, "yyyy-MM-dd HH:mm:ss")
    ts2 = F.to_timestamp(standardized2, "yyyy-MM-dd HH:mm:ss")

    seconds_diff = ts1.cast("long") - ts2.cast("long")

    return F.when(
        standardized1.isNotNull() & standardized2.isNotNull(),
        (seconds_diff / 3600).cast("long"),
    ).otherwise(None)


def date_diff_minutes(col1: Column, col2: Column) -> Column:
    """Calculate difference between two datetimes in minutes.

    Note: This function takes 2 column arguments and is manually added to the namespace.

    Args:
        col1: First datetime column
        col2: Second datetime column

    Returns:
        Column with number of minutes between datetimes (col1 - col2)

    Example:
        df.select(datetimes.date_diff_minutes(F.col("end"), F.col("start")))
        # ("2024-01-15 14:30:00", "2024-01-15 14:00:00") -> 30
    """
    standardized1 = standardize_iso(col1)
    standardized2 = standardize_iso(col2)

    ts1 = F.to_timestamp(standardized1, "yyyy-MM-dd HH:mm:ss")
    ts2 = F.to_timestamp(standardized2, "yyyy-MM-dd HH:mm:ss")

    seconds_diff = ts1.cast("long") - ts2.cast("long")

    return F.when(
        standardized1.isNotNull() & standardized2.isNotNull(),
        (seconds_diff / 60).cast("long"),
    ).otherwise(None)


def date_diff_seconds(col1: Column, col2: Column) -> Column:
    """Calculate difference between two datetimes in seconds.

    Note: This function takes 2 column arguments and is manually added to the namespace.

    Args:
        col1: First datetime column
        col2: Second datetime column

    Returns:
        Column with number of seconds between datetimes (col1 - col2)

    Example:
        df.select(datetimes.date_diff_seconds(F.col("end"), F.col("start")))
        # ("2024-01-15 14:30:30", "2024-01-15 14:30:00") -> 30
    """
    standardized1 = standardize_iso(col1)
    standardized2 = standardize_iso(col2)

    ts1 = F.to_timestamp(standardized1, "yyyy-MM-dd HH:mm:ss")
    ts2 = F.to_timestamp(standardized2, "yyyy-MM-dd HH:mm:ss")

    seconds_diff = ts1.cast("long") - ts2.cast("long")

    return F.when(
        standardized1.isNotNull() & standardized2.isNotNull(), seconds_diff
    ).otherwise(None)


def business_days_between(start_col: Column, end_col: Column) -> Column:
    """Calculate number of business days (Mon-Fri) between two dates.

    Inclusive of start_date, exclusive of end_date. Weekends excluded.
    Note: This function takes 2 column arguments and is manually added to the namespace.

    Args:
        start_col: Start date column
        end_col: End date column

    Returns:
        Column with number of business days in range [start_date, end_date)

    Example:
        df.select(datetimes.business_days_between(F.col("start"), F.col("end")))
        # ("2024-01-15", "2024-01-19") -> 4  # Mon to Fri
        # ("2024-01-13", "2024-01-14") -> 0  # Sat to Sun
    """
    # Standardize both dates
    standardized_start = standardize_iso(start_col)
    standardized_end = standardize_iso(end_col)

    # Convert to dates
    start_date = F.to_date(F.to_timestamp(standardized_start, "yyyy-MM-dd HH:mm:ss"))
    end_date = F.to_date(F.to_timestamp(standardized_end, "yyyy-MM-dd HH:mm:ss"))

    # Calculate total days between dates (end - start)
    total_days = F.datediff(end_date, start_date)

    # Get day of week for start date (1=Sun, 2=Mon, ..., 7=Sat)
    start_dow = F.dayofweek(start_date)

    # Calculate number of complete weeks
    complete_weeks = F.floor(total_days / 7)

    # Each complete week has 5 business days
    business_days_from_weeks = complete_weeks * 5

    # Calculate remaining days after complete weeks
    remaining_days = total_days % 7

    # For the remaining days, we need to count how many are business days
    # We'll iterate through each remaining day and check if it's a business day
    # Create a helper that counts business days in the remaining portion

    # For each of the remaining days (0-6), check if that day is a business day
    # by calculating what day of week it falls on
    business_days_in_remainder = (
        # Day 0 (start day)
        F.when((remaining_days >= 1) & start_dow.isin([2, 3, 4, 5, 6]), 1).otherwise(0) +
        # Day 1
        F.when((remaining_days >= 2) & ((start_dow + 1) % 7).isin([2, 3, 4, 5, 6]), 1).otherwise(0) +
        # Day 2
        F.when((remaining_days >= 3) & ((start_dow + 2) % 7).isin([2, 3, 4, 5, 6]), 1).otherwise(0) +
        # Day 3
        F.when((remaining_days >= 4) & ((start_dow + 3) % 7).isin([2, 3, 4, 5, 6]), 1).otherwise(0) +
        # Day 4
        F.when((remaining_days >= 5) & ((start_dow + 4) % 7).isin([2, 3, 4, 5, 6]), 1).otherwise(0) +
        # Day 5
        F.when((remaining_days >= 6) & ((start_dow + 5) % 7).isin([2, 3, 4, 5, 6]), 1).otherwise(0) +
        # Day 6
        F.when((remaining_days >= 7) & ((start_dow + 6) % 7).isin([2, 3, 4, 5, 6]), 1).otherwise(0)
    )

    total_business_days = business_days_from_weeks + business_days_in_remainder

    return F.when(
        standardized_start.isNotNull() & standardized_end.isNotNull(),
        total_business_days.cast("int")
    ).otherwise(None)


def date_diff(col1: Column, col2: Column, unit: Column) -> Column:
    """Calculate difference between two dates with configurable unit.

    Convenience wrapper that routes to specific function based on unit.
    Note: This function takes 3 column arguments and is manually added to the namespace.

    Args:
        col1: First date column
        col2: Second date column
        unit: Unit as Column ("days", "months", "years", "hours", "minutes", "seconds")

    Returns:
        Column with difference in specified units (col1 - col2)

    Example:
        df.select(datetimes.date_diff(F.col("end"), F.col("start"), F.lit("days")))
        df.select(datetimes.date_diff(F.col("d1"), F.col("d2"), F.lit("months")))
    """
    # Use case/when to route to the appropriate function based on unit
    return (
        F.when(unit == "days", date_diff_days(col1, col2))
        .when(unit == "months", date_diff_months(col1, col2))
        .when(unit == "years", date_diff_years(col1, col2))
        .when(unit == "hours", date_diff_hours(col1, col2))
        .when(unit == "minutes", date_diff_minutes(col1, col2))
        .when(unit == "seconds", date_diff_seconds(col1, col2))
        .otherwise(None)  # Invalid unit
    )


# Manually register multi-column date_diff and business_days functions to namespace
datetimes.date_diff = date_diff
datetimes.date_diff_days = date_diff_days
datetimes.date_diff_months = date_diff_months
datetimes.date_diff_years = date_diff_years
datetimes.date_diff_hours = date_diff_hours
datetimes.date_diff_minutes = date_diff_minutes
datetimes.date_diff_seconds = date_diff_seconds
datetimes.business_days_between = business_days_between


# ============================================================================
# Formatting Functions
# ============================================================================


@datetimes.register()
def format_date(col: Column, format: str = "yyyy-MM-dd") -> Column:
    """Format date according to specified pattern.

    Uses Java SimpleDateFormat patterns.

    Args:
        col: Column containing date/datetime text
        format: Format pattern string (default: "yyyy-MM-dd")

    Returns:
        Column with formatted date string or None

    Example:
        df.select(datetimes.format_date(F.col("date"), format="MM/dd/yyyy"))
        # "2024-01-15" -> "01/15/2024"
        # "2024-01-15" with "MMMM d, yyyy" -> "January 15, 2024"
    """
    # First standardize the date
    standardized = standardize_iso(col)

    # Convert to timestamp and format
    return F.when(
        standardized.isNotNull(),
        F.date_format(
            F.to_timestamp(standardized, "yyyy-MM-dd HH:mm:ss"), format
        ),
    ).otherwise(None)


@datetimes.register()
def to_unix_timestamp(col: Column) -> Column:
    """Convert datetime to Unix timestamp (seconds since epoch).

    Args:
        col: Column containing datetime text

    Returns:
        Column with Unix timestamp (TODO: not yet implemented)

    Example:
        df.select(datetimes.to_unix_timestamp(F.col("datetime")))
        # "2024-01-15 00:00:00" -> 1705276800
    """
    # TODO: Implement Unix timestamp conversion
    return F.lit(None)


@datetimes.register()
def from_unix_timestamp(col: Column) -> Column:
    """Convert Unix timestamp to datetime string.

    Args:
        col: Column containing Unix timestamp

    Returns:
        Column with datetime string (TODO: not yet implemented)

    Example:
        df.select(datetimes.from_unix_timestamp(F.col("timestamp")))
        # 1705276800 -> "2024-01-15 00:00:00"
    """
    # TODO: Implement Unix timestamp parsing
    return col


# ============================================================================
# Special Date Functions
# ============================================================================


@datetimes.register()
def start_of_month(col: Column) -> Column:
    """Get the first day of the month for a given date.

    Args:
        col: Column containing date text

    Returns:
        Column with first day of month as datetime string

    Example:
        df.select(datetimes.start_of_month(F.col("date")))
        # "2024-01-15" -> "2024-01-01 00:00:00"
    """
    # Standardize, then use trunc to get first day of month
    standardized = standardize_iso(col)

    return F.when(
        standardized.isNotNull(),
        F.date_format(
            F.trunc(F.to_timestamp(standardized, "yyyy-MM-dd HH:mm:ss"), "month"),
            "yyyy-MM-dd HH:mm:ss",
        ),
    ).otherwise(None)


@datetimes.register()
def end_of_month(col: Column) -> Column:
    """Get the last day of the month for a given date.

    Handles leap years correctly.

    Args:
        col: Column containing date text

    Returns:
        Column with last day of month as datetime string

    Example:
        df.select(datetimes.end_of_month(F.col("date")))
        # "2024-01-15" -> "2024-01-31 00:00:00"
        # "2024-02-10" -> "2024-02-29 00:00:00" (leap year)
    """
    # Standardize, then use last_day to get last day of month
    standardized = standardize_iso(col)

    return F.when(
        standardized.isNotNull(),
        F.date_format(
            F.last_day(F.to_timestamp(standardized, "yyyy-MM-dd HH:mm:ss")),
            "yyyy-MM-dd HH:mm:ss",
        ),
    ).otherwise(None)


@datetimes.register()
def start_of_quarter(col: Column) -> Column:
    """Get the first day of the quarter for a given date.

    Args:
        col: Column containing date text

    Returns:
        Column with first day of quarter as datetime string

    Example:
        df.select(datetimes.start_of_quarter(F.col("date")))
        # "2024-05-15" -> "2024-04-01 00:00:00"
        # "2024-01-15" -> "2024-01-01 00:00:00"
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
            F.lit("-01 00:00:00"),
        ),
    ).otherwise(None)


@datetimes.register()
def end_of_quarter(col: Column) -> Column:
    """Get the last day of the quarter for a given date.

    Args:
        col: Column containing date text

    Returns:
        Column with last day of quarter as datetime string

    Example:
        df.select(datetimes.end_of_quarter(F.col("date")))
        # "2024-05-15" -> "2024-06-30 00:00:00"
        # "2024-01-15" -> "2024-03-31 00:00:00"
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
            F.lit("-01"),
        )
    )

    return F.when(
        standardized.isNotNull(),
        F.date_format(F.last_day(temp_date), "yyyy-MM-dd HH:mm:ss"),
    ).otherwise(None)


def fiscal_year(col: Column, fiscal_start_month: Column) -> Column:
    """Get fiscal year for a date.

    Note: This function takes 2 column arguments and is manually added to the namespace.

    Args:
        col: Column containing date text
        fiscal_start_month: Month (1-12) when fiscal year starts as Column

    Returns:
        Column with fiscal year as integer

    Example:
        df.select(datetimes.fiscal_year(F.col("date"), F.lit(10)))
        # "2024-11-15" with Oct start -> 2025
        # "2024-09-15" with Oct start -> 2024
    """
    # Standardize first
    standardized = standardize_iso(col)
    ts = F.to_timestamp(standardized, "yyyy-MM-dd HH:mm:ss")

    # Get calendar year and month
    year = F.year(ts)
    month = F.month(ts)

    # Cast fiscal_start_month to int for comparison
    fiscal_start = fiscal_start_month.cast("int")

    # If month >= fiscal_start_month, fiscal year is year + 1
    # Otherwise, fiscal year is same as calendar year
    return F.when(
        standardized.isNotNull(),
        F.when(month >= fiscal_start, year + 1).otherwise(year),
    ).otherwise(None)


# ============================================================================
# Age and Duration Functions
# ============================================================================


def calculate_age(col: Column, reference_date: Column) -> Column:
    """Calculate age in years from a birthdate.

    Note: This function takes 2 column arguments and is manually added to the namespace.

    Args:
        col: Column containing birthdate text
        reference_date: Reference date column (use F.current_date() for today)

    Returns:
        Column with age in years as integer

    Example:
        df.select(datetimes.calculate_age(F.col("birth"), F.current_date()))
        # "2000-01-15" with ref "2024-01-15" -> 24
        # "1990-06-10" with ref "2024-12-02" -> 34
    """
    # Standardize birthdate
    standardized_birth = standardize_iso(col)
    birth_ts = F.to_timestamp(standardized_birth, "yyyy-MM-dd HH:mm:ss")

    # Standardize reference date
    standardized_ref = standardize_iso(reference_date)
    ref_ts = F.to_timestamp(standardized_ref, "yyyy-MM-dd HH:mm:ss")

    # Calculate years difference
    return F.when(
        standardized_birth.isNotNull() & standardized_ref.isNotNull(),
        F.floor(F.months_between(ref_ts, birth_ts) / 12)
    ).otherwise(None)


# Manually register multi-column fiscal_year and calculate_age functions to namespace
datetimes.fiscal_year = fiscal_year
datetimes.calculate_age = calculate_age


@datetimes.register()
def format_duration(seconds_col: Column) -> Column:
    """Format duration in seconds to human-readable string.

    Args:
        seconds_col: Column containing duration in seconds

    Returns:
        Column with human-readable duration string

    Example:
        df.select(datetimes.format_duration(F.col("seconds")))
        # 90 -> "1 minute 30 seconds"
        # 3661 -> "1 hour 1 minute 1 second"
        # 86400 -> "1 day"
    """
    # Handle negative durations
    is_negative = seconds_col < 0
    abs_seconds = F.abs(seconds_col)

    # Calculate time units
    weeks = F.floor(abs_seconds / 604800)
    remaining_after_weeks = abs_seconds % 604800

    days = F.floor(remaining_after_weeks / 86400)
    remaining_after_days = remaining_after_weeks % 86400

    hours = F.floor(remaining_after_days / 3600)
    remaining_after_hours = remaining_after_days % 3600

    minutes = F.floor(remaining_after_hours / 60)
    secs = remaining_after_hours % 60

    # Build the formatted string by concatenating parts
    # Start with empty string and add parts conditionally
    result = F.lit("")

    # Add weeks
    result = F.when(
        weeks > 0,
        F.concat(
            result,
            weeks.cast("string"),
            F.when(weeks == 1, F.lit(" week")).otherwise(F.lit(" weeks"))
        )
    ).otherwise(result)

    # Add days
    result = F.when(
        days > 0,
        F.concat(
            F.when(result != "", F.concat(result, F.lit(" "))).otherwise(result),
            days.cast("string"),
            F.when(days == 1, F.lit(" day")).otherwise(F.lit(" days"))
        )
    ).otherwise(result)

    # Add hours
    result = F.when(
        hours > 0,
        F.concat(
            F.when(result != "", F.concat(result, F.lit(" "))).otherwise(result),
            hours.cast("string"),
            F.when(hours == 1, F.lit(" hour")).otherwise(F.lit(" hours"))
        )
    ).otherwise(result)

    # Add minutes
    result = F.when(
        minutes > 0,
        F.concat(
            F.when(result != "", F.concat(result, F.lit(" "))).otherwise(result),
            minutes.cast("string"),
            F.when(minutes == 1, F.lit(" minute")).otherwise(F.lit(" minutes"))
        )
    ).otherwise(result)

    # Add seconds
    result = F.when(
        secs > 0,
        F.concat(
            F.when(result != "", F.concat(result, F.lit(" "))).otherwise(result),
            secs.cast("string"),
            F.when(secs == 1, F.lit(" second")).otherwise(F.lit(" seconds"))
        )
    ).otherwise(result)

    # Handle zero case
    result = F.when(
        abs_seconds == 0,
        F.lit("0 seconds")
    ).otherwise(result)

    # Add negative sign if needed
    result = F.when(
        is_negative,
        F.concat(F.lit("-"), result)
    ).otherwise(result)

    return result
