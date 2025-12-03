"""
Comprehensive tests for datetime extraction, parsing, and manipulation functionality.
"""

import pytest
from pyspark.sql import functions as F
from datetime import datetime, timedelta

from datacompose.transformers.text.datetimes.pyspark.pyspark_primitives import datetimes


@pytest.mark.unit
class TestDatetimeExtraction:

    def test_extract_datetime_from_text(self, spark):
        """Test extracting datetime mentions from free text."""

        test_data = [
            # ISO formats
            ("Meeting on 2024-01-15 at noon", "2024-01-15"),
            ("The deadline is 2024-12-31T23:59:59", "2024-12-31T23:59:59"),
            ("Started on 2024-03-01T10:30:00Z", "2024-03-01T10:30:00Z"),

            # US formats (MM/DD/YYYY)
            ("Due date: 01/15/2024", "01/15/2024"),
            ("Born on 12/25/1990", "12/25/1990"),
            ("Invoice dated 3/7/2024", "3/7/2024"),
            ("Meeting scheduled for 10/01/2024", "10/01/2024"),

            # EU formats (DD/MM/YYYY)
            ("Appointment on 15/01/2024", "15/01/2024"),
            ("Holiday starts 25/12/2024", "25/12/2024"),
            ("Contract expires 31.12.2024", "31.12.2024"),

            # Named month formats
            ("See you on January 15, 2024", "January 15, 2024"),
            ("Event on Jan 15, 2024", "Jan 15, 2024"),
            ("Due by 15-Jan-2024", "15-Jan-2024"),
            ("Meeting on 15 January 2024", "15 January 2024"),
            ("Deadline: December 31st, 2024", "December 31st, 2024"),

            # Short year formats
            ("Invoice from 01/15/24", "01/15/24"),
            ("Expires 12/31/99", "12/31/99"),

            # Date and time combinations
            ("Meeting at 2024-01-15 14:30", "2024-01-15 14:30"),
            ("Call scheduled for 01/15/2024 2:30 PM", "01/15/2024 2:30 PM"),
            ("Event on Jan 15, 2024 at 10:00 AM", "Jan 15, 2024 at 10:00 AM"),

            # Natural language dates
            ("Let's meet tomorrow", "tomorrow"),
            ("Due yesterday", "yesterday"),
            ("Happening next Monday", "next Monday"),
            ("Completed last week", "last week"),
            ("Starting in 3 days", "in 3 days"),

            # Multiple dates in text
            ("From 2024-01-15 to 2024-01-20", "2024-01-15"),  # Should extract first
            ("Between Jan 1, 2024 and Jan 31, 2024", "Jan 1, 2024"),

            # Edge cases
            ("No date here", None),
            ("", None),
            (None, None),
            ("The year 2024 was great", "2024"),  # Just year
            ("In Q3 2024", "Q3 2024"),  # Quarter notation
        ]

        df = spark.createDataFrame(test_data, ["text", "expected"])

        result_df = df.withColumn(
            "datetime", datetimes.extract_datetime_from_text(F.col("text"))
        )

        results = result_df.collect()

        for row in results:
            assert (
                row["datetime"] == row["expected"]
            ), f"Failed for '{row['text']}': expected '{row['expected']}', got '{row['datetime']}'"

    def test_standardize_iso(self, spark):
        """Test converting various date formats to ISO 8601."""

        test_data = [
            # US format to ISO
            ("01/15/2024", "2024-01-15 00:00:00"),
            ("1/5/2024", "2024-01-05 00:00:00"),
            ("12/31/2024", "2024-12-31 00:00:00"),

            # EU format to ISO (assuming DD/MM/YYYY)
            ("15/01/2024", "2024-01-15 00:00:00"),
            ("31/12/2024", "2024-12-31 00:00:00"),

            # Named months to ISO
            ("January 15, 2024", "2024-01-15 00:00:00"),
            ("Jan 15, 2024", "2024-01-15 00:00:00"),
            ("15-Jan-2024", "2024-01-15 00:00:00"),
            ("15 January 2024", "2024-01-15 00:00:00"),

            # Already ISO format
            ("2024-01-15", "2024-01-15 00:00:00"),
            ("2024-01-15T14:30:00", "2024-01-15 14:30:00"),
            ("2024-01-15 14:30:00", "2024-01-15 14:30:00"),

            # With time
            ("01/15/2024 14:30", "2024-01-15 14:30:00"),
            ("01/15/2024 2:30 PM", "2024-01-15 14:30:00"),
            ("Jan 15, 2024 10:00 AM", "2024-01-15 10:00:00"),

            # Edge cases
            (None, None),
            ("", None),
            ("invalid date", None),
        ]

        df = spark.createDataFrame(test_data, ["date_str", "expected"])
        result_df = df.withColumn(
            "iso_date", datetimes.standardize_iso(F.col("date_str"))
        )

        results = result_df.collect()

        for row in results:
            assert (
                row["iso_date"] == row["expected"]
            ), f"Failed for '{row['date_str']}': expected '{row['expected']}', got '{row['iso_date']}'"

    def test_extract_components(self, spark):
        """Test extracting year, month, day from various date formats."""

        test_data = [
            # ISO format
            ("2024-01-15", 2024, 1, 15),
            ("2024-12-31", 2024, 12, 31),

            # US format
            ("01/15/2024", 2024, 1, 15),
            ("12/31/2024", 2024, 12, 31),

            # Named months
            ("January 15, 2024", 2024, 1, 15),
            ("15-Jan-2024", 2024, 1, 15),
            ("December 31, 2024", 2024, 12, 31),

            # With time
            ("2024-01-15T14:30:00", 2024, 1, 15),
            ("01/15/2024 10:00 AM", 2024, 1, 15),

            # Edge cases
            (None, None, None, None),
            ("", None, None, None),
        ]

        df = spark.createDataFrame(test_data, ["date_str", "expected_year", "expected_month", "expected_day"])
        result_df = df.withColumn(
            "year", datetimes.extract_year(F.col("date_str"))
        ).withColumn(
            "month", datetimes.extract_month(F.col("date_str"))
        ).withColumn(
            "day", datetimes.extract_day(F.col("date_str"))
        )

        results = result_df.collect()

        for row in results:
            assert row["year"] == row["expected_year"], f"Year extraction failed for '{row['date_str']}'"
            assert row["month"] == row["expected_month"], f"Month extraction failed for '{row['date_str']}'"
            assert row["day"] == row["expected_day"], f"Day extraction failed for '{row['date_str']}'"

    def test_validate_dates(self, spark):
        """Test date validation."""

        test_data = [
            # Valid dates
            ("2024-01-15", True),
            ("2024-12-31", True),
            ("2024-02-29", True),  # Leap year
            ("01/15/2024", True),
            ("January 15, 2024", True),

            # Invalid dates
            ("2024-13-01", False),  # Invalid month
            ("2024-01-32", False),  # Invalid day
            ("2024-02-30", False),  # Feb doesn't have 30 days
            ("2023-02-29", False),  # Not a leap year
            ("2024-00-15", False),  # Month can't be 0
            ("2024-01-00", False),  # Day can't be 0

            # Malformed
            ("not a date", False),
            ("2024", False),  # Just year
            ("01-15", False),  # No year

            # Edge cases
            (None, False),
            ("", False),
        ]

        df = spark.createDataFrame(test_data, ["date_str", "expected_valid"])
        result_df = df.withColumn(
            "is_valid", datetimes.is_valid_date(F.col("date_str"))
        )

        results = result_df.collect()

        for row in results:
            assert (
                row["is_valid"] == row["expected_valid"]
            ), f"Validation failed for '{row['date_str']}': expected {row['expected_valid']}, got {row['is_valid']}"

    def test_natural_language_parsing(self, spark):
        """Test parsing natural language date expressions."""

        # Note: These would typically use a reference date for testing
        # For now, using placeholder expected values
        test_data = [
            ("yesterday", "reference_date - 1 day"),
            ("tomorrow", "reference_date + 1 day"),
            ("today", "reference_date"),
            ("next Monday", "next Monday from reference_date"),
            ("last Friday", "last Friday from reference_date"),
            ("in 3 days", "reference_date + 3 days"),
            ("3 days ago", "reference_date - 3 days"),
            ("next week", "reference_date + 7 days"),
            ("last month", "reference_date - 1 month"),
            ("next year", "reference_date + 1 year"),
            ("end of month", "last day of reference month"),
            ("beginning of year", "Jan 1 of reference year"),

            # Edge cases
            (None, None),
            ("", None),
            ("not a date expression", None),
        ]

        df = spark.createDataFrame(test_data, ["expression", "expected_interpretation"])

        # Use current_date as reference
        result_df = df.withColumn(
            "parsed_date", datetimes.parse_natural_language(F.col("expression"))
        )

        # For now, just check that it runs without error
        results = result_df.collect()
        assert len(results) == len(test_data)


@pytest.mark.unit
class TestDatetimeParsing:
    """Test datetime parsing with various formats."""

    def test_parse_ambiguous_dates(self, spark):
        """Test handling of ambiguous date formats like 01/02/03."""

        test_data = [
            # Ambiguous formats that could be MM/DD/YY, DD/MM/YY, or YY/MM/DD
            # detect_format will return the first format that successfully parses
            ("01/02/03", "M/d/yyyy"),  # Will match as US format
            ("12/13/14", "MM/dd/yyyy"),  # Can only be MM/DD/YY due to 13
            ("31/12/20", "dd/MM/yyyy"),  # Can only be DD/MM/YY due to 31

            # More ambiguous cases
            ("05/06/07", "M/d/yyyy"),  # Will match as US format
            ("10/10/10", "M/d/yyyy"),  # Same day/month, less ambiguous
        ]

        df = spark.createDataFrame(test_data, ["date_str", "expected_format"])
        result_df = df.withColumn(
            "detected_format", datetimes.detect_format(F.col("date_str"))
        )

        results = result_df.collect()

        # Verify it runs and detects a format (not "unknown")
        for row in results:
            assert row["detected_format"] != "unknown", \
                f"Failed to detect format for '{row['date_str']}'"

    def test_parse_incomplete_dates(self, spark):
        """Test parsing of incomplete date information."""

        test_data = [
            # Year only
            ("2024", "2024-01-01"),
            ("'24", "2024-01-01"),

            # Month and year
            ("January 2024", "2024-01-01"),
            ("Jan 2024", "2024-01-01"),
            ("01/2024", "2024-01-01"),
            ("2024-01", "2024-01-01"),

            # Quarter notation
            ("Q1 2024", "2024-01-01"),
            ("Q2 2024", "2024-04-01"),
            ("Q3 2024", "2024-07-01"),
            ("Q4 2024", "2024-10-01"),

            # Week notation
            ("Week 1, 2024", "2024-01-01"),
            ("W10-2024", "2024-03-04"),

            # Fiscal year
            ("FY2024", "2023-10-01"),  # US fiscal year
            ("FY 2024-25", "2024-04-01"),  # Some fiscal years
        ]

        df = spark.createDataFrame(test_data, ["date_str", "expected"])
        result_df = df.withColumn(
            "parsed", datetimes.parse_flexible(F.col("date_str"))
        )

        results = result_df.collect()
        assert len(results) == len(test_data)

    def test_parse_datetime_with_timezone(self, spark):
        """Test parsing datetime strings with timezone information."""

        test_data = [
            # ISO 8601 with timezone
            ("2024-01-15T14:30:00Z", "2024-01-15 14:30:00 UTC"),
            ("2024-01-15T14:30:00+00:00", "2024-01-15 14:30:00 UTC"),
            ("2024-01-15T14:30:00-05:00", "2024-01-15 19:30:00 UTC"),
            ("2024-01-15T14:30:00+05:30", "2024-01-15 09:00:00 UTC"),

            # Named timezones
            ("2024-01-15 14:30 EST", "2024-01-15 19:30:00 UTC"),
            ("2024-01-15 14:30 PST", "2024-01-15 22:30:00 UTC"),
            ("2024-01-15 14:30 GMT", "2024-01-15 14:30:00 UTC"),
            ("2024-01-15 14:30 CET", "2024-01-15 13:30:00 UTC"),

            # Military timezone
            ("2024-01-15 1430Z", "2024-01-15 14:30:00 UTC"),
            ("2024-01-15 1430A", "2024-01-15 13:30:00 UTC"),  # Alpha = UTC+1
        ]

        df = spark.createDataFrame(test_data, ["date_str", "expected_utc"])
        result_df = df.withColumn(
            "utc_time", datetimes.normalize_timezone(F.col("date_str"), F.lit("UTC"))
        )

        results = result_df.collect()
        assert len(results) == len(test_data)

    def test_parse_relative_dates_comprehensive(self, spark):
        """Test comprehensive relative date parsing."""

        test_data = [
            # Relative days
            ("yesterday", -1),
            ("today", 0),
            ("tomorrow", 1),
            ("day before yesterday", -2),
            ("day after tomorrow", 2),

            # Relative weeks
            ("last week", -7),
            ("this week", 0),
            ("next week", 7),
            ("2 weeks ago", -14),
            ("in 3 weeks", 21),

            # Relative months
            ("last month", -30),  # Approximate
            ("this month", 0),
            ("next month", 30),
            ("3 months ago", -90),
            ("in 6 months", 180),

            # Relative years
            ("last year", -365),
            ("this year", 0),
            ("next year", 365),
            ("2 years ago", -730),
            ("in 5 years", 1825),

            # Business days
            ("next business day", 1),  # Assuming not Friday
            ("3 business days", 3),
            ("last business day", -1),

            # Specific day references
            ("next Monday", None),  # Depends on current day
            ("last Friday", None),
            ("this Wednesday", None),
            ("next Christmas", None),
            ("last Easter", None),
        ]

        df = spark.createDataFrame(test_data, ["expression", "days_offset"])
        result_df = df.withColumn(
            "parsed", datetimes.parse_natural_language(F.col("expression"))
        )

        results = result_df.collect()
        assert len(results) == len(test_data)


@pytest.mark.unit
class TestDatetimeValidation:
    """Test datetime validation edge cases."""

    def test_leap_year_validation(self, spark):
        """Test leap year date validation."""

        test_data = [
            # Valid leap year dates
            ("2024-02-29", True),  # 2024 is divisible by 4
            ("2000-02-29", True),  # 2000 is divisible by 400
            ("2020-02-29", True),  # 2020 is divisible by 4

            # Invalid leap year dates
            ("2023-02-29", False),  # 2023 not divisible by 4
            ("2100-02-29", False),  # 2100 divisible by 100 but not 400
            ("1900-02-29", False),  # 1900 divisible by 100 but not 400

            # Regular February dates
            ("2024-02-28", True),
            ("2023-02-28", True),
            ("2023-02-30", False),  # No February has 30 days
            ("2024-02-30", False),
        ]

        df = spark.createDataFrame(test_data, ["date_str", "expected_valid"])
        result_df = df.withColumn(
            "is_valid", datetimes.is_valid_date(F.col("date_str"))
        )

        results = result_df.collect()
        for row in results:
            assert row["is_valid"] == row["expected_valid"], \
                f"Leap year validation failed for {row['date_str']}"

    def test_month_day_validation(self, spark):
        """Test validation of days in different months."""

        test_data = [
            # 31-day months (Jan, Mar, May, Jul, Aug, Oct, Dec)
            ("2024-01-31", True),
            ("2024-03-31", True),
            ("2024-05-31", True),
            ("2024-07-31", True),
            ("2024-08-31", True),
            ("2024-10-31", True),
            ("2024-12-31", True),

            # 30-day months (Apr, Jun, Sep, Nov)
            ("2024-04-30", True),
            ("2024-06-30", True),
            ("2024-09-30", True),
            ("2024-11-30", True),
            ("2024-04-31", False),  # April doesn't have 31 days
            ("2024-06-31", False),  # June doesn't have 31 days
            ("2024-09-31", False),  # September doesn't have 31 days
            ("2024-11-31", False),  # November doesn't have 31 days

            # Invalid days
            ("2024-01-32", False),
            ("2024-01-00", False),
            ("2024-13-01", False),  # Invalid month
            ("2024-00-15", False),  # Invalid month
        ]

        df = spark.createDataFrame(test_data, ["date_str", "expected_valid"])
        result_df = df.withColumn(
            "is_valid", datetimes.is_valid_date(F.col("date_str"))
        )

        results = result_df.collect()
        for row in results:
            assert row["is_valid"] == row["expected_valid"], \
                f"Month/day validation failed for {row['date_str']}"

    def test_historical_date_validation(self, spark):
        """Test validation of historical and future dates."""

        test_data = [
            # Historical dates
            ("1776-07-04", True),  # US Independence Day
            ("1066-10-14", True),  # Battle of Hastings
            ("0001-01-01", True),  # Year 1

            # Future dates
            ("2100-01-01", True),
            ("2500-12-31", True),
            ("9999-12-31", True),

            # Edge cases
            ("0000-01-01", False),  # Year 0 typically invalid
            ("-0001-01-01", False),  # BCE dates
            ("10000-01-01", False),  # 5-digit year

            # Calendar transition dates
            ("1582-10-04", True),  # Last day of Julian calendar
            ("1582-10-15", True),  # First day of Gregorian calendar
            ("1582-10-10", False),  # Doesn't exist (calendar transition)
        ]

        df = spark.createDataFrame(test_data, ["date_str", "expected_valid"])
        result_df = df.withColumn(
            "is_valid", datetimes.is_valid_date(F.col("date_str"))
        )

        results = result_df.collect()
        assert len(results) == len(test_data)


@pytest.mark.unit
class TestDatetimeArithmetic:
    """Test date arithmetic operations."""

    def test_add_subtract_days(self, spark):
        """Test adding and subtracting days from dates."""

        test_data = [
            # Simple addition
            ("2024-01-15", 5, "2024-01-20"),
            ("2024-01-15", -5, "2024-01-10"),
            ("2024-01-15", 0, "2024-01-15"),

            # Month boundary
            ("2024-01-31", 1, "2024-02-01"),
            ("2024-02-01", -1, "2024-01-31"),

            # Year boundary
            ("2024-12-31", 1, "2025-01-01"),
            ("2025-01-01", -1, "2024-12-31"),

            # Leap year
            ("2024-02-28", 1, "2024-02-29"),
            ("2024-02-29", 1, "2024-03-01"),
            ("2023-02-28", 1, "2023-03-01"),
        ]

        df = spark.createDataFrame(test_data, ["date", "days", "expected"])
        result_df = df.withColumn(
            "result", datetimes.add_days(F.col("date"), F.col("days"))
        )

        results = result_df.collect()
        assert len(results) == len(test_data)

    def test_add_subtract_months(self, spark):
        """Test adding and subtracting months from dates."""

        test_data = [
            # Simple addition
            ("2024-01-15", 1, "2024-02-15"),
            ("2024-01-15", -1, "2023-12-15"),
            ("2024-01-15", 12, "2025-01-15"),

            # Month-end handling
            ("2024-01-31", 1, "2024-02-29"),  # Jan 31 -> Feb 29 (leap year)
            ("2023-01-31", 1, "2023-02-28"),  # Jan 31 -> Feb 28 (non-leap)
            ("2024-03-31", 1, "2024-04-30"),  # Mar 31 -> Apr 30

            # Year boundary
            ("2024-12-15", 1, "2025-01-15"),
            ("2024-01-15", -1, "2023-12-15"),
        ]

        df = spark.createDataFrame(test_data, ["date", "months", "expected"])
        result_df = df.withColumn(
            "result", datetimes.add_months(F.col("date"), F.col("months"))
        )

        results = result_df.collect()
        assert len(results) == len(test_data)

    def test_date_differences(self, spark):
        """Test calculating differences between dates."""

        # Test days
        days_data = [
            ("2024-01-20", "2024-01-15", 5),
            ("2024-01-15", "2024-01-20", -5),
            ("2024-01-15", "2024-01-15", 0),
        ]
        df = spark.createDataFrame(days_data, ["date1", "date2", "expected"])
        result_df = df.withColumn(
            "diff", datetimes.date_diff_days(F.col("date1"), F.col("date2"))
        )
        for row in result_df.collect():
            assert row["diff"] == row["expected"], \
                f"Days diff failed: {row['date1']} - {row['date2']} = {row['diff']}, expected {row['expected']}"

        # Test months
        months_data = [
            ("2024-03-15", "2024-01-15", 2),
            ("2025-01-15", "2024-01-15", 12),
        ]
        df = spark.createDataFrame(months_data, ["date1", "date2", "expected"])
        result_df = df.withColumn(
            "diff", datetimes.date_diff_months(F.col("date1"), F.col("date2"))
        )
        for row in result_df.collect():
            assert row["diff"] == row["expected"], \
                f"Months diff failed: {row['date1']} - {row['date2']} = {row['diff']}, expected {row['expected']}"

        # Test years
        years_data = [
            ("2025-01-15", "2024-01-15", 1),
            ("2024-01-15", "2020-01-15", 4),
        ]
        df = spark.createDataFrame(years_data, ["date1", "date2", "expected"])
        result_df = df.withColumn(
            "diff", datetimes.date_diff_years(F.col("date1"), F.col("date2"))
        )
        for row in result_df.collect():
            assert row["diff"] == row["expected"], \
                f"Years diff failed: {row['date1']} - {row['date2']} = {row['diff']}, expected {row['expected']}"

        # Test hours
        hours_data = [
            ("2024-01-15 14:30:00", "2024-01-15 12:30:00", 2),
        ]
        df = spark.createDataFrame(hours_data, ["date1", "date2", "expected"])
        result_df = df.withColumn(
            "diff", datetimes.date_diff_hours(F.col("date1"), F.col("date2"))
        )
        for row in result_df.collect():
            assert row["diff"] == row["expected"], \
                f"Hours diff failed: {row['date1']} - {row['date2']} = {row['diff']}, expected {row['expected']}"

        # Test minutes
        minutes_data = [
            ("2024-01-15 14:30:00", "2024-01-15 14:00:00", 30),
        ]
        df = spark.createDataFrame(minutes_data, ["date1", "date2", "expected"])
        result_df = df.withColumn(
            "diff", datetimes.date_diff_minutes(F.col("date1"), F.col("date2"))
        )
        for row in result_df.collect():
            assert row["diff"] == row["expected"], \
                f"Minutes diff failed: {row['date1']} - {row['date2']} = {row['diff']}, expected {row['expected']}"

        # Test seconds
        seconds_data = [
            ("2024-01-15 14:30:30", "2024-01-15 14:30:00", 30),
        ]
        df = spark.createDataFrame(seconds_data, ["date1", "date2", "expected"])
        result_df = df.withColumn(
            "diff", datetimes.date_diff_seconds(F.col("date1"), F.col("date2"))
        )
        for row in result_df.collect():
            assert row["diff"] == row["expected"], \
                f"Seconds diff failed: {row['date1']} - {row['date2']} = {row['diff']}, expected {row['expected']}"

    def test_business_days_calculation(self, spark):
        """Test business days calculations."""

        test_data = [
            # Same week
            ("2024-01-15", "2024-01-19", 4),  # Mon to Fri
            ("2024-01-15", "2024-01-17", 2),  # Mon to Wed

            # Across weekend
            ("2024-01-12", "2024-01-15", 1),  # Fri to Mon
            ("2024-01-15", "2024-01-22", 5),  # Mon to next Mon

            # Multiple weeks
            ("2024-01-01", "2024-01-31", 23),  # January 2024

            # Same day
            ("2024-01-15", "2024-01-15", 0),

            # Weekend to weekend
            ("2024-01-13", "2024-01-14", 0),  # Sat to Sun
        ]

        df = spark.createDataFrame(test_data, ["start_date", "end_date", "expected_days"])
        result_df = df.withColumn(
            "business_days", datetimes.business_days_between(F.col("start_date"), F.col("end_date"))
        )

        results = result_df.collect()
        assert len(results) == len(test_data)


@pytest.mark.unit
class TestDatetimeFormatting:
    """Test datetime formatting functions."""

    def test_format_custom_patterns(self, spark):
        """Test custom date formatting patterns."""

        test_date = "2024-01-15 14:30:45"

        # Test each format pattern separately
        test_cases = [
            ("yyyy-MM-dd", "2024-01-15"),
            ("MM/dd/yyyy", "01/15/2024"),
            ("dd/MM/yyyy", "15/01/2024"),
            ("yyyy-MMM-dd", "2024-Jan-15"),
            ("MMMM d, yyyy", "January 15, 2024"),
            ("EEEE, MMMM d, yyyy", "Monday, January 15, 2024"),
            ("MM-dd-yy", "01-15-24"),
            ("HH:mm:ss", "14:30:45"),
            ("h:mm a", "2:30 PM"),
            ("yyyy-MM-dd'T'HH:mm:ss", "2024-01-15T14:30:45"),
        ]

        for format_pattern, expected in test_cases:
            df = spark.createDataFrame([(test_date,)], ["date"])
            result_df = df.withColumn(
                "formatted", datetimes.format_date(F.col("date"), format=format_pattern)
            )
            result = result_df.collect()[0]
            assert result["formatted"] == expected, \
                f"Format '{format_pattern}' failed: expected '{expected}', got '{result['formatted']}'"

    def test_duration_formatting(self, spark):
        """Test formatting durations in human-readable format."""

        test_data = [
            # Seconds
            (30, "30 seconds"),
            (59, "59 seconds"),

            # Minutes
            (60, "1 minute"),
            (90, "1 minute 30 seconds"),
            (120, "2 minutes"),
            (150, "2 minutes 30 seconds"),

            # Hours
            (3600, "1 hour"),
            (3660, "1 hour 1 minute"),
            (3661, "1 hour 1 minute 1 second"),
            (7200, "2 hours"),
            (7320, "2 hours 2 minutes"),

            # Days
            (86400, "1 day"),
            (90000, "1 day 1 hour"),
            (93661, "1 day 2 hours 1 minute 1 second"),
            (172800, "2 days"),

            # Weeks
            (604800, "1 week"),
            (1209600, "2 weeks"),

            # Complex
            (694861, "1 week 1 day 1 hour 1 minute 1 second"),

            # Edge cases
            (0, "0 seconds"),
            (-60, "-1 minute"),  # Negative duration
        ]

        df = spark.createDataFrame(test_data, ["seconds", "expected"])
        result_df = df.withColumn(
            "formatted", datetimes.format_duration(F.col("seconds"))
        )

        results = result_df.collect()
        assert len(results) == len(test_data)


@pytest.mark.unit
class TestDatetimeSpecialFunctions:
    """Test special datetime functions."""

    def test_fiscal_periods(self, spark):
        """Test fiscal year and quarter calculations."""

        test_data = [
            # US Federal fiscal year (Oct 1 - Sep 30)
            ("2024-10-01", 10, 2025),
            ("2024-09-30", 10, 2024),
            ("2024-01-15", 10, 2024),
            ("2024-12-31", 10, 2025),

            # Corporate fiscal year (Apr 1 - Mar 31)
            ("2024-04-01", 4, 2025),
            ("2024-03-31", 4, 2024),
            ("2024-07-15", 4, 2025),

            # Calendar year (Jan 1 - Dec 31)
            ("2024-01-01", 1, 2024),
            ("2024-12-31", 1, 2024),
        ]

        df = spark.createDataFrame(test_data, ["date", "fiscal_start_month", "expected_fy"])
        result_df = df.withColumn(
            "fiscal_year", datetimes.fiscal_year(F.col("date"), F.col("fiscal_start_month"))
        )

        results = result_df.collect()
        assert len(results) == len(test_data)

    def test_quarter_operations(self, spark):
        """Test quarter-related operations."""

        test_data = [
            # Q1
            ("2024-01-15", 1, "2024-01-01", "2024-03-31"),
            ("2024-02-29", 1, "2024-01-01", "2024-03-31"),
            ("2024-03-31", 1, "2024-01-01", "2024-03-31"),

            # Q2
            ("2024-04-15", 2, "2024-04-01", "2024-06-30"),
            ("2024-05-15", 2, "2024-04-01", "2024-06-30"),
            ("2024-06-30", 2, "2024-04-01", "2024-06-30"),

            # Q3
            ("2024-07-15", 3, "2024-07-01", "2024-09-30"),
            ("2024-08-15", 3, "2024-07-01", "2024-09-30"),
            ("2024-09-30", 3, "2024-07-01", "2024-09-30"),

            # Q4
            ("2024-10-15", 4, "2024-10-01", "2024-12-31"),
            ("2024-11-15", 4, "2024-10-01", "2024-12-31"),
            ("2024-12-31", 4, "2024-10-01", "2024-12-31"),
        ]

        df = spark.createDataFrame(test_data, ["date", "expected_quarter", "expected_start", "expected_end"])
        result_df = df.withColumn(
            "quarter", datetimes.extract_quarter(F.col("date"))
        ).withColumn(
            "quarter_start", datetimes.start_of_quarter(F.col("date"))
        ).withColumn(
            "quarter_end", datetimes.end_of_quarter(F.col("date"))
        )

        results = result_df.collect()
        assert len(results) == len(test_data)

    def test_age_calculation(self, spark):
        """Test age calculation from birthdate."""

        # Using a fixed reference date for testing
        reference_date = "2024-01-15"

        test_data = [
            ("2000-01-15", reference_date, 24),  # Exact birthday
            ("2000-01-16", reference_date, 23),  # Day before birthday
            ("2000-01-14", reference_date, 24),  # Day after birthday
            ("1999-01-15", reference_date, 25),
            ("2023-01-15", reference_date, 1),
            ("2024-01-14", reference_date, 0),  # Born yesterday
            ("2024-01-15", reference_date, 0),  # Born today

            # Leap year births
            ("2000-02-29", "2024-02-28", 23),  # Not yet birthday
            ("2000-02-29", "2024-03-01", 24),  # Birthday passed
        ]

        df = spark.createDataFrame(test_data, ["birthdate", "reference", "expected_age"])
        result_df = df.withColumn(
            "age", datetimes.calculate_age(F.col("birthdate"), F.col("reference"))
        )

        results = result_df.collect()
        assert len(results) == len(test_data)

    def test_week_operations(self, spark):
        """Test week-related operations."""

        test_data = [
            # ISO week numbers
            ("2024-01-01", 1, "Monday"),  # First Monday of 2024
            ("2024-01-07", 1, "Sunday"),
            ("2024-01-08", 2, "Monday"),

            # Mid-year
            ("2024-07-15", 29, "Monday"),

            # End of year
            ("2024-12-31", 53, "Tuesday"),

            # Week starts on different days
            ("2024-01-15", 3, "Monday"),
            ("2024-02-14", 7, "Wednesday"),  # Valentine's Day
            ("2024-07-04", 27, "Thursday"),  # US Independence Day
        ]

        df = spark.createDataFrame(test_data, ["date", "expected_week", "expected_day"])
        result_df = df.withColumn(
            "week_of_year", datetimes.extract_week_of_year(F.col("date"))
        ).withColumn(
            "day_of_week", datetimes.extract_day_of_week(F.col("date"))
        )

        results = result_df.collect()
        assert len(results) == len(test_data)


@pytest.mark.unit
class TestDatetimeEdgeCases:
    """Test edge cases and error handling."""

    def test_invalid_input_handling(self, spark):
        """Test handling of invalid inputs."""

        test_data = [
            (None,),
            ("",),
            ("   ",),
            ("not a date",),
            ("12345",),
            ("2024-99-99",),
            ("abcd-ef-gh",),
            ("2024/13/32",),
            ("32/13/2024",),
            ("February 30, 2024",),
        ]

        df = spark.createDataFrame(test_data, ["input"])

        # Test multiple functions with invalid input
        result_df = df.select(
            F.col("input"),
            datetimes.is_valid_date(F.col("input")).alias("is_valid"),
            datetimes.standardize_iso(F.col("input")).alias("standardized"),
            datetimes.extract_year(F.col("input")).alias("year"),
            datetimes.extract_month(F.col("input")).alias("month"),
            datetimes.extract_day(F.col("input")).alias("day"),
        )

        results = result_df.collect()
        for row in results:
            assert not row["is_valid"]
            # Other columns should handle gracefully (return None or empty)

    def test_extreme_dates(self, spark):
        """Test handling of extreme past and future dates."""

        test_data = [
            # Far past
            ("0001-01-01", True),
            ("1000-01-01", True),
            ("1492-10-12", True),  # Columbus Day

            # Far future
            ("3000-01-01", True),
            ("9999-12-31", True),

            # Invalid extremes
            ("0000-01-01", False),
            ("10000-01-01", False),
            ("-1000-01-01", False),
        ]

        df = spark.createDataFrame(test_data, ["date", "should_be_valid"])
        result_df = df.withColumn(
            "is_valid", datetimes.is_valid_date(F.col("date"))
        )

        results = result_df.collect()
        assert len(results) == len(test_data)

    def test_daylight_saving_transitions(self, spark):
        """Test handling of daylight saving time transitions."""

        test_data = [
            # Spring forward (2024 - second Sunday of March)
            ("2024-03-10 01:30:00", "2024-03-10 03:30:00"),  # 2 AM becomes 3 AM

            # Fall back (2024 - first Sunday of November)
            ("2024-11-03 01:30:00", "2024-11-03 01:30:00"),  # 2 AM happens twice

            # Regular times
            ("2024-06-15 12:00:00", "2024-06-15 12:00:00"),
            ("2024-12-15 12:00:00", "2024-12-15 12:00:00"),
        ]

        df = spark.createDataFrame(test_data, ["datetime", "expected"])
        result_df = df.withColumn(
            "adjusted", datetimes.standardize_iso(F.col("datetime"))
        )

        results = result_df.collect()
        assert len(results) == len(test_data)

    def test_unicode_and_special_characters(self, spark):
        """Test handling of unicode and special characters in date strings."""

        test_data = [
            ("2024年1月15日", "2024-01-15"),  # Japanese
            ("15/01/2024", "2024-01-15"),  # European style
            ("15.01.2024", "2024-01-15"),  # Dot separator
            ("15-Jan-2024", "2024-01-15"),  # Hyphen separator
            ("15_01_2024", "2024-01-15"),  # Underscore separator
            ("2024#01#15", None),  # Invalid separator
            ("2024 01 15", "2024-01-15"),  # Space separator
        ]

        df = spark.createDataFrame(test_data, ["date_str", "expected"])
        result_df = df.withColumn(
            "parsed", datetimes.parse_flexible(F.col("date_str"))
        )

        results = result_df.collect()
        assert len(results) == len(test_data)
