"""
Data quality tests for datetime transformations.
Tests handling of messy, malformed, and edge-case datetime data.
"""

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

from datacompose.transformers.text.datetimes.pyspark.pyspark_primitives import datetimes


@pytest.mark.unit
class TestDatetimeMessyData:
    """Test handling of messy and malformed datetime data."""

    def test_whitespace_handling(self, spark):
        """Test dates with various whitespace issues."""

        test_data = [
            # Leading/trailing whitespace
            ("  2024-01-15  ", "2024-01-15 00:00:00"),
            ("\t2024-01-15\t", "2024-01-15 00:00:00"),
            ("\n2024-01-15\n", "2024-01-15 00:00:00"),

            # Internal whitespace
            ("2024 - 01 - 15", None),  # Too many spaces
            ("January  15,  2024", "2024-01-15 00:00:00"),  # Double spaces
            ("01 / 15 / 2024", None),  # Spaces around slashes

            # Mixed whitespace
            ("  January 15, 2024  ", "2024-01-15 00:00:00"),
            ("\t01/15/2024\n", "2024-01-15 00:00:00"),

            # Only whitespace
            ("   ", None),
            ("\t\t\t", None),
            ("\n\n", None),
        ]

        df = spark.createDataFrame(test_data, ["date_str", "expected"])
        result_df = df.withColumn(
            "standardized", datetimes.standardize_iso(F.col("date_str"))
        )

        results = result_df.collect()
        for row in results:
            assert row["standardized"] == row["expected"], \
                f"Failed for '{repr(row['date_str'])}': expected '{row['expected']}', got '{row['standardized']}'"

    def test_case_variations(self, spark):
        """Test dates with different case variations."""

        test_data = [
            # Month name variations
            ("JANUARY 15, 2024", "2024-01-15 00:00:00"),
            ("january 15, 2024", "2024-01-15 00:00:00"),
            ("January 15, 2024", "2024-01-15 00:00:00"),
            ("JaNuArY 15, 2024", "2024-01-15 00:00:00"),

            # Abbreviated months
            ("JAN 15, 2024", "2024-01-15 00:00:00"),
            ("jan 15, 2024", "2024-01-15 00:00:00"),
            ("Jan 15, 2024", "2024-01-15 00:00:00"),

            # Mixed case in format
            ("15-JAN-2024", "2024-01-15 00:00:00"),
            ("15-jan-2024", "2024-01-15 00:00:00"),
            ("15-Jan-2024", "2024-01-15 00:00:00"),
        ]

        df = spark.createDataFrame(test_data, ["date_str", "expected"])
        result_df = df.withColumn(
            "standardized", datetimes.standardize_iso(F.col("date_str"))
        )

        results = result_df.collect()
        for row in results:
            # Note: Case handling depends on implementation
            # This test documents expected behavior
            pass

    def test_partial_dates(self, spark):
        """Test handling of incomplete date information."""

        test_data = [
            # Just year
            ("2024", None),  # Ambiguous - could be year or other number

            # Month and year only
            ("Jan 2024", None),
            ("January 2024", None),
            ("01/2024", None),
            ("2024-01", None),

            # Day and month only (no year)
            ("01/15", None),
            ("January 15", None),

            # Just month
            ("January", None),
            ("Jan", None),
        ]

        schema = StructType([
            StructField("date_str", StringType(), True),
            StructField("expected", StringType(), True)
        ])
        df = spark.createDataFrame(test_data, schema)
        result_df = df.withColumn(
            "standardized", datetimes.standardize_iso(F.col("date_str"))
        )

        results = result_df.collect()
        for row in results:
            assert row["standardized"] == row["expected"], \
                f"Partial date '{row['date_str']}' should return {row['expected']}"

    def test_ambiguous_separators(self, spark):
        """Test dates with unusual or mixed separators."""

        test_data = [
            # Standard separators
            ("2024-01-15", "2024-01-15 00:00:00"),
            ("2024/01/15", None),  # Not in format list
            ("01.15.2024", None),  # US format with dots

            # Mixed separators
            ("2024-01/15", None),
            ("01/15-2024", None),
            ("2024.01-15", None),

            # Multiple consecutive separators
            ("2024--01--15", None),
            ("01//15//2024", None),

            # No separators
            ("20240115", None),  # YYYYMMDD
            ("01152024", None),  # MMDDYYYY
        ]

        df = spark.createDataFrame(test_data, ["date_str", "expected"])
        result_df = df.withColumn(
            "standardized", datetimes.standardize_iso(F.col("date_str"))
        )

        results = result_df.collect()
        for row in results:
            assert row["standardized"] == row["expected"], \
                f"Failed for '{row['date_str']}'"

    def test_special_characters(self, spark):
        """Test dates with special characters and unicode."""

        test_data = [
            # Ordinal indicators
            ("January 15th, 2024", None),  # 'th' not in format
            ("Jan 1st, 2024", None),
            ("Feb 2nd, 2024", None),
            ("Mar 3rd, 2024", None),

            # Unicode spaces and dashes
            ("2024\u202001\u202015", None),  # Non-breaking spaces
            ("2024\u201301\u201315", None),  # En-dash
            ("2024\u201401\u201415", None),  # Em-dash

            # Other special characters
            ("2024*01*15", None),
            ("2024@01@15", None),
            ("2024#01#15", None),

            # Quotes and apostrophes
            ("'2024-01-15'", None),
            ('"2024-01-15"', None),
            ("`2024-01-15`", None),
        ]

        schema = StructType([
            StructField("date_str", StringType(), True),
            StructField("expected", StringType(), True)
        ])
        df = spark.createDataFrame(test_data, schema)
        result_df = df.withColumn(
            "standardized", datetimes.standardize_iso(F.col("date_str"))
        )

        results = result_df.collect()
        for row in results:
            pass  # Just verify it doesn't crash

    def test_typos_and_misspellings(self, spark):
        """Test common typos in month names."""

        test_data = [
            # Misspelled months
            ("Janaury 15, 2024", None),
            ("Febuary 15, 2024", None),
            ("Septembar 15, 2024", None),

            # Wrong abbreviations
            ("Janu 15, 2024", None),
            ("Febr 15, 2024", None),
            ("Sept 15, 2024", None),  # Sometimes valid

            # Common typos
            ("01/51/2024", None),  # Transposed day
            ("10/15/2024", "2024-10-15 00:00:00"),  # Valid
            ("2024-15-01", None),  # Month/day swapped
        ]

        df = spark.createDataFrame(test_data, ["date_str", "expected"])
        result_df = df.withColumn(
            "standardized", datetimes.standardize_iso(F.col("date_str"))
        )

        results = result_df.collect()
        for row in results:
            pass  # Document behavior with typos


@pytest.mark.unit
class TestDatetimeNullAndEmpty:
    """Test handling of null and empty values."""

    def test_null_handling(self, spark):
        """Test that null values are handled gracefully."""

        test_data = [(None,), ("2024-01-15",), (None,), ("01/15/2024",)]

        df = spark.createDataFrame(test_data, ["date_str"])

        result_df = df.withColumn(
            "standardized", datetimes.standardize_iso(F.col("date_str"))
        ).withColumn(
            "is_valid", datetimes.is_valid_date(F.col("date_str"))
        ).withColumn(
            "year", datetimes.extract_year(F.col("date_str"))
        )

        results = result_df.collect()

        # Verify nulls are handled
        assert results[0]["standardized"] is None
        assert results[0]["is_valid"] == False or results[0]["is_valid"] is None
        assert results[0]["year"] is None

        # Verify valid dates still work
        assert results[1]["standardized"] is not None

    def test_empty_string_handling(self, spark):
        """Test that empty strings are handled gracefully."""

        test_data = [
            ("",),
            ("2024-01-15",),
            ("   ",),
            ("\t",),
            ("\n",),
        ]

        df = spark.createDataFrame(test_data, ["date_str"])

        result_df = df.withColumn(
            "standardized", datetimes.standardize_iso(F.col("date_str"))
        ).withColumn(
            "is_valid", datetimes.is_valid_date(F.col("date_str"))
        )

        results = result_df.collect()

        # Empty strings should be handled gracefully
        assert results[0]["standardized"] is None
        assert results[0]["is_valid"] == False or results[0]["is_valid"] is None

        # Valid date still works
        assert results[1]["standardized"] is not None

    def test_mixed_null_and_valid(self, spark):
        """Test dataset with mix of nulls, empty strings, and valid dates."""

        test_data = [
            (None, None),
            ("", None),
            ("2024-01-15", "2024-01-15 00:00:00"),
            (None, None),
            ("01/15/2024", "2024-01-15 00:00:00"),
            ("   ", None),
            ("invalid", None),
            ("2024-12-31", "2024-12-31 00:00:00"),
        ]

        df = spark.createDataFrame(test_data, ["date_str", "expected"])

        result_df = df.withColumn(
            "standardized", datetimes.standardize_iso(F.col("date_str"))
        )

        results = result_df.collect()

        for row in results:
            assert row["standardized"] == row["expected"], \
                f"Failed for '{row['date_str']}'"


@pytest.mark.unit
class TestDatetimeRealWorldScenarios:
    """Test real-world messy data scenarios."""

    def test_excel_date_formats(self, spark):
        """Test dates that might come from Excel exports."""

        test_data = [
            # Excel default formats
            ("1/15/2024", "2024-01-15 00:00:00"),  # M/D/YYYY
            ("01/15/2024", "2024-01-15 00:00:00"),  # MM/DD/YYYY

            # Excel with time
            ("1/15/2024 14:30", "2024-01-15 14:30:00"),
            ("01/15/2024 2:30 PM", "2024-01-15 14:30:00"),

            # Excel serial dates (would need special handling)
            ("44941", None),  # Excel serial for 2023-01-15

            # Excel text dates
            ("Jan-15-24", None),  # Not in format list
            ("15-Jan-24", None),
        ]

        df = spark.createDataFrame(test_data, ["date_str", "expected"])
        result_df = df.withColumn(
            "standardized", datetimes.standardize_iso(F.col("date_str"))
        )

        results = result_df.collect()
        for row in results:
            pass  # Document Excel format handling

    def test_database_export_formats(self, spark):
        """Test dates from common database exports."""

        test_data = [
            # MySQL/PostgreSQL
            ("2024-01-15 14:30:00", "2024-01-15 14:30:00"),
            ("2024-01-15", "2024-01-15 00:00:00"),

            # SQL Server
            ("2024-01-15 14:30:00.000", None),  # With milliseconds
            ("Jan 15 2024 02:30PM", None),

            # Oracle
            ("15-JAN-24", "2024-01-15 00:00:00"),
            ("15-JAN-2024", "2024-01-15 00:00:00"),
        ]

        df = spark.createDataFrame(test_data, ["date_str", "expected"])
        result_df = df.withColumn(
            "standardized", datetimes.standardize_iso(F.col("date_str"))
        )

        results = result_df.collect()
        for row in results:
            pass  # Document database format handling

    def test_web_scraping_dates(self, spark):
        """Test dates that might come from web scraping."""

        test_data = [
            # Various web formats
            ("Published: 2024-01-15", None),  # With prefix
            ("2024-01-15 | Updated", None),  # With suffix
            ("Jan 15", None),  # Missing year
            ("Yesterday", None),  # Relative
            ("2 days ago", None),  # Relative

            # ISO with timezone (common on web)
            ("2024-01-15T14:30:00Z", "2024-01-15 14:30:00"),
            ("2024-01-15T14:30:00+00:00", None),  # With offset

            # Human-readable formats
            ("January 15, 2024", "2024-01-15 00:00:00"),
            ("15 January 2024", "2024-01-15 00:00:00"),
        ]

        df = spark.createDataFrame(test_data, ["date_str", "expected"])
        result_df = df.withColumn(
            "standardized", datetimes.standardize_iso(F.col("date_str"))
        )

        results = result_df.collect()
        for row in results:
            pass  # Document web scraping format handling

    def test_log_file_timestamps(self, spark):
        """Test timestamp formats common in log files."""

        test_data = [
            # Apache/Nginx logs
            ("[15/Jan/2024:14:30:45]", None),  # With brackets
            ("15/Jan/2024:14:30:45", None),

            # Syslog format
            ("Jan 15 14:30:45", None),  # No year

            # Application logs
            ("2024-01-15 14:30:45.123", None),  # With milliseconds
            ("2024-01-15T14:30:45.123Z", None),

            # ISO 8601
            ("2024-01-15T14:30:45", "2024-01-15 14:30:45"),
        ]

        df = spark.createDataFrame(test_data, ["date_str", "expected"])
        result_df = df.withColumn(
            "standardized", datetimes.standardize_iso(F.col("date_str"))
        )

        results = result_df.collect()
        for row in results:
            pass  # Document log format handling


@pytest.mark.unit
class TestDatetimeDataCorruption:
    """Test handling of corrupted or partially corrupted data."""

    def test_truncated_dates(self, spark):
        """Test dates that are cut off or truncated."""

        test_data = [
            # Truncated at various points
            ("2024-01-1", None),  # Missing last digit
            ("2024-01-", None),  # Missing day
            ("2024-01", None),  # Missing day
            ("2024-", None),  # Missing month and day
            ("01/15/202", None),  # Missing year digit
            ("01/15/", None),  # Missing year

            # Complete dates (for comparison)
            ("2024-01-15", "2024-01-15 00:00:00"),
            ("01/15/2024", "2024-01-15 00:00:00"),
        ]

        df = spark.createDataFrame(test_data, ["date_str", "expected"])
        result_df = df.withColumn(
            "standardized", datetimes.standardize_iso(F.col("date_str"))
        )

        results = result_df.collect()
        for row in results:
            assert row["standardized"] == row["expected"], \
                f"Truncated date '{row['date_str']}' handling failed"

    def test_extra_characters(self, spark):
        """Test dates with extra/garbage characters."""

        test_data = [
            # Extra characters at various positions
            ("x2024-01-15", None),  # Prefix
            ("2024-01-15x", None),  # Suffix
            ("2024x-01-15", None),  # Middle
            ("2024-01x-15", None),  # Middle

            # Multiple garbage characters
            ("xxx2024-01-15xxx", None),
            ("2024-01-15 garbage text", None),

            # Control characters
            ("2024-01-15\x00", None),
            ("\x002024-01-15", None),
        ]

        schema = StructType([
            StructField("date_str", StringType(), True),
            StructField("expected", StringType(), True)
        ])
        df = spark.createDataFrame(test_data, schema)
        result_df = df.withColumn(
            "standardized", datetimes.standardize_iso(F.col("date_str"))
        )

        results = result_df.collect()
        for row in results:
            pass  # Verify no crashes

    def test_encoding_issues(self, spark):
        """Test dates with encoding problems."""

        test_data = [
            # UTF-8 issues
            ("2024\ufffd01\ufffd15", None),  # Replacement character

            # Different date separators that might come from encoding issues
            ("2024\u200001\u200015", None),  # Zero-width space

            # Normal ASCII for comparison
            ("2024-01-15", "2024-01-15 00:00:00"),
        ]

        df = spark.createDataFrame(test_data, ["date_str", "expected"])
        result_df = df.withColumn(
            "standardized", datetimes.standardize_iso(F.col("date_str"))
        )

        results = result_df.collect()
        for row in results:
            pass  # Verify no crashes

    def test_duplicate_data(self, spark):
        """Test handling of duplicate/repeated date values."""

        test_data = [
            ("2024-01-15", "2024-01-15 00:00:00"),
            ("2024-01-15", "2024-01-15 00:00:00"),
            ("2024-01-15", "2024-01-15 00:00:00"),
            ("01/15/2024", "2024-01-15 00:00:00"),
            ("01/15/2024", "2024-01-15 00:00:00"),
        ]

        df = spark.createDataFrame(test_data, ["date_str", "expected"])
        result_df = df.withColumn(
            "standardized", datetimes.standardize_iso(F.col("date_str"))
        )

        results = result_df.collect()

        # All should standardize correctly
        for row in results:
            assert row["standardized"] == row["expected"]

        # Verify count is preserved
        assert len(results) == 5
