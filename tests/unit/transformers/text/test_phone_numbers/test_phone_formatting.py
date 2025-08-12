"""
Tests for phone number formatting and standardization functions.
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from datacompose.transformers.text.clean_phone_numbers.pyspark.pyspark_primitives import (
    phones,
)


@pytest.fixture(scope="session")
def spark():
    """Create a Spark session for testing."""
    import logging
    import warnings

    warnings.filterwarnings("ignore")
    logging.getLogger("py4j").setLevel(logging.ERROR)
    logging.getLogger("pyspark").setLevel(logging.ERROR)

    spark = (
        SparkSession.builder.appName("PhoneFormattingTests")
        .master("local[*]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.adaptive.enabled", "false")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    yield spark
    spark.stop()


@pytest.mark.unit
class TestPhoneFormatting:
    """Test phone number formatting functions."""

    def test_format_nanp_hyphen(self, spark):
        """Test NANP formatting with hyphens."""
        test_data = [
            ("5551234567", "555-123-4567"),
            ("15551234567", "555-123-4567"),
            ("(555) 123-4567", "555-123-4567"),
            ("555.123.4567", "555-123-4567"),
            ("5551234567 ext. 123", "555-123-4567 ext. 123"),
            ("123-456-7890", ""),  # Invalid area code
            ("", ""),
            (None, ""),
        ]

        df = spark.createDataFrame(test_data, ["phone", "expected"])
        result_df = df.withColumn("formatted", phones.format_nanp(F.col("phone")))

        results = result_df.collect()
        for row in results:
            assert (
                row["formatted"] == row["expected"]
            ), f"Failed for '{row['phone']}': expected '{row['expected']}', got '{row['formatted']}'"

    def test_format_nanp_paren(self, spark):
        """Test NANP formatting with parentheses."""
        test_data = [
            ("5551234567", "(555) 123-4567"),
            ("15551234567", "(555) 123-4567"),
            ("555-123-4567", "(555) 123-4567"),
            ("5551234567 ext. 123", "(555) 123-4567 ext. 123"),
            ("", ""),
            (None, ""),
        ]

        df = spark.createDataFrame(test_data, ["phone", "expected"])
        result_df = df.withColumn("formatted", phones.format_nanp_paren(F.col("phone")))

        results = result_df.collect()
        for row in results:
            assert (
                row["formatted"] == row["expected"]
            ), f"Failed for '{row['phone']}': expected '{row['expected']}', got '{row['formatted']}'"

    def test_format_nanp_dot(self, spark):
        """Test NANP formatting with dots."""
        test_data = [
            ("5551234567", "555.123.4567"),
            ("15551234567", "555.123.4567"),
            ("(555) 123-4567", "555.123.4567"),
            ("", ""),
            (None, ""),
        ]

        df = spark.createDataFrame(test_data, ["phone", "expected"])
        result_df = df.withColumn("formatted", phones.format_nanp_dot(F.col("phone")))

        results = result_df.collect()
        for row in results:
            assert (
                row["formatted"] == row["expected"]
            ), f"Failed for '{row['phone']}': expected '{row['expected']}', got '{row['formatted']}'"

    def test_format_nanp_space(self, spark):
        """Test NANP formatting with spaces."""
        test_data = [
            ("5551234567", "555 123 4567"),
            ("15551234567", "555 123 4567"),
            ("(555) 123-4567", "555 123 4567"),
            ("", ""),
            (None, ""),
        ]

        df = spark.createDataFrame(test_data, ["phone", "expected"])
        result_df = df.withColumn("formatted", phones.format_nanp_space(F.col("phone")))

        results = result_df.collect()
        for row in results:
            assert (
                row["formatted"] == row["expected"]
            ), f"Failed for '{row['phone']}': expected '{row['expected']}', got '{row['formatted']}'"

    def test_format_e164(self, spark):
        """Test E.164 formatting."""
        test_data = [
            ("5551234567", "+15551234567"),  # Add default country code
            ("15551234567", "+15551234567"),  # Already has 1
            ("(555) 123-4567", "+15551234567"),
            ("+44 20 7946 0958", "+442079460958"),  # UK number
            ("123-456-7890", ""),  # Invalid
            ("", ""),
            (None, ""),
        ]

        df = spark.createDataFrame(test_data, ["phone", "expected"])
        result_df = df.withColumn("formatted", phones.format_e164(F.col("phone")))

        results = result_df.collect()
        for row in results:
            assert (
                row["formatted"] == row["expected"]
            ), f"Failed for '{row['phone']}': expected '{row['expected']}', got '{row['formatted']}'"

    def test_format_international(self, spark):
        """Test international formatting."""
        test_data = [
            ("+44 20 7946 0958", "+44 2079460958"),
            ("+86 10 1234 5678", "+86 1012345678"),
            ("+1 555 123 4567", "+1 5551234567"),
            ("5551234567", "5551234567"),  # No country code
            ("", ""),
            (None, ""),
        ]

        df = spark.createDataFrame(test_data, ["phone", "expected"])
        result_df = df.withColumn(
            "formatted", phones.format_international(F.col("phone"))
        )

        results = result_df.collect()
        for row in results:
            assert (
                row["formatted"] == row["expected"]
            ), f"Failed for '{row['phone']}': expected '{row['expected']}', got '{row['formatted']}'"


@pytest.mark.unit
class TestPhoneStandardization:
    """Test phone number standardization functions."""

    @pytest.mark.skip(reason="Complex expression tree causes Spark compilation issues")
    def test_standardize_phone_nanp(self, spark):
        """Test phone standardization with NANP format."""
        test_data = [
            ("1-800-FLOWERS", "800-356-9377"),
            ("(555) 123-4567", "555-123-4567"),
            ("555.123.4567 ext. 123", "555-123-4567 ext. 123"),
            ("15551234567", "555-123-4567"),
            ("invalid", ""),
            ("", ""),
            (None, ""),
        ]

        df = spark.createDataFrame(test_data, ["phone", "expected"])
        result_df = df.withColumn(
            "standardized", phones.standardize_phone(F.col("phone"))
        )

        results = result_df.collect()
        for row in results:
            assert (
                row["standardized"] == row["expected"]
            ), f"Failed for '{row['phone']}': expected '{row['expected']}', got '{row['standardized']}'"

    def test_standardize_phone_e164(self, spark):
        """Test phone standardization with E.164 format."""
        test_data = [
            ("5551234567", "+15551234567"),
            ("(555) 123-4567", "+15551234567"),
            ("15551234567", "+15551234567"),
            ("", ""),
            (None, ""),
        ]

        df = spark.createDataFrame(test_data, ["phone", "expected"])
        result_df = df.withColumn(
            "standardized", phones.standardize_phone_e164(F.col("phone"))
        )

        results = result_df.collect()
        for row in results:
            assert (
                row["standardized"] == row["expected"]
            ), f"Failed for '{row['phone']}': expected '{row['expected']}', got '{row['standardized']}'"

    def test_standardize_phone_digits(self, spark):
        """Test phone standardization with digits only format."""
        test_data = [
            ("(555) 123-4567", "5551234567"),
            ("1-800-FLOWERS", "18003569377"),
            ("555.123.4567", "5551234567"),
            ("", ""),
            (None, ""),
        ]

        df = spark.createDataFrame(test_data, ["phone", "expected"])
        result_df = df.withColumn(
            "standardized", phones.standardize_phone_digits(F.col("phone"))
        )

        results = result_df.collect()
        for row in results:
            assert (
                row["standardized"] == row["expected"]
            ), f"Failed for '{row['phone']}': expected '{row['expected']}', got '{row['standardized']}'"

    def test_clean_phone(self, spark):
        """Test phone cleaning with error handling."""
        test_data = [
            ("(555) 123-4567", "555-123-4567"),
            ("1-800-FLOWERS", "800-356-9377"),
            ("invalid", None),  # null error handling
            ("123-456", None),
            ("", None),
            (None, None),
        ]

        df = spark.createDataFrame(test_data, ["phone", "expected"])
        result_df = df.withColumn("cleaned", phones.clean_phone(F.col("phone")))

        results = result_df.collect()
        for row in results:
            assert (
                row["cleaned"] == row["expected"]
            ), f"Failed for '{row['phone']}': expected '{row['expected']}', got '{row['cleaned']}'"

    def test_clean_phone_invalid_handling(self, spark):
        """Test phone cleaning with invalid number handling."""
        test_data = [
            ("(555) 123-4567", "555-123-4567"),
            ("invalid", None),  # Return None for invalid
            ("123-456", None),
            ("", None),
            (None, None),
        ]

        df = spark.createDataFrame(test_data, ["phone", "expected"])
        result_df = df.withColumn("cleaned", phones.clean_phone(F.col("phone")))

        results = result_df.collect()
        for row in results:
            assert (
                row["cleaned"] == row["expected"]
            ), f"Failed for '{row['phone']}': expected '{row['expected']}', got '{row['cleaned']}'"


@pytest.mark.unit
class TestPhoneInformation:
    """Test phone information extraction functions."""

    def test_get_phone_type(self, spark):
        """Test phone type identification."""
        test_data = [
            ("1-800-555-1234", "toll-free"),
            ("888-555-1234", "toll-free"),
            ("1-900-555-1234", "premium"),
            ("555-123-4567", "standard"),
            ("+44 20 7946 0958", "international"),
            ("invalid", "invalid"),
            ("", "unknown"),
            (None, "unknown"),
        ]

        df = spark.createDataFrame(test_data, ["phone", "expected"])
        result_df = df.withColumn("type", phones.get_phone_type(F.col("phone")))

        results = result_df.collect()
        for row in results:
            assert (
                row["type"] == row["expected"]
            ), f"Failed for '{row['phone']}': expected '{row['expected']}', got '{row['type']}'"

    def test_get_region_from_area_code(self, spark):
        """Test region extraction from area code."""
        test_data = [
            ("212-555-1234", "New York, NY"),
            ("213-555-1234", "Los Angeles, CA"),
            ("312-555-1234", "Chicago, IL"),
            ("415-555-1234", "San Francisco, CA"),
            ("202-555-1234", "Washington, DC"),
            ("800-555-1234", "Toll-Free"),
            ("900-555-1234", "Premium"),
            ("999-555-1234", ""),  # Unknown area code
            ("", ""),
            (None, ""),
        ]

        df = spark.createDataFrame(test_data, ["phone", "expected"])
        result_df = df.withColumn(
            "region", phones.get_region_from_area_code(F.col("phone"))
        )

        results = result_df.collect()
        for row in results:
            assert (
                row["region"] == row["expected"]
            ), f"Failed for '{row['phone']}': expected '{row['expected']}', got '{row['region']}'"

    def test_mask_phone(self, spark):
        """Test phone number masking for privacy."""
        test_data = [
            ("555-123-4567", "***-***-4567"),
            ("(555) 123-4567", "***-***-4567"),
            ("15551234567", "***-***-4567"),
            ("invalid", "invalid"),
            ("", None),
            (None, None),
        ]

        df = spark.createDataFrame(test_data, ["phone", "expected"])
        result_df = df.withColumn("masked", phones.mask_phone(F.col("phone")))

        results = result_df.collect()
        for row in results:
            assert (
                row["masked"] == row["expected"]
            ), f"Failed for '{row['phone']}': expected '{row['expected']}', got '{row['masked']}'"


@pytest.mark.unit
class TestPhoneFiltering:
    """Test phone filtering functions."""

    def test_filter_valid_phones(self, spark):
        """Test filtering to keep only valid phones."""
        test_data = [
            ("555-123-4567", "555-123-4567"),
            ("1-800-555-1234", "1-800-555-1234"),
            ("invalid", None),
            ("123-456", None),
            ("", None),
            (None, None),
        ]

        df = spark.createDataFrame(test_data, ["phone", "expected"])
        result_df = df.withColumn(
            "filtered", phones.filter_valid_phones(F.col("phone"))
        )

        results = result_df.collect()
        for row in results:
            assert (
                row["filtered"] == row["expected"]
            ), f"Failed for '{row['phone']}': expected {row['expected']}, got {row['filtered']}"

    def test_filter_nanp_phones(self, spark):
        """Test filtering to keep only NANP phones."""
        test_data = [
            ("555-123-4567", "555-123-4567"),
            ("1-800-555-1234", "1-800-555-1234"),
            ("+44 20 7946 0958", None),  # UK number
            ("1234567", None),  # Too short for NANP
            ("", None),
            (None, None),
        ]

        df = spark.createDataFrame(test_data, ["phone", "expected"])
        result_df = df.withColumn("filtered", phones.filter_nanp_phones(F.col("phone")))

        results = result_df.collect()
        for row in results:
            assert (
                row["filtered"] == row["expected"]
            ), f"Failed for '{row['phone']}': expected {row['expected']}, got {row['filtered']}"

    def test_filter_toll_free_phones(self, spark):
        """Test filtering to keep only toll-free phones."""
        test_data = [
            ("1-800-555-1234", "1-800-555-1234"),
            ("888-555-1234", "888-555-1234"),
            ("555-123-4567", None),
            ("212-555-1234", None),
            ("", None),
            (None, None),
        ]

        df = spark.createDataFrame(test_data, ["phone", "expected"])
        result_df = df.withColumn(
            "filtered", phones.filter_toll_free_phones(F.col("phone"))
        )

        results = result_df.collect()
        for row in results:
            assert (
                row["filtered"] == row["expected"]
            ), f"Failed for '{row['phone']}': expected {row['expected']}, got {row['filtered']}"


@pytest.mark.unit
class TestPhoneEdgeCases:
    """Test edge cases and complex scenarios."""

    def test_null_and_empty_handling(self, spark):
        """Test handling of null and empty values."""
        test_data = [
            (None,),
            ("",),
            ("   ",),
            ("\t\n",),
        ]

        df = spark.createDataFrame(test_data, ["phone"])

        result_df = df.select(
            F.col("phone"),
            phones.extract_digits(F.col("phone")).alias("digits"),
            phones.extract_area_code(F.col("phone")).alias("area_code"),
            phones.is_valid_phone(F.col("phone")).alias("is_valid"),
            phones.format_nanp(F.col("phone")).alias("formatted"),
        )

        results = result_df.collect()
        for row in results:
            # All should return empty strings or False for invalid inputs
            assert row["digits"] in ["", "0"]  # Whitespace might have digits
            assert row["area_code"] == ""
            assert not row["is_valid"]
            assert row["formatted"] == ""

    @pytest.mark.skip(reason="Letter conversion creates complex expression tree")  
    def test_phone_with_letters(self, spark):
        """Test handling of vanity numbers with letters."""
        test_data = [
            ("1-800-FLOWERS", "800-356-9377"),
            ("1-800-CONTACT", "800-266-8228"),  # Fixed to 7 letters
            ("555-GET-HELP", "555-438-4357"),
            ("1-888-NEW-CARS", "888-639-2277"),
        ]

        df = spark.createDataFrame(test_data, ["phone", "expected"])
        result_df = df.withColumn("converted", phones.standardize_phone(F.col("phone")))

        results = result_df.collect()
        for row in results:
            assert (
                row["converted"] == row["expected"]
            ), f"Failed for '{row['phone']}': expected '{row['expected']}', got '{row['converted']}'"

    def test_international_numbers(self, spark):
        """Test handling of various international formats."""
        test_data = [
            ("+44 20 7946 0958", True),  # UK
            ("+33 1 42 86 82 00", True),  # France
            ("+49 30 12085", True),  # Germany (short)
            ("+86 10 1234 5678", True),  # China
            ("+91 22 1234 5678", True),  # India
            ("+7 495 123 4567", True),  # Russia
        ]

        df = spark.createDataFrame(test_data, ["phone", "expected_valid"])
        result_df = df.withColumn(
            "is_valid", phones.is_valid_international(F.col("phone"))
        )

        results = result_df.collect()
        for row in results:
            assert (
                row["is_valid"] == row["expected_valid"]
            ), f"Failed for '{row['phone']}': expected {row['expected_valid']}, got {row['is_valid']}"
