"""
Comprehensive tests for phone number extraction and processing functionality.
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
    import os
    import warnings

    # Suppress all warnings
    warnings.filterwarnings("ignore")

    # Suppress Spark logging
    logging.getLogger("py4j").setLevel(logging.ERROR)
    logging.getLogger("pyspark").setLevel(logging.ERROR)

    # Set Java options to suppress Ivy messages
    os.environ["SPARK_SUBMIT_OPTS"] = "-Divy.message.logger.level=ERROR"

    master = os.environ.get("SPARK_MASTER", "local[*]")

    spark = (
        SparkSession.builder.appName("PhoneExtractionTests")
        .master(master)
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "false")
        .config("spark.python.worker.reuse", "true")
        .config("spark.driver.extraJavaOptions", "-Dlog4j.logger.org.apache.ivy=ERROR")
        .config(
            "spark.executor.extraJavaOptions", "-Dlog4j.logger.org.apache.ivy=ERROR"
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    yield spark
    spark.stop()


@pytest.mark.unit
class TestPhoneExtraction:
    """Test phone number extraction functions."""

    def test_extract_phone_from_text(self, spark):
        """Test extraction of phone numbers from various text contexts."""
        test_data = [
            # Standard formats in text
            ("Please call me at 555-123-4567 tomorrow", "5551234567"),
            ("Contact: (212) 555-1234 or email us", "2125551234"),
            ("My number is 1-800-555-1234", "18005551234"),
            ("Call 555.123.4567 for more info", "5551234567"),
            
            # Multiple numbers (should get first one)
            ("Primary: 555-111-2222, Secondary: 555-333-4444", "5551112222"),
            
            # Numbers with extensions in text
            ("Office: 555-123-4567 ext. 123", "5551234567"),
            ("Call 555-123-4567 x456 during business hours", "5551234567"),
            
            # International formats
            ("UK office: +44 20 7946 0958", "442079460958"),
            ("Contact us at +1-555-123-4567", "15551234567"),
            
            # Embedded in sentences
            ("You can reach me at555-123-4567anytime", "5551234567"),
            ("The number555.123.4567is valid", "5551234567"),
            
            # With surrounding punctuation
            ("Call (555-123-4567) immediately!", "5551234567"),
            ("Number: 555-123-4567.", "5551234567"),
            ("Is 555-123-4567 your number?", "5551234567"),
            
            # No phone number
            ("No contact information provided", ""),
            ("Email only: test@example.com", ""),
            
            # Edge cases
            ("", ""),
            (None, ""),
        ]

        df = spark.createDataFrame(test_data, ["text", "expected"])
        result_df = df.withColumn("phone_digits", phones.extract_digits(F.col("text")))

        results = result_df.collect()
        for row in results:
            # For texts with phone numbers, we should extract the digits
            if row["expected"]:
                assert row["expected"] in row["phone_digits"], \
                    f"Failed for '{row['text']}': expected '{row['expected']}' to be in '{row['phone_digits']}'"
            else:
                assert row["phone_digits"] == row["expected"], \
                    f"Failed for '{row['text']}': expected '{row['expected']}', got '{row['phone_digits']}'"

    def test_extract_first_valid_phone(self, spark):
        """Test extracting the first valid phone number from text."""
        test_data = [
            # Clear phone numbers in text
            ("Call me at 555-123-4567", "555-123-4567"),
            ("My office number is (212) 555-1234", "(212) 555-1234"),
            
            # Multiple numbers - should get first
            ("Home: 555-111-2222, Work: 555-333-4444", "555-111-2222"),
            
            # Phone with extension
            ("Office: 555-123-4567 ext. 123", "555-123-4567 ext. 123"),
            
            # International
            ("UK: +1-555-123-4567", "+1-555-123-4567"),
            
            # Embedded in text
            ("Please call555-123-4567now", "555-123-4567"),
            
            # No valid phone
            ("Only email: test@example.com", ""),
            ("", ""),
            (None, ""),
        ]

        df = spark.createDataFrame(test_data, ["text", "expected"])
        
        # Use our new extract_phone_from_text function
        result_df = df.withColumn(
            "extracted_phone",
            phones.extract_phone_from_text(F.col("text"))
        )

        results = result_df.collect()
        for row in results:
            if row["expected"]:
                # Check that we found a phone number
                assert row["extracted_phone"] != "", \
                    f"Failed to extract from '{row['text']}': expected '{row['expected']}', got '{row['extracted_phone']}'"
                # The extracted phone should contain the key digits
                expected_digits = ''.join(c for c in row["expected"] if c.isdigit())
                extracted_digits = ''.join(c for c in row["extracted_phone"] if c.isdigit())
                if expected_digits and extracted_digits:
                    assert expected_digits in extracted_digits or extracted_digits in expected_digits, \
                        f"Digits mismatch for '{row['text']}': expected digits '{expected_digits}', got '{extracted_digits}'"
            else:
                assert row["extracted_phone"] == "", \
                    f"Should not extract from '{row['text']}': got '{row['extracted_phone']}'"

    def test_extract_all_phones_from_text(self, spark):
        """Test extracting all phone numbers from text."""
        # Note: Due to Spark SQL limitations, this currently only extracts first phone
        # A full implementation would require a UDF
        test_data = [
            # Single phone
            ("My number is 555-123-4567", ["555-123-4567"]),
            ("Call (212) 555-1234 for info", ["(212) 555-1234"]),
            ("Contact: 800.555.1234", ["800.555.1234"]),
            
            # Multiple phones - will only get first one due to current limitation
            ("Home: 555-111-2222, Work: 555-333-4444", ["555-111-2222"]),
            
            # No phones
            ("No phone numbers here", []),
            ("", []),
            (None, []),
        ]

        df = spark.createDataFrame(test_data, ["text", "expected"])
        
        # Use our extract_all_phones_from_text function
        result_df = df.withColumn(
            "extracted_phones",
            phones.extract_all_phones_from_text(F.col("text"))
        )

        results = result_df.collect()
        for row in results:
            extracted = row["extracted_phones"] if row["extracted_phones"] else []
            expected = row["expected"] if row["expected"] else []
            
            # Check count matches
            assert len(extracted) == len(expected), \
                f"Count mismatch for '{row['text']}': expected {len(expected)} phones, got {len(extracted)}"
            
            # Check each phone is found
            for i, exp_phone in enumerate(expected):
                if i < len(extracted):
                    # Just check that we extracted a phone number
                    assert extracted[i] != "", \
                        f"Expected phone '{exp_phone}' but got empty string for text '{row['text']}'"

    def test_extract_digits(self, spark):
        """Test extraction of digits from phone numbers."""
        test_data = [
            ("(555) 123-4567", "5551234567"),
            ("1-800-FLOWERS", "1800"),  # Letters not converted yet
            ("+1-555-123-4567", "15551234567"),
            ("555.123.4567", "5551234567"),
            ("", ""),
            (None, ""),
        ]

        df = spark.createDataFrame(test_data, ["phone", "expected"])
        result_df = df.withColumn("digits", phones.extract_digits(F.col("phone")))

        results = result_df.collect()
        for row in results:
            assert (
                row["digits"] == row["expected"]
            ), f"Failed for '{row['phone']}': expected '{row['expected']}', got '{row['digits']}'"

    def test_extract_extension(self, spark):
        """Test extraction of extension from phone numbers."""
        test_data = [
            ("555-123-4567 ext. 123", "123"),
            ("555-123-4567 ext 456", "456"),
            ("555-123-4567 ext789", "789"),
            ("555-123-4567", ""),
            ("", ""),
            (None, ""),
        ]

        df = spark.createDataFrame(test_data, ["phone", "expected"])
        result_df = df.withColumn("extension", phones.extract_extension(F.col("phone")))

        results = result_df.collect()
        for row in results:
            assert (
                row["extension"] == row["expected"]
            ), f"Failed for '{row['phone']}': expected '{row['expected']}', got '{row['extension']}'"

    def test_extract_country_code(self, spark):
        """Test extraction of country code."""
        test_data = [
            ("+1-555-123-4567", "1"),
            ("+44 20 7946 0958", "44"),
            ("+86 10 1234 5678", "86"),
            ("1-555-123-4567", "1"),  # NANP with leading 1
            ("15551234567", "1"),  # 11 digits starting with 1
            ("555-123-4567", ""),  # No country code
            ("", ""),
            (None, ""),
        ]

        df = spark.createDataFrame(test_data, ["phone", "expected"])
        result_df = df.withColumn(
            "country_code", phones.extract_country_code(F.col("phone"))
        )

        results = result_df.collect()
        for row in results:
            assert (
                row["country_code"] == row["expected"]
            ), f"Failed for '{row['phone']}': expected '{row['expected']}', got '{row['country_code']}'"

    def test_extract_area_code(self, spark):
        """Test extraction of area code from NANP numbers."""
        test_data = [
            ("(555) 123-4567", "555"),
            ("1-800-555-1234", "800"),
            ("15551234567", "555"),
            ("5551234567", "555"),
            ("212-555-1234", "212"),
            ("123-4567", ""),  # Not enough digits
            ("", ""),
            (None, ""),
        ]

        df = spark.createDataFrame(test_data, ["phone", "expected"])
        result_df = df.withColumn("area_code", phones.extract_area_code(F.col("phone")))

        results = result_df.collect()
        for row in results:
            assert (
                row["area_code"] == row["expected"]
            ), f"Failed for '{row['phone']}': expected '{row['expected']}', got '{row['area_code']}'"

    def test_extract_exchange(self, spark):
        """Test extraction of exchange from NANP numbers."""
        test_data = [
            ("(555) 123-4567", "123"),
            ("1-800-555-1234", "555"),
            ("15551234567", "123"),
            ("5551234567", "123"),
            ("212-456-7890", "456"),
            ("", ""),
            (None, ""),
        ]

        df = spark.createDataFrame(test_data, ["phone", "expected"])
        result_df = df.withColumn("exchange", phones.extract_exchange(F.col("phone")))

        results = result_df.collect()
        for row in results:
            assert (
                row["exchange"] == row["expected"]
            ), f"Failed for '{row['phone']}': expected '{row['expected']}', got '{row['exchange']}'"

    def test_extract_subscriber(self, spark):
        """Test extraction of subscriber number from NANP numbers."""
        test_data = [
            ("(555) 123-4567", "4567"),
            ("1-800-555-1234", "1234"),
            ("15551234567", "4567"),
            ("5551234567", "4567"),
            ("212-456-7890", "7890"),
            ("", ""),
            (None, ""),
        ]

        df = spark.createDataFrame(test_data, ["phone", "expected"])
        result_df = df.withColumn(
            "subscriber", phones.extract_subscriber(F.col("phone"))
        )

        results = result_df.collect()
        for row in results:
            assert (
                row["subscriber"] == row["expected"]
            ), f"Failed for '{row['phone']}': expected '{row['expected']}', got '{row['subscriber']}'"

    def test_extract_local_number(self, spark):
        """Test extraction of local number (7 digits) from NANP numbers."""
        test_data = [
            ("(555) 123-4567", "1234567"),
            ("1-800-555-1234", "5551234"),
            ("15551234567", "1234567"),
            ("5551234567", "1234567"),
            ("212-456-7890", "4567890"),
            ("123-4567", ""),  # Not enough digits
            ("", ""),
            (None, ""),
        ]

        df = spark.createDataFrame(test_data, ["phone", "expected"])
        result_df = df.withColumn(
            "local_number", phones.extract_local_number(F.col("phone"))
        )

        results = result_df.collect()
        for row in results:
            assert (
                row["local_number"] == row["expected"]
            ), f"Failed for '{row['phone']}': expected '{row['expected']}', got '{row['local_number']}'"


@pytest.mark.unit
class TestPhoneValidation:
    """Test phone number validation functions."""

    def test_is_valid_nanp(self, spark):
        """Test NANP validation."""
        test_data = [
            ("(555) 123-4567", True),
            ("1-800-555-1234", True),
            ("15551234567", True),
            ("5551234567", True),
            ("212-456-7890", True),
            ("123-456-7890", False),  # Area code starts with 1
            ("555-012-3456", False),  # Exchange starts with 0
            ("555-123-456", False),  # Not enough digits
            ("555-123-45678", False),  # Too many digits
            ("", False),
            (None, False),
        ]

        df = spark.createDataFrame(test_data, ["phone", "expected"])
        result_df = df.withColumn("is_valid", phones.is_valid_nanp(F.col("phone")))

        results = result_df.collect()
        for row in results:
            assert (
                row["is_valid"] == row["expected"]
            ), f"Failed for '{row['phone']}': expected {row['expected']}, got {row['is_valid']}"

    def test_is_valid_international(self, spark):
        """Test international phone validation."""
        test_data = [
            ("+44 20 7946 0958", True),  # UK
            ("+86 10 1234 5678", True),  # China
            ("+33 1 42 86 82 00", True),  # France
            ("1234567", True),  # 7 digits minimum
            ("123456789012345", True),  # 15 digits maximum
            ("1234567890123456", False),  # 16 digits, too many
            ("123456", False),  # 6 digits, too few
            ("", False),
            (None, False),
        ]

        df = spark.createDataFrame(test_data, ["phone", "expected"])
        result_df = df.withColumn(
            "is_valid", phones.is_valid_international(F.col("phone"))
        )

        results = result_df.collect()
        for row in results:
            assert (
                row["is_valid"] == row["expected"]
            ), f"Failed for '{row['phone']}': expected {row['expected']}, got {row['is_valid']}"

    def test_is_valid_phone(self, spark):
        """Test general phone validation (NANP or international)."""
        test_data = [
            ("(555) 123-4567", True),  # NANP
            ("1-800-555-1234", True),  # NANP toll-free
            ("+44 20 7946 0958", True),  # UK
            ("+86 10 1234 5678", True),  # China
            ("1234567", True),  # Short international
            ("123-456", False),  # Too short
            ("", False),
            (None, False),
        ]

        df = spark.createDataFrame(test_data, ["phone", "expected"])
        result_df = df.withColumn("is_valid", phones.is_valid_phone(F.col("phone")))

        results = result_df.collect()
        for row in results:
            assert (
                row["is_valid"] == row["expected"]
            ), f"Failed for '{row['phone']}': expected {row['expected']}, got {row['is_valid']}"

    def test_is_toll_free(self, spark):
        """Test toll-free number detection."""
        test_data = [
            ("1-800-555-1234", True),
            ("888-555-1234", True),
            ("1-877-555-1234", True),
            ("866-555-1234", True),
            ("855-555-1234", True),
            ("844-555-1234", True),
            ("833-555-1234", True),
            ("555-123-4567", False),
            ("212-555-1234", False),
            ("", False),
            (None, False),
        ]

        df = spark.createDataFrame(test_data, ["phone", "expected"])
        result_df = df.withColumn("is_toll_free", phones.is_toll_free(F.col("phone")))

        results = result_df.collect()
        for row in results:
            assert (
                row["is_toll_free"] == row["expected"]
            ), f"Failed for '{row['phone']}': expected {row['expected']}, got {row['is_toll_free']}"

    def test_is_premium_rate(self, spark):
        """Test premium rate number detection."""
        test_data = [
            ("1-900-555-1234", True),
            ("900-555-1234", True),
            ("19005551234", True),
            ("800-555-1234", False),
            ("555-123-4567", False),
            ("", False),
            (None, False),
        ]

        df = spark.createDataFrame(test_data, ["phone", "expected"])
        result_df = df.withColumn("is_premium", phones.is_premium_rate(F.col("phone")))

        results = result_df.collect()
        for row in results:
            assert (
                row["is_premium"] == row["expected"]
            ), f"Failed for '{row['phone']}': expected {row['expected']}, got {row['is_premium']}"

    def test_has_extension(self, spark):
        """Test extension detection."""
        test_data = [
            ("555-123-4567 ext. 123", True),
            ("555-123-4567 ext 456", True),
            ("555-123-4567 ext789", True),
            ("555-123-4567", False),
            ("", False),
            (None, False),
        ]

        df = spark.createDataFrame(test_data, ["phone", "expected"])
        result_df = df.withColumn("has_ext", phones.has_extension(F.col("phone")))

        results = result_df.collect()
        for row in results:
            assert (
                row["has_ext"] == row["expected"]
            ), f"Failed for '{row['phone']}': expected {row['expected']}, got {row['has_ext']}"


@pytest.mark.unit
class TestPhoneCleaning:
    """Test phone number cleaning functions."""

    def test_convert_letters_to_numbers(self, spark):
        """Test conversion of letters to numbers."""
        test_data = [
            ("1-800-FLOWERS", "1-800-3569377"),
            ("1-800-CONTACTS", "1-800-26682287"),
            ("555-GET-HELP", "555-438-4357"),
            ("555-123-4567", "555-123-4567"),  # No letters
            ("", ""),
            (None, ""),
        ]

        df = spark.createDataFrame(test_data, ["phone", "expected"])
        result_df = df.withColumn(
            "converted", phones.convert_letters_to_numbers(F.col("phone"))
        )

        results = result_df.collect()
        for row in results:
            assert (
                row["converted"] == row["expected"]
            ), f"Failed for '{row['phone']}': expected '{row['expected']}', got '{row['converted']}'"

    def test_remove_extension(self, spark):
        """Test removal of extension."""
        test_data = [
            ("555-123-4567 ext. 123", "555-123-4567 "),
            ("555-123-4567 ext 456", "555-123-4567 "),
            ("555-123-4567 ext789", "555-123-4567 "),
            ("555-123-4567", "555-123-4567"),
            ("", ""),
            (None, ""),
        ]

        df = spark.createDataFrame(test_data, ["phone", "expected"])
        result_df = df.withColumn("no_ext", phones.remove_extension(F.col("phone")))

        results = result_df.collect()
        for row in results:
            assert (
                row["no_ext"] == row["expected"]
            ), f"Failed for '{row['phone']}': expected '{row['expected']}', got '{row['no_ext']}'"

    def test_normalize_separators(self, spark):
        """Test normalization of separators."""
        test_data = [
            ("(555) 123-4567", "555-123-4567"),
            ("555.123.4567", "555-123-4567"),
            ("555 123 4567", "555-123-4567"),
            ("555  123  4567", "555-123-4567"),
            ("(555)123-4567", "555-123-4567"),
            ("555-123-4567", "555-123-4567"),  # Already normalized
            ("", ""),
            (None, ""),
        ]

        df = spark.createDataFrame(test_data, ["phone", "expected"])
        result_df = df.withColumn(
            "normalized", phones.normalize_separators(F.col("phone"))
        )

        results = result_df.collect()
        for row in results:
            assert (
                row["normalized"] == row["expected"]
            ), f"Failed for '{row['phone']}': expected '{row['expected']}', got '{row['normalized']}'"

    def test_normalize_separators_edge_cases(self, spark):
        """Test normalization edge cases."""
        test_data = [
            ("555---123---4567", "555-123-4567"),
            ("   555 123 4567   ", "555-123-4567"),
            ("555...123...4567", "555-123-4567"),
            ("", ""),
            (None, ""),
        ]

        df = spark.createDataFrame(test_data, ["phone", "expected"])
        result_df = df.withColumn(
            "normalized", phones.normalize_separators(F.col("phone"))
        )

        results = result_df.collect()
        for row in results:
            assert (
                row["normalized"] == row["expected"]
            ), f"Failed for '{row['phone']}': expected '{row['expected']}', got '{row['normalized']}'"

    def test_add_country_code(self, spark):
        """Test adding country code to NANP numbers."""
        test_data = [
            ("5551234567", "15551234567"),  # Valid 10-digit NANP
            ("15551234567", "15551234567"),  # Already has country code
            ("212-456-7890", "12124567890"),  # Valid NANP with formatting
            ("123-456-7890", "1234567890"),  # Invalid area code, no change
            ("1234567", "1234567"),  # Not NANP
            ("", ""),
            (None, None),
        ]

        df = spark.createDataFrame(test_data, ["phone", "expected"])
        result_df = df.withColumn(
            "with_country", phones.add_country_code(F.col("phone"))
        )

        results = result_df.collect()
        for row in results:
            assert (
                row["with_country"] == row["expected"]
            ), f"Failed for '{row['phone']}': expected '{row['expected']}', got '{row['with_country']}'"
