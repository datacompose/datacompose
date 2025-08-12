"""
Test address cleaning functionality.
"""

import os

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as f

# Import test data from consolidated file
from tests.unit.transformers.text.test_addresses.test_data_addresses import (
    ADDRESS_ABBREVIATIONS,
    ADDRESS_PATTERNS,
    ADDRESS_TEST_DATA,
    ADDRESS_TEST_DATA_COLUMNS,
    MESSY_ADDRESS_DATA,
)


@pytest.fixture(scope="session")
def spark():
    """Create a Spark session for testing."""
    import logging
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
        SparkSession.builder.appName("AddressCleaningTests")
        .master(master)
        .config("spark.ui.enabled", "false")
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


@pytest.fixture
def address_test_data(spark):
    """Create test data with various address components."""
    return spark.createDataFrame(ADDRESS_TEST_DATA, ADDRESS_TEST_DATA_COLUMNS)


@pytest.fixture
def messy_address_data(spark):
    """Create test data with particularly messy addresses for edge case testing."""
    return spark.createDataFrame(MESSY_ADDRESS_DATA, ["raw_address"])


@pytest.fixture
def address_abbreviations():
    """Common address abbreviations to expand."""
    return ADDRESS_ABBREVIATIONS


@pytest.fixture
def address_patterns():
    """Regex patterns for parsing addresses."""
    return ADDRESS_PATTERNS


@pytest.mark.unit
class TestAddressCleaning:
    """Test address cleaning functionality."""

    def test_address_components(self, address_test_data):
        """Test that address test data has all expected components."""
        # Verify the fixture has data
        assert address_test_data.count() == 20

        # Check columns
        expected_columns = [
            "street_number",
            "street_name",
            "unit",
            "city",
            "state",
            "zip_code",
            "country",
            "full_address",
        ]
        assert address_test_data.columns == expected_columns

        # Sample a few rows to verify structure
        first_row = address_test_data.first()
        assert first_row["street_number"] == "123"
        assert first_row["street_name"] == "Main St"
        assert first_row["city"] == "New York"

    def test_messy_addresses(self, messy_address_data):
        """Test that messy address data covers various edge cases."""
        assert messy_address_data.count() == 21

        # Check we have nulls and empty strings
        null_count = messy_address_data.filter(f.col("raw_address").isNull()).count()
        assert null_count == 1

        empty_count = messy_address_data.filter(
            f.trim(f.col("raw_address")) == ""
        ).count()
        assert empty_count >= 1

    def test_extract_zipcode(self, spark, address_test_data):
        """Test zip code extraction from various formats."""
        from datacompose.transformers.text.clean_addresses.pyspark.pyspark_primitives import (
            extract_zip_code,
        )

        # Test that the function is callable
        assert callable(extract_zip_code)

        # Test extraction from zip_code column
        result_df = address_test_data.select(
            "zip_code", extract_zip_code(f.col("zip_code")).alias("extracted_zip")
        )

        # Collect results for validation
        results = result_df.collect()

        # Verify extraction for standard 5-digit zips
        assert results[0]["extracted_zip"] == "10001"  # NY
        assert results[1]["extracted_zip"] == "90001"  # CA
        assert results[2]["extracted_zip"] == "60601"  # IL
        assert results[3]["extracted_zip"] == "94102"  # SF
        assert results[4]["extracted_zip"] == "98101"  # Seattle

        # Check null handling
        assert results[5]["extracted_zip"] == ""  # Null input

        # Check extended zip code (9-digit)
        assert results[13]["extracted_zip"] == "90001-1234"  # Extended format

    def test_extract_zipcode_from_full_address(
        self, spark, address_test_data, messy_address_data
    ):
        """Test zip code extraction from full address strings."""
        from datacompose.transformers.text.clean_addresses.pyspark.pyspark_primitives import (
            extract_zip_code,
        )

        # Test extraction from full_address column
        result_df = address_test_data.select(
            "full_address",
            extract_zip_code(f.col("full_address")).alias("extracted_zip"),
        )

        results = result_df.collect()

        # Verify extraction from complete addresses
        assert (
            results[0]["extracted_zip"] == "10001"
        )  # "123 Main St Apt 4B, New York, NY 10001"
        assert (
            results[1]["extracted_zip"] == "90001"
        )  # "456 Oak Avenue, Los Angeles, CA 90001"
        assert (
            results[2]["extracted_zip"] == "60601"
        )  # "789 Elm Street Suite 200, Chicago, IL 60601"

        # Test with messy addresses
        messy_result = messy_address_data.select(
            "raw_address", extract_zip_code(f.col("raw_address")).alias("extracted_zip")
        )

        messy_results = messy_result.collect()

        # Check various messy formats
        assert messy_results[0]["extracted_zip"] == "10001"  # Missing commas
        assert messy_results[1]["extracted_zip"] == "90001"  # All lowercase
        assert messy_results[2]["extracted_zip"] == "60601"  # All uppercase
        assert messy_results[3]["extracted_zip"] == "98101"  # Extra spaces
        assert messy_results[6]["extracted_zip"] == "19103-1234"  # Extended zip

        # Check PO Box addresses
        assert messy_results[15]["extracted_zip"] == "75201"  # PO Box
        assert messy_results[16]["extracted_zip"] == "77001"  # P.O.Box

    def test_extract_zipcode_edge_cases(self, spark):
        """Test zip code extraction with edge cases and invalid formats."""
        from datacompose.transformers.text.clean_addresses.pyspark.pyspark_primitives import (
            extract_zip_code,
        )

        # Create test data with edge cases
        edge_cases = [
            ("12345",),  # Just zip
            ("12345-6789",),  # Extended zip only
            ("00000",),  # All zeros
            ("99999",),  # All nines
            ("1234",),  # Too short
            ("123456",),  # Too long without dash
            ("12345-678",),  # Invalid extended format
            ("12345-67890",),  # Extended too long
            ("abcde",),  # Letters
            ("123 45",),  # Space in middle
            ("",),  # Empty string
            (None,),  # Null
            ("multiple 12345 and 67890 zips",),  # Multiple zips (should get first)
            ("no zip here",),  # No zip code
            ("partial12345zip",),  # No word boundary
            ("12345.",),  # Zip with punctuation
            ("(12345)",),  # Zip in parentheses
            ("ZIP: 12345",),  # With label
        ]

        df = spark.createDataFrame(edge_cases, ["text"])

        result_df = df.select(
            "text", extract_zip_code(f.col("text")).alias("extracted_zip")
        )

        results = result_df.collect()

        # Verify edge case handling
        assert results[0]["extracted_zip"] == "12345"  # Just zip
        assert results[1]["extracted_zip"] == "12345-6789"  # Extended zip only
        assert results[2]["extracted_zip"] == "00000"  # All zeros
        assert results[3]["extracted_zip"] == "99999"  # All nines
        assert results[4]["extracted_zip"] == ""  # Too short (not a valid zip)
        assert results[5]["extracted_zip"] == ""  # Too long (not a valid zip)
        assert (
            results[6]["extracted_zip"] == "12345"
        )  # Invalid extended format (extracts valid 5-digit part)
        assert (
            results[7]["extracted_zip"] == "12345"
        )  # Extended too long (extracts valid 5-digit part)
        assert results[8]["extracted_zip"] == ""  # Letters
        assert results[9]["extracted_zip"] == ""  # Space in middle
        assert results[10]["extracted_zip"] == ""  # Empty string
        assert results[11]["extracted_zip"] == ""  # Null
        assert results[12]["extracted_zip"] == "12345"  # First of multiple zips
        assert results[13]["extracted_zip"] == ""  # No zip code
        assert results[14]["extracted_zip"] == ""  # No word boundary
        assert results[15]["extracted_zip"] == "12345"  # Zip with punctuation
        assert results[16]["extracted_zip"] == "12345"  # Zip in parentheses
        assert results[17]["extracted_zip"] == "12345"  # With label

    def test_extract_zipcode_international(self, spark):
        """Test that international postal codes are handled appropriately."""
        from datacompose.transformers.text.clean_addresses.pyspark.pyspark_primitives import (
            extract_zip_code,
        )

        # International postal codes (should not match US zip pattern)
        international_data = [
            ("SW1A 2AA",),  # UK postcode
            ("75008",),  # French postal code (5 digits like US)
            ("M5V 3A8",),  # Canadian postal code
            ("100-0001",),  # Japanese postal code
            ("28001",),  # Spanish postal code (5 digits)
            ("1000",),  # Too short for US zip
            ("12345 Paris France",),  # US-format zip in international context
        ]

        df = spark.createDataFrame(international_data, ["postal_code"])

        result_df = df.select(
            "postal_code", extract_zip_code(f.col("postal_code")).alias("extracted_zip")
        )

        results = result_df.collect()

        # Only valid US zip codes should be extracted
        assert results[0]["extracted_zip"] == ""  # UK postcode - not US format
        assert results[1]["extracted_zip"] == "75008"  # French but matches US format
        assert results[2]["extracted_zip"] == ""  # Canadian - not US format
        assert results[3]["extracted_zip"] == ""  # Japanese - dash in wrong place
        assert results[4]["extracted_zip"] == "28001"  # Spanish but matches US format
        assert results[5]["extracted_zip"] == ""  # Too short
        assert results[6]["extracted_zip"] == "12345"  # Valid US zip
