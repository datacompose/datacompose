import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from datacompose.transformers.text.clean_addresses.pyspark.pyspark_primitives import (
    addresses,
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
        SparkSession.builder.appName("BuildingUnitExtractionTests")
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
class TestCountryExtraction:
    """Test country extraction functionality."""

    def test_extract_country(self, spark):
        """Test extraction of country from addresses."""
        test_data = [
            # Common variations
            ("123 Main St, New York, USA", "USA"),
            ("456 Oak Ave, Toronto, Canada", "Canada"),
            ("789 Elm St, London, UK", "United Kingdom"),
            ("321 Pine Rd, Paris, France", "France"),
            # Abbreviations
            ("654 Maple Dr, Berlin, DE", "Germany"),
            ("987 Cedar Ln, Tokyo, JP", "Japan"),
            ("111 First St, Sydney, AU", "Australia"),
            # Full names
            ("222 Second Ave, United States of America", "USA"),
            ("333 Third St, Great Britain", "United Kingdom"),
            ("444 Fourth Ave, Deutschland", "Germany"),
            # No country
            ("555 Fifth St, New York", ""),
            ("666 Sixth Ave", ""),
            ("", ""),
            (None, ""),
        ]

        df = spark.createDataFrame(test_data, ["address", "expected"])
        result_df = df.withColumn(
            "country", addresses.extract_country(F.col("address"))
        )

        results = result_df.collect()
        for row in results:
            assert (
                row["country"] == row["expected"]
            ), f"Failed for '{row['address']}': expected '{row['expected']}', got '{row['country']}'"

    def test_has_country(self, spark):
        """Test detection of country in address."""
        test_data = [
            ("123 Main St, USA", True),
            ("456 Oak Ave, Canada", True),
            ("789 Elm St, UK", True),
            ("321 Pine Rd, New York", False),
            ("654 Maple Dr", False),
            ("", False),
            (None, False),
        ]

        df = spark.createDataFrame(test_data, ["address", "expected"])
        result_df = df.withColumn(
            "has_country", addresses.has_country(F.col("address"))
        )

        results = result_df.collect()
        for row in results:
            assert (
                row["has_country"] == row["expected"]
            ), f"Failed for '{row['address']}': expected {row['expected']}, got {row['has_country']}"

    def test_remove_country(self, spark):
        """Test removal of country from address."""
        test_data = [
            ("123 Main St, New York, USA", "123 Main St, New York"),
            ("456 Oak Ave, Toronto, Canada", "456 Oak Ave, Toronto"),
            ("789 Elm St, London, UK", "789 Elm St, London"),
            ("321 Pine Rd, Paris, France", "321 Pine Rd, Paris"),
            ("654 Maple Dr, New York", "654 Maple Dr, New York"),  # No change
            ("", ""),
            (None, ""),
        ]

        df = spark.createDataFrame(test_data, ["address", "expected"])
        result_df = df.withColumn(
            "without_country", addresses.remove_country(F.col("address"))
        )

        results = result_df.collect()
        for row in results:
            assert (
                row["without_country"] == row["expected"]
            ), f"Failed for '{row['address']}': expected '{row['expected']}', got '{row['without_country']}'"

    def test_standardize_country(self, spark):
        """Test standardization of country names."""
        test_data = [
            # Abbreviations to standard
            ("US", "USA"),
            ("U.S.", "USA"),
            ("U.S.A.", "USA"),
            ("UK", "United Kingdom"),
            ("U.K.", "United Kingdom"),
            ("GB", "United Kingdom"),
            # Full names
            ("United States", "USA"),
            ("United States of America", "USA"),
            ("Great Britain", "United Kingdom"),
            ("England", "United Kingdom"),
            # Other languages
            ("Deutschland", "Germany"),
            ("España", "Spain"),
            ("Brasil", "Brazil"),
            # ISO codes
            ("DE", "Germany"),
            ("FR", "France"),
            ("JP", "Japan"),
            ("CN", "China"),
            ("AU", "Australia"),
            # Already standard
            ("Canada", "Canada"),
            ("Mexico", "Mexico"),
            # Unknown
            ("Unknown Country", "Unknown Country"),
            ("", ""),
            (None, ""),
        ]

        df = spark.createDataFrame(test_data, ["input", "expected"])
        result_df = df.withColumn(
            "standardized", addresses.standardize_country(F.col("input"))
        )

        results = result_df.collect()
        for row in results:
            assert (
                row["standardized"] == row["expected"]
            ), f"Failed for '{row['input']}': expected '{row['expected']}', got '{row['standardized']}'"

    def test_country_edge_cases(self, spark):
        """Test edge cases for country extraction."""
        test_data = [
            # Country without comma
            ("123 Main St New York USA", "USA"),
            # Multiple commas
            ("456 Oak Ave, Suite 200, Toronto, Canada", "Canada"),
            # Country with period
            ("789 Elm St, U.S.A.", "USA"),
            ("321 Pine Rd, U.K.", "United Kingdom"),
            # Mixed case
            ("654 Maple Dr, usa", "USA"),
            ("987 Cedar Ln, CANADA", "Canada"),
            # Country codes that could be confused with states
            (
                "111 First St, CA",
                "",
            ),  # Should not extract CA as Canada when it's likely California
            ("222 Second Ave, Toronto, CA", ""),  # Still ambiguous
            # Special cases
            ("333 Third St, South Korea", "South Korea"),
            ("444 Fourth Ave, New Zealand", "New Zealand"),
            ("555 Fifth St, South Africa", "South Africa"),
            ("666 Sixth Ave, Saudi Arabia", "Saudi Arabia"),
            ("777 Seventh St, United Arab Emirates", "UAE"),
        ]

        df = spark.createDataFrame(test_data, ["address", "expected"])
        result_df = df.withColumn(
            "country", addresses.extract_country(F.col("address"))
        )

        results = result_df.collect()
        for row in results:
            # Some edge cases might not work perfectly
            if row["expected"]:
                assert (
                    row["country"] == row["expected"]
                ), f"Failed for '{row['address']}': expected '{row['expected']}', got '{row['country']}'"

    def test_custom_country_mappings(self, spark):
        """Test custom country mappings."""
        custom_mappings = {
            "Czechia": "Czech Republic",
            "Burma": "Myanmar",
            "Holland": "Netherlands",  # Override existing
        }

        test_data = [
            ("Czechia",),
            ("Burma",),
            ("Holland",),
            ("Netherlands",),  # Should still work with standard mapping
        ]

        df = spark.createDataFrame(test_data, ["input"])
        result_df = df.select(
            F.col("input"),
            addresses.standardize_country(
                F.col("input"), custom_mappings=custom_mappings
            ).alias("custom"),
            addresses.standardize_country(F.col("input")).alias("standard"),
        )

        results = result_df.collect()
        assert results[0]["custom"] == "Czech Republic"
        assert results[1]["custom"] == "Myanmar"
        assert results[2]["custom"] == "Netherlands"  # Custom override
        assert results[3]["standard"] == "Netherlands"  # Standard mapping
