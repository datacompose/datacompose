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
        SparkSession.builder.appName("POBoxExtractionTests")
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
class TestPOBoxExtraction:
    """Test PO Box extraction functionality."""

    def test_extract_po_box(self, spark):
        """Test extraction of PO Box numbers from addresses."""
        test_data = [
            # Standard formats
            ("PO Box 123", "123"),
            ("P.O. Box 456", "456"),
            ("POB 789", "789"),
            ("Post Office Box 1011", "1011"),
            # Mixed case
            ("po box 123", "123"),
            ("P.o. BOX 456", "456"),
            ("pob 789", "789"),
            # With spaces variations
            ("P O Box 123", "123"),
            ("P. O. Box 456", "456"),
            # Alphanumeric box numbers
            ("PO Box 123A", "123A"),
            ("PO Box A123", "A123"),
            ("PO Box 12-34", "12-34"),
            # In full addresses
            ("123 Main St, PO Box 456", "456"),
            ("PO Box 789, New York, NY 10001", "789"),
            # No PO Box
            ("123 Main St", ""),
            ("456 Oak Ave, Suite 200", ""),
            ("", ""),
            (None, ""),
        ]

        df = spark.createDataFrame(test_data, ["address", "expected"])
        result_df = df.withColumn("po_box", addresses.extract_po_box(F.col("address")))

        results = result_df.collect()
        for row in results:
            assert (
                row["po_box"] == row["expected"]
            ), f"Failed for '{row['address']}': expected '{row['expected']}', got '{row['po_box']}'"

    def test_has_po_box(self, spark):
        """Test detection of PO Box in address."""
        test_data = [
            ("PO Box 123", True),
            ("P.O. Box 456", True),
            ("POB 789", True),
            ("123 Main St, PO Box 456", True),
            ("123 Main St", False),
            ("456 Oak Ave", False),
            ("", False),
            (None, False),
        ]

        df = spark.createDataFrame(test_data, ["address", "expected"])
        result_df = df.withColumn("has_box", addresses.has_po_box(F.col("address")))

        results = result_df.collect()
        for row in results:
            assert (
                row["has_box"] == row["expected"]
            ), f"Failed for '{row['address']}': expected {row['expected']}, got {row['has_box']}"

    def test_is_po_box_only(self, spark):
        """Test detection of PO Box-only addresses."""
        test_data = [
            # PO Box only
            ("PO Box 123", True),
            ("P.O. Box 456", True),
            ("POB 789", True),
            ("PO Box 123, New York, NY", True),
            ("PO Box 456, New York, NY 10001", True),
            # Mixed addresses (has both street and PO Box)
            ("123 Main St, PO Box 456", False),
            ("456 Oak Ave, P.O. Box 789", False),
            # Street addresses only
            ("123 Main St", False),
            ("456 Oak Ave", False),
            # Edge cases
            ("", False),
            (None, False),
        ]

        df = spark.createDataFrame(test_data, ["address", "expected"])
        result_df = df.withColumn("po_only", addresses.is_po_box_only(F.col("address")))

        results = result_df.collect()
        for row in results:
            assert (
                row["po_only"] == row["expected"]
            ), f"Failed for '{row['address']}': expected {row['expected']}, got {row['po_only']}"

    def test_remove_po_box(self, spark):
        """Test removal of PO Box from address."""
        test_data = [
            # PO Box with street address
            ("123 Main St, PO Box 456", "123 Main St"),
            ("456 Oak Ave, P.O. Box 789", "456 Oak Ave"),
            # PO Box only addresses
            ("PO Box 123", ""),
            ("PO Box 456, New York, NY", "New York, NY"),
            ("P.O. Box 789, New York, NY 10001", "New York, NY 10001"),
            # PO Box in the middle
            ("123 Main St, PO Box 456, New York, NY", "123 Main St, New York, NY"),
            # No PO Box (no change)
            ("123 Main St", "123 Main St"),
            ("456 Oak Ave, Suite 200", "456 Oak Ave, Suite 200"),
            # Empty
            ("", ""),
            (None, ""),
        ]

        df = spark.createDataFrame(test_data, ["address", "expected"])
        result_df = df.withColumn(
            "without_box", addresses.remove_po_box(F.col("address"))
        )

        results = result_df.collect()
        for row in results:
            assert (
                row["without_box"] == row["expected"]
            ), f"Failed for '{row['address']}': expected '{row['expected']}', got '{row['without_box']}'"

    def test_standardize_po_box(self, spark):
        """Test standardization of PO Box format."""
        test_data = [
            # Various formats to standard
            ("P.O. Box 123", "PO Box 123"),
            ("P O Box 456", "PO Box 456"),
            ("POB 789", "PO Box 789"),
            ("Post Office Box 1011", "PO Box 1011"),
            ("po box 123", "PO Box 123"),
            # In full addresses
            ("123 Main St, P.O. Box 456", "123 Main St, PO Box 456"),
            ("POB 789, New York, NY", "PO Box 789, New York, NY"),
            # Already standard
            ("PO Box 123", "PO Box 123"),
            # No PO Box (no change)
            ("123 Main St", "123 Main St"),
            ("", ""),
            (None, ""),
        ]

        df = spark.createDataFrame(test_data, ["address", "expected"])
        result_df = df.withColumn(
            "standardized", addresses.standardize_po_box(F.col("address"))
        )

        results = result_df.collect()
        for row in results:
            assert (
                row["standardized"] == row["expected"]
            ), f"Failed for '{row['address']}': expected '{row['expected']}', got '{row['standardized']}'"

    def test_extract_private_mailbox(self, spark):
        """Test extraction of private mailbox (PMB) numbers."""
        test_data = [
            # Standard PMB formats
            ("123 Main St PMB 456", "456"),
            ("789 Oak Ave PMB 12", "12"),
            ("456 Elm St Private Mail Box 789", "789"),
            # PMB with suite/unit
            ("123 Main St Suite 100 PMB 456", "456"),
            ("789 Oak Ave #101 PMB 12", "12"),
            # Mixed case
            ("123 Main St pmb 456", "456"),
            ("789 Oak Ave PMB 12", "12"),
            # No PMB
            ("123 Main St", ""),
            ("PO Box 123", ""),
            ("", ""),
            (None, ""),
        ]

        df = spark.createDataFrame(test_data, ["address", "expected"])
        result_df = df.withColumn(
            "pmb", addresses.extract_private_mailbox(F.col("address"))
        )

        results = result_df.collect()
        for row in results:
            assert (
                row["pmb"] == row["expected"]
            ), f"Failed for '{row['address']}': expected '{row['expected']}', got '{row['pmb']}'"

    def test_po_box_edge_cases(self, spark):
        """Test edge cases for PO Box extraction."""
        test_data = [
            # Multiple PO Boxes (should get first)
            ("PO Box 123, PO Box 456", "123"),
            # PO Box with special characters
            ("PO Box #123", "#123"),
            ("PO Box 123-A", "123-A"),
            ("PO Box 123/124", "123/124"),
            # Very long box numbers
            ("PO Box 1234567890", "1234567890"),
            # PO Box with no number (should not extract)
            ("PO Box", ""),
            ("P.O. Box", ""),
            # False positives to avoid
            ("123 Pob Street", ""),  # Street name contains "Pob"
            ("456 Post Office Drive", ""),  # Street name, not PO Box
            # International variations
            ("Postfach 123", ""),  # German - not handled yet
            ("Boîte Postale 456", ""),  # French - not handled yet
            ("Casilla de Correo 789", ""),  # Spanish - not handled yet
        ]

        df = spark.createDataFrame(test_data, ["address", "expected"])
        result_df = df.withColumn("po_box", addresses.extract_po_box(F.col("address")))

        results = result_df.collect()
        for row in results:
            assert (
                row["po_box"] == row["expected"]
            ), f"Failed for '{row['address']}': expected '{row['expected']}', got '{row['po_box']}'"

    def test_combined_po_box_operations(self, spark):
        """Test combining multiple PO Box operations."""
        test_data = [
            # Full address with PO Box
            ("123 Main St, PO Box 456, New York, NY 10001",),
            # PO Box only
            ("P.O. Box 789, Los Angeles, CA 90001",),
            # PMB address
            ("456 Oak Ave Suite 200 PMB 123, Chicago, IL 60601",),
        ]

        df = spark.createDataFrame(test_data, ["address"])

        result_df = df.select(
            F.col("address"),
            addresses.extract_po_box(F.col("address")).alias("po_box"),
            addresses.has_po_box(F.col("address")).alias("has_box"),
            addresses.is_po_box_only(F.col("address")).alias("box_only"),
            addresses.remove_po_box(F.col("address")).alias("without_box"),
            addresses.standardize_po_box(F.col("address")).alias("standardized"),
            addresses.extract_private_mailbox(F.col("address")).alias("pmb"),
        )

        results = result_df.collect()

        # First address: street with PO Box
        assert results[0]["po_box"] == "456"
        assert results[0]["has_box"]
        assert not results[0]["box_only"]
        assert results[0]["without_box"] == "123 Main St, New York, NY 10001"
        assert (
            results[0]["standardized"] == "123 Main St, PO Box 456, New York, NY 10001"
        )
        assert results[0]["pmb"] == ""

        # Second address: PO Box only
        assert results[1]["po_box"] == "789"
        assert results[1]["has_box"]
        assert results[1]["box_only"]
        assert results[1]["without_box"] == "Los Angeles, CA 90001"
        assert results[1]["standardized"] == "PO Box 789, Los Angeles, CA 90001"
        assert results[1]["pmb"] == ""

        # Third address: PMB
        assert results[2]["po_box"] == ""
        assert not results[2]["has_box"]
        assert not results[2]["box_only"]
        assert results[2]["without_box"] == results[2]["address"]  # No change
        assert results[2]["standardized"] == results[2]["address"]  # No change
        assert results[2]["pmb"] == "123"
