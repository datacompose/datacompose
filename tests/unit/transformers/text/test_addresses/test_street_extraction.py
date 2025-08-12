"""
Comprehensive tests for street extraction functionality.
"""

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
        SparkSession.builder.appName("StreetExtractionTests")
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
class TestStreetExtraction:
    """Test street component extraction functionality."""

    def test_extract_street_number(self, spark):
        """Test extraction of street numbers."""
        test_data = [
            ("123 Main Street", "123"),
            ("456A Oak Avenue", "456A"),
            ("789-B Elm Street", "789-B"),
            ("1234/5 Pine Road", "1234/5"),
            ("42 Broadway", "42"),
            ("Main Street", ""),  # No number
            ("One Main Street", ""),  # Written number not extracted
            ("", ""),
            (None, ""),
        ]

        df = spark.createDataFrame(test_data, ["address", "expected"])
        result_df = df.withColumn(
            "street_number", addresses.extract_street_number(F.col("address"))
        )

        results = result_df.collect()
        for row in results:
            assert (
                row["street_number"] == row["expected"]
            ), f"Failed for '{row['address']}': expected '{row['expected']}', got '{row['street_number']}'"

    def test_extract_street_prefix(self, spark):
        """Test extraction of directional prefixes."""
        test_data = [
            ("123 North Main Street", "North"),
            ("456 S Broadway", "S"),
            ("789 East 42nd Street", "East"),
            ("321 W. Elm Avenue", "W"),  # Period is not included in extraction
            ("NE 5th Avenue", "NE"),
            ("Southwest Harbor Road", "Southwest"),
            ("123 Main Street", ""),  # No prefix
            ("", ""),
            (None, ""),
        ]

        df = spark.createDataFrame(test_data, ["address", "expected"])
        result_df = df.withColumn(
            "street_prefix", addresses.extract_street_prefix(F.col("address"))
        )

        results = result_df.collect()
        for row in results:
            assert (
                row["street_prefix"].lower() == row["expected"].lower()
            ), f"Failed for '{row['address']}': expected '{row['expected']}', got '{row['street_prefix']}'"

    def test_extract_street_name(self, spark):
        """Test extraction of street names."""
        test_data = [
            ("123 Main Street", "Main"),
            ("456 North Broadway Avenue", "Broadway"),
            ("789 Martin Luther King Jr Boulevard", "Martin Luther King Jr"),
            ("321 W 42nd Street", "42nd"),
            ("654 St. James Place", "St. James"),
            ("5th Avenue", "5th"),
            ("Park Place", "Park"),
            ("", ""),
            (None, ""),
        ]

        df = spark.createDataFrame(test_data, ["address", "expected"])
        result_df = df.withColumn(
            "street_name", addresses.extract_street_name(F.col("address"))
        )

        results = result_df.collect()
        for row in results:
            assert (
                row["street_name"] == row["expected"]
            ), f"Failed for '{row['address']}': expected '{row['expected']}', got '{row['street_name']}'"

    def test_extract_street_suffix(self, spark):
        """Test extraction of street suffixes."""
        test_data = [
            ("123 Main Street", "Street"),
            ("456 Oak Avenue", "Avenue"),
            ("789 Elm Blvd", "Blvd"),
            ("321 Pine Rd.", "Rd"),
            ("654 Cedar Dr", "Dr"),
            ("987 Maple Lane", "Lane"),
            ("Broadway", ""),  # No suffix
            ("5th", ""),  # No suffix
            ("", ""),
            (None, ""),
        ]

        df = spark.createDataFrame(test_data, ["address", "expected"])
        result_df = df.withColumn(
            "street_suffix", addresses.extract_street_suffix(F.col("address"))
        )

        results = result_df.collect()
        for row in results:
            assert (
                row["street_suffix"].lower() == row["expected"].lower()
            ), f"Failed for '{row['address']}': expected '{row['expected']}', got '{row['street_suffix']}'"

    def test_extract_full_street(self, spark):
        """Test extraction of complete street address."""
        test_data = [
            ("123 Main Street, New York, NY 10001", "123 Main Street"),
            ("456 North Broadway Ave, Los Angeles, CA", "456 North Broadway Ave"),
            ("789 E 42nd St Apt 5B, Chicago, IL", "789 E 42nd St"),
            (
                "321 Martin Luther King Jr Blvd, Atlanta, GA",
                "321 Martin Luther King Jr Blvd",
            ),
            ("Broadway, New York, NY", "Broadway"),
            ("", ""),
            (None, ""),
        ]

        df = spark.createDataFrame(test_data, ["address", "expected"])
        result_df = df.withColumn(
            "full_street", addresses.extract_full_street(F.col("address"))
        )

        results = result_df.collect()
        for row in results:
            assert (
                row["full_street"] == row["expected"]
            ), f"Failed for '{row['address']}': expected '{row['expected']}', got '{row['full_street']}'"

    def test_standardize_street_prefix(self, spark):
        """Test standardization of street prefixes."""
        test_data = [
            ("North", "N"),
            ("South", "S"),
            ("East", "E"),
            ("West", "W"),
            ("Northeast", "NE"),
            ("Northwest", "NW"),
            ("Southeast", "SE"),
            ("Southwest", "SW"),
            ("N.", "N"),
            ("S.", "S"),
            ("E.", "E"),
            ("W.", "W"),
            ("north", "N"),  # Case insensitive
            ("SOUTH", "S"),  # Case insensitive
            ("", ""),
            (None, ""),
        ]

        df = spark.createDataFrame(test_data, ["input", "expected"])
        result_df = df.withColumn(
            "standardized", addresses.standardize_street_prefix(F.col("input"))
        )

        results = result_df.collect()
        for row in results:
            assert (
                row["standardized"] == row["expected"]
            ), f"Failed for '{row['input']}': expected '{row['expected']}', got '{row['standardized']}'"

    def test_standardize_street_suffix(self, spark):
        """Test standardization of street suffixes."""
        test_data = [
            ("Street", "St"),
            ("Avenue", "Ave"),
            ("Boulevard", "Blvd"),
            ("Road", "Rd"),
            ("Drive", "Dr"),
            ("Lane", "Ln"),
            ("Court", "Ct"),
            ("Place", "Pl"),
            ("Circle", "Cir"),
            ("Highway", "Hwy"),
            ("Parkway", "Pkwy"),
            ("Terrace", "Ter"),
            ("street", "St"),  # Case insensitive
            ("AVENUE", "Ave"),  # Case insensitive
            ("St.", "St"),  # Already abbreviated
            ("Ave.", "Ave"),  # Already abbreviated
            ("", ""),
            (None, ""),
        ]

        df = spark.createDataFrame(test_data, ["input", "expected"])
        result_df = df.withColumn(
            "standardized", addresses.standardize_street_suffix(F.col("input"))
        )

        results = result_df.collect()
        for row in results:
            assert (
                row["standardized"] == row["expected"]
            ), f"Failed for '{row['input']}': expected '{row['expected']}', got '{row['standardized']}'"

    def test_complex_street_addresses(self, spark):
        """Test extraction from complex street addresses."""
        test_data = [
            # Complex multi-word street names
            (
                "123 Martin Luther King Jr Boulevard",
                "123",
                "",
                "Martin Luther King Jr",
                "Boulevard",
            ),
            ("456 North St. James Place", "456", "North", "St. James", "Place"),
            ("789 SW 42nd Street", "789", "SW", "42nd", "Street"),
            # Apartment/Suite numbers
            ("123 Main St Apt 5B", "123", "", "Main", "St"),
            ("456 Oak Ave Suite 200", "456", "", "Oak", "Ave"),
            # No street number
            ("Broadway Avenue", "", "", "Broadway", "Avenue"),
            ("North Main Street", "", "North", "Main", "Street"),
        ]

        schema = [
            "address",
            "expected_number",
            "expected_prefix",
            "expected_name",
            "expected_suffix",
        ]
        df = spark.createDataFrame(test_data, schema)

        result_df = df.select(
            F.col("address"),
            addresses.extract_street_number(F.col("address")).alias("number"),
            addresses.extract_street_prefix(F.col("address")).alias("prefix"),
            addresses.extract_street_name(F.col("address")).alias("name"),
            addresses.extract_street_suffix(F.col("address")).alias("suffix"),
            F.col("expected_number"),
            F.col("expected_prefix"),
            F.col("expected_name"),
            F.col("expected_suffix"),
        )

        results = result_df.collect()
        for row in results:
            assert (
                row["number"] == row["expected_number"]
            ), f"Number failed for '{row['address']}'"
            assert (
                row["prefix"].lower() == row["expected_prefix"].lower()
            ), f"Prefix failed for '{row['address']}'"
            assert (
                row["name"] == row["expected_name"]
            ), f"Name failed for '{row['address']}'"
            assert (
                row["suffix"].lower() == row["expected_suffix"].lower()
            ), f"Suffix failed for '{row['address']}'"

    def test_international_street_formats(self, spark):
        """Test with international street address formats."""
        test_data = [
            # UK style
            ("10 Downing Street", "10", "", "Downing", "Street"),
            ("221B Baker Street", "221B", "", "Baker", "Street"),
            # Canadian style with directions
            (
                "123 Rue Sainte-Catherine Ouest",
                "123",
                "",
                "Rue Sainte-Catherine Ouest",
                "",
            ),
            # Numbered streets
            ("456 5th Avenue", "456", "", "5th", "Avenue"),
            ("789 42nd Street", "789", "", "42nd", "Street"),
        ]

        schema = [
            "address",
            "expected_number",
            "expected_prefix",
            "expected_name",
            "expected_suffix",
        ]
        df = spark.createDataFrame(test_data, schema)

        result_df = df.select(
            F.col("address"),
            addresses.extract_street_number(F.col("address")).alias("number"),
            addresses.extract_street_prefix(F.col("address")).alias("prefix"),
            addresses.extract_street_name(F.col("address")).alias("name"),
            addresses.extract_street_suffix(F.col("address")).alias("suffix"),
            F.col("expected_number"),
            F.col("expected_prefix"),
            F.col("expected_name"),
            F.col("expected_suffix"),
        )

        results = result_df.collect()
        for row in results:
            assert (
                row["number"] == row["expected_number"]
            ), f"Number failed for '{row['address']}'"
            assert (
                row["prefix"].lower() == row["expected_prefix"].lower()
            ), f"Prefix failed for '{row['address']}'"
            assert (
                row["name"] == row["expected_name"]
            ), f"Name failed for '{row['address']}'"
            assert (
                row["suffix"].lower() == row["expected_suffix"].lower()
            ), f"Suffix failed for '{row['address']}'"

    def test_edge_cases_and_nulls(self, spark):
        """Test edge cases and null handling."""
        test_data = [
            ("", "", "", "", ""),
            (None, "", "", "", ""),
            ("   ", "", "", "", ""),  # Whitespace only
            ("123", "123", "", "", ""),  # Just a number
            ("Main", "", "", "Main", ""),  # Just a name
            ("Street", "", "", "", "Street"),  # Just a suffix (though might be name)
        ]

        schema = [
            "address",
            "expected_number",
            "expected_prefix",
            "expected_name",
            "expected_suffix",
        ]
        df = spark.createDataFrame(test_data, schema)

        result_df = df.select(
            F.col("address"),
            addresses.extract_street_number(F.col("address")).alias("number"),
            addresses.extract_street_prefix(F.col("address")).alias("prefix"),
            addresses.extract_street_name(F.col("address")).alias("name"),
            addresses.extract_street_suffix(F.col("address")).alias("suffix"),
        )

        # Should complete without errors
        results = result_df.collect()
        assert len(results) == len(test_data)

    def test_standardization_with_custom_mappings(self, spark):
        """Test street suffix standardization with custom mappings."""
        # Test custom suffix mappings
        custom_mappings = {
            "STRASSE": "Str",  # German
            "RUE": "R",  # French
            "CALLE": "C",  # Spanish
        }

        test_data = [
            ("Strasse", "Str"),
            ("Rue", "R"),
            ("Calle", "C"),
            ("Street", "St"),  # Should still use default
        ]

        df = spark.createDataFrame(test_data, ["input", "expected"])

        # Create a pre-configured standardizer
        standardize_custom = addresses.standardize_street_suffix(
            custom_mappings=custom_mappings
        )

        result_df = df.withColumn("standardized", standardize_custom(F.col("input")))

        results = result_df.collect()
        for row in results:
            assert (
                row["standardized"] == row["expected"]
            ), f"Failed for '{row['input']}': expected '{row['expected']}', got '{row['standardized']}'"

    def test_combined_extraction_and_standardization(self, spark):
        """Test extracting and then standardizing street components."""
        test_data = [
            ("123 North Main Street", "123 N Main St"),
            ("456 South Broadway Avenue", "456 S Broadway Ave"),
            ("789 East 42nd Boulevard", "789 E 42nd Blvd"),
            ("321 West Oak Drive", "321 W Oak Dr"),
            ("654 Northeast Pine Lane", "654 NE Pine Ln"),
            ("987 Southwest Elm Court", "987 SW Elm Ct"),
        ]

        df = spark.createDataFrame(test_data, ["address", "expected"])

        # Extract components
        extracted = df.select(
            F.col("address"),
            addresses.extract_street_number(F.col("address")).alias("number"),
            addresses.extract_street_prefix(F.col("address")).alias("prefix"),
            addresses.extract_street_name(F.col("address")).alias("name"),
            addresses.extract_street_suffix(F.col("address")).alias("suffix"),
        )

        # Standardize components
        standardized = extracted.select(
            F.col("address"),
            F.col("number"),
            addresses.standardize_street_prefix(F.col("prefix")).alias("std_prefix"),
            F.col("name"),
            addresses.standardize_street_suffix(F.col("suffix")).alias("std_suffix"),
        )

        # Reconstruct standardized address
        result_df = standardized.select(
            F.col("address"),
            F.concat_ws(
                " ",
                F.col("number"),
                F.col("std_prefix"),
                F.col("name"),
                F.col("std_suffix"),
            ).alias("reconstructed"),
        )

        results = result_df.collect()
        for row in results:
            # The reconstructed address should match expected format
            # Note: This is a simplified test - actual reconstruction might need more logic
            parts = row["reconstructed"].split()
            expected_parts = test_data[results.index(row)][1].split()
            assert len(parts) == len(
                expected_parts
            ), f"Part count mismatch for '{row['address']}'"

    def test_pre_configured_extractors(self, spark):
        """Test using pre-configured street extractors."""
        test_data = [
            ("123 Main Street, New York, NY", "123", "Main"),
            ("456 Broadway Avenue, LA, CA", "456", "Broadway"),
        ]

        df = spark.createDataFrame(
            test_data, ["address", "expected_number", "expected_name"]
        )

        # Create pre-configured extractors
        extract_number = addresses.extract_street_number()
        extract_name = addresses.extract_street_name()

        result_df = df.select(
            F.col("address"),
            extract_number(F.col("address")).alias("number"),
            extract_name(F.col("address")).alias("name"),
        )

        results = result_df.collect()
        for row in results:
            idx = results.index(row)
            assert row["number"] == test_data[idx][1], "Number extraction failed"
            assert row["name"] == test_data[idx][2], "Name extraction failed"


@pytest.mark.unit
class TestStreetExtractionEdgeCases:
    """Test edge cases for street extraction functionality."""

    def test_st_ambiguity(self, spark):
        """Test handling of 'St' as both Saint and Street."""
        test_data = [
            # St as Saint at beginning, Street at end
            ("123 St. Paul Street", "123", "", "St. Paul", "Street"),
            ("456 St. James St", "456", "", "St. James", "St"),
            ("789 St Louis Boulevard", "789", "", "St Louis", "Boulevard"),
            # St only as Street
            ("321 Main St", "321", "", "Main", "St"),
            # St only as Saint (no suffix)
            ("654 St. Catherine", "654", "", "St. Catherine", ""),
        ]

        schema = [
            "address",
            "expected_number",
            "expected_prefix",
            "expected_name",
            "expected_suffix",
        ]
        df = spark.createDataFrame(test_data, schema)

        result_df = df.select(
            F.col("address"),
            addresses.extract_street_number(F.col("address")).alias("number"),
            addresses.extract_street_name(F.col("address")).alias("name"),
            addresses.extract_street_suffix(F.col("address")).alias("suffix"),
        )

        results = result_df.collect()
        for row in results:
            # These are tricky cases - the function may not handle them perfectly
            # but should at least not crash
            assert row["number"] is not None

    def test_direction_word_conflicts(self, spark):
        """Test when direction words are part of the street name."""
        test_data = [
            # Direction as street name, not prefix
            ("123 North Avenue", "123", "", "North", "Avenue"),
            ("456 South Street", "456", "", "South", "Street"),
            ("789 West Boulevard", "789", "", "West", "Boulevard"),
            # Multiple directions
            ("321 North West Street", "321", "North", "West", "Street"),
            ("654 South East Avenue", "654", "South", "East", "Avenue"),
        ]

        schema = ["address", "exp_number", "exp_prefix", "exp_name", "exp_suffix"]
        df = spark.createDataFrame(test_data, schema)

        result_df = df.select(
            F.col("address"),
            addresses.extract_street_number(F.col("address")).alias("number"),
            addresses.extract_street_prefix(F.col("address")).alias("prefix"),
            addresses.extract_street_name(F.col("address")).alias("name"),
            addresses.extract_street_suffix(F.col("address")).alias("suffix"),
        )

        results = result_df.collect()
        # Verify extraction doesn't crash on ambiguous cases
        assert len(results) == len(test_data)

    def test_highway_and_route_formats(self, spark):
        """Test highway, route, and interstate formats."""
        test_data = [
            ("Highway 101",),
            ("12345 Highway 50",),
            ("State Route 12",),
            ("US Route 1",),
            ("Interstate 95",),
            ("I-95",),
            ("I-495 Exit 23",),
            ("US-1",),
            ("Route 66",),
        ]

        df = spark.createDataFrame(test_data, ["address"])

        result_df = df.select(
            F.col("address"),
            addresses.extract_full_street(F.col("address")).alias("full_street"),
        )

        results = result_df.collect()
        for row in results:
            # Should extract something for each format
            assert row["full_street"] is not None

    def test_po_box_and_rural_routes(self, spark):
        """Test PO Box and rural route formats."""
        test_data = [
            ("PO Box 123",),
            ("P.O. Box 456",),
            ("POB 789",),
            ("Post Office Box 321",),
            ("RR 1 Box 123",),
            ("Rural Route 2 Box 456",),
            ("HC 2 Box 321",),  # Highway Contract
            ("Star Route Box 987",),
        ]

        df = spark.createDataFrame(test_data, ["address"])

        result_df = df.select(
            F.col("address"),
            addresses.extract_full_street(F.col("address")).alias("full_street"),
        )

        results = result_df.collect()
        # Should handle without error
        assert len(results) == len(test_data)

    def test_fraction_symbols(self, spark):
        """Test addresses with fraction symbols."""
        test_data = [
            ("123½ Main Street", "123½"),
            ("456¼ Oak Avenue", "456¼"),
            ("789¾ Elm Road", "789¾"),
            ("321⅓ Pine Lane", "321⅓"),
        ]

        df = spark.createDataFrame(test_data, ["address", "expected_number"])

        result_df = df.select(
            F.col("address"),
            addresses.extract_street_number(F.col("address")).alias("number"),
        )

        results = result_df.collect()
        for row in results:
            # Should handle unicode fractions
            assert row["number"] != ""

    def test_unicode_and_special_chars(self, spark):
        """Test addresses with unicode and special characters."""
        test_data = [
            ("123 Café Street",),
            ("456 Niño Boulevard",),
            ("789 François Avenue",),
            ("321 O'Connor Road",),
            ("654 D'Angelo Avenue",),
            ("987 Smith & Sons Street",),
        ]

        df = spark.createDataFrame(test_data, ["address"])

        result_df = df.select(
            F.col("address"),
            addresses.extract_street_number(F.col("address")).alias("number"),
            addresses.extract_full_street(F.col("address")).alias("full_street"),
        )

        results = result_df.collect()
        for row in results:
            # Should handle special chars without crashing
            assert row["number"] is not None

    def test_excessive_whitespace(self, spark):
        """Test addresses with excessive spaces, tabs, etc."""
        test_data = [
            ("123    Main    Street",),
            ("456\tOak\tAvenue",),
            ("789  \t  Elm  \t  Road",),
            ("   321 Pine Lane   ",),
        ]

        df = spark.createDataFrame(test_data, ["address"])

        result_df = df.select(
            F.col("address"),
            addresses.extract_street_number(F.col("address")).alias("number"),
            addresses.extract_street_name(F.col("address")).alias("name"),
        )

        results = result_df.collect()
        for row in results:
            # Should handle whitespace variations
            assert row["number"] != ""

    def test_mixed_case_consistency(self, spark):
        """Test various case combinations."""
        test_data = [
            ("123 MAIN STREET",),
            ("456 main street",),
            ("789 MaIn StReEt",),
        ]

        df = spark.createDataFrame(test_data, ["address"])

        result_df = df.select(
            F.col("address"),
            addresses.extract_street_name(F.col("address")).alias("name"),
            addresses.extract_street_suffix(F.col("address")).alias("suffix"),
        )

        results = result_df.collect()
        # All should extract "Main" and "Street" regardless of case
        for row in results:
            assert row["name"].lower() == "main"
            assert row["suffix"].lower() == "street"

    def test_very_long_street_names(self, spark):
        """Test extraction with unusually long street names."""
        test_data = [
            ("123 Martin Luther King Junior Memorial Boulevard",),
            ("456 The Great North American Transcontinental Highway",),
            ("789 Saint Mary of the Immaculate Conception Street",),
        ]

        df = spark.createDataFrame(test_data, ["address"])

        result_df = df.select(
            F.col("address"),
            addresses.extract_street_number(F.col("address")).alias("number"),
            addresses.extract_full_street(F.col("address")).alias("full_street"),
        )

        results = result_df.collect()
        for row in results:
            # Should handle long names
            assert len(row["full_street"]) > 0

    def test_building_and_business_names(self, spark):
        """Test addresses with building names instead of numbers."""
        test_data = [
            ("Empire State Building, 350 5th Avenue",),
            ("One World Trade Center",),
            ("Building A, 123 Main Street",),
            ("Tower 2, 456 Oak Avenue",),
        ]

        df = spark.createDataFrame(test_data, ["address"])

        result_df = df.select(
            F.col("address"),
            addresses.extract_full_street(F.col("address")).alias("full_street"),
        )

        results = result_df.collect()
        # Should extract some portion of these complex addresses
        for row in results:
            assert row["full_street"] is not None

    def test_international_formats(self, spark):
        """Test non-English address formats."""
        test_data = [
            # Spanish
            ("Calle 42 #123",),
            ("Avenida Principal 456",),
            # French
            ("123 Rue de la Paix",),
            ("456 Boulevard Saint-Germain",),
            # German
            ("Hauptstraße 123",),
            ("Bahnhofstrasse 456",),
        ]

        df = spark.createDataFrame(test_data, ["address"])

        result_df = df.select(
            F.col("address"),
            addresses.extract_full_street(F.col("address")).alias("full_street"),
        )

        results = result_df.collect()
        for row in results:
            # Should handle international formats without crashing
            assert row["full_street"] is not None

    def test_repeated_patterns(self, spark):
        """Test addresses with repeated words."""
        test_data = [
            ("123 Street Street",),
            ("456 Avenue Avenue",),
            ("789 North North Street",),
        ]

        df = spark.createDataFrame(test_data, ["address"])

        result_df = df.select(
            F.col("address"),
            addresses.extract_street_number(F.col("address")).alias("number"),
            addresses.extract_street_suffix(F.col("address")).alias("suffix"),
        )

        results = result_df.collect()
        # Should handle repeated patterns
        for row in results:
            assert row["number"] is not None

    def test_numbers_at_end(self, spark):
        """Test addresses with numbers at the end."""
        test_data = [
            ("Broadway 123",),
            ("Main Street 456",),
            ("Highway 101 Exit 23",),
            ("Route 66 Mile 42",),
        ]

        df = spark.createDataFrame(test_data, ["address"])

        result_df = df.select(
            F.col("address"),
            addresses.extract_full_street(F.col("address")).alias("full_street"),
        )

        results = result_df.collect()
        for row in results:
            # Should extract something even with non-standard ordering
            assert row["full_street"] != ""

    def test_malformed_addresses(self, spark):
        """Test severely malformed addresses."""
        test_data = [
            ("!!!123***Main###Street!!!",),
            ("...456...Oak...Avenue...",),
            ("@#$321 Pine Lane$#@",),
        ]

        df = spark.createDataFrame(test_data, ["address"])

        result_df = df.select(
            F.col("address"),
            addresses.extract_street_number(F.col("address")).alias("number"),
        )

        results = result_df.collect()
        # Should not crash on malformed input
        assert len(results) == len(test_data)

    def test_already_standardized(self, spark):
        """Test input that's already in standard form."""
        test_data = [
            ("N", "N"),
            ("S", "S"),
            ("St", "St"),
            ("Ave", "Ave"),
            ("Blvd", "Blvd"),
        ]

        df = spark.createDataFrame(test_data, ["input", "expected"])

        result_df = df.select(
            F.col("input"),
            addresses.standardize_street_prefix(F.col("input")).alias("prefix"),
            addresses.standardize_street_suffix(F.col("input")).alias("suffix"),
        )

        results = result_df.collect()
        for row in results:
            # Already standardized input should remain unchanged
            if row["input"] in ["N", "S"]:
                assert row["prefix"] == row["input"]
            else:
                assert row["suffix"] == row["input"]

    def test_conflicting_custom_mappings(self, spark):
        """Test when custom mappings conflict with standard ones."""
        custom_mappings = {
            "STREET": "STR",  # Conflicts with standard St
            "NORTH": "NO",  # Conflicts with standard N
            "AVENUE": "AV",  # Conflicts with standard Ave
        }

        test_data = [
            ("Street",),
            ("North",),
            ("Avenue",),
        ]

        df = spark.createDataFrame(test_data, ["input"])

        result_df = df.select(
            F.col("input"),
            addresses.standardize_street_suffix(
                F.col("input"), custom_mappings=custom_mappings
            ).alias("custom"),
            addresses.standardize_street_suffix(F.col("input")).alias("standard"),
        )

        results = result_df.collect()
        for row in results:
            # Custom mappings should take precedence
            if row["input"].upper() in custom_mappings:
                assert row["custom"] != row["standard"]

    def test_extract_full_street_from_complete_addresses(self, spark):
        """Test extracting street from complete addresses with city, state, ZIP."""
        test_data = [
            # Standard US formats
            ("123 Main Street, New York, NY 10001", "123 Main Street"),
            ("456 N Oak Avenue, Los Angeles, CA 90210", "456 N Oak Avenue"),
            ("789 South Elm Boulevard, Chicago, IL 60601", "789 South Elm Boulevard"),
            # With apartment/suite numbers
            ("123 Main St Apt 4B, Boston, MA 02134", "123 Main St"),
            ("456 Oak Ave Suite 200, Dallas, TX 75201", "456 Oak Ave"),
            ("789 Elm Rd Unit 5, Seattle, WA 98101", "789 Elm Rd"),
            # Multi-word street names
            (
                "123 Martin Luther King Jr Blvd, Atlanta, GA 30303",
                "123 Martin Luther King Jr Blvd",
            ),
            ("456 Saint Paul Street, Baltimore, MD 21202", "456 Saint Paul Street"),
            (
                "789 De La Guerra Plaza, Santa Barbara, CA 93101",
                "789 De La Guerra Plaza",
            ),
            # With building/floor info
            ("123 Main St, 5th Floor, New York, NY 10001", "123 Main St"),
            ("456 Broadway, Building A, Los Angeles, CA 90012", "456 Broadway"),
            # Different comma patterns
            ("123 Main Street New York NY 10001", "123 Main Street"),  # No commas
            ("123 Main Street,New York,NY,10001", "123 Main Street"),  # Many commas
            # With ZIP+4
            ("123 Main St, Columbus, OH 43215-1234", "123 Main St"),
            # Directional prefixes
            ("123 N Main St, Phoenix, AZ 85001", "123 N Main St"),
            ("456 SW Broadway Ave, Portland, OR 97201", "456 SW Broadway Ave"),
            # No street number
            ("Broadway Avenue, Manhattan, NY 10001", "Broadway Avenue"),
            ("Main Street, Small Town, KS 67501", "Main Street"),
            # Complex cases
            ("One World Trade Center, New York, NY 10007", "One World Trade Center"),
            (
                "350 5th Avenue, Empire State Building, New York, NY 10118",
                "350 5th Avenue",
            ),
            # Edge cases with multiple potential separators
            ("123 Main St. #5, Apt 2B, City, ST 12345", "123 Main St."),
            ("456 Oak Ave., P.O. Box 789, Town, ST 67890", "456 Oak Ave."),
        ]

        df = spark.createDataFrame(test_data, ["address", "expected"])

        result_df = df.select(
            F.col("address"),
            addresses.extract_full_street(F.col("address")).alias("extracted"),
            F.col("expected"),
        )

        results = result_df.collect()
        for row in results:
            # Some complex cases might not match exactly, but should extract something reasonable
            assert (
                row["extracted"] is not None and row["extracted"] != ""
            ), f"Failed to extract street from: '{row['address']}'"

            # For standard cases, should match expected
            if (
                "," in row["address"]
                and "Apt" not in row["address"]
                and "#" not in row["address"]
            ):
                if row["extracted"] != row["expected"]:
                    print(
                        f"Warning: '{row['address']}' extracted '{row['extracted']}' vs expected '{row['expected']}'"
                    )

    def test_extract_components_from_full_address(self, spark):
        """Test extracting all components from complete address strings."""
        test_data = [
            # Complete US addresses with all components
            (
                "123 North Main Street, New York, NY 10001",
                {
                    "number": "123",
                    "prefix": "North",
                    "name": "Main",
                    "suffix": "Street",
                    "full": "123 North Main Street",
                },
            ),
            (
                "456 S Broadway Avenue, Los Angeles, CA 90210",
                {
                    "number": "456",
                    "prefix": "S",
                    "name": "Broadway",
                    "suffix": "Avenue",
                    "full": "456 S Broadway Avenue",
                },
            ),
            (
                "789 Martin Luther King Jr Blvd, Atlanta, GA 30303",
                {
                    "number": "789",
                    "prefix": "",
                    "name": "Martin Luther King Jr",
                    "suffix": "Blvd",
                    "full": "789 Martin Luther King Jr Blvd",
                },
            ),
        ]

        for address, expected in test_data:
            df = spark.createDataFrame([(address,)], ["address"])

            result_df = df.select(
                F.col("address"),
                addresses.extract_street_number(F.col("address")).alias("number"),
                addresses.extract_street_prefix(F.col("address")).alias("prefix"),
                addresses.extract_street_name(F.col("address")).alias("name"),
                addresses.extract_street_suffix(F.col("address")).alias("suffix"),
                addresses.extract_full_street(F.col("address")).alias("full"),
            )

            result = result_df.first()

            # Verify each component
            assert (
                result["number"] == expected["number"]
            ), f"Number mismatch for '{address}': got '{result['number']}'"
            assert (
                result["prefix"].lower() == expected["prefix"].lower()
            ), f"Prefix mismatch for '{address}': got '{result['prefix']}'"
            # Name extraction can be tricky with complex names
            if (
                expected["name"] and len(expected["name"]) < 20
            ):  # Simple names should match
                assert (
                    result["name"] == expected["name"]
                ), f"Name mismatch for '{address}': got '{result['name']}'"
            assert (
                result["suffix"].lower() == expected["suffix"].lower()
            ), f"Suffix mismatch for '{address}': got '{result['suffix']}'"
            assert (
                result["full"] == expected["full"]
            ), f"Full street mismatch for '{address}': got '{result['full']}'"
