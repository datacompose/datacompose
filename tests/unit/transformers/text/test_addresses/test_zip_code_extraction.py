"""
Comprehensive tests for ZIP code extraction functionality.
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

# Import test data
from tests.unit.transformers.text.test_addresses.test_data_addresses import (
    BOUNDARY_ZIP_CODES,
    INTERNATIONAL_POSTAL_CODES,
    INVALID_ZIP_CODES,
    NULL_HANDLING,
    SPECIAL_CASES,
    UNICODE_SPECIAL_CHARS,
    VALID_ZIP_CODES,
    ZIP_CODES_IN_TEXT,
    generate_performance_test_data,
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
        SparkSession.builder.appName("ZipCodeExtractionTests")
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


@pytest.fixture
def valid_zip_codes_df(spark):
    """Create DataFrame with valid ZIP code formats."""
    schema = StructType(
        [
            StructField("input", StringType(), True),
            StructField("expected", StringType(), True),
        ]
    )
    return spark.createDataFrame(VALID_ZIP_CODES, schema)


@pytest.fixture
def invalid_zip_codes_df(spark):
    """Create DataFrame with invalid ZIP code formats."""
    schema = StructType(
        [
            StructField("input", StringType(), True),
            StructField("expected", StringType(), True),
        ]
    )
    return spark.createDataFrame(INVALID_ZIP_CODES, schema)


@pytest.fixture
def zip_codes_in_text_df(spark):
    """Create DataFrame with ZIP codes embedded in text."""
    schema = StructType(
        [
            StructField("text", StringType(), True),
            StructField("expected", StringType(), True),
        ]
    )
    return spark.createDataFrame(ZIP_CODES_IN_TEXT, schema)


@pytest.fixture
def special_cases_df(spark):
    """Create DataFrame with special ZIP code cases."""
    schema = StructType(
        [
            StructField("input", StringType(), True),
            StructField("expected", StringType(), True),
        ]
    )
    return spark.createDataFrame(SPECIAL_CASES, schema)


@pytest.fixture
def international_postal_codes_df(spark):
    """Create DataFrame with international postal codes."""
    schema = StructType(
        [
            StructField("input", StringType(), True),
            StructField("expected", StringType(), True),
        ]
    )
    return spark.createDataFrame(INTERNATIONAL_POSTAL_CODES, schema)


@pytest.fixture
def boundary_zip_codes_df(spark):
    """Create DataFrame with boundary ZIP codes."""
    schema = StructType(
        [
            StructField("input", StringType(), True),
            StructField("expected", StringType(), True),
        ]
    )
    return spark.createDataFrame(BOUNDARY_ZIP_CODES, schema)


@pytest.fixture
def unicode_special_chars_df(spark):
    """Create DataFrame with Unicode and special characters."""
    schema = StructType(
        [
            StructField("input", StringType(), True),
            StructField("expected", StringType(), True),
        ]
    )
    return spark.createDataFrame(UNICODE_SPECIAL_CHARS, schema)


@pytest.fixture
def null_handling_df(spark):
    """Create DataFrame for null handling tests."""
    schema = StructType(
        [
            StructField("input", StringType(), True),
            StructField("expected", StringType(), True),
        ]
    )
    return spark.createDataFrame(NULL_HANDLING, schema)


@pytest.fixture
def performance_test_df(spark):
    """Create large DataFrame for performance testing."""
    schema = StructType(
        [StructField("id", StringType(), True), StructField("text", StringType(), True)]
    )
    return spark.createDataFrame(generate_performance_test_data(10000), schema)


@pytest.mark.unit
class TestZipCodeExtraction:
    """Comprehensive tests for ZIP code extraction functionality."""

    def test_valid_zip_codes(self, valid_zip_codes_df):
        """Test extraction of valid ZIP code formats."""
        from datacompose.transformers.text.clean_addresses.pyspark.pyspark_primitives import (
            extract_zip_code,
        )

        result_df = valid_zip_codes_df.withColumn(
            "extracted", extract_zip_code(F.col("input"))
        )

        results = result_df.collect()

        for row in results:
            assert (
                row["extracted"] == row["expected"]
            ), f"Failed for input '{row['input']}': expected '{row['expected']}', got '{row['extracted']}'"

    def test_invalid_zip_codes(self, invalid_zip_codes_df):
        """Test handling of invalid ZIP code formats."""
        from datacompose.transformers.text.clean_addresses.pyspark.pyspark_primitives import (
            extract_zip_code,
        )

        result_df = invalid_zip_codes_df.withColumn(
            "extracted", extract_zip_code(F.col("input"))
        )

        results = result_df.collect()

        for row in results:
            assert (
                row["extracted"] == row["expected"]
            ), f"Failed for input '{row['input']}': expected '{row['expected']}', got '{row['extracted']}'"

    def test_zip_codes_in_text(self, zip_codes_in_text_df):
        """Test extraction of ZIP codes from text strings."""
        from datacompose.transformers.text.clean_addresses.pyspark.pyspark_primitives import (
            extract_zip_code,
        )

        result_df = zip_codes_in_text_df.withColumn(
            "extracted", extract_zip_code(F.col("text"))
        )

        results = result_df.collect()

        for row in results:
            assert (
                row["extracted"] == row["expected"]
            ), f"Failed for text '{row['text']}': expected '{row['expected']}', got '{row['extracted']}'"

    def test_special_cases(self, special_cases_df):
        """Test special ZIP code cases and formats."""
        from datacompose.transformers.text.clean_addresses.pyspark.pyspark_primitives import (
            extract_zip_code,
        )

        result_df = special_cases_df.withColumn(
            "extracted", extract_zip_code(F.col("input"))
        )

        results = result_df.collect()

        for row in results:
            assert (
                row["extracted"] == row["expected"]
            ), f"Failed for input '{row['input']}': expected '{row['expected']}', got '{row['extracted']}'"

    def test_international_postal_codes(self, international_postal_codes_df):
        """Test that international postal codes are handled correctly."""
        from datacompose.transformers.text.clean_addresses.pyspark.pyspark_primitives import (
            extract_zip_code,
        )

        result_df = international_postal_codes_df.withColumn(
            "extracted", extract_zip_code(F.col("input"))
        )

        results = result_df.collect()

        for row in results:
            assert (
                row["extracted"] == row["expected"]
            ), f"Failed for '{row['input']}': expected '{row['expected']}', got '{row['extracted']}'"

    def test_boundary_zip_codes(self, boundary_zip_codes_df):
        """Test ZIP codes at the boundaries of valid ranges."""
        from datacompose.transformers.text.clean_addresses.pyspark.pyspark_primitives import (
            extract_zip_code,
        )

        result_df = boundary_zip_codes_df.withColumn(
            "extracted", extract_zip_code(F.col("input"))
        )

        results = result_df.collect()

        for row in results:
            assert (
                row["extracted"] == row["expected"]
            ), f"Failed for '{row['input']}': expected '{row['expected']}', got '{row['extracted']}'"

    def test_unicode_and_special_chars(self, unicode_special_chars_df):
        """Test ZIP code extraction with Unicode and special characters."""
        from datacompose.transformers.text.clean_addresses.pyspark.pyspark_primitives import (
            extract_zip_code,
        )

        result_df = unicode_special_chars_df.withColumn(
            "extracted", extract_zip_code(F.col("input"))
        )

        results = result_df.collect()

        for row in results:
            assert (
                row["extracted"] == row["expected"]
            ), f"Failed for '{row['input']}': expected '{row['expected']}', got '{row['extracted']}'"

    def test_null_handling(self, null_handling_df):
        """Test proper handling of null values."""
        from datacompose.transformers.text.clean_addresses.pyspark.pyspark_primitives import (
            extract_zip_code,
        )

        result_df = null_handling_df.withColumn(
            "extracted", extract_zip_code(F.col("input"))
        )

        results = result_df.collect()

        for row in results:
            assert row["extracted"] == row["expected"]

        # Verify no nulls in output (should be empty strings)
        null_count = result_df.filter(F.col("extracted").isNull()).count()
        assert null_count == 0

    def test_column_operations(self, spark, valid_zip_codes_df):
        """Test ZIP code extraction with various column operations."""
        from datacompose.transformers.text.clean_addresses.pyspark.pyspark_primitives import (
            extract_zip_code,
        )

        # Test with column alias
        result_df = valid_zip_codes_df.select(
            F.col("input").alias("zip_input"),
            extract_zip_code(F.col("input")).alias("zip_output"),
        )

        assert "zip_input" in result_df.columns
        assert "zip_output" in result_df.columns

        # Test with multiple columns
        df_with_multiple = valid_zip_codes_df.withColumn(
            "secondary", F.lit("Another ZIP: 54321")
        )

        result_df = df_with_multiple.withColumn(
            "primary_zip", extract_zip_code(F.col("input"))
        ).withColumn("secondary_zip", extract_zip_code(F.col("secondary")))

        first_row = result_df.first()
        assert first_row["secondary_zip"] == "54321"

    def test_chained_transformations(self, spark, zip_codes_in_text_df):
        """Test ZIP code extraction in combination with other transformations."""
        from datacompose.transformers.text.clean_addresses.pyspark.pyspark_primitives import (
            extract_zip_code,
        )

        # Chain with upper/lower transformations
        result_df = (
            zip_codes_in_text_df.withColumn("upper_text", F.upper(F.col("text")))
            .withColumn("extracted_from_upper", extract_zip_code(F.col("upper_text")))
            .withColumn("lower_text", F.lower(F.col("text")))
            .withColumn("extracted_from_lower", extract_zip_code(F.col("lower_text")))
        )

        # ZIP extraction should work regardless of case
        results = result_df.collect()
        for row in results:
            assert row["extracted_from_upper"] == row["expected"]
            assert row["extracted_from_lower"] == row["expected"]

    def test_performance(self, performance_test_df):
        """Test performance with large dataset."""
        import time

        from datacompose.transformers.text.clean_addresses.pyspark.pyspark_primitives import (
            extract_zip_code,
        )

        # Warm up
        _ = performance_test_df.withColumn(
            "zip", extract_zip_code(F.col("text"))
        ).count()

        # Measure extraction time
        start_time = time.time()

        result_df = performance_test_df.withColumn(
            "extracted_zip", extract_zip_code(F.col("text"))
        )

        # Force computation
        count = result_df.filter(F.col("extracted_zip") != "").count()

        end_time = time.time()
        elapsed_time = end_time - start_time

        # Should process 10000 rows in reasonable time (< 10 seconds)
        assert elapsed_time < 10, f"Performance test took {elapsed_time:.2f} seconds"

        # Verify some results were extracted
        assert count > 0

    def test_multiple_zip_extraction_precedence(self, spark):
        """Test that first valid ZIP is extracted when multiple are present."""
        from datacompose.transformers.text.clean_addresses.pyspark.pyspark_primitives import (
            extract_zip_code,
        )

        data = [
            ("From 10001 to 90210", "10001"),
            ("90210 before 10001", "90210"),
            ("Text 12345-6789 then 98765", "12345-6789"),
            ("Invalid 123 then valid 12345", "12345"),
            ("12345 90210 88888", "12345"),
        ]

        df = spark.createDataFrame(data, ["text", "expected"])

        result_df = df.withColumn("extracted", extract_zip_code(F.col("text")))

        results = result_df.collect()

        for row in results:
            assert (
                row["extracted"] == row["expected"]
            ), f"Failed for '{row['text']}': expected '{row['expected']}', got '{row['extracted']}'"

    def test_zip_with_different_separators(self, spark):
        """Test ZIP codes with various separator characters."""
        from datacompose.transformers.text.clean_addresses.pyspark.pyspark_primitives import (
            extract_zip_code,
        )

        data = [
            ("12345-6789", "12345-6789"),  # Standard
            ("12345–6789", "12345"),  # En dash (different character)
            ("12345—6789", "12345"),  # Em dash
            ("12345−6789", "12345"),  # Minus sign
            ("12345‐6789", "12345"),  # Hyphen
            ("12345⁃6789", "12345"),  # Hyphen bullet
        ]

        df = spark.createDataFrame(data, ["input", "expected"])

        result_df = df.withColumn("extracted", extract_zip_code(F.col("input")))

        results = result_df.collect()

        for row in results:
            assert (
                row["extracted"] == row["expected"]
            ), f"Failed for '{row['input']}': expected '{row['expected']}', got '{row['extracted']}'"

    def test_extreme_edge_cases(self, spark):
        """Test extreme edge cases that might break the regex."""
        from datacompose.transformers.text.clean_addresses.pyspark.pyspark_primitives import (
            extract_zip_code,
        )

        extreme_cases = [
            # Very long strings
            ("x" * 10000 + " 12345 " + "y" * 10000, "12345"),
            # Many numbers
            (" ".join([str(i) for i in range(1000, 2000)]) + " 12345", "12345"),
            # Repeated patterns
            ("12345 " * 1000, "12345"),
            ("12345-6789 " * 100, "12345-6789"),
            # Near-matches
            ("012345", ""),  # 6 digits
            ("1234", ""),  # 4 digits
            ("12345-", "12345"),  # Incomplete ZIP+4
            ("12345-67", "12345"),  # Incomplete ZIP+4
            ("12345-678", "12345"),  # Incomplete ZIP+4
            ("12345-67890", "12345"),  # Too long extension
            # Special number patterns
            ("00000", "00000"),  # All zeros
            ("99999", "99999"),  # All nines
            ("11111", "11111"),  # All ones
            ("12345", "12345"),  # Sequential
            ("54321", "54321"),  # Reverse sequential
            # With various delimiters
            ("zip:12345", "12345"),
            ("zip=12345", "12345"),
            ("zip 12345", "12345"),
            ("zip\t12345", "12345"),
            ("zip\n12345", "12345"),
            # Empty and null cases
            ("", ""),
            ("   ", ""),
            ("\n\n\n", ""),
            (None, ""),
        ]

        df = spark.createDataFrame(extreme_cases, ["input", "expected"])
        result_df = df.withColumn("extracted", extract_zip_code(F.col("input")))
        results = result_df.collect()

        for row in results:
            assert (
                row["extracted"] == row["expected"]
            ), f"Failed for edge case: expected '{row['expected']}', got '{row['extracted']}'"

    def test_malformed_input_handling(self, spark):
        """Test handling of malformed and malicious inputs."""
        from datacompose.transformers.text.clean_addresses.pyspark.pyspark_primitives import (
            extract_zip_code,
        )

        malformed_cases = [
            # Regex injection attempts
            ("12345(?:x)", "12345"),
            ("12345.*", "12345"),
            ("12345|67890", "12345"),
            ("(12345)", "12345"),
            ("[12345]", "12345"),
            ("{12345}", "12345"),
            # Control characters
            ("12345\x00", "12345"),
            ("\x0012345", "12345"),
            ("12345\r\n", "12345"),
            # Unicode edge cases
            ("12345\u200b", "12345"),  # Zero-width space after
            ("\u200b12345", "12345"),  # Zero-width space before
            ("12\u200b345", ""),  # Zero-width space in middle (breaks number)
            # Mixed scripts
            ("١٢٣٤٥", ""),  # Arabic numerals
            ("१२३४५", ""),  # Devanagari numerals
            ("一二三四五", ""),  # Chinese numerals
            # Extreme whitespace
            (" " * 1000 + "12345" + " " * 1000, "12345"),
            ("\t" * 100 + "12345" + "\n" * 100, "12345"),
            # Binary-like data
            ("\x00\x01\x02\x0312345\x04\x05", "12345"),
            ("\\x3132333435", ""),  # Escaped hex (not actual numbers)
        ]

        df = spark.createDataFrame(malformed_cases, ["input", "expected"])
        result_df = df.withColumn("extracted", extract_zip_code(F.col("input")))
        results = result_df.collect()

        for row in results:
            assert (
                row["extracted"] == row["expected"]
            ), f"Failed for malformed input: expected '{row['expected']}', got '{row['extracted']}'"

    def test_consistency_across_runs(self, spark):
        """Test that results are consistent across multiple runs."""
        from datacompose.transformers.text.clean_addresses.pyspark.pyspark_primitives import (
            extract_zip_code,
        )

        test_data = [
            ("Address 12345 Street", "12345"),
            ("90210-1234 Beverly Hills", "90210-1234"),
            ("Multiple 12345 and 67890", "12345"),
            ("No postal code", ""),
        ]

        df = spark.createDataFrame(test_data, ["input", "expected"])

        # Run multiple times
        results = []
        for _ in range(10):
            result_df = df.withColumn("zip", extract_zip_code(F.col("input")))
            results.append(result_df.collect())

        # All runs should produce identical results
        first_result = results[0]
        for i, result in enumerate(results[1:], 1):
            assert result == first_result, f"Run {i+1} produced different results"

    def test_regex_performance_patterns(self, spark):
        """Test patterns that could cause regex performance issues."""
        import time

        from datacompose.transformers.text.clean_addresses.pyspark.pyspark_primitives import (
            extract_zip_code,
        )

        # Patterns that could cause catastrophic backtracking in poorly written regex
        performance_patterns = [
            # Repeated similar patterns
            (
                "12344 " * 100 + "12345",
                "12344",
            ),  # Many near-matches (gets first valid 5-digit)
            ("1234 " * 100 + "12345", "12345"),  # Many incomplete matches
            ("123456 " * 100 + "12345", "12345"),  # Many too-long matches
            # Nested patterns
            ("(" * 100 + "12345" + ")" * 100, "12345"),
            ("[" * 50 + "12345" + "]" * 50, "12345"),
            # Alternating patterns
            ("1 2 3 4 5 " * 100 + "12345", "12345"),
            ("12 34 56 78 90 " * 100 + "12345", "12345"),
        ]

        for pattern, expected in performance_patterns:
            df = spark.createDataFrame([(pattern,)], ["input"])

            start_time = time.time()
            result_df = df.withColumn("zip", extract_zip_code(F.col("input")))
            result = result_df.first()["zip"]
            elapsed = time.time() - start_time

            assert result == expected
            # Should handle even complex patterns quickly
            assert elapsed < 0.5, f"Pattern took too long: {elapsed:.3f}s"

    def test_boundary_word_detection(self, spark):
        """Test word boundary detection edge cases."""
        from datacompose.transformers.text.clean_addresses.pyspark.pyspark_primitives import (
            extract_zip_code,
        )

        boundary_cases = [
            # Clear word boundaries
            (" 12345 ", "12345"),
            ("(12345)", "12345"),
            ("[12345]", "12345"),
            ("{12345}", "12345"),
            ("'12345'", "12345"),
            ('"12345"', "12345"),
            # Start/end of string
            ("12345", "12345"),
            ("12345 end", "12345"),
            ("start 12345", "12345"),
            # Punctuation boundaries
            ("text,12345", "12345"),
            ("text.12345", "12345"),
            ("text;12345", "12345"),
            ("text:12345", "12345"),
            ("text!12345", "12345"),
            ("text?12345", "12345"),
            # No word boundaries
            ("a12345", ""),
            ("12345b", ""),
            ("a12345b", ""),
            ("_12345", ""),
            ("12345_", ""),
            ("_12345_", ""),
            # Special cases
            (
                "12345-12345",
                "12345",
            ),  # Looks like 10 digits (but extracts first valid 5-digit)
            ("12345x12345", ""),  # Letter in middle
            ("12345 12345", "12345"),  # Two valid ZIPs (gets first)
        ]

        df = spark.createDataFrame(boundary_cases, ["input", "expected"])
        result_df = df.withColumn("extracted", extract_zip_code(F.col("input")))
        results = result_df.collect()

        for row in results:
            assert (
                row["extracted"] == row["expected"]
            ), f"Boundary detection failed for '{row['input']}': expected '{row['expected']}', got '{row['extracted']}'"


@pytest.mark.unit
class TestZipCodeValidation:
    """Test ZIP code validation and utility functions."""

    def test_validate_zip_code_valid_formats(self, spark):
        """Test validation of valid ZIP code formats."""
        from datacompose.transformers.text.clean_addresses.pyspark.pyspark_primitives import (
            validate_zip_code,
        )

        valid_data = [
            ("12345", True),  # Standard 5-digit
            ("00000", True),  # All zeros (valid)
            ("99999", True),  # All nines
            ("12345-6789", True),  # ZIP+4
            ("00501", True),  # Lowest assigned
            ("99950", True),  # Highest assigned
            ("12345-0000", True),  # ZIP+4 with zero extension
            ("12345-9999", True),  # ZIP+4 with max extension
        ]

        df = spark.createDataFrame(valid_data, ["zip", "expected"])
        result_df = df.withColumn("is_valid", validate_zip_code(F.col("zip")))

        results = result_df.collect()
        for row in results:
            assert (
                row["is_valid"] == row["expected"]
            ), f"Validation failed for '{row['zip']}': expected {row['expected']}, got {row['is_valid']}"

    def test_validate_zip_code_invalid_formats(self, spark):
        """Test validation of invalid ZIP code formats."""
        from datacompose.transformers.text.clean_addresses.pyspark.pyspark_primitives import (
            validate_zip_code,
        )

        invalid_data = [
            ("1234", False),  # Too short
            ("123456", False),  # Too long
            ("12345-", False),  # Incomplete ZIP+4
            ("12345-678", False),  # Invalid extension
            ("12345-67890", False),  # Extension too long
            ("abcde", False),  # Letters
            ("12 345", False),  # Space in middle
            ("", False),  # Empty
            (None, False),  # Null
            ("  12345  ", False),  # Extra spaces (not standardized)
            ("12345 6789", False),  # Space instead of dash
            ("12345.6789", False),  # Dot instead of dash
            ("12345_6789", False),  # Underscore instead of dash
            ("ZIP 12345", False),  # Text before
            ("12345 USA", False),  # Text after
        ]

        df = spark.createDataFrame(invalid_data, ["zip", "expected"])
        result_df = df.withColumn("is_valid", validate_zip_code(F.col("zip")))

        results = result_df.collect()
        for row in results:
            assert (
                row["is_valid"] == row["expected"]
            ), f"Validation failed for '{row['zip']}': expected {row['expected']}, got {row['is_valid']}"

    def test_is_valid_zip_code_alias(self, spark):
        """Test that is_valid_zip_code is an alias for validate_zip_code."""
        from datacompose.transformers.text.clean_addresses.pyspark.pyspark_primitives import (
            is_valid_zip_code,
            validate_zip_code,
        )

        test_data = [
            ("12345", True),
            ("invalid", False),
            ("90210-1234", True),
            (None, False),
        ]

        df = spark.createDataFrame(test_data, ["zip", "expected"])

        # Both functions should return identical results
        result_df = df.withColumn(
            "validate", validate_zip_code(F.col("zip"))
        ).withColumn("is_valid", is_valid_zip_code(F.col("zip")))

        results = result_df.collect()
        for row in results:
            assert (
                row["validate"] == row["is_valid"]
            ), f"Alias mismatch for '{row['zip']}': validate={row['validate']}, is_valid={row['is_valid']}"

    def test_standardize_zip_code(self, spark):
        """Test ZIP code standardization."""
        from datacompose.transformers.text.clean_addresses.pyspark.pyspark_primitives import (
            standardize_zip_code,
        )

        test_data = [
            # Valid inputs that should be standardized
            ("12345", "12345"),
            ("90210-1234", "90210-1234"),
            ("  12345  ", "12345"),  # Extracted and standardized
            ("ZIP: 12345", "12345"),  # Extracted from text
            ("12345, USA", "12345"),  # Extracted from text
            # Invalid inputs that should return empty
            ("1234", ""),  # Too short
            ("abcde", ""),  # Letters
            ("", ""),  # Empty
            (None, ""),  # Null
            ("no zip here", ""),  # No valid ZIP
        ]

        df = spark.createDataFrame(test_data, ["input", "expected"])
        result_df = df.withColumn("standardized", standardize_zip_code(F.col("input")))

        results = result_df.collect()
        for row in results:
            assert (
                row["standardized"] == row["expected"]
            ), f"Standardization failed for '{row['input']}': expected '{row['expected']}', got '{row['standardized']}'"

    def test_get_zip_code_type(self, spark):
        """Test ZIP code type detection."""
        from datacompose.transformers.text.clean_addresses.pyspark.pyspark_primitives import (
            get_zip_code_type,
        )

        test_data = [
            ("12345", "standard"),  # 5-digit
            ("90210", "standard"),  # 5-digit
            ("12345-6789", "plus4"),  # ZIP+4
            ("00501-1234", "plus4"),  # ZIP+4
            ("1234", "invalid"),  # Too short
            ("123456", "invalid"),  # Too long
            ("abcde", "invalid"),  # Letters
            ("12345-", "invalid"),  # Incomplete
            ("12345-678", "invalid"),  # Invalid extension
            ("", "empty"),  # Empty string
            (None, "empty"),  # Null
            ("   ", "empty"),  # Only spaces
            ("12345 6789", "invalid"),  # Space instead of dash
        ]

        df = spark.createDataFrame(test_data, ["zip", "expected"])
        result_df = df.withColumn("type", get_zip_code_type(F.col("zip")))

        results = result_df.collect()
        for row in results:
            assert (
                row["type"] == row["expected"]
            ), f"Type detection failed for '{row['zip']}': expected '{row['expected']}', got '{row['type']}'"

    def test_split_zip_code(self, spark):
        """Test ZIP code splitting into base and extension."""
        from datacompose.transformers.text.clean_addresses.pyspark.pyspark_primitives import (
            split_zip_code,
        )

        test_data = [
            ("12345", "12345", None),  # Standard - no extension
            ("12345-6789", "12345", "6789"),  # ZIP+4
            ("00501-0000", "00501", "0000"),  # ZIP+4 with zero extension
            ("99950-9999", "99950", "9999"),  # ZIP+4 with max extension
            ("1234", "", None),  # Invalid - too short
            ("abcde", "", None),  # Invalid - letters
            ("12345-", "12345", None),  # Invalid - incomplete (but gets base)
            (
                "12345 6789",
                "12345",
                None,
            ),  # Invalid - space instead of dash (but gets base)
            ("", "", None),  # Empty
            (None, None, None),  # Null
        ]

        df = spark.createDataFrame(test_data, ["zip", "expected_base", "expected_ext"])
        result_df = df.withColumn("split", split_zip_code(F.col("zip")))

        # Extract base and extension from struct
        result_df = result_df.select(
            "zip",
            "expected_base",
            "expected_ext",
            F.col("split.base").alias("actual_base"),
            F.col("split.extension").alias("actual_ext"),
        )

        results = result_df.collect()
        for row in results:
            assert (
                row["actual_base"] == row["expected_base"]
            ), f"Base extraction failed for '{row['zip']}': expected '{row['expected_base']}', got '{row['actual_base']}'"
            assert (
                row["actual_ext"] == row["expected_ext"]
            ), f"Extension extraction failed for '{row['zip']}': expected '{row['expected_ext']}', got '{row['actual_ext']}'"

    def test_combined_workflow(self, spark):
        """Test a combined workflow using multiple functions."""
        from datacompose.transformers.text.clean_addresses.pyspark.pyspark_primitives import (
            extract_zip_code,
            get_zip_code_type,
            split_zip_code,
            standardize_zip_code,
            validate_zip_code,
        )

        # Simulate real-world messy data
        test_data = [
            ("Customer at 123 Main St, NYC, NY 10001",),
            ("Ship to: 90210-1234",),
            ("Invalid address with no zip",),
            ("  12345  ",),  # Needs standardization
            ("Multiple zips: 12345 and 67890",),
            (None,),
        ]

        df = spark.createDataFrame(test_data, ["address"])

        # Apply full processing pipeline
        result_df = (
            df.withColumn("extracted", extract_zip_code(F.col("address")))
            .withColumn("is_valid", validate_zip_code(F.col("extracted")))
            .withColumn("standardized", standardize_zip_code(F.col("address")))
            .withColumn("type", get_zip_code_type(F.col("standardized")))
            .withColumn("split", split_zip_code(F.col("standardized")))
        )

        # Verify results
        results = result_df.collect()

        # Check first row (NYC address)
        assert results[0]["extracted"] == "10001"
        assert results[0]["is_valid"]
        assert results[0]["standardized"] == "10001"
        assert results[0]["type"] == "standard"
        assert results[0]["split"]["base"] == "10001"
        assert results[0]["split"]["extension"] is None

        # Check second row (ZIP+4)
        assert results[1]["extracted"] == "90210-1234"
        assert results[1]["is_valid"]
        assert results[1]["standardized"] == "90210-1234"
        assert results[1]["type"] == "plus4"
        assert results[1]["split"]["base"] == "90210"
        assert results[1]["split"]["extension"] == "1234"

        # Check invalid address
        assert results[2]["extracted"] == ""
        assert not results[2]["is_valid"]
        assert results[2]["standardized"] == ""
        assert results[2]["type"] == "empty"

    def test_validation_edge_cases(self, spark):
        """Test validation with edge cases."""
        from datacompose.transformers.text.clean_addresses.pyspark.pyspark_primitives import (
            validate_zip_code,
        )

        edge_cases = [
            # Valid edge cases
            ("00000", True),  # All zeros is technically valid
            ("00001", True),  # Minimum non-zero
            ("99998", True),  # Near maximum
            ("99999", True),  # Maximum
            ("00000-0000", True),  # All zeros ZIP+4
            ("99999-9999", True),  # Maximum ZIP+4
            # Invalid edge cases
            ("00000-", False),  # Trailing dash
            ("-12345", False),  # Leading dash
            ("12345--6789", False),  # Double dash
            ("12345-6789-", False),  # Trailing dash after valid
            ("12345-6789-0000", False),  # Too many parts
            ("\n12345", False),  # Leading newline
            ("12345\n", True),  # Trailing newline (gets extracted/validated)
            ("12345\t6789", False),  # Tab separator
        ]

        df = spark.createDataFrame(edge_cases, ["zip", "expected"])
        result_df = df.withColumn("is_valid", validate_zip_code(F.col("zip")))

        results = result_df.collect()
        for row in results:
            assert (
                row["is_valid"] == row["expected"]
            ), f"Edge case validation failed for '{row['zip']}': expected {row['expected']}, got {row['is_valid']}"

    def test_null_safety_all_functions(self, spark):
        """Test that all functions handle nulls safely."""
        # Create DataFrame with nulls
        from pyspark.sql.types import StringType, StructField, StructType

        from datacompose.transformers.text.clean_addresses.pyspark.pyspark_primitives import (
            get_zip_code_type,
            split_zip_code,
            standardize_zip_code,
            validate_zip_code,
        )

        schema = StructType([StructField("zip", StringType(), True)])
        df = spark.createDataFrame([(None,), (None,), (None,)], schema)

        # Apply all functions - none should throw errors
        result_df = (
            df.withColumn("validated", validate_zip_code(F.col("zip")))
            .withColumn("standardized", standardize_zip_code(F.col("zip")))
            .withColumn("type", get_zip_code_type(F.col("zip")))
            .withColumn("split", split_zip_code(F.col("zip")))
        )

        # All operations should complete without error
        results = result_df.collect()

        for row in results:
            assert not row["validated"]
            assert row["standardized"] == ""
            assert row["type"] == "empty"
            # For null input, base might be None or empty string depending on implementation
            assert row["split"]["base"] in ("", None)
            assert row["split"]["extension"] is None

    def test_validation_with_extraction(self, spark):
        """Test validation after extraction from text."""
        from datacompose.transformers.text.clean_addresses.pyspark.pyspark_primitives import (
            extract_zip_code,
            validate_zip_code,
        )

        test_data = [
            ("Address: 12345", "12345", True),
            ("ZIP 90210-1234 USA", "90210-1234", True),
            ("Invalid: 1234", "", False),
            ("No zip here", "", False),
            ("Multiple: 12345 and 67890", "12345", True),
        ]

        df = spark.createDataFrame(
            test_data, ["text", "expected_zip", "expected_valid"]
        )

        result_df = df.withColumn(
            "extracted", extract_zip_code(F.col("text"))
        ).withColumn("is_valid", validate_zip_code(F.col("extracted")))

        results = result_df.collect()
        for row in results:
            assert (
                row["extracted"] == row["expected_zip"]
            ), f"Extraction failed for '{row['text']}'"
            assert (
                row["is_valid"] == row["expected_valid"]
            ), f"Validation failed for extracted ZIP from '{row['text']}'"
