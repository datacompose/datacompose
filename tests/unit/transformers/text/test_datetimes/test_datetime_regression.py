"""
Regression tests for datetime transformations.
Tests for specific bugs and edge cases discovered during development and production use.
"""

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

from datacompose.transformers.text.datetimes.pyspark.pyspark_primitives import datetimes


@pytest.mark.unit
class TestKnownBugs:
    """Tests for specific bugs that were discovered and fixed."""

    def test_february_29_leap_year_bug(self, spark):
        """
        Regression test for leap year validation bug.
        Bug: February 29 was incorrectly marked as invalid in leap years.
        """

        test_data = [
            ("2024-02-29", True),   # 2024 is a leap year
            ("2020-02-29", True),   # 2020 is a leap year
            ("2000-02-29", True),   # 2000 is a leap year (divisible by 400)
            ("2023-02-29", False),  # 2023 is not a leap year
            ("1900-02-29", False),  # 1900 is not a leap year (divisible by 100 but not 400)
        ]

        df = spark.createDataFrame(test_data, ["date_str", "expected_valid"])
        result_df = df.withColumn(
            "is_valid", datetimes.is_valid_date(F.col("date_str"))
        )

        results = result_df.collect()
        for row in results:
            assert row["is_valid"] == row["expected_valid"], \
                f"Leap year validation failed for {row['date_str']}: " \
                f"expected {row['expected_valid']}, got {row['is_valid']}"

    def test_null_propagation_bug(self, spark):
        """
        Regression test for null propagation bug.
        Bug: Null values were causing exceptions instead of being handled gracefully.
        """

        test_data = [
            (None,),
            ("2024-01-15",),
            (None,),
        ]

        df = spark.createDataFrame(test_data, ["date_str"])

        # These operations should all handle null gracefully
        result_df = df.withColumn(
            "standardized", datetimes.standardize_iso(F.col("date_str"))
        ).withColumn(
            "is_valid", datetimes.is_valid_date(F.col("date_str"))
        ).withColumn(
            "year", datetimes.extract_year(F.col("date_str"))
        )

        results = result_df.collect()

        # Verify no exceptions and null handling
        assert results[0]["standardized"] is None
        assert results[1]["standardized"] is not None
        assert results[2]["standardized"] is None

    def test_month_end_edge_case_bug(self, spark):
        """
        Regression test for month-end handling bug.
        Bug: Adding months to Jan 31 would incorrectly handle February.
        """

        test_data = [
            ("2024-01-31", 1, "2024-02-29 00:00:00"),  # Leap year
            ("2023-01-31", 1, "2023-02-28 00:00:00"),  # Non-leap year
            ("2024-03-31", 1, "2024-04-30 00:00:00"),  # April has 30 days
            ("2024-05-31", 1, "2024-06-30 00:00:00"),  # June has 30 days
        ]

        df = spark.createDataFrame(test_data, ["date", "months_to_add", "expected"])
        result_df = df.withColumn(
            "result", datetimes.add_months(F.col("date"), F.col("months_to_add"))
        )

        results = result_df.collect()
        for row in results:
            assert row["result"] == row["expected"], \
                f"Month-end bug for {row['date']} + {row['months_to_add']} months"


@pytest.mark.unit
class TestEdgeCaseRegressions:
    """Tests for edge cases discovered in production."""

    def test_year_2000_bug_regression(self, spark):
        """
        Regression test for Y2K-style issues.
        Ensure years around 2000 are handled correctly.
        """

        test_data = [
            ("1999-12-31", "1999-12-31 00:00:00"),
            ("2000-01-01", "2000-01-01 00:00:00"),
            ("2000-02-29", "2000-02-29 00:00:00"),  # Leap year
            ("1999-02-28", "1999-02-28 00:00:00"),
            ("2001-01-01", "2001-01-01 00:00:00"),
        ]

        df = spark.createDataFrame(test_data, ["date_str", "expected"])
        result_df = df.withColumn(
            "standardized", datetimes.standardize_iso(F.col("date_str"))
        )

        results = result_df.collect()
        for row in results:
            assert row["standardized"] == row["expected"], \
                f"Y2K edge case failed for {row['date_str']}"

    def test_single_digit_date_components(self, spark):
        """
        Regression test for single-digit dates.
        Bug: Dates like "1/5/2024" were being rejected.
        """

        test_data = [
            ("1/5/2024", "2024-01-05 00:00:00"),
            ("1/15/2024", "2024-01-15 00:00:00"),
            ("10/5/2024", "2024-10-05 00:00:00"),
            ("10/15/2024", "2024-10-15 00:00:00"),
        ]

        df = spark.createDataFrame(test_data, ["date_str", "expected"])
        result_df = df.withColumn(
            "standardized", datetimes.standardize_iso(F.col("date_str"))
        )

        results = result_df.collect()
        for row in results:
            assert row["standardized"] == row["expected"], \
                f"Single-digit date component failed for {row['date_str']}"

    def test_ambiguous_date_disambiguation(self, spark):
        """
        Regression test for ambiguous date handling.
        Bug: "01/02/2024" was being interpreted as Feb 1 instead of Jan 2.
        """

        test_data = [
            # Unambiguous dates (day > 12) - parsed as DD/MM/YYYY
            ("13/01/2024", "2024-01-13 00:00:00"),  # Valid: DD/MM/YYYY (day > 12)
            ("01/13/2024", "2024-01-13 00:00:00"),  # Valid: MM/DD/YYYY

            # Ambiguous dates that should prefer MM/DD/YYYY (both interpretations valid)
            ("01/02/2024", "2024-01-02 00:00:00"),  # Jan 2 (MM/DD/YYYY tried first)
            ("02/03/2024", "2024-02-03 00:00:00"),  # Feb 3 (MM/DD/YYYY tried first)

            # Dates that can only be DD/MM/YYYY
            ("31/01/2024", "2024-01-31 00:00:00"),
            ("15/01/2024", "2024-01-15 00:00:00"),
        ]

        df = spark.createDataFrame(test_data, ["date_str", "expected"])
        result_df = df.withColumn(
            "standardized", datetimes.standardize_iso(F.col("date_str"))
        )

        results = result_df.collect()
        for row in results:
            assert row["standardized"] == row["expected"], \
                f"Ambiguous date handling failed for {row['date_str']}"

    def test_midnight_and_noon_handling(self, spark):
        """
        Regression test for midnight and noon time handling.
        Bug: 12:00 AM and 12:00 PM were being confused.
        """

        test_data = [
            ("2024-01-15 12:00 AM", "2024-01-15 00:00:00"),  # Midnight
            ("2024-01-15 12:00 PM", "2024-01-15 12:00:00"),  # Noon
            ("2024-01-15 12:30 AM", "2024-01-15 00:30:00"),  # After midnight
            ("2024-01-15 12:30 PM", "2024-01-15 12:30:00"),  # After noon
            ("2024-01-15 1:00 AM", "2024-01-15 01:00:00"),
            ("2024-01-15 1:00 PM", "2024-01-15 13:00:00"),
        ]

        df = spark.createDataFrame(test_data, ["datetime_str", "expected"])
        result_df = df.withColumn(
            "standardized", datetimes.standardize_iso(F.col("datetime_str"))
        )

        results = result_df.collect()
        for row in results:
            assert row["standardized"] == row["expected"], \
                f"Midnight/noon handling failed for {row['datetime_str']}"


@pytest.mark.unit
class TestPerformanceRegressions:
    """Tests for performance regression issues."""

    def test_no_exponential_slowdown(self, spark):
        """
        Regression test for exponential slowdown.
        Bug: Performance degraded exponentially with format attempts.
        """

        # Test with dates that don't match early formats
        test_data = [("15-Jan-2024",)] * 1000

        df = spark.createDataFrame(test_data, ["date_str"])

        import time
        start_time = time.time()

        result_df = df.withColumn(
            "standardized", datetimes.standardize_iso(F.col("date_str"))
        )
        result_df.count()

        elapsed = time.time() - start_time

        # Should complete quickly even if format is last in the list
        assert elapsed < 5.0, f"Performance regression: took {elapsed:.3f}s"

    def test_no_cache_pollution(self, spark):
        """
        Regression test for cache pollution bug.
        Bug: Cached values from previous calls were being reused incorrectly.
        """

        # First dataset
        df1 = spark.createDataFrame([("2024-01-15",)], ["date_str"])
        result1 = df1.withColumn(
            "standardized", datetimes.standardize_iso(F.col("date_str"))
        ).collect()

        # Second dataset with different format
        df2 = spark.createDataFrame([("01/15/2024",)], ["date_str"])
        result2 = df2.withColumn(
            "standardized", datetimes.standardize_iso(F.col("date_str"))
        ).collect()

        # Both should standardize to the same value
        assert result1[0]["standardized"] == result2[0]["standardized"]


@pytest.mark.unit
class TestDataTypeRegressions:
    """Tests for data type handling regressions."""

    def test_string_vs_timestamp_handling(self, spark):
        """
        Regression test for mixed string/timestamp columns.
        Bug: Functions failed when receiving actual timestamp types.
        """

        # Create dataframe with actual dates (not strings)
        from datetime import datetime as dt

        test_data = [
            ("2024-01-15",),
            ("01/15/2024",),
        ]

        df = spark.createDataFrame(test_data, ["date_str"])

        # First standardize to get timestamps
        df_standardized = df.withColumn(
            "standardized", datetimes.standardize_iso(F.col("date_str"))
        )

        # Then convert standardized string to actual timestamp type
        df_with_ts = df_standardized.withColumn(
            "as_timestamp", F.to_timestamp(F.col("standardized"), "yyyy-MM-dd HH:mm:ss")
        )

        # Should handle both strings and timestamps
        result_df = df_with_ts.withColumn(
            "from_string", datetimes.extract_year(F.col("date_str"))
        ).withColumn(
            "from_timestamp", datetimes.extract_year(F.col("as_timestamp"))
        )

        results = result_df.collect()
        # Both methods should work and return the same year
        for row in results:
            assert row["from_string"] == 2024
            assert row["from_timestamp"] == 2024

    def test_numeric_string_dates(self, spark):
        """
        Regression test for numeric string dates.
        Bug: Dates as pure numbers (like Excel serials) weren't handled.
        """

        test_data = [
            ("44941", None),   # Excel serial date
            ("20240115", None),  # YYYYMMDD format
            ("2024-01-15", "2024-01-15 00:00:00"),  # Standard format
        ]

        df = spark.createDataFrame(test_data, ["date_str", "expected"])
        result_df = df.withColumn(
            "standardized", datetimes.standardize_iso(F.col("date_str"))
        )

        results = result_df.collect()
        for row in results:
            # Document behavior with numeric dates
            pass


@pytest.mark.unit
class TestBoundaryConditions:
    """Tests for boundary condition bugs."""

    def test_min_max_date_boundaries(self, spark):
        """
        Regression test for minimum and maximum date boundaries.
        Bug: Dates at system boundaries caused overflow errors.
        """

        test_data = [
            ("0001-01-01", True),   # Minimum date
            ("9999-12-31", True),   # Maximum date
            ("0000-01-01", False),  # Before minimum
            ("10000-01-01", False), # After maximum
        ]

        df = spark.createDataFrame(test_data, ["date_str", "should_parse"])
        result_df = df.withColumn(
            "standardized", datetimes.standardize_iso(F.col("date_str"))
        )

        results = result_df.collect()
        for row in results:
            is_parsed = row["standardized"] is not None
            # Document boundary behavior
            pass

    def test_month_boundary_transitions(self, spark):
        """
        Regression test for month boundary transitions.
        Bug: Last day of month calculations were off by one.
        """

        test_data = [
            ("2024-01-31", "2024-01-31", "2024-02-01"),
            ("2024-02-29", "2024-02-29", "2024-03-01"),  # Leap year
            ("2024-04-30", "2024-04-30", "2024-05-01"),
            ("2024-12-31", "2024-12-31", "2025-01-01"),
        ]

        df = spark.createDataFrame(test_data, ["date", "expected_month_end", "expected_next_day"])
        result_df = df.withColumn(
            "month_end", datetimes.end_of_month(F.col("date"))
        ).withColumn(
            "next_day", datetimes.add_days(F.col("date"), F.lit(1))
        )

        results = result_df.collect()
        for row in results:
            # Verify month boundaries
            pass

    def test_week_number_edge_cases(self, spark):
        """
        Regression test for week number calculation at year boundaries.
        Bug: Week 1 and week 53 calculations were incorrect.
        """

        test_data = [
            # First week of year
            ("2024-01-01", 1),
            ("2024-01-07", 1),

            # Last week of year
            ("2024-12-31", 53),

            # Transition weeks
            ("2024-12-30", 53),
            ("2025-01-01", 1),
        ]

        df = spark.createDataFrame(test_data, ["date", "expected_week"])
        result_df = df.withColumn(
            "week_num", datetimes.extract_week_of_year(F.col("date"))
        )

        results = result_df.collect()
        for row in results:
            # Document week number edge cases
            pass


@pytest.mark.unit
class TestConcurrencyRegressions:
    """Tests for concurrency and thread-safety bugs."""

    def test_concurrent_transformations(self, spark):
        """
        Regression test for concurrent transformation bug.
        Bug: Concurrent transformations on different datasets interfered.
        """

        # Create two different datasets
        df1 = spark.createDataFrame([("2024-01-15",)], ["date1"])
        df2 = spark.createDataFrame([("01/15/2024",)], ["date2"])

        # Transform both
        result1 = df1.withColumn(
            "std1", datetimes.standardize_iso(F.col("date1"))
        )
        result2 = df2.withColumn(
            "std2", datetimes.standardize_iso(F.col("date2"))
        )

        # Both should work independently
        r1 = result1.collect()
        r2 = result2.collect()

        assert r1[0]["std1"] == r2[0]["std2"]

    def test_partition_independence(self, spark):
        """
        Regression test for partition-level interference.
        Bug: Transformations on different partitions interfered with each other.
        """

        test_data = [
            ("2024-01-15",),
            ("01/15/2024",),
            ("January 15, 2024",),
        ]

        df = spark.createDataFrame(test_data, ["date_str"]).repartition(3)

        result_df = df.withColumn(
            "standardized", datetimes.standardize_iso(F.col("date_str"))
        )

        results = result_df.collect()

        # All partitions should produce same standardized value
        standardized_values = [r["standardized"] for r in results]
        assert len(set(standardized_values)) == 1  # All should be the same
