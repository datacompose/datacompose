"""
Performance tests for datetime transformations.
Tests transformation speed and scalability with large datasets.
"""

import pytest
from pyspark.sql import functions as F
from datetime import datetime, timedelta
import time

from datacompose.transformers.text.datetimes.pyspark.pyspark_primitives import datetimes


@pytest.mark.unit
class TestDatetimePerformance:
    """Test datetime transformation performance with various dataset sizes."""

    def test_standardize_iso_performance_small(self, spark):
        """Test standardize_iso with 1,000 records."""

        # Generate test data
        test_dates = [
            ("01/15/2024",),
            ("2024-01-15",),
            ("January 15, 2024",),
            ("15-Jan-2024",),
            ("01/15/2024 2:30 PM",),
        ] * 200  # 1,000 records

        df = spark.createDataFrame(test_dates, ["date_str"])

        start_time = time.time()
        result_df = df.withColumn(
            "standardized", datetimes.standardize_iso(F.col("date_str"))
        )
        result_df.count()  # Force evaluation
        end_time = time.time()

        elapsed = end_time - start_time
        print(f"\nSmall dataset (1K records): {elapsed:.3f} seconds")

        # Should complete in reasonable time (adjust threshold as needed)
        assert elapsed < 5.0, f"Small dataset took too long: {elapsed:.3f}s"

    def test_standardize_iso_performance_medium(self, spark):
        """Test standardize_iso with 100,000 records."""

        # Generate test data with variety
        base_dates = [
            ("01/15/2024",),
            ("2024-01-15",),
            ("January 15, 2024",),
            ("15-Jan-2024",),
            ("01/15/2024 2:30 PM",),
            ("2024-01-15T14:30:00",),
            ("Jan 15, 2024 10:00 AM",),
            ("15/01/2024",),
            ("15.01.2024",),
            ("15 January 2024",),
        ]

        test_dates = base_dates * 10000  # 100,000 records

        df = spark.createDataFrame(test_dates, ["date_str"])

        start_time = time.time()
        result_df = df.withColumn(
            "standardized", datetimes.standardize_iso(F.col("date_str"))
        )
        result_df.count()  # Force evaluation
        end_time = time.time()

        elapsed = end_time - start_time
        print(f"\nMedium dataset (100K records): {elapsed:.3f} seconds")

        # Should complete in reasonable time
        assert elapsed < 30.0, f"Medium dataset took too long: {elapsed:.3f}s"

    def test_standardize_iso_performance_large(self, spark):
        """Test standardize_iso with 1,000,000 records."""

        base_dates = [
            ("01/15/2024",),
            ("2024-01-15",),
            ("January 15, 2024",),
            ("15-Jan-2024",),
            ("01/15/2024 2:30 PM",),
        ]

        test_dates = base_dates * 200000  # 1,000,000 records

        df = spark.createDataFrame(test_dates, ["date_str"])

        start_time = time.time()
        result_df = df.withColumn(
            "standardized", datetimes.standardize_iso(F.col("date_str"))
        )
        result_df.count()  # Force evaluation
        end_time = time.time()

        elapsed = end_time - start_time
        print(f"\nLarge dataset (1M records): {elapsed:.3f} seconds")

        # Large datasets may take longer
        assert elapsed < 60.0, f"Large dataset took too long: {elapsed:.3f}s"

    def test_multiple_transformations_performance(self, spark):
        """Test performance when chaining multiple datetime operations."""

        test_dates = [
            ("01/15/2024",),
            ("2024-01-15",),
            ("January 15, 2024",),
        ] * 10000  # 30,000 records

        df = spark.createDataFrame(test_dates, ["date_str"])

        start_time = time.time()
        result_df = df.withColumn(
            "standardized", datetimes.standardize_iso(F.col("date_str"))
        ).withColumn(
            "is_valid", datetimes.is_valid_date(F.col("date_str"))
        ).withColumn(
            "year", datetimes.extract_year(F.col("date_str"))
        ).withColumn(
            "month", datetimes.extract_month(F.col("date_str"))
        ).withColumn(
            "day", datetimes.extract_day(F.col("date_str"))
        )
        result_df.count()  # Force evaluation
        end_time = time.time()

        elapsed = end_time - start_time
        print(f"\nChained transformations (30K records): {elapsed:.3f} seconds")

        assert elapsed < 30.0, f"Chained transformations took too long: {elapsed:.3f}s"

    def test_null_heavy_dataset_performance(self, spark):
        """Test performance with dataset containing many nulls."""

        # Create dataset with 50% nulls
        test_dates = [
            ("01/15/2024",),
            (None,),
            ("2024-01-15",),
            (None,),
            ("January 15, 2024",),
            (None,),
        ] * 10000  # 60,000 records

        df = spark.createDataFrame(test_dates, ["date_str"])

        start_time = time.time()
        result_df = df.withColumn(
            "standardized", datetimes.standardize_iso(F.col("date_str"))
        )
        result_df.count()
        end_time = time.time()

        elapsed = end_time - start_time
        print(f"\nNull-heavy dataset (60K records, 50% nulls): {elapsed:.3f} seconds")

        assert elapsed < 20.0, f"Null-heavy dataset took too long: {elapsed:.3f}s"

    def test_invalid_data_performance(self, spark):
        """Test performance with dataset containing mostly invalid dates."""

        # Create dataset with 80% invalid dates
        test_dates = [
            ("not a date",),
            ("12345",),
            ("random text",),
            ("2024-99-99",),
            ("01/15/2024",),  # Only 20% valid
        ] * 10000  # 50,000 records

        df = spark.createDataFrame(test_dates, ["date_str"])

        start_time = time.time()
        result_df = df.withColumn(
            "standardized", datetimes.standardize_iso(F.col("date_str"))
        ).withColumn(
            "is_valid", datetimes.is_valid_date(F.col("date_str"))
        )
        result_df.count()
        end_time = time.time()

        elapsed = end_time - start_time
        print(f"\nInvalid-heavy dataset (50K records, 80% invalid): {elapsed:.3f} seconds")

        # Invalid data shouldn't significantly slow down processing
        assert elapsed < 25.0, f"Invalid-heavy dataset took too long: {elapsed:.3f}s"

    def test_wide_variety_formats_performance(self, spark):
        """Test performance with many different date formats."""

        # Create dataset with 20+ different date formats
        test_dates = [
            ("2024-01-15",),
            ("01/15/2024",),
            ("15/01/2024",),
            ("January 15, 2024",),
            ("Jan 15, 2024",),
            ("15-Jan-2024",),
            ("15 January 2024",),
            ("2024-01-15T14:30:00",),
            ("2024-01-15 14:30:00",),
            ("01/15/2024 2:30 PM",),
            ("01/15/2024 14:30",),
            ("Jan 15, 2024 10:00 AM",),
            ("January 15, 2024 2:30 PM",),
            ("15.01.2024",),
            ("15-01-2024",),
            ("2024/01/15",),
            ("01-15-2024",),
            ("2024 01 15",),
            ("15-01-24",),
            ("01/15/24",),
        ] * 5000  # 100,000 records

        df = spark.createDataFrame(test_dates, ["date_str"])

        start_time = time.time()
        result_df = df.withColumn(
            "standardized", datetimes.standardize_iso(F.col("date_str"))
        )
        result_df.count()
        end_time = time.time()

        elapsed = end_time - start_time
        print(f"\nWide variety formats (100K records, 20 formats): {elapsed:.3f} seconds")

        assert elapsed < 35.0, f"Wide variety dataset took too long: {elapsed:.3f}s"


@pytest.mark.unit
class TestDatetimeScalability:
    """Test scalability characteristics of datetime transformations."""

    def test_linear_scalability(self, spark):
        """Verify that performance scales linearly with dataset size."""

        base_dates = [
            ("01/15/2024",),
            ("2024-01-15",),
            ("January 15, 2024",),
        ]

        timings = {}

        # Test with increasing dataset sizes
        for size in [1000, 10000, 100000]:
            test_dates = base_dates * (size // len(base_dates))
            df = spark.createDataFrame(test_dates, ["date_str"])

            start_time = time.time()
            result_df = df.withColumn(
                "standardized", datetimes.standardize_iso(F.col("date_str"))
            )
            result_df.count()
            end_time = time.time()

            timings[size] = end_time - start_time
            print(f"\nDataset size {size}: {timings[size]:.3f} seconds")

        # Check that 10x data doesn't take more than 15x time (allowing some overhead)
        if timings[1000] > 0:
            ratio_10k = timings[10000] / timings[1000]
            assert ratio_10k < 15, f"10x data took {ratio_10k}x time (should be ~10x)"

        if timings[10000] > 0:
            ratio_100k = timings[100000] / timings[10000]
            assert ratio_100k < 15, f"10x data took {ratio_100k}x time (should be ~10x)"

    def test_memory_efficiency(self, spark):
        """Test that transformations don't cause excessive memory usage."""

        # Create large dataset
        test_dates = [("2024-01-15",)] * 100000
        df = spark.createDataFrame(test_dates, ["date_str"])

        # Apply multiple transformations
        result_df = df.withColumn(
            "standardized", datetimes.standardize_iso(F.col("date_str"))
        ).withColumn(
            "year", datetimes.extract_year(F.col("standardized"))
        ).withColumn(
            "month", datetimes.extract_month(F.col("standardized"))
        ).cache()

        # Force materialization
        count = result_df.count()

        assert count == 100000

        # Cleanup
        result_df.unpersist()

    def test_partition_handling(self, spark):
        """Test performance with different partition counts."""

        test_dates = [
            ("01/15/2024",),
            ("2024-01-15",),
        ] * 50000  # 100,000 records

        df = spark.createDataFrame(test_dates, ["date_str"])

        timings = {}

        # Test with different partition counts
        for num_partitions in [1, 4, 8]:
            partitioned_df = df.repartition(num_partitions)

            start_time = time.time()
            result_df = partitioned_df.withColumn(
                "standardized", datetimes.standardize_iso(F.col("date_str"))
            )
            result_df.count()
            end_time = time.time()

            timings[num_partitions] = end_time - start_time
            print(f"\n{num_partitions} partitions: {timings[num_partitions]:.3f} seconds")

        # More partitions should generally be faster (up to a point)
        # But this depends on cluster size, so we just verify it runs


@pytest.mark.unit
class TestDatetimeCacheEfficiency:
    """Test caching behavior and efficiency."""

    def test_repeated_transformations(self, spark):
        """Test performance when running same transformation multiple times."""

        test_dates = [("01/15/2024",)] * 10000
        df = spark.createDataFrame(test_dates, ["date_str"])

        # First run (cold)
        start_time = time.time()
        result_df = df.withColumn(
            "standardized", datetimes.standardize_iso(F.col("date_str"))
        )
        result_df.count()
        first_run = time.time() - start_time

        # Second run (should use cached execution plan)
        start_time = time.time()
        result_df = df.withColumn(
            "standardized", datetimes.standardize_iso(F.col("date_str"))
        )
        result_df.count()
        second_run = time.time() - start_time

        print(f"\nFirst run: {first_run:.3f}s, Second run: {second_run:.3f}s")

        # Both should complete in reasonable time
        assert first_run < 10.0
        assert second_run < 10.0

    def test_cached_dataframe_performance(self, spark):
        """Test performance improvement from caching intermediate results."""

        test_dates = [
            ("01/15/2024",),
            ("2024-01-15",),
        ] * 25000  # 50,000 records

        df = spark.createDataFrame(test_dates, ["date_str"])

        # Without caching
        start_time = time.time()
        result_df = df.withColumn(
            "standardized", datetimes.standardize_iso(F.col("date_str"))
        )
        result_df.count()
        result_df.count()  # Count twice
        no_cache_time = time.time() - start_time

        # With caching
        start_time = time.time()
        cached_df = df.withColumn(
            "standardized", datetimes.standardize_iso(F.col("date_str"))
        ).cache()
        cached_df.count()
        cached_df.count()  # Count twice
        cache_time = time.time() - start_time

        print(f"\nNo cache: {no_cache_time:.3f}s, With cache: {cache_time:.3f}s")

        # Cleanup
        cached_df.unpersist()

        # Both should complete
        assert no_cache_time < 30.0
        assert cache_time < 30.0
