"""
Integration tests for datetime transformations.
Tests realistic data pipeline scenarios combining multiple datetime operations.
"""

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

from datacompose.transformers.text.datetimes.pyspark.pyspark_primitives import datetimes


@pytest.mark.unit
class TestDatetimePipelineScenarios:
    """Test realistic data pipeline scenarios."""

    def test_user_registration_pipeline(self, spark):
        """Test cleaning user registration dates from multiple sources."""

        # Simulate data from different systems with different formats
        test_data = [
            ("user_001", "2024-01-15 14:30:00", "web"),          # ISO format
            ("user_002", "01/15/2024", "mobile_app"),            # US format
            ("user_003", "15-Jan-2024", "admin_panel"),          # Named month
            ("user_004", "January 15, 2024 2:30 PM", "api"),     # Full text with time
            ("user_005", "2024-01-15T14:30:00Z", "integration"), # ISO with timezone
            ("user_006", None, "legacy_import"),                 # Missing data
            ("user_007", "", "bulk_upload"),                     # Empty string
            ("user_008", "invalid date", "csv_import"),          # Invalid data
        ]

        df = spark.createDataFrame(
            test_data, ["user_id", "registration_date", "source_system"]
        )

        # Clean and standardize pipeline
        result_df = df.withColumn(
            # Standardize to ISO format
            "clean_date", datetimes.standardize_iso(F.col("registration_date"))
        ).withColumn(
            # Validate the date
            "is_valid", datetimes.is_valid_date(F.col("registration_date"))
        ).withColumn(
            # Extract components for analytics
            "year", datetimes.extract_year(F.col("clean_date"))
        ).withColumn(
            "month", datetimes.extract_month(F.col("clean_date"))
        ).withColumn(
            "day", datetimes.extract_day(F.col("clean_date"))
        ).withColumn(
            # Calculate days since registration (if we had a reference date)
            "quarter", datetimes.extract_quarter(F.col("clean_date"))
        )

        results = result_df.collect()

        # Verify pipeline handling
        assert len(results) == 8

        # Valid dates should be standardized
        valid_results = [r for r in results if r["clean_date"] is not None]
        assert len(valid_results) >= 5

    def test_event_log_analysis_pipeline(self, spark):
        """Test analyzing event logs with timestamps."""

        test_data = [
            ("event_001", "2024-01-15 09:00:00", "session_start"),
            ("event_002", "2024-01-15 09:30:00", "page_view"),
            ("event_003", "2024-01-15 10:15:00", "purchase"),
            ("event_004", "2024-01-15 10:20:00", "session_end"),
            ("event_005", "2024-01-16 14:30:00", "session_start"),
        ]

        df = spark.createDataFrame(
            test_data, ["event_id", "timestamp", "event_type"]
        )

        # Analysis pipeline
        result_df = df.withColumn(
            "clean_timestamp", datetimes.standardize_iso(F.col("timestamp"))
        ).withColumn(
            "date_only", datetimes.standardize_date(F.col("clean_timestamp"))
        ).withColumn(
            "time_only", datetimes.standardize_time(F.col("clean_timestamp"))
        ).withColumn(
            "hour", datetimes.extract_year(F.col("clean_timestamp"))  # Would extract hour
        ).withColumn(
            "day_of_week", datetimes.extract_day_of_week(F.col("clean_timestamp"))
        ).withColumn(
            "is_business_day", datetimes.is_business_day(F.col("clean_timestamp"))
        )

        results = result_df.collect()
        assert len(results) == 5

    def test_financial_data_pipeline(self, spark):
        """Test processing financial transaction dates."""

        test_data = [
            ("txn_001", "2024-01-15", 100.00, "payment"),
            ("txn_002", "01/31/2024", 250.50, "invoice"),
            ("txn_003", "2024-02-15", 75.25, "refund"),
            ("txn_004", "03/15/2024", 500.00, "payment"),
            ("txn_005", "2024-04-01", 1000.00, "invoice"),
        ]

        df = spark.createDataFrame(
            test_data, ["transaction_id", "date", "amount", "type"]
        )

        # Financial processing pipeline
        result_df = df.withColumn(
            "clean_date", datetimes.standardize_iso(F.col("date"))
        ).withColumn(
            # Fiscal year (assuming Oct 1 start)
            "fiscal_year", datetimes.fiscal_year(F.col("clean_date"), F.lit(10))
        ).withColumn(
            # Quarter for reporting
            "fiscal_quarter", datetimes.extract_quarter(F.col("clean_date"))
        ).withColumn(
            # Month end dates for reconciliation
            "month_end", datetimes.end_of_month(F.col("clean_date"))
        ).withColumn(
            "quarter_end", datetimes.end_of_quarter(F.col("clean_date"))
        )

        results = result_df.collect()
        assert len(results) == 5

    def test_customer_age_calculation_pipeline(self, spark):
        """Test calculating customer ages and segments."""

        test_data = [
            ("cust_001", "2000-01-15", "John Doe"),
            ("cust_002", "1990-06-30", "Jane Smith"),
            ("cust_003", "1985-12-25", "Bob Johnson"),
            ("cust_004", "2005-03-10", "Alice Williams"),
            ("cust_005", "1975-09-22", "Charlie Brown"),
        ]

        df = spark.createDataFrame(
            test_data, ["customer_id", "birth_date", "name"]
        )

        # Reference date for age calculation
        reference_date = F.lit("2024-01-15")

        result_df = df.withColumn(
            "clean_birth_date", datetimes.standardize_iso(F.col("birth_date"))
        ).withColumn(
            "age", datetimes.calculate_age(F.col("clean_birth_date"), reference_date)
        ).withColumn(
            # Age segment
            "age_segment",
            F.when(F.col("age") < 18, "under_18")
            .when((F.col("age") >= 18) & (F.col("age") < 25), "18_24")
            .when((F.col("age") >= 25) & (F.col("age") < 35), "25_34")
            .when((F.col("age") >= 35) & (F.col("age") < 50), "35_49")
            .otherwise("50_plus")
        )

        results = result_df.collect()
        assert len(results) == 5


@pytest.mark.unit
class TestDatetimeDataQualityPipeline:
    """Test data quality workflows for datetime data."""

    def test_date_validation_and_flagging(self, spark):
        """Test pipeline that validates and flags date issues."""

        test_data = [
            ("rec_001", "2024-01-15", "valid"),
            ("rec_002", "2024-13-01", "invalid_month"),
            ("rec_003", "2024-02-30", "invalid_day"),
            ("rec_004", None, "missing"),
            ("rec_005", "", "empty"),
            ("rec_006", "not a date", "malformed"),
            ("rec_007", "2024-12-31", "valid"),
        ]

        df = spark.createDataFrame(
            test_data, ["record_id", "date_field", "expected_issue"]
        )

        # Data quality pipeline
        result_df = df.withColumn(
            "is_valid", datetimes.is_valid_date(F.col("date_field"))
        ).withColumn(
            "is_null", F.col("date_field").isNull()
        ).withColumn(
            "is_empty", F.length(F.trim(F.col("date_field"))) == 0
        ).withColumn(
            "standardized", datetimes.standardize_iso(F.col("date_field"))
        ).withColumn(
            # Create quality flag
            "quality_flag",
            F.when(F.col("is_null"), "NULL")
            .when(F.col("is_empty"), "EMPTY")
            .when(~F.col("is_valid"), "INVALID")
            .otherwise("OK")
        )

        results = result_df.collect()

        # Verify quality flags
        assert len(results) == 7
        quality_flags = [r["quality_flag"] for r in results]
        assert "OK" in quality_flags
        assert "INVALID" in quality_flags or "NULL" in quality_flags

    def test_date_range_validation(self, spark):
        """Test validating dates are within acceptable ranges."""

        test_data = [
            ("rec_001", "2024-01-15", True),   # Current year
            ("rec_002", "2023-06-30", True),   # Recent past
            ("rec_003", "1900-01-01", False),  # Too old
            ("rec_004", "2050-12-31", False),  # Too far future
            ("rec_005", "2024-12-31", True),   # Valid future
        ]

        df = spark.createDataFrame(
            test_data, ["record_id", "date_field", "expected_in_range"]
        )

        # Range validation pipeline
        min_date = F.lit("2020-01-01")
        max_date = F.lit("2030-12-31")

        result_df = df.withColumn(
            "clean_date", datetimes.standardize_iso(F.col("date_field"))
        ).withColumn(
            "is_past", datetimes.is_past_date(F.col("clean_date"))
        ).withColumn(
            "is_future", datetimes.is_future_date(F.col("clean_date"))
        ).withColumn(
            "in_range",
            (F.col("clean_date") >= min_date) & (F.col("clean_date") <= max_date)
        )

        results = result_df.collect()
        assert len(results) == 5

    def test_duplicate_detection_pipeline(self, spark):
        """Test detecting and handling duplicate dates."""

        test_data = [
            ("user_001", "2024-01-15"),
            ("user_002", "2024-01-15"),  # Duplicate date
            ("user_003", "01/15/2024"),  # Same date, different format
            ("user_004", "2024-01-16"),
            ("user_005", "January 15, 2024"),  # Same date again
        ]

        df = spark.createDataFrame(test_data, ["user_id", "signup_date"])

        # Dedupe pipeline
        result_df = df.withColumn(
            "clean_date", datetimes.standardize_iso(F.col("signup_date"))
        ).groupBy("clean_date").agg(
            F.count("*").alias("count"),
            F.collect_list("user_id").alias("user_ids")
        ).withColumn(
            "is_duplicate", F.col("count") > 1
        )

        results = result_df.collect()

        # Should identify duplicates
        duplicate_rows = [r for r in results if r["is_duplicate"]]
        assert len(duplicate_rows) > 0


@pytest.mark.unit
class TestDatetimeTransformationChains:
    """Test chaining multiple datetime transformations."""

    def test_parse_standardize_extract_chain(self, spark):
        """Test chain: parse -> standardize -> extract components."""

        test_data = [
            ("01/15/2024 2:30 PM",),
            ("2024-01-15T14:30:00",),
            ("January 15, 2024",),
        ]

        df = spark.createDataFrame(test_data, ["original_date"])

        # Transformation chain
        result_df = df.withColumn(
            # Step 1: Standardize
            "standardized", datetimes.standardize_iso(F.col("original_date"))
        ).withColumn(
            # Step 2: Validate
            "is_valid", datetimes.is_valid_date(F.col("original_date"))
        ).withColumn(
            # Step 3: Extract components
            "year", datetimes.extract_year(F.col("standardized"))
        ).withColumn(
            "month", datetimes.extract_month(F.col("standardized"))
        ).withColumn(
            "quarter", datetimes.extract_quarter(F.col("standardized"))
        ).withColumn(
            "day_of_week", datetimes.extract_day_of_week(F.col("standardized"))
        )

        results = result_df.collect()
        assert len(results) == 3

    def test_date_arithmetic_chain(self, spark):
        """Test chain of date arithmetic operations."""

        test_data = [
            ("2024-01-15",),
            ("2024-06-30",),
            ("2024-12-31",),
        ]

        df = spark.createDataFrame(test_data, ["start_date"])

        # Arithmetic chain
        result_df = df.withColumn(
            "plus_30_days", datetimes.add_days(F.col("start_date"), F.lit(30))
        ).withColumn(
            "plus_2_months", datetimes.add_months(F.col("start_date"), F.lit(2))
        ).withColumn(
            "month_start", datetimes.start_of_month(F.col("start_date"))
        ).withColumn(
            "month_end", datetimes.end_of_month(F.col("start_date"))
        ).withColumn(
            "quarter_end", datetimes.end_of_quarter(F.col("start_date"))
        )

        results = result_df.collect()
        assert len(results) == 3

    def test_timezone_conversion_chain(self, spark):
        """Test chain of timezone operations."""

        test_data = [
            ("2024-01-15T14:30:00Z",),  # UTC
            ("2024-01-15 09:30:00",),    # Naive
        ]

        df = spark.createDataFrame(test_data, ["timestamp"])

        # Timezone chain
        result_df = df.withColumn(
            # Normalize to UTC
            "utc_time", datetimes.normalize_timezone(F.col("timestamp"), F.lit("UTC"))
        ).withColumn(
            # Convert to Eastern
            "est_time", datetimes.normalize_timezone(F.col("timestamp"), F.lit("America/New_York"))
        ).withColumn(
            # Convert to Pacific
            "pst_time", datetimes.normalize_timezone(F.col("timestamp"), F.lit("America/Los_Angeles"))
        ).withColumn(
            # Remove timezone info
            "naive_time", datetimes.remove_timezone(F.col("timestamp"))
        )

        results = result_df.collect()
        assert len(results) == 2


@pytest.mark.unit
class TestDatetimeEnrichmentPipeline:
    """Test pipelines that enrich data with datetime-derived fields."""

    def test_add_calendar_dimensions(self, spark):
        """Test adding calendar dimension fields for analytics."""

        test_data = [
            ("txn_001", "2024-01-15"),
            ("txn_002", "2024-07-04"),
            ("txn_003", "2024-12-25"),
        ]

        df = spark.createDataFrame(test_data, ["transaction_id", "date"])

        # Enrichment pipeline
        result_df = df.withColumn(
            "clean_date", datetimes.standardize_iso(F.col("date"))
        ).withColumn(
            # Calendar dimensions
            "year", datetimes.extract_year(F.col("clean_date"))
        ).withColumn(
            "quarter", datetimes.extract_quarter(F.col("clean_date"))
        ).withColumn(
            "month", datetimes.extract_month(F.col("clean_date"))
        ).withColumn(
            "week_of_year", datetimes.extract_week_of_year(F.col("clean_date"))
        ).withColumn(
            "day_of_week", datetimes.extract_day_of_week(F.col("clean_date"))
        ).withColumn(
            "is_business_day", datetimes.is_business_day(F.col("clean_date"))
        ).withColumn(
            "fiscal_year", datetimes.fiscal_year(F.col("clean_date"), F.lit(10))
        )

        results = result_df.collect()
        assert len(results) == 3

    def test_add_derived_dates(self, spark):
        """Test adding derived date fields."""

        test_data = [
            ("event_001", "2024-01-15"),
            ("event_002", "2024-06-30"),
        ]

        df = spark.createDataFrame(test_data, ["event_id", "event_date"])

        # Add derived dates
        result_df = df.withColumn(
            "clean_date", datetimes.standardize_iso(F.col("event_date"))
        ).withColumn(
            "month_start", datetimes.start_of_month(F.col("clean_date"))
        ).withColumn(
            "month_end", datetimes.end_of_month(F.col("clean_date"))
        ).withColumn(
            "quarter_start", datetimes.start_of_quarter(F.col("clean_date"))
        ).withColumn(
            "quarter_end", datetimes.end_of_quarter(F.col("clean_date"))
        ).withColumn(
            "days_until_month_end",
            datetimes.date_diff(
                F.col("month_end"),
                F.col("clean_date"),
                F.lit("days")
            )
        )

        results = result_df.collect()
        assert len(results) == 2

    def test_business_metrics_enrichment(self, spark):
        """Test enriching with business-relevant datetime metrics."""

        test_data = [
            ("cust_001", "2024-01-15", "2024-03-20"),
            ("cust_002", "2024-02-10", "2024-04-15"),
        ]

        df = spark.createDataFrame(
            test_data, ["customer_id", "signup_date", "first_purchase_date"]
        )

        # Business metrics
        result_df = df.withColumn(
            "clean_signup", datetimes.standardize_iso(F.col("signup_date"))
        ).withColumn(
            "clean_purchase", datetimes.standardize_iso(F.col("first_purchase_date"))
        ).withColumn(
            # Time to first purchase
            "days_to_purchase",
            datetimes.date_diff(
                F.col("clean_purchase"),
                F.col("clean_signup"),
                F.lit("days")
            )
        ).withColumn(
            # Business days to purchase
            "business_days_to_purchase",
            datetimes.business_days_between(
                F.col("clean_signup"),
                F.col("clean_purchase")
            )
        ).withColumn(
            # Cohort month
            "signup_month", datetimes.extract_month(F.col("clean_signup"))
        ).withColumn(
            "signup_quarter", datetimes.extract_quarter(F.col("clean_signup"))
        )

        results = result_df.collect()
        assert len(results) == 2
