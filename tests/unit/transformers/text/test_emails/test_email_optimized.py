"""
Optimized email typo fixing using broadcast join approach.
This avoids the deep expression tree problem of nested when/otherwise statements.
"""

import pytest
from pyspark.sql import SparkSession, functions as F
from datacompose.transformers.text.clean_emails.pyspark.pyspark_primitives import (
    DOMAIN_TYPO_MAPPINGS,
    emails,
)


@pytest.fixture(scope="session")
def spark():
    """Create a Spark session for testing."""
    spark = (
        SparkSession.builder.appName("OptimizedEmailTests")
        .master("local[*]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    yield spark
    spark.stop()


def fix_typos_optimized(spark, email_df, email_col="email"):
    """
    Fix email typos using broadcast join instead of nested when/otherwise.
    Much more efficient for large numbers of typo mappings.
    """
    # Create typo mapping DataFrame
    typo_data = [(k, v) for k, v in DOMAIN_TYPO_MAPPINGS.items()]
    typo_df = spark.createDataFrame(typo_data, ["typo_domain", "correct_domain"])

    # Extract domain and join with typo mappings
    result_df = (
        email_df.withColumn("original_email", F.col(email_col))
        .withColumn("username", emails.extract_username(F.col(email_col)))
        .withColumn("domain", emails.extract_domain(F.col(email_col)))
        # Left join with broadcast hint for small typo table
        .join(
            F.broadcast(typo_df),
            F.lower(F.col("domain")) == F.lower(F.col("typo_domain")),
            "left",
        )
        # Replace domain if typo found
        .withColumn(
            "fixed_email",
            F.when(
                F.col("correct_domain").isNotNull(),
                F.concat(F.col("username"), F.lit("@"), F.col("correct_domain")),
            ).otherwise(F.col(email_col)),
        )
        .select("original_email", "fixed_email")
    )

    return result_df


@pytest.mark.unit
class TestOptimizedEmailFunctions:
    """Test optimized email processing approaches."""

    def test_fix_typos_with_broadcast_join(self, spark):
        """Test typo fixing using broadcast join approach."""
        test_data = [
            ("user@gmai.com", "user@gmail.com"),
            ("admin@yahooo.com", "admin@yahoo.com"),
            ("test@hotmial.com", "test@hotmail.com"),
            ("valid@gmail.com", "valid@gmail.com"),  # Already correct
            ("user@unknown.com", "user@unknown.com"),  # No typo mapping
        ]

        # Create test DataFrame
        df = spark.createDataFrame([(row[0],) for row in test_data], ["email"])

        # Apply optimized typo fixing
        result_df = fix_typos_optimized(spark, df)

        # Collect results
        results = result_df.collect()

        # Verify each result
        for i, (original, expected) in enumerate(test_data):
            result = results[i]
            assert result["original_email"] == original
            assert (
                result["fixed_email"] == expected
            ), f"Failed for '{original}': expected '{expected}', got '{result['fixed_email']}'"

    def test_standardize_email_simplified(self, spark):
        """Test simplified email standardization without deep nesting."""
        test_data = [
            ("  user@gmail.com  ", "user@gmail.com"),
            ("USER@YAHOO.COM", "user@yahoo.com"),
            ("admin@HOTMAIL.COM", "admin@hotmail.com"),
        ]

        df = spark.createDataFrame(test_data, ["email", "expected"])

        # Apply individual operations instead of complex standardize_email
        result_df = (
            df.withColumn("cleaned", emails.remove_whitespace(F.col("email")))
            .withColumn("lowercased", emails.lowercase_email(F.col("cleaned")))
            .select("email", "expected", F.col("lowercased").alias("result"))
        )

        results = result_df.collect()
        for row in results:
            assert (
                row["result"] == row["expected"]
            ), f"Failed for '{row['email']}': expected '{row['expected']}', got '{row['result']}'"

    def test_gmail_normalization_simplified(self, spark):
        """Test Gmail normalization using individual operations."""
        test_data = [
            ("john.doe+work@gmail.com", "johndoe@gmail.com"),
            ("USER.NAME@GMAIL.COM", "username@gmail.com"),
            ("regular@yahoo.com", "regular@yahoo.com"),  # Not Gmail
        ]

        df = spark.createDataFrame(test_data, ["email", "expected"])

        # Check if Gmail and apply operations conditionally
        result_df = (
            df.withColumn("domain", emails.extract_domain(F.col("email")))
            .withColumn(
                "is_gmail",
                F.lower(F.col("domain")).isin(["gmail.com", "googlemail.com"]),
            )
            .withColumn(
                "processed",
                F.when(
                    F.col("is_gmail"),
                    # For Gmail: lowercase, remove dots, remove plus
                    emails.remove_dots_from_gmail(
                        emails.remove_plus_addressing(
                            emails.lowercase_email(F.col("email"))
                        )
                    ),
                ).otherwise(F.col("email")),
            )
            .select("email", "expected", F.col("processed").alias("result"))
        )

        results = result_df.collect()
        for row in results:
            assert (
                row["result"] == row["expected"]
            ), f"Failed for '{row['email']}': expected '{row['expected']}', got '{row['result']}'"


@pytest.mark.unit
class TestBestPractices:
    """Test recommended usage patterns for email functions."""

    def test_use_individual_functions(self, spark):
        """Demonstrate using individual functions instead of complex compositions."""
        email = "  John.Doe+work@Gmail.COM  "

        df = spark.createDataFrame([(email,)], ["email"])

        # Step-by-step processing (recommended)
        result_df = (
            df.withColumn("step1_trimmed", emails.remove_whitespace(F.col("email")))
            .withColumn(
                "step2_lowercase", emails.lowercase_email(F.col("step1_trimmed"))
            )
            .withColumn(
                "step3_no_plus", emails.remove_plus_addressing(F.col("step2_lowercase"))
            )
            .withColumn(
                "step4_no_dots", emails.remove_dots_from_gmail(F.col("step3_no_plus"))
            )
            .withColumn("final", F.col("step4_no_dots"))
        )

        result = result_df.first()
        assert result["final"] == "johndoe@gmail.com"

        # Each intermediate step is clear and debuggable
        assert result["step1_trimmed"] == "John.Doe+work@Gmail.COM"
        assert result["step2_lowercase"] == "john.doe+work@gmail.com"
        assert result["step3_no_plus"] == "john.doe@gmail.com"
        assert result["step4_no_dots"] == "johndoe@gmail.com"

    def test_batch_processing_pattern(self, spark):
        """Test efficient batch processing pattern."""
        # Large batch of emails
        test_emails = [
            "user1@gmail.com",
            "user2@yahoo.com",
            "user3@hotmail.com",
            "user4@company.com",
            "user5@outlook.com",
        ] * 100  # Simulate 500 emails

        df = spark.createDataFrame([(e,) for e in test_emails], ["email"])

        # Process in batch with simple operations
        result_df = (
            df.withColumn("domain", emails.extract_domain(F.col("email")))
            .withColumn("provider", emails.get_email_provider(F.col("email")))
            .withColumn("is_corporate", emails.is_corporate_email(F.col("email")))
            .groupBy("provider", "is_corporate")
            .count()
            .orderBy("count", ascending=False)
        )

        # Should complete without memory issues
        results = result_df.collect()
        assert len(results) > 0

        # Verify counts
        total = sum(row["count"] for row in results)
        assert total == 500
