"""Debug tests for long email validation."""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from datacompose.transformers.text.clean_emails.pyspark.pyspark_primitives import emails


def test_debug_long_emails():
    """Debug why long emails are being validated differently than expected."""
    spark = SparkSession.builder.master("local").appName("test").getOrCreate()

    # Create a very long but valid email
    long_username = "a" * 64  # Max username length
    long_domain = "sub." * 50 + "example.com"  # Very long domain
    long_email = f"{long_username}@{long_domain}"

    test_data = [
        (long_email, f"64 char username, domain length: {len(long_domain)}", False),
        ("a" * 100 + "@example.com", "100 char username", False),
        ("user@" + "a" * 300 + ".com", "300 char domain name", False),
        ("normal@example.com", "normal email", True),
    ]

    df = spark.createDataFrame(test_data, ["email", "description", "expected"])
    result_df = df.withColumn("email_length", F.length(F.col("email")))
    result_df = result_df.withColumn("is_valid", emails.is_valid_email(F.col("email")))

    print("\nTesting long emails:")
    print("=" * 80)
    results = result_df.collect()
    for row in results:
        print(f"Description: {row['description']}")
        print(f"  Email length: {row['email_length']}")
        print(f"  Expected valid: {row['expected']}")
        print(f"  Got valid: {row['is_valid']}")
        print(f"  Match: {row['is_valid'] == row['expected']}")
        if row["email_length"] > 254:
            print("  -> Email exceeds max length of 254 chars")
        print()

    spark.stop()


if __name__ == "__main__":
    test_debug_long_emails()
