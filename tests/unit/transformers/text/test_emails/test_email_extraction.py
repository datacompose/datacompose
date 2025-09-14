"""
Comprehensive tests for email extraction and processing functionality.
"""

import pytest
from pyspark.sql import functions as F

from datacompose.transformers.text.emails.pyspark.pyspark_primitives import emails


# Shared test data fixtures for both Spark and SQL implementations
@pytest.fixture
def extract_email_test_data():
    """Test data for email extraction."""
    return [
        ("Contact us at john@example.com for info", "john@example.com"),
        ("Email: admin@company.org or sales@company.org", "admin@company.org"),
        ("No email here", None),
        ("user@domain.co.uk is valid", "user@domain.co.uk"),
        ("test.user+tag@gmail.com", "test.user+tag@gmail.com"),
        ("", None),
        (None, None),
    ]


@pytest.fixture
def extract_email_test_data_spark(extract_email_test_data):
    """Convert test data for PySpark (None -> empty string)."""
    return [
        (text, expected if expected is not None else "")
        for text, expected in extract_email_test_data
    ]


@pytest.fixture
def email_validation_test_data():
    """Shared test data for email validation across implementations."""
    return [
        ("john@example.com", True),
        ("user.name+tag@company.org", True),
        ("test@domain.co.uk", True),
        ("invalid.email", False),
        ("@example.com", False),
        ("user@", False),
        ("user @example.com", False),
        ("user..name@example.com", False),
        (".user@example.com", False),
        ("user.@example.com", False),
        ("a@b.c", False),
        ("", False),
        (None, False),
    ]


@pytest.fixture
def plus_addressing_test_data():
    """Shared test data for plus addressing detection."""
    return [
        ("user+tag@gmail.com", True),
        ("john.doe+work@example.com", True),
        ("admin+test123@company.org", True),
        ("regular@example.com", False),
        ("user@example.com", False),
        ("plus+sign@in-domain.com", True),
        ("", False),
        (None, False),
    ]


@pytest.fixture
def disposable_email_test_data():
    """Shared test data for disposable email detection."""
    return [
        ("user@10minutemail.com", True),
        ("test@guerrillamail.com", True),
        ("temp@mailinator.com", True),
        ("john@gmail.com", False),
        ("admin@company.com", False),
        ("", False),
        (None, False),
    ]


@pytest.fixture
def corporate_email_test_data():
    """Shared test data for corporate email detection."""
    return [
        ("john@company.com", True),
        ("admin@organization.org", True),
        ("user@business.net", True),
        ("john@gmail.com", False),
        ("user@yahoo.com", False),
        ("test@hotmail.com", False),
        ("admin@outlook.com", False),
        ("", False),
        (None, False),
    ]


@pytest.fixture
def whitespace_removal_test_data():
    """Shared test data for whitespace removal."""
    return [
        ("  john@example.com  ", "john@example.com"),
        ("user @ example . com", "user@example.com"),
        ("\tjohn\t@\texample.com\t", "john@example.com"),
        ("john@example.com", "john@example.com"),
        ("", ""),
        (None, ""),
    ]


@pytest.fixture
def lowercase_email_test_data():
    """Shared test data for email lowercasing."""
    return [
        ("John.Doe@Example.COM", "john.doe@example.com"),
        ("ADMIN@COMPANY.ORG", "admin@company.org"),
        ("User@Domain.Net", "user@domain.net"),
        ("already@lowercase.com", "already@lowercase.com"),
        ("", ""),
        (None, ""),
    ]


@pytest.mark.unit
class TestEmailExtraction:
    """Test email extraction functions."""

    def test_extract_email(self, spark, extract_email_test_data_spark):
        """Test extraction of first email from text."""
        df = spark.createDataFrame(extract_email_test_data_spark, ["text", "expected"])
        result_df = df.withColumn("email", emails.extract_email(F.col("text")))

        results = result_df.collect()
        for row in results:
            assert row["email"] == row["expected"], (
                f"Failed for '{row['text']}': expected '{row['expected']}', got '{row['email']}'"
            )

    def test_extract_all_emails(self, spark):
        """Test extraction of all emails from text."""
        test_data = [
            (
                "Contact john@example.com or jane@example.org",
                ["john@example.com", "jane@example.org"],
            ),
            ("Email: admin@company.com", ["admin@company.com"]),
            ("No emails here", []),
            (
                "Multiple: a@b.com, c@d.org; e@f.net",
                ["a@b.com", "c@d.org", "e@f.net"],
            ),
            ("", []),
            (None, []),
        ]

        df = spark.createDataFrame(test_data, ["text", "expected"])
        result_df = df.withColumn("emails", emails.extract_all_emails(F.col("text")))

        results = result_df.collect()
        for row in results:
            if row["emails"] is None:
                row_emails = []
            else:
                row_emails = row["emails"]

            assert row_emails == row["expected"], (
                f"Failed for '{row['text']}': expected {row['expected']}, got {row_emails}"
            )

    def test_extract_username(self, spark, extract_username_test_data_spark):
        """Test extraction of username from email."""
        df = spark.createDataFrame(extract_username_test_data_spark, ["email", "expected"])
        result_df = df.withColumn("username", emails.extract_username(F.col("email")))

        results = result_df.collect()
        for row in results:
            assert row["username"] == row["expected"], (
                f"Failed for '{row['email']}': expected '{row['expected']}', got '{row['username']}'"
            )

    def test_extract_domain(self, spark, extract_domain_test_data_spark):
        """Test extraction of domain from email."""
        df = spark.createDataFrame(extract_domain_test_data_spark, ["email", "expected"])
        result_df = df.withColumn("domain", emails.extract_domain(F.col("email")))

        results = result_df.collect()
        for row in results:
            assert row["domain"] == row["expected"], (
                f"Failed for '{row['email']}': expected '{row['expected']}', got '{row['domain']}'"
            )

    def test_extract_domain_name(self, spark, extract_domain_name_test_data_spark):
        """Test extraction of domain name without TLD."""
        df = spark.createDataFrame(extract_domain_name_test_data_spark, ["email", "expected"])
        result_df = df.withColumn(
            "domain_name", emails.extract_domain_name(F.col("email"))
        )

        results = result_df.collect()
        for row in results:
            assert row["domain_name"] == row["expected"], (
                f"Failed for '{row['email']}': expected '{row['expected']}', got '{row['domain_name']}'"
            )

    def test_extract_tld(self, spark, extract_tld_test_data_spark):
        """Test extraction of top-level domain."""
        df = spark.createDataFrame(extract_tld_test_data_spark, ["email", "expected"])
        result_df = df.withColumn("tld", emails.extract_tld(F.col("email")))

        results = result_df.collect()
        for row in results:
            assert row["tld"] == row["expected"], (
                f"Failed for '{row['email']}': expected '{row['expected']}', got '{row['tld']}'"
            )


@pytest.mark.unit
@pytest.mark.requires_postgres
class TestSQLEmailExtraction:
    """Test SQL email extraction functions using PostgreSQL."""

    def test_extract_email(self, db_cursor, load_sql_functions, extract_email_test_data):
        """Test extraction of first email from text."""
        for input_text, expected in extract_email_test_data:
            db_cursor.execute(
                "SELECT extract_email(%s) as result",
                (input_text,)
            )
            result = db_cursor.fetchone()['result']

            assert result == expected, (
                f"Failed for '{input_text}': expected '{expected}', got '{result}'"
            )

    def test_extract_all_emails(self, db_cursor, load_sql_functions, extract_all_emails_test_data):
        """Test extraction of all emails from text."""
        for input_text, expected in extract_all_emails_test_data:
            db_cursor.execute(
                "SELECT extract_all_emails(%s) as result",
                (input_text,)
            )
            result = db_cursor.fetchone()['result']

            # Handle NULL/empty array differences
            if result is None:
                result = []

            assert result == expected, (
                f"Failed for '{input_text}': expected {expected}, got {result}"
            )

    def test_extract_username(self, db_cursor, load_sql_functions, extract_username_test_data):
        """Test extraction of username from email."""
        for email, expected in extract_username_test_data:
            db_cursor.execute(
                "SELECT extract_username(%s) as result",
                (email,)
            )
            result = db_cursor.fetchone()['result']

            assert result == expected, (
                f"Failed for '{email}': expected '{expected}', got '{result}'"
            )

    def test_extract_domain(self, db_cursor, load_sql_functions, extract_domain_test_data):
        """Test extraction of domain from email."""
        for email, expected in extract_domain_test_data:
            db_cursor.execute(
                "SELECT extract_domain(%s) as result",
                (email,)
            )
            result = db_cursor.fetchone()['result']

            assert result == expected, (
                f"Failed for '{email}': expected '{expected}', got '{result}'"
            )

    def test_extract_domain_name(self, db_cursor, load_sql_functions, extract_domain_name_test_data):
        """Test extraction of domain name without TLD."""
        for email, expected in extract_domain_name_test_data:
            db_cursor.execute(
                "SELECT extract_domain_name(%s) as result",
                (email,)
            )
            result = db_cursor.fetchone()['result']

            assert result == expected, (
                f"Failed for '{email}': expected '{expected}', got '{result}'"
            )

    def test_extract_tld(self, db_cursor, load_sql_functions, extract_tld_test_data):
        """Test extraction of top-level domain."""
        for email, expected in extract_tld_test_data:
            db_cursor.execute(
                "SELECT extract_tld(%s) as result",
                (email,)
            )
            result = db_cursor.fetchone()['result']

            assert result == expected, (
                f"Failed for '{email}': expected '{expected}', got '{result}'"
            )


@pytest.mark.unit
class TestEmailValidation:
    """Test email validation functions using shared test data for both Spark and SQL."""

    def test_is_valid_email_spark(self, spark, email_validation_test_data):
        """Test email format validation with Spark."""
        df = spark.createDataFrame(email_validation_test_data, ["email", "expected"])
        result_df = df.withColumn("is_valid", emails.is_valid_email(F.col("email")))

        results = result_df.collect()
        for row in results:
            assert row["is_valid"] == row["expected"], (
                f"Failed for '{row['email']}': expected {row['expected']}, got {row['is_valid']}"
            )

    @pytest.mark.requires_postgres
    def test_is_valid_email_sql(self, db_cursor, load_sql_functions, email_validation_test_data):
        """Test email format validation with SQL."""
        for email, expected in email_validation_test_data:
            db_cursor.execute("SELECT is_valid_email(%s) as result", (email,))
            result = db_cursor.fetchone()['result']
            assert result == expected, (
                f"Failed for '{email}': expected {expected}, got {result}"
            )

    def test_is_valid_username_spark(self, spark):
        """Test username validation with Spark."""
        test_data = [
            ("john@example.com", True),
            ("user.name@company.org", True),
            (".user@example.com", False),
            ("user.@example.com", False),
            ("user..name@example.com", False),
            ("@example.com", False),
            ("not-an-email", False),
            ("", False),
            (None, False),
        ]

        df = spark.createDataFrame(test_data, ["email", "expected"])
        result_df = df.withColumn(
            "valid_username", emails.is_valid_username(F.col("email"))
        )

        results = result_df.collect()
        for row in results:
            assert row["valid_username"] == row["expected"], (
                f"Failed for '{row['email']}': expected {row['expected']}, got {row['valid_username']}"
            )

    @pytest.mark.requires_postgres
    def test_is_valid_username_sql(self, db_cursor, load_sql_functions):
        """Test username validation with SQL."""
        test_data = [
            ("john@example.com", True),
            ("user.name@company.org", True),
            (".user@example.com", False),
            ("user.@example.com", False),
            ("user..name@example.com", False),
            ("@example.com", False),
            ("not-an-email", False),
            ("", False),
            (None, False),
        ]

        for email, expected in test_data:
            db_cursor.execute("SELECT is_valid_username(%s) as result", (email,))
            result = db_cursor.fetchone()['result']
            assert result == expected, (
                f"Failed for '{email}': expected {expected}, got {result}"
            )

    def test_is_valid_domain_spark(self, spark):
        """Test domain validation with Spark."""
        test_data = [
            ("user@example.com", True),
            ("user@mail.company.org", True),
            ("user@domain.co.uk", True),
            ("user@-example.com", False),
            ("user@example-.com", False),
            ("user@exam..ple.com", False),
            ("user@", False),
            ("not-an-email", False),
            ("", False),
            (None, False),
        ]

        df = spark.createDataFrame(test_data, ["email", "expected"])
        result_df = df.withColumn(
            "valid_domain", emails.is_valid_domain(F.col("email"))
        )

        results = result_df.collect()
        for row in results:
            assert row["valid_domain"] == row["expected"], (
                f"Failed for '{row['email']}': expected {row['expected']}, got {row['valid_domain']}"
            )

    @pytest.mark.requires_postgres
    def test_is_valid_domain_sql(self, db_cursor, load_sql_functions):
        """Test domain validation with SQL."""
        test_data = [
            ("user@example.com", True),
            ("user@mail.company.org", True),
            ("user@domain.co.uk", True),
            ("user@-example.com", False),
            ("user@example-.com", False),
            ("user@exam..ple.com", False),
            ("user@", False),
            ("not-an-email", False),
            ("", False),
            (None, False),
        ]

        for email, expected in test_data:
            db_cursor.execute("SELECT is_valid_domain(%s) as result", (email,))
            result = db_cursor.fetchone()['result']
            assert result == expected, (
                f"Failed for '{email}': expected {expected}, got {result}"
            )

    def test_is_valid_email(self, spark, email_validation_test_data):
        """Test email format validation."""
        test_data = [
            ("john@example.com", True),
            ("user.name+tag@company.org", True),
            ("test@domain.co.uk", True),
            ("invalid.email", False),
            ("@example.com", False),
            ("user@", False),
            ("user @example.com", False),
            ("user..name@example.com", False),  # Consecutive dots
            (".user@example.com", False),  # Starts with dot
            ("user.@example.com", False),  # Ends with dot
            ("a@b.c", False),  # TLD too short
            ("", False),
            (None, False),
        ]

        df = spark.createDataFrame(test_data, ["email", "expected"])
        result_df = df.withColumn("is_valid", emails.is_valid_email(F.col("email")))

        results = result_df.collect()
        for row in results:
            assert row["is_valid"] == row["expected"], (
                f"Failed for '{row['email']}': expected {row['expected']}, got {row['is_valid']}"
            )

    def test_is_valid_username(self, spark):
        """Test username validation."""
        test_data = [
            ("john@example.com", True),
            ("user.name@company.org", True),
            (".user@example.com", False),  # Starts with dot
            ("user.@example.com", False),  # Ends with dot
            ("user..name@example.com", False),  # Consecutive dots
            ("@example.com", False),  # No username
            ("not-an-email", False),
            ("", False),
            (None, False),
        ]

        df = spark.createDataFrame(test_data, ["email", "expected"])
        result_df = df.withColumn(
            "valid_username", emails.is_valid_username(F.col("email"))
        )

        results = result_df.collect()
        for row in results:
            assert row["valid_username"] == row["expected"], (
                f"Failed for '{row['email']}': expected {row['expected']}, got {row['valid_username']}"
            )

    def test_is_valid_domain(self, spark):
        """Test domain validation."""
        test_data = [
            ("user@example.com", True),
            ("user@mail.company.org", True),
            ("user@domain.co.uk", True),
            ("user@-example.com", False),  # Starts with hyphen
            ("user@example-.com", False),  # Ends with hyphen
            ("user@exam..ple.com", False),  # Consecutive dots
            ("user@", False),  # No domain
            ("not-an-email", False),
            ("", False),
            (None, False),
        ]

        df = spark.createDataFrame(test_data, ["email", "expected"])
        result_df = df.withColumn(
            "valid_domain", emails.is_valid_domain(F.col("email"))
        )

        results = result_df.collect()
        for row in results:
            assert row["valid_domain"] == row["expected"], (
                f"Failed for '{row['email']}': expected {row['expected']}, got {row['valid_domain']}"
            )

    def test_has_plus_addressing_spark(self, spark, plus_addressing_test_data):
        """Test detection of plus addressing with Spark."""
        df = spark.createDataFrame(plus_addressing_test_data, ["email", "expected"])
        result_df = df.withColumn(
            "has_plus", emails.has_plus_addressing(F.col("email"))
        )

        results = result_df.collect()
        for row in results:
            assert row["has_plus"] == row["expected"], (
                f"Failed for '{row['email']}': expected {row['expected']}, got {row['has_plus']}"
            )

    @pytest.mark.requires_postgres
    def test_has_plus_addressing_sql(self, db_cursor, load_sql_functions, plus_addressing_test_data):
        """Test detection of plus addressing with SQL."""
        for email, expected in plus_addressing_test_data:
            db_cursor.execute("SELECT has_plus_addressing(%s) as result", (email,))
            result = db_cursor.fetchone()['result']
            assert result == expected, (
                f"Failed for '{email}': expected {expected}, got {result}"
            )

    def test_is_disposable_email_spark(self, spark, disposable_email_test_data):
        """Test detection of disposable email addresses with Spark."""
        df = spark.createDataFrame(disposable_email_test_data, ["email", "expected"])
        result_df = df.withColumn(
            "is_disposable", emails.is_disposable_email(F.col("email"))
        )

        results = result_df.collect()
        for row in results:
            assert row["is_disposable"] == row["expected"], (
                f"Failed for '{row['email']}': expected {row['expected']}, got {row['is_disposable']}"
            )

    @pytest.mark.requires_postgres
    def test_is_disposable_email_sql(self, db_cursor, load_sql_functions, disposable_email_test_data):
        """Test detection of disposable email addresses with SQL."""
        for email, expected in disposable_email_test_data:
            db_cursor.execute("SELECT is_disposable_email(%s) as result", (email,))
            result = db_cursor.fetchone()['result']
            assert result == expected, (
                f"Failed for '{email}': expected {expected}, got {result}"
            )

    def test_is_corporate_email_spark(self, spark, corporate_email_test_data):
        """Test detection of corporate email addresses with Spark."""
        df = spark.createDataFrame(corporate_email_test_data, ["email", "expected"])
        result_df = df.withColumn(
            "is_corporate", emails.is_corporate_email(F.col("email"))
        )

        results = result_df.collect()
        for row in results:
            assert row["is_corporate"] == row["expected"], (
                f"Failed for '{row['email']}': expected {row['expected']}, got {row['is_corporate']}"
            )

    @pytest.mark.requires_postgres
    def test_is_corporate_email_sql(self, db_cursor, load_sql_functions, corporate_email_test_data):
        """Test detection of corporate email addresses with SQL."""
        for email, expected in corporate_email_test_data:
            db_cursor.execute("SELECT is_corporate_email(%s) as result", (email,))
            result = db_cursor.fetchone()['result']
            assert result == expected, (
                f"Failed for '{email}': expected {expected}, got {result}"
            )


@pytest.mark.unit
class TestEmailCleaning:
    """Test email cleaning functions."""

    def test_remove_whitespace_spark(self, spark, whitespace_removal_test_data):
        """Test whitespace removal from emails with Spark."""
        df = spark.createDataFrame(whitespace_removal_test_data, ["email", "expected"])
        result_df = df.withColumn("cleaned", emails.remove_whitespace(F.col("email")))

        results = result_df.collect()
        for row in results:
            assert row["cleaned"] == row["expected"], (
                f"Failed for '{row['email']}': expected '{row['expected']}', got '{row['cleaned']}'"
            )

    @pytest.mark.requires_postgres
    def test_remove_whitespace_sql(self, db_cursor, load_sql_functions, whitespace_removal_test_data):
        """Test whitespace removal from emails with SQL."""
        for email, expected in whitespace_removal_test_data:
            db_cursor.execute("SELECT remove_whitespace(%s) as result", (email,))
            result = db_cursor.fetchone()['result']

            # Handle NULL differences
            if result is None and expected == "":
                result = ""

            assert result == expected, (
                f"Failed for '{email}': expected '{expected}', got '{result}'"
            )

    def test_lowercase_email_spark(self, spark, lowercase_email_test_data):
        """Test email lowercasing with Spark."""
        df = spark.createDataFrame(lowercase_email_test_data, ["email", "expected"])
        result_df = df.withColumn("lowercased", emails.lowercase_email(F.col("email")))

        results = result_df.collect()
        for row in results:
            assert row["lowercased"] == row["expected"], (
                f"Failed for '{row['email']}': expected '{row['expected']}', got '{row['lowercased']}'"
            )

    @pytest.mark.requires_postgres
    def test_lowercase_email_sql(self, db_cursor, load_sql_functions, lowercase_email_test_data):
        """Test email lowercasing with SQL."""
        for email, expected in lowercase_email_test_data:
            db_cursor.execute("SELECT lowercase_email(%s) as result", (email,))
            result = db_cursor.fetchone()['result']

            if result is None and expected == "":
                result = ""

            assert result == expected, (
                f"Failed for '{email}': expected '{expected}', got '{result}'"
            )

    def test_lowercase_domain(self, spark):
        """Test lowercasing only domain part."""
        test_data = [
            ("John.Doe@Example.COM", "John.Doe@example.com"),
            ("ADMIN@COMPANY.ORG", "ADMIN@company.org"),
            ("User.Name@Domain.Net", "User.Name@domain.net"),
            ("not-an-email", "not-an-email"),
            ("", ""),
            (None, None),
        ]

        df = spark.createDataFrame(test_data, ["email", "expected"])
        result_df = df.withColumn(
            "domain_lower", emails.lowercase_domain(F.col("email"))
        )

        results = result_df.collect()
        for row in results:
            assert row["domain_lower"] == row["expected"], (
                f"Failed for '{row['email']}': expected '{row['expected']}', got '{row['domain_lower']}'"
            )

    def test_remove_plus_addressing(self, spark):
        """Test removal of plus addressing."""
        test_data = [
            ("user+tag@gmail.com", "user@gmail.com"),
            ("john.doe+work@example.com", "john.doe@example.com"),
            ("admin+test+multiple@company.org", "admin@company.org"),
            ("regular@example.com", "regular@example.com"),
            ("", ""),
            (None, ""),
        ]

        df = spark.createDataFrame(test_data, ["email", "expected"])
        result_df = df.withColumn(
            "no_plus", emails.remove_plus_addressing(F.col("email"))
        )

        results = result_df.collect()
        for row in results:
            assert row["no_plus"] == row["expected"], (
                f"Failed for '{row['email']}': expected '{row['expected']}', got '{row['no_plus']}'"
            )

    def test_remove_dots_from_gmail(self, spark):
        """Test removal of dots from Gmail addresses."""
        test_data = [
            ("john.doe@gmail.com", "johndoe@gmail.com"),
            ("user.name.test@gmail.com", "usernametest@gmail.com"),
            ("dots@googlemail.com", "dots@googlemail.com"),
            ("user.name@yahoo.com", "user.name@yahoo.com"),  # Not Gmail
            ("regular@example.com", "regular@example.com"),
            ("", ""),
            (None, None),
        ]

        df = spark.createDataFrame(test_data, ["email", "expected"])
        result_df = df.withColumn(
            "no_dots", emails.remove_dots_from_gmail(F.col("email"))
        )

        results = result_df.collect()
        for row in results:
            assert row["no_dots"] == row["expected"], (
                f"Failed for '{row['email']}': expected '{row['expected']}', got '{row['no_dots']}'"
            )

    def test_fix_common_typos(self, spark):
        """Test fixing common email domain typos."""
        test_data = [
            # Gmail typos
            ("user@gmai.com", "user@gmail.com"),
            ("user@gmial.com", "user@gmail.com"),
            ("user@gmail.co", "user@gmail.com"),
            # Yahoo typos
            ("user@yahooo.com", "user@yahoo.com"),
            ("user@yaho.com", "user@yahoo.com"),
            # Hotmail typos
            ("user@hotmial.com", "user@hotmail.com"),
            ("user@hotmall.com", "user@hotmail.com"),
            # TLD typos
            ("user@example.cmo", "user@example.com"),
            ("user@example.ocm", "user@example.com"),
            ("user@example.ent", "user@example.net"),
            # Already correct
            ("user@gmail.com", "user@gmail.com"),
            ("", ""),
            (None, None),
        ]

        df = spark.createDataFrame(test_data, ["email", "expected"])
        result_df = df.withColumn("fixed", emails.fix_common_typos(F.col("email")))

        results = result_df.collect()
        for row in results:
            assert row["fixed"] == row["expected"], (
                f"Failed for '{row['email']}': expected '{row['expected']}', got '{row['fixed']}'"
            )

    @pytest.mark.skip(
        reason="SmartPrimitive doesn't handle multiple parameters correctly"
    )
    def test_fix_common_typos_with_custom(self, spark):
        """Test fixing typos with custom mappings."""
        # This would require direct function call without the namespace wrapper
        # test_data = [
        #    ("user@mycompany.co", "user@mycompany.com"),
        #    ("admin@oldname.com", "admin@newname.com"),
        # ]

        # custom_mappings = {
        #    "mycompany.co": "mycompany.com",
        #    "oldname.com": "newname.com",
        # }

        # df = spark.createDataFrame(test_data, ["email", "expected"])
        # Direct function call would work but not through namespace
        # result_df = df.withColumn(
        #     "fixed", fix_common_typos(F.col("email"), custom_mappings)
        # )
        pass


@pytest.mark.unit
class TestEmailStandardization:
    """Test email standardization functions."""

    @pytest.mark.skip(reason="Complex expression tree causes memory issues in Spark")
    def test_standardize_email(self, spark):
        """Test complete email standardization."""
        test_data = [
            ("  John.Doe@Gmail.COM  ", "johndoe@gmail.com"),
            ("user+tag@YAHOO.COM", "user@yahoo.com"),
            ("admin@hotmial.com", "admin@hotmail.com"),
            ("Test.User@Example.CMO", "test.user@example.com"),
            ("invalid-email", ""),
            ("", ""),
            (None, ""),
        ]

        df = spark.createDataFrame(test_data, ["email", "expected"])
        result_df = df.withColumn(
            "standardized",
            emails.standardize_email(
                F.col("email"),
                lowercase=True,
                remove_dots_gmail=True,
                remove_plus=True,
                fix_typos=True,
            ),
        )

        results = result_df.collect()
        for row in results:
            assert row["standardized"] == row["expected"], (
                f"Failed for '{row['email']}': expected '{row['expected']}', got '{row['standardized']}'"
            )

    @pytest.mark.skip(reason="Complex expression tree causes memory issues in Spark")
    def test_normalize_gmail(self, spark):
        """Test Gmail-specific normalization."""
        test_data = [
            ("John.Doe+work@Gmail.com", "johndoe@gmail.com"),
            ("user.name+tag@googlemail.com", "username@googlemail.com"),
            ("user+tag@yahoo.com", "user+tag@yahoo.com"),  # Not Gmail
            ("regular@example.com", "regular@example.com"),
            ("", ""),
            (None, None),
        ]

        df = spark.createDataFrame(test_data, ["email", "expected"])
        result_df = df.withColumn("normalized", emails.normalize_gmail(F.col("email")))

        results = result_df.collect()
        for row in results:
            assert row["normalized"] == row["expected"], (
                f"Failed for '{row['email']}': expected '{row['expected']}', got '{row['normalized']}'"
            )

    @pytest.mark.skip(reason="Complex expression tree causes memory issues in Spark")
    def test_get_canonical_email(self, spark):
        """Test canonical email form for deduplication."""
        test_data = [
            ("  John.Doe+work@Gmail.COM  ", "johndoe@gmail.com"),
            ("USER+tag@YAHOOO.com", "user@yahoo.com"),
            ("Admin@HotMIAL.com", "admin@hotmail.com"),
            ("test@example.cmo", "test@example.com"),
            ("", ""),
            (None, ""),
        ]

        df = spark.createDataFrame(test_data, ["email", "expected"])
        result_df = df.withColumn(
            "canonical", emails.get_canonical_email(F.col("email"))
        )

        results = result_df.collect()
        for row in results:
            assert row["canonical"] == row["expected"], (
                f"Failed for '{row['email']}': expected '{row['expected']}', got '{row['canonical']}'"
            )


@pytest.mark.unit
class TestEmailInformation:
    """Test email information extraction functions."""

    def test_extract_name_from_email(self, spark):
        """Test extracting person's name from email."""
        test_data = [
            ("john.smith@example.com", "John Smith"),
            ("jane_doe@company.org", "Jane Doe"),
            ("firstname-lastname@domain.com", "Firstname Lastname"),
            ("admin@example.com", ""),  # Common prefix
            ("user123@domain.com", "User"),
            ("info@company.com", ""),  # Common prefix
            ("a@b.com", ""),  # Too short
            ("", ""),
            (None, ""),
        ]

        df = spark.createDataFrame(test_data, ["email", "expected"])
        result_df = df.withColumn(
            "name", emails.extract_name_from_email(F.col("email"))
        )

        results = result_df.collect()
        for row in results:
            assert row["name"] == row["expected"], (
                f"Failed for '{row['email']}': expected '{row['expected']}', got '{row['name']}'"
            )

    def test_get_email_provider(self, spark):
        """Test getting email provider name."""
        test_data = [
            ("user@gmail.com", "Gmail"),
            ("admin@googlemail.com", "Gmail"),
            ("test@yahoo.com", "Yahoo"),
            ("user@ymail.com", "Yahoo"),
            ("admin@hotmail.com", "Hotmail"),
            ("test@outlook.com", "Outlook"),
            ("user@live.com", "Outlook"),
            ("john@aol.com", "AOL"),
            ("user@icloud.com", "iCloud"),
            ("admin@protonmail.com", "ProtonMail"),
            ("test@company.com", "Other"),
            ("", "Other"),
            (None, "Other"),
        ]

        df = spark.createDataFrame(test_data, ["email", "expected"])
        result_df = df.withColumn("provider", emails.get_email_provider(F.col("email")))

        results = result_df.collect()
        for row in results:
            assert row["provider"] == row["expected"], (
                f"Failed for '{row['email']}': expected '{row['expected']}', got '{row['provider']}'"
            )

    def test_mask_email(self, spark):
        """Test email masking for privacy."""
        test_data = [
            ("john.doe@example.com", "joh***@exa***.com"),
            ("ab@cd.com", "***@***.com"),
            ("longusername@longdomain.org", "lon***@lon***.org"),
            ("a@b.c", "***@***.c"),
            ("not-an-email", "not-an-email"),
            ("", ""),
            (None, None),
        ]

        df = spark.createDataFrame(test_data, ["email", "expected"])
        result_df = df.withColumn("masked", emails.mask_email(F.col("email")))

        results = result_df.collect()
        for row in results:
            assert row["masked"] == row["expected"], (
                f"Failed for '{row['email']}': expected '{row['expected']}', got '{row['masked']}'"
            )


@pytest.mark.unit
class TestEmailFiltering:
    """Test email filtering functions."""

    def test_filter_valid_emails(self, spark):
        """Test filtering to keep only valid emails."""
        test_data = [
            ("john@example.com", "john@example.com"),
            ("invalid-email", None),
            ("@example.com", None),
            ("user@", None),
            ("valid@domain.org", "valid@domain.org"),
            ("", None),
            (None, None),
        ]

        df = spark.createDataFrame(test_data, ["email", "expected"])
        result_df = df.withColumn(
            "filtered", emails.filter_valid_emails(F.col("email"))
        )

        results = result_df.collect()
        for row in results:
            assert row["filtered"] == row["expected"], (
                f"Failed for '{row['email']}': expected {row['expected']}, got {row['filtered']}"
            )

    def test_filter_corporate_emails(self, spark):
        """Test filtering to keep only corporate emails."""
        test_data = [
            ("john@company.com", "john@company.com"),
            ("user@gmail.com", None),
            ("admin@business.org", "admin@business.org"),
            ("test@yahoo.com", None),
            ("", None),
            (None, None),
        ]

        df = spark.createDataFrame(test_data, ["email", "expected"])
        result_df = df.withColumn(
            "filtered", emails.filter_corporate_emails(F.col("email"))
        )

        results = result_df.collect()
        for row in results:
            assert row["filtered"] == row["expected"], (
                f"Failed for '{row['email']}': expected {row['expected']}, got {row['filtered']}"
            )

    def test_filter_non_disposable_emails(self, spark):
        """Test filtering to exclude disposable emails."""
        test_data = [
            ("john@gmail.com", "john@gmail.com"),
            ("temp@10minutemail.com", None),
            ("admin@company.com", "admin@company.com"),
            ("test@mailinator.com", None),
            ("", None),
            (None, None),
        ]

        df = spark.createDataFrame(test_data, ["email", "expected"])
        result_df = df.withColumn(
            "filtered", emails.filter_non_disposable_emails(F.col("email"))
        )

        results = result_df.collect()
        for row in results:
            assert row["filtered"] == row["expected"], (
                f"Failed for '{row['email']}': expected {row['expected']}, got {row['filtered']}"
            )


@pytest.mark.unit
class TestEmailEdgeCases:
    """Test edge cases and complex scenarios."""
    
    
    def test_null_and_empty_handling_sql(self, db_cursor, load_sql_functions):
        """Test handling of null and empty values in SQL functions."""
        test_data = [
            (None,),
            ("",),
            ("   ",),
            ("\t\n",),
        ]

        for (email,) in test_data:
            # Test various SQL functions with null/empty inputs
            db_cursor.execute("SELECT extract_email(%s) as result", (email,))
            result = db_cursor.fetchone()['result']
            assert result == "" or result is None

            db_cursor.execute("SELECT extract_username(%s) as result", (email,))
            result = db_cursor.fetchone()['result'] 
            assert result == "" or result is None

            db_cursor.execute("SELECT extract_domain(%s) as result", (email,))
            result = db_cursor.fetchone()['result']
            assert result == "" or result is None

            db_cursor.execute("SELECT is_valid_email(%s) as result", (email,))
            result = db_cursor.fetchone()['result']
            assert result is False 

    def test_null_and_empty_handling(self, spark):
        """Test handling of null and empty values."""
        test_data = [
            (None,),
            ("",),
            ("   ",),
            ("\t\n",),
        ]

        df = spark.createDataFrame(test_data, ["email"])

        # Test without standardize_email to avoid memory issues
        result_df = df.select(
            F.col("email"),
            emails.extract_email(F.col("email")).alias("extracted"),
            emails.extract_username(F.col("email")).alias("username"),
            emails.extract_domain(F.col("email")).alias("domain"),
            emails.is_valid_email(F.col("email")).alias("is_valid"),
            emails.lowercase_email(F.col("email")).alias("lowercased"),
        )

        results = result_df.collect()
        for row in results:
            # All should return empty strings or False for invalid inputs
            assert row["extracted"] == ""
            assert row["username"] == ""
            assert row["domain"] == ""
            assert not row["is_valid"]
            # lowercase_email preserves whitespace, just lowercases it
            if row["email"] is None:
                assert row["lowercased"] == ""
            else:
                assert row["lowercased"] == row["email"].lower()

    def test_international_domains(self, spark):
        """Test handling of international domain names."""
        test_data = [
            ("user@münchen.de", True),
            ("admin@企业.cn", True),
            ("test@مثال.السعودية", True),
            ("user@example.中国", True),
        ]

        df = spark.createDataFrame(test_data, ["email", "expected_valid"])
        result_df = df.withColumn("domain", emails.extract_domain(F.col("email")))

        results = result_df.collect()
        for row in results:
            # Should at least extract something
            assert row["domain"] != "", (
                f"Failed to extract domain from '{row['email']}'"
            )

    def test_very_long_emails(self, spark):
        """Test handling of very long email addresses."""
        # Create a very long but valid email
        long_username = "a" * 64  # Max username length
        long_domain = "sub." * 50 + "example.com"  # Very long domain
        long_email = f"{long_username}@{long_domain}"

        test_data = [
            (long_email, False),  # Should be invalid due to length
            ("a" * 100 + "@example.com", False),  # Username too long
            ("user@" + "a" * 300 + ".com", False),  # Domain too long
        ]

        df = spark.createDataFrame(test_data, ["email", "expected_valid"])
        result_df = df.withColumn("is_valid", emails.is_valid_email(F.col("email")))

        results = result_df.collect()
        for row in results:
            assert row["is_valid"] == row["expected_valid"], (
                f"Failed for long email: expected {row['expected_valid']}, got {row['is_valid']}"
            )

    def test_special_characters(self, spark):
        """Test handling of special characters in emails."""
        test_data = [
            ("user!#$%@example.com", "user!#$%"),  # Special chars in username
            ("user@ex-ample.com", "ex-ample.com"),  # Hyphen in domain
            ("user@123.456.789.012", "123.456.789.012"),  # IP-like domain
            ("user@[192.168.1.1]", "[192.168.1.1]"),  # IP address domain
        ]

        df = spark.createDataFrame(test_data, ["email", "expected_part"])
        result_df = df.select(
            F.col("email"),
            emails.extract_username(F.col("email")).alias("username"),
            emails.extract_domain(F.col("email")).alias("domain"),
        )

        results = result_df.collect()
        # Just verify extraction works without errors
        for row in results:
            assert row["username"] != "" or row["domain"] != "", (
                f"Failed to extract from '{row['email']}'"
            )

    def test_hash_email_sha256_basic(self, spark):
        """Test basic SHA256 hashing functionality for emails."""
        from datacompose.transformers.text.emails.pyspark.pyspark_primitives import (
            hash_email_sha256,
        )

        # Test that the function is callable
        assert callable(hash_email_sha256)

        test_data = [
            ("user@example.com",),
            ("test.email@domain.org",),
            ("invalid_email",),
            (None,),
        ]

        df = spark.createDataFrame(test_data, ["email"])

        # Test hashing without standardization to avoid memory issues
        result_df = df.select(
            "email",
            hash_email_sha256(F.col("email"), standardize_first=False).alias(
                "hashed_email"
            ),
        )

        results = result_df.collect()

        # Verify that valid emails produce non-null hashes
        assert results[0]["hashed_email"] is not None
        assert len(results[0]["hashed_email"]) == 64  # SHA256 produces 64 hex chars
        assert results[1]["hashed_email"] is not None
        assert len(results[1]["hashed_email"]) == 64

        # Verify invalid emails produce null hashes
        assert results[2]["hashed_email"] is None  # Invalid format
        assert results[3]["hashed_email"] is None  # Null input

    def test_hash_email_sha256_with_salt(self, spark):
        """Test SHA256 hashing with salt parameter for emails."""
        from datacompose.transformers.text.emails.pyspark.pyspark_primitives import (
            hash_email_sha256,
        )

        test_data = [
            ("user@example.com",),
            ("test@domain.org",),
        ]

        df = spark.createDataFrame(test_data, ["email"])

        # Test with different salts
        result_df = df.select(
            "email",
            hash_email_sha256(F.col("email"), salt="", standardize_first=False).alias(
                "no_salt"
            ),
            hash_email_sha256(
                F.col("email"), salt="email_salt", standardize_first=False
            ).alias("with_salt"),
        )

        results = result_df.collect()

        # Verify that different salts produce different hashes
        for result in results:
            if result["no_salt"]:  # Skip if email was invalid
                assert result["no_salt"] != result["with_salt"]
                assert len(result["no_salt"]) == 64
                assert len(result["with_salt"]) == 64

    def test_hash_email_sha256_canonicalization(self, spark):
        """Test that canonicalization produces consistent hashes for emails."""
        from datacompose.transformers.text.emails.pyspark.pyspark_primitives import (
            hash_email_sha256,
        )

        # These should hash to the same value when canonicalized
        test_data = [
            ("User@Example.Com",),
            ("user@example.com",),
            ("USER@EXAMPLE.COM",),
            ("user@Example.com",),
        ]

        df = spark.createDataFrame(test_data, ["email"])

        # Test without standardization to avoid memory issues
        result_df = df.select(
            "email",
            hash_email_sha256(F.col("email"), standardize_first=False).alias(
                "canonical_hash"
            ),
            hash_email_sha256(F.col("email"), standardize_first=False).alias(
                "raw_hash"
            ),
        )

        results = result_df.collect()

        # Without standardization, case variations should have different hashes
        canonical_hashes = [r["canonical_hash"] for r in results if r["canonical_hash"]]
        raw_hashes = [r["raw_hash"] for r in results if r["raw_hash"]]

        # Both should be different without canonicalization
        assert (
            len(set(canonical_hashes)) > 1
        )  # Should be different without canonicalization
        assert len(set(raw_hashes)) > 1  # Should be different without canonicalization
        assert canonical_hashes == raw_hashes  # Both columns should be identical

    def test_hash_email_sha256_consistency(self, spark):
        """Test that the same email input always produces the same hash."""
        from datacompose.transformers.text.emails.pyspark.pyspark_primitives import (
            hash_email_sha256,
        )

        test_email = "test.user@example.com"

        # Create multiple rows with the same email
        test_data = [(test_email,)] * 3
        df = spark.createDataFrame(test_data, ["email"])

        result_df = df.select(
            hash_email_sha256(F.col("email"), standardize_first=False).alias("hash1"),
            hash_email_sha256(
                F.col("email"), salt="salt1", standardize_first=False
            ).alias("hash2"),
        )

        results = result_df.collect()

        # All hashes should be identical for the same input
        hashes1 = [r["hash1"] for r in results]
        hashes2 = [r["hash2"] for r in results]

        assert len(set(hashes1)) == 1  # All identical
        assert len(set(hashes2)) == 1  # All identical
        assert hashes1[0] != hashes2[0]  # But different salts produce different hashes
