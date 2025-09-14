"""
Root-level test configuration and shared fixtures.
"""

import logging
import os
import warnings

import pytest
from pyspark.sql import SparkSession

# PostgreSQL imports
try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
    POSTGRES_AVAILABLE = True
except ImportError:
    POSTGRES_AVAILABLE = False


@pytest.fixture(scope="session")
def spark():
    """Create a single Spark session for all tests."""
    # Suppress all warnings
    warnings.filterwarnings("ignore")

    # Suppress Spark logging
    logging.getLogger("py4j").setLevel(logging.ERROR)
    logging.getLogger("pyspark").setLevel(logging.ERROR)

    # Set Java options to suppress Ivy messages
    os.environ["SPARK_SUBMIT_OPTS"] = "-Divy.message.logger.level=ERROR"

    # Use SPARK_MASTER env var if available, otherwise local
    master = os.environ.get("SPARK_MASTER", "local[*]")

    spark = (
        SparkSession.builder.appName("DataComposeTests")
        .master(master)
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "false")
        .config("spark.python.worker.reuse", "true")
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse")
        .config("spark.driver.extraJavaOptions", "-Dlog4j.logger.org.apache.ivy=ERROR")
        .config(
            "spark.executor.extraJavaOptions", "-Dlog4j.logger.org.apache.ivy=ERROR"
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    yield spark
    spark.stop()


# Alias for backwards compatibility
@pytest.fixture(scope="session")
def sparksession(spark):
    """Alias for spark fixture for backwards compatibility."""
    return spark


# PostgreSQL Fixtures
@pytest.fixture(scope="session")
def postgres_connection():
    """Create PostgreSQL connection for testing SQL functions."""
    if not POSTGRES_AVAILABLE:
        pytest.skip("psycopg2 not installed, skipping PostgreSQL tests")

    try:
        conn = psycopg2.connect(
            host=os.environ.get("POSTGRES_HOST", "localhost"),
            port=os.environ.get("POSTGRES_PORT", 5432),
            database=os.environ.get("POSTGRES_DB", "datawarehouse"),
            user=os.environ.get("POSTGRES_USER", "postgres"),
            password=os.environ.get("POSTGRES_PASSWORD", "postgres123")
        )
        conn.autocommit = False
        return conn
    except psycopg2.OperationalError as e:
        pytest.skip(f"PostgreSQL not available: {e}")


@pytest.fixture(scope="session")
def postgres_db(postgres_connection):
    """Set up test schema for SQL function testing."""
    conn = postgres_connection

    with conn.cursor() as cur:
        # Create isolated test schema
        cur.execute("DROP SCHEMA IF EXISTS test_sql_functions CASCADE")
        cur.execute("CREATE SCHEMA test_sql_functions")
        cur.execute("SET search_path TO test_sql_functions")
        conn.commit()

    yield conn

    # Cleanup after all tests
    with conn.cursor() as cur:
        cur.execute("DROP SCHEMA IF EXISTS test_sql_functions CASCADE")
        conn.commit()
    conn.close()


@pytest.fixture
def db_cursor(postgres_db):
    """Provide a cursor with automatic rollback for each test."""
    cur = postgres_db.cursor(cursor_factory=RealDictCursor)
    cur.execute("SET search_path TO test_sql_functions")

    # Start a savepoint for this test
    cur.execute("SAVEPOINT test_savepoint")

    yield cur

    # Rollback to savepoint after test
    cur.execute("ROLLBACK TO SAVEPOINT test_savepoint")
    cur.close()


@pytest.fixture(scope="session")
def load_sql_functions(postgres_db):
    """Load SQL functions into test schema once per session."""
    from datacompose.transformers.text.emails.sql.sql_primitives import Emails

    with postgres_db.cursor() as cur:
        cur.execute("SET search_path TO test_sql_functions")

        # Load all email SQL functions
        emails = Emails("postgres")

        # Define loading order to respect function dependencies
        # Base functions first, then functions that depend on them
        method_names = [
            # Basic extraction functions (no dependencies)
            'extract_username', 'extract_domain', 'extract_domain_name', 'extract_tld',
            # Validation functions (no dependencies)
            'is_valid_email', 'is_valid_username', 'is_valid_domain',
            'has_plus_addressing', 'is_disposable_email', 'is_corporate_email',
            # Functions that depend on extraction functions
            'extract_name_from_email', 'get_email_provider', 'mask_email',
            # Cleaning functions (no dependencies)
            'remove_whitespace', 'to_lowercase', 'remove_dots_gmail', 'remove_plus_addressing',
            'normalize_gmail', 'fix_common_typos', 'standardize_email',
            # Filter functions (depend on validation functions)
            'filter_valid_emails', 'filter_corporate_emails', 'filter_non_disposable_emails',
        ]

        # Add any remaining methods not explicitly ordered
        all_methods = [
            method for method in dir(emails)
            if not method.startswith('_') and callable(getattr(emails, method)) and method != 'dialect'
        ]
        for method in all_methods:
            if method not in method_names:
                method_names.append(method)

        # Load functions one by one with individual transactions
        success_count = 0
        failed_functions = []

        for method_name in method_names:
            try:
                # Start a savepoint for each function
                cur.execute(f"SAVEPOINT {method_name}_load")

                sql_function = getattr(emails, method_name)()
                if sql_function and isinstance(sql_function, str) and sql_function.strip():
                    cur.execute(sql_function)
                    success_count += 1

                # Release the savepoint if successful
                cur.execute(f"RELEASE SAVEPOINT {method_name}_load")

            except Exception as e:
                # Rollback to savepoint on error
                try:
                    cur.execute(f"ROLLBACK TO SAVEPOINT {method_name}_load")
                    failed_functions.append((method_name, str(e)))
                except:
                    pass

        print(f"Successfully loaded {success_count} SQL functions")
        if failed_functions:
            print(f"Failed to load {len(failed_functions)} SQL functions:")
            for func_name, error in failed_functions[:3]:  # Show first 3 errors
                print(f"  - {func_name}: {error[:100]}...")
            if len(failed_functions) > 3:
                print(f"  ... and {len(failed_functions) - 3} more")

        postgres_db.commit()

    return postgres_db
