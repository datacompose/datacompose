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

        # Get all method names that return SQL functions (don't start with _)
        method_names = [
            method for method in dir(emails)
            if not method.startswith('_') and callable(getattr(emails, method)) and method != 'dialect'
        ]

        for method_name in method_names:
            try:
                sql_function = getattr(emails, method_name)()
                if sql_function and isinstance(sql_function, str) and sql_function.strip():
                    cur.execute(sql_function)
            except Exception as e:
                print(f"Warning: Failed to load SQL function {method_name}: {e}")

        postgres_db.commit()

    return postgres_db
