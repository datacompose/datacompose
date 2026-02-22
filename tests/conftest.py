"""
Root-level test configuration and shared fixtures.
"""

import logging
import os
import warnings

import pytest

from datacompose.functions import set_backend

# Backends to test against
BACKENDS = ["duckdb", "pyspark"]


@pytest.fixture(scope="session", params=BACKENDS)
def backend(request):
    """Parameterized backend fixture - tests run against each backend."""
    return request.param


@pytest.fixture(scope="session")
def setup_backend(backend):
    """Set the SQLFrame backend for the current test session."""
    set_backend(backend)
    yield backend


@pytest.fixture(scope="session")
def create_session(backend, setup_backend):
    """Create a SQLFrame session for the configured backend.

    This fixture provides a session object that mimics the PySpark API
    but uses SQLFrame under the hood, allowing tests to run against
    different backends (DuckDB, Spark, etc.).
    """
    warnings.filterwarnings("ignore")

    if backend == "duckdb":
        from sqlframe.duckdb import DuckDBSession

        session = DuckDBSession()

    elif backend == "pyspark":
        from sqlframe.spark import SparkSession

        logging.getLogger("py4j").setLevel(logging.ERROR)
        logging.getLogger("pyspark").setLevel(logging.ERROR)
        os.environ["SPARK_SUBMIT_OPTS"] = "-Divy.message.logger.level=ERROR"
        master = os.environ.get("SPARK_MASTER", "local[*]")
        session = (
            SparkSession.builder.appName("DataComposeTests")
            .master(master)
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "2")
            .getOrCreate()
        )

    elif backend == "postgres":
        from sqlframe.postgres import PostgresSession

        conn_string = os.environ.get(
            "POSTGRES_CONN", "postgresql://localhost:5432/test"
        )
        session = PostgresSession(conn_string)

    elif backend == "bigquery":
        from sqlframe.bigquery import BigQuerySession

        session = BigQuerySession()

    elif backend == "snowflake":
        from sqlframe.snowflake import SnowflakeSession

        session = SnowflakeSession()

    else:
        raise ValueError(f"Unknown backend: {backend}")

    yield session

    if hasattr(session, "stop"):
        session.stop()


# Alias for backwards compatibility
@pytest.fixture(scope="session")
def sparksession(create_session):
    """Alias for create_session fixture for backwards compatibility."""
    return create_session
