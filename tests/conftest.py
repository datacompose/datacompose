"""
Root-level test configuration and shared fixtures.
"""

import logging
import os
import warnings

import pytest

from datacompose.functions import set_backend

# Backends to test against (start with duckdb only)
BACKENDS = ["duckdb", "pyspark", "postgres"]


@pytest.fixture(scope="session", params=BACKENDS)
def backend(request):
    """Parameterized backend fixture - tests run against each backend."""
    return request.param


@pytest.fixture(scope="session", autouse=True)
def setup_backend(backend):
    """Set the SQLFrame backend for the current test session.

    This fixture is autouse=True so it runs before any test,
    ensuring the backend is set before imports that need it.
    """
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
        import re as _re

        import psycopg2
        from sqlframe.postgres import PostgresSession

        host = os.environ.get("POSTGRES_HOST", "localhost")
        port = os.environ.get("POSTGRES_PORT", "5432")
        db = os.environ.get("POSTGRES_DB", "datawarehouse")
        user = os.environ.get("POSTGRES_USER", "postgres")
        password = os.environ.get("POSTGRES_PASSWORD", "postgres123")
        conn = psycopg2.connect(
            host=host, port=port, dbname=db, user=user, password=password
        )
        conn.autocommit = True
        session = PostgresSession(conn)

        # Patch: PostgreSQL cannot determine the type of empty ARRAY().
        # sqlframe generates ARRAY() for empty lists; rewrite to typed form.
        _orig_execute = session._execute

        def _patched_execute(sql):
            sql = _re.sub(r"\bARRAY\[\](?!::)", "ARRAY[]::TEXT[]", sql)
            sql = _re.sub(
                r"CONTAINS\(([^,]+),\s*'([^']*)'\)",
                r"\1 LIKE '%\2%'",
                sql,
            )
            return _orig_execute(sql)

        session._execute = _patched_execute

    elif backend == "bigquery":
        from sqlframe.bigquery import BigQuerySession

        session = BigQuerySession()

    elif backend == "snowflake":
        from sqlframe.snowflake import SnowflakeSession

        session = SnowflakeSession()

    else:
        raise ValueError(f"Unknown backend: {backend}")

    yield session

    try:
        if hasattr(session, "stop"):
            session.stop()
    except AttributeError:
        pass


# Alias for backwards compatibility
@pytest.fixture(scope="session")
def sparksession(create_session):
    """Alias for create_session fixture for backwards compatibility."""
    return create_session
