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

        # Replace sqlframe's lenient try_to_timestamp with a strict version
        # that rejects inputs where trailing content is silently ignored.
        conn.cursor().execute("""
            CREATE OR REPLACE FUNCTION pg_temp.try_to_timestamp(
                input_text TEXT, format TEXT
            ) RETURNS TIMESTAMP AS $$
            DECLARE
                result TIMESTAMP;
                reformatted TEXT;
                cleaned TEXT;
            BEGIN
                result := TO_TIMESTAMP(input_text, format);
                reformatted := TO_CHAR(result, format);
                -- Strip timezone suffix (e.g. +00) that postgres adds after
                -- time portions (HH:MM:SS+00) before comparing lengths.
                cleaned := REGEXP_REPLACE(TRIM(input_text),
                    '(\\d{2}:\\d{2}(:\\d{2})?)[+-]\\d{2}(:\\d{2})?$', '\\1');
                IF LENGTH(cleaned) <> LENGTH(TRIM(reformatted)) THEN
                    RETURN NULL;
                END IF;
                RETURN result;
            EXCEPTION WHEN OTHERS THEN
                RETURN NULL;
            END;
            $$ LANGUAGE plpgsql;
        """)

        # Patch _execute to fix sqlframe SQL compatibility issues.
        _orig_execute = session._execute

        def _rewrite_contains(sql):
            """Replace CONTAINS(expr, 'lit') with (expr LIKE '%lit%').

            A simple regex can't handle nested expressions with commas
            (e.g. CONTAINS(CASE WHEN REGEXP_REPLACE(x,'a','b') END, 'q'))
            so we walk the string and count parentheses instead.
            """
            tag = "CONTAINS("
            out = []
            i = 0
            while i < len(sql):
                pos = sql.find(tag, i)
                if pos == -1:
                    out.append(sql[i:])
                    break
                out.append(sql[i:pos])
                start = pos + len(tag)
                depth = 1
                j = start
                while j < len(sql) and depth > 0:
                    if sql[j] == "(":
                        depth += 1
                    elif sql[j] == ")":
                        depth -= 1
                    j += 1
                inner = sql[start : j - 1]
                m = _re.search(r",\s*'([^']*)'$", inner)
                if m:
                    expr = inner[: m.start()]
                    lit = m.group(1).replace("%", "\\%")  # escape % for LIKE
                    out.append(f"({expr} LIKE '%{lit}%')")
                else:
                    out.append(sql[pos:j])  # leave unchanged
                i = j
            return "".join(out)

        def _patched_execute(sql):
            sql = _re.sub(r"\bARRAY\[\](?!::)", "ARRAY[]::TEXT[]", sql)
            sql = _rewrite_contains(sql)
            # sqlframe mistranslates Spark's AM/PM token 'a' to '%p';
            # PostgreSQL uses 'AM' (or 'PM') as the format token.
            sql = sql.replace("%p", "AM")
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
