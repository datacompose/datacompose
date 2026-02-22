"""
Root-level test configuration and shared fixtures.
"""

import logging
import os
import warnings

import pytest
from pyspark.sql import SparkSession

from datacompose.functions import set_backend


@pytest.fixture(scope="session", autouse=True)
def duckdb_backend():
    """Set the duckdb backend for SQLFrame functions."""
    set_backend("duckdb")
    yield


@pytest.fixture(scope="session")
def spark(duckdb_backend):
    """Create a single Spark session for all tests."""
    # Suppress all warnings
    warnings.filterwarnings("ignore")

    # Set the datacompose backend for pyspark functions
    set_backend("pyspark")

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
        .config("spark.executor.extraJavaOptions", "-Dlog4j.logger.org.apache.ivy=ERROR")
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