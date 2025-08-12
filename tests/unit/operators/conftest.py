"""
Shared fixtures and configuration for operator tests.
"""

import os
import sys
from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    BooleanType,
    DoubleType,
)

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))


@pytest.fixture(scope="session")
def spark():
    """Create SparkSession for tests"""
    master = os.environ.get("SPARK_MASTER", "local[*]")
    return (
        SparkSession.builder.appName("ConditionalTests")
        .master(master)
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


@pytest.fixture
def diverse_test_data(spark):
    """Create diverse test data for conditional testing"""
    data = [
        ("short", 1, True, "A", 10.5),
        ("medium_text", 2, False, "B", 20.3),
        ("very_long_text_here", 3, True, "A", 15.7),
        ("", 4, False, "C", 0.0),
        (None, 5, None, None, None),
        ("UPPERCASE", 6, True, "B", 100.0),
        ("  spaces  ", 7, False, "A", 50.0),
        ("special!@#", 8, True, "C", 75.5),
        ("12345", 9, False, "A", 33.3),
        ("mixed123ABC", 10, True, "B", 99.9),
    ]
    schema = StructType(
        [
            StructField("text", StringType(), True),
            StructField("id", IntegerType(), False),
            StructField("flag", BooleanType(), True),
            StructField("category", StringType(), True),
            StructField("value", DoubleType(), True),
        ]
    )
    return spark.createDataFrame(data, schema)
