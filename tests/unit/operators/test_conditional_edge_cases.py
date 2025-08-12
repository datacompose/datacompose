"""
Test edge cases and error conditions in conditional compilation.
"""

import pytest
from pyspark.sql import functions as f
from datacompose.operators.primitives import PrimitiveRegistry


@pytest.mark.unit
class TestConditionalEdgeCases:
    """Test edge cases and error conditions in conditional compilation"""

    def test_conditional_without_else_branch(self, spark):
        """Test if statement without else branch"""
        ns = PrimitiveRegistry("test")

        @ns.register()
        def make_upper(col):
            return f.upper(col)

        @ns.register(is_conditional=True)
        def is_short(col):
            return f.length(col) < 5

        @ns.compose(ns=ns, debug=True)
        def process():
            if ns.is_short():
                ns.make_upper()

        # Test with data
        data = [("hi",), ("hello world",), ("test",)]
        df = spark.createDataFrame(data, ["text"])

        result = df.withColumn("processed", process(f.col("text")))
        collected = result.collect()

        # Short strings should be uppercase
        assert collected[0]["processed"] == "HI"
        # Long strings should be unchanged
        assert collected[1]["processed"] == "hello world"
        # Edge case: exactly at boundary
        assert collected[2]["processed"] == "TEST"

    def test_multiple_sequential_conditionals(self, spark):
        """Test multiple if statements in sequence"""
        ns = PrimitiveRegistry("multi")

        @ns.register()
        def add_prefix(col, prefix):
            return f.concat(f.lit(prefix), col)

        @ns.register(is_conditional=True)
        def is_numeric(col):
            return col.rlike("^[0-9]+$")

        @ns.register(is_conditional=True)
        def is_alpha(col):
            return col.rlike("^[a-zA-Z]+$")

        @ns.register(is_conditional=True)
        def is_special(col):
            return col.rlike("[!@#$%]")

        @ns.compose(ns=ns, debug=True)
        def classify():
            if ns.is_numeric():
                ns.add_prefix(prefix="NUM:")

            if ns.is_alpha():
                ns.add_prefix(prefix="ALPHA:")

            if ns.is_special():
                ns.add_prefix(prefix="SPECIAL:")

        data = [("123",), ("abc",), ("!@#",), ("a1b2",)]
        df = spark.createDataFrame(data, ["text"])

        result = df.withColumn("classified", classify(f.col("text")))
        collected = result.collect()

        assert collected[0]["classified"] == "NUM:123"
        assert collected[1]["classified"] == "ALPHA:abc"
        assert collected[2]["classified"] == "SPECIAL:!@#"
        assert collected[3]["classified"] == "a1b2"  # Doesn't match any

    def test_conditional_with_null_values(self, diverse_test_data):
        """Test conditional behavior with NULL values"""
        ns = PrimitiveRegistry("null_test")

        @ns.register(is_conditional=True)
        def is_null(col):
            return col.isNull()

        @ns.register(is_conditional=True)
        def is_not_null(col):
            return col.isNotNull()

        @ns.register()
        def default_value(col):
            return f.lit("DEFAULT")

        @ns.register()
        def process_value(col):
            return f.upper(col)

        @ns.compose(ns=ns, debug=True)
        def handle_nulls():
            if ns.is_null():
                ns.default_value()
            else:
                ns.process_value()

        result = diverse_test_data.withColumn("processed", handle_nulls(f.col("text")))
        collected = result.collect()

        # Check NULL handling
        null_row = [r for r in collected if r["id"] == 5][0]
        assert null_row["processed"] == "DEFAULT"

        # Check non-NULL handling
        non_null_row = [r for r in collected if r["id"] == 1][0]
        assert non_null_row["processed"] == "SHORT"

    def test_conditional_with_empty_branches(self, spark):
        """Test conditionals with empty branches (no operations)"""
        ns = PrimitiveRegistry("empty")

        @ns.register(is_conditional=True)
        def always_true(col):
            return f.lit(True)

        @ns.register(is_conditional=True)
        def always_false(col):
            return f.lit(False)

        # This should compile but effectively be a no-op
        @ns.compose(ns=ns, debug=True)
        def empty_pipeline():
            if ns.always_false():
                pass  # Empty branch

        data = [("test",)]
        df = spark.createDataFrame(data, ["text"])

        result = df.withColumn("processed", empty_pipeline(f.col("text")))
        collected = result.collect()

        # Should return original value unchanged
        assert collected[0]["processed"] == "test"
