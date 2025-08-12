"""
Test conditionals with parameters and closures.
"""

import pytest
from pyspark.sql import functions as f
from datacompose.operators.primitives import PrimitiveRegistry


@pytest.mark.unit
class TestParameterizedConditionals:
    """Test conditionals with parameters and closures"""

    def test_conditional_with_parameters(self, spark):
        """Test passing parameters to conditional functions"""
        ns = PrimitiveRegistry("params")

        @ns.register(is_conditional=True)
        def length_in_range(col, min_len=1, max_len=10):
            return (f.length(col) >= min_len) & (f.length(col) <= max_len)

        @ns.register(is_conditional=True)
        def value_above_threshold(col, threshold=50.0):
            return col > threshold

        @ns.register()
        def tag_length(col, tag):
            return f.concat(col, f.lit(f":{tag}"))

        # Test with different parameter values
        @ns.compose(ns=ns, debug=True)
        def flexible_pipeline():
            if ns.length_in_range(min_len=3, max_len=6):
                ns.tag_length(tag="MEDIUM")
            else:
                if ns.length_in_range(min_len=7, max_len=100):
                    ns.tag_length(tag="LONG")
                else:
                    ns.tag_length(tag="SHORT")

        data = [("hi",), ("hello",), ("hello world",)]
        df = spark.createDataFrame(data, ["text"])

        result = df.withColumn("tagged", flexible_pipeline(f.col("text")))
        collected = result.collect()

        assert collected[0]["tagged"] == "hi:SHORT"
        assert collected[1]["tagged"] == "hello:MEDIUM"
        assert collected[2]["tagged"] == "hello world:LONG"

    def test_conditional_with_dynamic_thresholds(self, diverse_test_data):
        """Test conditions that use dynamic thresholds from data using closure pattern"""
        ns = PrimitiveRegistry("dynamic")

        # Compute threshold (could be from data, config, etc.)
        # For demo, using a fixed value, but could be:
        # threshold = diverse_test_data.agg(f.avg("value")).collect()[0][0]
        threshold_value = 50.0

        # Use closure to capture the threshold
        @ns.register(is_conditional=True)
        def above_threshold(col):
            # Closure captures threshold_value from outer scope
            return col > f.lit(threshold_value)

        @ns.register()
        def mark_high(col):
            return f.concat(f.lit("HIGH:"), col)

        @ns.register()
        def mark_low(col):
            return f.concat(f.lit("LOW:"), col)

        @ns.compose(ns=ns, debug=True)
        def compare_to_threshold():
            if ns.above_threshold():
                ns.mark_high()
            else:
                ns.mark_low()

        # Apply to the value column instead of text
        result = diverse_test_data.withColumn(
            "comparison", compare_to_threshold(f.col("value"))
        )

        collected = result.collect()

        # Check dynamic comparison
        for row in collected:
            if row["value"] is not None:
                if row["value"] > threshold_value:
                    assert row["comparison"].startswith("HIGH:")
                else:
                    assert row["comparison"].startswith("LOW:")
