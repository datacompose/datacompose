"""
Test error handling and edge cases in conditional compilation.
"""

import pytest
from pyspark.sql import functions as f
from datacompose.operators.primitives import PrimitiveRegistry


@pytest.mark.unit
class TestErrorHandlingInConditionals:
    """Test error handling and edge cases in conditional compilation"""

    def test_conditional_with_invalid_condition(self, spark):
        """Test handling of invalid condition functions"""
        ns = PrimitiveRegistry("invalid")

        @ns.register()  # Not marked as conditional!
        def not_a_condition(col):
            return f.upper(col)

        @ns.register()
        def transform(col):
            return f.lower(col)

        # This should handle gracefully
        @ns.compose(ns=ns, debug=True)
        def invalid_pipeline():
            ns.transform()
            # This might not work as expected since not_a_condition
            # is not registered as a conditional
            # The compiler should handle this gracefully

        data = [("TEST",)]
        df = spark.createDataFrame(data, ["text"])

        result = df.withColumn("processed", invalid_pipeline(f.col("text")))
        collected = result.collect()

        # Should at least apply the transform
        assert collected[0]["processed"] == "test"

    def test_conditional_with_type_mismatch(self, spark):
        """Test conditions that might return non-boolean values"""
        ns = PrimitiveRegistry("type")

        @ns.register(is_conditional=True)
        def returns_boolean(col):
            return col.isNotNull()  # Proper boolean

        @ns.register()
        def process(col):
            return f.upper(col)

        @ns.compose(ns=ns, debug=True)
        def type_safe():
            if ns.returns_boolean():
                ns.process()

        data = [("test",), (None,)]
        df = spark.createDataFrame(data, ["text"])

        result = df.withColumn("processed", type_safe(f.col("text")))
        collected = result.collect()

        assert collected[0]["processed"] == "TEST"
        assert collected[1]["processed"] is None
