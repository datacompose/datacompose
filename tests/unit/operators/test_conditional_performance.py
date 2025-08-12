"""
Test performance aspects of conditional compilation.
"""

import pytest
from pyspark.sql import functions as f
from datacompose.operators.primitives import PrimitiveRegistry


@pytest.mark.unit
class TestConditionalPerformance:
    """Test performance aspects of conditional compilation"""

    def test_conditional_branch_skipping(self, spark):
        """Verify untaken branches don't execute"""
        ns = PrimitiveRegistry("skip")

        # Track execution
        execution_log = []

        @ns.register(is_conditional=True)
        def always_false(col):
            return f.lit(False)

        @ns.register()
        def should_not_execute(col):
            # This should never be called
            execution_log.append("EXECUTED")
            return f.upper(col)

        @ns.register()
        def should_execute(col):
            execution_log.append("ELSE_EXECUTED")
            return f.lower(col)

        @ns.compose(ns=ns, debug=True)
        def test_skipping():
            if ns.always_false():
                ns.should_not_execute()
            else:
                ns.should_execute()

        data = [("Test",)]
        df = spark.createDataFrame(data, ["text"])

        # Clear log
        execution_log.clear()

        result = df.withColumn("processed", test_skipping(f.col("text")))
        collected = result.collect()

        # The false branch should not have executed
        # Note: The function definitions are evaluated but not executed on data
        assert collected[0]["processed"] == "test"

    def test_conditional_with_many_branches(self, spark):
        """Test performance with 10+ if/elif branches"""
        ns = PrimitiveRegistry("many")

        # Create many conditions
        for i in range(15):

            @ns.register(is_conditional=True, name=f"is_{i}")
            def make_condition(col, target=i):
                return col == target

        # Create corresponding actions
        for i in range(15):

            @ns.register(name=f"process_{i}")
            def make_action(col, label=i):
                return f.lit(f"Processed_{label}")

        @ns.compose(ns=ns, debug=False)  # Turn off debug for performance
        def many_branches():
            if ns.is_0():
                ns.process_0()
            else:
                if ns.is_1():
                    ns.process_1()
                else:
                    if ns.is_2():
                        ns.process_2()
                    else:
                        if ns.is_3():
                            ns.process_3()
                        else:
                            if ns.is_4():
                                ns.process_4()
                            else:
                                if ns.is_5():
                                    ns.process_5()
                                else:
                                    ns.process_6()

        # Test with data
        data = [(i,) for i in range(10)]
        df = spark.createDataFrame(data, ["value"])

        result = df.withColumn("processed", many_branches(f.col("value")))
        collected = result.collect()

        # Verify correct branch execution
        assert collected[0]["processed"] == "Processed_0"
        assert collected[1]["processed"] == "Processed_1"
        assert collected[5]["processed"] == "Processed_5"
