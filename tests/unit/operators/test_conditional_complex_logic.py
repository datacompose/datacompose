"""
Test complex conditional structures and logic.
"""

import pytest
from pyspark.sql import functions as f
from datacompose.operators.primitives import PrimitiveRegistry


@pytest.mark.unit
class TestComplexConditionalLogic:
    """Test complex conditional structures and logic"""

    def test_deeply_nested_conditionals(self, spark):
        """Test 3+ levels of nested conditionals"""
        ns = PrimitiveRegistry("nested")

        @ns.register(is_conditional=True)
        def is_text(col):
            return col.rlike("[a-zA-Z]")

        @ns.register(is_conditional=True)
        def is_long(col):
            return f.length(col) > 5

        @ns.register(is_conditional=True)
        def has_uppercase(col):
            return col != f.lower(col)

        @ns.register()
        def level1(col):
            return f.concat(f.lit("L1:"), col)

        @ns.register()
        def level2(col):
            return f.concat(f.lit("L2:"), col)

        @ns.register()
        def level3(col):
            return f.concat(f.lit("L3:"), col)

        @ns.register()
        def default(col):
            return f.concat(f.lit("DEFAULT:"), col)

        @ns.compose(ns=ns, debug=True)
        def deep_nested():
            if ns.is_text():
                ns.level1()
                if ns.is_long():
                    ns.level2()
                    if ns.has_uppercase():
                        ns.level3()
                    else:
                        ns.default()
                else:
                    ns.default()
            else:
                ns.default()

        data = [
            ("VERYLONGTEXT",),  # All conditions true
            ("short",),  # Only first condition true
            ("123456",),  # First condition false
            ("longtext",),  # First two true, third false
        ]
        df = spark.createDataFrame(data, ["text"])

        result = df.withColumn("processed", deep_nested(f.col("text")))
        collected = result.collect()

        # Trace through the logic:
        # "VERYLONGTEXT": is_text=T, L1 applied, is_long=T, L2 applied, has_uppercase=T, L3 applied
        assert collected[0]["processed"] == "L3:L2:L1:VERYLONGTEXT"

        # "short": is_text=T, L1 applied -> "L1:short" (8 chars)
        # is_long("L1:short")=T (8>5), L2 applied -> "L2:L1:short"
        # has_uppercase("L2:L1:short")=T (has 'L'), L3 applied
        assert collected[1]["processed"] == "L3:L2:L1:short"

        # "123456": is_text=F (no letters), DEFAULT applied
        assert collected[2]["processed"] == "DEFAULT:123456"

        # "longtext": is_text=T, L1 applied -> "L1:longtext"
        # is_long=T, L2 applied -> "L2:L1:longtext"
        # has_uppercase=T (has 'L'), L3 applied
        assert collected[3]["processed"] == "L3:L2:L1:longtext"

    def test_elif_chain(self, diverse_test_data):
        """Test if/elif/elif/else chains"""
        ns = PrimitiveRegistry("elif")

        @ns.register(is_conditional=True)
        def is_category_a(col):
            return col == "A"

        @ns.register(is_conditional=True)
        def is_category_b(col):
            return col == "B"

        @ns.register(is_conditional=True)
        def is_category_c(col):
            return col == "C"

        @ns.register()
        def process_a(col):
            return f.lit("Processing A")

        @ns.register()
        def process_b(col):
            return f.lit("Processing B")

        @ns.register()
        def process_c(col):
            return f.lit("Processing C")

        @ns.register()
        def process_unknown(col):
            return f.lit("Unknown Category")

        # Simulate elif with nested if/else
        @ns.compose(ns=ns, debug=True)
        def category_processor():
            if ns.is_category_a():
                ns.process_a()
            else:
                if ns.is_category_b():
                    ns.process_b()
                else:
                    if ns.is_category_c():
                        ns.process_c()
                    else:
                        ns.process_unknown()

        result = diverse_test_data.withColumn(
            "processed", category_processor(f.col("category"))
        )

        collected = result.collect()

        # Count each category result
        results = [r["processed"] for r in collected]
        assert results.count("Processing A") == 4  # 4 A's in test data
        assert results.count("Processing B") == 3  # 3 B's
        assert results.count("Processing C") == 2  # 2 C's
        assert results.count("Unknown Category") == 1  # 1 NULL

    def test_conditional_with_complex_boolean_logic(self, diverse_test_data):
        """Test conditions with AND/OR combinations"""
        ns = PrimitiveRegistry("boolean")

        # Single-column conditionals that work with our framework
        @ns.register(is_conditional=True)
        def is_text_or_numeric(col):
            # Either condition can be true
            return col.rlike("[a-zA-Z]") | col.rlike("[0-9]")

        @ns.register(is_conditional=True)
        def has_special_chars(col):
            return col.rlike("[!@#$%]")

        @ns.register(is_conditional=True)
        def is_long_text(col):
            return f.length(col) > 10

        @ns.register()
        def mark_complex(col):
            return f.concat(f.lit("COMPLEX:"), col)

        @ns.register()
        def mark_simple(col):
            return f.concat(f.lit("SIMPLE:"), col)

        # Test OR logic with single column
        @ns.compose(ns=ns, debug=True)
        def complexity_check():
            if ns.is_long_text():
                ns.mark_complex()
            else:
                if ns.has_special_chars():
                    ns.mark_complex()
                else:
                    ns.mark_simple()

        result = diverse_test_data.withColumn(
            "complexity", complexity_check(f.col("text"))
        )

        collected = result.collect()

        # Check complex items (long or special chars)
        long_items = [r for r in collected if r["text"] and len(r["text"]) > 10]
        for item in long_items:
            assert item["complexity"].startswith("COMPLEX:")

        special_items = [
            r for r in collected if r["text"] and any(c in r["text"] for c in "!@#$%")
        ]
        for item in special_items:
            assert item["complexity"].startswith("COMPLEX:")
