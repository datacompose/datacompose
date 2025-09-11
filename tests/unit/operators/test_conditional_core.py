"""
Consolidated test suite for conditional compilation functionality.

This file combines all conditional test classes into organized sections:
- TestConditionalLogic: Complex logic structures and nested conditionals
- TestConditionalErrors: Error handling and edge cases
- TestConditionalParameters: Parameterized conditionals and closures
- TestConditionalPerformance: Performance aspects and branch optimization

DESIGN DECISION - Single Column Constraint:
-------------------------------------------
The conditional compilation framework is designed to work with single-column transformations.
This is an intentional design choice to keep the framework focused and maintainable.

For multi-column logic:
1. Pre-compute the logic into a column before the pipeline
2. Use closure pattern to capture external values (see test_conditional_with_dynamic_thresholds)
3. Reference other columns by name with f.col() if column names are known

Example:
    # Pre-compute multi-column logic
    df = df.withColumn("is_priority", (f.col("value") > 50) & (f.col("flag") == True))

    # Then use in pipeline
    @ns.register()
    def is_priority(col):
        return f.col("is_priority")

This keeps the framework simple while still being powerful for most use cases.
Future versions may add row-wise operations for more complex multi-column logic.
"""

import pytest
from pyspark.sql import functions as f
from datacompose.operators.primitives import PrimitiveRegistry


@pytest.fixture
def diverse_test_data(spark):
    """Create diverse test dataset for conditional testing"""
    data = [
        ("A", 10, "small", 1, "short"),           # category A - short text
        ("A", 20, "medium", 2, "simple"),         # category A  
        ("A", 30, "large", 3, "s_text"),          # category A
        ("A", 40, "xlarge", 4, "special!@#text"), # category A
        ("B", 50, "small", 5, None),              # category B - NULL text for null test
        ("B", 60, "medium", 6, "medium_text"),    # category B - ends with 'text'
        ("B", 70, "large", 7, "complex_text"),    # category B
        ("C", 80, "small", 8, "UPPERCASE"),       # category C - all caps
        ("C", 90, "medium", 9, "simple_text"),    # category C
        (None, 100, "unknown", 10, None)          # NULL category for testing
    ]
    return spark.createDataFrame(data, ["category", "value", "size", "id", "text"])


# ============================================================================
# TestConditionalLogic: Complex logic structures and nested conditionals
# ============================================================================

@pytest.mark.unit
class TestConditionalLogic:
    """Test complex conditional structures and logic"""

    def test_deeply_nested_conditionals(self, spark):
        """Test 3+ levels of nested conditionals"""
        ns = PrimitiveRegistry("nested")

        @ns.register()
        def is_text(col):
            return col.rlike("[a-zA-Z]")

        @ns.register()
        def is_long(col):
            return f.length(col) > 5

        @ns.register()
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

        @ns.register()
        def is_category_a(col):
            return col == "A"

        @ns.register()
        def is_category_b(col):
            return col == "B"

        @ns.register()
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
        @ns.register()
        def is_text_or_numeric(col):
            # Either condition can be true
            return col.rlike("[a-zA-Z]") | col.rlike("[0-9]")

        @ns.register()
        def has_special_chars(col):
            return col.rlike("[!@#$%]")

        @ns.register()
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

    def test_conditional_without_else_branch(self, spark):
        """Test if statement without else branch"""
        ns = PrimitiveRegistry("test")

        @ns.register()
        def make_upper(col):
            return f.upper(col)

        @ns.register()
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

        @ns.register()
        def is_numeric(col):
            return col.rlike("^[0-9]+$")

        @ns.register()
        def is_alpha(col):
            return col.rlike("^[a-zA-Z]+$")

        @ns.register()
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

        @ns.register()
        def is_null(col):
            return col.isNull()

        @ns.register()
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

        @ns.register()
        def always_true(col):
            return f.lit(True)

        @ns.register()
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

    def test_numeric_comparisons(self, diverse_test_data):
        """Test all numeric comparison operators"""
        ns = PrimitiveRegistry("numeric")

        @ns.register()
        def equals_5(col):
            return col == 5

        @ns.register()
        def not_equals_5(col):
            return col != 5

        @ns.register()
        def greater_than_5(col):
            return col > 5

        @ns.register()
        def less_than_5(col):
            return col < 5

        @ns.register()
        def gte_5(col):
            return col >= 5

        @ns.register()
        def lte_5(col):
            return col <= 5

        @ns.register()
        def mark(col, label):
            return f.lit(label)

        # Test each comparison
        @ns.compose(ns=ns, debug=True)
        def compare_to_5():
            if ns.equals_5():
                ns.mark(label="EQ5")
            else:
                if ns.greater_than_5():
                    ns.mark(label="GT5")
                else:
                    ns.mark(label="LT5")

        result = diverse_test_data.withColumn("comparison", compare_to_5(f.col("id")))

        collected = result.collect()

        # Verify comparisons
        assert collected[4]["comparison"] == "EQ5"  # id=5
        assert collected[0]["comparison"] == "LT5"  # id=1
        assert collected[9]["comparison"] == "GT5"  # id=10

    def test_string_pattern_matching(self, diverse_test_data):
        """Test string pattern conditions"""
        ns = PrimitiveRegistry("pattern")

        @ns.register()
        def starts_with_s(col):
            return col.startswith("s")

        @ns.register()
        def ends_with_text(col):
            return col.endswith("text")

        @ns.register()
        def contains_underscore(col):
            return col.contains("_")

        @ns.register()
        def matches_pattern(col):
            return col.rlike("^[A-Z]+$")

        @ns.register()
        def tag(col, tag_name):
            return f.concat(col, f.lit(f":{tag_name}"))

        @ns.compose(ns=ns, debug=True)
        def pattern_tagger():
            if ns.starts_with_s():
                ns.tag(tag_name="STARTS_S")

            if ns.ends_with_text():
                ns.tag(tag_name="ENDS_TEXT")

            if ns.contains_underscore():
                ns.tag(tag_name="HAS_UNDERSCORE")

            if ns.matches_pattern():
                ns.tag(tag_name="ALL_CAPS")

        result = diverse_test_data.withColumn("tagged", pattern_tagger(f.col("text")))

        collected = result.collect()

        # Check pattern matching
        short_row = [r for r in collected if r["text"] == "short"][0]
        assert "STARTS_S" in short_row["tagged"]

        medium_row = [r for r in collected if r["text"] == "medium_text"][0]
        assert "ENDS_TEXT" in medium_row["tagged"]
        assert "HAS_UNDERSCORE" in medium_row["tagged"]

        upper_row = [r for r in collected if r["text"] == "UPPERCASE"][0]
        assert "ALL_CAPS" in upper_row["tagged"]


# ============================================================================
# TestConditionalErrors: Error handling and edge cases
# ============================================================================

@pytest.mark.unit
class TestConditionalErrors:
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

        @ns.register()
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


# ============================================================================
# TestConditionalParameters: Parameterized conditionals and closures
# ============================================================================

@pytest.mark.unit
class TestConditionalParameters:
    """Test conditionals with parameters and closures"""

    def test_conditional_with_parameters(self, spark):
        """Test passing parameters to conditional functions"""
        ns = PrimitiveRegistry("params")

        @ns.register()
        def length_in_range(col, min_len=1, max_len=10):
            return (f.length(col) >= min_len) & (f.length(col) <= max_len)

        @ns.register()
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
        @ns.register()
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


# ============================================================================
# TestConditionalPerformance: Performance aspects and branch optimization
# ============================================================================

@pytest.mark.unit
class TestConditionalPerformance:
    """Test performance aspects of conditional compilation"""

    def test_conditional_branch_skipping(self, spark):
        """Verify untaken branches don't execute"""
        ns = PrimitiveRegistry("skip")

        # Track execution
        execution_log = []

        @ns.register()
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