"""
Test conditionals with various data comparisons and operations.
"""

import pytest
from pyspark.sql import functions as f
from datacompose.operators.primitives import PrimitiveRegistry


@pytest.mark.unit
class TestDataDrivenConditionals:
    """Test conditionals with various data comparisons and operations"""

    def test_numeric_comparisons(self, diverse_test_data):
        """Test all numeric comparison operators"""
        ns = PrimitiveRegistry("numeric")

        @ns.register(is_conditional=True)
        def equals_5(col):
            return col == 5

        @ns.register(is_conditional=True)
        def not_equals_5(col):
            return col != 5

        @ns.register(is_conditional=True)
        def greater_than_5(col):
            return col > 5

        @ns.register(is_conditional=True)
        def less_than_5(col):
            return col < 5

        @ns.register(is_conditional=True)
        def gte_5(col):
            return col >= 5

        @ns.register(is_conditional=True)
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

        @ns.register(is_conditional=True)
        def starts_with_s(col):
            return col.startswith("s")

        @ns.register(is_conditional=True)
        def ends_with_text(col):
            return col.endswith("text")

        @ns.register(is_conditional=True)
        def contains_underscore(col):
            return col.contains("_")

        @ns.register(is_conditional=True)
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
