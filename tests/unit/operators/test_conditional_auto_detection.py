"""Test automatic detection of conditional operators based on naming patterns."""

import pytest
from pyspark.sql import functions as f

from datacompose.operators.primitives import PrimitiveRegistry


@pytest.mark.unit
class TestConditionalAutoDetection:
    """Test automatic detection of conditional operators"""

    def test_auto_detection_with_common_prefixes(self, spark):
        """Test that common conditional prefixes are auto-detected"""
        ns = PrimitiveRegistry("auto_detect")
        
        # These should be auto-detected as conditionals
        @ns.register()
        def is_valid(col):
            return col.isNotNull()
        
        @ns.register()
        def has_value(col):
            return col != f.lit("")
        
        @ns.register()
        def needs_cleaning(col):
            return f.length(col) > 10
        
        @ns.register()
        def should_process(col):
            return col.rlike(r"^[A-Z]")
        
        @ns.register()
        def can_transform(col):
            return col.isNotNull() & (f.length(col) > 0)
        
        @ns.register()
        def contains_digit(col):
            return col.rlike(r"\d")
        
        @ns.register()
        def matches_pattern(col):
            return col.rlike(r"^test")
        
        @ns.register()
        def starts_with_letter(col):
            return col.rlike(r"^[a-zA-Z]")
        
        @ns.register()
        def ends_with_number(col):
            return col.rlike(r"\d$")
        
        # Verify all are registered as conditionals
        assert "is_valid" in ns._conditionals
        assert "has_value" in ns._conditionals
        assert "needs_cleaning" in ns._conditionals
        assert "should_process" in ns._conditionals
        assert "can_transform" in ns._conditionals
        assert "contains_digit" in ns._conditionals
        assert "matches_pattern" in ns._conditionals
        assert "starts_with_letter" in ns._conditionals
        assert "ends_with_number" in ns._conditionals
        
        # Verify none are registered as regular primitives
        assert "is_valid" not in ns._primitives
        assert "has_value" not in ns._primitives

    def test_non_conditional_functions(self, spark):
        """Test that non-conditional functions are registered as transforms"""
        ns = PrimitiveRegistry("transforms")
        
        # These should NOT be auto-detected as conditionals
        @ns.register()
        def clean(col):
            return f.trim(f.lower(col))
        
        @ns.register()
        def transform(col):
            return f.upper(col)
        
        @ns.register()
        def normalize(col):
            return f.regexp_replace(col, r"\s+", " ")
        
        @ns.register()
        def format_text(col):
            return f.concat(f.lit("PREFIX_"), col)
        
        # Verify all are registered as regular primitives
        assert "clean" in ns._primitives
        assert "transform" in ns._primitives
        assert "normalize" in ns._primitives
        assert "format_text" in ns._primitives
        
        # Verify none are registered as conditionals
        assert "clean" not in ns._conditionals
        assert "transform" not in ns._conditionals

    def test_explicit_conditional_override(self, spark):
        """Test explicit is_conditional flag overrides auto-detection"""
        ns = PrimitiveRegistry("override")
        
        # Force a non-pattern function to be conditional
        @ns.register(is_conditional=True)
        def check_quality(col):
            return col.isNotNull() & (f.length(col) > 0)
        
        # Force a pattern-matching function to NOT be conditional
        @ns.register(is_conditional=False)
        def is_uppercase_transform(col):
            """This actually transforms to uppercase, not a check"""
            return f.upper(col)
        
        # Verify explicit overrides work
        assert "check_quality" in ns._conditionals
        assert "check_quality" not in ns._primitives
        
        assert "is_uppercase_transform" in ns._primitives
        assert "is_uppercase_transform" not in ns._conditionals

    def test_conditional_in_pipeline(self, spark):
        """Test that auto-detected conditionals work in pipelines"""
        ns = PrimitiveRegistry("pipeline_test")
        
        @ns.register()  # Auto-detected as conditional
        def is_valid(col):
            return col.isNotNull() & (f.length(col) > 0)
        
        @ns.register()  # Auto-detected as conditional
        def needs_upper(col):
            return col != f.upper(col)
        
        @ns.register()  # Regular transform
        def make_upper(col):
            return f.upper(col)
        
        @ns.register()  # Regular transform
        def add_prefix(col):
            return f.concat(f.lit("PROCESSED_"), col)
        
        @ns.compose(ns=ns, debug=True)
        def process_text():
            if ns.is_valid():
                if ns.needs_upper():
                    ns.make_upper()
                ns.add_prefix()
        
        # Test with sample data
        data = [("hello",), ("WORLD",), ("",), (None,)]
        df = spark.createDataFrame(data, ["text"])
        
        result = df.withColumn("processed", process_text(f.col("text")))
        collected = result.collect()
        
        # Verify processing
        assert collected[0]["processed"] == "PROCESSED_HELLO"  # was lowercase, made upper
        assert collected[1]["processed"] == "PROCESSED_WORLD"  # was already upper
        assert collected[2]["processed"] == ""  # empty string returns empty
        assert collected[3]["processed"] is None  # null, not valid

    def test_all_conditional_patterns(self, spark):
        """Test all supported conditional naming patterns"""
        ns = PrimitiveRegistry("all_patterns")
        
        # Test each pattern that should be auto-detected
        patterns_to_test = [
            ("is_", "is_ready"),
            ("has_", "has_data"),
            ("needs_", "needs_work"),
            ("should_", "should_run"),
            ("can_", "can_execute"),
            ("contains_", "contains_value"),
            ("matches_", "matches_regex"),
            ("equals_", "equals_target"),
            ("starts_with_", "starts_with_prefix"),
            ("ends_with_", "ends_with_suffix"),
        ]
        
        for prefix, func_name in patterns_to_test:
            # Dynamically create and register function
            def make_func():
                def func(col):
                    return col.isNotNull()
                func.__name__ = func_name
                return func
            
            ns.register()(make_func())
            
            # Verify it was registered as conditional
            assert func_name in ns._conditionals, f"Function {func_name} with prefix {prefix} should be conditional"
            assert func_name not in ns._primitives, f"Function {func_name} should not be in primitives"