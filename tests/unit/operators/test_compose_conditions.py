"""
Test compose decorator with if-then conditional logic.
Tests must have @compose decorated functions at module level for proper AST parsing.
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from datacompose.operators.primitives import PrimitiveRegistry

# Create a test namespace with primitives
test_ns = PrimitiveRegistry("test")


# Register test primitives
@test_ns.register()
def is_long(col):
    """Check if text is longer than 5 characters."""
    return F.length(col) > 5


@test_ns.register()
def is_short(col):
    """Check if text is 5 characters or less."""
    return F.length(col) <= 5


@test_ns.register()
def is_uppercase(col):
    """Check if text is all uppercase."""
    return col == F.upper(col)


@test_ns.register()
def is_null(col):
    """Check if value is null."""
    return col.isNull()


@test_ns.register()
def to_upper(col):
    """Convert to uppercase."""
    return F.upper(col)


@test_ns.register()
def to_lower(col):
    """Convert to lowercase."""
    return F.lower(col)


@test_ns.register()
def add_prefix(col, prefix="PREFIX:"):
    """Add a prefix to the text."""
    return F.concat(F.lit(prefix), col)


@test_ns.register()
def add_suffix(col, suffix=":SUFFIX"):
    """Add a suffix to the text."""
    return F.concat(col, F.lit(suffix))


@test_ns.register()
def mask(col):
    """Mask the text."""
    return F.lit("***MASKED***")


@test_ns.register()
def reverse(col):
    """Reverse the text."""
    return F.reverse(col)


@test_ns.register()
def default_value(col, value="DEFAULT"):
    """Return a default value."""
    return F.lit(value)


# Module-level composed functions with conditionals


@test_ns.compose(debug=True)
def simple_if_then():
    """Simple if-then without else."""
    if test_ns.is_long():
        test_ns.to_upper()


@test_ns.compose(debug=True)
def if_then_else():
    """If-then-else conditional."""
    if test_ns.is_long():
        test_ns.to_upper()
    else:
        test_ns.to_lower()


@test_ns.compose(test=test_ns, debug=True)
def nested_conditions():
    """Nested if-elif-else conditions."""
    if test_ns.is_long():
        if test_ns.is_uppercase():
            test_ns.to_lower()
        else:
            test_ns.to_upper()
    else:
        test_ns.add_prefix()


@test_ns.compose(test=test_ns, debug=True)
def multiple_transforms_in_branch():
    """Multiple transformations in a single branch."""
    if test_ns.is_short():
        test_ns.to_upper()
        test_ns.add_prefix(prefix="SHORT:")
        test_ns.add_suffix(suffix=":END")
    else:
        test_ns.to_lower()
        test_ns.reverse()


@test_ns.compose(test=test_ns, debug=True)
def elif_chain():
    """Elif chain simulation with nested if-else."""
    if test_ns.is_short():
        test_ns.add_prefix(prefix="S:")
    elif test_ns.is_uppercase():
        test_ns.add_prefix(prefix="U:")
    else:
        test_ns.add_prefix(prefix="DEFAULT:")


@test_ns.compose(test=test_ns, debug=True)
def handle_nulls():
    """Handle null values explicitly."""
    if test_ns.is_null():
        test_ns.default_value(value="NULL_REPLACED")
    else:
        if test_ns.is_long():
            test_ns.to_upper()
        else:
            test_ns.to_lower()


@test_ns.compose(test=test_ns, debug=True)
def complex_nested():
    """Complex nested conditions with multiple levels."""
    if test_ns.is_null():
        test_ns.default_value(value="WAS_NULL")
    else:
        if test_ns.is_long():
            if test_ns.is_uppercase():
                test_ns.add_prefix(prefix="LONG_UPPER:")
                test_ns.to_lower()
            else:
                test_ns.add_prefix(prefix="LONG_LOWER:")
                test_ns.to_upper()
        else:
            if test_ns.is_uppercase():
                test_ns.add_suffix(suffix=":SHORT_UPPER")
            else:
                test_ns.add_suffix(suffix=":SHORT_LOWER")


class TestComposeConditions:
    """Test class for compose decorator with conditional logic."""


    def test_simple_if_then(self, spark):
        """Test simple if-then without else branch."""
        data = [
            ("short",),  # 5 chars - not long
            ("this is long",),  # > 5 chars - long
        ]
        df = spark.createDataFrame(data, ["text"])

        result_df = df.withColumn("processed", simple_if_then(F.col("text")))
        results = result_df.collect()

        # Short text should remain unchanged (no else branch)
        assert results[0]["processed"] == "short"
        # Long text should be uppercased
        assert results[1]["processed"] == "THIS IS LONG"

    def test_if_then_else(self, spark):
        """Test if-then-else conditional."""
        data = [
            ("short",),  # 5 chars - short
            ("this is long",),  # > 5 chars - long
        ]
        df = spark.createDataFrame(data, ["text"])

        result_df = df.withColumn("processed", if_then_else(F.col("text")))
        results = result_df.collect()

        # Short text should be lowercased (else branch)
        assert results[0]["processed"] == "short"
        # Long text should be uppercased (then branch)
        assert results[1]["processed"] == "THIS IS LONG"

    def test_nested_conditions(self, spark):
        """Test nested if conditions."""
        data = [
            ("short",),  # Short - gets prefix
            ("ALREADY UPPER",),  # Long and uppercase - to lower
            ("long text",),  # Long and not uppercase - to upper
        ]
        df = spark.createDataFrame(data, ["text"])

        result_df = df.withColumn("processed", nested_conditions(F.col("text")))
        results = result_df.collect()

        assert results[0]["processed"] == "PREFIX:short"
        assert results[1]["processed"] == "already upper"
        assert results[2]["processed"] == "LONG TEXT"

    def test_multiple_transforms_in_branch(self, spark):
        """Test multiple transformations in a single branch."""
        data = [
            ("hi",),  # Short - gets multiple transforms
            ("this is longer",),  # Long - gets different transforms
        ]
        df = spark.createDataFrame(data, ["text"])

        result_df = df.withColumn(
            "processed", multiple_transforms_in_branch(F.col("text"))
        )
        results = result_df.collect()

        # Short: uppercase -> prefix -> suffix
        assert results[0]["processed"] == "SHORT:HI:END"
        # Long: lowercase -> reverse
        assert results[1]["processed"] == "regnol si siht"

    def test_elif_chain(self, spark):
        """Test elif chain with different conditions."""
        test_cases = [
            (("short",), "S:short"),  # First condition
            (("UPPERCASE",), "U:UPPERCASE"),  # Second condition
            (("Mixed Case Long",), "DEFAULT:Mixed Case Long"),  # Default
        ]

        for data, expected in test_cases:
            df = spark.createDataFrame([data], ["text"])
            result_df = df.withColumn("processed", elif_chain(F.col("text")))
            result = result_df.collect()[0]
            assert result["processed"] == expected

    def test_handle_nulls(self, spark):
        """Test null handling in conditions."""
        data = [
            ("text",),  # Short text
            ("long text",),  # Long text
            (None,),  # Null value
        ]
        df = spark.createDataFrame(data, ["text"])

        result_df = df.withColumn("processed", handle_nulls(F.col("text")))
        results = result_df.collect()

        assert results[0]["processed"] == "text"  # Short -> lowercase
        assert results[1]["processed"] == "LONG TEXT"  # Long -> uppercase
        assert results[2]["processed"] == "NULL_REPLACED"  # Null -> replaced

    def test_complex_nested(self, spark):
        """Test complex nested conditions with multiple levels."""
        data = [
            (None,),  # Null
            ("LONG TEXT",),  # Long uppercase
            ("long text",),  # Long lowercase
            ("SHORT",),  # Short uppercase
            ("short",),  # Short lowercase
        ]
        df = spark.createDataFrame(data, ["text"])

        result_df = df.withColumn("processed", complex_nested(F.col("text")))
        results = result_df.collect()

        assert results[0]["processed"] == "WAS_NULL"
        assert results[1]["processed"] == "long_upper:long text"  # to_lower affects whole string
        assert results[2]["processed"] == "LONG_LOWER:LONG TEXT"
        assert results[3]["processed"] == "SHORT:SHORT_UPPER"
        assert results[4]["processed"] == "short:SHORT_LOWER"

    def test_empty_dataframe(self, spark):
        """Test compose functions with empty dataframe."""
        df = spark.createDataFrame([], "text: string")

        result_df = df.withColumn("processed", if_then_else(F.col("text")))
        assert result_df.count() == 0

    def test_multiple_columns(self, spark):
        """Test that compose functions work with multiple columns."""
        data = [
            (1, "short"),
            (2, "this is long"),
        ]
        df = spark.createDataFrame(data, ["id", "text"])

        result_df = df.withColumn("processed", if_then_else(F.col("text")))
        results = result_df.collect()

        assert results[0]["id"] == 1
        assert results[0]["processed"] == "short"
        assert results[1]["id"] == 2
        assert results[1]["processed"] == "THIS IS LONG"

    def test_chained_compose_functions(self, spark):
        """Test chaining multiple compose functions."""
        data = [("test text",)]
        df = spark.createDataFrame(data, ["text"])

        # Apply multiple composed functions in sequence
        result_df = df.withColumn("step1", if_then_else(F.col("text"))).withColumn(
            "step2", simple_if_then(F.col("step1"))
        )
        result = result_df.collect()[0]

        # "test text" is long -> uppercase -> "TEST TEXT"
        # "TEST TEXT" is still long -> uppercase again -> "TEST TEXT"
        assert result["step1"] == "TEST TEXT"
        assert result["step2"] == "TEST TEXT"

    @pytest.mark.parametrize(
        "input_text,expected",
        [
            ("hi", "SHORT:HI:END"),
            ("hello", "SHORT:HELLO:END"),  # 5 chars is short
            ("hello world", "dlrow olleh"),
        ],
    )
    def test_parametrized_transforms(self, spark, input_text, expected):
        """Test with parametrized inputs."""
        df = spark.createDataFrame([(input_text,)], ["text"])

        result_df = df.withColumn(
            "processed", multiple_transforms_in_branch(F.col("text"))
        )
        result = result_df.collect()[0]

        assert result["processed"] == expected


class TestComposeEdgeCases:
    """Test edge cases for compose with conditionals."""


    def test_empty_branches_should_compile(self):
        """Test that empty branches still compile but may not transform."""

        @test_ns.compose(debug=True)
        def empty_then_branch():
            """Empty then branch."""
            if test_ns.is_long():
                pass  # Empty branch
            else:
                test_ns.to_lower()

        # Function should be callable
        assert callable(empty_then_branch)

    def test_only_condition_no_transform(self):
        """Test condition without any transformations."""

        @test_ns.compose(debug=True)
        def only_condition():
            """Only has a condition check, no transforms."""
            if test_ns.is_long():
                pass

        assert callable(only_condition)

    def test_deeply_nested_conditions(self, spark):
        """Test very deeply nested conditions."""

        @test_ns.compose(debug=True)
        def deeply_nested():
            """Multiple levels of nesting."""
            if test_ns.is_long():
                if test_ns.is_uppercase():
                    if test_ns.is_null():
                        test_ns.default_value()
                    else:
                        test_ns.to_lower()
                else:
                    test_ns.to_upper()
            else:
                test_ns.mask()

        data = [
            ("SHORT",),  # Short -> masked
            ("LONG UPPER",),  # Long, upper, not null -> lower
            ("long lower",),  # Long, not upper -> upper
        ]
        df = spark.createDataFrame(data, ["text"])

        result_df = df.withColumn("processed", deeply_nested(F.col("text")))
        results = result_df.collect()

        assert results[0]["processed"] == "***MASKED***"
        assert results[1]["processed"] == "long upper"
        assert results[2]["processed"] == "LONG LOWER"


class TestMultipleNamespaces:
    """Test compose with multiple namespaces auto-detection."""
    
    
    def test_setup_namespaces(self):
        """Set up multiple namespaces for testing."""
        # Create additional namespaces
        global text_ns, num_ns, str_ns
        
        text_ns = PrimitiveRegistry("text")
        num_ns = PrimitiveRegistry("num")
        str_ns = PrimitiveRegistry("str")
        
        # Register text operations
        @text_ns.register()
        def is_long(col):
            return F.length(col) > 5
        
        @text_ns.register()
        def to_upper(col):
            return F.upper(col)
        
        @text_ns.register()
        def add_prefix(col, prefix="PREFIX:"):
            return F.concat(F.lit(prefix), col)
        
        # Register number operations
        @num_ns.register()
        def multiply_by_ten(col):
            return col * 10
        
        @num_ns.register()
        def to_string(col):
            return F.cast(col, "string")
        
        # Register string operations
        @str_ns.register()
        def add_suffix(col, suffix=":SUFFIX"):
            return F.concat(col, F.lit(suffix))
        
        @str_ns.register()
        def reverse(col):
            return F.reverse(col)
        
        # Verify namespaces are registered
        assert hasattr(text_ns, "is_long")
        assert hasattr(num_ns, "multiply_by_ten")
        assert hasattr(str_ns, "add_suffix")
    
    def test_two_namespaces(self, spark):
        """Test using two different namespaces without passing them."""
        # Ensure namespaces are set up
        self.test_setup_namespaces()
        
        @text_ns.compose(debug=True)
        def two_namespace_pipeline():
            """Use text_ns and str_ns together."""
            if text_ns.is_long():
                text_ns.to_upper()
                text_ns.add_prefix(prefix="LONG:")
                str_ns.add_suffix(suffix=":DONE")  # Different namespace!
        
        data = [
            ("short",),
            ("this is long",),
        ]
        df = spark.createDataFrame(data, ["text"])
        
        result_df = df.withColumn("processed", two_namespace_pipeline(F.col("text")))
        results = result_df.collect()
        
        # Short text unchanged
        assert results[0]["processed"] == "short"
        # Long text transformed by both namespaces
        assert results[1]["processed"] == "LONG:THIS IS LONG:DONE"
    
    def test_three_namespaces(self, spark):
        """Test using three different namespaces."""
        # Ensure namespaces are set up
        self.test_setup_namespaces()
        
        @text_ns.compose(debug=True)
        def three_namespace_pipeline():
            """Use all three namespaces."""
            if text_ns.is_long():
                text_ns.to_upper()
                str_ns.reverse()
                str_ns.add_suffix(suffix=":END")
        
        data = [("hello world",)]
        df = spark.createDataFrame(data, ["text"])
        
        result_df = df.withColumn("processed", three_namespace_pipeline(F.col("text")))
        result = result_df.collect()[0]
        
        # "hello world" -> "HELLO WORLD" -> "DLROW OLLEH" -> "DLROW OLLEH:END"
        assert result["processed"] == "DLROW OLLEH:END"
    
    def test_namespace_with_original_test_ns(self, spark):
        """Test that original test_ns still works with new namespaces."""
        # Ensure namespaces are set up
        self.test_setup_namespaces()
        
        @test_ns.compose(debug=True)
        def mixed_with_test_ns():
            """Mix test_ns with text_ns."""
            if test_ns.is_short():
                test_ns.to_upper()
                text_ns.add_prefix(prefix="SHORT:")  # Use text_ns
            else:
                str_ns.reverse()  # Use str_ns
        
        data = [
            ("hi",),
            ("hello world",),
        ]
        df = spark.createDataFrame(data, ["text"])
        
        result_df = df.withColumn("processed", mixed_with_test_ns(F.col("text")))
        results = result_df.collect()
        
        # "hi" is short -> "HI" -> "SHORT:HI"
        assert results[0]["processed"] == "SHORT:HI"
        # "hello world" is long -> reversed
        assert results[1]["processed"] == "dlrow olleh"
    
    def test_namespace_override_in_decorator(self, spark):
        """Test that explicitly passed namespaces override auto-detection."""
        # Ensure namespaces are set up
        self.test_setup_namespaces()
        
        # Create a dummy namespace for override
        dummy_ns = PrimitiveRegistry("dummy")
        
        @dummy_ns.register()
        def always_foo(col):
            return F.lit("FOO")
        
        # Pass text_ns explicitly as 'text_ns' even though it's auto-detected
        @test_ns.compose(debug=True, text_ns=dummy_ns)  # Override text_ns!
        def override_pipeline():
            """text_ns should be overridden to dummy_ns."""
            text_ns.always_foo()  # This should call dummy_ns.always_foo
        
        data = [("test",)]
        df = spark.createDataFrame(data, ["text"])
        
        result_df = df.withColumn("processed", override_pipeline(F.col("text")))
        result = result_df.collect()[0]
        
        # Should return "FOO" from dummy_ns, not process with real text_ns
        assert result["processed"] == "FOO"
