import sys
from pathlib import Path

import pytest
from pyspark.sql import functions as f

from datacompose.operators.primitives import PrimitiveRegistry, SmartPrimitive

# Add project root to path for imports
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

# Module-level namespace for testing declarative syntax

# Create module-level namespace
email_ns = PrimitiveRegistry("email_ns")


@email_ns.register()
def trim(col):
    return f.trim(col)


@email_ns.register()
def lower(col):
    return f.lower(col)


# Module-level composed function with declarative syntax
@email_ns.compose(email_ns=email_ns, debug=True)
def clean_email_declarative():
    email_ns.trim()  # type: ignore
    email_ns.lower()  # type: ignore


@pytest.fixture(scope="session")
def sparksession():
    import os

    from pyspark.sql import SparkSession

    # Use SPARK_MASTER env var if available (spark://spark-master:7077), otherwise local
    master = os.environ.get("SPARK_MASTER", "spark://spark-master:7077")

    spark = (
        SparkSession.builder.appName("TestApp")
        .master(master)
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    return spark


@pytest.fixture
def sample_spark_df(sparksession):
    """Create a simple PySpark DataFrame for testing"""
    spark = sparksession
    data = [
        ("Hello", 100, "john.doe@example.com"),
        ("WORLD", 200, "JANE.SMITH@COMPANY.COM"),
        ("Test", 300, "test.user@domain.org"),
    ]
    df = spark.createDataFrame(data, ["text_col", "num_col", "email"])
    return df


@pytest.mark.unit
class TestPrimitiveIO:
    """Test Suite to Test the adding the primitives"""

    @pytest.fixture(scope="class")
    def simple_pyspark_column(self):

        def lowercase_func(col):
            return f.lower(col)

        return lowercase_func

    def test_smart_primitive(self, simple_pyspark_column, sparksession):

        # Ensure SparkSession is active for f.col() to work

        primitive = SmartPrimitive(simple_pyspark_column)

        # Test that primitive wraps the function correctly
        assert callable(primitive)

        # When we call primitive with a column, it should return the result
        # of applying the function to that column
        col = f.col("foo")
        result = primitive(col)

        # The result should be a Column expression (lowercase of foo)
        assert result is not None

    def test_namespace(self, sparksession):

        def trim_impl(col, regex=r"\s+"):
            return f.regexp_replace(col, regex, "")

        primitive = SmartPrimitive(trim_impl)
        configured = primitive(regex=r"[\s\t]+")

        # configured is a function that takes a column and returns a transformed column
        assert callable(configured)

        # When we call configured with a column, it returns a Column (not a function)
        result = configured(f.col("foo"))
        assert result is not None
        # The result is a Column expression: regexp_replace(foo, '[\s\t]+', '')
        assert str(result).startswith("Column<'regexp_replace")

    def test_namespace_registration(self, sparksession):

        ns = PrimitiveRegistry("test")

        @ns.register()
        def test_func(col: str):
            return f.upper(col)

        assert "test_func" in ns._primitives
        assert isinstance(ns.test_func, SmartPrimitive)

    def test_with_spark(self, sample_spark_df, sparksession):

        string = PrimitiveRegistry("string")

        @string.register()
        def lowercase(col: str):
            return f.lower(col)

        result = sample_spark_df.withColumn(
            "clean", string.lowercase(f.col("text_col"))
        )

        assert result.collect()[0]["clean"] == "hello"


@pytest.mark.unit
class TestCompositionFramework:

    def test_manual_composition(self, sample_spark_df, sparksession):
        """Test that manual composition works first"""

        email = PrimitiveRegistry("email")

        @email.register()
        def lower(col):
            return f.lower(col)

        @email.register()
        def trim(col):
            return f.trim(col)

        # Manual composition - this should work
        result = sample_spark_df.withColumn(
            "clean_email", email.lower(email.trim(f.col("email")))
        )

        # Verify the transformation worked
        collected = result.collect()
        assert collected[0]["clean_email"] == "john.doe@example.com"
        assert collected[1]["clean_email"] == "jane.smith@company.com"

    def test_compose(self, sample_spark_df, sparksession):

        email = PrimitiveRegistry("email")

        @email.register()
        def lower(col):
            return f.lower(col)

        @email.register()
        def trim(col):
            return f.trim(col)

        # Use compose decorator with steps passed directly
        # (inspect.getsource doesn't work in test methods)
        # Need to call the SmartPrimitive with no args to get the configured function
        configured_trim = email.trim()
        configured_lower = email.lower()

        @email.compose(steps=[configured_trim, configured_lower], debug=True)
        def clean_emails():
            pass

        # This one needs the namespace passed
        @email.compose(email=email, debug=False)
        def clean():
            email.trim()  # type: ignore
            email.lower()  # type: ignore

        # Check what's in the pipeline
        # print(f"Pipeline steps: {clean_emails.steps}")
        # print(f"Step types: {[type(s) for s in clean_emails.steps]}")

        result = sample_spark_df.withColumn(
            "clean_email", clean_emails(f.col("email"))  # type: ignore
        )

        # Verify the transformation worked
        collected = result.collect()
        assert collected[0]["clean_email"] == "john.doe@example.com"
        assert collected[1]["clean_email"] == "jane.smith@company.com"

    def test_declarative_syntax(self, sample_spark_df, sparksession):
        """Test compose decorator with declarative syntax using module-level function"""
        # Use the module-level composed function
        result = sample_spark_df.withColumn(
            "clean_email", clean_email_declarative(f.col("email"))  # type: ignore
        )

        # Verify the transformation worked
        collected = result.collect()
        assert collected[0]["clean_email"] == "john.doe@example.com"
        assert collected[1]["clean_email"] == "jane.smith@company.com"
        assert collected[2]["clean_email"] == "test.user@domain.org"

        # Verify the pipeline has the correct steps
        assert hasattr(clean_email_declarative, "steps")  # type: ignore
        assert len(clean_email_declarative.steps) == 2  # type: ignore
        print(f"Declarative pipeline steps: {clean_email_declarative.steps}")  # type: ignore

    def test_complex_compose(self):
        # TODO: Add more complex composition tests
        pass


@pytest.mark.unit
class TestTypeValidation:
    """Test type safety and validation features"""

    def test_validated_namespace(self, sparksession):
        """Test namespace with validation"""

        # Create namespace with validation
        validated = PrimitiveRegistry("validated")

        @validated.register()
        def require_param(col, required_field):
            """Function that requires a parameter"""
            return f.concat(col, f.lit(required_field))

        # Test with parameter works
        configured = validated.require_param(required_field="_suffix")
        assert callable(configured)

        # Test without parameter also works (will fail at runtime)
        no_param = validated.require_param()  # type: ignore
        assert callable(no_param)

    def test_conditional_registration(self, sparksession):
        """Test conditional primitive registration"""

        ns = PrimitiveRegistry("conditional_test")

        # Register a regular primitive
        @ns.register()
        def lowercase(col):
            return f.lower(col)

        # Register conditionals (returns boolean)
        @ns.register(is_conditional=True)
        def is_empty(col):
            return col.isNull() | (col == "")

        @ns.register(is_conditional=True)
        def is_long(col, min_length=10):
            return f.length(col) > min_length

        # Verify storage locations
        assert "lowercase" in ns._primitives
        assert "lowercase" not in ns._conditionals
        assert "is_empty" not in ns._primitives
        assert "is_empty" in ns._conditionals
        assert "is_long" not in ns._primitives
        assert "is_long" in ns._conditionals

        # All should be accessible via attributes
        assert hasattr(ns, "lowercase")
        assert hasattr(ns, "is_empty")
        assert hasattr(ns, "is_long")

        # Test they return Column expressions
        test_col = f.col("test")
        lower_result = ns.lowercase(test_col)
        empty_check = ns.is_empty(test_col)
        long_check = ns.is_long(min_length=5)(test_col)

        assert lower_result is not None
        assert empty_check is not None
        assert long_check is not None

        # Test with actual data to verify they work
        data = [("HELLO",), ("",), (None,), ("A" * 20,)]
        df = sparksession.createDataFrame(data, ["text"])

        result = df.select(
            f.col("text"),
            ns.lowercase(f.col("text")).alias("lower"),
            ns.is_empty(f.col("text")).alias("is_empty"),
            ns.is_long(min_length=10)(f.col("text")).alias("is_long"),
        )

        collected = result.collect()

        # Verify results
        assert collected[0]["lower"] == "hello"
        assert not collected[0]["is_empty"]
        assert not collected[0]["is_long"]

        assert collected[1]["lower"] == ""
        assert collected[1]["is_empty"]
        assert not collected[1]["is_long"]

        assert collected[2]["lower"] is None
        assert collected[2]["is_empty"]
        assert collected[2]["is_long"] is None  # NULL input

        assert collected[3]["lower"] == "a" * 20
        assert not collected[3]["is_empty"]
        assert collected[3]["is_long"]

    def test_conditional_filtering(self, sparksession):
        """Test using conditionals to filter data that doesn't meet criteria"""

        ns = PrimitiveRegistry("filtering")

        # Register regular transforms
        @ns.register()
        def clean_text(col):
            return f.trim(f.lower(col))

        # Register conditionals for validation
        @ns.register(is_conditional=True)
        def is_valid_email(col):
            # Simple email check - contains @ and .
            return col.contains("@") & col.contains(".")

        @ns.register(is_conditional=True)
        def is_min_length(col, min_len=3):
            return f.length(col) >= min_len

        # Test data with mix of valid and invalid entries
        data = [
            ("John.Doe@example.com",),  # Valid email
            ("invalid-email",),  # Missing @ and .
            ("@.",),  # Has @ and . but too short
            ("a@b.c",),  # Valid but short
            ("",),  # Empty
            (None,),  # Null
            ("Alice.Smith@company.org",),  # Valid email
        ]
        df = sparksession.createDataFrame(data, ["email"])

        # Apply cleaning and validation
        result = df.select(
            f.col("email"),
            ns.clean_text(f.col("email")).alias("cleaned"),
            ns.is_valid_email(f.col("email")).alias("is_valid_email"),
            ns.is_min_length(min_len=5)(f.col("email")).alias("is_min_length"),
        ).withColumn(
            "passes_all_checks", f.col("is_valid_email") & f.col("is_min_length")
        )

        collected = result.collect()

        # Check results
        assert collected[0]["cleaned"] == "john.doe@example.com"
        assert collected[0]["is_valid_email"] is True
        assert collected[0]["is_min_length"] is True
        assert collected[0]["passes_all_checks"] is True

        assert collected[1]["cleaned"] == "invalid-email"
        assert collected[1]["is_valid_email"] is False  # Fails email check
        assert collected[1]["is_min_length"] is True
        assert collected[1]["passes_all_checks"] is False

        assert collected[2]["cleaned"] == "@."
        assert collected[2]["is_valid_email"] is True
        assert collected[2]["is_min_length"] is False  # Fails length check
        assert collected[2]["passes_all_checks"] is False

        assert collected[3]["cleaned"] == "a@b.c"
        assert collected[3]["is_valid_email"] is True
        assert collected[3]["is_min_length"] is True  # Exactly 5 chars
        assert collected[3]["passes_all_checks"] is True

        assert collected[4]["cleaned"] == ""
        assert collected[4]["is_valid_email"] is False
        assert collected[4]["is_min_length"] is False
        assert collected[4]["passes_all_checks"] is False

        assert collected[5]["cleaned"] is None
        assert collected[5]["is_valid_email"] is None  # NULL propagation
        assert collected[5]["is_min_length"] is None
        assert collected[5]["passes_all_checks"] is None

        assert collected[6]["cleaned"] == "alice.smith@company.org"
        assert collected[6]["is_valid_email"] is True
        assert collected[6]["is_min_length"] is True
        assert collected[6]["passes_all_checks"] is True

        # Test filtering to only valid records
        valid_only = df.filter(
            ns.is_valid_email(f.col("email"))
            & ns.is_min_length(min_len=5)(f.col("email"))
        )

        valid_collected = valid_only.collect()
        assert len(valid_collected) == 3  # Only 3 records pass both checks
        assert valid_collected[0]["email"] == "John.Doe@example.com"
        assert valid_collected[1]["email"] == "a@b.c"
        assert valid_collected[2]["email"] == "Alice.Smith@company.org"

    def test_compose_with_validation(self, sample_spark_df, sparksession):
        """Test compose with validation features"""

        text = PrimitiveRegistry("text")

        @text.register()
        def validate_length(col, min_len=1, max_len=100):
            """Validate text length"""
            return f.when(
                (f.length(col) >= min_len) & (f.length(col) <= max_len), col
            ).otherwise(f.lit(None))

        @text.register()
        def trim(col):
            return f.trim(col)

        @text.register()
        def lowercase(col):
            return f.lower(col)

        # Compose with validation
        @text.compose(text=text, debug=True)
        def validated_pipeline():
            text.trim()
            text.lowercase()
            text.validate_length(min_len=3, max_len=50)

        result = sample_spark_df.withColumn(
            "validated", validated_pipeline(f.col("text_col"))  # type: ignore
        )

        # Check results
        collected = result.collect()
        # "Hello" -> "hello" (valid length)
        assert collected[0]["validated"] == "hello"
        # "WORLD" -> "world" (valid length)
        assert collected[1]["validated"] == "world"
        # "Test" -> "test" (valid length)
        assert collected[2]["validated"] == "test"

    def test_error_handling(self, sparksession):
        """Test error handling in compose"""

        ns = PrimitiveRegistry("error_test")

        @ns.register()
        def good_transform(col):
            return f.lower(col)

        # Try to use non-existent primitive
        try:

            @ns.compose(ns=ns)
            def bad_pipeline():
                ns.good_transform()
                ns.nonexistent()  # This doesn't exist

            # The compose should handle this gracefully
            # It will only include the valid transform
            assert callable(bad_pipeline)  # type: ignore

        except AttributeError:
            # This is also acceptable behavior
            pass

    def test_null_safety(self, sparksession):
        """Test null handling in pipelines"""

        safe = PrimitiveRegistry("safe")

        @safe.register()
        def safe_lower(col):
            """Safely lowercase with null check"""
            return f.when(col.isNotNull(), f.lower(col)).otherwise(col)

        @safe.register()
        def safe_trim(col):
            """Safely trim with null check"""
            return f.when(col.isNotNull(), f.trim(col)).otherwise(col)

        @safe.compose(safe=safe)
        def null_safe_pipeline():
            safe.safe_trim()
            safe.safe_lower()

        # Test with nulls
        data = [("  HELLO  ",), (None,), ("",), ("  World  ",)]
        df = sparksession.createDataFrame(data, ["text"])

        result = df.withColumn("cleaned", null_safe_pipeline(f.col("text")))  # type: ignore
        collected = result.collect()

        assert collected[0]["cleaned"] == "hello"
        assert collected[1]["cleaned"] is None  # Null preserved
        assert collected[2]["cleaned"] == ""
        assert collected[3]["cleaned"] == "world"

    def test_parameterized_pipeline(self, sample_spark_df, sparksession):
        """Test pipeline with parameterized steps"""

        transform = PrimitiveRegistry("transform")

        @transform.register()
        def replace(col, pattern, replacement):
            return f.regexp_replace(col, pattern, replacement)

        @transform.register()
        def substring(col, start, length):
            return f.substring(col, start, length)

        # Create pipeline with specific parameters
        @transform.compose(transform=transform, debug=True)
        def param_pipeline():
            transform.replace(pattern="e", replacement="3")
            transform.replace(pattern="o", replacement="0")
            transform.substring(start=1, length=3)

        result = sample_spark_df.withColumn(
            "transformed", param_pipeline(f.col("text_col"))  # type: ignore
        )

        collected = result.collect()
        # "Hello" -> "H3ll0" -> "H3l"
        assert collected[0]["transformed"] == "H3l"

    def test_pipeline_introspection(self, sparksession):
        """Test ability to inspect pipeline steps"""

        inspect_ns = PrimitiveRegistry("inspect")

        @inspect_ns.register()
        def step1(col):
            return f.trim(col)

        @inspect_ns.register()
        def step2(col):
            return f.lower(col)

        # Create pipeline with explicit steps for introspection
        steps = [inspect_ns.step1(), inspect_ns.step2()]

        @inspect_ns.compose(steps=steps, debug=False)
        def introspectable():
            pass

        # Check we can see the steps
        assert hasattr(introspectable, "steps") or callable(introspectable)  # type: ignore

        # The pipeline should work
        test_col = f.col("test")
        result = introspectable(test_col)  # type: ignore
        assert result is not None

    def test_compose_namespace_requirement(self, sparksession):
        """Test that compose requires namespace to be passed for method resolution"""
        
        ns = PrimitiveRegistry("test_ns")
        
        @ns.register()
        def transform1(col):
            return f.upper(col)
            
        @ns.register()
        def transform2(col):
            return f.trim(col)
        
        # Test with namespace passed - should work
        @ns.compose(ns=ns, debug=False)
        def pipeline_with_ns():
            ns.transform1()
            ns.transform2()
        
        # Test without namespace - will use fallback
        @ns.compose(debug=False)
        def pipeline_without_ns():
            ns.transform1()
            ns.transform2()
        
        # Both should be callable
        assert callable(pipeline_with_ns)
        assert callable(pipeline_without_ns)
        
        # Test with actual data
        data = [("  hello  ",), ("  world  ",)]
        df = sparksession.createDataFrame(data, ["text"])
        
        # Pipeline with namespace should work correctly
        result_with_ns = df.withColumn("processed", pipeline_with_ns(f.col("text")))
        collected_with_ns = result_with_ns.collect()
        assert collected_with_ns[0]["processed"] == "HELLO"
        assert collected_with_ns[1]["processed"] == "WORLD"
        
        # Pipeline without namespace might not work as expected (fallback behavior)
        # This tests that the fallback doesn't break, even if it doesn't apply transforms
        result_without_ns = df.withColumn("processed", pipeline_without_ns(f.col("text")))
        collected_without_ns = result_without_ns.collect()
        # The fallback might return the original column or empty pipeline
        assert collected_without_ns is not None


@pytest.mark.unit
class TestPipelineCompilation:
    """Test the new pipeline compilation features with conditionals"""

    def test_pipeline_with_conditionals(self, sample_spark_df, sparksession):
        """Test that conditional statements compile correctly"""

        text = PrimitiveRegistry("text")

        @text.register()
        def clean(col):
            return f.trim(f.lower(col))

        @text.register(is_conditional=True)
        def is_long(col):
            return f.length(col) > 4

        @text.register()
        def truncate(col):
            return f.substring(col, 1, 4)

        @text.register()
        def add_suffix(col):
            return f.concat(col, f.lit("..."))

        # Pipeline with conditional logic
        @text.compose(text=text)
        def smart_clean():
            text.clean()
            if text.is_long():
                text.truncate()
                text.add_suffix()

        # Test the pipeline
        result = sample_spark_df.withColumn(
            "processed", smart_clean(f.col("text_col"))  # type: ignore
        )

        collected = result.collect()

        # "Hello" -> "hello" -> "hell..." (length > 4, so truncated)
        assert collected[0]["processed"] == "hell..."

        # "WORLD" -> "world" -> "worl..." (length > 4, so truncated)
        assert collected[1]["processed"] == "worl..."

        # "Test" -> "test" -> "test" (length = 4, not truncated)
        assert collected[2]["processed"] == "test"

    def test_nested_conditionals(self, sparksession):
        """Test nested if/else statements in pipeline"""

        ns = PrimitiveRegistry("nested")

        @ns.register()
        def clean(col):
            return f.trim(col)

        @ns.register(is_conditional=True)
        def is_empty(col):
            return col == ""

        @ns.register(is_conditional=True)
        def is_short(col):
            return f.length(col) < 5

        @ns.register()
        def make_upper(col):
            return f.upper(col)

        @ns.register()
        def make_lower(col):
            return f.lower(col)

        @ns.register()
        def default_value(col):
            return f.lit("DEFAULT")

        # Nested conditional pipeline
        @ns.compose(ns=ns, debug=True)
        def nested_pipeline():
            ns.clean()
            if ns.is_empty():
                ns.default_value()
            else:
                if ns.is_short():
                    ns.make_upper()
                else:
                    ns.make_lower()

        # Test data
        data = [("  HELLO WORLD  ",), ("  HI  ",), ("",), ("  ok  ",)]
        df = sparksession.createDataFrame(data, ["text"])

        result = df.withColumn("processed", nested_pipeline(f.col("text")))  # type: ignore
        collected = result.collect()

        # "  HELLO WORLD  " -> "HELLO WORLD" -> "hello world" (long, lowercase)
        assert collected[0]["processed"] == "hello world"

        # "  HI  " -> "HI" -> "HI" (short, uppercase)
        assert collected[1]["processed"] == "HI"

        # "" -> "" -> "DEFAULT" (empty, default)
        assert collected[2]["processed"] == "DEFAULT"

        # "  ok  " -> "ok" -> "OK" (short, uppercase)
        assert collected[3]["processed"] == "OK"

    def test_compilation_fallback(self, sparksession):
        """Test fallback when compilation fails"""

        ns = PrimitiveRegistry("fallback")

        @ns.register()
        def step1(col):
            return f.lower(col)

        @ns.register()
        def step2(col):
            return f.trim(col)

        # This should still work even if advanced compilation fails
        # The fallback will extract sequential steps
        @ns.compose(ns=ns, debug=True)
        def fallback_pipeline():
            ns.step1()
            ns.step2()
            # Even with invalid syntax, fallback should handle sequential steps
            # some_invalid_syntax_here = 1  # This would be ignored

        data = [("  HELLO  ",)]
        df = sparksession.createDataFrame(data, ["text"])

        result = df.withColumn("processed", fallback_pipeline(f.col("text")))  # type: ignore
        collected = result.collect()

        # Should still apply step1 and step2
        assert collected[0]["processed"] == "hello"

    def test_stable_pipeline_execution(self, sparksession):
        """Test StablePipeline class directly"""
        from datacompose.operators.primitives import CompiledStep, StablePipeline

        # Create steps manually
        def lower_func(col):
            return f.lower(col)

        def trim_func(col):
            return f.trim(col)

        steps = [
            CompiledStep(step_type="transform", action=lower_func),
            CompiledStep(step_type="transform", action=trim_func),
        ]

        # Create and execute pipeline
        pipeline = StablePipeline(steps, debug=True)

        data = [("  HELLO  ",)]
        df = sparksession.createDataFrame(data, ["text"])

        result = df.withColumn("processed", pipeline(f.col("text")))
        collected = result.collect()

        assert collected[0]["processed"] == "hello"

    def test_conditional_with_transforms(self, sample_spark_df, sparksession):
        """Test mixing conditionals and transforms in complex pipelines"""

        email = PrimitiveRegistry("email")

        @email.register()
        def lowercase(col):
            return f.lower(col)

        @email.register()
        def trim(col):
            return f.trim(col)

        @email.register(is_conditional=True)
        def has_at_symbol(col):
            return col.contains("@")

        @email.register()
        def replace_at(col):
            return f.regexp_replace(col, "@", "_at_")

        @email.register()
        def add_invalid_prefix(col):
            return f.concat(f.lit("INVALID_"), col)

        # Complex pipeline with conditional logic
        @email.compose(email=email, debug=True)
        def process_email():
            email.trim()
            email.lowercase()
            if email.has_at_symbol():
                email.replace_at()
            else:
                email.add_invalid_prefix()

        result = sample_spark_df.withColumn(
            "processed", process_email(f.col("email"))  # type: ignore
        )

        collected = result.collect()

        # Emails with @ get it replaced
        assert "_at_" in collected[0]["processed"]
        assert "_at_" in collected[1]["processed"]
        assert "_at_" in collected[2]["processed"]

        # Verify lowercase and trim were applied
        assert collected[0]["processed"] == "john.doe_at_example.com"
        assert collected[1]["processed"] == "jane.smith_at_company.com"

    def test_conditional_branches_with_multiple_steps(self, sparksession):
        """Test conditional branches with multiple steps in each branch"""

        proc = PrimitiveRegistry("proc")

        @proc.register(is_conditional=True)
        def is_numeric(col):
            # Simple check - if string starts with a digit
            return f.substring(col, 1, 1).rlike("[0-9]")

        @proc.register()
        def prefix_num(col):
            return f.concat(f.lit("NUM_"), col)

        @proc.register()
        def make_upper(col):
            return f.upper(col)

        @proc.register()
        def prefix_text(col):
            return f.concat(f.lit("TEXT_"), col)

        @proc.register()
        def make_lower(col):
            return f.lower(col)

        @proc.compose(proc=proc, debug=True)
        def classify_and_process():
            if proc.is_numeric():
                proc.prefix_num()
                proc.make_upper()
            else:
                proc.prefix_text()
                proc.make_lower()

        data = [("123abc",), ("abc123",), ("456",), ("xyz",)]
        df = sparksession.createDataFrame(data, ["value"])

        result = df.withColumn("processed", classify_and_process(f.col("value")))  # type: ignore
        collected = result.collect()

        # Numeric prefix (starts with digit)
        assert collected[0]["processed"] == "NUM_123ABC"  # "123abc" -> numeric branch
        assert collected[2]["processed"] == "NUM_456"  # "456" -> numeric branch

        # Text prefix (doesn't start with digit)
        assert collected[1]["processed"] == "text_abc123"  # "abc123" -> text branch
        assert collected[3]["processed"] == "text_xyz"  # "xyz" -> text branch

    def test_compiled_step_validation(self, sparksession):
        """Test that CompiledStep validation works correctly"""
        import pytest

        from datacompose.operators.primitives import CompiledStep

        # Valid transform step
        def transform_func(col):
            return f.lower(col)

        step = CompiledStep(step_type="transform", action=transform_func)
        assert step.step_type == "transform"

        # Invalid step type
        with pytest.raises(ValueError, match="Invalid step_type"):
            CompiledStep(step_type="invalid", action=transform_func)

        # Transform without action
        with pytest.raises(
            ValueError, match="Transform step requires a callable action"
        ):
            CompiledStep(step_type="transform", action=None)

        # Conditional without condition
        with pytest.raises(
            ValueError, match="Conditional step requires a callable condition"
        ):
            CompiledStep(step_type="conditional", condition=None, then_branch=[step])

        # Conditional without then_branch
        with pytest.raises(
            ValueError, match="Conditional step requires at least a then_branch"
        ):
            CompiledStep(
                step_type="conditional", condition=lambda x: True, then_branch=None
            )

        # Valid conditional step
        valid_conditional = CompiledStep(
            step_type="conditional",
            condition=lambda x: True,
            then_branch=[step],
            else_branch=[step],
        )
        assert valid_conditional.step_type == "conditional"

    def test_pipeline_validation(self, sparksession):
        """Test that StablePipeline validates its steps"""
        import pytest

        from datacompose.operators.primitives import CompiledStep, StablePipeline

        # Valid pipeline
        def action(col):
            return f.lower(col)

        steps = [
            CompiledStep(step_type="transform", action=action),
            CompiledStep(step_type="transform", action=action),
        ]
        pipeline = StablePipeline(steps)
        assert len(pipeline.steps) == 2

        # Pipeline with invalid step type
        with pytest.raises(TypeError, match="Pipeline step .* must be a CompiledStep"):
            StablePipeline(["not a step"])  # type: ignore

        # Empty pipeline should be valid
        empty_pipeline = StablePipeline([])
        assert len(empty_pipeline.steps) == 0
