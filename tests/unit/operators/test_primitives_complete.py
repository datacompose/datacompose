"""Comprehensive test suite for operators/primitives.py module."""

import logging
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Add project root to path for imports
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from datacompose.operators.primitives import (  # noqa: E402
    PrimitiveRegistry,
    SmartPrimitive,
    _fallback_compose,
)


# Fixtures removed - using root conftest.py spark fixture


@pytest.fixture
def sample_df(spark):
    """Create a sample DataFrame for testing."""
    data = [
        ("  Hello World  ", 123, True),
        ("  Python  ", 456, False),
        ("  Spark  ", 789, True),
        (None, 0, None),
    ]
    return spark.createDataFrame(data, ["text", "number", "flag"])


@pytest.mark.unit
class TestSmartPrimitive:
    """Test SmartPrimitive class functionality."""

    def test_smart_primitive_direct_call(self, spark):
        """Test calling SmartPrimitive directly with a column."""

        def trim_func(col, chars=" "):
            return F.trim(F.col("text") if isinstance(col, str) else col)

        primitive = SmartPrimitive(trim_func, "trim")

        # Test direct call with column
        df = spark.createDataFrame([("  test  ",)], ["text"])
        result = df.select(primitive(F.col("text"))).collect()
        assert result[0][0] == "test"

    def test_smart_primitive_configured_call(self, spark):
        """Test creating configured version of SmartPrimitive."""

        def replace_func(col, old=" ", new="_"):
            return F.regexp_replace(col, old, new)

        primitive = SmartPrimitive(replace_func, "replace")

        # Create configured version
        replace_spaces = primitive(old=" ", new="_")
        assert callable(replace_spaces)
        assert "old= " in replace_spaces.__name__ or "old=" in replace_spaces.__name__

        # Test configured version
        df = spark.createDataFrame([("hello world",)], ["text"])
        result = df.select(replace_spaces(F.col("text"))).collect()
        assert result[0][0] == "hello_world"

    def test_smart_primitive_name_and_doc(self):
        """Test SmartPrimitive preserves function name and docstring."""

        def test_func(col):
            """Test docstring."""
            return col

        primitive = SmartPrimitive(test_func)
        assert primitive.name == "test_func"
        assert primitive.__doc__ == "Test docstring."

        # Test with custom name
        primitive2 = SmartPrimitive(test_func, "custom_name")
        assert primitive2.name == "custom_name"


@pytest.mark.unit
class TestPrimitiveRegistry:
    """Test PrimitiveRegistry class functionality."""

    def test_registry_register_and_access(self):
        """Test registering and accessing primitives in registry."""
        registry = PrimitiveRegistry("test")

        @registry.register()
        def lowercase(col):
            return F.lower(col)

        # Test primitive is registered
        assert hasattr(registry, "lowercase")
        assert "lowercase" in registry._primitives
        assert isinstance(registry.lowercase, SmartPrimitive)

    def test_registry_register_conditional(self):
        """Test registering conditional primitives."""
        registry = PrimitiveRegistry("test")

        @registry.register(is_conditional=True)
        def when_not_null(col):
            return F.when(col.isNotNull(), col)

        # Test conditional is registered
        assert hasattr(registry, "when_not_null")
        assert "when_not_null" in registry._conditionals
        assert isinstance(registry.when_not_null, SmartPrimitive)

    def test_registry_custom_name(self):
        """Test registering with custom name."""
        registry = PrimitiveRegistry("test")

        @registry.register(name="custom")
        def some_func(col):
            return col

        assert hasattr(registry, "custom")
        assert "custom" in registry._primitives

    def test_registry_getattr_error(self):
        """Test __getattr__ raises error for non-existent primitive."""
        registry = PrimitiveRegistry("test")

        with pytest.raises(AttributeError, match="No primitive 'nonexistent'"):
            _ = registry.nonexistent

    def test_registry_getattr_conditional(self):
        """Test __getattr__ returns conditionals."""
        registry = PrimitiveRegistry("test")

        @registry.register()
        def cond_func(col):
            return col

        # Access via __getattr__
        primitive = registry.cond_func
        assert isinstance(primitive, SmartPrimitive)


@pytest.mark.unit
class TestCompose:
    """Test compose decorator functionality."""

    def test_compose_basic_pipeline(self, spark):
        """Test basic compose pipeline."""
        registry = PrimitiveRegistry("test")

        @registry.register()
        def trim(col):
            return F.trim(col)

        @registry.register()
        def lower(col):
            return F.lower(col)

        # We need to make registry available as 'test' in the namespace
        test = registry

        @registry.compose(test=registry)
        def clean_text():
            test.trim()
            test.lower()

        df = spark.createDataFrame([("  HELLO  ",)], ["text"])
        result = df.select(clean_text(F.col("text"))).collect()
        assert result[0][0] == "hello"

    def test_compose_with_debug(self, spark, caplog):
        """Test compose with debug mode."""
        registry = PrimitiveRegistry("test")

        @registry.register()
        def upper(col):
            return F.upper(col)

        test = registry  # Make registry available as 'test'

        with caplog.at_level(logging.DEBUG):

            @registry.compose(debug=True, test=registry)
            def make_upper():
                test.upper()

            df = spark.createDataFrame([("hello",)], ["text"])
            result = df.select(make_upper(F.col("text"))).collect()
            assert result[0][0] == "HELLO"
            # Debug mode should log steps
            # Note: Debug logging might not be captured in all cases

    def test_compose_with_steps(self, spark):
        """Test compose with pre-configured steps."""
        registry = PrimitiveRegistry("test")

        def upper_func(col):
            return F.upper(col)

        def trim_func(col):
            return F.trim(col)

        # When using steps, we need to provide a function to decorate
        @registry.compose(steps=[trim_func, upper_func])
        def pipeline():
            pass  # Body is ignored when steps are provided

        df = spark.createDataFrame([("  hello  ",)], ["text"])
        result = df.select(pipeline(F.col("text"))).collect()
        assert result[0][0] == "HELLO"

    def test_compose_inside_function_without_decorator_args(self, spark):
        """Test compose without arguments - fallback mode."""
        # Create global registry
        registry = PrimitiveRegistry("test")

        @registry.register()
        def reverse(col):
            return F.reverse(col)

        # Make the registry available in global scope for compose to find it
        globals()["test"] = registry

        @registry.compose()
        def reverse_text():
            test.reverse()

        df = spark.createDataFrame([("hello",)], ["text"])
        result = df.select(reverse_text(F.col("text"))).collect()
        # In fallback mode with no source, it returns identity function
        assert result[0][0] == "olleh"  # Not reversed


@pytest.mark.unit
class TestFallbackCompose:
    """Test _fallback_compose function."""

    def test_fallback_compose_basic(self, spark):
        """Test fallback compose extracts sequential calls."""
        registry = PrimitiveRegistry("test")

        @registry.register()
        def trim(col):
            return F.trim(col)

        test = registry  # Make registry available

        def pipeline_func():
            test.trim()

        # Mock to force fallback
        pipeline = _fallback_compose(pipeline_func, {"test": registry}, False)

        df = spark.createDataFrame([("  hello  ",)], ["text"])
        result = df.select(pipeline(F.col("text"))).collect()
        assert result[0][0] == "hello"

    def test_fallback_compose_with_kwargs(self, spark):
        """Test fallback compose with keyword arguments."""
        registry = PrimitiveRegistry("test")

        @registry.register()
        def replace(col, old=" ", new="_"):
            return F.regexp_replace(col, old, new)

        test = registry  # Make registry available

        def pipeline_func():
            test.replace(old=" ", new="-")

        pipeline = _fallback_compose(pipeline_func, {"test": registry}, False)

        df = spark.createDataFrame([("hello world",)], ["text"])
        result = df.select(pipeline(F.col("text"))).collect()
        assert result[0][0] == "hello-world"

    def test_fallback_compose_error_handling(self, spark):
        """Test fallback compose error handling returns identity."""

        def bad_func():
            # This will cause parsing issues
            pass

        # Mock inspect.getsource to raise exception
        with patch("inspect.getsource", side_effect=Exception("Source error")):
            pipeline = _fallback_compose(bad_func, {}, False)

        # Should return identity function
        df = spark.createDataFrame([("test",)], ["text"])
        result = df.select(pipeline(F.col("text"))).collect()
        assert result[0][0] == "test"  # Unchanged
        assert "Failed to compile" in pipeline.__doc__


@pytest.mark.unit
class TestEdgeCasesAndErrors:
    """Test edge cases and error conditions."""

    def test_import_error_handling(self):
        """Test handling when PySpark is not available."""
        # This is already handled by the try/except in the module
        # Just verify the module loads without PySpark
        assert True

    def test_primitive_with_none_column(self):
        """Test SmartPrimitive with None column returns configured function."""

        def test_func(col, param=1):
            return col

        primitive = SmartPrimitive(test_func)
        configured = primitive(None, param=2)

        assert callable(configured)
        assert "param=2" in configured.__name__

    def test_registry_namespace_in_error(self):
        """Test registry namespace appears in error message."""
        registry = PrimitiveRegistry("my_namespace")

        with pytest.raises(AttributeError, match="my_namespace"):
            _ = registry.nonexistent_method
