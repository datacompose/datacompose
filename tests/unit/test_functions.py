"""Tests for the backend-agnostic functions module."""

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# Add project root to path for imports
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from datacompose import functions
from datacompose.functions import (
    SUPPORTED_BACKENDS,
    BackendNotInitializedError,
    UnsupportedBackendError,
    get_backend,
    set_backend,
)


@pytest.fixture(autouse=True)
def reset_functions_state():
    """Reset the functions module state before each test."""
    functions._backend = None
    functions._module = None
    yield
    # Clean up after test
    functions._backend = None
    functions._module = None


@pytest.mark.unit
class TestSetBackend:
    """Tests for set_backend function."""

    def test_set_backend_duckdb(self):
        """Test setting backend to duckdb."""
        set_backend("duckdb")
        assert get_backend() == "duckdb"

    def test_set_backend_bigquery(self):
        """Test setting backend to bigquery."""
        set_backend("bigquery")
        assert get_backend() == "bigquery"

    def test_set_backend_snowflake(self):
        """Test setting backend to snowflake."""
        set_backend("snowflake")
        assert get_backend() == "snowflake"

    def test_set_backend_pyspark(self):
        """Test setting backend to pyspark."""
        set_backend("pyspark")
        assert get_backend() == "pyspark"

    def test_set_backend_postgres(self):
        """Test setting backend to postgres."""
        set_backend("postgres")
        assert get_backend() == "postgres"

    def test_set_backend_all_supported(self):
        """Test that all SUPPORTED_BACKENDS can be set."""
        for backend in SUPPORTED_BACKENDS:
            set_backend(backend)
            assert get_backend() == backend

    def test_set_backend_invalid_raises_error(self):
        """Test that invalid backend raises UnsupportedBackendError."""
        with pytest.raises(UnsupportedBackendError) as exc_info:
            set_backend("invalid_backend")

        assert "Unsupported backend: 'invalid_backend'" in str(exc_info.value)
        assert "Supported backends:" in str(exc_info.value)

    def test_set_backend_empty_string_raises_error(self):
        """Test that empty string raises UnsupportedBackendError."""
        with pytest.raises(UnsupportedBackendError):
            set_backend("")

    def test_set_backend_resets_module_cache(self):
        """Test that changing backend resets the module cache."""
        set_backend("duckdb")
        functions._module = MagicMock()  # Simulate cached module

        set_backend("bigquery")
        assert functions._module is None  # Should be reset


@pytest.mark.unit
class TestGetBackend:
    """Tests for get_backend function."""

    def test_get_backend_returns_none_initially(self):
        """Test that get_backend returns None before set_backend is called."""
        assert get_backend() is None

    def test_get_backend_returns_set_value(self):
        """Test that get_backend returns the value set by set_backend."""
        set_backend("duckdb")
        assert get_backend() == "duckdb"


@pytest.mark.unit
class TestFunctionsGetattr:
    """Tests for the __getattr__ lazy loading behavior."""

    def test_getattr_without_backend_raises_error(self):
        """Test that accessing functions without set_backend raises error."""
        with pytest.raises(BackendNotInitializedError) as exc_info:
            _ = functions.lower

        assert "Backend not initialized" in str(exc_info.value)
        assert "set_backend()" in str(exc_info.value)

    def test_getattr_with_backend_loads_module(self):
        """Test that accessing functions with backend set loads the module."""
        set_backend("duckdb")

        with patch("datacompose.functions.importlib.import_module") as mock_import:
            mock_module = MagicMock()
            mock_module.lower = MagicMock()
            mock_import.return_value = mock_module

            result = functions.lower

            mock_import.assert_called_once_with("sqlframe.duckdb.functions")
            assert result == mock_module.lower

    def test_getattr_caches_module(self):
        """Test that the module is cached after first access."""
        set_backend("duckdb")

        with patch("datacompose.functions.importlib.import_module") as mock_import:
            mock_module = MagicMock()
            mock_module.lower = MagicMock()
            mock_module.upper = MagicMock()
            mock_import.return_value = mock_module

            # First access
            _ = functions.lower
            # Second access
            _ = functions.upper

            # Should only import once
            assert mock_import.call_count == 1

    def test_getattr_import_error_gives_helpful_message(self):
        """Test that ImportError gives helpful installation message."""
        set_backend("duckdb")

        with patch("datacompose.functions.importlib.import_module") as mock_import:
            mock_import.side_effect = ImportError("No module named 'sqlframe'")

            with pytest.raises(ImportError) as exc_info:
                _ = functions.lower

            assert "sqlframe" in str(exc_info.value)
            assert "pip install" in str(exc_info.value)
            assert "duckdb" in str(exc_info.value)


@pytest.mark.unit
class TestBackendModulePaths:
    """Tests to verify correct module paths for each backend."""

    @pytest.mark.parametrize("backend,expected_path", [
        ("duckdb", "sqlframe.duckdb.functions"),
        ("bigquery", "sqlframe.bigquery.functions"),
        ("snowflake", "sqlframe.snowflake.functions"),
        ("pyspark", "sqlframe.spark.functions"),
        ("postgres", "sqlframe.postgres.functions"),
    ])
    def test_backend_module_path(self, backend, expected_path):
        """Test that each backend maps to the correct module path."""
        set_backend(backend)

        with patch("datacompose.functions.importlib.import_module") as mock_import:
            mock_module = MagicMock()
            mock_import.return_value = mock_module

            # Trigger lazy load
            _ = functions.col

            mock_import.assert_called_once_with(expected_path)


@pytest.mark.unit
class TestSupportedBackends:
    """Tests for SUPPORTED_BACKENDS constant."""

    def test_supported_backends_contains_expected(self):
        """Test that SUPPORTED_BACKENDS contains all expected backends."""
        expected = {"duckdb", "bigquery", "snowflake", "pyspark", "postgres"}
        assert set(SUPPORTED_BACKENDS) == expected

    def test_supported_backends_is_list(self):
        """Test that SUPPORTED_BACKENDS is a list."""
        assert isinstance(SUPPORTED_BACKENDS, list)
