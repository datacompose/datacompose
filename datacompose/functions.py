"""
Backend-agnostic functions module for DataCompose.

This module provides a unified interface to SQL functions across different backends
(DuckDB, BigQuery, Snowflake, Spark, Postgres) via SQLFrame.

"""

import importlib
from typing import Optional

SUPPORTED_BACKENDS = ["duckdb", "bigquery", "snowflake", "pyspark", "postgres"]

_backend: Optional[str] = None
_module = None


class UnsupportedBackendError(Exception):
    """Raised when an unsupported backend is specified."""

    pass


class BackendNotInitializedError(Exception):
    """Raised when functions are used before set_backend() is called."""

    pass


class _LazyFunctionsProxy:
    """Lazy proxy that defers backend module loading until actual attribute access."""

    def __getattr__(self, name: str):
        global _module

        if _backend is None:
            raise BackendNotInitializedError(
                "Backend not initialized. Call set_backend() before using functions. "
                f"Example: set_backend('duckdb')"
            )

        if _module is None:
            backend_modules = {
                "duckdb": "sqlframe.duckdb.functions",
                "bigquery": "sqlframe.bigquery.functions",
                "snowflake": "sqlframe.snowflake.functions",
                "pyspark": "sqlframe.spark.functions",
                "postgres": "sqlframe.postgres.functions",
            }

            module_path = backend_modules[_backend]
            try:
                _module = importlib.import_module(module_path)
            except ImportError as e:
                raise ImportError(
                    f"Failed to import {module_path}. "
                    f"Make sure sqlframe is installed with the '{_backend}' extra: "
                    f"pip install 'sqlframe[{_backend}]'"
                ) from e

        return getattr(_module, name)


# Lazy proxy instance that can be imported as `from datacompose.functions import functions`
functions = _LazyFunctionsProxy()


def set_backend(name: str) -> None:
    """Set the active backend for SQL functions.

    Args:
        name: Backend name (duckdb, bigquery, snowflake, pyspark, postgres)

    Raises:
        UnsupportedBackendError: If the backend is not supported
    """
    global _backend, _module

    if name not in SUPPORTED_BACKENDS:
        raise UnsupportedBackendError(
            f"Unsupported backend: '{name}'. "
            f"Supported backends: {', '.join(SUPPORTED_BACKENDS)}"
        )

    _backend = name
    _module = None  # Reset cache so next access loads the new backend


def get_backend() -> Optional[str]:
    """Get the currently configured backend name."""
    return _backend


def __getattr__(name: str):
    """Lazy-load and delegate attribute access to the backend's functions module."""
    global _module

    # Don't intercept dunder attributes or the module's own public API
    if name.startswith("_") or name in (
        "set_backend",
        "get_backend",
        "SUPPORTED_BACKENDS",
        "UnsupportedBackendError",
        "BackendNotInitializedError",
        "functions",
    ):
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

    if _backend is None:
        raise BackendNotInitializedError(
            "Backend not initialized. Call set_backend() before using functions. "
            f"Example: set_backend('duckdb')"
        )

    if _module is None:
        backend_modules = {
            "duckdb": "sqlframe.duckdb.functions",
            "bigquery": "sqlframe.bigquery.functions",
            "snowflake": "sqlframe.snowflake.functions",
            "pyspark": "sqlframe.spark.functions",
            "postgres": "sqlframe.postgres.functions",
        }

        module_path = backend_modules[_backend]
        try:
            _module = importlib.import_module(module_path)
        except ImportError as e:
            raise ImportError(
                f"Failed to import {module_path}. "
                f"Make sure sqlframe is installed with the '{_backend}' extra: "
                f"pip install 'sqlframe[{_backend}]'"
            ) from e

    return getattr(_module, name)
