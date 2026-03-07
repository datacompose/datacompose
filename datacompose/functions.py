"""
Backend-agnostic functions module for DataCompose.

This module provides a unified interface to SQL functions across different backends
(DuckDB, BigQuery, Snowflake, Spark, Postgres) via SQLFrame.

"""

import importlib
import re
from typing import Optional

SUPPORTED_BACKENDS = ["duckdb", "pyspark", "postgres"]

BACKEND_MODULES = {
    "duckdb": "sqlframe.duckdb.functions",
    "bigquery": "sqlframe.bigquery.functions",
    "snowflake": "sqlframe.snowflake.functions",
    "pyspark": "sqlframe.spark.functions",
    "postgres": "sqlframe.postgres.functions",
}

_backend: Optional[str] = None
_module = None


class UnsupportedBackendError(Exception):
    """Raised when an unsupported backend is specified."""

    pass


class BackendNotInitializedError(Exception):
    """Raised when functions are used before set_backend() is called."""

    pass


# ---------------------------------------------------------------------------
# PostgreSQL regex compatibility
# ---------------------------------------------------------------------------
# PostgreSQL ARE regex uses \y for word boundary (not \b which means backspace).
# We also need to shim regexp_extract which sqlframe.postgres doesn't provide.


def _pg_regex(pattern):
    """Translate common regex tokens to PostgreSQL ARE equivalents."""
    if isinstance(pattern, str):
        pattern = pattern.replace("\\b", "\\y")
        # PostgreSQL ARE requires embedded flags like (?i) at the very start
        # of the pattern.  Collect all (?i), (?s), etc. from anywhere in the
        # pattern, merge them, and prepend once.
        found_flags = set()
        def _collect(m):
            found_flags.update(m.group(1))
            return ""
        pattern = re.sub(r"\(\?([imsxn]+)\)", _collect, pattern)
        if found_flags:
            pattern = "(?" + "".join(sorted(found_flags)) + ")" + pattern
    return pattern


def _patch_postgres_module(module):
    """Add regexp_extract and fix word-boundary translation for postgres."""
    if getattr(module, "_dc_patched", False):
        return
    module._dc_patched = True

    _orig_replace = module.regexp_replace
    _orig_like = module.regexp_like

    # -- regexp_extract shim via regexp_replace + regexp_like ----------------
    def regexp_extract(col, pattern, idx):
        pattern = _pg_regex(pattern)

        # PostgreSQL requires embedded flags like (?i) before ^,
        # so pull them out and re-prepend after we add our own ^.
        flag_match = re.match(r"^(\(\?[imsxn]+\))", pattern)
        flags = flag_match.group(1) if flag_match else ""
        rest = pattern[len(flags):]

        inner = rest
        has_start = inner.startswith("^")
        has_end = inner.endswith("$")
        if has_start:
            inner = inner[1:]
        if has_end:
            inner = inner[:-1]
        prefix = "" if has_start else ".*?"
        suffix = "" if has_end else ".*"
        if idx == 0:
            full = flags + "^" + prefix + "(" + inner + ")" + suffix + "$"
            repl = "\\1"
        else:
            full = flags + "^" + prefix + inner + suffix + "$"
            repl = "\\" + str(idx)
        extracted = _orig_replace(col, module.lit(full), module.lit(repl))
        return module.when(
            _orig_like(col, module.lit(pattern)), extracted
        ).otherwise(module.lit(""))

    # -- helper to translate regex in both strings and Column(Lit(...)) ------
    def _translate_pattern(pattern):
        """Apply _pg_regex to a string or a Column wrapping a string literal."""
        if isinstance(pattern, str):
            return module.lit(_pg_regex(pattern))
        # Column wrapping a string literal (e.g. F.lit(r"\bfoo"))
        try:
            expr = pattern.column_expression
            if getattr(expr, "is_string", False) and isinstance(expr.this, str):
                translated = _pg_regex(expr.this)
                if translated != expr.this:
                    return module.lit(translated)
        except Exception:
            pass
        return pattern

    # -- wrappers that translate \b -> \y ------------------------------------
    def patched_replace(col, pattern, replacement, *a, **kw):
        return _orig_replace(col, _translate_pattern(pattern), replacement, *a, **kw)

    def patched_like(col, pattern, *a, **kw):
        return _orig_like(col, _translate_pattern(pattern), *a, **kw)

    # -- regexp_extract_all shim via ARRAY(SELECT ... regexp_matches(..., 'g')) --
    def regexp_extract_all(col, pattern, idx=0):
        import sqlglot
        from sqlframe.postgres.column import Column

        col_sql = col.column_expression.sql(dialect="postgres")
        if hasattr(pattern, "column_expression"):
            pat_str = _pg_regex(pattern.column_expression.this)
        else:
            pat_str = _pg_regex(pattern)
        group = idx if idx > 0 else 1
        raw_sql = (
            f"COALESCE(ARRAY(SELECT m[{group}] FROM "
            f"regexp_matches({col_sql}, '{pat_str}', 'g') AS m), ARRAY[]::TEXT[])"
        )
        raw_expr = sqlglot.parse_one(raw_sql, dialect="postgres")
        return Column(raw_expr)

    # -- sha2 shim via pgcrypto: encode(digest(col, 'sha256'), 'hex') ---------
    def sha2(col, num_bits):
        import sqlglot
        from sqlframe.postgres.column import Column

        algo = {256: "sha256", 384: "sha384", 512: "sha512"}.get(num_bits, "sha256")
        col_sql = col.column_expression.sql(dialect="postgres")
        raw_sql = f"encode(digest({col_sql}, '{algo}'), 'hex')"
        raw_expr = sqlglot.parse_one(raw_sql, dialect="postgres")
        return Column(raw_expr)

    module.regexp_extract = regexp_extract
    module.regexp_extract_all = regexp_extract_all
    module.regexp_replace = patched_replace
    module.regexp_like = patched_like
    module.sha2 = sha2


def _load_backend_module():
    """Import the backend functions module and apply any patches."""
    global _module
    module_path = BACKEND_MODULES[_backend]
    try:
        _module = importlib.import_module(module_path)
    except ImportError as e:
        raise ImportError(
            f"Failed to import {module_path}. "
            f"Make sure sqlframe is installed with the '{_backend}' extra: "
            f"pip install 'sqlframe[{_backend}]'"
        ) from e
    if _backend == "postgres":
        _patch_postgres_module(_module)


class _LazyFunctionsProxy:
    """Lazy proxy that defers backend module loading until actual attribute access."""

    def __getattr__(self, name: str):
        global _module

        if _backend is None:
            raise BackendNotInitializedError(
                "Backend not initialized. Call set_backend() before using functions. "
                "Example: set_backend('duckdb')"
            )

        if _module is None:
            _load_backend_module()

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
        "BACKEND_MODULES",
        "UnsupportedBackendError",
        "BackendNotInitializedError",
        "functions",
    ):
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

    if _backend is None:
        raise BackendNotInitializedError(
            "Backend not initialized. Call set_backend() before using functions. "
            "Example: set_backend('duckdb')"
        )

    if _module is None:
        _load_backend_module()

    return getattr(_module, name)
