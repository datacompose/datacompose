"""
Shared fixtures and helpers for text transformers tests (Spark and SQL).
"""

from __future__ import annotations

import pytest


# -----------------------------
# Canonical test data fixtures
# -----------------------------

@pytest.fixture
def extract_email_test_data():
    """Canonical data for first-email extraction from text."""
    return [
        ("Contact us at john@example.com for info", "john@example.com"),
        ("Email: admin@company.org or sales@company.org", "admin@company.org"),
        ("No email here", None),
        ("user@domain.co.uk is valid", "user@domain.co.uk"),
        ("test.user+tag@gmail.com", "test.user+tag@gmail.com"),
        ("", None),
        (None, None),
    ]


@pytest.fixture
def extract_all_emails_test_data():
    """Canonical data for extracting all emails from text."""
    return [
        (
            "Contact john@example.com or jane@example.org",
            ["john@example.com", "jane@example.org"],
        ),
        ("Email: admin@company.com", ["admin@company.com"]),
        ("No emails here", []),
        (
            "Multiple: a@b.com, c@d.org; e@f.net",
            ["a@b.com", "c@d.org", "e@f.net"],
        ),
        ("", []),
        (None, []),
    ]


@pytest.fixture
def extract_username_test_data():
    """Canonical data for username extraction from email."""
    return [
        ("john.doe@example.com", "john.doe"),
        ("admin+test@company.org", "admin+test"),
        ("user123@domain.net", "user123"),
        ("a@b.com", "a"),
        ("not-an-email", None),
        ("", None),
        (None, None),
    ]


@pytest.fixture
def extract_domain_test_data():
    """Canonical data for domain extraction from email."""
    return [
        ("john@example.com", "example.com"),
        ("user@mail.company.org", "mail.company.org"),
        ("test@domain.co.uk", "domain.co.uk"),
        ("admin@localhost", "localhost"),
        ("not-an-email", None),
        ("", None),
        (None, None),
    ]


@pytest.fixture
def extract_domain_name_test_data():
    """Canonical data for domain name (without TLD) extraction from email."""
    return [
        ("user@gmail.com", "gmail"),
        ("admin@mail.company.org", "mail"),
        ("test@example.co.uk", "example"),
        ("user@localhost.localdomain", "localhost"),
        ("not-an-email", None),
        ("", None),
        (None, None),
    ]


@pytest.fixture
def extract_tld_test_data():
    """Canonical data for TLD extraction from email."""
    return [
        ("user@example.com", "com"),
        ("admin@company.org", "org"),
        ("test@domain.co.uk", "co.uk"),
        ("user@site.edu", "edu"),
        ("test@domain.travel", "travel"),
        ("not-an-email", None),
        ("", None),
        (None, None),
    ]


# ----------------------------------
# Spark-adapted variants of fixtures
# ----------------------------------

def _sparkify(rows):
    """Map None expected outputs to empty-string for Spark expectations."""
    return [(x, (y if y is not None else "")) for x, y in rows]


@pytest.fixture
def extract_email_test_data_spark(extract_email_test_data):
    return _sparkify(extract_email_test_data)


@pytest.fixture
def extract_username_test_data_spark(extract_username_test_data):
    return _sparkify(extract_username_test_data)


@pytest.fixture
def extract_domain_test_data_spark(extract_domain_test_data):
    return _sparkify(extract_domain_test_data)


@pytest.fixture
def extract_domain_name_test_data_spark(extract_domain_name_test_data):
    return _sparkify(extract_domain_name_test_data)


@pytest.fixture
def extract_tld_test_data_spark(extract_tld_test_data):
    return _sparkify(extract_tld_test_data)


# -----------------------------
# SQL helpers / cursor factory
# -----------------------------

@pytest.fixture
def get_db_cursor(postgres_db):
    """Return a function that yields a DB cursor for a given dialect.

    Currently supports 'postgres' by reusing the postgres test schema.
    Other dialects will be skipped until their backends are wired.
    """

    def _get(dialect: str):
        if dialect != "postgres":
            pytest.skip(f"Dialect '{dialect}' not configured for tests")

        # Import lazily so environments without psycopg2 can still collect tests
        try:
            from psycopg2.extras import RealDictCursor  # type: ignore
        except Exception as e:  # pragma: no cover - skip if driver missing
            pytest.skip(f"psycopg2 not available: {e}")

        cur = postgres_db.cursor(cursor_factory=RealDictCursor)
        cur.execute("SET search_path TO test_sql_functions")
        return cur

    return _get

