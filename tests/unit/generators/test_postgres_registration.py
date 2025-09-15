"""Test PostgreSQL generator registration functionality."""

import shutil
import sys
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock

import pytest

# Add project root to path for imports
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from datacompose.generators.sql.postgres_generator import PostgreSQLGenerator


@pytest.mark.unit
class TestPostgreSQLGeneratorRegistration:
    """Test suite for PostgreSQL generator registration functionality."""

    @pytest.fixture
    def temp_dir(self):
        """Fixture to provide temporary directory for tests."""
        temp_dir = tempfile.mkdtemp()
        yield Path(temp_dir)
        shutil.rmtree(temp_dir)

    @pytest.fixture
    def template_dir(self, temp_dir):
        """Fixture to provide template directory."""
        template_dir = temp_dir / "templates"
        template_dir.mkdir()
        return template_dir

    @pytest.fixture
    def output_dir(self, temp_dir):
        """Fixture to provide output directory."""
        output_dir = temp_dir / "output"
        output_dir.mkdir()
        return output_dir

    @pytest.fixture
    def postgres_generator(self, template_dir, output_dir):
        """Fixture to provide PostgreSQLGenerator instance."""
        return PostgreSQLGenerator(template_dir, output_dir, verbose=True)

    @pytest.fixture
    def sample_sql_content(self):
        """Fixture providing sample SQL content with multiple functions."""
        return """-- SQL Functions for Emails
-- Generated for dialect: postgres

CREATE OR REPLACE FUNCTION extract_email(input_text TEXT)
RETURNS TEXT AS $$
    SELECT (regexp_matches(input_text, '[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}'))[1]
$$ LANGUAGE SQL IMMUTABLE;

CREATE OR REPLACE FUNCTION is_valid_email(email TEXT)
RETURNS BOOLEAN AS $$
    SELECT email ~ '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$'
$$ LANGUAGE SQL IMMUTABLE;

CREATE OR REPLACE FUNCTION standardize_email(email TEXT)
RETURNS TEXT AS $$
    SELECT LOWER(TRIM(email))
$$ LANGUAGE SQL IMMUTABLE;
"""

    def test_register_functions_method_exists(self, postgres_generator):
        """Test that register_functions method exists."""
        assert hasattr(postgres_generator, 'register_functions')
        assert callable(postgres_generator.register_functions)

    def test_register_functions_missing_psycopg2(self, postgres_generator, sample_sql_content):
        """Test register_functions when psycopg2 is not installed."""
        with patch('builtins.__import__', side_effect=ImportError("No module named 'psycopg2'")):
            success, message = postgres_generator.register_functions(sample_sql_content)

            assert success is False
            assert "psycopg2 not installed" in message
            assert "pip install psycopg2-binary" in message

    def test_register_functions_successful_registration(self, postgres_generator, sample_sql_content):
        """Test successful function registration."""
        # Mock psycopg2 and database connection
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor

        with patch('datacompose.generators.sql.postgres_generator.psycopg2') as mock_psycopg2:
            with patch('datacompose.cli.config.ConfigLoader.get_postgres_config') as mock_config:
                mock_psycopg2.connect.return_value = mock_conn
                mock_config.return_value = {
                    'host': 'localhost',
                    'port': 5432,
                    'database': 'test_db',
                    'user': 'test_user',
                    'password': 'test_pass'
                }

                success, message = postgres_generator.register_functions(sample_sql_content)

                assert success is True
                assert "Successfully registered 3 functions" in message
                assert "extract_email, is_valid_email, standardize_email" in message

                # Verify database operations
                mock_psycopg2.connect.assert_called_once_with(
                    host='localhost',
                    port=5432,
                    database='test_db',
                    user='test_user',
                    password='test_pass'
                )
                mock_conn.cursor.assert_called_once()
                assert mock_cursor.execute.call_count == 3  # Three functions
                mock_conn.commit.assert_called_once()
                mock_cursor.close.assert_called_once()
                mock_conn.close.assert_called_once()

    def test_register_functions_connection_failure(self, postgres_generator, sample_sql_content):
        """Test registration with database connection failure."""
        with patch('datacompose.generators.sql.postgres_generator.psycopg2') as mock_psycopg2:
            with patch('datacompose.cli.config.ConfigLoader.get_postgres_config') as mock_config:
                mock_config.return_value = {
                    'host': 'localhost',
                    'port': 5432,
                    'database': 'test_db',
                    'user': 'test_user',
                    'password': 'test_pass'
                }
                # Simulate connection failure
                mock_psycopg2.connect.side_effect = Exception("Connection refused")

                success, message = postgres_generator.register_functions(sample_sql_content)

                assert success is False
                assert "Database connection failed: Connection refused" in message

    def test_register_functions_partial_failure(self, postgres_generator, sample_sql_content):
        """Test registration with some functions failing."""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor

        # Make the second function execution fail
        def execute_side_effect(sql):
            if "is_valid_email" in sql:
                raise Exception("Syntax error in function")

        mock_cursor.execute.side_effect = execute_side_effect

        with patch('datacompose.generators.sql.postgres_generator.psycopg2') as mock_psycopg2:
            with patch('datacompose.cli.config.ConfigLoader.get_postgres_config') as mock_config:
                mock_psycopg2.connect.return_value = mock_conn
                mock_config.return_value = {
                    'host': 'localhost',
                    'port': 5432,
                    'database': 'test_db',
                    'user': 'test_user',
                    'password': 'test_pass'
                }

                success, message = postgres_generator.register_functions(sample_sql_content)

                assert success is False
                assert "Registered 2 functions, 1 failed" in message
                assert "is_valid_email: Syntax error in function" in message

    def test_register_functions_with_custom_config(self, postgres_generator, sample_sql_content):
        """Test registration with custom database configuration."""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor

        custom_config = {
            'host': 'custom-host',
            'port': 5433,
            'database': 'custom_db',
            'user': 'custom_user',
            'password': 'custom_pass'
        }

        with patch('datacompose.generators.sql.postgres_generator.psycopg2') as mock_psycopg2:
            mock_psycopg2.connect.return_value = mock_conn

            success, message = postgres_generator.register_functions(sample_sql_content, custom_config)

            assert success is True
            mock_psycopg2.connect.assert_called_once_with(
                host='custom-host',
                port=5433,
                database='custom_db',
                user='custom_user',
                password='custom_pass'
            )

    def test_extract_sql_statements(self, postgres_generator, sample_sql_content):
        """Test SQL statement extraction."""
        statements = postgres_generator._extract_sql_statements(sample_sql_content)

        assert len(statements) == 3
        assert "CREATE OR REPLACE FUNCTION extract_email" in statements[0]
        assert "CREATE OR REPLACE FUNCTION is_valid_email" in statements[1]
        assert "CREATE OR REPLACE FUNCTION standardize_email" in statements[2]

        # Each statement should end with semicolon
        for statement in statements:
            assert statement.strip().endswith(';')

    def test_extract_sql_statements_with_comments(self, postgres_generator):
        """Test SQL statement extraction ignoring comments."""
        sql_with_comments = """-- This is a comment
CREATE OR REPLACE FUNCTION test_func(input TEXT)
RETURNS TEXT AS $$
    -- Internal comment
    SELECT input || '_processed'
$$ LANGUAGE SQL IMMUTABLE;

-- Another comment
-- Multiple lines
CREATE OR REPLACE FUNCTION another_func()
RETURNS TEXT AS $$
    SELECT 'test'
$$ LANGUAGE SQL IMMUTABLE;
"""

        statements = postgres_generator._extract_sql_statements(sql_with_comments)
        assert len(statements) == 2
        assert "test_func" in statements[0]
        assert "another_func" in statements[1]

    def test_extract_sql_statements_complex_dollar_quotes(self, postgres_generator):
        """Test SQL statement extraction with complex dollar quote scenarios."""
        complex_sql = """CREATE OR REPLACE FUNCTION complex_func(input TEXT)
RETURNS TEXT AS $BODY$
BEGIN
    -- This contains $$ inside the function body
    IF input LIKE '%$$%' THEN
        RETURN 'Contains dollar signs';
    END IF;
    RETURN input;
END;
$BODY$ LANGUAGE plpgsql IMMUTABLE;

CREATE OR REPLACE FUNCTION simple_func()
RETURNS TEXT AS $$
    SELECT 'simple'
$$ LANGUAGE SQL IMMUTABLE;
"""

        statements = postgres_generator._extract_sql_statements(complex_sql)
        assert len(statements) == 2
        assert "complex_func" in statements[0]
        assert "$BODY$" in statements[0]
        assert "simple_func" in statements[1]

    def test_extract_function_name(self, postgres_generator):
        """Test function name extraction from SQL statements."""
        test_cases = [
            ("CREATE OR REPLACE FUNCTION extract_email(input TEXT)", "extract_email"),
            ("CREATE FUNCTION is_valid_email(email TEXT)", "is_valid_email"),
            ("  CREATE OR REPLACE FUNCTION   standardize_email  (email TEXT)", "standardize_email"),
            ("create or replace function lowercase_func()", "lowercase_func"),
            ("invalid sql statement", "unknown_function"),
        ]

        for sql, expected_name in test_cases:
            actual_name = postgres_generator._extract_function_name(sql)
            assert actual_name == expected_name

    def test_extract_sql_statements_empty_content(self, postgres_generator):
        """Test SQL statement extraction with empty content."""
        statements = postgres_generator._extract_sql_statements("")
        assert statements == []

        statements = postgres_generator._extract_sql_statements("-- Only comments\n-- No functions")
        assert statements == []

    def test_extract_sql_statements_malformed_sql(self, postgres_generator):
        """Test SQL statement extraction with malformed SQL."""
        malformed_sql = """CREATE OR REPLACE FUNCTION broken_func(input TEXT)
RETURNS TEXT AS $$
    SELECT input
-- Missing closing $$ and semicolon

CREATE OR REPLACE FUNCTION good_func()
RETURNS TEXT AS $$
    SELECT 'good'
$$ LANGUAGE SQL IMMUTABLE;
"""

        statements = postgres_generator._extract_sql_statements(malformed_sql)
        # Should extract the good function, may or may not extract the broken one
        assert len(statements) >= 1
        assert "good_func" in statements[-1]  # Last statement should be the good one

    def test_verbose_output_during_registration(self, template_dir, output_dir, sample_sql_content):
        """Test verbose output during registration."""
        verbose_generator = PostgreSQLGenerator(template_dir, output_dir, verbose=True)

        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor

        with patch('datacompose.generators.sql.postgres_generator.psycopg2') as mock_psycopg2:
            with patch('datacompose.cli.config.ConfigLoader.get_postgres_config') as mock_config:
                with patch('builtins.print') as mock_print:
                    mock_psycopg2.connect.return_value = mock_conn
                    mock_config.return_value = {
                        'host': 'localhost',
                        'port': 5432,
                        'database': 'test_db',
                        'user': 'test_user',
                        'password': 'test_pass'
                    }

                    success, message = verbose_generator.register_functions(sample_sql_content)

                    assert success is True
                    # Should have printed connection info and function registration details
                    mock_print.assert_any_call("Connecting to PostgreSQL at localhost:5432")
                    mock_print.assert_any_call("✓ Registered function: extract_email")
                    mock_print.assert_any_call("✓ Registered function: is_valid_email")
                    mock_print.assert_any_call("✓ Registered function: standardize_email")

    def test_register_functions_quiet_mode(self, template_dir, output_dir, sample_sql_content):
        """Test registration in quiet mode (verbose=False)."""
        quiet_generator = PostgreSQLGenerator(template_dir, output_dir, verbose=False)

        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor

        with patch('datacompose.generators.sql.postgres_generator.psycopg2') as mock_psycopg2:
            with patch('datacompose.cli.config.ConfigLoader.get_postgres_config') as mock_config:
                with patch('builtins.print') as mock_print:
                    mock_psycopg2.connect.return_value = mock_conn
                    mock_config.return_value = {
                        'host': 'localhost',
                        'port': 5432,
                        'database': 'test_db',
                        'user': 'test_user',
                        'password': 'test_pass'
                    }

                    success, message = quiet_generator.register_functions(sample_sql_content)

                    assert success is True
                    # Should not have printed detailed connection and registration info
                    mock_print.assert_not_called()