"""Test the SQL Generator classes."""

import shutil
import sys
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

# Add project root to path for imports
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from datacompose.generators.sql.generator import SQLGenerator
from datacompose.generators.sql.postgres_generator import PostgreSQLGenerator


@pytest.mark.unit
class TestBaseSQLGenerator:
    """Test suite for base SQLGenerator functionality."""

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
    def transformer_dir(self, temp_dir):
        """Fixture to provide transformer directory."""
        transformer_dir = temp_dir / "emails"
        transformer_dir.mkdir()
        return transformer_dir

    def test_sql_generator_abstract_class(self, template_dir, output_dir):
        """Test that SQLGenerator cannot be instantiated without dialect."""
        with pytest.raises(
            NotImplementedError, match="must define the dialect class attribute"
        ):
            SQLGenerator(template_dir, output_dir)

    def test_get_output_filename(self, template_dir, output_dir):
        """Test SQL generator produces correct .sql filenames."""

        # Create a concrete subclass for testing
        class TestSQLGenerator(SQLGenerator):
            dialect = "test"

        generator = TestSQLGenerator(template_dir, output_dir)

        # Test different transformer names
        assert generator._get_output_filename("emails") == "emails_test.sql"
        assert generator._get_output_filename("addresses") == "addresses_test.sql"
        assert (
            generator._get_output_filename("phone_numbers") == "phone_numbers_test.sql"
        )

    def test_copy_utils_files_skip(self, template_dir, output_dir):
        """Test that SQL generators skip copying utils files."""

        class TestSQLGenerator(SQLGenerator):
            dialect = "test"

        generator = TestSQLGenerator(template_dir, output_dir)
        output_path = output_dir / "test.sql"

        # Should not raise any errors and should do nothing
        generator.copy_utils_files(output_path)

        # Verify no utils directory was created
        assert not (output_path.parent / "utils").exists()

    def test_get_primitives_file_missing_transformer_dir(
        self, template_dir, output_dir
    ):
        """Test error handling when transformer directory is missing."""

        class TestSQLGenerator(SQLGenerator):
            dialect = "test"

        generator = TestSQLGenerator(template_dir, output_dir)

        with pytest.raises(ValueError, match="transformer_dir is required"):
            generator._get_primitives_file(None)

    def test_get_primitives_file_missing_sql_primitives(
        self, template_dir, output_dir, transformer_dir
    ):
        """Test error handling when sql_primitives.py is missing."""

        class TestSQLGenerator(SQLGenerator):
            dialect = "test"

        generator = TestSQLGenerator(template_dir, output_dir)

        with pytest.raises(FileNotFoundError, match="No sql_primitives.py found"):
            generator._get_primitives_file(transformer_dir)


@pytest.mark.unit
class TestPostgreSQLGenerator:
    """Test suite for PostgreSQLGenerator functionality."""

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
    def transformer_dir(self, temp_dir):
        """Fixture to provide transformer directory."""
        transformer_dir = temp_dir / "emails"
        transformer_dir.mkdir()
        return transformer_dir

    @pytest.fixture
    def postgres_generator(self, template_dir, output_dir):
        """Fixture to provide PostgreSQLGenerator instance."""
        return PostgreSQLGenerator(template_dir, output_dir, verbose=False)

    @pytest.fixture
    def mock_sql_primitives(self, transformer_dir):
        """Fixture to create mock SQL primitives file."""
        # Create sql subdirectory
        sql_dir = transformer_dir / "sql"
        sql_dir.mkdir(exist_ok=True)

        primitives_content = '''
class Emails:
    def __init__(self, dialect):
        self.dialect = dialect

    def extract_email(self):
        return """
CREATE OR REPLACE FUNCTION extract_email(input_text TEXT)
RETURNS TEXT AS $$
    SELECT REGEXP_EXTRACT(input_text, '[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}', 1)
$$ LANGUAGE SQL IMMUTABLE;
"""

    def is_valid_email(self):
        return """
CREATE OR REPLACE FUNCTION is_valid_email(email TEXT)
RETURNS BOOLEAN AS $$
    SELECT email ~ '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$'
$$ LANGUAGE SQL IMMUTABLE;
"""

    def fix_common_typos(self):
        return """
CREATE OR REPLACE FUNCTION fix_common_typos(email TEXT)
RETURNS TEXT AS $$
    SELECT REGEXP_REPLACE(
        REGEXP_REPLACE(email, '@gmai\\.com$', '@gmail.com'),
        '@yahooo\\.com$', '@yahoo.com'
    )
$$ LANGUAGE SQL IMMUTABLE;
"""
'''
        primitives_file = sql_dir / "sql_primitives.py"
        primitives_file.write_text(primitives_content)
        return primitives_file

    def test_postgres_generator_initialization(self, template_dir, output_dir):
        """Test PostgreSQLGenerator initialization."""
        generator = PostgreSQLGenerator(template_dir, output_dir, verbose=True)

        assert generator.template_dir == template_dir
        assert generator.output_dir == output_dir
        assert generator.verbose is True
        assert generator.dialect == "postgres"

    def test_postgres_generator_dialect(self, postgres_generator):
        """Test that PostgreSQL generator has correct dialect."""
        assert postgres_generator.dialect == "postgres"

    def test_postgres_transpile_no_change(self, postgres_generator):
        """Test that PostgreSQL generator doesn't transpile (returns original SQL)."""
        original_sql = """
CREATE OR REPLACE FUNCTION test_function(input TEXT)
RETURNS TEXT AS $$
    SELECT input || '_processed'
$$ LANGUAGE SQL IMMUTABLE;
"""

        result = postgres_generator.transpile(original_sql)
        assert result == original_sql

    def test_postgres_output_filename(self, postgres_generator):
        """Test PostgreSQL generator produces correct filenames."""
        assert (
            postgres_generator._get_output_filename("emails") == "emails_postgres.sql"
        )
        assert (
            postgres_generator._get_output_filename("addresses")
            == "addresses_postgres.sql"
        )

    def test_get_primitives_file_with_mock(
        self, postgres_generator, mock_sql_primitives, transformer_dir
    ):
        """Test SQL function generation with mock primitives."""
        result = postgres_generator._get_primitives_file(transformer_dir)

        # Verify it contains expected content
        assert "-- SQL Functions for Emails" in result
        assert "-- Generated for dialect: postgres" in result
        assert "EXTRACTION FUNCTIONS" in result
        assert "VALIDATION FUNCTIONS" in result
        assert "CLEANING FUNCTIONS" in result
        assert "CREATE OR REPLACE FUNCTION extract_email" in result
        assert "CREATE OR REPLACE FUNCTION is_valid_email" in result
        assert "CREATE OR REPLACE FUNCTION fix_common_typos" in result

    def test_function_categorization(
        self, postgres_generator, mock_sql_primitives, transformer_dir
    ):
        """Test that functions are properly categorized in output."""
        result = postgres_generator._get_primitives_file(transformer_dir)

        # Check that functions appear in the right categories
        lines = result.split("\n")

        # Find category headers and verify functions appear in correct sections
        extraction_section_start = -1
        validation_section_start = -1
        cleaning_section_start = -1

        for i, line in enumerate(lines):
            if "-- EXTRACTION FUNCTIONS" in line:
                extraction_section_start = i
            elif "-- VALIDATION FUNCTIONS" in line:
                validation_section_start = i
            elif "-- CLEANING FUNCTIONS" in line:
                cleaning_section_start = i

        # Verify sections were found
        assert extraction_section_start > -1
        assert validation_section_start > -1
        assert cleaning_section_start > -1

        # Verify extract_email appears after EXTRACTION FUNCTIONS
        extract_email_line = -1
        for i, line in enumerate(lines):
            if "extract_email" in line and "CREATE" in line:
                extract_email_line = i
                break
        assert extract_email_line > extraction_section_start

    def test_inheritance_from_base_generator(self, postgres_generator):
        """Test that PostgreSQLGenerator properly inherits from SQLGenerator."""
        # Check that base methods are available
        assert hasattr(postgres_generator, "generate")
        assert hasattr(postgres_generator, "_calculate_hash")
        assert hasattr(postgres_generator, "_write_output")

        # Check that abstract methods are implemented
        assert hasattr(postgres_generator, "_get_primitives_file")
        assert hasattr(postgres_generator, "_get_output_filename")

    def test_verbose_mode(self, template_dir, output_dir):
        """Test verbose mode configuration."""
        verbose_generator = PostgreSQLGenerator(template_dir, output_dir, verbose=True)
        quiet_generator = PostgreSQLGenerator(template_dir, output_dir, verbose=False)

        assert verbose_generator.verbose is True
        assert quiet_generator.verbose is False

    def test_transformer_class_discovery(self, postgres_generator, transformer_dir):
        """Test transformer class discovery logic."""
        # Create sql subdirectory
        sql_dir = transformer_dir / "sql"
        sql_dir.mkdir(exist_ok=True)

        # Test case-insensitive matching
        primitives_content = """
class emails:  # lowercase class name
    def __init__(self, dialect):
        self.dialect = dialect

    def extract_email(self):
        return "CREATE OR REPLACE FUNCTION test() RETURNS TEXT AS $$ SELECT 'test' $$ LANGUAGE SQL;"
"""
        primitives_file = sql_dir / "sql_primitives.py"
        primitives_file.write_text(primitives_content)

        # Should find the lowercase 'emails' class when looking for 'Emails'
        result = postgres_generator._get_primitives_file(transformer_dir)
        assert "emails" in result.lower()

    def test_transformer_class_not_found(self, postgres_generator, transformer_dir):
        """Test error handling when transformer class is not found."""
        # Create sql subdirectory
        sql_dir = transformer_dir / "sql"
        sql_dir.mkdir(exist_ok=True)

        # Create primitives file without the expected class
        primitives_content = """
class WrongClass:
    def __init__(self, dialect):
        self.dialect = dialect
"""
        primitives_file = sql_dir / "sql_primitives.py"
        primitives_file.write_text(primitives_content)

        with pytest.raises(ValueError, match="No transformer class found"):
            postgres_generator._get_primitives_file(transformer_dir)

    def test_method_error_handling(self, postgres_generator, transformer_dir):
        """Test error handling when SQL methods fail."""
        # Create sql subdirectory
        sql_dir = transformer_dir / "sql"
        sql_dir.mkdir(exist_ok=True)

        # Create primitives with a method that raises an exception
        primitives_content = """
class Emails:
    def __init__(self, dialect):
        self.dialect = dialect

    def extract_email(self):
        return "CREATE OR REPLACE FUNCTION test() RETURNS TEXT AS $$ SELECT 'test' $$ LANGUAGE SQL;"

    def broken_method(self):
        raise Exception("This method is broken")
"""
        primitives_file = sql_dir / "sql_primitives.py"
        primitives_file.write_text(primitives_content)

        # Should handle the error gracefully and continue with other methods
        result = postgres_generator._get_primitives_file(transformer_dir)

        # Should contain the working method
        assert "test()" in result

        # Should contain error comment for broken method
        assert "Failed to transpile function: broken_method" in result
        assert "This method is broken" in result
