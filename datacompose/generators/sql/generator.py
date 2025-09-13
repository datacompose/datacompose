"""
SQL function generator for multiple database dialects.
"""

import sqlglotrs
from pathlib import Path
from typing import Optional

from ..base import BaseGenerator


class SQLGenerator(BaseGenerator):
    """SQL function generator and transpiler for different SQL dialects.

    This generator creates SQL functions that can be deployed to various
    database systems. It uses SQLGlot for dialect transpilation, allowing
    functions written in PostgreSQL syntax to be converted to other SQL dialects.

    Supported dialects include:
        - postgres (PostgreSQL)
        - mysql (MySQL) 
        - snowflake (Snowflake)
        - bigquery (BigQuery)
        - sqlite (SQLite)
        - and more via SQLGlot
    """

    ENGINE_SUBDIRECTORY = "sql"
    PRIMITIVES_FILENAME = "sql_primitives.py"

    def __init__(self, template_dir: Path, output_dir: Path, verbose: bool = False, dialect: str = "postgres"):
        """Initialize the SQL generator with a specific dialect.

        Args:
            template_dir: Directory containing templates (not used for SQL)
            output_dir: Directory to write generated SQL files
            verbose: Enable verbose output
            dialect: Target SQL dialect for transpilation
        """
        super().__init__(template_dir, output_dir, verbose)
        self.dialect = dialect

    def _get_primitives_file(self, transformer_dir: Path | None = None) -> str:
        """Get the SQL functions content for this transformer.
        
        This method imports the SQL primitives module, instantiates the
        transformer class, and generates all SQL functions transpiled 
        to the target dialect.
        """
        if not transformer_dir:
            raise ValueError("transformer_dir is required for SQL generation")
        
        # Import the SQL primitives module
        primitives_path = transformer_dir / self.ENGINE_SUBDIRECTORY / self.PRIMITIVES_FILENAME
        if not primitives_path.exists():
            raise FileNotFoundError(
                f"No {self.PRIMITIVES_FILENAME} found in {transformer_dir / self.ENGINE_SUBDIRECTORY}"
            )
        
        # Import the module dynamically
        import importlib.util
        spec = importlib.util.spec_from_file_location("sql_primitives", primitives_path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        
        # Find the transformer class (e.g., Emails, PhoneNumbers, Addresses)
        transformer_name = transformer_dir.name.replace("_", "").title()
        transformer_class = None
        
        # Try exact match first
        if hasattr(module, transformer_name):
            transformer_class = getattr(module, transformer_name)
        else:
            # Try case-insensitive match
            for attr_name in dir(module):
                attr = getattr(module, attr_name)
                if (isinstance(attr, type) and 
                    attr_name.lower() == transformer_name.lower() and
                    not attr_name.startswith('_')):
                    transformer_class = attr
                    break
        
        if not transformer_class:
            raise ValueError(f"No transformer class found in {primitives_path}")
        
        # Generate SQL functions using the class methods
        return self._generate_sql_functions(transformer_class)

    def _generate_sql_functions(self, cls):
        """Generate and transpile all SQL functions from a primitive class."""
        obj = cls(self.dialect)
        method_list = [
            func
            for func in dir(cls)
            if callable(getattr(cls, func)) and not func.startswith("__")
        ]

        # Organize methods by category based on naming patterns
        extraction_methods = [m for m in method_list if m.startswith("extract_")]
        validation_methods = [
            m for m in method_list if m.startswith(("is_", "has_", "validate_"))
        ]
        cleaning_methods = [
            m for m in method_list if m.startswith(("remove_", "lowercase_", "fix_"))
        ]
        standardization_methods = [
            m
            for m in method_list
            if m.startswith(("standardize_", "normalize_", "get_canonical_"))
        ]
        formatting_methods = [
            m for m in method_list if m.startswith(("format_", "mask_"))
        ]
        hashing_methods = [m for m in method_list if "hash_" in m]
        filtering_methods = [m for m in method_list if m.startswith("filter_")]
        utility_methods = [
            m for m in method_list if m.startswith(("get_", "convert_", "add_"))
        ]

        # Catch any methods that don't fit the categories
        categorized = set(
            extraction_methods
            + validation_methods
            + cleaning_methods
            + standardization_methods
            + formatting_methods
            + hashing_methods
            + filtering_methods
            + utility_methods
        )
        other_methods = [m for m in method_list if m not in categorized]

        sql_file = f"-- SQL Functions for {cls.__name__}\n"
        sql_file += f"-- Generated for dialect: {self.dialect}\n"
        sql_file += "-- " + "=" * 60 + "\n\n"

        # Process methods by category with section headers
        categories = [
            ("EXTRACTION FUNCTIONS", extraction_methods),
            ("VALIDATION FUNCTIONS", validation_methods),
            ("CLEANING FUNCTIONS", cleaning_methods),
            ("STANDARDIZATION FUNCTIONS", standardization_methods),
            ("FORMATTING FUNCTIONS", formatting_methods),
            ("HASHING FUNCTIONS", hashing_methods),
            ("FILTERING FUNCTIONS", filtering_methods),
            ("UTILITY FUNCTIONS", utility_methods),
            ("OTHER FUNCTIONS", other_methods),
        ]

        for category_name, methods in categories:
            if methods:  # Only add section if there are methods
                sql_file += f"\n-- ============================================\n"
                sql_file += f"-- {category_name}\n"
                sql_file += f"-- ============================================\n\n"

                for method in sorted(methods):  # Sort alphabetically within category
                    try:
                        sql_string = getattr(obj, method)()
                        if sql_string and isinstance(sql_string, str):
                            transpiled = self.transpile(sql_string)
                            if self.verbose:
                                print(f"✓ Function: {method}")
                            sql_file += transpiled + "\n\n"
                    except Exception as e:
                        if self.verbose:
                            print(f"✗ Function: {method} - Failed to process: {e}")
                        sql_file += f"-- Failed to transpile function: {method}\n"
                        sql_file += f"-- Error: {e}\n\n"

        return sql_file

    def _get_output_filename(self, transformer_name: str) -> str:
        """Get the output filename for SQL functions."""
        return f"{transformer_name}_{self.dialect}.sql"

    def copy_utils_files(self, output_path: Path):
        """Override to skip copying utils files for SQL generation."""
        # SQL doesn't need Python utils files
        pass

    # Legacy method for backward compatibility
    def get_functions(self, cls):
        """Legacy method - use _generate_sql_functions instead."""
        return self._generate_sql_functions(cls)

    def transpile(self, sql_string: str) -> str:
        """Transpile SQL from PostgreSQL to the target dialect.

        Takes a SQL CREATE FUNCTION statement written in PostgreSQL syntax and
        converts it to the equivalent syntax for the target dialect using SQLGlot.
        This enables writing functions once in PostgreSQL and deploying them
        across different database systems.

        Args:
            sql_string: A SQL CREATE FUNCTION statement in PostgreSQL syntax.

        Returns:
            str: The transpiled SQL statement in the target dialect's syntax.
                 If transpilation fails, returns a commented version of the
                 original SQL with an error message.

        Note:
            The input SQL is always parsed as PostgreSQL since that's the source
            format used in all primitive classes. SQLGlot handles the conversion
            to other dialects like MySQL, Snowflake, BigQuery, etc.

        Example:
            >>> chooser = PrimitiveChooser('mysql')
            >>> pg_sql = "CREATE OR REPLACE FUNCTION test() RETURNS TEXT AS $$ SELECT 'hello' $$ LANGUAGE SQL;"
            >>> mysql_sql = chooser.transpile(pg_sql)
            >>> # mysql_sql now contains MySQL-compatible function syntax
        """
        try:
            ast = sqlglotrs.parse_one(sql_string, "postgres")
            return ast.sql(dialect=self.dialect)
        except AttributeError as e:
            # Handle the case where sqlglotrs doesn't have parse_one
            print(f"Warning: SQLGlot method not found: {e}")
            print("Returning original PostgreSQL syntax")
            return sql_string
        except Exception as e:
            # Handle any other transpilation errors
            error_msg = f"Failed to transpile to {self.dialect}: {str(e)}"
            print(f"Warning: {error_msg}")

            # Return the original SQL as a comment block for manual review
            commented_sql = "-- " + "\n-- ".join(sql_string.split("\n"))
            return f"-- TRANSPILATION FAILED\n-- Error: {error_msg}\n-- Original PostgreSQL function:\n{commented_sql}"


# Factory functions for different SQL dialects
def PostgreSQLGenerator(template_dir: Path, output_dir: Path, verbose: bool = False):
    """Create a PostgreSQL generator."""
    return SQLGenerator(template_dir, output_dir, verbose, dialect="postgres")


def MySQLGenerator(template_dir: Path, output_dir: Path, verbose: bool = False):
    """Create a MySQL generator."""
    return SQLGenerator(template_dir, output_dir, verbose, dialect="mysql")


def SnowflakeGenerator(template_dir: Path, output_dir: Path, verbose: bool = False):
    """Create a Snowflake generator."""
    return SQLGenerator(template_dir, output_dir, verbose, dialect="snowflake")


def BigQueryGenerator(template_dir: Path, output_dir: Path, verbose: bool = False):
    """Create a BigQuery generator."""
    return SQLGenerator(template_dir, output_dir, verbose, dialect="bigquery")


def SQLiteGenerator(template_dir: Path, output_dir: Path, verbose: bool = False):
    """Create a SQLite generator."""
    return SQLGenerator(template_dir, output_dir, verbose, dialect="sqlite")
