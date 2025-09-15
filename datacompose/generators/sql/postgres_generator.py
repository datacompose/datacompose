"""
PostgreSQL SQL function generator.
"""

from pathlib import Path
from typing import Dict, Tuple, Optional, Any

from .generator import SQLGenerator


class PostgreSQLGenerator(SQLGenerator):
    """PostgreSQL-specific SQL function generator.

    Generates SQL CREATE FUNCTION statements optimized for PostgreSQL.
    Uses PostgreSQL syntax as the base format with no transpilation needed.

    Features:
    - Native PostgreSQL function syntax
    - Supports all PostgreSQL data types and functions
    - Optimized for PostgreSQL performance
    """

    dialect = "postgres"

    def __init__(self, template_dir: Path, output_dir: Path, verbose: bool = False):
        """Initialize the PostgreSQL generator.

        Args:
            template_dir: Directory containing templates (not used for SQL)
            output_dir: Directory to write generated SQL files
            verbose: Enable verbose output
        """
        super().__init__(template_dir, output_dir, verbose)

    def transpile(self, sql_string: str) -> str:
        """No transpilation needed for PostgreSQL - return original SQL.

        Since all base SQL functions are written in PostgreSQL syntax,
        no transpilation is needed for PostgreSQL targets.

        Args:
            sql_string: A SQL CREATE FUNCTION statement in PostgreSQL syntax.

        Returns:
            str: The original SQL statement (no changes needed).
        """
        return sql_string

    def register_functions(self, sql_content: str, config: Optional[Dict[str, Any]] = None) -> Tuple[bool, str]:
        """Register SQL functions directly in PostgreSQL database.

        Args:
            sql_content: The SQL content containing CREATE FUNCTION statements
            config: Optional database configuration. If None, will load from config file.

        Returns:
            Tuple of (success: bool, message: str)
        """
        try:
            import psycopg2
            from datacompose.cli.config import ConfigLoader

            # Get database connection parameters
            if config is None:
                conn_params = ConfigLoader.get_postgres_config()
            else:
                conn_params = config

            if self.verbose:
                print(f"Connecting to PostgreSQL at {conn_params['host']}:{conn_params['port']}")

            # Connect to database
            conn = psycopg2.connect(
                host=conn_params["host"],
                port=conn_params["port"],
                database=conn_params["database"],
                user=conn_params["user"],
                password=conn_params["password"],
            )

            # Execute SQL functions
            cursor = conn.cursor()

            # Split SQL content into individual statements
            sql_statements = self._extract_sql_statements(sql_content)

            successful_functions = []
            failed_functions = []

            for statement in sql_statements:
                if statement.strip():
                    try:
                        cursor.execute(statement)
                        # Extract function name for reporting
                        func_name = self._extract_function_name(statement)
                        successful_functions.append(func_name)
                        if self.verbose:
                            print(f"✓ Registered function: {func_name}")
                    except psycopg2.Error as e:
                        func_name = self._extract_function_name(statement)
                        failed_functions.append(f"{func_name}: {str(e)}")
                        if self.verbose:
                            print(f"✗ Failed to register function {func_name}: {e}")

            # Commit changes
            conn.commit()
            cursor.close()
            conn.close()

            if failed_functions:
                message = f"Registered {len(successful_functions)} functions, {len(failed_functions)} failed"
                if self.verbose:
                    message += f"\nFailed: {', '.join(failed_functions)}"
                return False, message
            else:
                message = f"Successfully registered {len(successful_functions)} functions"
                if self.verbose:
                    message += f": {', '.join(successful_functions)}"
                return True, message

        except ImportError:
            return False, "psycopg2 not installed. Install with: pip install psycopg2-binary"

        except Exception as e:
            if "psycopg2" in str(type(e)):
                return False, f"Database connection failed: {str(e)}"
            else:
                return False, f"Registration failed: {str(e)}"

    def _extract_sql_statements(self, sql_content: str) -> list[str]:
        """Extract individual CREATE FUNCTION statements from SQL content.

        Args:
            sql_content: Full SQL file content

        Returns:
            List of individual SQL statements
        """
        statements = []
        current_statement = []
        in_function = False
        dollar_quote_level = 0

        for line in sql_content.split('\n'):
            stripped = line.strip()

            # Skip comments and empty lines
            if not stripped or stripped.startswith('--'):
                continue

            # Check for start of CREATE FUNCTION
            if stripped.upper().startswith('CREATE OR REPLACE FUNCTION') or stripped.upper().startswith('CREATE FUNCTION'):
                in_function = True
                current_statement = [line]
                continue

            if in_function:
                current_statement.append(line)

                # Count dollar quotes to track function body boundaries
                dollar_quote_level += line.count('$$') % 2

                # End of function when we see semicolon at the end and not inside dollar quotes
                if stripped.endswith(';') and dollar_quote_level == 0:
                    statements.append('\n'.join(current_statement))
                    current_statement = []
                    in_function = False

        return statements

    def _extract_function_name(self, sql_statement: str) -> str:
        """Extract function name from CREATE FUNCTION statement.

        Args:
            sql_statement: SQL CREATE FUNCTION statement

        Returns:
            Function name
        """
        try:
            # Simple regex to extract function name
            import re
            match = re.search(r'CREATE (?:OR REPLACE )?FUNCTION\s+(\w+)', sql_statement, re.IGNORECASE)
            if match:
                return match.group(1)
            return "unknown_function"
        except Exception:
            return "unknown_function"