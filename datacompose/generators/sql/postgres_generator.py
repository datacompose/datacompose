"""
PostgreSQL SQL function generator.
"""

from pathlib import Path

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