"""
Spark pandas UDF generator.
"""

from pathlib import Path

from ..base import BaseGenerator


class SparkPandasUDFGenerator(BaseGenerator):
    """Generator for Apache Spark pandas UDFs."""

    ENGINE_SUBDIRECTORY = "pyspark"
    PRIMITIVES_FILENAME = "pyspark_primitives.py"

    @classmethod
    def _get_primitives_location(cls, transformer_dir: Path | None) -> Path | None:
        if transformer_dir is None:
            return None
        return transformer_dir / cls.ENGINE_SUBDIRECTORY / cls.PRIMITIVES_FILENAME

    def _get_primitives_file(self, transformer_dir: Path | None = None) -> str:
        """Get the template content for Spark pandas UDFs."""
        if transformer_dir:
            # Look for transformer-specific template first
            transformer_template = self._get_primitives_location(transformer_dir)
            if transformer_template and transformer_template.exists():
                return transformer_template.read_text()

        # Fallback to generator-specific template (if it exists)
        generator_template = Path(__file__).parent / self.PRIMITIVES_FILENAME
        if generator_template.exists():
            return generator_template.read_text()

        # If no templates found, raise error
        raise FileNotFoundError(
            f"No {self.PRIMITIVES_FILENAME} template found in {transformer_dir} or {Path(__file__).parent}"
        )

    def _get_output_filename(self, transformer_name: str) -> str:
        """Get the output filename for PySpark primitives."""
        # Use the transformer name directly as the filename
        # emails -> emails.py
        # addresses -> addresses.py
        # phone_numbers -> phone_numbers.py
        return f"{transformer_name}.py"
