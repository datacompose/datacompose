"""
Spark pandas UDF generator.
"""

from pathlib import Path

from ..base import BaseGenerator


class PySparkGenerator(BaseGenerator):
    """Generator for Apache Spark pandas UDFs."""

    ENGINE_SUBDIRECTORY = "pyspark"
    PRIMITIVES_FILENAME = "pyspark_primitives.py"

    def _get_primitives_file(self, transformer_dir: Path | None = None) -> str:
        """Get the template content for Spark pandas UDFs."""
        if transformer_dir:
            # Look for transformer-specific template first
            transformer_template = transformer_dir / self.ENGINE_SUBDIRECTORY / self.PRIMITIVES_FILENAME
            if transformer_template.exists():
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
    
    def post_generate_hook(self, output_path: Path):
        """Perform PySpark-specific post-generation tasks."""
        self.ensure_init_files(output_path)
        self.copy_utils_files(output_path)

    def ensure_init_files(self, output_path: Path):
        """Ensure __init__.py files exist to make directories importable."""
        # Get all directories from transformers down to the target directory
        path_parts = output_path.parts

        # Find the transformers directory index
        try:
            transformers_index = path_parts.index("transformers")
        except ValueError:
            # No transformers directory found, just create init for immediate parent
            init_file = output_path.parent / "__init__.py"
            if not init_file.exists():
                init_file.touch()
                if self.verbose:
                    print(f"Created {init_file}")
            return

        # Create __init__.py files for transformers and all subdirectories leading to output
        for i in range(
            transformers_index, len(path_parts) - 1
        ):  # -1 to exclude the file itself
            dir_path = Path(*path_parts[: i + 1])
            init_file = dir_path / "__init__.py"
            if not init_file.exists():
                init_file.touch()
                if self.verbose:
                    print(f"Created {init_file}")

    def copy_utils_files(self, output_path: Path):
        """Copy utility files like primitives.py to the transformers directory."""

        # Create utils directory in the same directory as the generated files
        # This puts it at transformers/pyspark/utils
        utils_dir = output_path.parent / "utils"
        utils_dir.mkdir(parents=True, exist_ok=True)

        # Create __init__.py in utils directory
        init_file = utils_dir / "__init__.py"
        if not init_file.exists():
            init_file.touch()
            if self.verbose:
                print(f"Created {init_file}")

        # Copy primitives.py from datacompose.operators
        primitives_source = Path(__file__).parent.parent.parent / "operators" / "primitives.py"
        primitives_dest = utils_dir / "primitives.py"

        if primitives_source.exists() and not primitives_dest.exists():
            import shutil

            shutil.copy2(primitives_source, primitives_dest)
            if self.verbose:
                print(f"Copied primitives.py to {primitives_dest}")
