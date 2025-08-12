"""
Base generator class for UDF generation.

The classes that inherit from this generator must implement the following methods:
def _get_template_content(self, transformer_dir: Path | None = None) -> str:
def __get_output_filename as well as any other build steps that you want.
"""

import hashlib
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional


class BaseGenerator(ABC):
    """Base class for UDF generators."""

    def __init__(self, template_dir: Path, output_dir: Path, verbose: bool = False):
        """Initialize the generator.

        Args:
            template_dir: Directory containing templates
            output_dir: Directory to write generated UDFs
            verbose: Enable verbose output
        """
        self.template_dir = template_dir
        self.output_dir = output_dir
        self.verbose = verbose

    def generate(
        self,
        transformer_name: str,
        force: bool = False,
        transformer_dir: Optional[Path] = None,
    ) -> Dict[str, Any]:
        """Generate UDF for transformer.

        Args:
            transformer_name: Name of the transformer
            force: Force regeneration even if hash matches
            transformer_dir: Directory containing the transformer (for template lookup)

        Returns:
            Dictionary with generation results
        """
        # Create a minimal spec-like dict from transformer name for compatibility
        spec = {"name": transformer_name}

        # Get template content
        template_content = self._get_template_content(transformer_dir)

        # Calculate hash for caching
        spec_hash = self._calculate_hash(spec, template_content)

        # Determine output path
        output_file = self._get_output_filename(spec["name"])
        output_path = self.output_dir / output_file

        # Check if regeneration is needed
        if not force and self._should_skip_generation(output_path, spec_hash):
            return {
                "skipped": True,
                "output_path": str(output_path),
                "hash": spec_hash,
                "function_name": f"{spec['name']}_udf",
            }
        
        # Copy utils/primitives.py to the output directory
        self._copy_utils_files(output_path)


        return {
            "skipped": False,
            "output_path": str(output_path),
            "hash": spec_hash,
            "function_name": f"{spec['name']}_udf",
        }

    @staticmethod
    def _calculate_hash(spec: Dict[str, Any], template_content: str) -> str:
        """Calculate hash for cache invalidation."""
        content = str(spec) + template_content
        return hashlib.sha256(content.encode("utf-8")).hexdigest()[:8]

    @staticmethod
    def _should_skip_generation(output_path: Path, spec_hash: str) -> bool:
        """Check if generation should be skipped based on hash."""
        if not output_path.exists():
            return False

        try:
            with open(output_path, "r") as f:
                first_lines = "".join(f.readlines()[:5])
                return f"Hash: {spec_hash}" in first_lines
        except Exception:
            return False

    def _write_output(self, output_path: Path, content: str):
        """Write generated content to output file."""
        # Create output directory if it doesn't exist
        output_path.parent.mkdir(parents=True, exist_ok=True)

        # Create __init__.py files to make directories importable as Python packages
        self._ensure_init_files(output_path)

        with open(output_path, "w") as f:
            f.write(content)

        if self.verbose:
            print(f"Wrote output to: {output_path}")

    def _ensure_init_files(self, output_path: Path):
        """Ensure __init__.py files exist to make directories importable."""
        # Get all directories from build down to the target directory
        path_parts = output_path.parts

        # Find the build directory index
        try:
            build_index = path_parts.index("build")
        except ValueError:
            # No build directory found, just create init for immediate parent
            init_file = output_path.parent / "__init__.py"
            if not init_file.exists():
                init_file.touch()
                if self.verbose:
                    print(f"Created {init_file}")
            return

        # Create __init__.py files for build and all subdirectories leading to output
        for i in range(
            build_index, len(path_parts) - 1
        ):  # -1 to exclude the file itself
            dir_path = Path(*path_parts[: i + 1])
            init_file = dir_path / "__init__.py"
            if not init_file.exists():
                init_file.touch()
                if self.verbose:
                    print(f"Created {init_file}")

    @staticmethod
    def _prepare_template_vars(spec: Dict[str, Any], spec_hash: str) -> Dict[str, Any]:
        """Prepare variables for template rendering."""
        return {
            "transformer_name": spec["name"],
            "udf_name": f"{spec['name']}_udf",
            "hash": spec_hash,
            "generation_timestamp": datetime.now().isoformat(),
            "typo_map": spec.get("typo_map", {}),
            "regex_patterns": spec.get("regex", {}),
            "flags": spec.get("flags", {}),
            "options": spec.get("options", {}),
            "custom_rules": spec.get("custom_rules", {}),
        }


    def _copy_utils_files(self, output_path: Path):
        """Copy utility files like primitives.py to the output directory."""
        # Create utils directory at the same level as the output file
        utils_dir = output_path.parent / "utils"
        utils_dir.mkdir(parents=True, exist_ok=True)
        
        # Create __init__.py in utils directory
        init_file = utils_dir / "__init__.py"
        if not init_file.exists():
            init_file.touch()
            if self.verbose:
                print(f"Created {init_file}")
        
        # Copy primitives.py from datacompose.operators
        primitives_source = Path(__file__).parent.parent / "operators" / "primitives.py"
        primitives_dest = utils_dir / "primitives.py"
        
        if primitives_source.exists() and not primitives_dest.exists():
            import shutil
            shutil.copy2(primitives_source, primitives_dest)
            if self.verbose:
                print(f"Copied primitives.py to {primitives_dest}")

    @classmethod
    @abstractmethod
    def _get_template_location(cls, transformer_dir: Path | None) -> Path | None:
        pass

    @abstractmethod
    def _get_template_content(self, transformer_dir: Path | None) -> str:
        """Get the template content for this generator."""
        pass

    @abstractmethod
    def _get_output_filename(self, transformer_name: str) -> str:
        """Get the output filename for generated UDF."""
        pass