"""
Base generator class for UDF generation.

The classes that inherit from this generator must implement the following methods:
def _get_template_content(self, transformer_dir: Path | None = None) -> str:
def __get_output_filename as well as any other build steps that you want.
"""

import hashlib
from abc import ABC, abstractmethod
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
        transformer = {"name": transformer_name}

        file_content: str = self._get_primitives_file(transformer_dir)
        spec_hash = self._calculate_hash(transformer, file_content)
        output_file = self._get_output_filename(transformer["name"])
        output_path = self.output_dir / output_file

        # Check if regeneration is needed
        if not force and self._should_skip_generation(output_path, spec_hash):
            return {
                "skipped": True,
                "output_path": str(output_path),
                "hash": spec_hash,
                "function_name": f"{transformer['name']}_udf",
            }

        self._write_output(output_path, file_content)
        
        # Allow subclasses to perform post-generation tasks
        self.post_generate_hook(output_path)

        return {
            "skipped": False,
            "output_path": str(output_path),
            "hash": spec_hash,
            "function_name": f"{transformer['name']}_udf",
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

        with open(output_path, "w") as f:
            f.write(content)

        if self.verbose:
            print(f"Wrote output to: {output_path}")
    
    def post_generate_hook(self, output_path: Path):
        """Hook for subclasses to perform post-generation tasks.
        
        Override this method to perform generator-specific tasks after
        the main file has been written.
        """
        pass

    @abstractmethod
    def _get_primitives_file(self, transformer_dir: Path | None) -> str:
        """Get the file content for this generator."""
        pass

    @abstractmethod
    def _get_output_filename(self, transformer_name: str) -> str:
        """Get the output filename for generated UDF."""
        pass
