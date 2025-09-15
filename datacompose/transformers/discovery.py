"""
Transformer and generator discovery system.
"""

import importlib
from pathlib import Path
from typing import Dict, List, Optional, Tuple


class TransformerDiscovery:
    """Discovers available transformers and generators."""

    # Platform aliases for user-friendly target names
    PLATFORM_ALIASES = {
        "postgres": "sql.postgres_generator",
        "postgresql": "sql.postgres_generator",
        # Future SQL database aliases can be added here
        # "mysql": "sql.mysql_generator",
        # "sqlite": "sql.sqlite_generator",
    }

    def __init__(
        self, transformers_dir: Path | None = None, generators_dir: Path | None = None
    ):
        """Initialize discovery with custom directories."""
        self.transformers_dir = transformers_dir or Path(__file__).parent
        self.generators_dir = (
            generators_dir or Path(__file__).parent.parent / "generators"
        )

    def discover_transformers(self) -> Dict[str, Path]:
        """
        Discover available transformers.

        Returns:
            Dict[transformer_name, transformer_path]
        """
        transformers = {}

        # Look for domain directories, then transformer directories inside them
        for domain_dir in self.transformers_dir.iterdir():
            if (
                domain_dir.is_dir()
                and not domain_dir.name.startswith((".", "__"))
                and domain_dir.name not in ("discovery.py")
            ):
                # Look for transformer directories within domain
                for transformer_dir in domain_dir.iterdir():
                    if transformer_dir.is_dir() and not transformer_dir.name.startswith(
                        (".", "__")
                    ):
                        transformer_name = transformer_dir.name
                        # Just store the transformer directory path
                        transformers[transformer_name] = transformer_dir

        return transformers

    def discover_generators(self) -> Dict[str, Dict[str, type]]:
        """
        Discover available generators by platform.

        Returns:
            Dict[platform, Dict[generator_type, generator_class]]
        """
        generators = {}

        # Look for platform directories
        for platform_dir in self.generators_dir.iterdir():
            if (
                platform_dir.is_dir()
                and not platform_dir.name.startswith((".", "__"))
                and platform_dir.name != "base.py"
            ):
                platform_name = platform_dir.name
                generators[platform_name] = {}

                # Look for generator files within platform
                for generator_file in platform_dir.glob("*.py"):
                    if not generator_file.name.startswith((".", "__")):
                        generator_name = generator_file.stem

                        try:
                            # Import the generator module
                            module_path = f"datacompose.generators.{platform_name}.{generator_name}"
                            module = importlib.import_module(module_path)

                            # Find generator classes or factory functions
                            # First, look for classes defined in this specific module (not imported)
                            module_generators = []
                            for attr_name in dir(module):
                                attr = getattr(module, attr_name)
                                # Check for generator classes defined in this module
                                if (
                                    isinstance(attr, type)
                                    and hasattr(attr, "generate")
                                    and attr.__name__.endswith("Generator")
                                    and attr.__module__ == module.__name__  # Must be defined in this module
                                ):
                                    module_generators.append((attr_name, attr))
                                # Check for factory functions that create generators
                                elif (
                                    callable(attr)
                                    and attr_name.endswith("Generator")
                                    and not attr_name.startswith("_")
                                ):
                                    module_generators.append((attr_name, attr))

                            # Use the first generator found (prioritizing those defined in this module)
                            if module_generators:
                                # Sort to prefer more specific names (e.g., PostgreSQLGenerator over SQLGenerator)
                                module_generators.sort(key=lambda x: len(x[0]), reverse=True)
                                generators[platform_name][generator_name] = module_generators[0][1]
                        except Exception:
                            # Skip modules that can't be imported
                            continue

        return generators

    def get_transformer_info(self, transformer: str) -> Optional[Dict]:
        """Get info for a specific transformer."""
        transformers = self.discover_transformers()

        if transformer in transformers:
            # Return basic info about the transformer
            return {"name": transformer, "path": str(transformers[transformer])}

        return None

    def resolve_transformer(
        self, transformer_ref: str
    ) -> Tuple[Optional[str], Optional[Path]]:
        """
        Resolve transformer reference to name and transformer path.

        Args:
            transformer_ref: transformer name

        Returns:
            Tuple of (transformer_name, transformer_path) or (None, None)
        """
        transformers = self.discover_transformers()

        if transformer_ref in transformers:
            return transformer_ref, transformers[transformer_ref]

        return None, None

    def resolve_generator(self, generator_ref: str) -> Optional[type]:
        """
        Resolve generator reference to generator class.

        Args:
            generator_ref: Either "platform.type", just "platform", or a platform alias

        Returns:
            Generator class or None
        """
        # Check if generator_ref is a platform alias
        if generator_ref in self.PLATFORM_ALIASES:
            generator_ref = self.PLATFORM_ALIASES[generator_ref]

        if "." in generator_ref:
            # New format: platform.type
            platform, gen_type = generator_ref.split(".", 1)
        else:
            # Legacy format: just platform, use default type or first available
            platform = generator_ref
            gen_type = None

        generators = self.discover_generators()

        if platform in generators:
            if gen_type and gen_type in generators[platform]:
                return generators[platform][gen_type]
            elif not gen_type:
                # No specific type requested, try to find a default or use first available
                if "pandas_udf" in generators[platform]:
                    return generators[platform]["pandas_udf"]
                elif "generator" in generators[platform]:
                    return generators[platform]["generator"]
                elif generators[platform]:
                    # Use the first available generator for this platform
                    return next(iter(generators[platform].values()))

        return None

    def list_transformers(self) -> List[str]:
        """List all available transformers."""
        transformers = self.discover_transformers()
        return sorted(transformers.keys())

    def list_generators(self) -> List[str]:
        """List all available generators in platform.type format."""
        generators = self.discover_generators()
        result = []

        for platform, platform_generators in generators.items():
            for gen_type in platform_generators.keys():
                result.append(f"{platform}.{gen_type}")

        return sorted(result)
