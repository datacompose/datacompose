"""
Validation utilities for CLI commands.
"""

from datacompose.transformers.discovery import TransformerDiscovery
from datacompose.cli.colors import error, info


def validate_platform(platform: str, discovery: TransformerDiscovery) -> bool:
    """Validate that platform exists.

    Args:
        platform: Platform name (e.g., 'pyspark', 'postgres', 'snowflake')
        discovery: TransformerDiscovery instance

    Returns:
        True if platform is valid, False otherwise
    """
    available_generators = discovery.list_generators()
    available_platforms = list(set(g.split(".")[0] for g in available_generators))

    if platform not in available_platforms:
        print(error(f"Platform '{platform}' not found."))
        print(info(f"Available platforms: {', '.join(sorted(available_platforms))}"))
        return False
    return True


def validate_type_for_platform(
    platform: str, type_name: str, discovery: TransformerDiscovery
) -> bool:
    """Validate that type exists for the given platform.

    Args:
        platform: Platform name (e.g., 'pyspark', 'postgres')
        type_name: Type name (e.g., 'pandas_udf', 'sql_udf')
        discovery: TransformerDiscovery instance

    Returns:
        True if type is valid for platform, False otherwise
    """
    available_generators = discovery.list_generators()
    platform_generators = [
        g for g in available_generators if g.startswith(f"{platform}.")
    ]
    available_types = [g.split(".")[1] for g in platform_generators]

    if type_name not in available_types:
        print(error(f"Type '{type_name}' not available for platform '{platform}'."))
        if available_types:
            print(info(f"Available types for {platform}: {', '.join(available_types)}"))
        else:
            print(info(f"No generators available for platform '{platform}'."))
        return False
    return True


def get_available_platforms(discovery: TransformerDiscovery) -> list[str]:
    """Get list of available platforms."""
    available_generators = discovery.list_generators()
    return sorted(set(g.split(".")[0] for g in available_generators))


def get_available_types_for_platform(
    platform: str, discovery: TransformerDiscovery
) -> list[str]:
    """Get list of available types for a specific platform."""
    available_generators = discovery.list_generators()
    platform_generators = [
        g for g in available_generators if g.startswith(f"{platform}.")
    ]
    return [g.split(".")[1] for g in platform_generators]
