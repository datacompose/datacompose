"""
Add command for generating UDFs.
"""

import json
from pathlib import Path

import click

from datacompose.cli.colors import dim, error, highlight, info, success
from datacompose.cli.validation import validate_platform, validate_type_for_platform
from datacompose.transformers.discovery import TransformerDiscovery


# Completion functions for Click shell completion
def complete_transformer(ctx, param, incomplete):
    """Complete transformer names from discovery system."""
    try:
        discovery = TransformerDiscovery()
        transformers = discovery.list_transformers()
        return [
            click.shell_completion.CompletionItem(t)  # type: ignore
            for t in transformers
            if t.startswith(incomplete)
        ]
    except Exception:
        return []


def complete_target(ctx, param, incomplete):
    """Complete target platforms from discovery system."""
    try:
        discovery = TransformerDiscovery()
        generators = discovery.list_generators()
        # Extract platform names (part before the dot)
        platforms = list(set(gen.split(".")[0] for gen in generators))
        return [
            click.shell_completion.CompletionItem(p)  # type: ignore
            for p in platforms
            if p.startswith(incomplete)
        ]
    except Exception:
        return []


def complete_type(ctx, param, incomplete):
    """Complete generator types based on selected target."""
    try:
        discovery = TransformerDiscovery()
        generators = discovery.list_generators()

        # Try to get the target from context
        target = None
        if ctx.params.get("target"):
            target = ctx.params["target"]

        if target:
            # Filter to types for this specific target
            target_generators = [
                gen for gen in generators if gen.startswith(f"{target}.")
            ]
            types = [gen.split(".", 1)[1] for gen in target_generators if "." in gen]
            return [
                click.shell_completion.CompletionItem(t)  # type: ignore
                for t in types
                if t.startswith(incomplete)
            ]
        else:
            # Return all available types
            types = [gen.split(".", 1)[1] for gen in generators if "." in gen]
            return [
                click.shell_completion.CompletionItem(t)  # type: ignore
                for t in types
                if t.startswith(incomplete)
            ]
    except Exception:
        return []


# Get the directory where this module is located
_MODULE_DIR = Path(__file__).parent


@click.command()
@click.argument("transformer", shell_complete=complete_transformer)
@click.option(
    "--target",
    "-t",
    default="pyspark",
    shell_complete=complete_target,
    help="Target platform (e.g., 'pyspark', 'postgres', 'snowflake'). Default: pyspark",
)
@click.option(
    "--type",
    shell_complete=complete_type,
    help="UDF type for the platform (e.g., 'pandas_udf', 'sql_udf'). Uses platform default if not specified",
)
@click.option("--output", "-o", help="Output directory (default: build/{target})")
@click.option(
    "--template-dir",
    default="src/transformers/templates",
    help="Directory containing templates (default: src/transformers/templates)",
)
@click.option("--verbose", "-v", is_flag=True, help="Verbose output")
@click.pass_context
def add(ctx, transformer, target, type, output, template_dir, verbose):
    """Add UDFs for transformers.

    TRANSFORMER: Transformer to add UDF for (e.g., 'clean_emails')
    """
    # Initialize discovery for validation
    discovery = TransformerDiscovery()

    # Validate platform first
    if not validate_platform(target, discovery):
        ctx.exit(1)

    # Validate type if specified
    if type and not validate_type_for_platform(target, type, discovery):
        ctx.exit(1)

    # Combine target and type into generator reference
    exit_code = _run_add(transformer, target, output, template_dir, verbose)
    if exit_code != 0:
        ctx.exit(exit_code)


def _run_add(transformer, target, output, template_dir, verbose) -> int:
    """Execute the add command."""
    # Initialize discovery
    discovery = TransformerDiscovery()

    # Resolve transformer
    transformer_name, transformer_path = discovery.resolve_transformer(transformer)

    if not transformer_path:
        print(error(f"Error: Transformer not found: {transformer}"))
        print(
            info(
                f"Available transformers: {', '.join(discovery.list_transformers())}"
            )
        )
        return 1
    else:
        print(info(f"Using transformer: {transformer_name}"))
        if verbose:
            print(dim(f"Transformer path: {transformer_path}"))
        # For discovered transformers, set transformer_dir
        transformer_dir = transformer_path

    # Resolve generator
    generator_class = discovery.resolve_generator(target)
    if not generator_class:
        print(error(f"Error: Generator not found: {target}"))
        print(info(f"Available generators: {', '.join(discovery.list_generators())}"))
        return 1

    # Determine output directory
    # Extract platform from target (e.g., "pyspark.pandas_udf" -> "pyspark")
    platform = target.split(".")[0]

    if not output:
        output_dir = f"build/{platform}/{transformer_name}"
    else:
        output_dir = f"{output}/{platform}/{transformer_name}"

    # Create generator instance
    generator = generator_class(
        template_dir=Path(template_dir), output_dir=Path(output_dir), verbose=verbose
    )

    try:
        # Generate the UDF
        result = generator.generate(
            transformer_name, force=False, transformer_dir=transformer_dir
        )

        if result.get("skipped"):
            print(info(f"UDF already exists: {result['output_path']}"))
            print(dim("No changes needed (hash matches)"))
            if verbose:
                print(dim(f"   Hash: {result.get('hash', 'N/A')}"))
        else:
            print(success(f"✓ UDF generated: {result['output_path']}"))
            print(success(f"✓ Test created: {result['test_path']}"))
            print(highlight(f"Function name: {result['function_name']}"))
            if verbose:
                print(dim(f"   Target: {target}"))
                print(highlight("\nGenerated package contents:"))
                print(f"  - UDF code: {result['output_path']}")
                print(f"  - Test file: {result['test_path']}")

        return 0

    except Exception as e:
        print(error(f"Add failed: {e}"))
        if verbose:
            import traceback

            traceback.print_exc()
        return 1


def _load_config() -> dict:
    """Load datacompose.json configuration if it exists."""
    config_path = Path("datacompose.json")
    if config_path.exists():
        try:
            with open(config_path, "r") as f:
                return json.load(f)
        except Exception:
            pass
    return {}


