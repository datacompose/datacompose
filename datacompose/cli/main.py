#!/usr/bin/env python3
# PYTHON_ARGCOMPLETE_OK
"""
Main CLI entry point for Datacompose.
"""

import click
import sys

# Import argcomplete for tab completion
try:
    import argcomplete
except ImportError:
    argcomplete = None

from datacompose.cli.commands.add import add
from datacompose.cli.commands.init import init
from datacompose.cli.commands.list import list_cmd


@click.group()
@click.version_option("0.1.0", prog_name="datacompose")
@click.pass_context
def cli(ctx):
    """Generate data cleaning UDFs for various platforms.

    Examples:
      datacompose init
      datacompose add clean_emails --target pyspark
      datacompose add clean_emails --target snowflake --output sql/udfs/
      datacompose list targets
    """
    pass


# Add commands to the main CLI group
cli.add_command(init)
cli.add_command(add)
cli.add_command(list_cmd)


def main():
    """Main CLI entry point."""
    # Enable argcomplete for tab completion
    if argcomplete:
        argcomplete.autocomplete(cli)

    try:
        cli()
    except KeyboardInterrupt:
        click.echo("\nOperation cancelled by user", err=True)
        sys.exit(1)
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
