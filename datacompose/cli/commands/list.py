"""
List command for showing available targets and transformers.
"""

import click

from datacompose.transformers.discovery import TransformerDiscovery


# Completion function for list items
def complete_list_items(ctx, param, incomplete):
    """Complete list item choices."""
    items = ["targets", "transformers", "generators"]
    return [
        click.shell_completion.CompletionItem(item)  # type ignore
        for item in items
        if item.startswith(incomplete)
    ]


@click.command(name="list")
@click.argument(
    "item",
    type=click.Choice(["targets", "transformers", "generators"]),
    shell_complete=complete_list_items,
)
@click.pass_context
def list_cmd(ctx, item):
    """List available targets, transformers, or generators.

    ITEM: What to list: targets, transformers, or generators
    """
    exit_code = _run_list(item)
    if exit_code != 0:
        ctx.exit(exit_code)


def _run_list(item) -> int:
    """Execute the list command."""
    discovery = TransformerDiscovery()

    if item == "transformers":
        return ListCommand._list_transformers(discovery)
    elif item == "generators":
        return ListCommand._list_generators(discovery)
    elif item == "targets":
        return ListCommand._list_generators(discovery)
    else:
        print(f"Unknown item: {item}")
        return 1


class ListCommand:
    """Command to list available targets and transformers."""

    @staticmethod
    def _list_targets() -> int:
        """List available target platforms."""
        from cli.commands.add import AddCommand

        print(" Available targets:")
        for target in AddCommand.AVAILABLE_TARGETS.keys():
            print(f"  • {target}")

        print("\n💡 Use 'datacompose add <transformer> --target <target>' to generate UDFs")
        return 0

    @staticmethod
    def _list_transformers(discovery: TransformerDiscovery) -> int:
        """List available transformers by domain."""
        transformers = discovery.discover_transformers()

        if not transformers:
            print(" No transformers found.")
            return 0

        print(" Available transformers:")

        # Group transformers by domain (extracted from path)
        domains = {}
        for transformer_name, transformer_path in transformers.items():
            # Extract domain from path
            domain = (
                transformer_path.parent.parent.name
                if transformer_path.parent.parent.name != "transformers"
                else "legacy"
            )
            if domain not in domains:
                domains[domain] = {}
            domains[domain][transformer_name] = transformer_path

        for domain, domain_transformers in sorted(domains.items()):
            print(f"\n  {domain}/")
            for transformer_name, transformer_path in sorted(domain_transformers.items()):
                print(f"    • {transformer_name}")

        print("\nUsage: datacompose add <transformer> --target <platform> [--type <type>]")
        print("Example: datacompose add clean_emails --target pyspark")
        return 0

    @staticmethod
    def _list_generators(discovery: TransformerDiscovery) -> int:
        """List available generators by platform."""
        generators = discovery.discover_generators()

        if not generators:
            print(" No generators found.")
            return 0

        print(" Available generators:")
        for platform, platform_generators in sorted(generators.items()):
            print(f"\n  {platform}/")
            for gen_type, gen_class in sorted(platform_generators.items()):
                print(f"    • {gen_type} ({gen_class.__name__})")

        print("\nUsage: datacompose add <transformer> --target <platform> [--type <type>]")
        print("Example: datacompose add clean_emails --target pyspark")
        return 0
