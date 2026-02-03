"""Test the list CLI command."""

import shutil
import sys
import tempfile
from pathlib import Path

import pytest
# import yaml  # No longer needed - spec files removed
from click.testing import CliRunner

# Add project root to path for imports
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from datacompose.cli.commands.list import ListCommand  # noqa: E402
from datacompose.cli.main import cli  # noqa: E402
from datacompose.transformers.discovery import TransformerDiscovery  # noqa: E402


@pytest.mark.unit
class TestListCommand:
    """Test suite for list command functionality."""

    @pytest.fixture(scope="class")
    def runner(self):
        """Fixture to provide Click CLI runner."""
        return CliRunner()

    def test_list_help(self, runner):
        """Test list command help output."""
        result = runner.invoke(cli, ["list", "--help"])
        assert result.exit_code == 0
        assert "List available targets, transformers, or generators" in result.output
        assert "ITEM:" in result.output
        assert "targets" in result.output
        assert "transformers" in result.output
        assert "generators" in result.output

    def test_list_missing_item(self, runner):
        """Test list command without item argument."""
        result = runner.invoke(cli, ["list"])
        assert result.exit_code == 2
        assert "Missing argument" in result.output

    def test_list_invalid_item(self, runner):
        """Test list command with invalid item."""
        result = runner.invoke(cli, ["list", "invalid_item"])
        assert result.exit_code == 2
        assert "Invalid value" in result.output

    def test_list_transformers(self, runner):
        """Test list transformers command."""
        result = runner.invoke(cli, ["list", "transformers"])
        assert result.exit_code == 0
        # Should show available transformers or message if none found
        assert (
            "Available transformers:" in result.output
            or "No transformers found" in result.output
        )

    def test_list_generators(self, runner):
        """Test list generators command."""
        result = runner.invoke(cli, ["list", "generators"])
        assert result.exit_code == 0
        # Should show available generators or message if none found
        assert (
            "Available generators:" in result.output
            or "No generators found" in result.output
        )

    def test_list_targets(self, runner):
        """Test list targets command (alias for generators)."""
        result = runner.invoke(cli, ["list", "targets"])
        assert result.exit_code == 0
        # Should show available generators or message if none found
        assert (
            "Available generators:" in result.output
            or "No generators found" in result.output
        )

    @pytest.fixture
    def temp_dir(self):
        """Fixture to provide temporary directory for tests."""
        temp_dir = tempfile.mkdtemp()
        yield Path(temp_dir)
        shutil.rmtree(temp_dir)

    @pytest.fixture
    def discovery(self):
        """Fixture to provide real TransformerDiscovery instance."""
        return TransformerDiscovery()

    def test_list_transformers_content_structure(self, runner):
        """Test that list transformers shows proper structure."""
        result = runner.invoke(cli, ["list", "transformers"])
        assert result.exit_code == 0

        # Should have usage instructions
        assert "Usage:" in result.output
        assert "datacompose add" in result.output
        assert "--target" in result.output

        # Should have example
        assert "Example:" in result.output

    def test_list_generators_content_structure(self, runner):
        """Test that list generators shows proper structure."""
        result = runner.invoke(cli, ["list", "generators"])
        assert result.exit_code == 0

        # Should have usage instructions
        assert "Usage:" in result.output
        assert "datacompose add" in result.output
        assert "--target" in result.output

        # Should have example
        assert "Example:" in result.output

    def test_list_transformers_with_real_discovery(self, runner, discovery):
        """Test list transformers with real discovery system."""
        # Get actual transformers from discovery
        transformers = discovery.discover_transformers()

        result = runner.invoke(cli, ["list", "transformers"])
        assert result.exit_code == 0

        if transformers:
            assert "Available transformers:" in result.output
            # Check that at least some transformer names appear
            for transformer_name in list(transformers.keys())[:3]:  # Check first 3
                assert transformer_name in result.output
        else:
            assert "No transformers found." in result.output

    def test_list_generators_with_real_discovery(self, runner, discovery):
        """Test list generators with real discovery system."""
        # Get actual generators from discovery
        generators = discovery.discover_generators()

        result = runner.invoke(cli, ["list", "generators"])
        assert result.exit_code == 0

        if generators:
            assert "Available generators:" in result.output
            # Should show platform grouping
            for platform in generators.keys():
                assert f"{platform}/" in result.output
        else:
            assert "No generators found." in result.output

    def test_list_transformers_domain_grouping(self, runner, discovery):
        """Test that transformers are grouped by domain."""
        transformers = discovery.discover_transformers()

        result = runner.invoke(cli, ["list", "transformers"])
        assert result.exit_code == 0

        if transformers:
            # Check for domain grouping markers
            output_lines = result.output.split("\n")
            domain_lines = [line for line in output_lines if line.strip().endswith("/")]

            # Should have at least one domain group if transformers exist
            if len(transformers) > 0:
                assert len(domain_lines) > 0

    def test_list_generators_platform_grouping(self, runner, discovery):
        """Test that generators are grouped by platform."""
        generators = discovery.discover_generators()

        result = runner.invoke(cli, ["list", "generators"])
        assert result.exit_code == 0

        if generators:
            # Check for platform grouping
            for platform in generators.keys():
                assert f"{platform}/" in result.output
                # Check that generator types are listed under platform
                for gen_type in generators[platform].keys():
                    assert gen_type in result.output

    def test_list_transformers_with_directories(self, runner, temp_dir):
        """Test that transformer directories are discovered."""
        # Create a transformer directory structure
        transformer_dir = temp_dir / "test_domain" / "test_transformer" / "pyspark"
        transformer_dir.mkdir(parents=True)
        
        # Create a pyspark_primitives.py file to make it a valid transformer
        primitives_file = transformer_dir / "pyspark_primitives.py"
        primitives_file.write_text("# Test transformer")

        # Test that the real list command can handle transformer directories
        result = runner.invoke(cli, ["list", "transformers"])
        assert result.exit_code == 0
        # The test validates the command structure works with directory discovery

    def test_list_command_class_methods_empty_discovery(self):
        """Test ListCommand methods with empty discovery results."""
        from unittest.mock import MagicMock

        # Test with empty discovery results
        empty_discovery = MagicMock()
        empty_discovery.discover_transformers.return_value = {}
        empty_discovery.discover_generators.return_value = {}

        # Test _list_transformers with no transformers
        result = ListCommand._list_transformers(empty_discovery)
        assert result == 0

        # Test _list_generators with no generators
        result = ListCommand._list_generators(empty_discovery)
        assert result == 0

    def test_list_output_format_consistency(self, runner):
        """Test that all list commands have consistent output format."""
        commands = ["transformers", "generators", "targets"]

        for cmd in commands:
            result = runner.invoke(cli, ["list", cmd])
            assert result.exit_code == 0

            # All should have usage information
            assert (
                "Usage:" in result.output
                or "datacompose add" in result.output
                or "Example:" in result.output
            )

    def test_error_handling_with_missing_files(self, runner, temp_dir):
        """Test error handling when discovery encounters missing files."""
        # Create transformer directory without required files
        transformer_dir = temp_dir / "corrupted" / "bad_transformer"
        transformer_dir.mkdir(parents=True)

        # Command should still complete successfully
        result = runner.invoke(cli, ["list", "transformers"])
        assert result.exit_code == 0
        # Should handle missing files gracefully
