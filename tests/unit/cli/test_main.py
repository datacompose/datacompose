"""Test the main CLI entry point."""

import sys
from pathlib import Path

import pytest
from click.testing import CliRunner

# Add project root to path for imports
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from datacompose.cli.main import cli  # noqa: E402


@pytest.mark.unit
class TestMainCLI:
    """Test suite for main CLI group functionality."""

    @pytest.fixture(scope="class")
    def runner(self):
        """Fixture to provide Click CLI runner."""
        return CliRunner()

    def test_cli_help(self, runner):
        """Test main CLI help output."""
        result = runner.invoke(cli, ["--help"])
        assert result.exit_code == 0
        assert "Generate data cleaning UDFs" in result.output
        assert "Commands:" in result.output
        assert "add" in result.output
        assert "init" in result.output
        assert "list" in result.output

    def test_cli_version(self, runner):
        """Test CLI version output."""
        result = runner.invoke(cli, ["--version"])
        assert result.exit_code == 0
        assert "0.1.0" in result.output

    def test_cli_no_command(self, runner):
        """Test CLI without any command."""
        result = runner.invoke(cli)
        assert result.exit_code == 2
        assert "Usage:" in result.output

    def test_cli_invalid_command(self, runner):
        """Test CLI with invalid command."""
        result = runner.invoke(cli, ["invalid-command"])
        assert result.exit_code == 2
        assert "No such command" in result.output
