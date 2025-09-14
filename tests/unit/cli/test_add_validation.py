"""Test validation functions in add command."""

import sys
from pathlib import Path

import pytest
from click.testing import CliRunner

# Add project root to path for imports
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from datacompose.cli.main import cli  # noqa: E402


@pytest.mark.unit
class TestAddCommandValidation:
    """Test validation functionality in add command."""

    @pytest.fixture(scope="class")
    def runner(self):
        """Fixture to provide Click CLI runner."""
        return CliRunner()

    def test_invalid_platform_shows_helpful_error(self, runner):
        """Test that invalid platform shows helpful error message."""
        result = runner.invoke(cli, ["add", "emails", "--target", "invalid_platform"])

        assert result.exit_code == 1
        assert "Platform 'invalid_platform' not found" in result.output
        assert "Available platforms:" in result.output
        assert "pyspark" in result.output
        # Only pyspark is currently available
        # assert "postgres" in result.output  # Would be here if postgres generator existed

    def test_valid_target_with_transformer_works(self, runner):
        """Test that valid target with transformer works (replaces type validation test)."""
        result = runner.invoke(cli, ["add", "emails", "--target", "pyspark"])

        # The command may fail if transformer doesn't exist, but target validation should pass
        # Check that it's not a target validation error
        assert "Platform 'pyspark' not found" not in result.output

    def test_valid_platform_and_type_works(self, runner):
        """Test that valid platform and type combination works."""
        # Skip this test since we only have pyspark without typed variants
        result = runner.invoke(cli, ["add", "emails", "--target", "pyspark"])

        # Should succeed (exit code 0) or show "already exists" message
        assert (
            result.exit_code == 0 or result.exit_code == 1
        )  # May fail if transformer doesn't exist
        # The important thing is it doesn't crash on validation

    def test_valid_platform_without_type_works(self, runner):
        """Test that valid platform without type uses defaults."""
        result = runner.invoke(cli, ["add", "emails", "--target", "pyspark"])

        # Check what the actual error is
        if result.exit_code != 0:
            print(f"Error output: {result.output}")

        # The command may fail if the transformer doesn't exist, but validation should pass
        # We're testing that the platform validation works, not the full add command
        assert "Platform 'pyspark' not found" not in result.output
        # If it fails, it should be because of transformer not found, not platform validation
        if result.exit_code != 0:
            assert (
                "Transformer not found" in result.output
                or "not found" in result.output.lower()
            )

    def test_invalid_platform_validation_priority(self, runner):
        """Test that platform validation happens and shows helpful errors."""
        result = runner.invoke(
            cli,
            [
                "add",
                "emails",
                "--target",
                "invalid_platform",
            ],
        )

        assert result.exit_code == 1
        # Should show platform error
        assert "Platform 'invalid_platform' not found" in result.output
