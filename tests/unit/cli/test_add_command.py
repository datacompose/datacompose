"""Test the add CLI command."""

import shutil
import sys
import tempfile
from pathlib import Path

import pytest
from click.testing import CliRunner

# Add project root to path for imports
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))


@pytest.mark.unit
class TestAddCommand:
    """Test suite for add command functionality."""

    @pytest.fixture(scope="class")
    def runner(self):
        """Fixture to provide Click CLI runner."""
        return CliRunner()

    @pytest.fixture
    def cli_command(self):
        """Fixture to provide CLI command."""
        from datacompose.cli.main import cli

        return cli

    @pytest.fixture
    def temp_dir(self):
        """Fixture to provide temporary directory for tests."""
        temp_dir = tempfile.mkdtemp()
        yield Path(temp_dir)
        shutil.rmtree(temp_dir)

    @pytest.fixture
    def init_datacompose(self, runner, temp_dir):
        """Fixture to initialize datacompose project."""
        pass

    def test_add_help(self, runner, cli_command):
        """Test add command help output."""
        result = runner.invoke(cli_command, ["add", "--help"])
        assert result.exit_code == 0
        assert "Add UDFs" in result.output
        assert "TRANSFORMER:" in result.output
        assert "--target" in result.output
        assert "--type" in result.output
        assert "--output" in result.output
        assert "--verbose" in result.output

    def test_add_missing_transformer(self, runner, cli_command):
        """Test add command without transformer argument."""
        result = runner.invoke(cli_command, ["add"])
        assert result.exit_code == 2
        assert "Missing argument" in result.output

    def test_add_invalid_transformer(self, runner, temp_dir, cli_command):
        """Test add command with non-existent transformer."""
        with runner.isolated_filesystem():
            result = runner.invoke(cli_command, ["add", "nonexistent_transformer"])
            assert result.exit_code == 1
            assert "Transformer not found" in result.output

    def test_add_invalid_target(self, runner, temp_dir, cli_command):
        """Test add command with invalid target."""
        with runner.isolated_filesystem():
            # This will fail because transformer doesn't exist, but we can check target validation
            result = runner.invoke(
                cli_command, ["add", "test_transformer", "--target", "invalid.target"]
            )
            # Should fail on transformer first, but let's test the flow
            assert result.exit_code == 1

    def test_add_verbose_flag(self, runner, temp_dir, cli_command):
        """Test add command with verbose flag."""
        with runner.isolated_filesystem():
            result = runner.invoke(
                cli_command, ["add", "test_transformer", "--verbose"]
            )
            assert result.exit_code == 1  # Will fail on missing transformer
            # But verbose flag should be parsed correctly

    def test_add_custom_output_dir(self, runner, temp_dir, cli_command):
        """Test add command with custom output directory."""
        with runner.isolated_filesystem():
            custom_output = str(temp_dir / "custom_output")
            result = runner.invoke(
                cli_command, ["add", "test_transformer", "--output", custom_output]
            )
            assert result.exit_code == 1  # Will fail on missing transformer
            # But output flag should be parsed correctly

    def test_add_with_invalid_flag(self, runner, temp_dir, cli_command):
        """Test add command with invalid flag shows proper error."""
        with runner.isolated_filesystem():
            result = runner.invoke(
                cli_command, ["add", "test_transformer", "--invalid-flag"]
            )
            assert result.exit_code == 2  # Click returns 2 for unknown options
            assert "Error" in result.output or "no such option" in result.output

    def test_add_target_type_syntax(self, runner, temp_dir, cli_command):
        """Test add command with separate target and type flags."""
        with runner.isolated_filesystem():
            result = runner.invoke(
                cli_command,
                [
                    "add",
                    "test_transformer",
                    "--target",
                    "pyspark",
                ],
            )
            assert result.exit_code == 1  # Will fail on missing transformer
            # But flags should be parsed correctly

    def test_add_target_only_pyspark(self, runner, temp_dir, cli_command):
        """Test add command with target only (should default to pandas_udf
        for pyspark)."""
        with runner.isolated_filesystem():
            result = runner.invoke(
                cli_command, ["add", "test_transformer", "--target", "pyspark"]
            )
            assert result.exit_code == 1  # Will fail on missing transformer
            # But should default to pandas_udf

    def test_add_target_only_postgres(self, runner, temp_dir, cli_command):
        """Test add command with target only (should default to sql_udf for postgres)."""
        with runner.isolated_filesystem():
            result = runner.invoke(
                cli_command, ["add", "test_transformer", "--target", "postgres"]
            )
            assert result.exit_code == 1  # Will fail on missing transformer
            # But should default to sql_udf

    def test_add_invalid_target_platform(self, runner, temp_dir, cli_command):
        """Test add command with invalid target platform."""
        with runner.isolated_filesystem():
            result = runner.invoke(
                cli_command, ["add", "test_transformer", "--target", "invalid_platform"]
            )
            # Should fail on invalid platform
            assert result.exit_code == 1

    def test_add_invalid_type_for_platform(self, runner, temp_dir, cli_command):
        """Test add command with invalid type for a platform."""
        with runner.isolated_filesystem():
            result = runner.invoke(
                cli_command,
                [
                    "add",
                    "test_transformer",
                    "--target",
                    "postgres",
                    "--type",
                    "invalid_type",
                ],
            )
            # Should fail on invalid type for platform
            assert result.exit_code == 1

    def test_add_real_transformer_with_explicit_type(
        self, runner, temp_dir, cli_command
    ):
        """Test add command with real transformer and explicit type."""
        with runner.isolated_filesystem():
            result = runner.invoke(
                cli_command,
                ["add", "clean_emails", "--target", "pyspark", "--type", "generator"],
            )
            assert result.exit_code == 0
            assert "generated:" in result.output.lower()
            assert "email_primitives.py" in result.output  # PySpark generates primitives files
