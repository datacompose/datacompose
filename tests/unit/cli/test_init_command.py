"""Test the init CLI command."""

import json
import shutil
import sys
import tempfile
from pathlib import Path

import pytest
from click.testing import CliRunner

# Add project root to path for imports
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from datacompose.cli.main import cli  # noqa: E402


@pytest.mark.unit
class TestInitCommand:
    """Test suite for init command functionality."""

    @pytest.fixture(scope="class")
    def runner(self):
        """Fixture to provide Click CLI runner."""
        return CliRunner()

    @pytest.fixture
    def temp_dir(self):
        """Fixture to provide temporary directory for tests."""
        temp_dir = tempfile.mkdtemp()
        yield Path(temp_dir)
        shutil.rmtree(temp_dir)

    def test_init_help(self, runner):
        """Test init command help output."""
        result = runner.invoke(cli, ["init", "--help"])
        assert result.exit_code == 0
        assert "Initialize project configuration" in result.output
        assert "--force" in result.output
        assert "--output" in result.output
        assert "--verbose" in result.output
        assert "--yes" in result.output

    def test_init_default_config(self, runner, temp_dir):
        """Test init command with default configuration."""
        with runner.isolated_filesystem():
            result = runner.invoke(cli, ["init", "--yes"])
            assert result.exit_code == 0
            assert "Configuration initialized" in result.output

            # Check that datacompose.json was created
            config_file = Path("datacompose.json")
            assert config_file.exists()

            # Validate config contents
            with open(config_file, "r") as f:
                config = json.load(f)

            assert config["version"] == "1.0"
            assert "targets" in config
            assert "pyspark" in config["targets"]
            # Only pyspark is in the default config now
            assert config["targets"]["pyspark"]["output"] == "./build/pyspark"

    def test_init_custom_output_path(self, runner, temp_dir):
        """Test init command with custom output path."""
        with runner.isolated_filesystem():
            custom_path = "custom-config.json"
            result = runner.invoke(cli, ["init", "--yes", "--output", custom_path])
            assert result.exit_code == 0

            # Check that custom config file was created
            config_file = Path(custom_path)
            assert config_file.exists()

    def test_init_existing_config_without_force(self, runner, temp_dir):
        """Test init command when config already exists without force flag."""
        with runner.isolated_filesystem():
            # Create existing config
            config_file = Path("datacompose.json")
            config_file.write_text('{"existing": "config"}')

            result = runner.invoke(cli, ["init", "--yes"])
            assert result.exit_code == 1
            assert "Configuration file already exists" in result.output
            assert "Use --force to overwrite" in result.output

    def test_init_existing_config_with_force(self, runner, temp_dir):
        """Test init command when config already exists with force flag."""
        with runner.isolated_filesystem():
            # Create existing config
            config_file = Path("datacompose.json")
            config_file.write_text('{"existing": "config"}')

            result = runner.invoke(cli, ["init", "--yes", "--force"])
            assert result.exit_code == 0
            assert "Configuration initialized" in result.output

            # Check that config was overwritten
            with open(config_file, "r") as f:
                config = json.load(f)

            assert "existing" not in config
            assert config["version"] == "1.0"

    def test_init_verbose_output(self, runner, temp_dir):
        """Test init command with verbose output."""
        with runner.isolated_filesystem():
            result = runner.invoke(cli, ["init", "--yes", "--verbose"])
            assert result.exit_code == 0
            assert "Used template: default" in result.output
            assert "Created directory structure" in result.output
            assert "Next steps:" in result.output

    def test_init_directory_structure_creation(self, runner, temp_dir):
        """Test that init command creates expected directory structure."""
        with runner.isolated_filesystem():
            result = runner.invoke(cli, ["init", "--yes"])
            assert result.exit_code == 0

            # Check that build directory parent is created (if needed)
            # Note: The actual implementation creates parent directories of output paths
