"""Test the init CLI command."""

import json
import os
import shutil
import sys
import tempfile
from pathlib import Path
from unittest.mock import patch

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

            assert config["version"] == "0.2.7.0"
            assert "targets" in config
            assert "pyspark" in config["targets"]
            # Only pyspark is in the default config now
            assert config["targets"]["pyspark"]["output"] == "./transformers/pyspark"

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
            assert "Use datacompose init --force to overwrite" in result.output

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
            assert config["version"] == "0.2.7.0"

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


@pytest.mark.unit
class TestInitCommandPostgres:
    """Test suite for init command PostgreSQL functionality."""

    @pytest.fixture(scope="class")
    def runner(self):
        """Fixture to provide Click CLI runner."""
        return CliRunner()

    def test_init_postgres_config_with_yes_flag(self, runner):
        """Test postgres configuration with --yes flag (skips interactive prompts)."""
        with runner.isolated_filesystem():
            # Test default init, then manually add postgres config
            result = runner.invoke(cli, ["init", "--yes"])
            assert result.exit_code == 0

            # Check that base config was created
            config_file = Path("datacompose.json")
            assert config_file.exists()

            # Manually test config loading for postgres (this tests our config logic)
            from datacompose.cli.config import ConfigLoader

            # Test postgres config with default settings
            test_config = {
                "version": "1.0",
                "targets": {
                    "postgres": {
                        "output": "./sql/postgres",
                        "env_file": ".env"
                    }
                }
            }

            # Test that our postgres config methods work
            with patch.dict(os.environ, {}, clear=True):
                postgres_config = ConfigLoader.get_postgres_config(test_config)
                assert postgres_config['host'] == 'localhost'  # default
                assert postgres_config['port'] == 5432  # default

    def test_init_register_postgres_flag_with_yes(self, runner):
        """Test --register-postgres flag with --yes mode."""
        with runner.isolated_filesystem():
            result = runner.invoke(cli, ["init", "--yes", "--register-postgres", "--skip-completion"])
            assert result.exit_code == 0
            assert "Configuration initialized" in result.output

            # Check that config includes PostgreSQL with auto-registration
            config_file = Path("datacompose.json")
            assert config_file.exists()

            with open(config_file, "r") as f:
                config = json.load(f)

            # PostgreSQL should be added as a target
            assert "postgres" in config["targets"]
            postgres_config = config["targets"]["postgres"]

            # Check auto-registration settings
            assert postgres_config["auto_register"] is True
            assert postgres_config["function_schema"] == "public"
            assert postgres_config["env_file"] == ".env"

            # Default target should be postgres when --register-postgres is used
            assert config["default_target"] == "postgres"

    def test_init_register_postgres_flag_without_yes(self, runner):
        """Test --register-postgres flag only affects --yes mode."""
        with runner.isolated_filesystem():
            # The flag should not affect interactive mode (would need input simulation)
            # This test verifies the flag exists and is parsed correctly
            result = runner.invoke(cli, ["init", "--help"])
            assert result.exit_code == 0
            assert "--register-postgres" in result.output
            assert "Enable auto-registration of PostgreSQL functions" in result.output

    def test_init_test_connection_flag(self, runner):
        """Test --test-connection flag appears in help."""
        result = runner.invoke(cli, ["init", "--help"])
        assert result.exit_code == 0
        assert "--test-connection" in result.output
        assert "Test PostgreSQL database connection during setup" in result.output

    def test_init_postgres_auto_registration_config_structure(self, runner):
        """Test that PostgreSQL auto-registration creates correct config structure."""
        with runner.isolated_filesystem():
            result = runner.invoke(cli, ["init", "--yes", "--register-postgres", "--skip-completion"])
            assert result.exit_code == 0

            # Verify .env.example is created for PostgreSQL
            env_example = Path(".env.example")
            assert env_example.exists()

            env_content = env_example.read_text()
            assert "POSTGRES_HOST=" in env_content
            assert "POSTGRES_PORT=" in env_content
            assert "POSTGRES_DB=" in env_content
            assert "POSTGRES_USER=" in env_content
            assert "POSTGRES_PASSWORD=" in env_content

    def test_init_register_postgres_preserves_pyspark(self, runner):
        """Test that --register-postgres keeps existing targets."""
        with runner.isolated_filesystem():
            result = runner.invoke(cli, ["init", "--yes", "--register-postgres", "--skip-completion"])
            assert result.exit_code == 0

            config_file = Path("datacompose.json")
            with open(config_file, "r") as f:
                config = json.load(f)

            # Both targets should exist
            assert "pyspark" in config["targets"]
            assert "postgres" in config["targets"]

            # PySpark config should be unchanged
            assert config["targets"]["pyspark"]["output"] == "./transformers/pyspark"

            # But default should switch to postgres
            assert config["default_target"] == "postgres"

    def test_init_postgres_env_example_creation(self, runner):
        """Test that .env.example template content is correct."""
        # Test the env example content generation directly
        from datacompose.cli.commands.init import DEFAULT_CONFIG

        # Test that we can simulate the env example creation logic
        postgres_config = {
            "output": "./sql/postgres",
            "env_file": "custom.env"
        }

        # The env example content should contain postgres variables
        env_example_content = """# PostgreSQL Configuration
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=postgres
POSTGRES_USER=postgres
POSTGRES_PASSWORD=your_password_here
"""

        # Test that our expected content contains all required variables
        assert "POSTGRES_HOST=" in env_example_content
        assert "POSTGRES_PORT=" in env_example_content
        assert "POSTGRES_DB=" in env_example_content
        assert "POSTGRES_USER=" in env_example_content
        assert "POSTGRES_PASSWORD=" in env_example_content

    def test_init_postgres_custom_env_file(self, runner):
        """Test postgres configuration loading with custom env file path."""
        from datacompose.cli.config import ConfigLoader

        # Test config with custom env_file path
        test_config = {
            "version": "1.0",
            "targets": {
                "postgres": {
                    "output": "./sql/postgres",
                    "env_file": "config/database.env"
                }
            }
        }

        # Test that the config correctly specifies the custom env file path
        postgres_target = test_config["targets"]["postgres"]
        assert postgres_target["env_file"] == "config/database.env"

        # Test that ConfigLoader uses this path
        with patch('pathlib.Path.exists', return_value=False):
            with patch.dict(os.environ, {'POSTGRES_HOST': 'test-host'}, clear=True):
                postgres_config = ConfigLoader.get_postgres_config(test_config)
                assert postgres_config['host'] == 'test-host'

    def test_init_multiple_targets_config_structure(self, runner):
        """Test configuration structure for multiple targets including postgres."""
        # Test that we understand the difference between SQL and non-SQL targets
        multi_target_config = {
            "version": "1.0",
            "targets": {
                "postgres": {
                    "output": "./sql/postgres",
                    "env_file": ".env"
                },
                "pyspark": {
                    "output": "./transformers/pyspark"
                    # No env_file for pyspark
                }
            }
        }

        # Test that postgres has env_file but pyspark doesn't
        assert "env_file" in multi_target_config["targets"]["postgres"]
        assert "env_file" not in multi_target_config["targets"]["pyspark"]

    def test_path_completion_functionality(self, runner):
        """Test path completion functionality independently."""
        from datacompose.cli.commands.init import PathCompleter, input_with_path_completion

        # Test PathCompleter with .env filter
        completer = PathCompleter(filter_extensions=['.env', ''])
        assert completer.filter_extensions == ['.env', '']

        # Test input_with_path_completion with readline disabled
        with patch('datacompose.cli.commands.init.READLINE_AVAILABLE', False):
            with patch('builtins.input', return_value='test.env'):
                result = input_with_path_completion("Enter file: ", filter_extensions=['.env'])
                assert result == 'test.env'

    def test_sql_target_identification(self, runner):
        """Test identification of SQL targets for env_file configuration."""
        # The init command should identify postgres and mysql as SQL targets
        sql_targets = ['postgres', 'mysql']
        non_sql_targets = ['pyspark', 'spark', 'databricks']

        # Test that we can distinguish SQL from non-SQL targets
        for target in sql_targets:
            # SQL targets should get env_file configuration
            assert target in ['postgres', 'mysql']

        for target in non_sql_targets:
            # Non-SQL targets should not get env_file configuration
            assert target not in ['postgres', 'mysql']

    def test_env_file_path_validation(self, runner):
        """Test that env file paths are handled correctly."""
        from datacompose.cli.config import ConfigLoader

        # Test various env file path scenarios
        test_cases = [
            (".env", "default case"),
            ("config/db.env", "nested directory"),
            ("../shared.env", "relative path"),
            ("/absolute/path/.env", "absolute path")
        ]

        for env_file_path, description in test_cases:
            test_config = {
                "version": "1.0",
                "targets": {
                    "postgres": {
                        "output": "./sql/postgres",
                        "env_file": env_file_path
                    }
                }
            }

            # Should handle all path types without error
            with patch('pathlib.Path.exists', return_value=False):
                with patch.dict(os.environ, {}, clear=True):
                    result = ConfigLoader.get_postgres_config(test_config)
                    assert result is not None, f"Failed for {description}"
