"""
Integration tests for complete .env file workflow.
"""

import json
import os
from unittest.mock import Mock, patch

import pytest
from click.testing import CliRunner

from datacompose.cli.config import ConfigLoader
from datacompose.cli.main import cli


@pytest.mark.integration
class TestEnvWorkflowIntegration:
    """Integration tests for complete .env workflow."""

    @pytest.fixture
    def runner(self):
        """Fixture to provide Click CLI runner."""
        return CliRunner()

    @pytest.fixture
    def temp_env_file(self, tmp_path):
        """Create a temporary .env file with test credentials."""
        env_file = tmp_path / "test.env"
        env_content = """
# PostgreSQL configuration
POSTGRES_HOST=test-db.example.com
POSTGRES_PORT=5433
POSTGRES_DB=test_database
POSTGRES_USER=test_user
POSTGRES_PASSWORD=test_password123
"""
        env_file.write_text(env_content)
        return env_file

    @pytest.fixture
    def config_with_env_file(self, temp_env_file):
        """Create config with env_file path."""
        return {
            "version": "1.0",
            "default_target": "postgres",
            "targets": {
                "postgres": {"output": "./sql/postgres", "env_file": str(temp_env_file)}
            },
        }

    def test_complete_init_to_config_loading_workflow(self, runner, tmp_path):
        """Test complete workflow from init command to config loading."""
        # Change to temp directory
        original_cwd = os.getcwd()
        os.chdir(tmp_path)

        try:
            # Step 1: Create a basic init with --yes to avoid interactive prompts
            result = runner.invoke(cli, ["init", "--yes", "--verbose"])
            assert result.exit_code == 0

            # Step 2: Manually create postgres configuration (simulating what interactive would do)
            config_file = tmp_path / "datacompose.json"
            with open(config_file, "r") as f:
                config = json.load(f)

            # Add postgres target to config
            config["targets"]["postgres"] = {
                "output": "./sql/postgres",
                "env_file": "database.env",
            }

            # Write updated config back
            with open(config_file, "w") as f:
                json.dump(config, f)

            # Step 3: Manually create .env.example (simulating what init would do for postgres)
            env_example = tmp_path / ".env.example"
            env_example_content = """# PostgreSQL Configuration
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=postgres
POSTGRES_USER=postgres
POSTGRES_PASSWORD=your_password_here
"""
            env_example.write_text(env_example_content)

            # Step 4: Create actual .env file based on example
            env_file = tmp_path / "database.env"
            env_content = """
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=myapp
POSTGRES_USER=myuser
POSTGRES_PASSWORD=mypass
"""
            env_file.write_text(env_content)

            # Step 5: Load config and verify env file is used
            loaded_config = ConfigLoader.load_config(config_file)
            assert loaded_config is not None
            assert "postgres" in loaded_config["targets"]
            assert loaded_config["targets"]["postgres"]["env_file"] == "database.env"

            # Step 6: Test postgres config loading
            with patch.dict(os.environ, {}, clear=True):  # Clear environment
                with patch("dotenv.load_dotenv") as mock_load_dotenv:
                    # Simulate dotenv loading environment variables
                    def mock_load_env_side_effect(env_path):
                        os.environ.update(
                            {
                                "POSTGRES_HOST": "localhost",
                                "POSTGRES_PORT": "5432",
                                "POSTGRES_DB": "myapp",
                                "POSTGRES_USER": "myuser",
                                "POSTGRES_PASSWORD": "mypass",
                            }
                        )

                    mock_load_dotenv.side_effect = mock_load_env_side_effect

                    postgres_config = ConfigLoader.get_postgres_config(loaded_config)

                    assert postgres_config["host"] == "localhost"
                    assert postgres_config["port"] == 5432
                    assert postgres_config["database"] == "myapp"
                    assert postgres_config["user"] == "myuser"
                    assert postgres_config["password"] == "mypass"

        finally:
            os.chdir(original_cwd)

    def test_env_file_precedence_over_environment(
        self, config_with_env_file, temp_env_file
    ):
        """Test that .env file values take precedence over environment variables."""
        # Set conflicting environment variables
        env_vars = {
            "POSTGRES_HOST": "env-host",
            "POSTGRES_PORT": "9999",
            "POSTGRES_DB": "env-db",
            "POSTGRES_USER": "env-user",
            "POSTGRES_PASSWORD": "env-pass",
        }

        with patch.dict(os.environ, env_vars, clear=True):
            with patch("dotenv.load_dotenv") as mock_load_dotenv:
                # Simulate dotenv overriding environment variables
                def mock_load_env_side_effect(env_path):
                    os.environ.update(
                        {
                            "POSTGRES_HOST": "test-db.example.com",
                            "POSTGRES_PORT": "5433",
                            "POSTGRES_DB": "test_database",
                            "POSTGRES_USER": "test_user",
                            "POSTGRES_PASSWORD": "test_password123",
                        }
                    )

                mock_load_dotenv.side_effect = mock_load_env_side_effect

                postgres_config = ConfigLoader.get_postgres_config(config_with_env_file)

                # Should use values from .env file, not environment
                assert postgres_config["host"] == "test-db.example.com"
                assert postgres_config["port"] == 5433
                assert postgres_config["database"] == "test_database"
                assert postgres_config["user"] == "test_user"
                assert postgres_config["password"] == "test_password123"

    def test_fallback_to_environment_when_env_file_missing(self, tmp_path):
        """Test fallback to environment variables when .env file is missing."""
        config = {
            "version": "1.0",
            "targets": {
                "postgres": {
                    "output": "./sql/postgres",
                    "env_file": str(tmp_path / "nonexistent.env"),
                }
            },
        }

        env_vars = {
            "POSTGRES_HOST": "fallback-host",
            "POSTGRES_PORT": "6543",
            "POSTGRES_DB": "fallback-db",
            "POSTGRES_USER": "fallback-user",
            "POSTGRES_PASSWORD": "fallback-pass",
        }

        with patch.dict(os.environ, env_vars, clear=True):
            postgres_config = ConfigLoader.get_postgres_config(config)

            # Should use environment variables since .env file doesn't exist
            assert postgres_config["host"] == "fallback-host"
            assert postgres_config["port"] == 6543
            assert postgres_config["database"] == "fallback-db"
            assert postgres_config["user"] == "fallback-user"
            assert postgres_config["password"] == "fallback-pass"

    def test_defaults_when_no_env_file_or_environment(self, tmp_path):
        """Test default values when neither .env file nor environment variables exist."""
        config = {
            "version": "1.0",
            "targets": {
                "postgres": {
                    "output": "./sql/postgres",
                    "env_file": str(tmp_path / "nonexistent.env"),
                }
            },
        }

        with patch.dict(os.environ, {}, clear=True):
            postgres_config = ConfigLoader.get_postgres_config(config)

            # Should use default values
            assert postgres_config["host"] == "localhost"
            assert postgres_config["port"] == 5432
            assert postgres_config["database"] == "postgres"
            assert postgres_config["user"] == "postgres"
            assert postgres_config["password"] == ""

    def test_connection_test_integration(self, config_with_env_file):
        """Test connection testing integration with config loading."""
        mock_conn = Mock()

        with patch("psycopg2.connect", return_value=mock_conn) as mock_connect:
            with patch("dotenv.load_dotenv") as mock_load_dotenv:
                # Simulate loading from .env file
                def mock_load_env_side_effect(env_path):
                    os.environ.update(
                        {
                            "POSTGRES_HOST": "test-db.example.com",
                            "POSTGRES_PORT": "5433",
                            "POSTGRES_DB": "test_database",
                            "POSTGRES_USER": "test_user",
                            "POSTGRES_PASSWORD": "test_password123",
                        }
                    )

                mock_load_dotenv.side_effect = mock_load_env_side_effect

                success, message = ConfigLoader.test_postgres_connection(
                    config_with_env_file
                )

                assert success is True
                assert "test_user@test-db.example.com:5433/test_database" in message

                # Verify connection was attempted with correct parameters
                mock_connect.assert_called_once_with(
                    host="test-db.example.com",
                    port=5433,
                    database="test_database",
                    user="test_user",
                    password="test_password123",
                )
                mock_conn.close.assert_called_once()

    def test_multiple_env_files_workflow(self, runner, tmp_path):
        """Test workflow with multiple targets using different env files."""
        # Change to temp directory
        original_cwd = os.getcwd()
        os.chdir(tmp_path)

        try:
            # Create different env files
            prod_env = tmp_path / "prod.env"
            prod_env.write_text(
                "POSTGRES_HOST=prod-db.example.com\nPOSTGRES_DB=production\n"
            )

            dev_env = tmp_path / "dev.env"
            dev_env.write_text(
                "POSTGRES_HOST=dev-db.example.com\nPOSTGRES_DB=development\n"
            )

            # Create config manually for this test
            config = {
                "version": "1.0",
                "targets": {
                    "postgres_prod": {
                        "output": "./sql/prod",
                        "env_file": str(prod_env),
                    },
                    "postgres_dev": {"output": "./sql/dev", "env_file": str(dev_env)},
                },
            }

            config_file = tmp_path / "datacompose.json"
            with open(config_file, "w") as f:
                json.dump(config, f)

            # Test loading different configurations
            with patch("dotenv.load_dotenv") as mock_load_dotenv:
                # Test prod config
                def mock_load_prod_env(env_path):
                    if "prod.env" in str(env_path):
                        os.environ.update(
                            {
                                "POSTGRES_HOST": "prod-db.example.com",
                                "POSTGRES_DB": "production",
                            }
                        )

                mock_load_dotenv.side_effect = mock_load_prod_env

                prod_config = {
                    **config,
                    "targets": {"postgres": config["targets"]["postgres_prod"]},
                }
                postgres_config = ConfigLoader.get_postgres_config(prod_config)

                assert postgres_config["host"] == "prod-db.example.com"
                assert postgres_config["database"] == "production"

        finally:
            os.chdir(original_cwd)

    def test_path_completion_integration_with_env_files(self, runner, tmp_path):
        """Test that path completion works with .env files during init."""
        # Create some .env files in the temp directory
        (tmp_path / "database.env").touch()
        (tmp_path / "local.env").touch()
        (tmp_path / "production.env").touch()
        (tmp_path / "config.json").touch()  # Non-.env file

        original_cwd = os.getcwd()
        os.chdir(tmp_path)

        try:
            from datacompose.cli.commands.init import PathCompleter

            completer = PathCompleter(filter_extensions=[".env", ""])

            with patch(
                "os.listdir",
                return_value=[
                    "database.env",
                    "local.env",
                    "production.env",
                    "config.json",
                ],
            ):
                with patch("os.path.isdir", return_value=False):
                    # Should complete to .env files
                    result = completer.complete(str(tmp_path) + "/", 0)
                    if result:
                        assert result.endswith(".env") or result.endswith("/")

        finally:
            os.chdir(original_cwd)

    def test_error_handling_in_workflow(self, runner, tmp_path):
        """Test error handling throughout the workflow."""
        original_cwd = os.getcwd()
        os.chdir(tmp_path)

        try:
            config = {
                "version": "1.0",
                "targets": {
                    "postgres": {
                        "output": "./sql/postgres",
                        "env_file": "/invalid/path/nonexistent.env",
                    }
                },
            }

            config_file = tmp_path / "datacompose.json"
            with open(config_file, "w") as f:
                json.dump(config, f)

            # Should handle missing .env file gracefully
            with patch.dict(os.environ, {"POSTGRES_HOST": "fallback"}, clear=True):
                postgres_config = ConfigLoader.get_postgres_config(config)
                assert (
                    postgres_config["host"] == "fallback"
                )  # Falls back to environment

            with patch("psycopg2.connect", side_effect=Exception("Connection failed")):
                success, message = ConfigLoader.test_postgres_connection(config)
                assert success is False
                assert "Connection failed" in message

        finally:
            os.chdir(original_cwd)
