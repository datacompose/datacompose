"""Test auto-registration functionality in add command."""

import json
import sys
from pathlib import Path
from unittest.mock import Mock, patch

import pytest
from click.testing import CliRunner

# Add project root to path for imports
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from datacompose.cli.main import cli


@pytest.mark.unit
class TestAddAutoRegistration:
    """Test suite for auto-registration functionality in add command."""

    @pytest.fixture(scope="class")
    def runner(self):
        """Fixture to provide Click CLI runner."""
        return CliRunner()

    @pytest.fixture
    def config_with_auto_registration(self):
        """Fixture providing config with auto-registration enabled."""
        return {
            "version": "0.2.7.0",
            "default_target": "postgres",
            "targets": {
                "postgres": {
                    "output": "./transformers/postgres",
                    "env_file": ".env",
                    "auto_register": True,
                    "function_schema": "public"
                }
            }
        }

    @pytest.fixture
    def config_without_auto_registration(self):
        """Fixture providing config with auto-registration disabled."""
        return {
            "version": "0.2.7.0",
            "default_target": "postgres",
            "targets": {
                "postgres": {
                    "output": "./transformers/postgres",
                    "env_file": ".env",
                    "auto_register": False
                }
            }
        }

    def test_add_auto_registration_enabled(self, runner, config_with_auto_registration):
        """Test add command with auto-registration enabled."""
        with runner.isolated_filesystem():
            # Create config file
            with open("datacompose.json", "w") as f:
                json.dump(config_with_auto_registration, f)

            # Mock the generator to return success and have register_functions method
            mock_generator = Mock()
            mock_generator.generate.return_value = {
                "output_path": "./transformers/postgres/emails_postgres.sql",
                "function_name": "emails_udf",
                "skipped": False
            }
            mock_generator.register_functions.return_value = (True, "Successfully registered 5 functions")

            with patch("datacompose.cli.commands.add.ConfigLoader.load_config") as mock_load:
                with patch("datacompose.transformers.discovery.TransformerDiscovery.resolve_generator") as mock_resolve:
                    with patch("datacompose.transformers.discovery.TransformerDiscovery.resolve_transformer") as mock_transformer:
                        with patch("builtins.open", create=True) as mock_open:
                            mock_load.return_value = config_with_auto_registration
                            # Mock the generator class to return our mock generator instance
                            mock_generator_class = Mock(return_value=mock_generator)
                            mock_resolve.return_value = mock_generator_class
                            mock_transformer.return_value = ("emails", Path("./emails"))
                            mock_open.return_value.__enter__.return_value.read.return_value = "-- SQL functions"

                            result = runner.invoke(cli, ["add", "emails", "--target", "postgres"])

                            assert result.exit_code == 0
                            assert "Auto-registration enabled for PostgreSQL" in result.output
                            assert "Successfully registered 5 functions" in result.output
                            mock_generator.register_functions.assert_called_once()

    def test_add_auto_registration_disabled(self, runner, config_without_auto_registration):
        """Test add command with auto-registration disabled."""
        with runner.isolated_filesystem():
            # Create config file
            with open("datacompose.json", "w") as f:
                json.dump(config_without_auto_registration, f)

            # Mock the generator
            mock_generator = Mock()
            mock_generator.generate.return_value = {
                "output_path": "./transformers/postgres/emails_postgres.sql",
                "function_name": "emails_udf",
                "skipped": False
            }

            with patch("datacompose.cli.commands.add.ConfigLoader.load_config") as mock_load:
                with patch("datacompose.transformers.discovery.TransformerDiscovery.resolve_generator") as mock_resolve:
                    with patch("datacompose.transformers.discovery.TransformerDiscovery.resolve_transformer") as mock_transformer:
                        mock_load.return_value = config_without_auto_registration
                        mock_generator_class = Mock(return_value=mock_generator)
                        mock_resolve.return_value = mock_generator_class
                        mock_transformer.return_value = ("emails", Path("./emails"))

                        result = runner.invoke(cli, ["add", "emails", "--target", "postgres", "--verbose"])

                        assert result.exit_code == 0
                        assert "Auto-registration disabled for PostgreSQL" in result.output
                        # register_functions should not be called
                        assert not hasattr(mock_generator, 'register_functions') or not mock_generator.register_functions.called

    def test_add_auto_registration_missing_config(self, runner):
        """Test add command when no config file exists."""
        with runner.isolated_filesystem():
            # No config file - should still work but no auto-registration
            result = runner.invoke(cli, ["add", "emails", "--target", "postgres"])
            # Will fail due to missing transformer, but not due to config issues
            assert result.exit_code == 1

    def test_add_auto_registration_missing_postgres_config(self, runner):
        """Test add command when postgres not in config."""
        with runner.isolated_filesystem():
            config = {
                "version": "0.2.7.0",
                "targets": {
                    "pyspark": {"output": "./transformers/pyspark"}
                }
            }

            with open("datacompose.json", "w") as f:
                json.dump(config, f)

            # Mock the generator
            mock_generator = Mock()
            mock_generator.generate.return_value = {
                "output_path": "./transformers/postgres/emails_postgres.sql",
                "function_name": "emails_udf",
                "skipped": False
            }

            with patch("datacompose.cli.commands.add.ConfigLoader.load_config") as mock_load:
                with patch("datacompose.transformers.discovery.TransformerDiscovery.resolve_generator") as mock_resolve:
                    with patch("datacompose.transformers.discovery.TransformerDiscovery.resolve_transformer") as mock_transformer:
                        mock_load.return_value = config
                        mock_generator_class = Mock(return_value=mock_generator)
                        mock_resolve.return_value = mock_generator_class
                        mock_transformer.return_value = ("emails", Path("./emails"))

                        result = runner.invoke(cli, ["add", "emails", "--target", "postgres"])

                        assert result.exit_code == 0
                        # Should not attempt auto-registration without postgres config
                        assert "Auto-registration" not in result.output

    def test_add_auto_registration_failure(self, runner, config_with_auto_registration):
        """Test add command when auto-registration fails."""
        with runner.isolated_filesystem():
            # Create config file
            with open("datacompose.json", "w") as f:
                json.dump(config_with_auto_registration, f)

            # Mock the generator to return failure
            mock_generator = Mock()
            mock_generator.generate.return_value = {
                "output_path": "./transformers/postgres/emails_postgres.sql",
                "function_name": "emails_udf",
                "skipped": False
            }
            mock_generator.register_functions.return_value = (False, "Connection failed: database not found")

            with patch("datacompose.cli.commands.add.ConfigLoader.load_config") as mock_load:
                with patch("datacompose.transformers.discovery.TransformerDiscovery.resolve_generator") as mock_resolve:
                    with patch("datacompose.transformers.discovery.TransformerDiscovery.resolve_transformer") as mock_transformer:
                        with patch("builtins.open", create=True) as mock_open:
                            mock_load.return_value = config_with_auto_registration
                            mock_generator_class = Mock(return_value=mock_generator)
                            mock_resolve.return_value = mock_generator_class
                            mock_transformer.return_value = ("emails", Path("./emails"))
                            mock_open.return_value.__enter__.return_value.read.return_value = "-- SQL functions"

                            result = runner.invoke(cli, ["add", "emails", "--target", "postgres"])

                            assert result.exit_code == 0  # Should still succeed overall
                            assert "Registration failed: Connection failed" in result.output
                            assert "You can manually register functions by running:" in result.output
                            assert "psql -d your_database -f" in result.output

    def test_add_auto_registration_exception(self, runner, config_with_auto_registration):
        """Test add command when auto-registration raises exception."""
        with runner.isolated_filesystem():
            # Create config file
            with open("datacompose.json", "w") as f:
                json.dump(config_with_auto_registration, f)

            # Mock the generator to raise exception
            mock_generator = Mock()
            mock_generator.generate.return_value = {
                "output_path": "./transformers/postgres/emails_postgres.sql",
                "function_name": "emails_udf",
                "skipped": False
            }
            mock_generator.register_functions.side_effect = Exception("Unexpected error")

            with patch("datacompose.cli.commands.add.ConfigLoader.load_config") as mock_load:
                with patch("datacompose.transformers.discovery.TransformerDiscovery.resolve_generator") as mock_resolve:
                    with patch("datacompose.transformers.discovery.TransformerDiscovery.resolve_transformer") as mock_transformer:
                        with patch("builtins.open", create=True) as mock_open:
                            mock_load.return_value = config_with_auto_registration
                            mock_generator_class = Mock(return_value=mock_generator)
                            mock_resolve.return_value = mock_generator_class
                            mock_transformer.return_value = ("emails", Path("./emails"))
                            mock_open.return_value.__enter__.return_value.read.return_value = "-- SQL functions"

                            result = runner.invoke(cli, ["add", "emails", "--target", "postgres"])

                            assert result.exit_code == 0  # Should still succeed overall
                            assert "Auto-registration failed: Unexpected error" in result.output
                            assert "You can manually register functions by running:" in result.output

    def test_add_auto_registration_generator_without_method(self, runner, config_with_auto_registration):
        """Test add command when generator doesn't support auto-registration."""
        with runner.isolated_filesystem():
            # Create config file
            with open("datacompose.json", "w") as f:
                json.dump(config_with_auto_registration, f)

            # Mock generator without register_functions method
            mock_generator = Mock()
            mock_generator.generate.return_value = {
                "output_path": "./transformers/postgres/emails_postgres.sql",
                "function_name": "emails_udf",
                "skipped": False
            }
            # Don't add register_functions method to simulate non-PostgreSQL generator

            with patch("datacompose.cli.commands.add.ConfigLoader.load_config") as mock_load:
                with patch("datacompose.transformers.discovery.TransformerDiscovery.resolve_generator") as mock_resolve:
                    with patch("datacompose.transformers.discovery.TransformerDiscovery.resolve_transformer") as mock_transformer:
                        mock_load.return_value = config_with_auto_registration
                        mock_generator_class = Mock(return_value=mock_generator)
                        mock_resolve.return_value = mock_generator_class
                        mock_transformer.return_value = ("emails", Path("./emails"))

                        # Remove the register_functions method if it exists
                        if hasattr(mock_generator, 'register_functions'):
                            delattr(mock_generator, 'register_functions')

                        result = runner.invoke(cli, ["add", "emails", "--target", "postgres"])

                        assert result.exit_code == 0
                        assert "Auto-registration not supported for this generator" in result.output

    def test_add_non_postgres_target_ignores_auto_registration(self, runner):
        """Test add command with non-postgres target ignores auto-registration."""
        with runner.isolated_filesystem():
            config = {
                "version": "0.2.7.0",
                "targets": {
                    "pyspark": {"output": "./transformers/pyspark"},
                    "postgres": {
                        "output": "./transformers/postgres",
                        "auto_register": True
                    }
                }
            }

            with open("datacompose.json", "w") as f:
                json.dump(config, f)

            # Mock the generator
            mock_generator = Mock()
            mock_generator.generate.return_value = {
                "output_path": "./transformers/pyspark/emails.py",
                "function_name": "emails_udf",
                "skipped": False
            }

            with patch("datacompose.cli.commands.add.ConfigLoader.load_config") as mock_load:
                with patch("datacompose.transformers.discovery.TransformerDiscovery.resolve_generator") as mock_resolve:
                    with patch("datacompose.transformers.discovery.TransformerDiscovery.resolve_transformer") as mock_transformer:
                        mock_load.return_value = config
                        mock_generator_class = Mock(return_value=mock_generator)
                        mock_resolve.return_value = mock_generator_class
                        mock_transformer.return_value = ("emails", Path("./emails"))

                        result = runner.invoke(cli, ["add", "emails", "--target", "pyspark"])

                        assert result.exit_code == 0
                        # Should not mention auto-registration for non-postgres targets
                        assert "Auto-registration" not in result.output