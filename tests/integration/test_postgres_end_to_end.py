"""End-to-end integration tests for PostgreSQL auto-registration workflow."""

import json
import shutil
import sys
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch

import pytest
from click.testing import CliRunner

# Add project root to path for imports
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from datacompose.cli.main import cli


@pytest.mark.integration
class TestPostgreSQLEndToEnd:
    """Integration tests for complete PostgreSQL auto-registration workflow."""

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

    def test_complete_workflow_init_and_add_with_auto_registration(self, runner):
        """Test complete workflow: init with PostgreSQL auto-registration → add emails."""
        with runner.isolated_filesystem():
            # Step 1: Initialize with PostgreSQL auto-registration
            init_result = runner.invoke(cli, [
                "init", "--yes", "--register-postgres", "--skip-completion"
            ])
            assert init_result.exit_code == 0
            assert "Configuration initialized" in init_result.output

            # Verify config was created correctly
            config_file = Path("datacompose.json")
            assert config_file.exists()

            with open(config_file, "r") as f:
                config = json.load(f)

            assert "postgres" in config["targets"]
            assert config["targets"]["postgres"]["auto_register"] is True
            assert config["default_target"] == "postgres"

            # Step 2: Add emails transformer with auto-registration
            # Mock the database connection for auto-registration
            mock_conn = Mock()
            mock_cursor = Mock()
            mock_conn.cursor.return_value = mock_cursor

            with patch('datacompose.generators.sql.postgres_generator.psycopg2') as mock_psycopg2:
                with patch('datacompose.cli.config.ConfigLoader.get_postgres_config') as mock_config:
                    mock_psycopg2.connect.return_value = mock_conn
                    mock_config.return_value = {
                        'host': 'localhost',
                        'port': 5432,
                        'database': 'test_db',
                        'user': 'test_user',
                        'password': 'test_pass'
                    }

                    add_result = runner.invoke(cli, ["add", "emails", "--target", "postgres"])

                    assert add_result.exit_code == 0
                    assert "UDF generated:" in add_result.output
                    assert "emails_postgres.sql" in add_result.output
                    assert "Auto-registration enabled for PostgreSQL" in add_result.output
                    assert "Successfully registered" in add_result.output

                    # Verify SQL file was created
                    sql_file = Path("transformers/postgres/emails_postgres.sql")
                    assert sql_file.exists()

                    # Verify database operations were called
                    mock_psycopg2.connect.assert_called_once()
                    mock_conn.commit.assert_called_once()

    def test_workflow_with_auto_registration_disabled(self, runner):
        """Test workflow when auto-registration is disabled."""
        with runner.isolated_filesystem():
            # Create config with auto-registration disabled
            config = {
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

            with open("datacompose.json", "w") as f:
                json.dump(config, f)

            add_result = runner.invoke(cli, ["add", "emails", "--target", "postgres", "--verbose"])

            assert add_result.exit_code == 0
            assert "UDF generated:" in add_result.output
            assert "Auto-registration disabled for PostgreSQL" in add_result.output
            assert "You can manually register functions by running:" not in add_result.output

    def test_workflow_with_postgres_alias(self, runner):
        """Test workflow using postgres alias instead of full generator path."""
        with runner.isolated_filesystem():
            # Initialize with default config
            init_result = runner.invoke(cli, ["init", "--yes", "--skip-completion"])
            assert init_result.exit_code == 0

            # Update config to add PostgreSQL with auto-registration
            with open("datacompose.json", "r") as f:
                config = json.load(f)

            config["targets"]["postgres"] = {
                "output": "./transformers/postgres",
                "env_file": ".env",
                "auto_register": True,
                "function_schema": "public"
            }

            with open("datacompose.json", "w") as f:
                json.dump(config, f)

            # Mock database connection
            mock_conn = Mock()
            mock_cursor = Mock()
            mock_conn.cursor.return_value = mock_cursor

            with patch('datacompose.generators.sql.postgres_generator.psycopg2') as mock_psycopg2:
                with patch('datacompose.cli.config.ConfigLoader.get_postgres_config') as mock_config:
                    mock_psycopg2.connect.return_value = mock_conn
                    mock_config.return_value = {
                        'host': 'localhost',
                        'port': 5432,
                        'database': 'test_db',
                        'user': 'test_user',
                        'password': 'test_pass'
                    }

                    # Test both postgres and postgresql aliases
                    for alias in ["postgres", "postgresql"]:
                        add_result = runner.invoke(cli, ["add", "emails", "--target", alias])

                        assert add_result.exit_code == 0
                        assert "UDF generated:" in add_result.output
                        assert "Auto-registration enabled for PostgreSQL" in add_result.output

    def test_workflow_with_auto_registration_failure(self, runner):
        """Test workflow when auto-registration fails gracefully."""
        with runner.isolated_filesystem():
            # Create config with auto-registration enabled
            config = {
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

            with open("datacompose.json", "w") as f:
                json.dump(config, f)

            # Mock database connection failure
            with patch('datacompose.generators.sql.postgres_generator.psycopg2') as mock_psycopg2:
                with patch('datacompose.cli.config.ConfigLoader.get_postgres_config') as mock_config:
                    mock_config.return_value = {
                        'host': 'localhost',
                        'port': 5432,
                        'database': 'test_db',
                        'user': 'test_user',
                        'password': 'test_pass'
                    }
                    # Simulate connection failure
                    mock_psycopg2.connect.side_effect = Exception("Connection refused")

                    add_result = runner.invoke(cli, ["add", "emails", "--target", "postgres"])

                    # Should still succeed overall (file generation works)
                    assert add_result.exit_code == 0
                    assert "UDF generated:" in add_result.output

                    # But auto-registration should fail gracefully
                    assert "Registration failed:" in add_result.output
                    assert "You can manually register functions by running:" in add_result.output
                    assert "psql -d your_database -f" in add_result.output

    def test_workflow_with_missing_config(self, runner):
        """Test workflow when no config file exists."""
        with runner.isolated_filesystem():
            # No config file - should use defaults
            add_result = runner.invoke(cli, ["add", "emails", "--target", "postgres"])

            # Will succeed but without auto-registration (no config)
            assert add_result.exit_code == 0
            assert "UDF generated:" in add_result.output
            # Should not mention auto-registration
            assert "Auto-registration" not in add_result.output

    def test_workflow_mixed_targets_only_postgres_auto_registers(self, runner):
        """Test workflow with multiple targets, only PostgreSQL auto-registers."""
        with runner.isolated_filesystem():
            # Create config with multiple targets
            config = {
                "version": "0.2.7.0",
                "default_target": "pyspark",
                "targets": {
                    "pyspark": {
                        "output": "./transformers/pyspark"
                    },
                    "postgres": {
                        "output": "./transformers/postgres",
                        "env_file": ".env",
                        "auto_register": True,
                        "function_schema": "public"
                    }
                }
            }

            with open("datacompose.json", "w") as f:
                json.dump(config, f)

            # Test PySpark target (no auto-registration)
            pyspark_result = runner.invoke(cli, ["add", "emails", "--target", "pyspark"])
            assert pyspark_result.exit_code == 0
            assert "Auto-registration" not in pyspark_result.output

            # Test PostgreSQL target (with auto-registration)
            mock_conn = Mock()
            mock_cursor = Mock()
            mock_conn.cursor.return_value = mock_cursor

            with patch('datacompose.generators.sql.postgres_generator.psycopg2') as mock_psycopg2:
                with patch('datacompose.cli.config.ConfigLoader.get_postgres_config') as mock_config:
                    mock_psycopg2.connect.return_value = mock_conn
                    mock_config.return_value = {
                        'host': 'localhost',
                        'port': 5432,
                        'database': 'test_db',
                        'user': 'test_user',
                        'password': 'test_pass'
                    }

                    postgres_result = runner.invoke(cli, ["add", "emails", "--target", "postgres"])

                    assert postgres_result.exit_code == 0
                    assert "Auto-registration enabled for PostgreSQL" in postgres_result.output

    def test_workflow_file_generation_without_database(self, runner):
        """Test that file generation works even if database is not available."""
        with runner.isolated_filesystem():
            # Initialize with PostgreSQL auto-registration
            init_result = runner.invoke(cli, [
                "init", "--yes", "--register-postgres", "--skip-completion"
            ])
            assert init_result.exit_code == 0

            # Add emails without mocking database (will fail registration but succeed overall)
            add_result = runner.invoke(cli, ["add", "emails", "--target", "postgres"])

            # File generation should succeed
            assert add_result.exit_code == 0
            assert "UDF generated:" in add_result.output

            # Verify SQL file was created and contains expected content
            sql_file = Path("transformers/postgres/emails_postgres.sql")
            assert sql_file.exists()

            sql_content = sql_file.read_text()
            assert "CREATE OR REPLACE FUNCTION" in sql_content
            assert "extract_email" in sql_content
            assert "is_valid_email" in sql_content
            assert "-- Generated for dialect: postgres" in sql_content

    def test_workflow_help_commands_include_postgres_options(self, runner):
        """Test that help commands include PostgreSQL-related options."""
        # Test init help includes new flags
        init_help = runner.invoke(cli, ["init", "--help"])
        assert init_help.exit_code == 0
        assert "--register-postgres" in init_help.output
        assert "--test-connection" in init_help.output

        # Test add help still works
        add_help = runner.invoke(cli, ["add", "--help"])
        assert add_help.exit_code == 0
        assert "--target" in add_help.output