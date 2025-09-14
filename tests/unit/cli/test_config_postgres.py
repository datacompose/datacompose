"""
Tests for PostgreSQL-related ConfigLoader methods.
"""

import json
import os
import tempfile
from pathlib import Path
from unittest.mock import Mock, mock_open, patch

import pytest

from datacompose.cli.config import ConfigLoader


@pytest.mark.unit
class TestConfigLoaderPostgres:
    """Test PostgreSQL-related ConfigLoader methods."""

    def test_get_postgres_config_no_config(self):
        """Test get_postgres_config when no config is provided or found."""
        with patch.object(ConfigLoader, 'load_config', return_value=None):
            with patch.dict(os.environ, {}, clear=True):
                result = ConfigLoader.get_postgres_config()

                # Should return defaults
                assert result['host'] == 'localhost'
                assert result['port'] == 5432
                assert result['database'] == 'postgres'
                assert result['user'] == 'postgres'
                assert result['password'] == ''

    def test_get_postgres_config_from_environment(self):
        """Test get_postgres_config loads from environment variables."""
        env_vars = {
            'POSTGRES_HOST': 'db.example.com',
            'POSTGRES_PORT': '5433',
            'POSTGRES_DB': 'myapp',
            'POSTGRES_USER': 'dbuser',
            'POSTGRES_PASSWORD': 'secret123'
        }

        with patch.object(ConfigLoader, 'load_config', return_value=None):
            with patch.dict(os.environ, env_vars, clear=True):
                result = ConfigLoader.get_postgres_config()

                assert result['host'] == 'db.example.com'
                assert result['port'] == 5433
                assert result['database'] == 'myapp'
                assert result['user'] == 'dbuser'
                assert result['password'] == 'secret123'

    def test_get_postgres_config_from_config_file(self):
        """Test get_postgres_config loads from config file."""
        config_data = {
            'version': '1.0',
            'targets': {
                'postgres': {
                    'env_file': '/path/to/custom.env',
                    'output': './sql'
                }
            }
        }

        with patch.object(ConfigLoader, 'load_config', return_value=config_data):
            with patch('pathlib.Path.exists', return_value=False):  # env file doesn't exist
                with patch.dict(os.environ, {'POSTGRES_HOST': 'config-host'}, clear=True):
                    result = ConfigLoader.get_postgres_config()

                    # Should load from environment since .env file doesn't exist
                    assert result['host'] == 'config-host'
                    assert result['port'] == 5432  # default
                    assert result['database'] == 'postgres'  # default

    def test_get_postgres_config_with_env_file(self, tmp_path):
        """Test get_postgres_config loads from .env file."""
        # Create test .env file
        env_file = tmp_path / 'test.env'
        env_content = """
POSTGRES_HOST=envfile-host
POSTGRES_PORT=9999
POSTGRES_DB=envfile-db
POSTGRES_USER=envfile-user
POSTGRES_PASSWORD=envfile-pass
"""
        env_file.write_text(env_content)

        config_data = {
            'version': '1.0',
            'targets': {
                'postgres': {
                    'env_file': str(env_file)
                }
            }
        }

        # Mock dotenv to simulate loading
        mock_load_dotenv = Mock()

        with patch.object(ConfigLoader, 'load_config', return_value=config_data):
            with patch('pathlib.Path.exists', return_value=True):
                with patch('dotenv.load_dotenv', mock_load_dotenv):
                    # Simulate environment after dotenv loading
                    env_vars = {
                        'POSTGRES_HOST': 'envfile-host',
                        'POSTGRES_PORT': '9999',
                        'POSTGRES_DB': 'envfile-db',
                        'POSTGRES_USER': 'envfile-user',
                        'POSTGRES_PASSWORD': 'envfile-pass'
                    }
                    with patch.dict(os.environ, env_vars, clear=True):
                        result = ConfigLoader.get_postgres_config()

                        assert result['host'] == 'envfile-host'
                        assert result['port'] == 9999
                        assert result['database'] == 'envfile-db'
                        assert result['user'] == 'envfile-user'
                        assert result['password'] == 'envfile-pass'

    def test_get_postgres_config_default_env_file(self):
        """Test get_postgres_config uses default .env file path."""
        config_data = {
            'version': '1.0',
            'targets': {
                'postgres': {
                    'output': './sql'
                    # No env_file specified - should default to .env
                }
            }
        }

        with patch.object(ConfigLoader, 'load_config', return_value=config_data):
            with patch('pathlib.Path.exists', return_value=False) as mock_exists:
                with patch.dict(os.environ, {}, clear=True):
                    ConfigLoader.get_postgres_config()

                    # Should check for .env file (default)
                    mock_exists.assert_called_once()
                    # Check that the Path object created ends with .env
                    call_args = mock_exists.call_args
                    path_arg = call_args[0][0] if call_args and call_args[0] else None
                    if path_arg:
                        assert str(path_arg).endswith('.env')

    def test_get_postgres_config_dotenv_import_error(self):
        """Test get_postgres_config handles missing python-dotenv gracefully."""
        config_data = {
            'version': '1.0',
            'targets': {
                'postgres': {
                    'env_file': '.env'
                }
            }
        }

        with patch.object(ConfigLoader, 'load_config', return_value=config_data):
            with patch('pathlib.Path.exists', return_value=True):
                with patch('dotenv.load_dotenv', side_effect=ImportError("No module named 'dotenv'")):
                    with patch.dict(os.environ, {'POSTGRES_HOST': 'fallback-host'}, clear=True):
                        result = ConfigLoader.get_postgres_config()

                        # Should fall back to environment variables
                        assert result['host'] == 'fallback-host'

    def test_get_postgres_config_explicit_config_param(self):
        """Test get_postgres_config with explicitly passed config."""
        config_data = {
            'version': '1.0',
            'targets': {
                'postgres': {
                    'env_file': 'explicit.env'
                }
            }
        }

        with patch('pathlib.Path.exists', return_value=False):
            with patch.dict(os.environ, {'POSTGRES_DB': 'explicit-db'}, clear=True):
                # Should not call load_config since config is passed explicitly
                with patch.object(ConfigLoader, 'load_config') as mock_load:
                    result = ConfigLoader.get_postgres_config(config_data)
                    mock_load.assert_not_called()
                    assert result['database'] == 'explicit-db'


@pytest.mark.unit
class TestPostgresConnectionTest:
    """Test the test_postgres_connection method."""

    def test_connection_success(self):
        """Test successful PostgreSQL connection."""
        mock_conn = Mock()
        mock_psycopg2 = Mock()
        mock_psycopg2.connect.return_value = mock_conn

        conn_params = {
            'host': 'localhost',
            'port': 5432,
            'database': 'testdb',
            'user': 'testuser',
            'password': 'testpass'
        }

        with patch.object(ConfigLoader, 'get_postgres_config', return_value=conn_params):
            with patch('psycopg2.connect', return_value=mock_conn):
                success, message = ConfigLoader.test_postgres_connection()

                assert success is True
                assert 'Connected to testuser@localhost:5432/testdb' in message
                mock_conn.close.assert_called_once()

    def test_connection_psycopg2_not_installed(self):
        """Test connection when psycopg2 is not installed."""
        with patch('psycopg2.connect', side_effect=ImportError("No module named 'psycopg2'")):
            success, message = ConfigLoader.test_postgres_connection()

            assert success is False
            assert 'psycopg2 not installed' in message
            assert 'pip install psycopg2-binary' in message

    def test_connection_operational_error(self):
        """Test connection when PostgreSQL connection fails."""
        # Create a custom exception class that behaves like OperationalError
        class MockOperationalError(Exception):
            pass

        operational_error = MockOperationalError("connection to server failed")

        conn_params = {
            'host': 'nonexistent',
            'port': 5432,
            'database': 'testdb',
            'user': 'testuser',
            'password': 'testpass'
        }

        with patch.object(ConfigLoader, 'get_postgres_config', return_value=conn_params):
            with patch('psycopg2.connect', side_effect=operational_error):
                with patch('psycopg2.OperationalError', MockOperationalError):
                    success, message = ConfigLoader.test_postgres_connection()

                    assert success is False
                    assert 'Connection failed' in message

    def test_connection_unexpected_error(self):
        """Test connection when unexpected error occurs."""
        conn_params = {
            'host': 'localhost',
            'port': 5432,
            'database': 'testdb',
            'user': 'testuser',
            'password': 'testpass'
        }

        with patch.object(ConfigLoader, 'get_postgres_config', return_value=conn_params):
            with patch('psycopg2.connect', side_effect=ValueError("Unexpected error")):
                success, message = ConfigLoader.test_postgres_connection()

                assert success is False
                assert 'Unexpected error' in message

    def test_connection_with_explicit_config(self):
        """Test connection with explicitly passed config."""
        config_data = {
            'version': '1.0',
            'targets': {
                'postgres': {
                    'env_file': 'test.env'
                }
            }
        }

        mock_conn = Mock()
        conn_params = {
            'host': 'explicit-host',
            'port': 5432,
            'database': 'explicit-db',
            'user': 'explicit-user',
            'password': 'explicit-pass'
        }

        with patch.object(ConfigLoader, 'get_postgres_config', return_value=conn_params) as mock_get_config:
            with patch('psycopg2.connect', return_value=mock_conn):
                success, message = ConfigLoader.test_postgres_connection(config_data)

                # Should pass the config to get_postgres_config
                mock_get_config.assert_called_once_with(config_data)
                assert success is True
                assert 'explicit-user@explicit-host:5432/explicit-db' in message