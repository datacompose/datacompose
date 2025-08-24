"""
Tests for ConfigLoader class and configuration management.
"""

import json
import tempfile
from pathlib import Path
from unittest.mock import mock_open, patch

import pytest

from datacompose.cli.config import ConfigLoader


@pytest.mark.unit
class TestConfigLoader:
    """Test the ConfigLoader class."""

    def test_load_config_file_not_found(self):
        """Test loading config when file doesn't exist."""
        with patch("pathlib.Path.exists", return_value=False):
            result = ConfigLoader.load_config()
            assert result is None

    def test_load_config_default_path(self):
        """Test loading config from default path."""
        config_data = {"version": "1.0", "default_target": "pyspark"}

        with patch("pathlib.Path.exists", return_value=True):
            with patch("builtins.open", mock_open(read_data=json.dumps(config_data))):
                result = ConfigLoader.load_config()
                assert result == config_data

    def test_load_config_custom_path(self):
        """Test loading config from custom path."""
        config_data = {"version": "1.0", "default_target": "postgres"}
        custom_path = Path("custom/config.json")

        with patch("pathlib.Path.exists", return_value=True):
            with patch("builtins.open", mock_open(read_data=json.dumps(config_data))):
                result = ConfigLoader.load_config(custom_path)
                assert result == config_data

    def test_load_config_invalid_json(self):
        """Test loading config with invalid JSON."""
        with patch("pathlib.Path.exists", return_value=True):
            with patch("builtins.open", mock_open(read_data="invalid json")):
                result = ConfigLoader.load_config()
                assert result is None

    def test_load_config_io_error(self):
        """Test loading config when IO error occurs."""
        with patch("pathlib.Path.exists", return_value=True):
            with patch("builtins.open", side_effect=IOError("File error")):
                result = ConfigLoader.load_config()
                assert result is None

    def test_get_default_target_explicit(self):
        """Test getting default target when explicitly set."""
        config = {
            "version": "1.0",
            "default_target": "pyspark",
            "targets": {
                "pyspark": {"output": "./transformers/pyspark"},
                "postgres": {"output": "./transformers/postgres"},
            },
        }
        result = ConfigLoader.get_default_target(config)
        assert result == "pyspark"

    def test_get_default_target_single_target(self):
        """Test getting default target when only one target exists."""
        config = {
            "version": "1.0",
            "targets": {"postgres": {"output": "./transformers/postgres"}},
        }
        result = ConfigLoader.get_default_target(config)
        assert result == "postgres"

    def test_get_default_target_multiple_no_default(self):
        """Test getting default target with multiple targets and no default."""
        config = {
            "version": "1.0",
            "targets": {
                "pyspark": {"output": "./transformers/pyspark"},
                "postgres": {"output": "./transformers/postgres"},
            },
        }
        result = ConfigLoader.get_default_target(config)
        assert result is None

    def test_get_default_target_no_config(self):
        """Test getting default target when config is None."""
        result = ConfigLoader.get_default_target(None)
        assert result == "pyspark"

    def test_get_default_target_no_targets(self):
        """Test getting default target when no targets defined."""
        config = {"version": "1.0"}
        result = ConfigLoader.get_default_target(config)
        assert result is None

    def test_get_default_target_loads_from_file(self):
        """Test that get_default_target loads config from file if not provided."""
        config_data = {"version": "1.0", "default_target": "snowflake"}

        with patch.object(
            ConfigLoader, "load_config", return_value=config_data
        ) as mock_load:
            result = ConfigLoader.get_default_target()
            assert result == "snowflake"
            mock_load.assert_called_once()

    def test_get_target_output_exists(self):
        """Test getting output directory for existing target."""
        config = {
            "version": "1.0",
            "targets": {
                "pyspark": {"output": "./transformers/pyspark"},
                "postgres": {"output": "./build/postgres"},
            },
        }
        result = ConfigLoader.get_target_output(config, "pyspark")
        assert result == "./transformers/pyspark"

    def test_get_target_output_not_exists(self):
        """Test getting output directory for non-existent target."""
        config = {
            "version": "1.0",
            "targets": {"pyspark": {"output": "./transformers/pyspark"}},
        }
        result = ConfigLoader.get_target_output(config, "postgres")
        assert result is None

    def test_get_target_output_no_config(self):
        """Test getting output directory when config is None."""
        result = ConfigLoader.get_target_output(None, "pyspark")
        assert result is None

    def test_get_target_output_no_targets(self):
        """Test getting output directory when no targets defined."""
        config = {"version": "1.0"}
        result = ConfigLoader.get_target_output(config, "pyspark")
        assert result is None

    def test_get_target_output_no_output_defined(self):
        """Test getting output when target exists but has no output defined."""
        config = {"version": "1.0", "targets": {"pyspark": {}}}
        result = ConfigLoader.get_target_output(config, "pyspark")
        assert result is None


@pytest.mark.unit
class TestConfigLoaderIntegration:
    """Integration tests for ConfigLoader with actual file operations."""

    def test_config_roundtrip(self, tmp_path):
        """Test writing and reading a config file."""
        config_data = {
            "version": "1.0",
            "default_target": "pyspark",
            "targets": {"pyspark": {"output": "./transformers/pyspark"}},
        }

        config_file = tmp_path / "datacompose.json"
        with open(config_file, "w") as f:
            json.dump(config_data, f)

        # Test loading with explicit path
        loaded = ConfigLoader.load_config(config_file)
        assert loaded == config_data

        # Test getting default target
        assert ConfigLoader.get_default_target(loaded) == "pyspark"

        # Test getting target output
        assert (
            ConfigLoader.get_target_output(loaded, "pyspark")
            == "./transformers/pyspark"
        )

    def test_config_with_multiple_targets(self, tmp_path):
        """Test config with multiple targets and explicit default."""
        config_data = {
            "version": "1.0",
            "default_target": "postgres",
            "targets": {
                "pyspark": {"output": "./transformers/pyspark"},
                "postgres": {"output": "./transformers/postgres"},
                "snowflake": {"output": "./transformers/snowflake"},
            },
        }

        config_file = tmp_path / "datacompose.json"
        with open(config_file, "w") as f:
            json.dump(config_data, f)

        loaded = ConfigLoader.load_config(config_file)

        # Should use explicit default even with multiple targets
        assert ConfigLoader.get_default_target(loaded) == "postgres"

        # Should get correct output for each target
        assert (
            ConfigLoader.get_target_output(loaded, "pyspark")
            == "./transformers/pyspark"
        )
        assert (
            ConfigLoader.get_target_output(loaded, "postgres")
            == "./transformers/postgres"
        )
        assert (
            ConfigLoader.get_target_output(loaded, "snowflake")
            == "./transformers/snowflake"
        )

    def test_config_auto_default_single_target(self, tmp_path):
        """Test automatic default selection with single target."""
        config_data = {
            "version": "1.0",
            "targets": {"databricks": {"output": "./build/databricks"}},
        }

        config_file = tmp_path / "datacompose.json"
        with open(config_file, "w") as f:
            json.dump(config_data, f)

        loaded = ConfigLoader.load_config(config_file)

        # Should automatically use single target as default
        assert ConfigLoader.get_default_target(loaded) == "databricks"

    def test_config_priority_explicit_over_single(self, tmp_path):
        """Test that explicit default takes priority over single target rule."""
        config_data = {
            "version": "1.0",
            "default_target": "custom",
            "targets": {"pyspark": {"output": "./transformers/pyspark"}},
        }

        config_file = tmp_path / "datacompose.json"
        with open(config_file, "w") as f:
            json.dump(config_data, f)

        loaded = ConfigLoader.load_config(config_file)

        # Should use explicit default even if it doesn't exist in targets
        # (This tests that explicit default is returned as-is)
        assert ConfigLoader.get_default_target(loaded) == "custom"
