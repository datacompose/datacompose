"""
Tests for add command's default target behavior from datacompose.json.
"""

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from datacompose.cli.main import cli


@pytest.mark.unit
class TestAddCommandDefaultTarget:
    """Test add command's default target behavior."""

    @pytest.fixture
    def runner(self):
        """Provide Click CLI runner."""
        return CliRunner()

    def test_add_uses_default_target_from_config(self, runner):
        """Test that add command uses default_target from datacompose.json."""
        with runner.isolated_filesystem():
            # Create config with explicit default_target
            config = {
                "version": "1.0",
                "default_target": "pyspark",
                "targets": {
                    "pyspark": {"output": "./transformers/pyspark"},
                    "postgres": {"output": "./transformers/postgres"}
                }
            }
            with open("datacompose.json", "w") as f:
                json.dump(config, f)
            
            with patch('datacompose.cli.commands.add._run_add') as mock_run:
                mock_run.return_value = 0
                
                # Run without specifying --target
                result = runner.invoke(cli, ['add', 'emails'])
                
                assert result.exit_code == 0
                # Verify pyspark was used as target
                mock_run.assert_called_once()
                assert mock_run.call_args[0][1] == 'pyspark'  # target parameter

    def test_add_auto_default_with_single_target(self, runner):
        """Test that add uses single target as default when only one exists."""
        with runner.isolated_filesystem():
            # Create config with single target, no explicit default
            config = {
                "version": "1.0",
                "targets": {
                    "snowflake": {"output": "./transformers/snowflake"}
                }
            }
            with open("datacompose.json", "w") as f:
                json.dump(config, f)
            
            # Mock validation to allow snowflake platform
            with patch('datacompose.cli.commands.add.validate_platform', return_value=True):
                with patch('datacompose.cli.commands.add._run_add') as mock_run:
                    mock_run.return_value = 0
                    
                    result = runner.invoke(cli, ['add', 'addresses'])
                    
                    assert result.exit_code == 0
                    # Should use snowflake as it's the only target
                    mock_run.assert_called_once()
                    assert mock_run.call_args[0][1] == 'snowflake'

    def test_add_fails_no_default_multiple_targets(self, runner):
        """Test that add fails when no default and multiple targets exist."""
        with runner.isolated_filesystem():
            # Create config with multiple targets, no default
            config = {
                "version": "1.0",
                "targets": {
                    "pyspark": {"output": "./transformers/pyspark"},
                    "postgres": {"output": "./transformers/postgres"}
                }
            }
            with open("datacompose.json", "w") as f:
                json.dump(config, f)
            
            result = runner.invoke(cli, ['add', 'emails'])
            
            assert result.exit_code == 1
            assert "No target specified" in result.output
            assert "no default target found" in result.output

    def test_add_fails_no_config_file(self, runner):
        """Test that add fails gracefully when no config file exists."""
        with runner.isolated_filesystem():
            # No datacompose.json file
            
            result = runner.invoke(cli, ['add', 'emails'])
            
            assert result.exit_code == 1
            assert "No target specified" in result.output
            assert "run 'datacompose init'" in result.output

    def test_add_explicit_target_overrides_default(self, runner):
        """Test that explicit --target overrides default from config."""
        with runner.isolated_filesystem():
            # Create config with default_target
            config = {
                "version": "1.0",
                "default_target": "pyspark",
                "targets": {
                    "pyspark": {"output": "./transformers/pyspark"},
                    "postgres": {"output": "./transformers/postgres"}
                }
            }
            with open("datacompose.json", "w") as f:
                json.dump(config, f)
            
            # Mock validation to allow postgres platform
            with patch('datacompose.cli.commands.add.validate_platform', return_value=True):
                with patch('datacompose.cli.commands.add._run_add') as mock_run:
                    mock_run.return_value = 0
                    
                    # Explicitly specify postgres even though default is pyspark
                    result = runner.invoke(cli, ['add', 'emails', '--target', 'postgres'])
                    
                    assert result.exit_code == 0
                    # Should use postgres, not the default pyspark
                    mock_run.assert_called_once()
                    assert mock_run.call_args[0][1] == 'postgres'

    def test_add_uses_config_output_path(self, runner):
        """Test that add uses output path from config for target."""
        with runner.isolated_filesystem():
            # Create config with custom output path
            config = {
                "version": "1.0",
                "default_target": "pyspark",
                "targets": {
                    "pyspark": {"output": "./custom/build/path"}
                }
            }
            with open("datacompose.json", "w") as f:
                json.dump(config, f)
            
            with patch('datacompose.cli.commands.add._run_add') as mock_run:
                mock_run.return_value = 0
                
                result = runner.invoke(cli, ['add', 'emails'])
                
                assert result.exit_code == 0
                mock_run.assert_called_once()
                # Check that custom output path was used
                assert mock_run.call_args[0][2] is None  # output not explicitly set in command

    def test_add_verbose_shows_default_target_used(self, runner):
        """Test that verbose mode shows when default target is used."""
        with runner.isolated_filesystem():
            config = {
                "version": "1.0",
                "default_target": "pyspark",
                "targets": {
                    "pyspark": {"output": "./transformers/pyspark"}
                }
            }
            with open("datacompose.json", "w") as f:
                json.dump(config, f)
            
            with patch('datacompose.cli.commands.add._run_add') as mock_run:
                mock_run.return_value = 0
                
                result = runner.invoke(cli, ['add', 'emails', '--verbose'])
                
                assert result.exit_code == 0
                assert "Using default target from config: pyspark" in result.output

    def test_add_empty_targets_in_config(self, runner):
        """Test add behavior when config has empty targets."""
        with runner.isolated_filesystem():
            config = {
                "version": "1.0",
                "default_target": "pyspark",
                "targets": {}
            }
            with open("datacompose.json", "w") as f:
                json.dump(config, f)
            
            with patch('datacompose.cli.commands.add._run_add') as mock_run:
                mock_run.return_value = 0
                
                # Even with default_target set, should use it
                result = runner.invoke(cli, ['add', 'emails'])
                
                assert result.exit_code == 0
                mock_run.assert_called_once()
                assert mock_run.call_args[0][1] == 'pyspark'

    def test_add_invalid_default_target_in_config(self, runner):
        """Test that invalid default target is still passed to validation."""
        with runner.isolated_filesystem():
            config = {
                "version": "1.0",
                "default_target": "invalid_platform",
                "targets": {
                    "pyspark": {"output": "./transformers/pyspark"}
                }
            }
            with open("datacompose.json", "w") as f:
                json.dump(config, f)
            
            # The validation will fail, but we should see the attempt
            result = runner.invoke(cli, ['add', 'emails'])
            
            assert result.exit_code == 1
            # Should fail validation for invalid platform
            assert "Platform 'invalid_platform' not found" in result.output

    def test_add_malformed_config_file(self, runner):
        """Test add behavior with malformed JSON config."""
        with runner.isolated_filesystem():
            # Write invalid JSON
            with open("datacompose.json", "w") as f:
                f.write("{ invalid json }")
            
            result = runner.invoke(cli, ['add', 'emails'])
            
            assert result.exit_code == 1
            assert "No target specified" in result.output


@pytest.mark.unit
class TestAddCommandOutputFromConfig:
    """Test add command's output directory behavior from config."""

    @pytest.fixture
    def runner(self):
        return CliRunner()

    def test_add_uses_target_output_from_config(self, runner):
        """Test that add uses output path from target config."""
        with runner.isolated_filesystem():
            config = {
                "version": "1.0",
                "default_target": "pyspark",
                "targets": {
                    "pyspark": {"output": "./my-custom-build"}
                }
            }
            with open("datacompose.json", "w") as f:
                json.dump(config, f)
            
            # Mock validation and discovery
            with patch('datacompose.cli.commands.add.validate_platform', return_value=True):
                with patch('datacompose.cli.commands.add.TransformerDiscovery') as MockDiscovery:
                    mock_instance = MockDiscovery.return_value
                    mock_instance.resolve_transformer.return_value = ('emails', Path('/path'))
                    
                    # Create a mock generator class
                    mock_generator_class = MagicMock()
                    mock_generator_instance = mock_generator_class.return_value
                    mock_generator_instance.generate.return_value = {
                        'output_path': 'my-custom-build/emails.py',
                        'function_name': 'emails_udf',
                        'skipped': False
                    }
                    mock_instance.resolve_generator.return_value = mock_generator_class
                    
                    result = runner.invoke(cli, ['add', 'emails'])
                    
                    # Should succeed and use custom output
                    assert result.exit_code == 0
                    assert "my-custom-build" in result.output

    def test_add_explicit_output_overrides_config(self, runner):
        """Test that explicit --output overrides config output."""
        with runner.isolated_filesystem():
            config = {
                "version": "1.0",
                "default_target": "pyspark",
                "targets": {
                    "pyspark": {"output": "./config-output"}
                }
            }
            with open("datacompose.json", "w") as f:
                json.dump(config, f)
            
            with patch('datacompose.cli.commands.add._run_add') as mock_run:
                mock_run.return_value = 0
                
                result = runner.invoke(cli, ['add', 'emails', '--output', './explicit-output'])
                
                assert result.exit_code == 0
                mock_run.assert_called_once()
                # Should use explicit output, not config
                assert mock_run.call_args[0][2] == './explicit-output'

    def test_add_fallback_output_when_not_in_config(self, runner):
        """Test that add falls back to transformers/{target} when no output in config."""
        with runner.isolated_filesystem():
            config = {
                "version": "1.0",
                "default_target": "pyspark",
                "targets": {
                    "pyspark": {}  # No output specified
                }
            }
            with open("datacompose.json", "w") as f:
                json.dump(config, f)
            
            # Mock validation and discovery
            with patch('datacompose.cli.commands.add.validate_platform', return_value=True):
                with patch('datacompose.cli.commands.add.TransformerDiscovery') as MockDiscovery:
                    mock_instance = MockDiscovery.return_value
                    mock_instance.resolve_transformer.return_value = ('emails', Path('/path'))
                    
                    mock_generator_class = MagicMock()
                    mock_instance.resolve_generator.return_value = mock_generator_class
                    
                    result = runner.invoke(cli, ['add', 'emails'])
                    
                    # Should use fallback path
                    assert result.exit_code == 0
                    mock_generator_class.assert_called_once()
                call_kwargs = mock_generator_class.call_args[1]
                assert str(call_kwargs['output_dir']) == 'transformers/pyspark'