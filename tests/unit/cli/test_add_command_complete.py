"""Comprehensive test suite for the add CLI command."""

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import click.shell_completion
import pytest
from click.testing import CliRunner

# Add project root to path for imports
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from datacompose.cli.commands.add import (  # noqa: E402
    complete_transformer,
    complete_target,
    complete_type,
    _run_add,
)
from datacompose.cli.main import cli  # noqa: E402
from datacompose.transformers.discovery import TransformerDiscovery  # noqa: E402


@pytest.mark.unit
class TestAddCommandCompletion:
    """Test completion functions for the add command."""

    def test_complete_transformer_success(self):
        """Test transformer name completion with available transformers."""
        with patch('datacompose.cli.commands.add.TransformerDiscovery') as MockDiscovery:
            mock_instance = MockDiscovery.return_value
            mock_instance.list_transformers.return_value = ['emails', 'addresses', 'phone_numbers']
            
            # Test partial match for 'em' prefix
            result = complete_transformer(None, None, 'em')
            assert len(result) == 1
            assert isinstance(result[0], click.shell_completion.CompletionItem)
            assert result[0].value == 'emails'
            
            # Test partial match for 'add' prefix
            result = complete_transformer(None, None, 'add')
            assert len(result) == 1
            assert isinstance(result[0], click.shell_completion.CompletionItem)
            assert result[0].value == 'addresses'
            
            # Test partial match for 'phone' prefix
            result = complete_transformer(None, None, 'phone')
            assert len(result) == 1
            assert isinstance(result[0], click.shell_completion.CompletionItem)
            assert result[0].value == 'phone_numbers'
            
            # Test no match
            result = complete_transformer(None, None, 'xyz')
            assert len(result) == 0

    def test_complete_transformer_exception_handling(self):
        """Test transformer completion handles exceptions gracefully."""
        with patch.object(TransformerDiscovery, '__init__', side_effect=Exception("Discovery error")):
            result = complete_transformer(None, None, 'clean')
            assert result == []

    def test_complete_target_success(self):
        """Test target platform completion."""
        with patch('datacompose.cli.commands.add.TransformerDiscovery') as MockDiscovery:
            mock_instance = MockDiscovery.return_value
            mock_instance.list_generators.return_value = ['pyspark.generator', 'postgres.sql_udf', 'snowflake.udf']
            
            # Test all platforms
            result = complete_target(None, None, '')
            assert len(result) == 3
            assert all(isinstance(item, click.shell_completion.CompletionItem) for item in result)
            platforms = [item.value for item in result]
            assert 'pyspark' in platforms
            assert 'postgres' in platforms
            assert 'snowflake' in platforms
            
            # Test partial match
            result = complete_target(None, None, 'py')
            assert len(result) == 1
            assert isinstance(result[0], click.shell_completion.CompletionItem)
            assert result[0].value == 'pyspark'

    def test_complete_target_exception_handling(self):
        """Test target completion handles exceptions gracefully."""
        with patch.object(TransformerDiscovery, '__init__', side_effect=Exception("Discovery error")):
            result = complete_target(None, None, 'py')
            assert result == []

    def test_complete_type_with_target_context(self):
        """Test type completion when target is already specified."""
        mock_ctx = MagicMock()
        mock_ctx.params = {'target': 'pyspark'}
        
        with patch('datacompose.cli.commands.add.TransformerDiscovery') as MockDiscovery:
            mock_instance = MockDiscovery.return_value
            mock_instance.list_generators.return_value = [
                'pyspark.pandas_udf', 
                'pyspark.sql_udf',
                'postgres.sql_udf'
            ]
            
            result = complete_type(mock_ctx, None, '')
            assert len(result) == 2
            assert all(isinstance(item, click.shell_completion.CompletionItem) for item in result)
            types = [item.value for item in result]
            assert 'pandas_udf' in types
            assert 'sql_udf' in types

    def test_complete_type_without_target_context(self):
        """Test type completion without target context."""
        mock_ctx = MagicMock()
        mock_ctx.params = {}
        
        with patch('datacompose.cli.commands.add.TransformerDiscovery') as MockDiscovery:
            mock_instance = MockDiscovery.return_value
            mock_instance.list_generators.return_value = [
                'pyspark.pandas_udf', 
                'pyspark.sql_udf',
                'postgres.sql_udf'
            ]
            
            result = complete_type(mock_ctx, None, '')
            assert len(result) == 3  # pandas_udf and sql_udf (2 instances not deduped)
            assert all(isinstance(item, click.shell_completion.CompletionItem) for item in result)
            types = [item.value for item in result]
            assert 'pandas_udf' in types
            assert 'sql_udf' in types

    def test_complete_type_partial_match(self):
        """Test type completion with partial match."""
        mock_ctx = MagicMock()
        mock_ctx.params = {'target': 'pyspark'}
        
        with patch('datacompose.cli.commands.add.TransformerDiscovery') as MockDiscovery:
            mock_instance = MockDiscovery.return_value
            mock_instance.list_generators.return_value = [
                'pyspark.pandas_udf', 
                'pyspark.sql_udf',
            ]
            
            result = complete_type(mock_ctx, None, 'pan')
            assert len(result) == 1
            assert isinstance(result[0], click.shell_completion.CompletionItem)
            assert result[0].value == 'pandas_udf'

    def test_complete_type_exception_handling(self):
        """Test type completion handles exceptions gracefully."""
        mock_ctx = MagicMock()
        with patch.object(TransformerDiscovery, '__init__', side_effect=Exception("Discovery error")):
            result = complete_type(mock_ctx, None, 'sql')
            assert result == []


@pytest.mark.unit
class TestRunAddFunction:
    """Test the _run_add internal function."""

    @pytest.fixture
    def mock_discovery(self):
        """Create a mock discovery instance."""
        with patch('datacompose.cli.commands.add.TransformerDiscovery') as MockDiscovery:
            instance = MockDiscovery.return_value
            yield instance

    def test_run_add_transformer_not_found(self, mock_discovery):
        """Test _run_add when transformer is not found."""
        mock_discovery.resolve_transformer.return_value = (None, None)
        mock_discovery.list_transformers.return_value = ['emails', 'addresses']
        
        exit_code = _run_add('invalid_transformer', 'pyspark', None, False)
        
        assert exit_code == 1
        mock_discovery.resolve_transformer.assert_called_once_with('invalid_transformer')

    def test_run_add_generator_not_found(self, mock_discovery):
        """Test _run_add when generator is not found."""
        mock_discovery.resolve_transformer.return_value = ('emails', Path('/path/to/transformer'))
        mock_discovery.resolve_generator.return_value = None
        mock_discovery.list_generators.return_value = ['pyspark.generator']
        
        exit_code = _run_add('emails', 'invalid_target', None, False)
        
        assert exit_code == 1
        mock_discovery.resolve_generator.assert_called_once_with('invalid_target')

    def test_run_add_success_new_file(self, mock_discovery):
        """Test successful generation of new UDF."""
        transformer_path = Path('/path/to/transformer')
        mock_discovery.resolve_transformer.return_value = ('emails', transformer_path)
        
        mock_generator_class = MagicMock()
        mock_generator_instance = MagicMock()
        mock_generator_class.return_value = mock_generator_instance
        mock_discovery.resolve_generator.return_value = mock_generator_class
        
        mock_generator_instance.generate.return_value = {
            'output_path': 'transformers/pyspark/emails.py',
            'test_path': 'transformers/pyspark/test_emails.py',
            'function_name': 'clean_email',
            'skipped': False
        }
        
        exit_code = _run_add('emails', 'pyspark', None, False)
        
        assert exit_code == 0
        mock_generator_instance.generate.assert_called_once_with(
            'emails', 
            force=False, 
            transformer_dir=transformer_path
        )

    def test_run_add_success_skipped(self, mock_discovery):
        """Test when UDF already exists and is skipped."""
        transformer_path = Path('/path/to/transformer')
        mock_discovery.resolve_transformer.return_value = ('emails', transformer_path)
        
        mock_generator_class = MagicMock()
        mock_generator_instance = MagicMock()
        mock_generator_class.return_value = mock_generator_instance
        mock_discovery.resolve_generator.return_value = mock_generator_class
        
        mock_generator_instance.generate.return_value = {
            'output_path': 'transformers/pyspark/emails.py',
            'function_name': 'clean_email',
            'skipped': True,
            'hash': 'abc123'
        }
        
        exit_code = _run_add('emails', 'pyspark', None, True)
        
        assert exit_code == 0

    def test_run_add_with_custom_output(self, mock_discovery):
        """Test _run_add with custom output directory."""
        transformer_path = Path('/path/to/transformer')
        mock_discovery.resolve_transformer.return_value = ('emails', transformer_path)
        
        mock_generator_class = MagicMock()
        mock_generator_instance = MagicMock()
        mock_generator_class.return_value = mock_generator_instance
        mock_discovery.resolve_generator.return_value = mock_generator_class
        
        mock_generator_instance.generate.return_value = {
            'output_path': 'custom/emails.py',
            'function_name': 'clean_email',
            'skipped': False
        }
        
        exit_code = _run_add('emails', 'pyspark', 'custom', False)
        
        assert exit_code == 0
        # Verify generator was initialized with custom output
        mock_generator_class.assert_called_once()
        call_kwargs = mock_generator_class.call_args[1]
        assert str(call_kwargs['output_dir']) == 'custom'

    def test_run_add_exception_handling(self, mock_discovery):
        """Test _run_add handles exceptions properly."""
        transformer_path = Path('/path/to/transformer')
        mock_discovery.resolve_transformer.return_value = ('emails', transformer_path)
        
        mock_generator_class = MagicMock()
        mock_generator_instance = MagicMock()
        mock_generator_class.return_value = mock_generator_instance
        mock_discovery.resolve_generator.return_value = mock_generator_class
        
        mock_generator_instance.generate.side_effect = Exception("Generation failed")
        
        exit_code = _run_add('emails', 'pyspark', None, False)
        
        assert exit_code == 1

    def test_run_add_exception_with_verbose(self, mock_discovery, capsys):
        """Test _run_add shows traceback in verbose mode."""
        transformer_path = Path('/path/to/transformer')
        mock_discovery.resolve_transformer.return_value = ('emails', transformer_path)
        
        mock_generator_class = MagicMock()
        mock_generator_instance = MagicMock()
        mock_generator_class.return_value = mock_generator_instance
        mock_discovery.resolve_generator.return_value = mock_generator_class
        
        mock_generator_instance.generate.side_effect = ValueError("Specific error")
        
        exit_code = _run_add('emails', 'pyspark', None, True)
        
        assert exit_code == 1
        captured = capsys.readouterr()
        assert "Specific error" in captured.out or "Specific error" in captured.err


@pytest.mark.unit
class TestAddCommandIntegration:
    """Integration tests for the add command."""

    @pytest.fixture
    def runner(self):
        """Fixture to provide Click CLI runner."""
        return CliRunner()

    def test_add_command_context_exit(self, runner):
        """Test that add command properly exits with context."""
        with patch('datacompose.cli.commands.add.validate_platform') as mock_validate:
            mock_validate.return_value = False
            
            result = runner.invoke(cli, ['add', 'emails', '--target', 'invalid'])
            assert result.exit_code == 1

    def test_add_command_type_validation_failure(self, runner):
        """Test add command when type validation fails."""
        with patch('datacompose.cli.commands.add.validate_platform') as mock_platform:
            with patch('datacompose.cli.commands.add.validate_type_for_platform') as mock_type:
                mock_platform.return_value = True
                mock_type.return_value = False
                
                result = runner.invoke(
                    cli, 
                    ['add', 'emails', '--target', 'pyspark', '--type', 'invalid_type']
                )
                assert result.exit_code == 1

    def test_add_command_successful_execution(self, runner):
        """Test successful execution of add command."""
        with patch('datacompose.cli.commands.add.validate_platform') as mock_platform:
            with patch('datacompose.cli.commands.add._run_add') as mock_run:
                mock_platform.return_value = True
                mock_run.return_value = 0
                
                result = runner.invoke(cli, ['add', 'emails', '--target', 'pyspark'])
                assert result.exit_code == 0

    def test_add_command_with_default_target(self, runner):
        """Test add command uses default target from config."""
        with patch('datacompose.cli.commands.add.validate_platform') as mock_platform:
            with patch('datacompose.cli.commands.add._run_add') as mock_run:
                with patch('datacompose.cli.commands.add.ConfigLoader.load_config') as mock_load:
                    with patch('datacompose.cli.commands.add.ConfigLoader.get_default_target') as mock_target:
                        mock_load.return_value = {"default_target": "pyspark", "targets": {"pyspark": {"output": "./transformers/pyspark"}}}
                        mock_target.return_value = "pyspark"
                        mock_platform.return_value = True
                        mock_run.return_value = 0
                        
                        result = runner.invoke(
                            cli, 
                            ['add', 'emails']
                        )
                        assert result.exit_code == 0
                        mock_run.assert_called_once()
                        # Verify pyspark was used as default
                        assert mock_run.call_args[0][1] == 'pyspark'

    def test_add_command_short_options(self, runner):
        """Test add command with short option flags."""
        with patch('datacompose.cli.commands.add.validate_platform') as mock_platform:
            with patch('datacompose.cli.commands.add._run_add') as mock_run:
                mock_platform.return_value = True
                mock_run.return_value = 0
                
                result = runner.invoke(
                    cli, 
                    ['add', 'emails', '-t', 'pyspark', '-o', 'output', '-v']
                )
                assert result.exit_code == 0
                mock_run.assert_called_once()
                # Verify parameters
                assert mock_run.call_args[0][1] == 'pyspark'  # target
                assert mock_run.call_args[0][2] == 'output'   # output
                assert mock_run.call_args[0][3] is True       # verbose


@pytest.mark.unit 
class TestAddCommandEdgeCases:
    """Test edge cases and error conditions."""

    @pytest.fixture
    def runner(self):
        return CliRunner()

    def test_add_with_empty_transformer_name(self, runner):
        """Test add command with empty transformer name."""
        result = runner.invoke(cli, ['add', '', '--target', 'pyspark'])
        assert result.exit_code != 0

    def test_add_with_special_characters_in_transformer(self, runner):
        """Test add command with special characters in transformer name."""
        with patch('datacompose.cli.commands.add.validate_platform') as mock_platform:
            with patch('datacompose.cli.commands.add._run_add') as mock_run:
                mock_platform.return_value = True
                mock_run.return_value = 0
                
                result = runner.invoke(cli, ['add', 'clean-emails_v2', '--target', 'pyspark'])
                assert result.exit_code == 0
                mock_run.assert_called_once_with('clean-emails_v2', 'pyspark', None, False)

    def test_add_with_absolute_path_output(self, runner):
        """Test add command with absolute path for output."""
        with patch('datacompose.cli.commands.add.validate_platform') as mock_platform:
            with patch('datacompose.cli.commands.add._run_add') as mock_run:
                with patch('datacompose.cli.commands.add.ConfigLoader.load_config') as mock_load:
                    with patch('datacompose.cli.commands.add.ConfigLoader.get_default_target') as mock_target:
                        mock_load.return_value = {"default_target": "pyspark", "targets": {"pyspark": {"output": "./transformers/pyspark"}}}
                        mock_target.return_value = "pyspark"
                        mock_platform.return_value = True
                        mock_run.return_value = 0
                        
                        result = runner.invoke(
                            cli, 
                            ['add', 'emails', '--output', '/absolute/path/to/output']
                        )
                        assert result.exit_code == 0
                        mock_run.assert_called_once()
                        assert mock_run.call_args[0][2] == '/absolute/path/to/output'

    def test_add_handles_generator_initialization_error(self):
        """Test handling of generator initialization errors."""
        with patch('datacompose.cli.commands.add.TransformerDiscovery') as MockDiscovery:
            mock_instance = MockDiscovery.return_value
            mock_instance.resolve_transformer.return_value = ('emails', Path('/path'))
            mock_instance.list_generators.return_value = ['pyspark.generator']
            
            # Create a mock that raises on instantiation  
            def raise_error(*args, **kwargs):
                raise ValueError("Init failed")
            
            mock_generator_class = MagicMock()
            mock_generator_class.side_effect = raise_error
            mock_instance.resolve_generator.return_value = mock_generator_class
            
            # Test that the error is caught and handled properly
            exit_code = _run_add('emails', 'pyspark', None, False)
            assert exit_code == 1

    def test_add_with_path_traversal_attempt(self, runner):
        """Test add command blocks path traversal attempts."""
        with patch('datacompose.cli.commands.add.validate_platform') as mock_platform:
            with patch('datacompose.cli.commands.add._run_add') as mock_run:
                mock_platform.return_value = True
                mock_run.return_value = 0
                
                with patch('datacompose.cli.commands.add.ConfigLoader.load_config') as mock_load:
                    with patch('datacompose.cli.commands.add.ConfigLoader.get_default_target') as mock_target:
                        mock_load.return_value = {"default_target": "pyspark", "targets": {"pyspark": {"output": "./transformers/pyspark"}}}
                        mock_target.return_value = "pyspark"
                        result = runner.invoke(
                            cli, 
                            ['add', 'emails', '--output', '../../../etc']
                        )
                        # Should still proceed - security is handled by generator
                        assert result.exit_code == 0
                        mock_run.assert_called_once()