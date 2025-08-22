"""Comprehensive test suite for the init CLI command."""

import json
import os
import sys
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch, call, mock_open
import pytest
from click.testing import CliRunner

# Add project root to path for imports
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from datacompose.cli.commands.init import (  # noqa: E402
    InitCommand,
    _run_init,
    DEFAULT_CONFIG,
    init,
)
from datacompose.cli.main import cli  # noqa: E402


@pytest.mark.unit
class TestInitCommand:
    """Test the InitCommand class methods."""

    def test_get_config_template_default(self):
        """Test getting the default config template."""
        config = InitCommand.get_config_template("default")
        assert config == DEFAULT_CONFIG
        assert config["version"] == "1.0"
        assert "targets" in config
        assert "pyspark" in config["targets"]

    def test_get_config_template_minimal(self):
        """Test getting the minimal config template."""
        config = InitCommand.get_config_template("minimal")
        assert config["version"] == "1.0"
        assert "targets" in config
        assert "pyspark" in config["targets"]
        assert config["targets"]["pyspark"]["output"] == "./transformers/pyspark"
        assert "aliases" not in config

    def test_get_config_template_advanced(self):
        """Test getting the advanced config template."""
        config = InitCommand.get_config_template("advanced")
        assert config["version"] == "1.0"
        assert "targets" in config
        assert "pyspark" in config["targets"]
        assert "aliases" in config
        assert config["aliases"]["utils"] == "./src/utils"
        assert config["aliases"]["transformers"] == "./transformers"
        assert "style" in config
        assert config["style"] == "custom"
        assert "include" in config
        assert "exclude" in config
        assert "testing" in config
        assert config["testing"]["framework"] == "pytest"

    def test_get_config_template_unknown(self):
        """Test getting config template with unknown name returns default."""
        config = InitCommand.get_config_template("unknown")
        assert config == DEFAULT_CONFIG

    @patch('sys.stdin')
    @patch('termios.tcgetattr')
    @patch('termios.tcsetattr')
    @patch('tty.setraw')
    def test_get_key_normal(self, mock_setraw, mock_tcsetattr, mock_tcgetattr, mock_stdin):
        """Test get_key method for normal key press."""
        mock_stdin.fileno.return_value = 0
        mock_stdin.read.return_value = 'a'
        mock_tcgetattr.return_value = []
        
        key = InitCommand.get_key()
        assert key == 'a'
        mock_setraw.assert_called_once()
        mock_tcsetattr.assert_called_once()

    @patch('sys.stdin')
    @patch('termios.tcgetattr')
    @patch('termios.tcsetattr')
    @patch('tty.setraw')
    def test_get_key_arrow(self, mock_setraw, mock_tcsetattr, mock_tcgetattr, mock_stdin):
        """Test get_key method for arrow key (escape sequence)."""
        mock_stdin.fileno.return_value = 0
        mock_stdin.read.side_effect = ['\x1b', '[A']
        mock_tcgetattr.return_value = []
        
        key = InitCommand.get_key()
        assert key == '\x1b[A'  # Up arrow

    @patch('sys.stdin')
    @patch('termios.tcgetattr', side_effect=Exception("No termios"))
    def test_get_key_fallback(self, mock_tcgetattr, mock_stdin):
        """Test get_key fallback when termios is not available."""
        with patch('builtins.input', return_value='test'):
            key = InitCommand.get_key()
            assert key == 'test'

    def test_create_directory_structure_no_targets(self):
        """Test create_directory_structure with no targets."""
        config = {"version": "1.0"}
        original_dir = os.getcwd()
        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                os.chdir(tmpdir)
                InitCommand.create_directory_structure(config, verbose=False)
                # Should not crash, just do nothing
        finally:
            os.chdir(original_dir)

    def test_create_directory_structure_with_targets(self):
        """Test create_directory_structure with target directories."""
        config = {
            "version": "1.0",
            "targets": {
                "pyspark": {"output": "./transformers/pyspark"},
                "postgres": {"output": "./transformers/postgres"}
            }
        }
        original_dir = os.getcwd()
        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                os.chdir(tmpdir)
                InitCommand.create_directory_structure(config, verbose=True)
                assert Path("transformers").exists()
        finally:
            os.chdir(original_dir)

    def test_create_directory_structure_verbose(self):
        """Test create_directory_structure with verbose output."""
        config = {
            "version": "1.0",
            "targets": {
                "pyspark": {"output": "./build/pyspark"}
            }
        }
        original_dir = os.getcwd()
        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                os.chdir(tmpdir)
                with patch('builtins.print') as mock_print:
                    InitCommand.create_directory_structure(config, verbose=True)
                    # Check if verbose message was printed
                    calls = [str(call) for call in mock_print.call_args_list]
                    assert any("Created directory" in str(call) for call in calls)
        finally:
            os.chdir(original_dir)

    @patch.dict(os.environ, {'SHELL': '/bin/bash'})
    @patch('pathlib.Path.exists')
    @patch('builtins.open', new_callable=mock_open)
    def test_setup_shell_completion_bash(self, mock_file, mock_exists):
        """Test shell completion setup for bash."""
        mock_exists.return_value = True
        mock_file.return_value.read.return_value = "# Existing content\n"
        
        result = InitCommand.setup_shell_completion(verbose=True)
        assert result is True
        
        # Check that completion line was written
        written_content = ''.join(call.args[0] for call in mock_file().write.call_args_list)
        assert 'register-python-argcomplete datacompose' in written_content

    @patch.dict(os.environ, {'SHELL': '/bin/zsh'})
    @patch('pathlib.Path.exists')
    @patch('builtins.open', new_callable=mock_open)
    def test_setup_shell_completion_zsh(self, mock_file, mock_exists):
        """Test shell completion setup for zsh."""
        mock_exists.return_value = True
        mock_file.return_value.read.return_value = "# Existing content\n"
        
        result = InitCommand.setup_shell_completion(verbose=True)
        assert result is True

    @patch.dict(os.environ, {'SHELL': '/bin/fish'})
    def test_setup_shell_completion_unsupported_shell(self):
        """Test shell completion setup with unsupported shell."""
        result = InitCommand.setup_shell_completion(verbose=True)
        assert result is False

    @patch.dict(os.environ, {'SHELL': '/bin/bash'})
    @patch('pathlib.Path.exists', return_value=False)
    def test_setup_shell_completion_no_config_file(self, mock_exists):
        """Test shell completion when config file doesn't exist."""
        result = InitCommand.setup_shell_completion(verbose=True)
        assert result is False

    @patch.dict(os.environ, {'SHELL': '/bin/bash'})
    @patch('pathlib.Path.exists', return_value=True)
    @patch('builtins.open', new_callable=mock_open)
    def test_setup_shell_completion_already_configured(self, mock_file, mock_exists):
        """Test shell completion when already configured."""
        mock_file.return_value.read.return_value = 'eval "$(register-python-argcomplete datacompose)"'
        
        result = InitCommand.setup_shell_completion(verbose=True)
        assert result is True
        # Should not write anything new
        mock_file().write.assert_not_called()

    @patch.dict(os.environ, {'SHELL': '/bin/bash'})
    @patch('pathlib.Path.exists', return_value=True)
    @patch('builtins.open')
    def test_setup_shell_completion_permission_denied_read(self, mock_open_func, mock_exists):
        """Test shell completion with permission denied on read."""
        mock_open_func.side_effect = PermissionError("Permission denied")
        
        result = InitCommand.setup_shell_completion(verbose=True)
        assert result is False

    @patch.dict(os.environ, {'SHELL': '/bin/bash'})
    @patch('pathlib.Path.exists', return_value=True)
    def test_setup_shell_completion_permission_denied_write(self, mock_exists):
        """Test shell completion with permission denied on write."""
        mock_file = mock_open(read_data="# Existing content\n")
        
        with patch('builtins.open', mock_file):
            # Make write raise PermissionError
            mock_file().write.side_effect = PermissionError("Permission denied")
            result = InitCommand.setup_shell_completion(verbose=True)
            assert result is False

    @patch.dict(os.environ, {'SHELL': '/bin/bash'})
    @patch('pathlib.Path.exists', return_value=True)
    @patch('builtins.open', new_callable=mock_open)
    def test_setup_shell_completion_creates_backup(self, mock_file, mock_exists):
        """Test that shell completion creates a backup file."""
        mock_file.return_value.read.return_value = "# Original content\n"
        
        result = InitCommand.setup_shell_completion(verbose=True)
        assert result is True
        
        # Check backup was created
        calls = mock_file.call_args_list
        backup_calls = [call for call in calls if '.datacompose-backup' in str(call)]
        assert len(backup_calls) > 0

    @patch('builtins.input', return_value='y')
    @patch.object(InitCommand, 'setup_shell_completion', return_value=True)
    def test_prompt_completion_setup_yes(self, mock_setup, mock_input):
        """Test prompting for completion setup with yes response."""
        result = InitCommand.prompt_completion_setup(verbose=True)
        assert result is True
        mock_setup.assert_called_once_with(True)

    @patch('builtins.input', return_value='n')
    def test_prompt_completion_setup_no(self, mock_input):
        """Test prompting for completion setup with no response."""
        with patch('builtins.print') as mock_print:
            result = InitCommand.prompt_completion_setup(verbose=False)
            assert result is False
            # Should print manual instructions
            assert any("Skipped" in str(call) for call in mock_print.call_args_list)

    @patch('builtins.input', return_value='')
    @patch.object(InitCommand, 'setup_shell_completion', return_value=True)
    def test_prompt_completion_setup_default(self, mock_setup, mock_input):
        """Test prompting for completion setup with default (empty) response."""
        result = InitCommand.prompt_completion_setup(verbose=False)
        assert result is True
        mock_setup.assert_called_once()

    @patch('builtins.input', side_effect=KeyboardInterrupt)
    def test_prompt_completion_setup_interrupt(self, mock_input):
        """Test prompting for completion setup with keyboard interrupt."""
        result = InitCommand.prompt_completion_setup(verbose=False)
        assert result is False

    @patch('builtins.input', side_effect=EOFError)
    def test_prompt_completion_setup_eof(self, mock_input):
        """Test prompting for completion setup with EOF."""
        result = InitCommand.prompt_completion_setup(verbose=False)
        assert result is False

    @patch.object(InitCommand, 'setup_shell_completion', return_value=False)
    @patch('builtins.input', return_value='y')
    def test_prompt_completion_setup_failed(self, mock_input, mock_setup):
        """Test prompting when setup fails."""
        with patch('builtins.print') as mock_print:
            result = InitCommand.prompt_completion_setup(verbose=True)
            assert result is False
            # Should print manual instructions
            assert any("Manual setup instructions" in str(call) for call in mock_print.call_args_list)


@pytest.mark.unit
class TestPromptForTargets:
    """Test the prompt_for_targets interactive method."""

    @patch.object(InitCommand, 'get_key')
    @patch('builtins.print')
    @patch('builtins.input', return_value='')
    def test_prompt_for_targets_basic_flow(self, mock_input, mock_print, mock_get_key):
        """Test basic flow of target selection."""
        # Simulate: select first item, then confirm
        mock_get_key.side_effect = [' ', '\n']
        
        available_targets = {
            "pyspark": {"output": "./transformers/pyspark", "name": "PySpark (Apache Spark)"}
        }
        
        result = InitCommand.prompt_for_targets(available_targets)
        assert "pyspark" in result
        assert result["pyspark"]["output"] == "./transformers/pyspark"

    @patch.object(InitCommand, 'get_key')
    @patch('builtins.print')
    def test_prompt_for_targets_navigation(self, mock_print, mock_get_key):
        """Test arrow key navigation in target selection."""
        # Simulate: down arrow, up arrow, space to select, enter
        mock_get_key.side_effect = ['\x1b[B', '\x1b[A', ' ', '\n']
        
        with patch('builtins.input', return_value=''):
            available_targets = {
                "pyspark": {"output": "./transformers/pyspark", "name": "PySpark"},
                "postgres": {"output": "./build/postgres", "name": "PostgreSQL"}
            }
            
            result = InitCommand.prompt_for_targets(available_targets)
            assert "pyspark" in result

    @patch.object(InitCommand, 'get_key')
    @patch('builtins.print')
    def test_prompt_for_targets_quit(self, mock_print, mock_get_key):
        """Test quitting target selection."""
        # Simulate: press 'q' to quit
        mock_get_key.return_value = 'q'
        
        available_targets = {
            "pyspark": {"output": "./transformers/pyspark", "name": "PySpark"}
        }
        
        result = InitCommand.prompt_for_targets(available_targets)
        assert result == {}

    @patch.object(InitCommand, 'get_key')
    @patch('builtins.print')
    def test_prompt_for_targets_escape(self, mock_print, mock_get_key):
        """Test escaping target selection."""
        # Simulate: press ESC to quit
        mock_get_key.return_value = '\x1b'
        
        available_targets = {
            "pyspark": {"output": "./transformers/pyspark", "name": "PySpark"}
        }
        
        result = InitCommand.prompt_for_targets(available_targets)
        assert result == {}

    @patch.object(InitCommand, 'get_key')
    @patch('builtins.print')
    @patch('builtins.input')
    def test_prompt_for_targets_custom_output(self, mock_input, mock_print, mock_get_key):
        """Test setting custom output directory."""
        # Select first item and confirm
        mock_get_key.side_effect = [' ', '\n']
        # Provide custom output path
        mock_input.return_value = './custom/output'
        
        available_targets = {
            "pyspark": {"output": "./transformers/pyspark", "name": "PySpark"}
        }
        
        result = InitCommand.prompt_for_targets(available_targets)
        assert result["pyspark"]["output"] == "./custom/output"

    @patch.object(InitCommand, 'get_key')
    @patch('builtins.print')
    @patch('builtins.input', return_value='')
    def test_prompt_for_targets_multiple_selection(self, mock_input, mock_print, mock_get_key):
        """Test selecting multiple targets."""
        # Select first, move down, select second, confirm
        mock_get_key.side_effect = [' ', '\x1b[B', ' ', '\n']
        
        available_targets = {
            "pyspark": {"output": "./transformers/pyspark", "name": "PySpark"},
            "postgres": {"output": "./build/postgres", "name": "PostgreSQL"}
        }
        
        result = InitCommand.prompt_for_targets(available_targets)
        assert "pyspark" in result
        assert "postgres" in result


@pytest.mark.unit
class TestPromptForConfig:
    """Test the prompt_for_config method."""

    @patch.object(InitCommand, 'prompt_for_targets')
    @patch('builtins.print')
    def test_prompt_for_config_success(self, mock_print, mock_targets):
        """Test successful config prompting."""
        mock_targets.return_value = {
            "pyspark": {"output": "./transformers/pyspark"}
        }
        
        template = DEFAULT_CONFIG.copy()
        result = InitCommand.prompt_for_config(template)
        
        assert result is not None
        assert result["version"] == "1.0"
        assert "pyspark" in result["targets"]

    @patch.object(InitCommand, 'prompt_for_targets')
    @patch('builtins.print')
    def test_prompt_for_config_cancelled(self, mock_print, mock_targets):
        """Test cancelled config prompting."""
        mock_targets.return_value = {}  # User quit selection
        
        template = DEFAULT_CONFIG.copy()
        result = InitCommand.prompt_for_config(template)
        
        assert result is None


@pytest.mark.unit
class TestRunInit:
    """Test the _run_init function."""

    def test_run_init_existing_file_no_force(self):
        """Test init with existing file and no force flag."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "datacompose.json"
            config_path.write_text('{"existing": true}')
            
            result = _run_init(False, str(config_path), False, True, True)
            assert result == 1
            
            # File should not be modified
            with open(config_path) as f:
                config = json.load(f)
            assert config["existing"] is True

    def test_run_init_existing_file_with_force(self):
        """Test init with existing file and force flag."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "datacompose.json"
            config_path.write_text('{"existing": true}')
            
            result = _run_init(True, str(config_path), False, True, True)
            assert result == 0
            
            # File should be overwritten
            with open(config_path) as f:
                config = json.load(f)
            assert "existing" not in config
            assert config["version"] == "1.0"

    def test_run_init_yes_mode(self):
        """Test init in non-interactive mode (--yes)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "datacompose.json"
            
            result = _run_init(False, str(config_path), False, True, True)
            assert result == 0
            assert config_path.exists()

    @patch.object(InitCommand, 'prompt_for_config')
    def test_run_init_interactive_mode(self, mock_prompt):
        """Test init in interactive mode."""
        mock_prompt.return_value = DEFAULT_CONFIG.copy()
        
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "datacompose.json"
            
            result = _run_init(False, str(config_path), False, False, True)
            assert result == 0
            assert config_path.exists()
            mock_prompt.assert_called_once()

    @patch.object(InitCommand, 'prompt_for_config')
    def test_run_init_interactive_cancelled(self, mock_prompt):
        """Test init when user cancels in interactive mode."""
        mock_prompt.return_value = None
        
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "datacompose.json"
            
            result = _run_init(False, str(config_path), False, False, True)
            assert result == 0
            assert not config_path.exists()

    @patch.object(InitCommand, 'prompt_completion_setup')
    @patch.object(InitCommand, 'prompt_for_config')
    def test_run_init_with_completion_prompt(self, mock_config, mock_completion):
        """Test init with shell completion prompt."""
        mock_config.return_value = DEFAULT_CONFIG.copy()
        mock_completion.return_value = True
        
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "datacompose.json"
            
            result = _run_init(False, str(config_path), False, False, False)
            assert result == 0
            mock_completion.assert_called_once()

    def test_run_init_skip_completion(self):
        """Test init with --skip-completion flag."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "datacompose.json"
            
            with patch.object(InitCommand, 'prompt_completion_setup') as mock_completion:
                result = _run_init(False, str(config_path), True, True, True)
                assert result == 0
                mock_completion.assert_not_called()

    def test_run_init_verbose_mode(self):
        """Test init with verbose output."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "datacompose.json"
            
            with patch('builtins.print') as mock_print:
                result = _run_init(False, str(config_path), True, True, True)
                assert result == 0
                
                # Check for verbose output
                output = ' '.join(str(call) for call in mock_print.call_args_list)
                assert "Used template: default" in output
                assert "Created directory structure" in output

    def test_run_init_exception_handling(self):
        """Test init with exception during execution."""
        with patch('builtins.open', side_effect=OSError("Cannot write")):
            result = _run_init(False, "test.json", False, True, True)
            assert result == 1

    def test_run_init_exception_verbose(self):
        """Test init with exception in verbose mode."""
        with patch('builtins.open', side_effect=OSError("Cannot write")):
            with patch('traceback.print_exc') as mock_traceback:
                result = _run_init(False, "test.json", True, True, True)
                assert result == 1
                mock_traceback.assert_called_once()

    @patch.object(InitCommand, 'create_directory_structure')
    def test_run_init_creates_directories(self, mock_create_dirs):
        """Test that init creates directory structure."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "datacompose.json"
            
            result = _run_init(False, str(config_path), False, True, True)
            assert result == 0
            mock_create_dirs.assert_called_once()


@pytest.mark.unit  
class TestInitCommandCLI:
    """Test the init command through the CLI."""

    @pytest.fixture
    def runner(self):
        """Fixture to provide Click CLI runner."""
        return CliRunner()

    def test_init_cli_basic(self, runner):
        """Test basic init command through CLI."""
        original_dir = os.getcwd()
        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                os.chdir(tmpdir)
                result = runner.invoke(cli, ['init', '--yes', '--skip-completion'])
                assert result.exit_code == 0
                assert Path("datacompose.json").exists()
        finally:
            os.chdir(original_dir)

    def test_init_cli_all_options(self, runner):
        """Test init command with all options."""
        original_dir = os.getcwd()
        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                os.chdir(tmpdir)
                result = runner.invoke(cli, [
                    'init',
                    '--force',
                    '--output', 'custom.json',
                    '--verbose',
                    '--yes',
                    '--skip-completion'
                ])
                assert result.exit_code == 0
                assert Path("custom.json").exists()
        finally:
            os.chdir(original_dir)

    def test_init_context_exit(self, runner):
        """Test that init properly exits with context."""
        original_dir = os.getcwd()
        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                os.chdir(tmpdir)
                # Create existing file
                Path("datacompose.json").write_text('{}')
                
                result = runner.invoke(cli, ['init', '--yes'])
                assert result.exit_code == 1
        finally:
            os.chdir(original_dir)


@pytest.mark.unit
class TestInitEdgeCases:
    """Test edge cases and error conditions."""

    def test_init_with_invalid_json_in_force(self):
        """Test force overwrite of invalid JSON file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "datacompose.json"
            config_path.write_text('invalid json{')
            
            result = _run_init(True, str(config_path), False, True, True)
            assert result == 0
            
            # Should have valid JSON now
            with open(config_path) as f:
                config = json.load(f)
            assert config["version"] == "1.0"

    def test_init_with_nested_output_path(self):
        """Test init with nested output path that doesn't exist."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "nested" / "path" / "datacompose.json"
            # Create parent directories first
            config_path.parent.mkdir(parents=True, exist_ok=True)
            
            result = _run_init(False, str(config_path), False, True, True)
            assert result == 0
            assert config_path.exists()

    @patch.dict(os.environ, {'SHELL': ''})
    def test_setup_completion_no_shell_env(self):
        """Test shell completion when SHELL env var is empty."""
        result = InitCommand.setup_shell_completion(verbose=True)
        assert result is False

    def test_get_config_template_copies(self):
        """Test that get_config_template returns copies, not references."""
        config1 = InitCommand.get_config_template("default")
        config2 = InitCommand.get_config_template("default")
        
        config1["modified"] = True
        assert "modified" not in config2

    @patch('sys.stdin')
    def test_get_key_with_no_stdin(self, mock_stdin):
        """Test get_key when stdin is not available."""
        mock_stdin.fileno.side_effect = AttributeError("No fileno")
        
        with patch('builtins.input', return_value='fallback'):
            key = InitCommand.get_key()
            assert key == 'fallback'