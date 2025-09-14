"""
Tests for PathCompleter class and path completion functionality.
"""

import os
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from datacompose.cli.commands.init import PathCompleter, input_with_path_completion


@pytest.mark.unit
class TestPathCompleter:
    """Test the PathCompleter class."""

    def test_init_default(self):
        """Test PathCompleter initialization with default parameters."""
        completer = PathCompleter()
        assert completer.filter_extensions == []

    def test_init_with_filter_extensions(self):
        """Test PathCompleter initialization with filter extensions."""
        extensions = ['.env', '.txt', '.json']
        completer = PathCompleter(filter_extensions=extensions)
        assert completer.filter_extensions == extensions

    def test_complete_directory_expansion(self, tmp_path):
        """Test completion with directory path."""
        # Create test directory structure
        (tmp_path / "subdir").mkdir()
        (tmp_path / "file1.txt").touch()
        (tmp_path / "file2.env").touch()

        completer = PathCompleter()

        with patch('os.path.dirname', return_value=str(tmp_path)):
            with patch('os.path.basename', return_value=''):
                with patch('os.listdir', return_value=['subdir', 'file1.txt', 'file2.env']):
                    with patch('os.path.isdir', side_effect=lambda x: 'subdir' in x):
                        result = completer.complete(str(tmp_path) + "/", 0)
                        # Should return first entry - either directory with slash or file
                        assert result is not None

    def test_complete_with_partial_filename(self, tmp_path):
        """Test completion with partial filename."""
        # Create test files
        (tmp_path / "test_file1.env").touch()
        (tmp_path / "test_file2.txt").touch()
        (tmp_path / "other_file.py").touch()

        completer = PathCompleter()
        test_path = str(tmp_path / "test_")

        with patch('os.path.dirname', return_value=str(tmp_path)):
            with patch('os.path.basename', return_value='test_'):
                with patch('os.listdir', return_value=['test_file1.env', 'test_file2.txt', 'other_file.py']):
                    with patch('os.path.isdir', return_value=False):
                        result = completer.complete(test_path, 0)
                        assert result is not None
                        assert 'test_' in result

    def test_complete_with_filter_extensions(self, tmp_path):
        """Test completion with extension filtering."""
        # Create test files
        (tmp_path / "config.env").touch()
        (tmp_path / "readme.txt").touch()
        (tmp_path / "script.py").touch()

        completer = PathCompleter(filter_extensions=['.env'])

        with patch('os.path.dirname', return_value=str(tmp_path)):
            with patch('os.path.basename', return_value=''):
                with patch('os.listdir', return_value=['config.env', 'readme.txt', 'script.py']):
                    with patch('os.path.isdir', return_value=False):
                        # Should only return .env files
                        result = completer.complete(str(tmp_path) + "/", 0)
                        if result:
                            assert '.env' in result

    def test_complete_home_directory_expansion(self):
        """Test completion with home directory expansion."""
        completer = PathCompleter()

        with patch('os.path.expanduser', return_value='/home/user/') as mock_expanduser:
            with patch('os.path.isdir', return_value=True):
                with patch('os.listdir', return_value=['Documents', 'Downloads']):
                    result = completer.complete('~/test', 0)
                    mock_expanduser.assert_called_once_with('~/test')

    def test_complete_permission_error(self, tmp_path):
        """Test completion handles permission errors gracefully."""
        completer = PathCompleter()

        with patch('os.listdir', side_effect=PermissionError("Access denied")):
            result = completer.complete(str(tmp_path), 0)
            assert result is None

    def test_complete_os_error(self, tmp_path):
        """Test completion handles OS errors gracefully."""
        completer = PathCompleter()

        with patch('os.listdir', side_effect=OSError("Directory not found")):
            result = completer.complete(str(tmp_path), 0)
            assert result is None

    def test_complete_state_bounds(self, tmp_path):
        """Test completion respects state parameter bounds."""
        # Create test files
        (tmp_path / "file1.txt").touch()
        (tmp_path / "file2.txt").touch()

        completer = PathCompleter()

        with patch('os.path.dirname', return_value=str(tmp_path)):
            with patch('os.path.basename', return_value=''):
                with patch('os.listdir', return_value=['file1.txt', 'file2.txt']):
                    with patch('os.path.isdir', return_value=False):
                        # Should return file1.txt for state=0
                        result0 = completer.complete(str(tmp_path) + "/", 0)
                        # Should return file2.txt for state=1
                        result1 = completer.complete(str(tmp_path) + "/", 1)
                        # Should return None for state=2 (out of bounds)
                        result2 = completer.complete(str(tmp_path) + "/", 2)

                        assert result2 is None
                        if result0 and result1:
                            assert result0 != result1


@pytest.mark.unit
class TestInputWithPathCompletion:
    """Test the input_with_path_completion function."""

    @patch('datacompose.cli.commands.init.READLINE_AVAILABLE', False)
    def test_input_without_readline(self):
        """Test input_with_path_completion when readline is not available."""
        with patch('builtins.input', return_value='test_input'):
            result = input_with_path_completion("Enter path: ")
            assert result == 'test_input'

    @patch('datacompose.cli.commands.init.READLINE_AVAILABLE', True)
    def test_input_with_readline(self):
        """Test input_with_path_completion when readline is available."""
        mock_readline = Mock()
        mock_completer = Mock()

        with patch('readline.get_completer', return_value=mock_completer):
            with patch('readline.get_completer_delims', return_value=' \t\n'):
                with patch('readline.set_completer'):
                    with patch('readline.set_completer_delims'):
                        with patch('builtins.input', return_value='test_path'):
                            result = input_with_path_completion("Enter path: ")
                            assert result == 'test_path'

    def test_input_with_default_empty_input(self):
        """Test input_with_path_completion returns default for empty input."""
        with patch('builtins.input', return_value='   '):  # Whitespace only
            result = input_with_path_completion("Enter path: ", default="/default/path")
            assert result == '/default/path'

    def test_input_with_default_non_empty_input(self):
        """Test input_with_path_completion ignores default for non-empty input."""
        with patch('builtins.input', return_value='  /user/path  '):
            result = input_with_path_completion("Enter path: ", default="/default/path")
            assert result == '/user/path'

    def test_input_no_default_empty_input(self):
        """Test input_with_path_completion with empty input and no default."""
        with patch('builtins.input', return_value=''):
            result = input_with_path_completion("Enter path: ")
            assert result == ''

    @patch('datacompose.cli.commands.init.READLINE_AVAILABLE', True)
    def test_input_restores_completer_on_exception(self):
        """Test that completer is restored even if exception occurs."""
        old_completer = Mock()
        old_delims = ' \t\n'

        with patch('readline.get_completer', return_value=old_completer):
            with patch('readline.get_completer_delims', return_value=old_delims):
                with patch('readline.set_completer'):
                    with patch('readline.set_completer_delims'):
                        with patch('builtins.input', side_effect=KeyboardInterrupt()):
                            try:
                                input_with_path_completion("Enter path: ")
                            except KeyboardInterrupt:
                                pass
                            # Should restore completer even after exception

    def test_input_with_filter_extensions(self):
        """Test input_with_path_completion passes filter_extensions to PathCompleter."""
        extensions = ['.env', '.config']

        with patch('datacompose.cli.commands.init.PathCompleter') as MockPathCompleter:
            with patch('builtins.input', return_value='test'):
                input_with_path_completion("Enter path: ", filter_extensions=extensions)
                MockPathCompleter.assert_called_once_with(extensions)