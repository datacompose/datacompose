"""Test that generated code can properly import and use PrimitiveRegistry."""

import sys
import tempfile
from pathlib import Path

import pytest
from click.testing import CliRunner

from datacompose.cli.main import cli


@pytest.mark.integration
class TestGeneratedImports:
    """Test that generated code can import PrimitiveRegistry from utils."""

    @pytest.fixture
    def runner(self):
        """Fixture to provide Click CLI runner."""
        return CliRunner()

    def test_generated_code_imports_primitives_from_utils(self, runner):
        """Test that generated code can import PrimitiveRegistry from local utils."""
        with runner.isolated_filesystem():
            # Generate emails primitives
            result = runner.invoke(
                cli, ["add", "emails", "--target", "pyspark"]
            )
            assert result.exit_code == 0

            # Add the pyspark directory to path
            pyspark_path = str(Path("transformers/pyspark").absolute())
            sys.path.insert(0, pyspark_path)

            try:
                # Import the generated module
                import emails

                # Check that PrimitiveRegistry is available
                assert hasattr(emails, "emails")
                assert emails.emails.__class__.__name__ == "PrimitiveRegistry"

                # Check some functions exist
                assert hasattr(emails.emails, "is_valid_email")
                assert hasattr(emails.emails, "extract_domain")
                assert hasattr(emails.emails, "extract_username")
                assert hasattr(emails.emails, "standardize_email")

            finally:
                # Clean up sys.path
                if pyspark_path in sys.path:
                    sys.path.remove(pyspark_path)
                # Remove from modules cache
                if "emails" in sys.modules:
                    del sys.modules["emails"]

    def test_all_transformers_import_correctly(self, runner):
        """Test that all transformers can import PrimitiveRegistry from utils."""
        with runner.isolated_filesystem():
            transformers = [
                ("emails", "emails", "emails"),
                ("addresses", "addresses", "addresses"),
                ("phone_numbers", "phone_numbers", "phone_numbers"),
            ]

            # Add pyspark directory to path
            pyspark_path = str(Path("transformers/pyspark").absolute())
            sys.path.insert(0, pyspark_path)

            try:
                for transformer_name, module_name, registry_name in transformers:
                    # Generate the transformer
                    result = runner.invoke(
                        cli, ["add", transformer_name, "--target", "pyspark"]
                    )
                    assert result.exit_code == 0

                    try:
                        # Import the module dynamically
                        module = __import__(module_name)

                        # Check registry exists
                        assert hasattr(module, registry_name)
                        registry = getattr(module, registry_name)
                        assert registry.__class__.__name__ == "PrimitiveRegistry"

                    finally:
                        # Remove from modules cache
                        if module_name in sys.modules:
                            del sys.modules[module_name]
            finally:
                if pyspark_path in sys.path:
                    sys.path.remove(pyspark_path)

    def test_utils_directory_structure(self, runner):
        """Test that utils directory is created correctly."""
        with runner.isolated_filesystem():
            # Generate a transformer
            result = runner.invoke(
                cli, ["add", "emails", "--target", "pyspark"]
            )
            assert result.exit_code == 0

            # Check directory structure
            utils_dir = Path("transformers/pyspark/utils")
            assert utils_dir.exists()
            assert (utils_dir / "__init__.py").exists()
            assert (utils_dir / "primitives.py").exists()

            # Check primitives.py has the right content
            primitives_content = (utils_dir / "primitives.py").read_text()
            assert "PrimitiveRegistry" in primitives_content
            assert "SmartPrimitive" in primitives_content

    def test_generated_code_fallback_import(self, runner):
        """Test that generated code has fallback import for primitives."""
        with runner.isolated_filesystem():
            # Generate emails primitives
            result = runner.invoke(
                cli, ["add", "emails", "--target", "pyspark"]
            )
            assert result.exit_code == 0

            # Check the generated file contains the fallback import
            generated_file = Path("transformers/pyspark/emails.py")
            assert generated_file.exists()

            content = generated_file.read_text()
            # Should have both local and package imports
            assert "from utils.primitives import PrimitiveRegistry" in content or \
                   "from .utils.primitives import PrimitiveRegistry" in content

    def test_no_platform_subdirectory(self, runner):
        """Test that transformers are directly in the pyspark directory."""
        with runner.isolated_filesystem():
            # Generate transformers
            for transformer in ["emails", "addresses", "phone_numbers"]:
                result = runner.invoke(
                    cli, ["add", transformer, "--target", "pyspark"]
                )
                assert result.exit_code == 0

            # Check that files are directly in transformers/pyspark/
            assert Path("transformers/pyspark/emails.py").exists()
            assert Path("transformers/pyspark/addresses.py").exists()
            assert Path("transformers/pyspark/phone_numbers.py").exists()

            # Check that utils is also in transformers/pyspark/
            assert Path("transformers/pyspark/utils/primitives.py").exists()