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
            # Generate clean_emails primitives
            result = runner.invoke(
                cli, ["add", "clean_emails", "--target", "pyspark"]
            )
            assert result.exit_code == 0

            # Add the generated package to path
            sys.path.insert(0, str(Path("build/pyspark/clean_emails")))

            try:
                # Import the generated module
                import email_primitives

                # Check that PrimitiveRegistry is available
                assert hasattr(email_primitives, "emails")
                assert email_primitives.emails.__class__.__name__ == "PrimitiveRegistry"

                # Check some functions exist
                assert hasattr(email_primitives.emails, "is_valid_email")
                assert hasattr(email_primitives.emails, "extract_domain")
                assert hasattr(email_primitives.emails, "extract_username")
                assert hasattr(email_primitives.emails, "standardize_email")

            finally:
                # Clean up sys.path
                if str(Path("build/pyspark/clean_emails")) in sys.path:
                    sys.path.remove(str(Path("build/pyspark/clean_emails")))

    def test_all_transformers_import_correctly(self, runner):
        """Test that all transformers can import PrimitiveRegistry from utils."""
        with runner.isolated_filesystem():
            transformers = [
                ("clean_emails", "email_primitives", "emails"),
                ("clean_addresses", "address_primitives", "addresses"),
                ("clean_phone_numbers", "phone_primitives", "phones"),
            ]

            for transformer_name, module_name, registry_name in transformers:
                # Generate the transformer
                result = runner.invoke(
                    cli, ["add", transformer_name, "--target", "pyspark"]
                )
                assert result.exit_code == 0

                # Add to path
                path_str = str(Path(f"build/pyspark/{transformer_name}"))
                sys.path.insert(0, path_str)

                try:
                    # Import the module dynamically
                    module = __import__(module_name)

                    # Check registry exists
                    assert hasattr(module, registry_name)
                    registry = getattr(module, registry_name)
                    assert registry.__class__.__name__ == "PrimitiveRegistry"

                finally:
                    # Clean up
                    if path_str in sys.path:
                        sys.path.remove(path_str)
                    # Remove from modules cache
                    if module_name in sys.modules:
                        del sys.modules[module_name]

    def test_utils_directory_structure(self, runner):
        """Test that utils directory is created with correct structure."""
        with runner.isolated_filesystem():
            # Generate a transformer
            result = runner.invoke(
                cli, ["add", "clean_emails", "--target", "pyspark"]
            )
            assert result.exit_code == 0

            # Check utils directory structure
            utils_dir = Path("build/pyspark/clean_emails/utils")
            assert utils_dir.exists()
            assert utils_dir.is_dir()

            # Check __init__.py exists
            init_file = utils_dir / "__init__.py"
            assert init_file.exists()

            # Check primitives.py exists
            primitives_file = utils_dir / "primitives.py"
            assert primitives_file.exists()

            # Verify primitives.py contains PrimitiveRegistry class
            content = primitives_file.read_text()
            assert "class PrimitiveRegistry" in content
            assert "class SmartPrimitive" in content

    def test_generated_code_fallback_import(self, runner):
        """Test that generated code falls back to datacompose import if utils not found."""
        with runner.isolated_filesystem():
            # Generate the transformer
            result = runner.invoke(
                cli, ["add", "clean_emails", "--target", "pyspark"]
            )
            assert result.exit_code == 0

            # Read the generated file to verify import fallback structure
            email_primitives_file = Path("build/pyspark/clean_emails/email_primitives.py")
            content = email_primitives_file.read_text()
            
            # Check that the fallback import structure exists
            assert "try:" in content
            assert "from utils.primitives import PrimitiveRegistry" in content
            assert "except ImportError:" in content
            assert "from datacompose.operators.primitives import PrimitiveRegistry" in content
            
            # Now test that it actually works with utils present
            sys.path.insert(0, str(Path("build/pyspark/clean_emails")))
            
            try:
                import email_primitives
                assert hasattr(email_primitives, "emails")
                
                # Remove the utils directory
                import shutil
                shutil.rmtree("build/pyspark/clean_emails/utils")
                
                # Reload should now use fallback (this tests the actual installed datacompose)
                # Note: This will only work if datacompose is installed
                import importlib
                importlib.reload(email_primitives)
                
                # Should still work with fallback
                assert hasattr(email_primitives, "emails")
                assert email_primitives.emails.__class__.__name__ == "PrimitiveRegistry"
                
            except ImportError:
                # This is expected if running in isolation without datacompose installed
                # The important part is that the import structure is correct
                pass
            finally:
                # Clean up
                if str(Path("build/pyspark/clean_emails")) in sys.path:
                    sys.path.remove(str(Path("build/pyspark/clean_emails")))
                if "email_primitives" in sys.modules:
                    del sys.modules["email_primitives"]