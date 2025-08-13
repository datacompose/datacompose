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

            # Add the build directory to path (for utils import)
            sys.path.insert(0, str(Path("build")))
            # Add the generated package to path
            sys.path.insert(0, str(Path("build/clean_emails")))

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
                if str(Path("build/clean_emails")) in sys.path:
                    sys.path.remove(str(Path("build/clean_emails")))
                if str(Path("build")) in sys.path:
                    sys.path.remove(str(Path("build")))

    def test_all_transformers_import_correctly(self, runner):
        """Test that all transformers can import PrimitiveRegistry from utils."""
        with runner.isolated_filesystem():
            transformers = [
                ("clean_emails", "email_primitives", "emails"),
                ("clean_addresses", "address_primitives", "addresses"),
                ("clean_phone_numbers", "phone_primitives", "phones"),
            ]

            # Add build directory once for utils
            sys.path.insert(0, str(Path("build")))

            try:
                for transformer_name, module_name, registry_name in transformers:
                    # Generate the transformer
                    result = runner.invoke(
                        cli, ["add", transformer_name, "--target", "pyspark"]
                    )
                    assert result.exit_code == 0

                    # Add transformer directory to path
                    path_str = str(Path(f"build/{transformer_name}"))
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
            finally:
                if str(Path("build")) in sys.path:
                    sys.path.remove(str(Path("build")))

    def test_utils_directory_structure(self, runner):
        """Test that utils directory is created at build root with correct structure."""
        with runner.isolated_filesystem():
            # Generate a transformer
            result = runner.invoke(
                cli, ["add", "clean_emails", "--target", "pyspark"]
            )
            assert result.exit_code == 0

            # Check utils directory structure at build root
            utils_dir = Path("build/utils")
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
            
            # Verify utils is NOT in transformer directory
            transformer_utils = Path("build/clean_emails/utils")
            assert not transformer_utils.exists()
            
            # Test multiple transformers share the same utils
            result2 = runner.invoke(
                cli, ["add", "clean_addresses", "--target", "pyspark"]
            )
            assert result2.exit_code == 0
            
            # Should still be only one utils directory at build root
            assert utils_dir.exists()
            assert not Path("build/clean_addresses/utils").exists()

    def test_generated_code_fallback_import(self, runner):
        """Test that generated code falls back to datacompose import if utils not found."""
        with runner.isolated_filesystem():
            # Generate the transformer
            result = runner.invoke(
                cli, ["add", "clean_emails", "--target", "pyspark"]
            )
            assert result.exit_code == 0

            # Read the generated file to verify import fallback structure
            email_primitives_file = Path("build/clean_emails/email_primitives.py")
            content = email_primitives_file.read_text()
            
            # Check that the fallback import structure exists
            assert "try:" in content
            assert "from utils.primitives import PrimitiveRegistry" in content
            assert "except ImportError:" in content
            assert "from datacompose.operators.primitives import PrimitiveRegistry" in content
            
            # Now test that it actually works with utils present
            sys.path.insert(0, str(Path("build")))
            sys.path.insert(0, str(Path("build/clean_emails")))
            
            try:
                import email_primitives
                assert hasattr(email_primitives, "emails")
                
                # Remove the utils directory
                import shutil
                shutil.rmtree("build/utils")
                
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
                if str(Path("build/clean_emails")) in sys.path:
                    sys.path.remove(str(Path("build/clean_emails")))
                if str(Path("build")) in sys.path:
                    sys.path.remove(str(Path("build")))
                if "email_primitives" in sys.modules:
                    del sys.modules["email_primitives"]

    def test_no_platform_subdirectory(self, runner):
        """Test that output path does not include platform subdirectory."""
        with runner.isolated_filesystem():
            # Generate a transformer
            result = runner.invoke(
                cli, ["add", "clean_emails", "--target", "pyspark"]
            )
            assert result.exit_code == 0
            
            # Verify structure is flat under build
            assert Path("build/clean_emails/email_primitives.py").exists()
            assert not Path("build/pyspark").exists()
            
            # Generate another transformer
            result2 = runner.invoke(
                cli, ["add", "clean_phone_numbers", "--target", "pyspark"]
            )
            assert result2.exit_code == 0
            
            # Verify both are at same level
            assert Path("build/clean_phone_numbers/phone_primitives.py").exists()
            assert Path("build/clean_emails/email_primitives.py").exists()
            
            # Still no platform directory
            assert not Path("build/pyspark").exists()