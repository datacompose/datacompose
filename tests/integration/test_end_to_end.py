"""End-to-end integration tests for the datacompose workflow with primitives."""

import json
import sys
from pathlib import Path

import pytest
from click.testing import CliRunner

# Add project root to path for imports
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from datacompose.cli.main import cli  # noqa: E402


@pytest.mark.integration
class TestEndToEndWorkflow:
    """Test complete end-to-end workflow with the new primitive-based system."""

    @pytest.fixture
    def runner(self):
        """Fixture to provide Click CLI runner."""
        return CliRunner()

    def test_complete_workflow_with_primitives(self, runner):
        """Test the complete workflow: init -> add -> use primitives."""
        with runner.isolated_filesystem():
            # Step 1: Initialize project
            result = runner.invoke(cli, ["init", "--yes"])
            assert result.exit_code == 0
            assert "Configuration initialized" in result.output

            # Verify config file
            config_file = Path("datacompose.json")
            assert config_file.exists()

            with open(config_file) as f:
                config = json.load(f)
                assert "pyspark" in config["targets"]
                assert config["targets"]["pyspark"]["output"] == "./build/pyspark"

            # Step 2: Add transformers for each domain
            transformers = [
                ("clean_emails", "email_primitives.py"),
                ("clean_addresses", "address_primitives.py"),
                ("clean_phone_numbers", "phone_primitives.py"),
            ]

            for transformer_name, expected_file in transformers:
                result = runner.invoke(
                    cli, ["add", transformer_name, "--target", "pyspark"]
                )
                assert result.exit_code == 0
                assert "generated" in result.output.lower()

                # Verify the generated file exists with correct name
                output_dir = Path(f"build/pyspark/{transformer_name}")
                assert output_dir.exists()

                output_file = output_dir / expected_file
                assert output_file.exists(), f"Expected {output_file} to exist"

                # Verify test file was created
                test_file = output_dir / f"test_{expected_file}"
                assert test_file.exists()

                # Verify the content includes PrimitiveRegistry
                content = output_file.read_text()
                assert "PrimitiveRegistry" in content

                # Check for the namespace object
                if "email" in transformer_name:
                    assert 'emails = PrimitiveRegistry("emails")' in content
                elif "address" in transformer_name:
                    assert 'addresses = PrimitiveRegistry("addresses")' in content
                elif "phone" in transformer_name:
                    assert 'phones = PrimitiveRegistry("phones")' in content

            # Step 3: Verify we can import and use the primitives
            # (This would work if PySpark was installed)
            email_primitives = Path("build/pyspark/clean_emails/email_primitives.py")
            content = email_primitives.read_text()

            # Check for some expected primitive functions
            assert "@emails" in content or "@emails.register" in content
            assert "def is_valid_email" in content
            assert "def extract_domain" in content
            assert "def extract_username" in content

            # Step 4: Test list command shows our transformers
            result = runner.invoke(cli, ["list", "transformers"])
            assert result.exit_code == 0
            assert "clean_emails" in result.output
            assert "clean_addresses" in result.output
            assert "clean_phone_numbers" in result.output

            # Step 5: Test regeneration works
            result = runner.invoke(cli, ["add", "clean_emails", "--target", "pyspark"])
            assert result.exit_code == 0
            # Should either skip (if hash matches) or regenerate successfully
            assert (
                "already exists" in result.output
                or "No changes needed" in result.output
                or "UDF generated" in result.output
            )

    def test_workflow_with_custom_output(self, runner):
        """Test workflow with custom output directory."""
        with runner.isolated_filesystem():
            # Initialize
            runner.invoke(cli, ["init", "--yes"])

            # Add with custom output
            custom_output = "custom/output"
            result = runner.invoke(
                cli,
                [
                    "add",
                    "clean_emails",
                    "--target",
                    "pyspark",
                    "--output",
                    custom_output,
                ],
            )
            assert result.exit_code == 0

            # Verify files in custom location
            output_file = Path(
                f"{custom_output}/pyspark/clean_emails/email_primitives.py"
            )
            assert output_file.exists()

    def test_workflow_without_init(self, runner):
        """Test that add command works even without init."""
        with runner.isolated_filesystem():
            # Try to add without init
            result = runner.invoke(cli, ["add", "clean_emails", "--target", "pyspark"])
            # Should work - init is not required
            assert result.exit_code == 0
            assert "generated" in result.output.lower()

    def test_invalid_transformer_error(self, runner):
        """Test helpful error for invalid transformer."""
        with runner.isolated_filesystem():
            runner.invoke(cli, ["init", "--yes"])

            result = runner.invoke(
                cli, ["add", "invalid_transformer", "--target", "pyspark"]
            )
            assert result.exit_code == 1
            assert "Transformer not found" in result.output
            assert "Available transformers" in result.output

    def test_invalid_platform_error(self, runner):
        """Test helpful error for invalid platform."""
        with runner.isolated_filesystem():
            runner.invoke(cli, ["init", "--yes"])

            result = runner.invoke(
                cli, ["add", "clean_emails", "--target", "invalid_platform"]
            )
            assert result.exit_code == 1
            assert "Platform 'invalid_platform' not found" in result.output
            assert "Available platforms" in result.output
            assert "pyspark" in result.output

    def test_verbose_mode_provides_details(self, runner):
        """Test that verbose mode provides additional information."""
        with runner.isolated_filesystem():
            runner.invoke(cli, ["init", "--yes"])

            result = runner.invoke(
                cli, ["add", "clean_emails", "--target", "pyspark", "--verbose"]
            )
            assert result.exit_code == 0
            # Verbose mode should show more details
            assert (
                "Using transformer:" in result.output
                or "Transformer path:" in result.output
            )
