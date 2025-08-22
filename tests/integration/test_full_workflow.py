"""Full end-to-end integration test: install, init, add, and transform."""

import subprocess
import sys
import tempfile
from pathlib import Path
import pytest


@pytest.mark.integration
class TestFullWorkflow:
    """Test complete workflow from install to transformation."""

    def test_complete_workflow_emails(self):
        """Test full workflow: init -> add -> import -> transform for emails."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir = Path(tmpdir)
            
            # Step 1: Initialize datacompose project
            result = subprocess.run(
                ["datacompose", "init", "--yes", "--skip-completion"],
                capture_output=True,
                text=True,
                cwd=tmpdir
            )
            assert result.returncode == 0, f"Init failed: {result.stderr}"
            assert (tmpdir / "datacompose.json").exists()
            
            # Step 2: Add email transformer
            result = subprocess.run(
                ["datacompose", "add", "emails", "-t", "pyspark"],
                capture_output=True,
                text=True,
                cwd=tmpdir
            )
            assert result.returncode == 0, f"Add failed: {result.stderr}"
            
            # Verify generated files - new structure
            email_file = tmpdir / "transformers" / "pyspark" / "emails.py"
            utils_file = tmpdir / "transformers" / "pyspark" / "utils" / "primitives.py"
            assert email_file.exists(), f"Email file not generated at {email_file}"
            assert utils_file.exists(), f"Utils not generated at {utils_file}"
            
            # Step 3: Create and run transformation script
            test_script = tmpdir / "test_emails.py"
            test_script.write_text("""
import sys
from pathlib import Path

# Add generated code to path
sys.path.insert(0, str(Path(__file__).parent / "transformers" / "pyspark"))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import emails

# Create Spark session
spark = SparkSession.builder.appName("email_test").master("local[*]").getOrCreate()

# Create test data
df = spark.createDataFrame([
    ("JOHN.DOE@EXAMPLE.COM", "  Contact me at john.doe@example.com  "),
    ("Jane@TEST.org", "Email: jane@test.org or call"),
    ("invalid-email", "No email here"),
    (None, None)
], ["email_col", "text_col"])

# Test various email transformations using the namespace functions
result = df.select(
    emails.emails.lowercase_email(F.col("email_col")).alias("normalized"),
    emails.emails.extract_email(F.col("text_col")).alias("extracted"),
    emails.emails.is_valid_email(F.col("email_col")).alias("is_valid"),
    emails.emails.extract_domain(F.col("email_col")).alias("domain")
).collect()

# Verify basic functionality works - just check that transformations run
assert len(result) == 4
assert result[0]["normalized"] is not None
assert result[0]["is_valid"] == True
assert result[2]["is_valid"] == False

print("SUCCESS: Email transformations work!")
spark.stop()
""")
            
            # Run the transformation test
            result = subprocess.run(
                [sys.executable, str(test_script)],
                capture_output=True,
                text=True,
                cwd=tmpdir
            )
            
            assert result.returncode == 0, f"Transformation failed: {result.stderr}"
            assert "SUCCESS" in result.stdout

    def test_complete_workflow_phone_numbers(self):
        """Test full workflow for phone numbers."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir = Path(tmpdir)
            
            # Initialize
            subprocess.run(
                ["datacompose", "init", "--yes", "--skip-completion"],
                cwd=tmpdir, capture_output=True
            )
            
            # Add phone transformer
            result = subprocess.run(
                ["datacompose", "add", "phone_numbers", "-t", "pyspark"],
                capture_output=True,
                text=True,
                cwd=tmpdir
            )
            assert result.returncode == 0
            
            # Create test script
            test_script = tmpdir / "test_phones.py"
            test_script.write_text("""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent / "transformers" / "pyspark"))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import phone_numbers

spark = SparkSession.builder.appName("phone_test").master("local[*]").getOrCreate()

df = spark.createDataFrame([
    ("(555) 123-4567",),
    ("1-800-FLOWERS",),
    ("555.123.4567 ext 999",),
], ["phone_number"])

# Test basic phone transformations
result = df.select(
    phone_numbers.phone_numbers.normalize_separators(F.col("phone_number")).alias("normalized"),
    phone_numbers.phone_numbers.extract_extension(F.col("phone_number")).alias("extension")
).collect()

# Just verify transformations run
assert len(result) == 3
assert result[0]["normalized"] is not None
assert result[2]["extension"] == "999"

print("SUCCESS: Phone transformations work!")
spark.stop()
""")
            
            result = subprocess.run(
                [sys.executable, str(test_script)],
                capture_output=True,
                text=True,
                cwd=tmpdir
            )
            
            assert result.returncode == 0, f"Phone test failed: {result.stderr}"
            assert "SUCCESS" in result.stdout

    def test_complete_workflow_addresses(self):
        """Test full workflow for addresses."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir = Path(tmpdir)
            
            # Initialize
            subprocess.run(
                ["datacompose", "init", "--yes", "--skip-completion"],
                cwd=tmpdir, capture_output=True
            )
            
            # Add address transformer
            result = subprocess.run(
                ["datacompose", "add", "addresses", "-t", "pyspark"],
                capture_output=True,
                text=True,
                cwd=tmpdir
            )
            assert result.returncode == 0
            
            # Create test script
            test_script = tmpdir / "test_addresses.py"
            test_script.write_text("""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent / "transformers" / "pyspark"))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import addresses

spark = SparkSession.builder.appName("address_test").master("local[*]").getOrCreate()

df = spark.createDataFrame([
    ("123 Main Street, Apt 4B",),
    ("456 5th Avenue",),
    ("PO Box 789",),
], ["address"])

# Test basic address transformations
result = df.select(
    addresses.addresses.extract_street_number(F.col("address")).alias("number"),
    addresses.addresses.extract_street_name(F.col("address")).alias("street")
).collect()

# Just verify transformations run
assert len(result) == 3
assert result[0]["number"] == "123"
assert result[0]["street"] == "Main"

print("SUCCESS: Address transformations work!")
spark.stop()
""")
            
            result = subprocess.run(
                [sys.executable, str(test_script)],
                capture_output=True,
                text=True,
                cwd=tmpdir
            )
            
            assert result.returncode == 0, f"Address test failed: {result.stderr}"
            assert "SUCCESS" in result.stdout

    def test_multiple_transformers_together(self):
        """Test multiple transformers in the same project."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir = Path(tmpdir)
            
            # Initialize project
            subprocess.run(
                ["datacompose", "init", "--yes", "--skip-completion"],
                cwd=tmpdir, capture_output=True
            )
            
            # Add multiple transformers
            for transformer in ["emails", "phone_numbers", "addresses"]:
                result = subprocess.run(
                    ["datacompose", "add", transformer, "-t", "pyspark"],
                    capture_output=True,
                    text=True,
                    cwd=tmpdir
                )
                assert result.returncode == 0, f"Failed to add {transformer}: {result.stderr}"
            
            # Verify all files were generated
            assert (tmpdir / "transformers" / "pyspark" / "emails.py").exists()
            assert (tmpdir / "transformers" / "pyspark" / "phone_numbers.py").exists()
            assert (tmpdir / "transformers" / "pyspark" / "addresses.py").exists()
            
            # Create combined test script
            test_script = tmpdir / "test_combined.py"
            test_script.write_text("""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent / "transformers" / "pyspark"))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import emails
import phone_numbers
import addresses

spark = SparkSession.builder.appName("combined_test").master("local[*]").getOrCreate()

# Create test data with all three types
df = spark.createDataFrame([
    ("john@example.com", "(555) 123-4567", "123 Main St"),
    ("jane@test.org", "555.987.6543", "456 Oak Ave"),
    ("invalid", "not-a-phone", "PO Box 789"),
], ["email", "phone", "address"])

# Test all transformers
result = df.select(
    emails.emails.is_valid_email(F.col("email")).alias("valid_email"),
    phone_numbers.phone_numbers.normalize_separators(F.col("phone")).alias("phone_normalized"),
    addresses.addresses.extract_street_number(F.col("address")).alias("street_num")
).collect()

# Basic checks
assert len(result) == 3
assert result[0]["valid_email"] == True
assert result[2]["valid_email"] == False
assert result[0]["street_num"] == "123"

print("SUCCESS: All transformers work together!")
spark.stop()
""")
            
            result = subprocess.run(
                [sys.executable, str(test_script)],
                capture_output=True,
                text=True,
                cwd=tmpdir
            )
            
            assert result.returncode == 0, f"Combined test failed: {result.stderr}"
            assert "SUCCESS" in result.stdout