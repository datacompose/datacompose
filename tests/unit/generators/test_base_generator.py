"""Test the BaseGenerator class through real generators."""

import shutil
import sys
import tempfile
from pathlib import Path

import pytest

# Add project root to path for imports
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from datacompose.generators.pyspark.generator import (
    SparkPandasUDFGenerator,  # noqa: E402
)


@pytest.mark.unit
class TestBaseGeneratorThroughRealGenerators:
    """Test BaseGenerator functionality through real gecanerator implementations."""

    @pytest.fixture
    def temp_dir(self):
        """Fixture to provide temporary directory for tests."""
        temp_dir = tempfile.mkdtemp()
        yield Path(temp_dir)
        shutil.rmtree(temp_dir)

    @pytest.fixture
    def temp_template_dir(self, temp_dir):
        """Fixture to provide template directory."""
        template_dir = temp_dir / "templates"
        template_dir.mkdir()
        return template_dir

    @pytest.fixture
    def output_dir(self, temp_dir):
        """Fixture to provide output directory."""
        output_dir = temp_dir / "output"
        output_dir.mkdir()
        return output_dir

    @pytest.fixture
    def spark_generator(self, temp_template_dir, output_dir):
        """Fixture to provide SparkPandasUDFGenerator instance."""
        return SparkPandasUDFGenerator(temp_template_dir, output_dir, verbose=False)

    @pytest.fixture
    def mock_template_content(self, temp_template_dir):
        """Fixture to create mock template content for testing."""
        # Create a simple template
        template_content = """# Generated UDF for {{ transformer_name }}
# Hash: {{ hash }}
# Generated on: {{ generation_timestamp }}

def {{ udf_name }}(input_value):
    \"\"\"{{ transformer_name }} UDF implementation.\"\"\"
    
    # Typo corrections
    typo_map = {{ typo_map }}
    
    # Apply typo corrections
    if input_value and input_value in typo_map:
        input_value = typo_map[input_value]
    
    # Apply regex patterns
    {% if regex_patterns %}
    import re
    pattern = "{{ regex_patterns.get('pattern', '') }}"
    if pattern and input_value:
        if not re.match(pattern, input_value):
            return None
    {% endif %}
    
    # Apply flags
    {% if flags.get('lowercase') %}
    if input_value:
        input_value = input_value.lower()
    {% endif %}
    
    return input_value
"""
        return template_content

    def test_generator_initialization(self, temp_template_dir, output_dir):
        """Test generator initialization with Spark generator."""
        spark_gen = SparkPandasUDFGenerator(temp_template_dir, output_dir, verbose=True)

        # Test Spark generator
        assert spark_gen.template_dir == temp_template_dir
        assert spark_gen.output_dir == output_dir
        assert spark_gen.verbose is True

    def test_calculate_hash_consistency(self, spark_generator):
        """Test that hash calculation is consistent."""
        spec = {"name": "test", "description": "test spec"}
        template_content = "test template"

        hash1 = spark_generator._calculate_hash(spec, template_content)
        hash2 = spark_generator._calculate_hash(spec, template_content)

        assert hash1 == hash2
        assert len(hash1) == 8  # First 8 characters of SHA256
        assert isinstance(hash1, str)

    def test_calculate_hash_changes_with_content(self, spark_generator):
        """Test that hash changes when content changes."""
        spec1 = {"name": "test1"}
        spec2 = {"name": "test2"}
        template_content = "test template"

        hash1 = spark_generator._calculate_hash(spec1, template_content)
        hash2 = spark_generator._calculate_hash(spec2, template_content)

        assert hash1 != hash2

    def test_should_skip_generation_file_not_exists(self, spark_generator, temp_dir):
        """Test skip generation check when output file doesn't exist."""
        output_path = temp_dir / "nonexistent.py"
        spec_hash = "abcd1234"

        should_skip = spark_generator._should_skip_generation(output_path, spec_hash)
        assert should_skip is False

    def test_should_skip_generation_hash_matches(self, spark_generator, temp_dir):
        """Test skip generation when hash matches existing file."""
        output_path = temp_dir / "existing.py"
        spec_hash = "abcd1234"

        # Create file with matching hash in header
        with open(output_path, "w") as f:
            f.write(f"# Generated UDF\n# Hash: {spec_hash}\n# Content\npass")

        should_skip = spark_generator._should_skip_generation(output_path, spec_hash)
        assert should_skip is True

    def test_should_skip_generation_hash_different(self, spark_generator, temp_dir):
        """Test skip generation when hash doesn't match."""
        output_path = temp_dir / "existing.py"
        spec_hash = "abcd1234"
        different_hash = "efgh5678"

        # Create file with different hash
        with open(output_path, "w") as f:
            f.write(f"# Generated UDF\n# Hash: {different_hash}\n# Content\npass")

        should_skip = spark_generator._should_skip_generation(output_path, spec_hash)
        assert should_skip is False

    def test_write_output_creates_directories(self, spark_generator, temp_dir):
        """Test that write_output creates necessary directories."""
        output_path = temp_dir / "nested" / "directories" / "file.py"
        content = "# Test content\nprint('hello')"

        spark_generator._write_output(output_path, content)

        assert output_path.exists()
        assert output_path.read_text() == content
        assert output_path.parent.exists()

    def test_ensure_init_files_creates_init_files(self, spark_generator, temp_dir):
        """Test that __init__.py files are created properly."""
        output_path = temp_dir / "transformers" / "spark" / "email_cleaner" / "file.py"
        output_path.parent.mkdir(parents=True)

        spark_generator._ensure_init_files(output_path)

        # Check that __init__.py files are created in transformers hierarchy
        assert (temp_dir / "transformers" / "__init__.py").exists()
        assert (temp_dir / "transformers" / "spark" / "__init__.py").exists()
        assert (temp_dir / "transformers" / "spark" / "email_cleaner" / "__init__.py").exists()

    # Test file generation methods were removed in refactoring
    # The generator no longer creates test files, only primitives files

    def test_output_filename_generation(self, spark_generator):
        """Test that generator produces correct output filename."""
        spec_name = "email_cleaner"

        spark_filename = spark_generator._get_output_filename(spec_name)

        assert spark_filename == "email_cleaner.py"

    def test_template_content_error_handling(self, spark_generator, temp_dir):
        """Test error handling when template content is missing."""
        # Create transformer directory without template
        transformer_dir = temp_dir / "transformer"
        transformer_dir.mkdir()

        with pytest.raises(FileNotFoundError):
            spark_generator._get_primitives_file(transformer_dir)

    def test_hash_includes_template_content(self, spark_generator):
        """Test that hash calculation includes template content changes."""
        spec = {"name": "test"}
        template1 = "old template"
        template2 = "new template"

        hash1 = spark_generator._calculate_hash(spec, template1)
        hash2 = spark_generator._calculate_hash(spec, template2)

        assert hash1 != hash2, "Hash should change when template content changes"
