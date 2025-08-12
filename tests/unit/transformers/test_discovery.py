"""Test the transformer discovery functionality."""

import sys
from pathlib import Path

import pytest

from datacompose.transformers.discovery import TransformerDiscovery

# Add project root to path for imports
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))


@pytest.mark.unit
class TestTransformerDiscovery:
    """Test suite for TransformerDiscovery functionality."""

    @pytest.fixture
    def discovery(self):
        """Fixture to provide TransformerDiscovery instance."""
        return TransformerDiscovery()

    def test_list_transformers(self, discovery):
        """Test listing available transformers."""
        transformers = discovery.list_transformers()
        assert isinstance(transformers, list)
        # Should return a list (may be empty if no transformers found)

    def test_resolve_transformer_with_dot_notation(self, discovery):
        """Test resolving transformer with dot notation (e.g., 'text.clean_emails')."""
        # This should work with the old format
        transformer_name, transformer_path = discovery.resolve_transformer("text.clean_emails")
        # May return None if transformer doesn't exist, but should not raise error
        assert transformer_name is None
        assert transformer_path is None

    def test_resolve_transformer_without_dot_notation(self, discovery):
        """Test resolving transformer without dot notation (e.g., 'clean_emails')."""
        # This should work with the new format
        transformer_name, transformer_path = discovery.resolve_transformer("clean_emails")
        # May return None if transformer doesn't exist, but should not raise error
        if transformer_name:
            assert transformer_name == "clean_emails"
            assert transformer_path.exists()
            assert transformer_path.is_dir()

    def test_resolve_nonexistent_transformer(self, discovery):
        """Test resolving a transformer that doesn't exist."""
        transformer_name, transformer_path = discovery.resolve_transformer(
            "nonexistent_transformer"
        )
        assert transformer_name is None
        assert transformer_path is None

    def test_resolve_empty_transformer_name(self, discovery):
        """Test resolving with empty transformer name."""
        transformer_name, transformer_path = discovery.resolve_transformer("")
        assert transformer_name is None
        assert transformer_path is None

    def test_list_generators(self, discovery):
        """Test listing available generators."""
        generators = discovery.list_generators()
        assert isinstance(generators, list)
        # Should return a list (may be empty if no generators found)

    def test_resolve_nonexistent_generator(self, discovery):
        """Test resolving a generator that doesn't exist."""
        generator_class = discovery.resolve_generator("nonexistent_generator")
        assert generator_class is None

    def test_discover_transformers(self, discovery):
        """Test discovering transformers returns a dictionary."""
        transformers = discovery.discover_transformers()
        assert isinstance(transformers, dict)
        # Dictionary should have transformer names as keys
        for key, value in transformers.items():
            assert isinstance(key, str)
            # Value should contain transformer info (path, etc.)

    def test_discover_generators(self, discovery):
        """Test discovering generators returns a dictionary."""
        generators = discovery.discover_generators()
        assert isinstance(generators, dict)
        # Dictionary should have generator names as keys
        for key, value in generators.items():
            assert isinstance(key, str)
            # Value should be generator class or info

    def test_resolve_transformer_case_sensitivity(self, discovery):
        """Test that transformer resolution is case sensitive."""
        # Should not match different case
        transformer_name, transformer_path = discovery.resolve_transformer("Clean_Emails")
        assert transformer_name is None
        assert transformer_path is None

        transformer_name, transformer_path = discovery.resolve_transformer("CLEAN_EMAILS")
        assert transformer_name is None
        assert transformer_path is None

    def test_resolve_transformer_whitespace(self, discovery):
        """Test transformer resolution with whitespace."""
        # Should not match with spaces
        transformer_name, transformer_path = discovery.resolve_transformer(" clean_emails ")
        assert transformer_name is None
        assert transformer_path is None

        transformer_name, transformer_path = discovery.resolve_transformer("clean emails")
        assert transformer_name is None
        assert transformer_path is None

    def test_resolve_transformer_special_characters(self, discovery):
        """Test transformer resolution with special characters."""
        # Test various invalid characters
        invalid_names = [
            "clean-emails",
            "clean.emails",
            "clean/emails",
            "clean\\emails",
        ]
        for name in invalid_names:
            transformer_name, transformer_path = discovery.resolve_transformer(name)
            # May or may not return None depending on implementation
            # Just ensure it doesn't crash

    def test_discover_transformers_structure(self, discovery):
        """Test the structure of discovered transformers dictionary."""
        transformers = discovery.discover_transformers()
        assert isinstance(transformers, dict)

        # If transformers exist, verify structure
        if transformers:
            for name, info in transformers.items():
                assert isinstance(name, str)
                assert len(name) > 0
                # Verify the info contains expected data

    def test_discover_generators_structure(self, discovery):
        """Test the structure of discovered generators dictionary."""
        generators = discovery.discover_generators()
        assert isinstance(generators, dict)

        # If generators exist, verify structure
        if generators:
            for name, info in generators.items():
                assert isinstance(name, str)
                assert len(name) > 0

    def test_list_transformers_consistency(self, discovery):
        """Test that list_transformers is consistent with discover_transformers."""
        discovered = discovery.discover_transformers()
        listed = discovery.list_transformers()

        # List should contain the same transformer names as discovered keys
        assert set(listed) == set(discovered.keys())

    def test_list_generators_consistency(self, discovery):
        """Test that list_generators is consistent with discover_generators."""
        discovered = discovery.discover_generators()
        listed = discovery.list_generators()

        # Build expected list from discovered generators that have actual generator classes
        expected = []
        for platform, platform_generators in discovered.items():
            for gen_type in platform_generators.keys():
                expected.append(f"{platform}.{gen_type}")

        # List should contain the same generator names as discovered with actual implementations
        assert set(listed) == set(expected)

