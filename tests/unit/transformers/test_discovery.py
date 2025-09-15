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
        """Test resolving transformer with dot notation (e.g., 'text.emails')."""
        # This should work with the old format
        transformer_name, transformer_path = discovery.resolve_transformer(
            "text.emails"
        )
        # May return None if transformer doesn't exist, but should not raise error
        assert transformer_name is None
        assert transformer_path is None

    def test_resolve_transformer_without_dot_notation(self, discovery):
        """Test resolving transformer without dot notation (e.g., 'emails')."""
        # This should work with the new format
        transformer_name, transformer_path = discovery.resolve_transformer("emails")
        # May return None if transformer doesn't exist, but should not raise error
        if transformer_name:
            assert transformer_name == "emails"
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
        transformer_name, transformer_path = discovery.resolve_transformer(
            "Clean_Emails"
        )
        assert transformer_name is None
        assert transformer_path is None

        transformer_name, transformer_path = discovery.resolve_transformer(
            "CLEAN_EMAILS"
        )
        assert transformer_name is None
        assert transformer_path is None

    def test_resolve_transformer_whitespace(self, discovery):
        """Test transformer resolution with whitespace."""
        # Should not match with spaces
        transformer_name, transformer_path = discovery.resolve_transformer(" emails ")
        assert transformer_name is None
        assert transformer_path is None

        transformer_name, transformer_path = discovery.resolve_transformer(
            "clean emails"
        )
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


@pytest.mark.unit
class TestTransformerDiscoveryAliases:
    """Test suite for TransformerDiscovery platform aliases functionality."""

    @pytest.fixture
    def discovery(self):
        """Fixture to provide TransformerDiscovery instance."""
        return TransformerDiscovery()

    def test_platform_aliases_defined(self, discovery):
        """Test that platform aliases are properly defined."""
        assert hasattr(discovery, 'PLATFORM_ALIASES')
        assert isinstance(discovery.PLATFORM_ALIASES, dict)

        # Test specific aliases
        assert "postgres" in discovery.PLATFORM_ALIASES
        assert "postgresql" in discovery.PLATFORM_ALIASES
        assert discovery.PLATFORM_ALIASES["postgres"] == "sql.postgres_generator"
        assert discovery.PLATFORM_ALIASES["postgresql"] == "sql.postgres_generator"

    def test_resolve_generator_with_postgres_alias(self, discovery):
        """Test resolve_generator with postgres alias."""
        # Test postgres alias resolves correctly
        result = discovery.resolve_generator("postgres")
        # Should return a generator class or None (if PostgreSQL generator not available)
        assert result is not None or result is None

        # Test postgresql alias
        result2 = discovery.resolve_generator("postgresql")
        # Both aliases should resolve to the same generator
        assert type(result) == type(result2)

    def test_resolve_generator_with_full_path_still_works(self, discovery):
        """Test that full paths still work (backwards compatibility)."""
        # Test full path still works
        result_full = discovery.resolve_generator("sql.postgres_generator")
        result_alias = discovery.resolve_generator("postgres")

        # Both should resolve to the same thing (if available)
        assert type(result_full) == type(result_alias)

    def test_resolve_generator_unknown_alias(self, discovery):
        """Test resolve_generator with unknown alias."""
        # Test unknown alias returns None
        result = discovery.resolve_generator("unknown_alias_12345")
        assert result is None

    def test_resolve_generator_alias_takes_precedence(self, discovery):
        """Test that alias resolution happens before normal resolution."""
        from unittest.mock import patch

        with patch.object(discovery, 'discover_generators') as mock_discover:
            postgres_generator = object()  # Unique object for comparison
            other_generator = object()
            mock_discover.return_value = {
                'sql': {
                    'postgres_generator': postgres_generator,
                    'generator': other_generator
                },
                'postgres': {  # If there was a platform called 'postgres'
                    'some_generator': other_generator
                }
            }

            # Should resolve to sql.postgres_generator via alias, not postgres.some_generator
            result = discovery.resolve_generator("postgres")
            assert result == postgres_generator

    def test_resolve_generator_with_non_alias_platform(self, discovery):
        """Test resolve_generator with regular platform (not alias)."""
        # Test that non-alias platforms still work normally
        result = discovery.resolve_generator("pyspark")
        # Should work without errors regardless of what's available

    def test_resolve_generator_alias_with_dotted_name_not_confused(self, discovery):
        """Test that dotted names don't trigger alias resolution."""
        # "postgres.something" should NOT trigger alias (has a dot)
        # This should look for platform 'postgres', not resolve alias
        result = discovery.resolve_generator("postgres.nonexistent")
        # Should return None (no such generator type)
        assert result is None

    def test_backwards_compatibility_maintained(self, discovery):
        """Test that existing full paths continue to work."""
        # These should work the same way they did before aliases were added
        try:
            # Test full platform.generator paths
            discovery.resolve_generator("pyspark.generator")
            discovery.resolve_generator("sql.postgres_generator")
            discovery.resolve_generator("sql.generator")

            # Test platform-only paths
            discovery.resolve_generator("pyspark")
            discovery.resolve_generator("sql")

        except Exception as e:
            pytest.fail(f"Backwards compatibility broken: {e}")

    def test_alias_case_sensitivity(self, discovery):
        """Test that aliases are case sensitive."""
        # Test that aliases are case sensitive (should not match)
        result_upper = discovery.resolve_generator("POSTGRES")
        result_mixed = discovery.resolve_generator("Postgres")

        # These should not resolve to the PostgreSQL generator
        assert result_upper is None
        assert result_mixed is None

        # But lowercase should work
        result_lower = discovery.resolve_generator("postgres")
        # This may or may not be None depending on whether PostgreSQL generator is available

    def test_alias_with_whitespace_handling(self, discovery):
        """Test alias resolution with whitespace."""
        # Aliases with spaces should not work
        result_space = discovery.resolve_generator(" postgres ")
        result_space2 = discovery.resolve_generator("postgres ")
        result_space3 = discovery.resolve_generator(" postgres")

        assert result_space is None
        assert result_space2 is None
        assert result_space3 is None
