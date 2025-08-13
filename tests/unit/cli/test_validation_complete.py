"""Comprehensive test suite for validation.py module."""

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch
import pytest

# Add project root to path for imports
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from datacompose.cli.validation import (  # noqa: E402
    validate_platform,
    validate_type_for_platform,
    get_available_platforms,
    get_available_types_for_platform,
)
from datacompose.transformers.discovery import TransformerDiscovery  # noqa: E402


@pytest.mark.unit
class TestValidatePlatform:
    """Test validate_platform function."""

    def test_validate_platform_valid(self):
        """Test validation with a valid platform."""
        mock_discovery = MagicMock(spec=TransformerDiscovery)
        mock_discovery.list_generators.return_value = [
            'pyspark.generator',
            'postgres.sql_udf',
            'snowflake.udf'
        ]
        
        result = validate_platform('pyspark', mock_discovery)
        assert result is True
        mock_discovery.list_generators.assert_called_once()

    def test_validate_platform_invalid(self):
        """Test validation with an invalid platform."""
        mock_discovery = MagicMock(spec=TransformerDiscovery)
        mock_discovery.list_generators.return_value = [
            'pyspark.generator',
            'postgres.sql_udf'
        ]
        
        with patch('builtins.print') as mock_print:
            result = validate_platform('invalid_platform', mock_discovery)
            assert result is False
            
            # Check that error messages were printed
            print_calls = [str(call) for call in mock_print.call_args_list]
            assert any('invalid_platform' in call for call in print_calls)
            assert any('Available platforms' in call for call in print_calls)

    def test_validate_platform_empty_generators(self):
        """Test validation when no generators are available."""
        mock_discovery = MagicMock(spec=TransformerDiscovery)
        mock_discovery.list_generators.return_value = []
        
        with patch('builtins.print') as mock_print:
            result = validate_platform('pyspark', mock_discovery)
            assert result is False
            
            # Should show that no platforms are available
            print_calls = [str(call) for call in mock_print.call_args_list]
            assert any('not found' in call for call in print_calls)

    def test_validate_platform_case_sensitive(self):
        """Test that platform validation is case-sensitive."""
        mock_discovery = MagicMock(spec=TransformerDiscovery)
        mock_discovery.list_generators.return_value = ['pyspark.generator']
        
        # Should fail with different case
        with patch('builtins.print'):
            result = validate_platform('PySpark', mock_discovery)
            assert result is False
            
            result = validate_platform('PYSPARK', mock_discovery)
            assert result is False
            
            result = validate_platform('pyspark', mock_discovery)
            assert result is True


@pytest.mark.unit
class TestValidateTypeForPlatform:
    """Test validate_type_for_platform function."""

    def test_validate_type_valid(self):
        """Test validation with a valid type for platform."""
        mock_discovery = MagicMock(spec=TransformerDiscovery)
        mock_discovery.list_generators.return_value = [
            'pyspark.pandas_udf',
            'pyspark.sql_udf',
            'postgres.sql_udf'
        ]
        
        result = validate_type_for_platform('pyspark', 'pandas_udf', mock_discovery)
        assert result is True
        
        result = validate_type_for_platform('pyspark', 'sql_udf', mock_discovery)
        assert result is True

    def test_validate_type_invalid(self):
        """Test validation with an invalid type for platform."""
        mock_discovery = MagicMock(spec=TransformerDiscovery)
        mock_discovery.list_generators.return_value = [
            'pyspark.pandas_udf',
            'postgres.sql_udf'
        ]
        
        with patch('builtins.print') as mock_print:
            result = validate_type_for_platform('pyspark', 'invalid_type', mock_discovery)
            assert result is False
            
            # Check error messages
            print_calls = [str(call) for call in mock_print.call_args_list]
            assert any('invalid_type' in call for call in print_calls)
            assert any('Available types' in call for call in print_calls)
            assert any('pandas_udf' in call for call in print_calls)

    def test_validate_type_platform_has_no_types(self):
        """Test validation when platform has no generators."""
        mock_discovery = MagicMock(spec=TransformerDiscovery)
        mock_discovery.list_generators.return_value = [
            'postgres.sql_udf',
            'snowflake.udf'
        ]
        
        with patch('builtins.print') as mock_print:
            result = validate_type_for_platform('pyspark', 'pandas_udf', mock_discovery)
            assert result is False
            
            # Should show no generators available message (line 53)
            print_calls = [str(call) for call in mock_print.call_args_list]
            assert any('No generators available' in call for call in print_calls)
            assert any('pyspark' in call for call in print_calls)

    def test_validate_type_empty_generators_list(self):
        """Test when no generators exist at all."""
        mock_discovery = MagicMock(spec=TransformerDiscovery)
        mock_discovery.list_generators.return_value = []
        
        with patch('builtins.print') as mock_print:
            result = validate_type_for_platform('pyspark', 'pandas_udf', mock_discovery)
            assert result is False
            
            # Should trigger line 53 (no generators for platform)
            print_calls = [str(call) for call in mock_print.call_args_list]
            assert any('No generators available' in call for call in print_calls)

    def test_validate_type_case_sensitive(self):
        """Test that type validation is case-sensitive."""
        mock_discovery = MagicMock(spec=TransformerDiscovery)
        mock_discovery.list_generators.return_value = ['pyspark.pandas_udf']
        
        # Should fail with different case
        with patch('builtins.print'):
            result = validate_type_for_platform('pyspark', 'Pandas_UDF', mock_discovery)
            assert result is False
            
            result = validate_type_for_platform('pyspark', 'PANDAS_UDF', mock_discovery)
            assert result is False
            
            result = validate_type_for_platform('pyspark', 'pandas_udf', mock_discovery)
            assert result is True


@pytest.mark.unit
class TestGetAvailablePlatforms:
    """Test get_available_platforms function."""

    def test_get_available_platforms_multiple(self):
        """Test getting available platforms with multiple generators."""
        mock_discovery = MagicMock(spec=TransformerDiscovery)
        mock_discovery.list_generators.return_value = [
            'pyspark.pandas_udf',
            'pyspark.sql_udf',
            'postgres.sql_udf',
            'snowflake.udf',
            'snowflake.stored_proc'
        ]
        
        platforms = get_available_platforms(mock_discovery)
        
        assert platforms == ['postgres', 'pyspark', 'snowflake']  # Sorted
        mock_discovery.list_generators.assert_called_once()

    def test_get_available_platforms_empty(self):
        """Test getting platforms when no generators exist."""
        mock_discovery = MagicMock(spec=TransformerDiscovery)
        mock_discovery.list_generators.return_value = []
        
        platforms = get_available_platforms(mock_discovery)
        
        assert platforms == []
        mock_discovery.list_generators.assert_called_once()

    def test_get_available_platforms_single(self):
        """Test getting platforms with single generator."""
        mock_discovery = MagicMock(spec=TransformerDiscovery)
        mock_discovery.list_generators.return_value = ['pyspark.generator']
        
        platforms = get_available_platforms(mock_discovery)
        
        assert platforms == ['pyspark']

    def test_get_available_platforms_deduplicated(self):
        """Test that platforms are deduplicated."""
        mock_discovery = MagicMock(spec=TransformerDiscovery)
        mock_discovery.list_generators.return_value = [
            'pyspark.udf1',
            'pyspark.udf2',
            'pyspark.udf3',
        ]
        
        platforms = get_available_platforms(mock_discovery)
        
        assert platforms == ['pyspark']
        assert len(platforms) == 1


@pytest.mark.unit
class TestGetAvailableTypesForPlatform:
    """Test get_available_types_for_platform function."""

    def test_get_types_for_platform_multiple(self):
        """Test getting types for a platform with multiple types."""
        mock_discovery = MagicMock(spec=TransformerDiscovery)
        mock_discovery.list_generators.return_value = [
            'pyspark.pandas_udf',
            'pyspark.sql_udf',
            'pyspark.scala_udf',
            'postgres.sql_udf'
        ]
        
        types = get_available_types_for_platform('pyspark', mock_discovery)
        
        assert set(types) == {'pandas_udf', 'sql_udf', 'scala_udf'}
        assert len(types) == 3
        mock_discovery.list_generators.assert_called_once()

    def test_get_types_for_platform_none(self):
        """Test getting types for a platform with no generators."""
        mock_discovery = MagicMock(spec=TransformerDiscovery)
        mock_discovery.list_generators.return_value = [
            'postgres.sql_udf',
            'snowflake.udf'
        ]
        
        types = get_available_types_for_platform('pyspark', mock_discovery)
        
        assert types == []
        mock_discovery.list_generators.assert_called_once()

    def test_get_types_for_platform_single(self):
        """Test getting types for a platform with single type."""
        mock_discovery = MagicMock(spec=TransformerDiscovery)
        mock_discovery.list_generators.return_value = [
            'pyspark.generator',
            'postgres.sql_udf'
        ]
        
        types = get_available_types_for_platform('pyspark', mock_discovery)
        
        assert types == ['generator']

    def test_get_types_for_platform_empty_generators(self):
        """Test getting types when no generators exist."""
        mock_discovery = MagicMock(spec=TransformerDiscovery)
        mock_discovery.list_generators.return_value = []
        
        types = get_available_types_for_platform('pyspark', mock_discovery)
        
        assert types == []

    def test_get_types_for_nonexistent_platform(self):
        """Test getting types for a platform that doesn't exist."""
        mock_discovery = MagicMock(spec=TransformerDiscovery)
        mock_discovery.list_generators.return_value = [
            'pyspark.pandas_udf',
            'postgres.sql_udf'
        ]
        
        types = get_available_types_for_platform('nonexistent', mock_discovery)
        
        assert types == []


@pytest.mark.unit
class TestValidationEdgeCases:
    """Test edge cases in validation functions."""

    def test_generator_with_multiple_dots(self):
        """Test handling generators with multiple dots in name."""
        mock_discovery = MagicMock(spec=TransformerDiscovery)
        mock_discovery.list_generators.return_value = [
            'pyspark.complex.nested.generator'
        ]
        
        # Should only split on first dot
        platforms = get_available_platforms(mock_discovery)
        assert platforms == ['pyspark']
        
        # The current implementation splits on dot and takes [1], so it only gets 'complex'
        types = get_available_types_for_platform('pyspark', mock_discovery)
        assert types == ['complex']  # Current behavior - only gets first part after platform

    def test_generator_without_dot(self):
        """Test handling generators without dots (malformed)."""
        mock_discovery = MagicMock(spec=TransformerDiscovery)
        mock_discovery.list_generators.return_value = [
            'standalone_generator',
            'pyspark.valid'
        ]
        
        # Should handle gracefully
        platforms = get_available_platforms(mock_discovery)
        assert 'standalone_generator' in platforms
        assert 'pyspark' in platforms

    def test_empty_string_platform(self):
        """Test validation with empty string platform."""
        mock_discovery = MagicMock(spec=TransformerDiscovery)
        mock_discovery.list_generators.return_value = ['pyspark.generator']
        
        with patch('builtins.print'):
            result = validate_platform('', mock_discovery)
            assert result is False

    def test_empty_string_type(self):
        """Test validation with empty string type."""
        mock_discovery = MagicMock(spec=TransformerDiscovery)
        mock_discovery.list_generators.return_value = ['pyspark.generator']
        
        with patch('builtins.print'):
            result = validate_type_for_platform('pyspark', '', mock_discovery)
            assert result is False

    def test_whitespace_platform(self):
        """Test validation with whitespace platform."""
        mock_discovery = MagicMock(spec=TransformerDiscovery)
        mock_discovery.list_generators.return_value = ['pyspark.generator']
        
        with patch('builtins.print'):
            result = validate_platform('  ', mock_discovery)
            assert result is False
            
            result = validate_platform('\t', mock_discovery)
            assert result is False
            
            result = validate_platform('\n', mock_discovery)
            assert result is False

    def test_special_characters_in_names(self):
        """Test handling special characters in generator names."""
        mock_discovery = MagicMock(spec=TransformerDiscovery)
        mock_discovery.list_generators.return_value = [
            'py-spark.pandas_udf',
            'post_gres.sql-udf'
        ]
        
        platforms = get_available_platforms(mock_discovery)
        assert 'py-spark' in platforms
        assert 'post_gres' in platforms
        
        types = get_available_types_for_platform('py-spark', mock_discovery)
        assert types == ['pandas_udf']


@pytest.mark.unit
class TestValidationWithRealDiscovery:
    """Test validation functions with real TransformerDiscovery."""

    def test_validate_with_real_discovery(self):
        """Test validation using actual discovery system."""
        discovery = TransformerDiscovery()
        
        # Test with actual available platforms
        generators = discovery.list_generators()
        if generators:
            # Get first available platform
            first_platform = generators[0].split('.')[0]
            result = validate_platform(first_platform, discovery)
            assert result is True
        
        # Test with definitely invalid platform
        with patch('builtins.print'):
            result = validate_platform('definitely_invalid_platform_xyz', discovery)
            assert result is False

    def test_get_platforms_with_real_discovery(self):
        """Test getting platforms with real discovery."""
        discovery = TransformerDiscovery()
        platforms = get_available_platforms(discovery)
        
        # Should return actual platforms from the system
        assert isinstance(platforms, list)
        assert all(isinstance(p, str) for p in platforms)
        assert platforms == sorted(platforms)  # Should be sorted