# Changelog

All notable changes to Datacompose will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.4] - 2024-08-12

### Added
- **Phone Number Primitives**: Complete set of 45+ phone number transformation functions
  - NANP validation and formatting (North American Numbering Plan)
  - International phone support with E.164 formatting
  - Extension handling and toll-free detection
  - Phone number extraction from text
  - Letter-to-number conversion (1-800-FLOWERS support)
- **Address Improvements**: Enhanced street extraction and standardization
  - Fixed numbered street extraction ("5th Avenue" correctly returns "5th")
  - Improved null handling in street extraction
  - Custom mapping support for street suffix standardization
- **Utils Export**: Generated code now includes `utils/primitives.py` for standalone deployment
  - PrimitiveRegistry class embedded with generated code
  - No runtime dependency on datacompose package
  - Fallback imports for maximum compatibility

### Changed
- **BREAKING**: Renamed `PrimitiveNameSpace` to `PrimitiveRegistry` throughout codebase
- **Major Architecture Shift**: Removed YAML/spec file system entirely
  - No more YAML specifications or JSON replacements
  - Direct primitive file copying instead of template rendering
  - Simplified discovery system works with transformer directories
  - Removed `validate` command completely
- **Import Strategy**: Primitives now try local utils import first, fall back to datacompose package
- **File Naming**: Generated files use plural form with primitives suffix
  - `clean_emails` → `email_primitives.py`
  - `clean_addresses` → `address_primitives.py`
  - `clean_phone_numbers` → `phone_primitives.py`

### Fixed
- Phone `normalize_separators` now correctly handles parentheses: `(555)123-4567` → `555-123-4567`
- Street extraction for numbered streets ("5th Avenue" issue)
- Compose decorator now requires namespace to be passed explicitly for proper method resolution
- `standardize_street_suffix` applies both custom and default mappings correctly
- Test failures due to namespace resolution in compose decorator

### Removed
- All YAML/spec file functionality
- PostgreSQL generator references
- Jinja2 template dependencies
- `validate` command from CLI
- Old Spark integration tests (replaced with end-to-end tests)

## [0.2.0] - 2024-XX-XX

### Added
- **Primitive System**: New composable primitive architecture for building data pipelines
  - `SmartPrimitive` class for partial application and parameter binding
  - `PrimitiveRegistry` (originally PrimitiveNameSpace) for organizing related transformations
  - Support for conditional primitives (boolean-returning functions)
- **Conditional Compilation**: AST-based pipeline compilation with if/else support
  - `PipelineCompiler` for parsing and compiling conditional logic
  - `StablePipeline` for executing compiled pipelines
  - Full support for nested conditionals and complex branching
- **Comprehensive Testing**: 44+ tests covering conditional compilation scenarios
  - Edge cases and null handling
  - Complex nested logic
  - Data-driven conditions
  - Performance optimization tests
  - Real-world use cases
  - Parameter handling
  - Error handling
- **Improved Architecture**: Dual approach for different runtime constraints
  - Primitives for flexible runtimes (Python, Spark, Scala)
  - Templates for rigid targets (SQL, PostgreSQL)

### Changed
- Made PySpark an optional dependency
- Reorganized test structure with focused test files and shared fixtures
- Refined architecture to support both template-based and primitive-based approaches

### Fixed
- Import paths for pipeline compilation modules
- Missing return statements in pipeline execution
- Conditional logic to use accumulated results correctly

## [0.1.4] - 2024-XX-XX

### Added
- Initial release of Datacompose
- Core framework for generating data cleaning UDFs
- Support for Spark, PostgreSQL, and Pandas targets
- Built-in specifications for common data cleaning tasks:
  - Email address cleaning
  - Phone number normalization
  - Address standardization
  - Job title standardization
  - Date/time parsing
- CLI interface with commands:
  - `datacompose init` - Initialize project
  - `datacompose add` - Generate UDFs from specs
  - `datacompose list` - List available targets and specs
  - `datacompose validate` - Validate specification files
- YAML-based specification format
- Jinja2 templating for code generation
- Comprehensive test suite
- Documentation with Sphinx and Furo theme