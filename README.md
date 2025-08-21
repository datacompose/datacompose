# Datacompose

[![PyPI version](https://badge.fury.io/py/datacompose.svg)](https://pypi.org/project/datacompose/)
[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![Coverage](https://img.shields.io/badge/coverage-92%25-brightgreen.svg)](https://github.com/your-username/datacompose)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A powerful data transformation framework for building reusable, composable data cleaning pipelines in PySpark.

## Installation

```bash
pip install datacompose
```

## What is Datacompose?

Datacompose provides production-ready PySpark data transformation primitives that become part of YOUR codebase. Inspired by [shadcn](https://ui.shadcn.com/)'s approach to components, we believe in giving you full ownership and control over your code.

### Key Features

- **No Runtime Dependencies**: Standalone PySpark code that runs without Datacompose
- **Composable Primitives**: Build complex transformations from simple, reusable functions
- **Smart Partial Application**: Pre-configure transformations with parameters for reuse
- **Optimized Operations**: Efficient Spark transformations with minimal overhead
- **Comprehensive Libraries**: Pre-built primitives for emails, addresses, and phone numbers

### Available Transformers

- **Emails**: Validation, extraction, standardization, typo correction
- **Addresses**: Street parsing, state/zip validation, PO Box detection  
- **Phone Numbers**: NANP/international validation, formatting, toll-free detection

## Documentation

For detailed documentation, examples, and API reference, visit [datacompose.io](https://datacompose.io).

## Philosophy

This is NOT a traditional library - it gives you production-ready data transformation primitives that you can modify to fit your exact needs. You own the code, with no external dependencies to manage or worry about breaking changes.

## License

MIT License - see LICENSE file for details