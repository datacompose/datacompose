# Datacompose

[![PyPI version](https://badge.fury.io/py/datacompose.svg)](https://pypi.org/project/datacompose/)
[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![Coverage](https://img.shields.io/badge/coverage-92%25-brightgreen.svg)](https://github.com/your-username/datacompose)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## A powerful data transformation framework for PySpark

Datacompose provides a declarative way to build data transformation pipelines using composable primitives. It generates optimized, standalone PySpark code that can be deployed without runtime dependencies.

<div class="grid cards" markdown>

-   :material-clock-fast:{ .lg .middle } **Quick Start**

    ---

    Get up and running with Datacompose in minutes

    [:octicons-arrow-right-24: Getting started](getting-started/quickstart.md)

-   :material-puzzle:{ .lg .middle } **Composable Primitives**

    ---

    Build complex transformations from simple, reusable functions

    [:octicons-arrow-right-24: Learn about primitives](user-guide/primitives.md)

-   :material-code-tags:{ .lg .middle } **Code Generation**

    ---

    Generate standalone PySpark code with embedded dependencies

    [:octicons-arrow-right-24: Explore examples](examples/index.md)

-   :material-api:{ .lg .middle } **API Reference**

    ---

    Complete documentation of all available primitives and commands

    [:octicons-arrow-right-24: API docs](api/index.md)

</div>

## Why Datacompose?

!!! info "Philosophy"
    Datacompose is inspired by [shadcn](https://ui.shadcn.com/)'s approach to component libraries. Just as shadcn provides "copy and paste" components rather than npm packages, Datacompose generates data transformation code that becomes part of YOUR codebase.

### Key Benefits

- **You Own Your Code**: No external dependencies to manage or worry about breaking changes
- **Full Transparency**: Every transformation is readable, debuggable PySpark code
- **Customization First**: Need to adjust a transformation? Just edit the code
- **Learn by Reading**: The generated code serves as documentation and learning material

## Features at a Glance

- ✅ **Composable Primitives** - Build complex transformations from simple functions
- ✅ **Smart Partial Application** - Configure transformations with parameters for reuse
- ✅ **Pipeline Compilation** - Convert declarative definitions to optimized Spark operations
- ✅ **Comprehensive Libraries** - Pre-built primitives for emails, addresses, and phone numbers
- ✅ **Conditional Logic** - Support for if/else branching in pipelines
- ✅ **Type-Safe Operations** - All transformations maintain Spark column type safety

## Quick Example

```python
from build.pyspark.clean_emails.email_primitives import emails
from pyspark.sql import functions as F

# Apply email transformations
cleaned_df = df.withColumn(
    "email_clean",
    emails.standardize_email(F.col("email"))
).withColumn(
    "email_domain",
    emails.extract_domain(F.col("email_clean"))
).withColumn(
    "is_valid",
    emails.is_valid_email(F.col("email_clean"))
)

# Filter to valid emails only
valid_emails = cleaned_df.filter(F.col("is_valid"))
```

## Test Coverage

!!! success "Well Tested"
    **335 tests passing** • **92% critical component coverage**

| Component | Coverage | Status |
|-----------|----------|--------|
| **Phone Number Primitives** | 95% | ✅ All formats validated |
| **Address Primitives** | 94% | ✅ Full parsing tested |
| **Email Primitives** | 89% | ✅ RFC compliant |
| **Code Generation** | 87-91% | ✅ All targets verified |

## Getting Started

Ready to transform your data? Get started with our [installation guide](getting-started/installation.md) or jump straight into the [quick start tutorial](getting-started/quickstart.md).