# Datacompose Documentation

```{toctree}
:maxdepth: 2
:hidden:

getting-started/index
user-guide/index
api/index
examples/index
contributing
changelog
```

## Welcome to Datacompose

**Datacompose** is a Python framework for generating data cleaning UDFs (User Defined Functions) for various data platforms from YAML specifications.

::::{grid} 2
:gutter: 2

:::{grid-item-card} 🚀 Getting Started
:link: getting-started/index
:link-type: doc

New to Datacompose? Start here with installation and quickstart guides.
:::

:::{grid-item-card} 📖 User Guide
:link: user-guide/index
:link-type: doc

Learn core concepts, specifications, and how to use Datacompose effectively.
:::

:::{grid-item-card} 🔧 API Reference
:link: api/index
:link-type: doc

Complete API documentation with detailed function and class references.
:::

:::{grid-item-card} 💡 Examples
:link: examples/index
:link-type: doc

Practical examples and recipes for common use cases.
:::
::::

## Key Features

- **Multi-Platform Support**: Generate UDFs for Spark, Postgres, Snowflake, and more
- **YAML-Based Specifications**: Define cleaning rules in simple, readable YAML
- **Extensible Architecture**: Easy to add new platforms and transformations
- **Type-Safe**: Built with type hints and validation
- **Well-Tested**: Comprehensive test suite with golden data validation

## Quick Example

```yaml
# email_spec.yaml
name: clean_emails
description: Email address cleaner and validator
type: text
flags:
  lowercase: true
  strip_whitespace: true
  validate: true
typo_map:
  gmial.com: gmail.com
  yahooo.com: yahoo.com
```

```bash
# Generate a Spark UDF from the spec
datacompose add clean_emails --target spark

# Use in your pipeline
from build.spark.email_cleaner_udf import email_cleaner_udf
```

## Supported Platforms

- **Apache Spark** (PySpark)
- **PostgreSQL** 
- **Pandas**
- **Snowflake** (coming soon)
- **DuckDB** (coming soon)
- **BigQuery** (coming soon)

## License

Datacompose is released under the MIT License.