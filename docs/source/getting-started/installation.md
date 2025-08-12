# Installation

## Using pip

### From PyPI (when published)

```bash
pip install datacompose
```

### From Source (Development)

Clone the repository and install in development mode:

```bash
git clone https://github.com/yourusername/datacompose.git
cd datacompose
pip install -e .
```

## Installing Extras

Install with specific platform support:

## Verify Installation

Check that Datacompose is installed correctly:

```bash
datacompose --version
datacompose --help
```

## Platform-Specific Requirements

### Apache Spark

For Spark UDF generation, you'll need:

```bash
pip install pyspark>=3.0.0
```

### PostgreSQL

For PostgreSQL UDF generation, ensure you have:

- PostgreSQL 12+ installed
- psycopg2 for Python connectivity

```bash
pip install psycopg2-binary
```

## Next Steps

Once installed, proceed to the [Quickstart Guide](quickstart.md) to create your first UDF.
