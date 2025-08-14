# Installation

## Requirements

- Python 3.8 or higher
- PySpark 3.0 or higher
- pip or uv package manager

## Install from PyPI

The easiest way to install Datacompose is from PyPI:

=== "uv"

    ```bash
    uv pip install datacompose
    ```

=== "pip"

    ```bash
    pip install datacompose
    ```

=== "pipx"

    ```bash
    pipx install datacompose
    ```

## Install from Source

To install the latest development version:

```bash
git clone https://github.com/datacompose/datacompose.git
cd datacompose
uv pip install -e .
```

## Verify Installation

After installation, verify that Datacompose is working:

```bash
datacompose --version
```

You should see the version number printed to the console.

## PySpark Setup

Datacompose generates code that requires PySpark. If you don't have PySpark installed:

=== "uv"

    ```bash
    uv pip install pyspark
    ```

=== "pip"

    ```bash
    pip install pyspark
    ```

### Java Requirements

PySpark requires Java 8 or 11. Verify your Java installation:

```bash
java -version
```

If Java is not installed, you can:

- **macOS**: `brew install openjdk@11`
- **Ubuntu/Debian**: `sudo apt-get install openjdk-11-jdk`
- **Windows**: Download from [AdoptOpenJDK](https://adoptopenjdk.net/)

## Development Installation

For development work with all test dependencies:

```bash
git clone https://github.com/datacompose/datacompose.git
cd datacompose
uv pip install -e ".[dev]"
```

This installs additional development tools:
- pytest for testing
- black for code formatting
- mypy for type checking
- ruff for linting

## Documentation Development

To work on the documentation:

```bash
uv pip install -e ".[docs]"
```

This installs MkDocs with the Material theme and all necessary plugins.

## Using uv (Recommended)

We recommend using [uv](https://github.com/astral-sh/uv) for faster dependency management:

```bash
# Install uv
curl -LsSf https://astral.sh/uv/install.sh | sh

# Create virtual environment
uv venv

# Activate virtual environment
source .venv/bin/activate  # On Unix/macOS
# or
.venv\Scripts\activate  # On Windows

# Install datacompose
uv pip install datacompose
```

## Next Steps

Once installed, proceed to the [Quick Start](quickstart.md) guide to create your first data transformation pipeline.