# Datacompose Documentation

This documentation is built with [MkDocs](https://www.mkdocs.org/) and the [Material for MkDocs](https://squidfunk.github.io/mkdocs-material/) theme.

## Setup with uv

### Install dependencies

```bash
# Install documentation dependencies
uv pip install -e ".[docs]"
```

### Development server

```bash
# Start the development server
uv run mkdocs serve
```

The documentation will be available at `http://127.0.0.1:8000`.

### Build documentation

```bash
# Build static site
uv run mkdocs build
```

The built site will be in the `site/` directory.

### Deploy to GitHub Pages

```bash
# Deploy to gh-pages branch
uv run mkdocs gh-deploy
```

## Alternative: Using pip

If you prefer using pip instead of uv:

```bash
# Install dependencies
pip install -e ".[docs]"

# Serve locally
mkdocs serve

# Build
mkdocs build

# Deploy
mkdocs gh-deploy
```

## Project Structure

```
docs/
├── index.md                 # Homepage
├── getting-started/         # Installation and quickstart
│   ├── index.md
│   ├── installation.md
│   └── quickstart.md
├── user-guide/             # Detailed guides
│   ├── index.md
│   ├── core-concepts.md
│   ├── primitives.md
│   ├── pipelines.md
│   └── configuration.md
├── api/                    # API reference
│   ├── index.md
│   ├── cli.md
│   ├── email-primitives.md
│   ├── address-primitives.md
│   ├── phone-primitives.md
│   └── custom-primitives.md
├── examples/               # Usage examples
│   ├── index.md
│   ├── data-cleaning.md
│   ├── etl-pipelines.md
│   └── custom-transformations.md
├── stylesheets/           # Custom CSS
├── javascripts/           # Custom JavaScript
└── assets/                # Images and other assets
```

## Writing Documentation

### Adding a new page

1. Create a new `.md` file in the appropriate directory
2. Add the page to the navigation in `mkdocs.yml`
3. Write your content using Markdown

### Using Material theme features

The Material theme provides many useful features:

- **Admonitions**: `!!! note "Title"` for callout boxes
- **Code blocks**: ` ```python` with syntax highlighting
- **Tabs**: For showing alternative code examples
- **Tables**: Standard Markdown tables with sorting
- **Icons**: `:material-icon-name:` for Material Design icons

See the [Material for MkDocs documentation](https://squidfunk.github.io/mkdocs-material/) for all available features.

## Live Reload

MkDocs includes live reload functionality. When running `mkdocs serve`, any changes to the documentation files will automatically trigger a browser refresh.

## Versioning

We use `mike` for documentation versioning:

```bash
# Deploy a new version
uv run mike deploy --push --update-aliases 0.2.4 latest

# Set default version
uv run mike set-default --push latest

# List versions
uv run mike list
```

## Contributing

When contributing to the documentation:

1. Follow the existing structure and style
2. Use clear, concise language
3. Include code examples where appropriate
4. Test all code examples
5. Preview your changes locally before submitting