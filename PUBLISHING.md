# Publishing Datacompose to PyPI

This guide explains how to publish Datacompose to PyPI.

## Prerequisites

1. **PyPI Account**: Create accounts at:
   - [PyPI](https://pypi.org) (production)
   - [TestPyPI](https://test.pypi.org) (testing)

2. **API Tokens**: Generate API tokens for both PyPI and TestPyPI:
   - Go to Account Settings → API tokens
   - Create a token with "Upload packages" scope
   - Save tokens securely

3. **GitHub Secrets**: Add tokens to your repository:
   - Go to Settings → Secrets → Actions
   - Add `PYPI_API_TOKEN` (for PyPI)
   - Add `TEST_PYPI_API_TOKEN` (for TestPyPI)

## Manual Publishing

### 1. Update Version

Edit `pyproject.toml`:
```toml
version = "0.2.0"  # Update version number
```

### 2. Build Package

```bash
# Clean previous builds
rm -rf dist/ build/ *.egg-info

# Install build tools
pip install build twine

# Build the package
python -m build

# Check the package
twine check dist/*
```

### 3. Test on TestPyPI

```bash
# Upload to TestPyPI
twine upload --repository testpypi dist/*

# Test installation
pip install --index-url https://test.pypi.org/simple/ --extra-index-url https://pypi.org/simple/ datacompose

# Verify it works
datacompose --version
```

### 4. Publish to PyPI

```bash
# Upload to PyPI
twine upload dist/*

# Test installation
pip install datacompose
```

## Automated Publishing (Recommended)

### GitHub Release

1. **Create a new release** on GitHub:
   - Go to Releases → Create new release
   - Create a new tag (e.g., `v0.2.0`)
   - Write release notes
   - Publish release

2. **GitHub Actions** will automatically:
   - Build the package
   - Run tests on multiple Python versions
   - Publish to PyPI

### Manual Trigger

You can also trigger publishing manually:

1. Go to Actions → "Publish to PyPI"
2. Click "Run workflow"
3. Choose to publish to TestPyPI or PyPI

## Version Management

### Semantic Versioning

Follow [Semantic Versioning](https://semver.org/):
- **MAJOR.MINOR.PATCH** (e.g., 1.2.3)
- **MAJOR**: Breaking changes
- **MINOR**: New features (backwards compatible)
- **PATCH**: Bug fixes

### Pre-release Versions

For testing:
```toml
version = "0.2.0a1"  # Alpha
version = "0.2.0b1"  # Beta
version = "0.2.0rc1" # Release candidate
```

## Checklist Before Publishing

- [ ] Update version in `pyproject.toml`
- [ ] Update CHANGELOG.md
- [ ] Run tests: `pytest`
- [ ] Build docs: `cd docs && make html`
- [ ] Test installation locally
- [ ] Create git tag: `git tag v0.2.0`
- [ ] Push tag: `git push origin v0.2.0`

## Troubleshooting

### Package Not Found After Publishing

PyPI has a CDN cache. Wait 5-10 minutes or use:
```bash
pip install datacompose --no-cache-dir
```

### Build Errors

Ensure all files are included:
```bash
# Check MANIFEST.in
python setup.py check --manifest

# See what's included
tar -tzf dist/datacompose-*.tar.gz
```

### Authentication Errors

Use API tokens, not username/password:
```bash
# Create ~/.pypirc
[pypi]
username = __token__
password = pypi-YOUR-TOKEN-HERE

[testpypi]
username = __token__
password = pypi-YOUR-TEST-TOKEN-HERE
```

## Post-Publishing

After successful publishing:

1. **Verify Installation**:
   ```bash
   pip install datacompose
   datacompose --version
   ```

2. **Update Documentation**:
   - Update installation instructions
   - Update badges in README

3. **Announce Release**:
   - Create GitHub release notes
   - Update project website
   - Notify users

## Resources

- [Python Packaging Guide](https://packaging.python.org)
- [PyPI Documentation](https://pypi.org/help/)
- [setuptools Documentation](https://setuptools.pypa.io/)