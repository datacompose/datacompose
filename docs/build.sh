#!/bin/bash

echo "Building Datacompose Documentation..."

# Install docs dependencies if not already installed
pip install -q -e ".[docs]" 2>/dev/null || echo "Dependencies already installed"

# Clean previous builds
make clean

# Build HTML documentation
make html

echo ""
echo "Documentation built successfully!"
echo "To view the docs, run:"
echo "  cd docs && make serve"
echo "Then open http://localhost:5500 in your browser"