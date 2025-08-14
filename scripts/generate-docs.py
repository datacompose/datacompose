#!/usr/bin/env python3
"""
Generate markdown documentation by scanning the transformers directory.
"""

import os
import ast
import json
from pathlib import Path
from typing import Dict, List, Any, Optional

def extract_functions_from_file(file_path: Path) -> List[Dict[str, Any]]:
    """Extract function definitions and their docstrings from a Python file."""
    functions = []
    
    try:
        with open(file_path, 'r') as f:
            tree = ast.parse(f.read())
        
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef):
                # Skip private functions
                if node.name.startswith('_'):
                    continue
                
                # Extract docstring
                docstring = ast.get_docstring(node)
                
                # Extract parameters
                params = []
                for arg in node.args.args:
                    if arg.arg not in ['self', 'df']:  # Skip self and df
                        params.append(arg.arg)
                
                functions.append({
                    'name': node.name,
                    'docstring': docstring or 'No description available',
                    'params': params
                })
    
    except Exception as e:
        print(f"Error parsing {file_path}: {e}")
    
    return functions


def scan_transformers_directory(transformers_dir: Path) -> Dict[str, Dict[str, Any]]:
    """Scan the transformers directory and extract all primitives."""
    primitives = {}
    
    # Map directory names to friendly titles
    name_mapping = {
        'clean_emails': {
            'title': 'Email Primitives',
            'description': 'Email cleaning, validation, and transformation functions',
            'import_path': 'clean_emails'
        },
        'clean_addresses': {
            'title': 'Address Primitives', 
            'description': 'Address parsing, validation, and standardization tools',
            'import_path': 'clean_addresses'
        },
        'clean_phone_numbers': {
            'title': 'Phone Number Primitives',
            'description': 'Phone number validation, formatting, and analysis',
            'import_path': 'clean_phone_numbers'
        }
    }
    
    for subdir in transformers_dir.iterdir():
        if subdir.is_dir() and not subdir.name.startswith('_'):
            primitive_name = subdir.name
            
            # Get metadata
            metadata = name_mapping.get(primitive_name, {
                'title': primitive_name.replace('_', ' ').title(),
                'description': f'{primitive_name} transformation functions',
                'import_path': primitive_name
            })
            
            # Scan for Python files
            methods = []
            for py_file in subdir.glob('*.py'):
                if py_file.name not in ['__init__.py', 'setup.py']:
                    functions = extract_functions_from_file(py_file)
                    for func in functions:
                        func['source_file'] = py_file.name
                    methods.extend(functions)
            
            if methods:
                primitives[primitive_name] = {
                    'title': metadata['title'],
                    'description': metadata['description'],
                    'import_path': metadata['import_path'],
                    'methods': methods,
                    'directory': primitive_name
                }
    
    return primitives


def generate_markdown_for_primitive(name: str, primitive: Dict[str, Any]) -> str:
    """Generate markdown documentation for a single primitive library."""
    
    title = primitive['title']
    description = primitive['description']
    import_path = primitive['import_path']
    methods = primitive['methods']
    
    # Group methods by source file
    methods_by_file = {}
    for method in methods:
        source = method.get('source_file', 'unknown')
        if source not in methods_by_file:
            methods_by_file[source] = []
        methods_by_file[source].append(method)
    
    # Build methods documentation
    methods_doc = ""
    for source_file, file_methods in sorted(methods_by_file.items()):
        if source_file != 'unknown':
            methods_doc += f"\n### {source_file.replace('.py', '').replace('_', ' ').title()}\n\n"
        
        for method in file_methods:
            params_str = ', '.join(method['params']) if method['params'] else 'No additional parameters'
            
            # Parse docstring for better formatting
            docstring_lines = method['docstring'].split('\n') if method['docstring'] else []
            description_line = docstring_lines[0] if docstring_lines else 'No description available'
            
            methods_doc += f"""#### `{method['name']}()`

{description_line}

**Parameters:** {params_str}

"""
    
    # Generate example pipelines based on available methods
    example_methods = methods[:3] if len(methods) >= 3 else methods
    example_pipeline = ""
    if example_methods:
        method_calls = '\n        '.join([f".{m['name']}(column_name)" for m in example_methods])
        example_pipeline = f"""## Example Pipeline

```python
from datacompose import {import_path}

@{import_path}.compose()
def process_pipeline(df):
    return (df
        {method_calls}
    )

# Apply to your DataFrame
result_df = process_pipeline(spark_df)
```"""
    
    markdown = f"""---
title: {title}
description: {description}
generated: true
---

# {title}

{description}

## Import

```python
from datacompose import {import_path}
```

## Available Methods
{methods_doc}

{example_pipeline}

## Using the @compose Decorator

The `@compose` decorator allows you to chain multiple operations efficiently:

```python
@{import_path}.compose()
def my_pipeline(df):
    return df.{methods[0]['name'] if methods else 'method'}("column_name")
```

## Custom Primitives

You can extend this library with custom primitives:

```python
from pyspark.sql import functions as F
from datacompose import {import_path}

@{import_path}.register_primitive
def custom_transformation(df, column_name):
    \"\"\"Your custom transformation logic\"\"\"
    return df.withColumn(
        f"{{column_name}}_transformed",
        F.col(column_name)  # Your logic here
    )
```

---
*This documentation was automatically generated from the source code.*
"""
    
    return markdown


def generate_individual_method_docs(primitives: Dict[str, Dict[str, Any]], output_dir: Path):
    """Generate individual markdown files for each method."""
    for primitive_name, primitive_data in primitives.items():
        # Create directory for this primitive's methods
        primitive_dir = output_dir / 'methods' / primitive_name
        primitive_dir.mkdir(parents=True, exist_ok=True)
        
        for method in primitive_data['methods']:
            method_markdown = f"""---
title: {method['name']}
primitive: {primitive_data['title']}
---

# `{method['name']}()`

{method['docstring']}

## Parameters

{', '.join(method['params']) if method['params'] else 'No additional parameters beyond the DataFrame and column name.'}

## Usage

```python
from datacompose import {primitive_data['import_path']}

@{primitive_data['import_path']}.compose()
def pipeline(df):
    return df.{method['name']}("column_name")
```

## Source

Found in: `transformers/{primitive_name}/{method.get('source_file', 'unknown')}`

---
[← Back to {primitive_data['title']}](/primitives/{primitive_name})
"""
            
            method_file = primitive_dir / f"{method['name']}.md"
            with open(method_file, 'w') as f:
                f.write(method_markdown)


def main():
    """Main function to generate documentation from transformers directory."""
    
    # Get paths
    script_dir = Path(__file__).parent
    project_root = script_dir.parent
    transformers_dir = project_root / 'datacompose' / 'transformers'
    
    if not transformers_dir.exists():
        print(f"❌ Transformers directory not found at: {transformers_dir}")
        return
    
    print(f"📂 Scanning transformers directory: {transformers_dir}")
    
    # Scan the transformers directory
    primitives = scan_transformers_directory(transformers_dir)
    
    if not primitives:
        print("❌ No primitives found in transformers directory")
        return
    
    print(f"📊 Found {len(primitives)} primitive libraries:")
    for name in primitives:
        print(f"  - {name}: {len(primitives[name]['methods'])} methods")
    
    # Create output directory
    output_dir = project_root / 'docs-svelte' / 'content' / 'primitives'
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Generate markdown for each primitive
    for name, primitive_data in primitives.items():
        markdown = generate_markdown_for_primitive(name, primitive_data)
        
        # Use directory name for consistency with routes
        file_name = name.replace('clean_', '').replace('_', '-') + '.md'
        file_path = output_dir / file_name
        
        with open(file_path, 'w') as f:
            f.write(markdown)
        
        print(f"✅ Generated: {file_path}")
    
    # Generate individual method documentation (optional)
    generate_individual_method_docs(primitives, output_dir)
    print(f"✅ Generated individual method documentation in {output_dir / 'methods'}")
    
    # Generate index file
    index_markdown = f"""---
title: Primitive Libraries
description: Auto-generated documentation for Datacompose primitives
---

# Datacompose Primitive Libraries

These primitive libraries provide powerful data transformation functions for PySpark DataFrames.

## Available Libraries

"""
    
    for name, primitive_data in primitives.items():
        route_name = name.replace('clean_', '').replace('_', '-')
        index_markdown += f"""
### [{primitive_data['title']}](/primitives/{route_name})

{primitive_data['description']}

- **Import**: `from datacompose import {primitive_data['import_path']}`
- **Methods**: {len(primitive_data['methods'])} available transformations

"""
    
    index_markdown += """
---
*This documentation was automatically generated from the source code.*
"""
    
    index_path = output_dir / 'index.md'
    with open(index_path, 'w') as f:
        f.write(index_markdown)
    
    print(f"✅ Generated index: {index_path}")
    
    # Create a JSON manifest for use by the Svelte app
    manifest = {
        'generated': True,
        'primitives': {
            name: {
                'title': data['title'],
                'description': data['description'],
                'import_path': data['import_path'],
                'method_count': len(data['methods']),
                'methods': [m['name'] for m in data['methods']]
            }
            for name, data in primitives.items()
        }
    }
    
    manifest_path = output_dir / 'manifest.json'
    with open(manifest_path, 'w') as f:
        json.dump(manifest, f, indent=2)
    
    print(f"✅ Generated manifest: {manifest_path}")
    
    print("\n📚 Documentation generation complete!")


if __name__ == "__main__":
    main()