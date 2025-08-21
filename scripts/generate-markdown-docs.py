#!/usr/bin/env python3
"""Generate markdown documentation files for all dialects."""

import os
import sys
from pathlib import Path
from typing import Dict, List, Any
import json
import ast
import inspect

def extract_function_info(func_code: str) -> Dict[str, Any]:
    """Extract function signature and docstring from function code."""
    try:
        tree = ast.parse(func_code)
        func_def = tree.body[0]
        
        # Get function signature
        args = []
        defaults_start = len(func_def.args.args) - len(func_def.args.defaults)
        
        for i, arg in enumerate(func_def.args.args):
            if arg.arg != 'self':
                arg_str = arg.arg
                # Add type annotation if present
                if arg.annotation:
                    arg_str += f": {ast.unparse(arg.annotation)}"
                # Add default value if present
                if i >= defaults_start:
                    default_idx = i - defaults_start
                    default_val = ast.unparse(func_def.args.defaults[default_idx])
                    arg_str += f" = {default_val}"
                args.append(arg_str)
        
        # Get docstring
        docstring = ast.get_docstring(func_def) or ""
        
        # Get decorator info
        has_compose = any(
            (isinstance(d, ast.Attribute) and d.attr == 'compose') or
            (isinstance(d, ast.Name) and d.id == 'compose')
            for d in func_def.decorator_list
        )
        
        return {
            'name': func_def.name,
            'args': args,
            'docstring': docstring,
            'has_compose': has_compose,
            'signature': f"{func_def.name}({', '.join(args)})"
        }
    except:
        return None

def scan_transformers_directory(transformers_dir: Path) -> Dict[str, Dict[str, Dict[str, List[Dict[str, Any]]]]]:
    """Scan the transformers directory and extract all primitives organized by library and dialect."""
    primitives = {}
    
    # Define the primitive library directories
    library_dirs = {
        'emails': 'emails',
        'addresses': 'addresses',
        'phone-numbers': 'phone_numbers'
    }
    
    for lib_name, lib_dir in library_dirs.items():
        primitives[lib_name] = {}
        lib_path = transformers_dir / 'text' / lib_dir
        
        if not lib_path.exists():
            continue
            
        # Check for different dialect subdirectories
        for dialect_dir in lib_path.iterdir():
            if dialect_dir.is_dir() and not dialect_dir.name.startswith('_'):
                dialect = dialect_dir.name
                primitives[lib_name][dialect] = []
                
                # Scan all Python files in the dialect directory
                for py_file in dialect_dir.glob('*.py'):
                    if py_file.name.startswith('_'):
                        continue
                        
                    with open(py_file, 'r') as f:
                        content = f.read()
                    
                    # Parse the file to find all functions
                    try:
                        tree = ast.parse(content)
                        for node in ast.walk(tree):
                            if isinstance(node, ast.FunctionDef):
                                if not node.name.startswith('_'):
                                    func_info = extract_function_info(ast.unparse(node))
                                    if func_info:
                                        func_info['file'] = py_file.stem
                                        primitives[lib_name][dialect].append(func_info)
                    except:
                        continue
                
                # Sort functions alphabetically
                primitives[lib_name][dialect].sort(key=lambda x: x['name'])
    
    return primitives

def get_dialect_install_instructions(dialect: str) -> str:
    """Get installation instructions for a specific dialect."""
    instructions = {
        'pyspark': """```bash
pip install datacompose[pyspark]
```""",
        'postgresql': """```bash
pip install datacompose[postgresql]
```""",
        'pandas': """```bash
pip install datacompose[pandas]
```""",
        'snowflake': """```bash
pip install datacompose[snowflake]
```""",
        'bigquery': """```bash
pip install datacompose[bigquery]
```"""
    }
    return instructions.get(dialect, f"```bash\npip install datacompose[{dialect}]\n```")

def get_dialect_import(lib_name: str, dialect: str) -> str:
    """Get import statement for a specific dialect."""
    lib_map = {
        'emails': 'emails',
        'addresses': 'addresses',
        'phone-numbers': 'phones'
    }
    lib_import = lib_map.get(lib_name, lib_name)
    
    if dialect == 'pyspark':
        return f"from datacompose.transformers.text.{lib_import} import {lib_import}"
    else:
        return f"from datacompose.transformers.text.{lib_import}.{dialect} import {lib_import}"

def generate_markdown_file(lib_name: str, dialect: str, methods: List[Dict[str, Any]], output_dir: Path):
    """Generate a markdown file for a specific library and dialect."""
    # Create subdirectory for dialect
    dialect_dir = output_dir / dialect
    dialect_dir.mkdir(parents=True, exist_ok=True)
    output_file = dialect_dir / "+page.md"
    
    # Create frontmatter
    frontmatter = f"""---
title: {dialect.upper()} Implementation
description: {lib_name.replace('-', ' ').title()} transformers for {dialect.upper()}
---"""
    
    # Create content
    content = [frontmatter, ""]
    
    # Add title
    content.append(f"# {dialect.upper()} Implementation")
    content.append("")
    
    # Add installation section
    content.append("## Installation")
    content.append("")
    content.append(get_dialect_install_instructions(dialect))
    content.append("")
    
    # Add usage section
    content.append("## Usage")
    content.append("")
    content.append("```python")
    content.append(get_dialect_import(lib_name, dialect))
    content.append("")
    if dialect == 'pyspark':
        content.append("from pyspark.sql import SparkSession")
        content.append("")
        content.append("spark = SparkSession.builder.appName('DataCompose').getOrCreate()")
        content.append("df = spark.read.parquet('data.parquet')")
        content.append("")
        lib_short = lib_name.split('-')[0]
        if lib_short == 'phone':
            lib_short = 'phones'
        content.append(f"# Using the @{lib_short}.compose() decorator")
        content.append(f"@{lib_short}.compose()")
        content.append("def clean_data(df):")
        content.append("    return df")
        content.append("")
        content.append("cleaned_df = clean_data(df)")
    content.append("```")
    content.append("")
    
    # Add methods section
    content.append("## Available Methods")
    content.append("")
    content.append(f"Total methods: **{len(methods)}**")
    content.append("")
    
    # Group methods by file
    methods_by_file = {}
    for method in methods:
        file = method.get('file', 'general')
        if file not in methods_by_file:
            methods_by_file[file] = []
        methods_by_file[file].append(method)
    
    for file, file_methods in sorted(methods_by_file.items()):
        if len(methods_by_file) > 1:
            content.append(f"### {file.replace('_', ' ').title()}")
            content.append("")
        
        for method in file_methods:
            content.append(f"#### `{method['signature']}`")
            content.append("")
            
            if method['docstring']:
                content.append(method['docstring'])
                content.append("")
            
            if method['has_compose']:
                content.append("*This method is available via the `@compose()` decorator*")
                content.append("")
            
            # Add example usage
            content.append("```python")
            if method['args']:
                args_example = ', '.join([arg.split(':')[0].split('=')[0] for arg in method['args']])
                content.append(f"df = df.{method['name']}({args_example})")
            else:
                content.append(f"df = df.{method['name']}()")
            content.append("```")
            content.append("")
    
    # Write the file
    with open(output_file, 'w') as f:
        f.write('\n'.join(content))
    
    print(f"  Created: {output_file}")

def generate_index_page(lib_name: str, dialects: List[str], output_dir: Path):
    """Generate an index page that lists all available dialects."""
    output_file = output_dir / "+page.md"
    
    frontmatter = f"""---
title: {lib_name.replace('-', ' ').title()}
description: Data cleaning transformers for {lib_name.replace('-', ' ')}
---"""
    
    content = [frontmatter, ""]
    
    # Add title
    content.append(f"# {lib_name.replace('-', ' ').title()}")
    content.append("")
    
    # Add overview
    lib_descriptions = {
        'emails': "Clean and validate email addresses in your data pipelines.",
        'addresses': "Parse, standardize, and validate physical addresses.",
        'phone-numbers': "Format and validate phone numbers across different regions."
    }
    
    content.append(lib_descriptions.get(lib_name, f"Transform and clean {lib_name.replace('-', ' ')} in your data."))
    content.append("")
    
    content.append("## Available Implementations")
    content.append("")
    content.append("Choose your data processing framework:")
    content.append("")
    
    for dialect in sorted(dialects):
        dialect_descriptions = {
            'pyspark': "Apache Spark - Distributed data processing",
            'postgresql': "PostgreSQL - SQL database transformations",
            'pandas': "Pandas - Python dataframe operations",
            'snowflake': "Snowflake - Cloud data warehouse",
            'bigquery': "Google BigQuery - Serverless data warehouse"
        }
        desc = dialect_descriptions.get(dialect, dialect.upper())
        content.append(f"- [{desc}](./{lib_name}/{dialect})")
    
    content.append("")
    
    # Add quick start example
    content.append("## Quick Start")
    content.append("")
    content.append("```python")
    lib_import = {
        'emails': 'emails',
        'addresses': 'addresses',
        'phone-numbers': 'phones'
    }.get(lib_name, lib_name)
    content.append(f"from datacompose.transformers.text.{lib_import} import {lib_import}")
    content.append("")
    content.append(f"# Use the @{lib_import}.compose() decorator to chain operations")
    content.append(f"@{lib_import}.compose()")
    content.append("def clean_data(df):")
    content.append("    return df")
    content.append("```")
    content.append("")
    
    # Write the file
    with open(output_file, 'w') as f:
        f.write('\n'.join(content))
    
    print(f"  Created: {output_file}")

def main():
    # Get project root
    project_root = Path.cwd()
    while not (project_root / 'datacompose').exists() and project_root.parent != project_root:
        project_root = project_root.parent
    
    if not (project_root / 'datacompose').exists():
        print("Error: Could not find datacompose directory")
        sys.exit(1)
    
    transformers_dir = project_root / 'datacompose' / 'transformers'
    docs_dir = project_root / 'docs-svelte' / 'src' / 'routes' / 'primitives'
    
    print(f"Scanning transformers directory: {transformers_dir}")
    print(f"Output directory: {docs_dir}")
    print()
    
    # Scan for all primitives
    primitives = scan_transformers_directory(transformers_dir)
    
    # Generate markdown files for each library and dialect
    for lib_name, dialects in primitives.items():
        if not dialects:
            continue
            
        print(f"Generating documentation for {lib_name}:")
        lib_dir = docs_dir / lib_name
        lib_dir.mkdir(parents=True, exist_ok=True)
        
        # Generate index page
        generate_index_page(lib_name, list(dialects.keys()), lib_dir)
        
        # Generate dialect-specific pages
        for dialect, methods in dialects.items():
            if methods:
                generate_markdown_file(lib_name, dialect, methods, lib_dir)
        
        print()
    
    print("Documentation generation complete!")

if __name__ == "__main__":
    main()