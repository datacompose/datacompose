#!/usr/bin/env python3
"""
Generate markdown documentation for multiple dialect implementations.
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


def extract_sql_functions(file_path: Path) -> List[Dict[str, Any]]:
    """Extract SQL function definitions from .sql files."""
    functions = []
    
    try:
        with open(file_path, 'r') as f:
            content = f.read()
            
        # Simple regex to find CREATE FUNCTION statements
        import re
        pattern = r'CREATE\s+(?:OR\s+REPLACE\s+)?FUNCTION\s+(\w+)\s*\((.*?)\)'
        matches = re.finditer(pattern, content, re.IGNORECASE | re.DOTALL)
        
        for match in matches:
            func_name = match.group(1)
            params_str = match.group(2)
            
            # Parse parameters
            params = []
            if params_str.strip():
                param_parts = params_str.split(',')
                for part in param_parts:
                    # Extract parameter name (first word after trimming)
                    param_name = part.strip().split()[0] if part.strip() else None
                    if param_name:
                        params.append(param_name)
            
            functions.append({
                'name': func_name,
                'docstring': f'PostgreSQL function: {func_name}',
                'params': params
            })
    
    except Exception as e:
        print(f"Error parsing SQL file {file_path}: {e}")
    
    return functions


def scan_transformers_multi_dialect(transformers_dir: Path) -> Dict[str, Dict[str, Any]]:
    """Scan transformers directory for multiple dialect implementations."""
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
    
    # Supported dialects
    dialects = ['pyspark', 'postgres', 'pandas', 'snowflake', 'bigquery']
    
    for subdir in transformers_dir.iterdir():
        if subdir.is_dir() and not subdir.name.startswith('_'):
            primitive_name = subdir.name
            
            # Get metadata
            metadata = name_mapping.get(primitive_name, {
                'title': primitive_name.replace('_', ' ').title(),
                'description': f'{primitive_name} transformation functions',
                'import_path': primitive_name
            })
            
            # Scan for dialect implementations
            dialect_methods = {}
            
            for dialect in dialects:
                dialect_dir = subdir / dialect
                methods = []
                
                if dialect_dir.exists():
                    # Scan Python files
                    for py_file in dialect_dir.glob('*.py'):
                        if py_file.name not in ['__init__.py', 'setup.py']:
                            functions = extract_functions_from_file(py_file)
                            for func in functions:
                                func['source_file'] = py_file.name
                                func['dialect'] = dialect
                            methods.extend(functions)
                    
                    # Scan SQL files for postgres/snowflake/bigquery
                    if dialect in ['postgres', 'snowflake', 'bigquery']:
                        for sql_file in dialect_dir.glob('*.sql'):
                            functions = extract_sql_functions(sql_file)
                            for func in functions:
                                func['source_file'] = sql_file.name
                                func['dialect'] = dialect
                            methods.extend(functions)
                
                if methods:
                    dialect_methods[dialect] = methods
            
            if dialect_methods:
                primitives[primitive_name] = {
                    'title': metadata['title'],
                    'description': metadata['description'],
                    'import_path': metadata['import_path'],
                    'dialects': dialect_methods,
                    'directory': primitive_name
                }
    
    return primitives


def generate_tabbed_page(name: str, primitive: Dict[str, Any]) -> str:
    """Generate a Svelte page with tabs for different dialects."""
    
    title = primitive['title']
    description = primitive['description']
    dialects = primitive['dialects']
    
    # Start building the Svelte component
    svelte_content = f'''<script lang="ts">
\timport * as Tabs from "$lib/components/ui/tabs";
\timport {{ Card, CardContent, CardDescription, CardHeader, CardTitle }} from "$lib/components/ui/card";
\t
\tconst dialects = {json.dumps(list(dialects.keys()))};
</script>

<div class="space-y-6">
\t<header>
\t\t<h1 class="text-4xl font-bold mb-2">{title}</h1>
\t\t<p class="text-lg text-muted-foreground">{description}</p>
\t</header>

\t<Tabs.Root value="{list(dialects.keys())[0]}" class="w-full">
\t\t<Tabs.List class="grid w-full grid-cols-{len(dialects)}">
'''
    
    # Add tab triggers
    for dialect in dialects:
        dialect_label = {
            'pyspark': 'PySpark',
            'postgres': 'PostgreSQL',
            'pandas': 'Pandas',
            'snowflake': 'Snowflake',
            'bigquery': 'BigQuery'
        }.get(dialect, dialect.title())
        svelte_content += f'\t\t\t<Tabs.Trigger value="{dialect}">{dialect_label}</Tabs.Trigger>\n'
    
    svelte_content += '\t\t</Tabs.List>\n\n'
    
    # Add content for each dialect
    for dialect, methods in dialects.items():
        dialect_label = {
            'pyspark': 'PySpark',
            'postgres': 'PostgreSQL', 
            'pandas': 'Pandas',
            'snowflake': 'Snowflake',
            'bigquery': 'BigQuery'
        }.get(dialect, dialect.title())
        
        # Installation command
        install_cmd = {
            'pyspark': f'pip install datacompose[pyspark]',
            'postgres': f'CREATE EXTENSION datacompose_{name.replace("clean_", "")};',
            'pandas': f'pip install datacompose[pandas]',
            'snowflake': f'pip install datacompose[snowflake]',
            'bigquery': f'pip install datacompose[bigquery]'
        }.get(dialect, f'pip install datacompose[{dialect}]')
        
        svelte_content += f'''\t\t<Tabs.Content value="{dialect}" class="space-y-6 mt-6">
\t\t\t<Card>
\t\t\t\t<CardHeader>
\t\t\t\t\t<CardTitle>Installation</CardTitle>
\t\t\t\t\t<CardDescription>Set up {dialect_label} {title.lower()}</CardDescription>
\t\t\t\t</CardHeader>
\t\t\t\t<CardContent>
\t\t\t\t\t<pre class="bg-slate-900 text-slate-50 rounded-lg p-4"><code>{install_cmd}</code></pre>
\t\t\t\t</CardContent>
\t\t\t</Card>
\t\t\t
\t\t\t<Card>
\t\t\t\t<CardHeader>
\t\t\t\t\t<CardTitle>Available Methods</CardTitle>
\t\t\t\t\t<CardDescription>{len(methods)} methods available for {dialect_label}</CardDescription>
\t\t\t\t</CardHeader>
\t\t\t\t<CardContent>
\t\t\t\t\t<div class="grid gap-2">
'''
        
        # Add method list
        for method in methods[:10]:  # Show first 10
            svelte_content += f'''\t\t\t\t\t\t<div class="border-b pb-2">
\t\t\t\t\t\t\t<a href="/primitives/{name.replace("clean_", "").replace("_", "-")}/methods/{method['name']}" class="font-mono text-sm hover:text-primary">
\t\t\t\t\t\t\t\t{method['name']}()
\t\t\t\t\t\t\t</a>
\t\t\t\t\t\t\t<p class="text-xs text-muted-foreground mt-1">{method['docstring'].split('.')[0] if method['docstring'] else 'No description'}</p>
\t\t\t\t\t\t</div>
'''
        
        if len(methods) > 10:
            svelte_content += f'''\t\t\t\t\t\t<a href="/primitives/{name.replace("clean_", "").replace("_", "-")}/methods" class="text-sm text-primary hover:underline">
\t\t\t\t\t\t\tView all {len(methods)} methods →
\t\t\t\t\t\t</a>
'''
        
        svelte_content += '''\t\t\t\t\t</div>
\t\t\t\t</CardContent>
\t\t\t</Card>
\t\t</Tabs.Content>
'''
    
    svelte_content += '''\t</Tabs.Root>
</div>'''
    
    return svelte_content


def main():
    """Main function to generate multi-dialect documentation."""
    
    # Get paths
    script_dir = Path(__file__).parent
    project_root = script_dir.parent
    transformers_dir = project_root / 'datacompose' / 'transformers' / 'text'
    
    if not transformers_dir.exists():
        print(f"❌ Transformers directory not found at: {transformers_dir}")
        return
    
    print(f"📂 Scanning transformers directory: {transformers_dir}")
    
    # Scan the transformers directory
    primitives = scan_transformers_multi_dialect(transformers_dir)
    
    if not primitives:
        print("❌ No primitives found in transformers directory")
        return
    
    print(f"📊 Found {len(primitives)} primitive libraries:")
    for name, data in primitives.items():
        dialect_info = ', '.join([f"{d}: {len(m)} methods" for d, m in data['dialects'].items()])
        print(f"  - {name}: {dialect_info}")
    
    # Create output directory
    output_dir = project_root / 'docs-svelte' / 'src' / 'routes' / 'primitives'
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Generate Svelte pages with tabs for each primitive
    for name, primitive_data in primitives.items():
        clean_name = name.replace('clean_', '').replace('_', '-')
        primitive_dir = output_dir / clean_name
        primitive_dir.mkdir(parents=True, exist_ok=True)
        
        # Generate the tabbed page
        svelte_content = generate_tabbed_page(name, primitive_data)
        
        file_path = primitive_dir / '+page.svelte'
        with open(file_path, 'w') as f:
            f.write(svelte_content)
        
        print(f"✅ Generated: {file_path}")
    
    print("\n📚 Multi-dialect documentation generation complete!")


if __name__ == "__main__":
    main()