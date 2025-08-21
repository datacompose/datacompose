#!/usr/bin/env python3
"""
Sync transformer functions from Python source to markdown documentation.
Maps transformers/text/clean_X to content/transformers/X format.
"""

import ast
from pathlib import Path
from typing import List, Dict


class TransformerDocGenerator:
    """Generate documentation from transformer Python source files."""
    
    # Mapping from source directory names to output names
    NAME_MAPPING = {
        'addresses': 'addresses',
        'emails': 'emails',
        'phone_numbers': 'phone_numbers'
    }
    
    # Transformers that should use table-only display
    TABLE_ONLY_TRANSFORMERS = {'addresses', 'emails', 'phone-numbers'}
    
    def __init__(self):
        # Fixed paths for your project
        self.source_dir = Path('/Users/tccole/Projects/datacompose/datacompose/transformers')
        self.output_dir = Path('content')
    
    def extract_registered_functions(self, python_file: Path, registry_name: str) -> List[Dict]:
        """Extract functions with @registry.register() decorator."""
        with open(python_file, 'r') as f:
            source = f.read()
        
        try:
            tree = ast.parse(source)
        except SyntaxError as e:
            print(f"Error parsing {python_file}: {e}")
            return []
        
        source_lines = source.split('\n')
        functions = []
        
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef):
                # Check if function has the register decorator
                has_register = False
                decorator_line = None
                
                for decorator in node.decorator_list:
                    # Get the line number of the first decorator
                    if decorator_line is None and hasattr(decorator, 'lineno'):
                        decorator_line = decorator.lineno - 1  # Convert to 0-based
                    
                    # Check if this is a register decorator for our registry
                    if isinstance(decorator, ast.Attribute):
                        if (hasattr(decorator.value, 'id') and 
                            decorator.value.id == registry_name and 
                            decorator.attr == 'register'):
                            has_register = True
                    elif isinstance(decorator, ast.Call):
                        if (isinstance(decorator.func, ast.Attribute) and
                            hasattr(decorator.func.value, 'id') and
                            decorator.func.value.id == registry_name and
                            decorator.func.attr == 'register'):
                            has_register = True
                
                if has_register and hasattr(node, 'lineno') and hasattr(node, 'end_lineno'):
                    # Extract complete function including decorator
                    start_line = decorator_line if decorator_line is not None else node.lineno - 1
                    end_line = node.end_lineno
                    
                    # Get the function source
                    function_lines = source_lines[start_line:end_line]
                    function_source = '\n'.join(function_lines)
                    
                    # Extract function description
                    description = self.parse_function_description(function_source)
                    
                    functions.append({
                        'name': node.name,
                        'source': function_source,
                        'description': description,
                        'line_number': node.lineno
                    })
        
        # Sort by line number to maintain order
        functions.sort(key=lambda x: x['line_number'])
        return functions
    
    def categorize_functions(self, functions: List[Dict]) -> Dict[str, List[Dict]]:
        """Categorize functions by their name patterns."""
        categories = {
            'Extract Functions': [],
            'Transform Functions': [],
            'Validation Functions': [],
            'Utility Functions': []
        }
        
        for func in functions:
            name = func['name'].lower()
            
            # Categorize based on function name patterns
            if name.startswith('extract_'):
                categories['Extract Functions'].append(func)
            elif name.startswith('transform_') or name.startswith('standardize_'):
                categories['Transform Functions'].append(func)
            elif name.startswith('validate_') or name.startswith('is_') or name.startswith('has_'):
                categories['Validation Functions'].append(func)
            else:
                categories['Utility Functions'].append(func)
        
        # Remove empty categories
        return {k: v for k, v in categories.items() if v}
    
    def parse_function_description(self, source: str) -> str:
        """Extract the description from function docstring."""
        import re
        
        # Extract docstring
        docstring_match = re.search(r'"""(.*?)"""', source, re.DOTALL)
        if not docstring_match:
            return ""
        
        docstring = docstring_match.group(1).strip()
        
        # Find the description (text before Args: section)
        # Split by Args: or Returns: to get just the description part
        description_match = re.search(r'^(.*?)(?:\n\s*(?:Args?|Returns?|Raises?|Examples?|Notes?):|$)', docstring, re.DOTALL)
        if description_match:
            description = description_match.group(1).strip()
            # Join multiple lines and clean up whitespace
            description = ' '.join(line.strip() for line in description.split('\n') if line.strip())
            return description
        
        return ""
    
    def parse_function_params(self, source: str) -> List[Dict]:
        """Parse parameters from function docstring."""
        import re
        
        # Extract docstring
        docstring_match = re.search(r'"""(.*?)"""', source, re.DOTALL)
        if not docstring_match:
            return []
        
        docstring = docstring_match.group(1)
        params = []
        
        # Find Args section
        args_match = re.search(r'Args?:\s*\n(.*?)(?:\n\s*Returns?:|$)', docstring, re.DOTALL)
        if args_match:
            args_text = args_match.group(1)
            # Parse each parameter line
            param_lines = args_text.strip().split('\n')
            for line in param_lines:
                line = line.strip()
                if line and ':' in line:
                    # Parse "param_name (type, optional): description" format
                    param_match = re.match(r'(\w+)(?:\s*\(([^)]*)\))?\s*:\s*(.+)', line)
                    if param_match:
                        param_name = param_match.group(1)
                        type_info = param_match.group(2) if param_match.group(2) else 'Column'
                        description = param_match.group(3).strip()
                        
                        # Check if parameter is optional
                        is_optional = 'optional' in type_info.lower() if type_info else False
                        
                        # Clean up type by removing 'optional'
                        param_type = type_info
                        if type_info:
                            param_type = re.sub(r',?\s*optional', '', type_info, flags=re.IGNORECASE).strip()
                            if not param_type:
                                param_type = 'Column'
                        
                        # Extract default value from description
                        default_value = None
                        # Match patterns like "(default 2)" or "default: 2" or "default = 2"
                        default_match = re.search(r'\(?\s*[Dd]efault(?:s)?(?:\s+is|\s*[=:]|\s+)?\s*([^),\n]+)\)?', description)
                        if default_match:
                            default_value = default_match.group(1).strip()
                            # Clean up the description by removing the default part
                            description = re.sub(r'\(?\s*[Dd]efault(?:s)?(?:\s+is|\s*[=:]|\s+)?[^),\n]+\)?', '', description).strip()
                        
                        params.append({
                            'name': param_name,
                            'type': param_type,
                            'description': description,
                            'required': not is_optional,
                            'default': default_value
                        })
        
        return params
    
    def generate_markdown(self, functions: List[Dict], transformer_name: str, 
                         registry_name: str) -> str:
        """Generate markdown content for a transformer."""
        # Determine if this transformer should use table-only
        table_only = transformer_name in self.TABLE_ONLY_TRANSFORMERS
        
        # Build markdown content
        lines = []
        
        # Add script tag with ParamsTable import for table-only mode
        if table_only:
            lines.append("<script>")
            lines.append("  import ParamsTable from '$lib/components/ParamsTable.svelte';")
            lines.append("</script>")
            lines.append("")
        
        lines.append(f"# API Reference\n")
        
        # Categorize and add functions
        categories = self.categorize_functions(functions)
        
        for category, funcs in categories.items():
            lines.append(f"\n## {category}\n")
            
            for func in funcs:
                if table_only:
                    # For table-only mode, add heading and ParamsTable component directly
                    lines.append(f"\n### {registry_name}.{func['name']}\n")
                    
                    # Add function description if available
                    if func.get('description'):
                        lines.append(f"{func['description']}\n")
                    
                    # Parse parameters from the function source
                    params = self.parse_function_params(func['source'])
                    if params:
                        # Generate ParamsTable component directly
                        import json
                        params_json = json.dumps(params)
                        lines.append(f'<ParamsTable params={{{params_json}}} title="" />\n')
                else:
                    # For normal mode, include heading and code block
                    lines.append(f"\n### {registry_name}.{func['name']}")
                    lines.append("\n```python")
                    lines.append(f"\n{func['source']}")
                    lines.append("\n```\n")
        
        return '\n'.join(lines)
    
    def get_registry_name(self, source_dir_name: str) -> str:
        """Get the registry name from the source directory name."""
        # Special cases
        if source_dir_name == 'phone_numbers':
            return 'phones'
        # Map clean_X to X, replacing underscores with nothing for registry names
        if source_dir_name.startswith('clean_'):
            return source_dir_name.replace('clean_', '').replace('_', '')
        return source_dir_name.replace('_', '')
    
    def process_transformer(self, source_path: Path):
        """Process a single transformer directory."""
        # Get the transformer names
        source_name = source_path.name  # e.g., 'addresses'
        output_name = self.NAME_MAPPING.get(source_name, source_name)  # e.g., 'addresses'
        registry_name = self.get_registry_name(source_name)  # e.g., 'addresses'
        
        print(f"Processing {source_name} -> {output_name} (registry: {registry_name})")
        
        # Look for dialect directories
        for dialect_dir in source_path.iterdir():
            if not dialect_dir.is_dir() or dialect_dir.name.startswith('.'):
                continue
            
            # Look for pyspark_primitives.py
            primitives_file = dialect_dir / 'pyspark_primitives.py'
            if not primitives_file.exists():
                print(f"  No primitives file found at {primitives_file}")
                continue
            
            print(f"  Found {primitives_file}")
            
            # Extract functions
            functions = self.extract_registered_functions(primitives_file, registry_name)
            
            if not functions:
                print(f"  No registered functions found")
                continue
            
            print(f"  Extracted {len(functions)} functions")
            
            # Generate markdown
            markdown_content = self.generate_markdown(functions, output_name, registry_name)
            
            # Write to output
            output_path = self.output_dir / 'transformers' / output_name / dialect_dir.name / '+page.md'
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            with open(output_path, 'w') as f:
                f.write(markdown_content)
            
            print(f"  Written to {output_path}")
    
    def sync_all(self):
        """Sync all transformers from source to documentation."""
        text_dir = self.source_dir / 'text'
        
        if not text_dir.exists():
            print(f"Source directory not found: {text_dir}")
            return
        
        # Process each transformer in the text directory
        for transformer_dir in text_dir.iterdir():
            if transformer_dir.is_dir() and not transformer_dir.name.startswith('.'):
                self.process_transformer(transformer_dir)
        
        print("\nSync complete!")


if __name__ == '__main__':
    generator = TransformerDocGenerator()
    generator.sync_all()