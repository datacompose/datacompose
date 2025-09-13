#!/usr/bin/env python3
"""
Sync transformer functions from Python source to markdown documentation.
Maps transformers/text/clean_X to content/transformers/X format.
"""

import ast
from pathlib import Path
from typing import List, Dict, Optional


class TransformerDocGenerator:
    """Generate documentation from transformer Python source files."""

    # Mapping from source directory names to output names
    NAME_MAPPING = {
        "addresses": "addresses",
        "emails": "emails",
        "phone_numbers": "phone-numbers",
    }

    # Transformers that should use table-only display
    TABLE_ONLY_TRANSFORMERS = {"addresses", "emails", "phone-numbers"}

    def __init__(self):
        # Use paths relative to script location
        script_dir = Path(__file__).parent
        # Go up one level from scripts/ to project root, then to sibling datacompose project
        self.source_dir = (
            script_dir.parent.parent / "datacompose" / "datacompose" / "transformers"
        )
        self.output_dir = script_dir.parent / "content"
        # Same directory for examples/documentation
        self.local_transformers_dir = self.source_dir

    def extract_registered_functions(
        self, python_file: Path, registry_name: str
    ) -> List[Dict]:
        """Extract functions with @registry.register() decorator."""
        with open(python_file, "r") as f:
            source = f.read()

        try:
            tree = ast.parse(source)
        except SyntaxError as e:
            print(f"Error parsing {python_file}: {e}")
            return []

        source_lines = source.split("\n")
        functions = []

        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef):
                # Check if function has the register decorator
                has_register = False
                decorator_line = None

                for decorator in node.decorator_list:
                    # Get the line number of the first decorator
                    if decorator_line is None and hasattr(decorator, "lineno"):
                        decorator_line = decorator.lineno - 1  # Convert to 0-based

                    # Check if this is a register decorator for our registry
                    if isinstance(decorator, ast.Attribute):
                        if (
                            hasattr(decorator.value, "id")
                            and decorator.value.id == registry_name
                            and decorator.attr == "register"
                        ):
                            has_register = True
                    elif isinstance(decorator, ast.Call):
                        if (
                            isinstance(decorator.func, ast.Attribute)
                            and hasattr(decorator.func.value, "id")
                            and decorator.func.value.id == registry_name
                            and decorator.func.attr == "register"
                        ):
                            has_register = True

                if (
                    has_register
                    and hasattr(node, "lineno")
                    and hasattr(node, "end_lineno")
                ):
                    # Extract complete function including decorator
                    start_line = (
                        decorator_line
                        if decorator_line is not None
                        else node.lineno - 1
                    )
                    end_line = node.end_lineno

                    # Get the function source
                    function_lines = source_lines[start_line:end_line]
                    function_source = "\n".join(function_lines)

                    # Extract function description
                    description = self.parse_function_description(function_source)

                    functions.append(
                        {
                            "name": node.name,
                            "source": function_source,
                            "description": description,
                            "line_number": node.lineno,
                        }
                    )

        # Sort by line number to maintain order
        functions.sort(key=lambda x: x["line_number"])
        return functions

    def categorize_functions(self, functions: List[Dict]) -> Dict[str, List[Dict]]:
        """Categorize functions by their name patterns."""
        categories = {
            "Extract Functions": [],
            "Transform Functions": [],
            "Validation Functions": [],
            "Utility Functions": [],
        }

        for func in functions:
            name = func["name"].lower()

            # Categorize based on function name patterns
            if name.startswith("extract_"):
                categories["Extract Functions"].append(func)
            elif name.startswith("transform_") or name.startswith("standardize_"):
                categories["Transform Functions"].append(func)
            elif (
                name.startswith("validate_")
                or name.startswith("is_")
                or name.startswith("has_")
            ):
                categories["Validation Functions"].append(func)
            else:
                categories["Utility Functions"].append(func)

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
        description_match = re.search(
            r"^(.*?)(?:\n\s*(?:Args?|Returns?|Raises?|Examples?|Notes?):|$)",
            docstring,
            re.DOTALL,
        )
        if description_match:
            description = description_match.group(1).strip()
            # Join multiple lines and clean up whitespace
            description = " ".join(
                line.strip() for line in description.split("\n") if line.strip()
            )
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
        args_match = re.search(
            r"Args?:\s*\n(.*?)(?:\n\s*Returns?:|$)", docstring, re.DOTALL
        )
        if args_match:
            args_text = args_match.group(1)
            # Parse each parameter line
            param_lines = args_text.strip().split("\n")
            for line in param_lines:
                line = line.strip()
                if line and ":" in line:
                    # Parse "param_name (type, optional): description" format
                    param_match = re.match(r"(\w+)(?:\s*\(([^)]*)\))?\s*:\s*(.+)", line)
                    if param_match:
                        param_name = param_match.group(1)
                        type_info = (
                            param_match.group(2) if param_match.group(2) else "Column"
                        )
                        description = param_match.group(3).strip()

                        # Check if parameter is optional
                        is_optional = (
                            "optional" in type_info.lower() if type_info else False
                        )

                        # Clean up type by removing 'optional'
                        param_type = type_info
                        if type_info:
                            param_type = re.sub(
                                r",?\s*optional", "", type_info, flags=re.IGNORECASE
                            ).strip()
                            if not param_type:
                                param_type = "Column"

                        # Extract default value from description
                        default_value = None
                        # Match patterns like "(default 2)" or "default: 2" or "default = 2"
                        default_match = re.search(
                            r"\(?\s*[Dd]efault(?:s)?(?:\s+is|\s*[=:]|\s+)?\s*([^),\n]+)\)?",
                            description,
                        )
                        if default_match:
                            default_value = default_match.group(1).strip()
                            # Clean up the description by removing the default part
                            description = re.sub(
                                r"\(?\s*[Dd]efault(?:s)?(?:\s+is|\s*[=:]|\s+)?[^),\n]+\)?",
                                "",
                                description,
                            ).strip()

                        params.append(
                            {
                                "name": param_name,
                                "type": param_type,
                                "description": description,
                                "required": not is_optional,
                                "default": default_value,
                            }
                        )

        return params

    def parse_function_examples(self, source: str) -> List[Dict]:
        """Parse examples from function docstring."""
        import re

        # Extract docstring
        docstring_match = re.search(r'"""(.*?)"""', source, re.DOTALL)
        if not docstring_match:
            return []

        docstring = docstring_match.group(1)
        examples = []

        # Find Example or Examples section
        example_match = re.search(
            r"Examples?:\s*\n(.*?)(?:\n\s*(?:Args?|Returns?|Raises?|Notes?):|$)",
            docstring,
            re.DOTALL,
        )
        if example_match:
            example_text = example_match.group(1)
            # Parse example lines looking for input -> output patterns
            lines = example_text.strip().split("\n")

            for i, line in enumerate(lines):
                line = line.strip()
                # Look for comment lines with arrow patterns: # "123 Main St" -> "123"
                arrow_match = re.match(r'#\s*"([^"]+)"\s*->\s*"([^"]*)"', line)
                if not arrow_match:
                    # Also try without quotes: # 123 Main St -> 123
                    arrow_match = re.match(r"#\s*(.+?)\s*->\s*(.+?)$", line)

                if arrow_match:
                    examples.append(
                        {
                            "input": arrow_match.group(1).strip('"'),
                            "output": arrow_match.group(2).strip('"'),
                        }
                    )

        return examples

    def parse_code_example(
        self, source: str, func_name: str, registry_name: str
    ) -> str:
        """Extract code example from function docstring or generate default."""
        import re

        # Extract docstring
        docstring_match = re.search(r'"""(.*?)"""', source, re.DOTALL)
        if not docstring_match:
            return ""

        docstring = docstring_match.group(1)

        # Find Example or Examples section
        example_match = re.search(
            r"Examples?:\s*\n(.*?)(?:\n\s*(?:Args?|Returns?|Raises?|Notes?):|$)",
            docstring,
            re.DOTALL,
        )
        if example_match:
            example_text = example_match.group(1)
            # Look for code lines (not comment lines with ->)
            code_lines = []
            for line in example_text.strip().split("\n"):
                # Skip comment lines with arrow patterns
                if not re.match(r"^\s*#.*->", line):
                    # Include actual code lines
                    if line.strip() and not line.strip().startswith("#"):
                        code_lines.append(line)

            if code_lines:
                return "\n".join(code_lines)

        # Generate default code example
        return f"df.select({registry_name}.{func_name}(F.col('address')))"

    def extract_module_docstring_sections(self, python_file: Path) -> Dict[str, str]:
        """Extract sections from module docstring."""
        with open(python_file, "r") as f:
            source = f.read()

        import ast

        try:
            tree = ast.parse(source)
            module_docstring = ast.get_docstring(tree)
            if not module_docstring:
                return {}
        except:
            return {}

        sections = {}

        # Extract Preview Output
        import re

        preview_match = re.search(
            r"Preview Output:\s*\n(.*?)(?=\n\n[A-Z]|\n\n$|$)",
            module_docstring,
            re.DOTALL,
        )
        if preview_match:
            sections["preview"] = preview_match.group(1).strip()

        # Extract Usage Example
        usage_match = re.search(
            r"Usage Example:\s*\n(.*?)(?=\n\nInstallation:|\n\n[A-Z]|\n\n$|$)",
            module_docstring,
            re.DOTALL,
        )
        if usage_match:
            sections["usage"] = usage_match.group(1).strip()

        # Extract Installation
        install_match = re.search(
            r"Installation:\s*\n(.*?)(?=\n\n|$)", module_docstring, re.DOTALL
        )
        if install_match:
            sections["installation"] = install_match.group(1).strip()

        return sections

    def colorize_ascii_table(self, table_text: str) -> str:
        """Add HTML color spans to ASCII table for better visualization."""
        import pandas as pd
        import re

        # Clean up the ASCII table for pandas to read
        lines = table_text.strip().split("\n")

        # Filter out border lines and prepare data for pandas
        data_lines = []
        for line in lines:
            if line.startswith("+"):
                continue  # Skip border lines
            if "|" in line:
                # Remove leading/trailing | and split by |
                parts = line.strip("|").split("|")
                # Clean up whitespace from each part
                parts = [p.strip() for p in parts]
                data_lines.append(parts)

        if len(data_lines) < 2:
            return table_text  # Not enough data

        # First line is header, rest is data
        headers = data_lines[0]
        data = data_lines[1:]

        # Create DataFrame
        try:
            df = pd.DataFrame(data, columns=headers)
        except:
            return table_text  # Fallback if parsing fails

        # Color schemes for each column position
        column_colors = [
            "#c9d1d9",  # Column 0: address (white/default)
            "#7ee83f",  # Column 1: street_number (green)
            "#ffa657",  # Column 2: street_name (orange)
            "#a5d6ff",  # Column 3: city (light blue)
            "#d2a8ff",  # Column 4: state (purple)
            "#ffd700",  # Column 5: zip (yellow)
            "#c9d1d9",  # Column 6+: default
        ]

        # Apply colors to each cell in the DataFrame
        colored_df = df.copy()
        for j, col in enumerate(colored_df.columns):
            color = column_colors[j] if j < len(column_colors) else column_colors[-1]
            # Color the header
            new_col_name = f'<span style="color:#79c0ff">{col}</span>'
            # Color each cell in the column
            colored_df[col] = colored_df[col].apply(
                lambda x: f'<span style="color:{color}">{x}</span>' if x else ""
            )
            colored_df = colored_df.rename(columns={col: new_col_name})

        # Generate markdown table with pandas
        md_table = colored_df.to_markdown(index=False, tablefmt="pipe")

        # Process the table line by line
        lines = md_table.split("\n")
        colored_lines = []

        for i, line in enumerate(lines):
            if i == 1 and ":" in line:
                # Skip the separator line entirely
                continue
            else:
                # For header and data lines, color the pipe separators
                colored_line = re.sub(
                    r"\|", '<span style="color:#8b949e">|</span>', line
                )
                colored_lines.append(colored_line)

        return "\n".join(colored_lines)

    def generate_markdown(
        self,
        functions: List[Dict],
        transformer_name: str,
        registry_name: str,
        primitives_file: Optional[Path] = None,
    ) -> str:
        """Generate markdown content for a transformer."""
        import json

        # Determine if this transformer should use table-only
        table_only = transformer_name in self.TABLE_ONLY_TRANSFORMERS

        # Extract examples from module docstring if available
        module_sections = {}
        if primitives_file and primitives_file.exists():
            module_sections = self.extract_module_docstring_sections(primitives_file)

        # Build markdown content
        lines = []

        # Add script tag with component imports for table-only mode
        if table_only:
            lines.append("<script>")
            lines.append(
                "  import ParamsTable from '$lib/components/ParamsTable.svelte';"
            )
            lines.append(
                "  import ExamplePopover from '$lib/components/ExamplePopover.svelte';"
            )
            lines.append("  import * as Tabs from '$lib/components/ui/tabs';")
            lines.append("</script>")
            lines.append("")

        # Add title and description based on transformer name
        title_map = {
            "addresses": "Address Transformers",
            "emails": "Email Transformers",
            "phone-numbers": "Phone Number Transformers",
        }
        description_map = {
            "addresses": "Extract, validate, and standardize address components from unstructured text.",
            "emails": "Clean, validate, and extract information from email addresses.",
            "phone-numbers": "Standardize, validate, and extract components from phone numbers.",
        }
        title = title_map.get(
            transformer_name, f"{transformer_name.title()} Transformers"
        )
        description = description_map.get(
            transformer_name, f"Transform and validate {transformer_name} data."
        )

        lines.append(f"# {title}\n")
        lines.append(
            f"<p class='text-lg text-muted-foreground border-b'>{description}</p>\n"
        )

        # Add combined Usage section with tabs
        lines.append("## Usage\n")
        lines.append('<Tabs.Root value="output" class="relative mr-auto w-full">')
        lines.append(
            '  <Tabs.List class="justify-start gap-4 rounded-none bg-transparent px-0">'
        )
        lines.append(
            '    <Tabs.Trigger value="output" class="text-muted-foreground data-[state=active]:text-foreground px-0 text-base font-light data-[state=active]:shadow-none dark:data-[state=active]:border-transparent dark:data-[state=active]:bg-transparent">Preview</Tabs.Trigger>'
        )
        lines.append(
            '    <Tabs.Trigger value="code" class="text-muted-foreground data-[state=active]:text-foreground px-0 text-base font-light data-[state=active]:shadow-none dark:data-[state=active]:border-transparent dark:data-[state=active]:bg-transparent">Code</Tabs.Trigger>'
        )
        lines.append("  </Tabs.List>")
        lines.append('  <Tabs.Content value="output">')
        lines.append("")

        # Add sample output in a styled container with large rounded corners and thin border
        lines.append('<div class="rounded-lg border bg-zinc-950 overflow-x-auto">')
        lines.append(
            '<pre class="shiki github-dark p-3 text-xs sm:p-4 sm:text-sm" style="background-color:#0d1117;color:#c9d1d9"><code>'
        )

        # Use preview from module docstring and colorize it
        if "preview" in module_sections:
            colored_preview = self.colorize_ascii_table(module_sections["preview"])
            # Split by lines and wrap each in a span
            for line in colored_preview.split("\n"):
                lines.append(f'<span class="line">{line}</span>')
        else:
            # If no preview available, add placeholder
            lines.append(
                '<span class="line"># Preview output will be shown here</span>'
            )

        lines.append("</code></pre>")
        lines.append("</div>")
        lines.append("")
        lines.append("  </Tabs.Content>")
        lines.append('  <Tabs.Content value="code">')
        lines.append("")
        lines.append("```python")

        # Use usage example from module docstring
        if "usage" in module_sections:
            lines.append(module_sections["usage"])
        else:
            # If no usage example available, add placeholder
            lines.append("# Usage example will be shown here")
        lines.append("```")
        lines.append("")
        lines.append("  </Tabs.Content>")
        lines.append("</Tabs.Root>")
        lines.append("")

        # Add Installation section without tabs
        lines.append("## Installation\n")
        lines.append("```bash")

        # Use installation from module docstring
        if "installation" in module_sections:
            lines.append(module_sections["installation"])
        else:
            # If no installation command available, add placeholder
            lines.append("# Installation command will be shown here")

        lines.append("```")
        lines.append("")

        lines.append("## API Reference\n")

        # Now add the actual function documentation
        categories = self.categorize_functions(functions)

        for category, funcs in categories.items():
            lines.append(f"\n## {category}\n")

            for func in funcs:
                if table_only:
                    # Parse examples and code example
                    examples = self.parse_function_examples(func["source"])
                    code_example = self.parse_code_example(
                        func["source"], func["name"], registry_name
                    )

                    # Add function with taller badge styling and muted color
                    # Add an anchor div for direct linking
                    lines.append(f"\n<div id='{func['name']}'></div>\n")
                    lines.append(
                        f"\n### <span class='inline-flex items-center rounded-md bg-sky-200 dark:bg-sky-200 px-4 py-2 text-sky-900 dark:text-sky-900' style='font-family: \"JetBrains Mono\", \"Fira Code\", monospace;'><span class='text-[14px] font-normal'>{registry_name}.</span><span class='text-[16px] font-semibold'>{func['name']}</span></span>\n"
                    )

                    # Add function description if available
                    if func.get("description"):
                        lines.append(
                            f"<p class='text-sm text-muted-foreground'>{func['description']}</p>\n"
                        )

                    # Add examples popover inline if available
                    if examples or code_example:
                        examples_json = json.dumps(examples)
                        code_example_json = json.dumps(code_example)
                        function_name = f"{registry_name}.{func['name']}"
                        lines.append(
                            f'\n<ExamplePopover examples={{{examples_json}}} codeExample={{{code_example_json}}} functionName="{function_name}" />\n'
                        )

                    # Parse parameters from the function source
                    params = self.parse_function_params(func["source"])
                    if params:
                        # Add parameters section with better formatting
                        lines.append("\n**Parameters**\n")
                        params_json = json.dumps(params)
                        lines.append(
                            f'<ParamsTable params={{{params_json}}} title="" />\n'
                        )
                else:
                    # For normal mode, include heading and code block
                    lines.append(f"\n### {registry_name}.{func['name']}")
                    lines.append("\n```python")
                    lines.append(f"\n{func['source']}")
                    lines.append("\n```\n")

        return "\n".join(lines)

    def get_registry_name(self, source_dir_name: str) -> str:
        """Get the registry name from the source directory name."""
        # Special cases - map directory names to actual registry names
        if source_dir_name == "phone_numbers":
            return "phone_numbers"  # Changed from 'phones' to match actual registry
        # For other transformers, keep underscores
        return source_dir_name

    def process_transformer(self, source_path: Path):
        """Process a single transformer directory."""
        # Get the transformer names
        source_name = source_path.name  # e.g., 'addresses'
        output_name = self.NAME_MAPPING.get(
            source_name, source_name
        )  # e.g., 'addresses'
        registry_name = self.get_registry_name(source_name)  # e.g., 'addresses'

        print(f"Processing {source_name} -> {output_name} (registry: {registry_name})")

        # Look for dialect directories
        for dialect_dir in source_path.iterdir():
            if not dialect_dir.is_dir() or dialect_dir.name.startswith("."):
                continue

            # Look for pyspark_primitives.py
            primitives_file = dialect_dir / "pyspark_primitives.py"
            if not primitives_file.exists():
                print(f"  No primitives file found at {primitives_file}")
                continue

            print(f"  Found {primitives_file}")

            # Extract functions
            functions = self.extract_registered_functions(
                primitives_file, registry_name
            )

            if not functions:
                print(f"  No registered functions found")
                continue

            print(f"  Extracted {len(functions)} functions")

            # Look for local copy of primitives file (which may have updated examples)
            local_primitives_file = (
                self.local_transformers_dir
                / "text"
                / source_name
                / dialect_dir.name
                / "pyspark_primitives.py"
            )
            if local_primitives_file.exists():
                print(f"  Using local examples from {local_primitives_file}")
                example_source_file = local_primitives_file
            else:
                example_source_file = primitives_file

            # Generate markdown
            markdown_content = self.generate_markdown(
                functions, output_name, registry_name, example_source_file
            )

            # Write to output
            output_path = (
                self.output_dir
                / "transformers"
                / output_name
                / dialect_dir.name
                / "index.md"
            )
            output_path.parent.mkdir(parents=True, exist_ok=True)

            with open(output_path, "w") as f:
                f.write(markdown_content)

            print(f"  Written to {output_path}")

    def sync_all(self):
        """Sync all transformers from source to documentation."""
        text_dir = self.source_dir / "text"

        if not text_dir.exists():
            print(f"Source directory not found: {text_dir}")
            return

        # Process each transformer in the text directory
        for transformer_dir in text_dir.iterdir():
            if transformer_dir.is_dir() and not transformer_dir.name.startswith("."):
                self.process_transformer(transformer_dir)

        print("\nSync complete!")


if __name__ == "__main__":
    generator = TransformerDocGenerator()
    generator.sync_all()
