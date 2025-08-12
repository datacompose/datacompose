"""
Init command for initializing a Datacompose project configuration.
"""

import json
import os
import sys
import termios
import tty
from pathlib import Path
from typing import Any, Dict

import click

from datacompose.cli.colors import dim, error, highlight, info, success

# Get the directory where this module is located

DEFAULT_CONFIG = {
    "version": "1.0",
    "aliases": {"utils": "./src/utils"},
    "targets": {
        "pyspark": {
            "output": "./build/pyspark",
        }
    },
}


@click.command()
@click.option(
    "--force", "-f", is_flag=True, help="Overwrite existing datacompose.json if it exists"
)
@click.option(
    "--output",
    "-o",
    default="./datacompose.json",
    help="Output path for the config file (default: ./datacompose.json)",
)
@click.option("--verbose", "-v", is_flag=True, help="Verbose output")
@click.option(
    "--yes", "-y", is_flag=True, help="Skip interactive prompts and use defaults"
)
@click.option("--skip-completion", is_flag=True, help="Skip shell completion setup")
@click.pass_context
def init(ctx, force, output, verbose, yes, skip_completion):
    """Initialize project configuration."""
    exit_code = _run_init(force, output, verbose, yes, skip_completion)
    if exit_code != 0:
        ctx.exit(exit_code)


class InitCommand:
    """Command to initialize a Datacompose project configuration."""

    @staticmethod
    def get_config_template(template_name: str) -> Dict[str, Any]:
        """Get configuration template by name."""
        if template_name == "minimal":
            return {"version": "1.0", "targets": {"pyspark": {"output": "./build/pyspark"}}}
        elif template_name == "advanced":
            config = DEFAULT_CONFIG.copy()
            config.update(
                {
                    "style": "custom",
                    "aliases": {
                        "utils": "./src/utils",
                        "build": "./build",
                    },
                    "include": ["src/**/*"],
                    "exclude": ["__pycache__", "build", "*.pyc", ".pytest_cache"],
                    "testing": {"framework": "pytest", "test_dir": "./tests"},
                }
            )
            return config
        else:  # default
            return DEFAULT_CONFIG.copy()

    @staticmethod
    def get_key():
        """Get a single key press from the user."""
        try:
            fd = sys.stdin.fileno()
            old_settings = termios.tcgetattr(fd)
            tty.setraw(sys.stdin.fileno())
            key = sys.stdin.read(1)

            # Handle arrow keys (escape sequences)
            if key == "\x1b":
                key += sys.stdin.read(2)

            termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)
            return key
        except Exception:
            # Fallback for systems without termios (like Windows)
            return input()

    @staticmethod
    def prompt_for_targets(available_targets: Dict[str, Dict]) -> Dict[str, Dict]:
        """Interactive multi-select for choosing targets with arrow key navigation."""
        target_keys = list(available_targets.keys())
        selected = [i == 0 for i in target_keys]
        current_pos = 0  # Current cursor position

        while True:
            # Clear screen and display
            print("\033[2J\033[H", end="")  # Clear screen, move cursor to top
            print(highlight("Platform Selection"))
            print(dim("Choose which platforms you'd like to generate UDFs for:\n"))

            for i, (key, target_info) in enumerate(available_targets.items()):
                # Selection indicators with better symbols
                if selected[i]:
                    marker = "[✓]"
                    name_color = success
                else:
                    marker = "[ ]"

                    def name_color(text):
                        return text

                # Current item indicator with better styling
                if i == current_pos:
                    cursor = "> "
                    # Highlighted current line
                    line = f"{cursor}{marker} {name_color(target_info['name'])} {dim('-> ' + target_info['output'])}"
                    print(f"\033[7m{line}\033[0m")
                else:
                    cursor = "  "
                    line = f"{cursor}{marker} {name_color(target_info['name'])} {dim('-> ' + target_info['output'])}"
                    print(line)

            # Summary section with better formatting
            selected_names = [target_keys[i] for i, sel in enumerate(selected) if sel]
            if selected_names:
                summary = highlight(f"Selected: {', '.join(selected_names)}")
            else:
                summary = dim("Selected: None")

            print(f"\n{summary}")
            print(
                f"\n{dim('Controls:')} ↑/↓ navigate • SPACE toggle • ENTER confirm • q/ESC quit"
            )

            # Get key input
            key = InitCommand.get_key()

            if key == "\x1b[A":  # Up arrow
                current_pos = (current_pos - 1) % len(target_keys)
            elif key == "\x1b[B":  # Down arrow
                current_pos = (current_pos + 1) % len(target_keys)
            elif key == " ":  # Space to toggle
                selected[current_pos] = not selected[current_pos]
            elif key == "\r" or key == "\n":  # Enter to confirm
                break
            elif key == "q" or key == "Q" or key == "\x1b":  # Quit with q or ESC
                return {}

        # Build selected targets with custom output paths
        print("\033[2J\033[H", end="")  # Clear screen
        print(highlight("Output Directory Configuration"))
        print(dim("Configure output directories for your selected platforms:\n"))

        result = {}
        for i, (key, target_info) in enumerate(available_targets.items()):
            if selected[i]:
                prompt = f"{success('[✓]')} {target_info['name']} output directory? {dim('(default: ' + target_info['output'] + ')')} "
                output_path = input(prompt).strip()
                if not output_path:
                    output_path = target_info["output"]

                result[key] = {"output": output_path}
                print(dim(f"   -> Set to: {output_path}\n"))

        return result

    @staticmethod
    def prompt_for_config(template_config: Dict[str, Any]) -> Dict[str, Any] | None:
        """Interactively prompt user for configuration options."""
        print(highlight("Setting up your Datacompose project configuration..."))
        print(dim("Press Enter to use the default value shown in brackets.\n"))

        print()

        # Select targets with multi-select
        available_targets = {
            "pyspark": {"output": "./build/pyspark", "name": "PySpark (Apache Spark)"},
        }

        selected_targets = InitCommand.prompt_for_targets(available_targets)

        # Check if user quit the selection
        if not selected_targets:
            print(dim("\nConfiguration cancelled."))
            return None

        # Update the configuration
        config = template_config.copy()

        # Update targets with user selections
        config["targets"] = selected_targets

        print()  # Add spacing
        return config

    @staticmethod
    def create_directory_structure(config: Dict[str, Any], verbose: bool = False):
        """Create the basic directory structure based on config."""
        directories_to_create = []

        # Output directories will be created automatically

        # Add target output directories
        if "targets" in config:
            for target_config in config["targets"].values():
                if "output" in target_config:
                    directories_to_create.append(Path(target_config["output"]).parent)

        # Add template directories if specified

        for directory in directories_to_create:
            if not directory.exists():
                directory.mkdir(parents=True, exist_ok=True)
                if verbose:
                    print(f"Created directory: {directory}")

    @staticmethod
    def setup_shell_completion(verbose: bool = False) -> bool:
        """Set up shell completion for datacompose commands. Returns True if successful."""
        try:
            # Detect current shell
            shell = os.environ.get("SHELL", "").lower()

            if "bash" in shell:
                config_file = Path.home() / ".bashrc"
                # Also check .bash_profile as fallback
                if not config_file.exists():
                    config_file = Path.home() / ".bash_profile"
            elif "zsh" in shell:
                config_file = Path.home() / ".zshrc"
            else:
                if verbose:
                    print(dim(f"Shell not detected or not supported: {shell}"))
                    print(dim("Supported shells: bash, zsh"))
                return False

            completion_line = 'eval "$(register-python-argcomplete datacompose)"'

            # Check if config file exists
            if not config_file.exists():
                if verbose:
                    print(dim(f"Shell config file not found: {config_file}"))
                return False

            # Read existing config
            try:
                with open(config_file, "r") as f:
                    content = f.read()
            except PermissionError:
                if verbose:
                    print(dim(f"Permission denied reading: {config_file}"))
                return False

            # Check if already configured
            if (
                completion_line in content
                or "register-python-argcomplete datacompose" in content
            ):
                if verbose:
                    print(success("✓ Shell completion already configured"))
                return True

            # Create backup
            backup_file = config_file.with_suffix(config_file.suffix + ".datacompose-backup")
            try:
                with open(backup_file, "w") as f:
                    f.write(content)
                if verbose:
                    print(dim(f"Created backup: {backup_file}"))
            except PermissionError:
                if verbose:
                    print(dim("Warning: Could not create backup file"))

            # Add completion line
            try:
                with open(config_file, "a") as f:
                    f.write(f"\n# Datacompose CLI completion\n{completion_line}\n")

                # shell_name = "bash" if "bash" in shell else "zsh"
                print(success(f"✓ Added tab completion to {config_file}"))
                print(
                    info(
                        f"Run 'source {config_file}' or restart your terminal to enable completion"
                    )
                )
                return True

            except PermissionError:
                if verbose:
                    print(dim(f"Permission denied writing to: {config_file}"))
                return False

        except Exception as e:
            if verbose:
                print(dim(f"Completion setup failed: {e}"))
            return False

    @staticmethod
    def prompt_completion_setup(verbose: bool = False) -> bool:
        """Prompt user to set up shell completion and do it if they agree."""
        try:
            print()  # Add some spacing
            response = (
                input(highlight("Set up tab completion for datacompose commands? (Y/n): "))
                .strip()
                .lower()
            )

            if response in ["", "y", "yes"]:
                success_setup = InitCommand.setup_shell_completion(verbose)
                if not success_setup:
                    print()
                    print(dim("Manual setup instructions:"))
                    print(
                        dim(
                            "  bash: echo 'eval \"$(register-python-argcomplete datacompose)\"' >> ~/.bashrc"
                        )
                    )
                    print(
                        dim(
                            "  zsh:  echo 'eval \"$(register-python-argcomplete datacompose)\"' >> ~/.zshrc"
                        )
                    )
                return success_setup
            else:
                print(dim("Skipped shell completion setup"))
                print(dim("You can set it up later with:"))
                print(
                    dim(
                        "  echo 'eval \"$(register-python-argcomplete datacompose)\"' >> ~/.bashrc"
                    )
                )
                return False

        except (KeyboardInterrupt, EOFError):
            print(dim("\nSkipped shell completion setup"))
            return False


def _run_init(force, output, verbose, yes, skip_completion) -> int:
    """Execute the init command."""
    config_path = Path(output)

    # Check if config already exists
    if config_path.exists() and not force:
        print(error(f"Configuration file already exists: {config_path}"))
        print(dim("Use --force to overwrite"))
        return 1

    try:
        # Get the default template
        template_config = InitCommand.get_config_template("default")

        # Either prompt for interactive configuration or use defaults
        if yes:
            config = template_config
            print("Using default configuration...")
        else:
            config = InitCommand.prompt_for_config(template_config)
            # Check if user cancelled the configuration
            if config is None:
                return 0

        # Write the configuration file
        with open(config_path, "w") as f:
            json.dump(config, f, indent=2)

        print(success(f"✓ Configuration initialized: {config_path}"))

        # Set up shell completion (unless skipping)
        completion_setup = False
        if (
            not yes and not skip_completion
        ):  # Only prompt in interactive mode and if not skipping
            completion_setup = InitCommand.prompt_completion_setup(verbose)
        elif skip_completion and verbose:
            print(dim("Skipped shell completion setup (--skip-completion)"))
        elif yes and verbose:
            print(dim("Skipped shell completion setup (non-interactive mode)"))

        # Create directory structure
        InitCommand.create_directory_structure(config, verbose)

        if verbose:
            print(success("✓ Used template: default"))
            print(success("✓ Created directory structure"))
            if completion_setup:
                print(success("✓ Shell completion configured"))
            print(highlight("\nNext steps:"))
            print("1. Review the configuration in datacompose.json")
            if completion_setup:
                print(
                    "2. Source your shell config or restart terminal for tab completion"
                )
                print(
                    "3. Add your first transformer: datacompose add clean_emails --target pyspark"
                )
            else:
                print(
                    "2. Add your first transformer: datacompose add clean_emails --target pyspark"
                )
                if not skip_completion:
                    print(
                        "4. Set up tab completion: echo 'eval \"$(register-python-argcomplete datacompose)\"' >> ~/.bashrc"
                    )
        else:
            print(success("✓ Directory structure created"))
            if completion_setup:
                print(success("✓ Tab completion configured"))
                print(
                    highlight(
                        "\nRun 'datacompose add clean_emails --target pyspark' to get started"
                    )
                )
                print(
                    dim(
                        "Restart your terminal or run 'source ~/.bashrc' to enable tab completion"
                    )
                )
            else:
                print(
                    highlight(
                        "\nRun 'datacompose add clean_emails --target pyspark' to get started"
                    )
                )
                if not skip_completion and not yes:
                    print(
                        dim(
                            "Tip: Set up tab completion with: echo 'eval \"$(register-python-argcomplete datacompose)\"' >> ~/.bashrc"
                        )
                    )

        return 0

    except Exception as e:
        print(error(f"Init failed: {e}"))
        if verbose:
            import traceback

            traceback.print_exc()
        return 1
