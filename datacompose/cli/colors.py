"""
Simple color utilities for CLI output.
"""

import os
import sys


class Colors:
    """ANSI color codes for terminal output."""

    # Text colors
    RED = "\033[91m"
    GREEN = "\033[92m"
    YELLOW = "\033[93m"
    BLUE = "\033[94m"
    MAGENTA = "\033[95m"
    CYAN = "\033[96m"
    WHITE = "\033[97m"
    GRAY = "\033[90m"

    # Styles
    BOLD = "\033[1m"
    DIM = "\033[2m"
    UNDERLINE = "\033[4m"

    # Reset
    RESET = "\033[0m"

    @classmethod
    def is_enabled(cls) -> bool:
        """Check if colors should be enabled."""
        # Disable colors if NO_COLOR env var is set
        if os.getenv("NO_COLOR"):
            return False

        # Disable colors if not in a TTY
        if not sys.stdout.isatty():
            return False

        return True


def colorize(text: str, color: str = "", style: str = "") -> str:
    """Colorize text if colors are enabled."""
    if not Colors.is_enabled():
        return text

    prefix = style + color
    return f"{prefix}{text}{Colors.RESET}"


def success(text: str) -> str:
    """Green text for success messages."""
    return colorize(text, Colors.GREEN, Colors.BOLD)


def error(text: str) -> str:
    """Red text for error messages."""
    return colorize(text, Colors.RED, Colors.BOLD)


def warning(text: str) -> str:
    """Yellow text for warning messages."""
    return colorize(text, Colors.YELLOW, Colors.BOLD)


def info(text: str) -> str:
    """Blue text for info messages."""
    return colorize(text, Colors.BLUE)


def highlight(text: str) -> str:
    """Cyan text for highlighted text."""
    return colorize(text, Colors.CYAN, Colors.BOLD)


def dim(text: str) -> str:
    """Dimmed text for less important info."""
    return colorize(text, Colors.GRAY)
