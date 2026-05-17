#!/usr/bin/env python
"""
Development runner for PySpark code in PyCharm Community Edition.
This script executes your PySpark code inside the Docker container.
"""

import sys
import subprocess
from pathlib import Path


def run_in_container(script_path, *args):
    """Execute a Python script in the jupyter-pyspark container."""

    # Get project root and relative path
    project_root = Path(__file__).parent.parent
    script_abs = Path(script_path).absolute()

    # Calculate relative path from project root
    try:
        relative_path = script_abs.relative_to(project_root)
    except ValueError:
        print(f"Error: Script must be within project directory: {project_root}")
        sys.exit(1)

    # Build docker exec command
    cmd = [
        "docker",
        "exec",
        "-it",
        "jupyter-pyspark",
        "python",
        f"/home/jovyan/datacompose/{relative_path}",
    ] + list(args)

    print(f"Running in container: {' '.join(cmd)}")

    # Execute
    result = subprocess.run(cmd)
    sys.exit(result.returncode)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python pyspark-dev.py <script.py> [args...]")
        sys.exit(1)

    run_in_container(*sys.argv[1:])
