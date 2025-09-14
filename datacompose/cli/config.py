"""
Configuration management for Datacompose CLI.
"""

import json
import os
from pathlib import Path
from typing import Any, Dict, Optional, Tuple


class ConfigLoader:
    """Load and manage Datacompose configuration."""

    DEFAULT_CONFIG_FILE = "datacompose.json"

    @staticmethod
    def load_config(config_path: Optional[Path] = None) -> Optional[Dict[str, Any]]:
        """Load configuration from datacompose.json.

        Args:
            config_path: Optional path to config file. Defaults to ./datacompose.json

        Returns:
            Config dictionary or None if not found
        """
        if config_path is None:
            config_path = Path(ConfigLoader.DEFAULT_CONFIG_FILE)

        if not config_path.exists():
            return None

        try:
            with open(config_path, "r") as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            return None

    @staticmethod
    def get_default_target(config: Optional[Dict[str, Any]] = None) -> Optional[str]:
        """Get the default target from config.

        Args:
            config: Optional config dict. If None, will load from file.

        Returns:
            Default target name or None
        """
        if config is None:
            config = ConfigLoader.load_config()
            if not config:
                return "pyspark"

        if not config:
            return None

        # Check for explicit default_target setting
        if "default_target" in config:
            return config["default_target"]

        # Otherwise use the first target if only one exists
        targets = config.get("targets", {})
        if len(targets) == 1:
            return list(targets.keys())[0]

        return None

    @staticmethod
    def get_target_output(
        config: Optional[Dict[str, Any]], target: str
    ) -> Optional[str]:
        """Get the output directory for a specific target.

        Args:
            config: Config dictionary
            target: Target name

        Returns:
            Output directory path or None
        """
        if not config:
            return None

        targets = config.get("targets", {})
        target_config = targets.get(target, {})
        return target_config.get("output")

    @staticmethod
    def get_postgres_config(config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Load PostgreSQL configuration from env file or environment.

        Args:
            config: Optional config dict. If None, will load from file.

        Returns:
            Dictionary with PostgreSQL connection parameters
        """
        if config is None:
            config = ConfigLoader.load_config()

        # Get postgres target configuration
        postgres_config = {}
        if config and "targets" in config and "postgres" in config["targets"]:
            postgres_config = config["targets"]["postgres"]

        # Get env_file path from config or use default
        env_file = postgres_config.get("env_file", ".env")

        # Load env file if it exists
        env_file_path = Path(env_file)
        if env_file_path.exists():
            try:
                from dotenv import load_dotenv
                load_dotenv(env_file_path)
            except ImportError:
                # python-dotenv not installed, fall back to environment variables
                pass

        # Return connection parameters with defaults
        return {
            "host": os.getenv("POSTGRES_HOST", "localhost"),
            "port": int(os.getenv("POSTGRES_PORT", "5432")),
            "database": os.getenv("POSTGRES_DB", "postgres"),
            "user": os.getenv("POSTGRES_USER", "postgres"),
            "password": os.getenv("POSTGRES_PASSWORD", ""),
        }

    @staticmethod
    def test_postgres_connection(config: Optional[Dict[str, Any]] = None) -> Tuple[bool, str]:
        """Test PostgreSQL connection using configuration.

        Args:
            config: Optional config dict. If None, will load from file.

        Returns:
            Tuple of (success: bool, message: str)
        """
        try:
            import psycopg2

            conn_params = ConfigLoader.get_postgres_config(config)

            # Try to connect
            conn = psycopg2.connect(
                host=conn_params["host"],
                port=conn_params["port"],
                database=conn_params["database"],
                user=conn_params["user"],
                password=conn_params["password"],
            )
            conn.close()

            return True, f"Connected to {conn_params['user']}@{conn_params['host']}:{conn_params['port']}/{conn_params['database']}"

        except ImportError:
            return False, "psycopg2 not installed. Install with: pip install psycopg2-binary"

        except psycopg2.OperationalError as e:
            return False, f"Connection failed: {str(e)}"

        except Exception as e:
            return False, f"Unexpected error: {str(e)}"
