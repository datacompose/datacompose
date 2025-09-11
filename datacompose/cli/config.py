"""
Configuration management for Datacompose CLI.
"""

import json
from pathlib import Path
from typing import Any, Dict, Optional


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
            with open(config_path, 'r') as f:
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
    def get_target_output(config: Optional[Dict[str, Any]], target: str) -> Optional[str]:
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