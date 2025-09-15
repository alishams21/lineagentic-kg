#!/usr/bin/env python3
from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict, List
import yaml
import importlib.resources


class RegistryLoader:
    """Handles loading and merging registry configuration files"""
    
    def __init__(self, registry_path: str):
        self.registry_path = registry_path
        self.registry_dir = Path(registry_path).parent
    
    def load(self) -> Dict[str, Any]:
        """Load and merge registry configuration"""
        # Load main registry file
        main_registry = self._load_yaml_file(self.registry_path)
        
        # Load additional configuration files if they exist
        merged_registry = self._merge_configurations(main_registry)
        
        return merged_registry
    
    def _load_yaml_file(self, file_path: str) -> Dict[str, Any]:
        """Load a single YAML file"""
        # First try to load from local file system
        if os.path.exists(file_path):
            with open(file_path, 'r') as f:
                return yaml.safe_load(f)
        
        # If not found locally, try to load from installed package
        try:
            # Try to load from the package's config directory using importlib.resources
            config_file_name = Path(file_path).name
            with importlib.resources.files('lineagentic_kg').joinpath('config', config_file_name).open('r') as f:
                return yaml.safe_load(f)
        except Exception:
            pass
        
        raise FileNotFoundError(f"Registry file not found: {file_path}")
    
    def _merge_configurations(self, main_registry: Dict[str, Any]) -> Dict[str, Any]:
        """Merge main registry with additional configuration files"""
        merged = main_registry.copy()
        
        # Check for includes in main registry
        includes = main_registry.get('includes', [])
        for include_file in includes:
            # Try local path first
            include_path = os.path.join(self.registry_dir, include_file)
            if os.path.exists(include_path):
                include_config = self._load_yaml_file(include_path)
                merged = self._deep_merge(merged, include_config)
            else:
                # Try package path
                try:
                    include_config = self._load_yaml_file(include_file)
                    merged = self._deep_merge(merged, include_config)
                except FileNotFoundError:
                    # If both fail, try with just the filename
                    try:
                        include_config = self._load_yaml_file(Path(include_file).name)
                        merged = self._deep_merge(merged, include_config)
                    except FileNotFoundError:
                        raise FileNotFoundError(f"Include file not found: {include_file}")
        
        return merged
    
    def _deep_merge(self, base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
        """Deep merge two dictionaries"""
        result = base.copy()
        
        for key, value in override.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = self._deep_merge(result[key], value)
            else:
                result[key] = value
        
        return result
