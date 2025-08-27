#!/usr/bin/env python3
"""
Factory wrapper for dependency injection
"""

import os
import sys
from typing import Optional

# Add the parent directory to sys.path to import the registry module
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

try:
    from lineagentic_catalog.registry.factory import RegistryFactory
except ImportError:
    # Fallback for when running as standalone
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
    from lineagentic_catalog.registry.factory import RegistryFactory


class FactoryWrapper:
    """Wrapper for RegistryFactory with singleton pattern"""
    
    _instance: Optional['FactoryWrapper'] = None
    _factory: Optional[RegistryFactory] = None
    _writer = None
    
    def __init__(self):
        # Try multiple possible registry paths
        possible_paths = [
            os.getenv("REGISTRY_PATH"),
            "config/main_registry.yaml",  # Local config directory (copied during generation)
            os.path.join(os.path.dirname(__file__), "config", "main_registry.yaml"),  # Relative to this file
            "../config/main_registry.yaml",
            "../../config/main_registry.yaml",
            os.path.join(os.path.dirname(__file__), "..", "config", "main_registry.yaml"),
            os.path.join(os.path.dirname(__file__), "..", "..", "config", "main_registry.yaml")
        ]
        
        registry_path = None
        for path in possible_paths:
            if path and os.path.exists(path):
                registry_path = path
                break
        
        if not registry_path:
            raise FileNotFoundError(f"Registry file not found. Tried paths: {possible_paths}")
        
        self._factory = RegistryFactory(registry_path)
        
        # Set methods based on registry configuration
        self._factory.entity_methods = list(self._factory.registry.get('entities', {}).keys())
        self._factory.aspect_methods = list(self._factory.registry.get('aspects', {}).keys())
        self._factory.utility_methods = list(self._factory.registry.get('utility_functions', {}).keys())
    

    
    def get_writer_instance(self):
        """Get or create writer instance"""
        if self._writer is None:
            uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
            user = os.getenv("NEO4J_USER", "neo4j")
            password = os.getenv("NEO4J_PASSWORD", "password")
            self._writer = self._factory.create_writer(uri, user, password)
        return self._writer
    
    @classmethod
    def get_instance(cls) -> 'FactoryWrapper':
        """Get singleton instance"""
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance


def get_factory_instance() -> RegistryFactory:
    """Get factory instance for dependency injection"""
    return FactoryWrapper.get_instance()._factory


def get_writer_instance():
    """Get writer instance for dependency injection"""
    return FactoryWrapper.get_instance().get_writer_instance()
