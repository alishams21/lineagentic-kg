#!/usr/bin/env python3
from __future__ import annotations

from typing import Any, Dict, Type
from .loaders import RegistryLoader
from .validators import RegistryValidator
from .generators import URNGenerator, AspectProcessor, UtilityFunctionBuilder
from .writers import Neo4jWriterGenerator


class RegistryFactory:
    """Main factory class that orchestrates registry loading and writer generation"""
    
    def __init__(self, registry_path: str):
        self.registry_path = registry_path
        
        # Load and validate registry
        self.loader = RegistryLoader(registry_path)
        self.validator = RegistryValidator()
        self.registry = self.loader.load()
        self.validator.validate(self.registry)
        
        # Create generators
        self.utility_builder = UtilityFunctionBuilder(self.registry)
        self.urn_generator = URNGenerator(self.registry, self.utility_builder)
        self.aspect_processor = AspectProcessor(self.registry, self.utility_builder)
        
        # Create utility functions and generators
        self.utility_functions = self.utility_builder.create_functions()
        self.urn_generators = self.urn_generator.create_generators()
        self.aspect_processors = self.aspect_processor.create_processors()
    
    def validate_aspect_payload(self, aspect_name: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Validate and enrich aspect payload based on registry definition"""
        if aspect_name not in self.aspect_processors:
            raise ValueError(f"Aspect '{aspect_name}' not defined in registry")
        
        return self.aspect_processors[aspect_name](payload)
    
    def generate_neo4j_writer_class(self) -> Type:
        """Generate Neo4jMetadataWriter class dynamically from registry"""
        writer_generator = Neo4jWriterGenerator(
            self.registry,
            self.urn_generators,
            self.utility_functions,
            self
        )
        return writer_generator.generate_class()
    
    def create_writer(self, uri: str, user: str, password: str) -> Any:
        """Create Neo4jMetadataWriter instance"""
        writer_class = self.generate_neo4j_writer_class()
        return writer_class(uri, user, password, self.registry, self.urn_generators, self.utility_functions, self)
