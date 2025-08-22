#!/usr/bin/env python3
from __future__ import annotations

from typing import Any, Dict, Type
from ..utils.logging_config import get_logger, log_function_call, log_function_result, log_error_with_context
from .loaders import RegistryLoader
from .validators import RegistryValidator
from .generators import URNGenerator, AspectProcessor, UtilityFunctionBuilder
from .writers import Neo4jWriterGenerator


class RegistryFactory:
    """Main factory class that orchestrates registry loading and writer generation"""
    
    def __init__(self, registry_path: str):
        self.logger = get_logger("lineagentic.registry.factory")
        self.registry_path = registry_path
        
        self.logger.info("Initializing RegistryFactory", registry_path=registry_path)
        
        try:
            # Load and validate registry
            self.logger.debug("Loading registry configuration")
            self.loader = RegistryLoader(registry_path)
            self.validator = RegistryValidator()
            self.registry = self.loader.load()
            self.validator.validate(self.registry)
            
            # Create generators
            self.logger.debug("Creating registry generators")
            self.utility_builder = UtilityFunctionBuilder(self.registry)
            self.urn_generator = URNGenerator(self.registry, self.utility_builder)
            self.aspect_processor = AspectProcessor(self.registry, self.utility_builder)
            
            # Create utility functions and generators
            self.logger.debug("Building utility functions and generators")
            self.utility_functions = self.utility_builder.create_functions()
            self.urn_generators = self.urn_generator.create_generators()
            self.aspect_processors = self.aspect_processor.create_processors()
            
            self.logger.info("RegistryFactory initialized successfully", 
                           registry_path=registry_path,
                           aspect_count=len(self.aspect_processors),
                           urn_generator_count=len(self.urn_generators))
            
        except Exception as e:
            log_error_with_context(self.logger, e, "RegistryFactory initialization")
            raise
    
    def validate_aspect_payload(self, aspect_name: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Validate and enrich aspect payload based on registry definition"""
        log_function_call(self.logger, "validate_aspect_payload", aspect_name=aspect_name)
        
        try:
            if aspect_name not in self.aspect_processors:
                self.logger.error(f"Aspect '{aspect_name}' not defined in registry", 
                                available_aspects=list(self.aspect_processors.keys()))
                raise ValueError(f"Aspect '{aspect_name}' not defined in registry")
            
            result = self.aspect_processors[aspect_name](payload)
            log_function_result(self.logger, "validate_aspect_payload", result=result)
            return result
            
        except Exception as e:
            log_error_with_context(self.logger, e, "validate_aspect_payload")
            raise
    
    def generate_neo4j_writer_class(self) -> Type:
        """Generate Neo4jMetadataWriter class dynamically from registry"""
        log_function_call(self.logger, "generate_neo4j_writer_class")
        
        try:
            writer_generator = Neo4jWriterGenerator(
                self.registry,
                self.urn_generators,
                self.utility_functions,
                self
            )
            result = writer_generator.generate_class()
            log_function_result(self.logger, "generate_neo4j_writer_class", 
                              result_class_name=result.__name__)
            return result
            
        except Exception as e:
            log_error_with_context(self.logger, e, "generate_neo4j_writer_class")
            raise
    
    def create_writer(self, uri: str, user: str, password: str) -> Any:
        """Create Neo4jMetadataWriter instance"""
        log_function_call(self.logger, "create_writer", uri=uri, user=user)
        
        try:
            writer_class = self.generate_neo4j_writer_class()
            writer_instance = writer_class(uri, user, password, self.registry, self.urn_generators, self.utility_functions, self)
            log_function_result(self.logger, "create_writer", 
                              writer_class_name=writer_class.__name__)
            return writer_instance
            
        except Exception as e:
            log_error_with_context(self.logger, e, "create_writer")
            raise
