"""
Registry Factory Module

A modular, YAML-driven registry system for dynamic Neo4jMetadataWriter generation.
"""

from .factory import RegistryFactory
from .loaders import RegistryLoader
from .validators import RegistryValidator
from .generators import URNGenerator, AspectProcessor, UtilityFunctionBuilder
from .writers import Neo4jWriterGenerator

# Setup logging for registry module
from ..utils.logging_config import get_logger
registry_logger = get_logger("lineagentic.registry")

__all__ = [
    "RegistryFactory",
    "RegistryLoader", 
    "RegistryValidator",
    "URNGenerator",
    "AspectProcessor", 
    "UtilityFunctionBuilder",
    "Neo4jWriterGenerator",
    "registry_logger"
]
