"""
LineAgentic Catalog - A dynamic registry system for Neo4j metadata management
"""

__version__ = "0.1.0"
__author__ = "LineAgentic Team"
__email__ = "team@lineagentic.com"

# Setup logging configuration
from .utils.logging_config import setup_logging, get_logger

# Initialize default logging
setup_logging()

from .registry.factory import RegistryFactory
from .api_generator.generator import APIGenerator
from .cli_generator.generator import CLIGenerator

__all__ = [
    "RegistryFactory",
    "APIGenerator",
    "CLIGenerator",
    "setup_logging",
    "get_logger",
]
