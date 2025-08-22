#!/usr/bin/env python3
"""
API Generator for RegistryFactory
Generates FastAPI files based on methods created by RegistryFactory
"""

from .generator import APIGenerator

# Setup logging for API generator module
from ..utils.logging_config import get_logger
api_logger = get_logger("lineagentic.api")

__all__ = ['APIGenerator', 'api_logger']
