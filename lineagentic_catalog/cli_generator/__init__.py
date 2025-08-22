#!/usr/bin/env python3
"""
CLI Generator module for generating command-line interface commands
"""

from .generator import CLIGenerator

# Setup logging for CLI generator module
from ..utils.logging_config import get_logger
cli_logger = get_logger("lineagentic.cli")

__all__ = ['CLIGenerator', 'cli_logger']
