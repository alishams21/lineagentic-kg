"""
Centralized logging configuration for LineAgentic Catalog.

This module provides a consistent logging setup across all modules with:
- Structured logging with timestamps and module names
- Configurable log levels
- Console and file handlers
- JSON formatting for production environments
- Color-coded console output for development
"""

import logging
import logging.handlers
import os
import sys
from pathlib import Path
from typing import Optional, Dict, Any
import json
from datetime import datetime


class ColoredFormatter(logging.Formatter):
    """Custom formatter with color support for console output."""
    
    # Color codes for different log levels
    COLORS = {
        'DEBUG': '\033[36m',      # Cyan
        'INFO': '\033[32m',       # Green
        'WARNING': '\033[33m',    # Yellow
        'ERROR': '\033[31m',      # Red
        'CRITICAL': '\033[35m',   # Magenta
        'RESET': '\033[0m'        # Reset
    }
    
    def format(self, record):
        # Add color to the level name
        if hasattr(record, 'levelname') and record.levelname in self.COLORS:
            record.levelname = f"{self.COLORS[record.levelname]}{record.levelname}{self.COLORS['RESET']}"
        
        return super().format(record)


class JSONFormatter(logging.Formatter):
    """JSON formatter for structured logging."""
    
    def format(self, record):
        log_entry = {
            'timestamp': datetime.utcnow().isoformat(),
            'level': record.levelname,
            'module': record.module,
            'function': record.funcName,
            'line': record.lineno,
            'message': record.getMessage()
        }
        
        # Add exception info if present
        if record.exc_info:
            log_entry['exception'] = self.formatException(record.exc_info)
        
        # Add extra fields if present
        if hasattr(record, 'extra_fields'):
            log_entry.update(record.extra_fields)
        
        return json.dumps(log_entry)


class LineAgenticLogger:
    """Centralized logger for LineAgentic Catalog modules."""
    
    def __init__(self, name: str, log_level: str = "INFO", log_file: Optional[str] = None):
        """
        Initialize the logger.
        
        Args:
            name: Logger name (usually module name)
            log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
            log_file: Optional log file path
        """
        self.name = name
        self.log_level = getattr(logging, log_level.upper())
        self.log_file = log_file
        
        # Create logger
        self.logger = logging.getLogger(name)
        self.logger.setLevel(self.log_level)
        
        # Prevent duplicate handlers
        if not self.logger.handlers:
            self._setup_handlers()
    
    def _setup_handlers(self):
        """Setup console and file handlers."""
        # Console handler with colored output
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(self.log_level)
        
        # Use colored formatter for console
        console_formatter = ColoredFormatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        console_handler.setFormatter(console_formatter)
        self.logger.addHandler(console_handler)
        
        # File handler if log_file is specified
        if self.log_file:
            # Ensure log directory exists
            log_path = Path(self.log_file)
            log_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Use rotating file handler
            file_handler = logging.handlers.RotatingFileHandler(
                self.log_file,
                maxBytes=10*1024*1024,  # 10MB
                backupCount=5
            )
            file_handler.setLevel(self.log_level)
            
            # Use JSON formatter for file output
            file_formatter = JSONFormatter()
            file_handler.setFormatter(file_formatter)
            self.logger.addHandler(file_handler)
    
    def debug(self, message: str, **kwargs):
        """Log debug message with optional extra fields."""
        self._log_with_extra(logging.DEBUG, message, **kwargs)
    
    def info(self, message: str, **kwargs):
        """Log info message with optional extra fields."""
        self._log_with_extra(logging.INFO, message, **kwargs)
    
    def warning(self, message: str, **kwargs):
        """Log warning message with optional extra fields."""
        self._log_with_extra(logging.WARNING, message, **kwargs)
    
    def error(self, message: str, **kwargs):
        """Log error message with optional extra fields."""
        self._log_with_extra(logging.ERROR, message, **kwargs)
    
    def critical(self, message: str, **kwargs):
        """Log critical message with optional extra fields."""
        self._log_with_extra(logging.CRITICAL, message, **kwargs)
    
    def exception(self, message: str, **kwargs):
        """Log exception message with traceback."""
        self._log_with_extra(logging.ERROR, message, exc_info=True, **kwargs)
    
    def _log_with_extra(self, level: int, message: str, **kwargs):
        """Log message with extra fields."""
        if kwargs:
            # Create a custom record with extra fields
            record = logging.LogRecord(
                name=self.name,
                level=level,
                pathname="",
                lineno=0,
                msg=message,
                args=(),
                exc_info=None
            )
            record.extra_fields = kwargs
            self.logger.handle(record)
        else:
            self.logger.log(level, message)


def get_logger(name: str, log_level: str = None, log_file: str = None) -> LineAgenticLogger:
    """
    Get a configured logger instance.
    
    Args:
        name: Logger name (usually module name)
        log_level: Optional log level override
        log_file: Optional log file path override
    
    Returns:
        Configured LineAgenticLogger instance
    """
    # Get log level from environment or use default
    if log_level is None:
        log_level = os.getenv('LINEAGENTIC_LOG_LEVEL', 'INFO')
    
    # Get log file from environment or use default
    if log_file is None:
        log_file = os.getenv('LINEAGENTIC_LOG_FILE')
    
    return LineAgenticLogger(name, log_level, log_file)


def setup_logging(
    default_level: str = "INFO",
    log_file: Optional[str] = None,
    enable_json: bool = False
) -> None:
    """
    Setup global logging configuration.
    
    Args:
        default_level: Default logging level
        log_file: Optional log file path
        enable_json: Whether to use JSON formatting for all loggers
    """
    # Set root logger level
    logging.getLogger().setLevel(getattr(logging, default_level.upper()))
    
    # Configure environment variables
    if log_file:
        os.environ['LINEAGENTIC_LOG_FILE'] = log_file
    os.environ['LINEAGENTIC_LOG_LEVEL'] = default_level
    
    # Create logs directory if log_file is specified
    if log_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)


# Convenience functions for common logging patterns
def log_function_call(logger: LineAgenticLogger, func_name: str, **kwargs):
    """Log function call with parameters."""
    logger.debug(f"Calling {func_name}", function=func_name, parameters=kwargs)


def log_function_result(logger: LineAgenticLogger, func_name: str, result: Any = None, **kwargs):
    """Log function result."""
    logger.debug(f"Function {func_name} completed", function=func_name, result=result, **kwargs)


def log_error_with_context(logger: LineAgenticLogger, error: Exception, context: str, **kwargs):
    """Log error with context information."""
    logger.error(
        f"Error in {context}: {str(error)}",
        error_type=type(error).__name__,
        context=context,
        **kwargs
    )


# Default logger instances for common modules
registry_logger = get_logger("lineagentic.registry")
api_logger = get_logger("lineagentic.api")
cli_logger = get_logger("lineagentic.cli")
config_logger = get_logger("lineagentic.config")
