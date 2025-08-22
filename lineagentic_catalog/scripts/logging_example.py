#!/usr/bin/env python3
"""
Example script demonstrating the centralized logging functionality
"""

import sys
import os
from pathlib import Path

# Add lineagentic_catalog to path
sys.path.append(str(Path(__file__).parent.parent))

from utils.logging_config import setup_logging, get_logger, log_function_call, log_function_result, log_error_with_context


def example_function(param1: str, param2: int) -> dict:
    """Example function to demonstrate logging"""
    logger = get_logger("example.module")
    
    log_function_call(logger, "example_function", param1=param1, param2=param2)
    
    try:
        # Simulate some work
        result = {"message": f"Processed {param1} with value {param2}", "status": "success"}
        
        log_function_result(logger, "example_function", result=result)
        return result
        
    except Exception as e:
        log_error_with_context(logger, e, "example_function")
        raise


def main():
    """Main function demonstrating logging setup and usage"""
    
    # Setup logging with different configurations
    print("Setting up logging...")
    setup_logging(
        default_level="DEBUG",
        log_file="logs/example.log"
    )
    
    # Get logger for main module
    logger = get_logger("main")
    
    logger.info("Starting logging example")
    
    # Demonstrate different log levels
    logger.debug("This is a debug message", extra_info="debug_data")
    logger.info("This is an info message", user="example_user")
    logger.warning("This is a warning message", warning_code="W001")
    logger.error("This is an error message", error_code="E001")
    
    # Demonstrate function logging
    try:
        result = example_function("test_param", 42)
        logger.info("Function executed successfully", result=result)
    except Exception as e:
        logger.exception("Function failed", error=str(e))
    
    # Demonstrate structured logging with extra fields
    logger.info("Processing batch", 
                batch_size=100, 
                batch_id="batch_001", 
                processor="main_worker")
    
    logger.info("Logging example completed")


if __name__ == "__main__":
    main()
