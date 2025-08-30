#!/usr/bin/env python3
"""
Simple test runner for CLI generator tests only.
"""

import unittest
import sys
import os
import time
from pathlib import Path

# Add the project root to the Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Import CLI test modules only
try:
    from tests.test_cli_generator import (
        TestCLIGenerator,
        TestRunGenerator as TestCLIRunGenerator
    )
    CLI_TESTS_AVAILABLE = True
except ImportError as e:
    print(f"⚠️  CLI generator tests not available: {e}")
    CLI_TESTS_AVAILABLE = False


def run_cli_tests():
    """Run CLI generator tests and provide a detailed summary."""
    if not CLI_TESTS_AVAILABLE:
        print("❌ CLI generator tests cannot be imported due to missing dependencies")
        return False
    
    print("🧪 LineAgentic KG CLI Generator Test Suite")
    print("=" * 50)
    print()
    
    # Create test suite
    suite = unittest.TestSuite()
    
    # CLI Generator tests
    cli_tests = [
        TestCLIGenerator,
        TestCLIRunGenerator
    ]
    
    for test_class in cli_tests:
        suite.addTests(unittest.TestLoader().loadTestsFromTestCase(test_class))
    
    # Run tests
    start_time = time.time()
    runner = unittest.TextTestRunner(verbosity=2, stream=sys.stdout)
    result = runner.run(suite)
    end_time = time.time()
    
    # Print summary
    print()
    print("📊 CLI Generator Test Summary")
    print("=" * 50)
    print(f"Total Tests: {result.testsRun}")
    print(f"Passed: {result.testsRun - len(result.failures) - len(result.errors)}")
    print(f"Failed: {len(result.failures)}")
    print(f"Errors: {len(result.errors)}")
    print(f"Execution Time: {end_time - start_time:.2f} seconds")
    
    if result.failures:
        print()
        print("❌ Failures:")
        print("-" * 20)
        for test, traceback in result.failures:
            print(f"• {test}: {traceback.split('AssertionError:')[-1].strip()}")
    
    if result.errors:
        print()
        print("🚨 Errors:")
        print("-" * 20)
        for test, traceback in result.errors:
            print(f"• {test}: {traceback.split('Exception:')[-1].strip()}")
    
    # Return success/failure
    return len(result.failures) == 0 and len(result.errors) == 0


if __name__ == '__main__':
    success = run_cli_tests()
    
    if success:
        print()
        print("✅ All CLI generator tests passed!")
        sys.exit(0)
    else:
        print()
        print("❌ Some CLI generator tests failed!")
        sys.exit(1)
