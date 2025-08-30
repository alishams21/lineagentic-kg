#!/usr/bin/env python3
"""
Simple test runner for API generator tests only.
"""

import unittest
import sys
import os
import time
from pathlib import Path

# Add the project root to the Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Import API test modules only
try:
    from tests.test_api_generator import (
        TestAPIGenerator,
        TestRunGenerator as TestAPIRunGenerator
    )
    API_TESTS_AVAILABLE = True
except ImportError as e:
    print(f"⚠️  API generator tests not available: {e}")
    API_TESTS_AVAILABLE = False


def run_api_tests():
    """Run API generator tests and provide a detailed summary."""
    if not API_TESTS_AVAILABLE:
        print("❌ API generator tests cannot be imported due to missing dependencies")
        return False
    
    print("🧪 LineAgentic KG API Generator Test Suite")
    print("=" * 50)
    print()
    
    # Create test suite
    suite = unittest.TestSuite()
    
    # API Generator tests
    api_tests = [
        TestAPIGenerator,
        TestAPIRunGenerator
    ]
    
    for test_class in api_tests:
        suite.addTests(unittest.TestLoader().loadTestsFromTestCase(test_class))
    
    # Run tests
    start_time = time.time()
    runner = unittest.TextTestRunner(verbosity=2, stream=sys.stdout)
    result = runner.run(suite)
    end_time = time.time()
    
    # Print summary
    print()
    print("📊 API Generator Test Summary")
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
    success = run_api_tests()
    
    if success:
        print()
        print("✅ All API generator tests passed!")
        sys.exit(0)
    else:
        print()
        print("❌ Some API generator tests failed!")
        sys.exit(1)
