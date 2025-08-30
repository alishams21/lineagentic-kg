#!/usr/bin/env python3
"""
Simple test runner for registry tests only.
"""

import unittest
import sys
import os
import time
from pathlib import Path

# Add the project root to the Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Import registry test modules only
from tests.test_registry import (
    TestRegistryFactory,
    TestRegistryLoader,
    TestRegistryValidator,
    TestUtilityFunctionBuilder,
    TestURNGeneration,
    TestNeo4jWriterGenerator
)


def run_registry_tests():
    """Run registry tests and provide a detailed summary."""
    print("🧪 LineAgentic KG Registry Test Suite")
    print("=" * 50)
    print()
    
    # Create test suite
    suite = unittest.TestSuite()
    
    # Registry tests
    registry_tests = [
        TestRegistryFactory,
        TestRegistryLoader,
        TestRegistryValidator,
        TestUtilityFunctionBuilder,
        TestURNGeneration,
        TestNeo4jWriterGenerator
    ]
    
    for test_class in registry_tests:
        suite.addTests(unittest.TestLoader().loadTestsFromTestCase(test_class))
    
    # Run tests
    start_time = time.time()
    runner = unittest.TextTestRunner(verbosity=2, stream=sys.stdout)
    result = runner.run(suite)
    end_time = time.time()
    
    # Print summary
    print()
    print("📊 Registry Test Summary")
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
    success = run_registry_tests()
    
    if success:
        print()
        print("✅ All registry tests passed!")
        sys.exit(0)
    else:
        print()
        print("❌ Some registry tests failed!")
        sys.exit(1)
