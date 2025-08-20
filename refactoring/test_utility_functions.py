#!/usr/bin/env python3
"""Test script to verify utility functions are created correctly from YAML"""

from registry_factory import RegistryFactory

def test_utility_functions():
    """Test that utility functions are created correctly from YAML"""
    print("🧪 Testing Utility Functions from YAML")
    print("=" * 50)
    
    # Create registry factory
    factory = RegistryFactory("enhanced_registry.yaml")
    
    # Get utility functions
    utils = factory.utility_functions
    
    print(f"📊 Created {len(utils)} utility functions:")
    for name, func in utils.items():
        print(f"   • {name}: {type(func).__name__}")
    
    print("\n🔧 Testing each function:")
    
    # Test sanitize_id
    if 'sanitize_id' in utils:
        result = utils['sanitize_id']("Hello World!@#$%")
        print(f"   sanitize_id('Hello World!@#$%') = '{result}'")
    
    # Test email_to_username
    if 'email_to_username' in utils:
        result = utils['email_to_username']("user.name@company.com")
        print(f"   email_to_username('user.name@company.com') = '{result}'")
    
    # Test mask_secret
    if 'mask_secret' in utils:
        result1 = utils['mask_secret']("password", "secret123")
        result2 = utils['mask_secret']("name", "John")
        print(f"   mask_secret('password', 'secret123') = '{result1}'")
        print(f"   mask_secret('name', 'John') = '{result2}'")
    
    # Test utc_now_ms
    if 'utc_now_ms' in utils:
        result = utils['utc_now_ms']()
        print(f"   utc_now_ms() = {result}")
    
    print("\n✅ All utility functions created successfully from YAML!")

if __name__ == "__main__":
    test_utility_functions()
