#!/usr/bin/env python3
"""
Test script to demonstrate dynamic transformation type discovery
"""

import os
import sys
import json
from registry_factory import RegistryFactory

def test_dynamic_transformation_discovery():
    """Test dynamic transformation type discovery functionality"""
    
    # Initialize RegistryFactory
    registry_path = "enhanced_registry.yaml"
    factory = RegistryFactory(registry_path)
    
    print("🧪 Testing Dynamic Transformation Type Discovery")
    print("=" * 60)
    
    # Load sample transformation data
    staging_data_path = os.path.join("data", "staging_customer_data.json")
    final_data_path = os.path.join("data", "final_customer_data.json")
    
    with open(staging_data_path, 'r') as f:
        staging_data = json.load(f)
    
    with open(final_data_path, 'r') as f:
        final_data = json.load(f)
    
    staging_transformations = staging_data.get('transformations', {})
    final_transformations = final_data.get('transformations', {})
    
    print("\n1. Testing Transformation Type Discovery:")
    print("-" * 40)
    
    # Discover transformation types from staging data
    staging_types = factory.discover_transformation_types(staging_transformations)
    print(f"Staging transformation types: {staging_types}")
    
    # Discover transformation types from final data
    final_types = factory.discover_transformation_types(final_transformations)
    print(f"Final transformation types: {final_types}")
    
    # All unique transformation types
    all_types = list(set(staging_types + final_types))
    print(f"All unique transformation types: {all_types}")
    
    print("\n2. Testing Transformation Statistics:")
    print("-" * 40)
    
    # Get statistics for staging transformations
    staging_stats = factory.get_transformation_statistics(staging_transformations)
    print("Staging transformations statistics:")
    print(f"  Total transformations: {staging_stats['total_transformations']}")
    print(f"  Transformation types: {staging_stats['transformation_types']}")
    print(f"  Input column usage: {staging_stats['input_column_usage']}")
    print(f"  Target columns: {staging_stats['target_columns']}")
    
    # Get statistics for final transformations
    final_stats = factory.get_transformation_statistics(final_transformations)
    print("\nFinal transformations statistics:")
    print(f"  Total transformations: {final_stats['total_transformations']}")
    print(f"  Transformation types: {final_stats['transformation_types']}")
    print(f"  Input column usage: {final_stats['input_column_usage']}")
    print(f"  Target columns: {final_stats['target_columns']}")
    
    print("\n3. Testing Dynamic Aspect Generation:")
    print("-" * 40)
    
    # Test aspect generation for each transformation type
    source_dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,raw_customer_data,PROD)"
    target_dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,staging_customer_data,PROD)"
    
    for column_name, transformation_data in staging_transformations.items():
        print(f"\n  Generating aspect for {column_name} ({transformation_data['type']}):")
        
        aspect_payload = factory.generate_transformation_aspect(
            transformation_data, source_dataset_urn, target_dataset_urn
        )
        
        print(f"    Input columns: {aspect_payload['inputColumns']}")
        print(f"    Transformation type: {aspect_payload['transformationType']}")
        print(f"    Steps: {len(aspect_payload['steps'])}")
    
    print("\n4. Testing Dynamic Relationship Properties Generation:")
    print("-" * 40)
    
    # Test relationship properties for each transformation type
    for column_name, transformation_data in staging_transformations.items():
        print(f"\n  Generating relationship properties for {column_name} ({transformation_data['type']}):")
        
        relationship_props = factory.generate_lineage_relationship_properties(
            transformation_data, source_dataset_urn, target_dataset_urn
        )
        
        print(f"    Type: {relationship_props.get('type')}")
        print(f"    Subtype: {relationship_props.get('subtype')}")
        print(f"    Transformation: {relationship_props.get('transformation')}")
        print(f"    Description: {relationship_props.get('description')}")
    
    print("\n5. Testing Custom Transformation Types:")
    print("-" * 40)
    
    # Test with custom transformation types not in patterns
    custom_transformations = {
        "custom_field": {
            "type": "CUSTOM_TRANSFORMATION",
            "input_columns": ["field1", "field2"],
            "description": "Custom transformation for testing",
            "config": {"custom_param": "value"}
        },
        "ai_enhanced": {
            "type": "AI_ENHANCEMENT",
            "input_columns": ["text_field"],
            "description": "AI-powered text enhancement",
            "config": {"model": "gpt-4", "temperature": 0.7}
        }
    }
    
    custom_types = factory.discover_transformation_types(custom_transformations)
    print(f"Custom transformation types: {custom_types}")
    
    # Test aspect generation for custom types
    for column_name, transformation_data in custom_transformations.items():
        print(f"\n  Custom transformation {column_name} ({transformation_data['type']}):")
        
        aspect_payload = factory.generate_transformation_aspect(
            transformation_data, source_dataset_urn, target_dataset_urn
        )
        
        relationship_props = factory.generate_lineage_relationship_properties(
            transformation_data, source_dataset_urn, target_dataset_urn
        )
        
        print(f"    Aspect generated: {aspect_payload['transformationType']}")
        print(f"    Relationship subtype: {relationship_props.get('subtype')}")
        print(f"    Relationship transformation: {relationship_props.get('transformation')}")
    
    print("\n6. Testing Transformation Validation:")
    print("-" * 40)
    
    # Test with invalid transformations
    invalid_transformations = {
        "missing_type": {
            "input_columns": ["field1"],
            "description": "Missing type field"
        },
        "missing_input": {
            "type": "VALIDATION",
            "description": "Missing input_columns field"
        },
        "valid_transformation": {
            "type": "VALIDATION",
            "input_columns": ["field1"],
            "description": "Valid transformation"
        }
    }
    
    validated = factory.validate_transformation_data(invalid_transformations)
    print(f"Valid transformations after validation: {list(validated.keys())}")
    
    print("\n✅ Dynamic transformation discovery test completed successfully!")

def test_registry_configuration():
    """Test registry configuration for dynamic discovery"""
    print("\n🔧 Testing Registry Configuration:")
    print("-" * 40)
    
    registry_path = "enhanced_registry.yaml"
    factory = RegistryFactory(registry_path)
    
    # Check lineage configuration
    lineage_config = factory.registry.get('lineage_config', {})
    transformation_discovery = lineage_config.get('transformation_discovery', {})
    transformation_templates = lineage_config.get('transformation_templates', {})
    
    print(f"Transformation discovery enabled: {transformation_discovery.get('enabled', False)}")
    print(f"Auto-generate properties: {transformation_discovery.get('auto_generate_properties', False)}")
    
    # Check available patterns
    patterns = transformation_templates.get('patterns', {})
    print(f"Available transformation patterns: {list(patterns.keys())}")
    
    # Check default template
    default_template = transformation_templates.get('default', {})
    print(f"Default template available: {bool(default_template)}")
    if default_template:
        print(f"  Description template: {default_template.get('description_template', 'N/A')}")

if __name__ == "__main__":
    try:
        test_dynamic_transformation_discovery()
        test_registry_configuration()
    except Exception as e:
        print(f"❌ Test failed with error: {str(e)}")
        import traceback
        traceback.print_exc()
