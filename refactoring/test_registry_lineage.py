#!/usr/bin/env python3
"""
Test script to demonstrate registry-driven column-level lineage generation
"""

import os
import sys
from registry_factory import RegistryFactory

def test_registry_lineage():
    """Test the registry-driven lineage generation functionality"""
    
    # Initialize RegistryFactory
    registry_path = "enhanced_registry.yaml"
    factory = RegistryFactory(registry_path)
    
    print("🧪 Testing Registry-Driven Column-Level Lineage")
    print("=" * 60)
    
    # Test transformation aspect generation
    print("\n1. Testing Transformation Aspect Generation:")
    print("-" * 40)
    
    # Sample transformation data
    transformation_data = {
        "type": "CONCATENATION",
        "input_columns": ["first_name", "last_name"],
        "description": "Combine first and last name with space separator",
        "config": {"separator": " ", "trim": True}
    }
    
    source_dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,raw_customer_data,PROD)"
    target_dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,staging_customer_data,PROD)"
    
    # Generate transformation aspect
    aspect_payload = factory.generate_transformation_aspect(
        transformation_data, source_dataset_urn, target_dataset_urn
    )
    
    print(f"Generated aspect payload:")
    for key, value in aspect_payload.items():
        print(f"  {key}: {value}")
    
    # Test lineage relationship properties generation
    print("\n2. Testing Lineage Relationship Properties Generation:")
    print("-" * 40)
    
    relationship_props = factory.generate_lineage_relationship_properties(
        transformation_data, source_dataset_urn, target_dataset_urn
    )
    
    print(f"Generated relationship properties:")
    for key, value in relationship_props.items():
        print(f"  {key}: {value}")
    
    # Test different transformation types
    print("\n3. Testing Different Transformation Types:")
    print("-" * 40)
    
    transformation_types = [
        {
            "type": "HASHING",
            "input_columns": ["email_address"],
            "description": "Hash email address using SHA-256",
            "config": {"algorithm": "SHA-256", "salt": "customer_salt"}
        },
        {
            "type": "EXTRACTION",
            "input_columns": ["registration_date"],
            "description": "Extract year from registration date",
            "config": {"format": "YYYY", "sourceFormat": "YYYY-MM-DD"}
        },
        {
            "type": "SCORING",
            "input_columns": ["customer_id", "full_name", "email_hash"],
            "description": "Calculate data quality score",
            "config": {"algorithm": "weighted_average", "weights": {"completeness": 0.4}}
        }
    ]
    
    for i, transform_data in enumerate(transformation_types, 1):
        print(f"\n  {i}. {transform_data['type']} transformation:")
        props = factory.generate_lineage_relationship_properties(
            transform_data, source_dataset_urn, target_dataset_urn
        )
        print(f"     Subtype: {props.get('subtype')}")
        print(f"     Transformation: {props.get('transformation')}")
        print(f"     Description: {props.get('description')}")
    
    # Test registry configuration access
    print("\n4. Testing Registry Configuration Access:")
    print("-" * 40)
    
    lineage_config = factory.registry.get('lineage_config', {})
    transformation_types_config = lineage_config.get('transformation_types', {})
    
    print(f"Available transformation types: {list(transformation_types_config.keys())}")
    
    # Show CONCATENATION configuration
    concat_config = transformation_types_config.get('CONCATENATION', {})
    print(f"\nCONCATENATION configuration:")
    print(f"  Description template: {concat_config.get('description_template')}")
    print(f"  Default config: {concat_config.get('default_config')}")
    print(f"  Relationship properties: {concat_config.get('relationship_properties')}")
    
    print("\n✅ Registry-driven lineage generation test completed successfully!")

def test_urn_generation():
    """Test URN generation for columns"""
    print("\n🔗 Testing Column URN Generation:")
    print("-" * 40)
    
    registry_path = "enhanced_registry.yaml"
    factory = RegistryFactory(registry_path)
    
    # Test column URN generation
    dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,staging_customer_data,PROD)"
    field_path = "full_name"
    
    column_urn = factory.urn_generators['column'](
        dataset_urn=dataset_urn,
        field_path=field_path
    )
    
    print(f"Dataset URN: {dataset_urn}")
    print(f"Field path: {field_path}")
    print(f"Generated column URN: {column_urn}")
    
    # Test with different field paths
    field_paths = ["customer_id", "email_hash", "phone_clean", "registration_year"]
    for field_path in field_paths:
        column_urn = factory.urn_generators['column'](
            dataset_urn=dataset_urn,
            field_path=field_path
        )
        print(f"  {field_path} -> {column_urn}")

if __name__ == "__main__":
    try:
        test_registry_lineage()
        test_urn_generation()
    except Exception as e:
        print(f"❌ Test failed with error: {str(e)}")
        import traceback
        traceback.print_exc()
