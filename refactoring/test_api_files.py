#!/usr/bin/env python3
"""
Test API Files Script

This script tests the API JSON files to ensure they are correctly formatted
and can be processed by the ingestion system without connecting to Neo4j.
"""

import json
import logging
import sys
import os
from typing import Dict, Any

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def load_and_validate_api_file(file_path: str) -> Dict[str, Any]:
    """
    Load and validate an API JSON file.
    
    Args:
        file_path: Path to the JSON file
        
    Returns:
        Dictionary containing the API data
    """
    try:
        logger.info(f"📁 Loading API file: {file_path}")
        with open(file_path, 'r') as f:
            data = json.load(f)
        
        logger.info(f"✅ Successfully loaded: {file_path}")
        return data
        
    except FileNotFoundError:
        logger.error(f"❌ File not found: {file_path}")
        raise
    except json.JSONDecodeError as e:
        logger.error(f"❌ Invalid JSON in {file_path}: {e}")
        raise


def validate_api_structure(api_data: Dict[str, Any], file_path: str):
    """
    Validate the structure of API data.
    
    Args:
        api_data: API response data
        file_path: Path to the file for logging
    """
    logger.info(f"🔍 Validating structure of: {file_path}")
    
    # Check for required metadata
    metadata = api_data.get('metadata', {})
    if not metadata:
        logger.warning(f"⚠️ No metadata found in {file_path}")
    else:
        logger.info(f"   📋 Source: {metadata.get('source', 'unknown')}")
        logger.info(f"   📋 Endpoint: {metadata.get('endpoint', 'unknown')}")
    
    # Check for entity information
    entity_type = api_data.get('entity_type')
    if entity_type:
        logger.info(f"   🏷️ Entity type: {entity_type}")
    
    # Check for key information
    key = api_data.get('key', {})
    if key:
        logger.info(f"   🔑 Key parameters: {list(key.keys())}")
    
    # Check for aspects
    aspects = api_data.get('aspects', {})
    if aspects:
        logger.info(f"   📊 Aspects: {list(aspects.keys())}")
    
    # Check for relationships
    relationships = api_data.get('relationships', [])
    if relationships:
        logger.info(f"   🔗 Relationships: {len(relationships)} found")
    
    # Check for relationship-specific format
    relationship_type = api_data.get('relationship_type')
    if relationship_type:
        logger.info(f"   🔗 Relationship type: {relationship_type}")
        source = api_data.get('source', {})
        target = api_data.get('target', {})
        if source:
            logger.info(f"   📤 Source: {source.get('entity_type', 'unknown')}")
        if target:
            logger.info(f"   📥 Target: {target.get('entity_type', 'unknown')}")
    
    logger.info(f"✅ Structure validation completed for: {file_path}")


def convert_api_to_record(api_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Convert API data format to the record format expected by ingestion.py.
    
    Args:
        api_data: API response data
        
    Returns:
        Record format data
    """
    # Extract the main entity information
    entity_type = api_data.get('entity_type')
    key = api_data.get('key', {})
    aspects = api_data.get('aspects', {})
    relationships = api_data.get('relationships', [])
    
    # Handle relationship-specific API format
    if 'relationship_type' in api_data:
        # This is a relationship-focused API response
        relationship_type = api_data['relationship_type']
        source = api_data.get('source', {})
        target = api_data.get('target', {})
        properties = api_data.get('properties', {})
        
        # Convert to record format
        record = {
            'entity_type': source.get('entity_type', 'Unknown'),
            'key': source.get('key', {}),
            'relationships': [{
                'type': relationship_type,
                'target': target,
                'direction': 'outgoing',
                'properties': properties
            }]
        }
    else:
        # This is an entity/aspect-focused API response
        record = {
            'entity_type': entity_type,
            'key': key,
            'aspects': aspects,
            'relationships': relationships
        }
    
    return record


def test_api_file_conversion(file_path: str):
    """
    Test the conversion of an API file to record format.
    
    Args:
        file_path: Path to the API file to test
    """
    try:
        logger.info(f"🔄 Testing conversion for: {file_path}")
        
        # Load the API file
        api_data = load_and_validate_api_file(file_path)
        
        # Validate structure
        validate_api_structure(api_data, file_path)
        
        # Convert to record format
        record = convert_api_to_record(api_data)
        
        # Validate converted record
        logger.info(f"📋 Converted record structure:")
        logger.info(f"   🏷️ Entity type: {record.get('entity_type', 'N/A')}")
        logger.info(f"   🔑 Key parameters: {list(record.get('key', {}).keys())}")
        
        aspects = record.get('aspects', {})
        if aspects:
            logger.info(f"   📊 Aspects: {list(aspects.keys())}")
        
        relationships = record.get('relationships', [])
        if relationships:
            logger.info(f"   🔗 Relationships: {len(relationships)}")
            for i, rel in enumerate(relationships[:3]):  # Show first 3
                rel_type = rel.get('type', 'unknown')
                target = rel.get('target', {})
                if isinstance(target, dict):
                    target_type = target.get('entity_type', 'unknown')
                else:
                    target_type = str(target)
                logger.info(f"      {i+1}. {rel_type} -> {target_type}")
            if len(relationships) > 3:
                logger.info(f"      ... and {len(relationships) - 3} more")
        
        logger.info(f"✅ Conversion test completed for: {file_path}")
        return True
        
    except Exception as e:
        logger.error(f"❌ Conversion test failed for {file_path}: {e}")
        return False


def main():
    """Main function to test all API files."""
    # API files to test
    api_files = [
        "api_entity_dataset.json",
        "api_aspect_datajob.json", 
        "api_relationship_ownership.json"
    ]
    
    logger.info("=" * 60)
    logger.info("🧪 TESTING API JSON FILES")
    logger.info("=" * 60)
    
    success_count = 0
    total_count = len(api_files)
    
    for file_path in api_files:
        logger.info(f"\n{'='*40}")
        if os.path.exists(file_path):
            if test_api_file_conversion(file_path):
                success_count += 1
        else:
            logger.warning(f"⚠️ File not found: {file_path}")
    
    logger.info(f"\n{'='*60}")
    logger.info("📊 TEST RESULTS SUMMARY")
    logger.info(f"{'='*60}")
    logger.info(f"Total files tested: {total_count}")
    logger.info(f"Successful conversions: {success_count}")
    logger.info(f"Failed conversions: {total_count - success_count}")
    
    if success_count == total_count:
        logger.info("🎉 All API files are ready for ingestion!")
    else:
        logger.warning("⚠️ Some API files have issues that need to be fixed.")
    
    logger.info(f"{'='*60}")


if __name__ == "__main__":
    main()
