#!/usr/bin/env python3
"""
Test Individual Aspect Files Script

This script tests the individual aspect JSON files to ensure they are correctly formatted
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


def load_and_validate_aspect_file(file_path: str) -> Dict[str, Any]:
    """
    Load and validate an individual aspect JSON file.
    
    Args:
        file_path: Path to the JSON file
        
    Returns:
        Dictionary containing the aspect data
    """
    try:
        logger.info(f"📁 Loading aspect file: {file_path}")
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


def validate_aspect_structure(aspect_data: Dict[str, Any], file_path: str):
    """
    Validate the structure of individual aspect data.
    
    Args:
        aspect_data: Aspect data dictionary
        file_path: Path to the file for logging
    """
    logger.info(f"🔍 Validating structure of: {file_path}")
    
    # Check for required metadata
    metadata = aspect_data.get('metadata', {})
    if not metadata:
        logger.warning(f"⚠️ No metadata found in {file_path}")
    else:
        logger.info(f"   📋 Source: {metadata.get('source', 'unknown')}")
        logger.info(f"   📋 Endpoint: {metadata.get('endpoint', 'unknown')}")
    
    # Check for required aspect fields
    required_fields = ['aspect_name', 'aspect_type', 'entity_type', 'entity_key', 'payload']
    missing_fields = []
    
    for field in required_fields:
        if field not in aspect_data:
            missing_fields.append(field)
        else:
            logger.info(f"   ✅ {field}: {aspect_data[field]}")
    
    if missing_fields:
        logger.error(f"❌ Missing required fields: {missing_fields}")
        return False
    
    # Validate aspect type
    aspect_type = aspect_data['aspect_type']
    if aspect_type not in ['versioned', 'timeseries']:
        logger.error(f"❌ Invalid aspect type: {aspect_type}")
        return False
    
    # Validate entity key
    entity_key = aspect_data['entity_key']
    if not isinstance(entity_key, dict) or not entity_key:
        logger.error(f"❌ Invalid entity_key: must be non-empty dictionary")
        return False
    
    # Validate payload
    payload = aspect_data['payload']
    if not isinstance(payload, dict):
        logger.error(f"❌ Invalid payload: must be dictionary")
        return False
    
    logger.info(f"   📊 Entity Key Parameters: {list(entity_key.keys())}")
    logger.info(f"   📊 Payload Keys: {list(payload.keys())}")
    
    logger.info(f"✅ Structure validation completed for: {file_path}")
    return True


def test_aspect_file_conversion(file_path: str):
    """
    Test the conversion of an aspect file to ingestion format.
    
    Args:
        file_path: Path to the aspect file to test
    """
    try:
        logger.info(f"🔄 Testing aspect file: {file_path}")
        
        # Load the aspect file
        aspect_data = load_and_validate_aspect_file(file_path)
        
        # Validate structure
        if not validate_aspect_structure(aspect_data, file_path):
            return False
        
        # Extract aspect information
        aspect_name = aspect_data['aspect_name']
        aspect_type = aspect_data['aspect_type']
        entity_type = aspect_data['entity_type']
        entity_key = aspect_data['entity_key']
        payload = aspect_data['payload']
        
        # Simulate method name generation
        method_name = f"upsert_{aspect_name.lower()}_aspect"
        
        logger.info(f"📋 Aspect Information:")
        logger.info(f"   🏷️ Aspect Name: {aspect_name}")
        logger.info(f"   📊 Aspect Type: {aspect_type}")
        logger.info(f"   🏷️ Entity Type: {entity_type}")
        logger.info(f"   🔧 Method Name: {method_name}")
        logger.info(f"   🔑 Entity Key: {entity_key}")
        logger.info(f"   📦 Payload Size: {len(str(payload))} characters")
        
        # Show sample payload content
        if payload:
            sample_keys = list(payload.keys())[:3]  # Show first 3 keys
            logger.info(f"   📦 Payload Sample Keys: {sample_keys}")
            if len(payload) > 3:
                logger.info(f"   📦 ... and {len(payload) - 3} more keys")
        
        logger.info(f"✅ Aspect file test completed for: {file_path}")
        return True
        
    except Exception as e:
        logger.error(f"❌ Aspect file test failed for {file_path}: {e}")
        return False


def main():
    """Main function to test all individual aspect files."""
    # Individual aspect files to test
    aspect_files = [
        "api_aspect_datasetproperties.json",
        "api_aspect_corpuserinfo.json",
        "api_aspect_schemametadata.json",
        "api_aspect_ownership.json",
        "api_aspect_datasetprofile.json"
    ]
    
    logger.info("=" * 60)
    logger.info("🧪 TESTING INDIVIDUAL ASPECT JSON FILES")
    logger.info("=" * 60)
    
    success_count = 0
    total_count = len(aspect_files)
    
    for file_path in aspect_files:
        logger.info(f"\n{'='*40}")
        if os.path.exists(file_path):
            if test_aspect_file_conversion(file_path):
                success_count += 1
        else:
            logger.warning(f"⚠️ File not found: {file_path}")
    
    logger.info(f"\n{'='*60}")
    logger.info("📊 TEST RESULTS SUMMARY")
    logger.info(f"{'='*60}")
    logger.info(f"Total files tested: {total_count}")
    logger.info(f"Successful validations: {success_count}")
    logger.info(f"Failed validations: {total_count - success_count}")
    
    if success_count == total_count:
        logger.info("🎉 All individual aspect files are ready for ingestion!")
        logger.info("\n📋 ASPECT FILES SUMMARY:")
        logger.info("   • api_aspect_datasetproperties.json - Dataset properties (versioned)")
        logger.info("   • api_aspect_corpuserinfo.json - CorpUser information (versioned)")
        logger.info("   • api_aspect_schemametadata.json - Schema metadata (versioned)")
        logger.info("   • api_aspect_ownership.json - Ownership information (versioned)")
        logger.info("   • api_aspect_datasetprofile.json - Dataset profile (timeseries)")
        
        logger.info("\n🚀 INDEPENDENT INGESTION FEATURES:")
        logger.info("   • Each file contains entity_key for automatic entity creation")
        logger.info("   • No URNs required - entities are created automatically")
        logger.info("   • Payload contains all aspect-specific data")
        logger.info("   • Ready for one-by-one ingestion")
    else:
        logger.warning("⚠️ Some aspect files have issues that need to be fixed.")
    
    logger.info(f"{'='*60}")


if __name__ == "__main__":
    main()
