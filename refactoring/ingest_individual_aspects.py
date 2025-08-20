#!/usr/bin/env python3
"""
Individual Aspect Ingestion Script

This script ingests individual aspect JSON files that contain the necessary
entity parameters for independent ingestion.
"""

import json
import logging
import sys
import os
from typing import Dict, Any

# Add the current directory to the path to import from ingestion.py
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from ingestion import AutomatedIngestion

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class IndividualAspectIngestion:
    """
    Class to handle ingestion of individual aspect JSON files.
    """
    
    def __init__(self, registry_path: str, neo4j_uri: str, neo4j_user: str, neo4j_password: str):
        """
        Initialize the individual aspect ingestion system.
        
        Args:
            registry_path: Path to the enhanced registry YAML file
            neo4j_uri: Neo4j connection URI
            neo4j_user: Neo4j username
            neo4j_password: Neo4j password
        """
        self.registry_path = registry_path
        self.neo4j_uri = neo4j_uri
        self.neo4j_user = neo4j_user
        self.neo4j_password = neo4j_password
        
        # Initialize the automated ingestion system
        self.ingestion = AutomatedIngestion(
            registry_path, neo4j_uri, neo4j_user, neo4j_password
        )
        
    def initialize(self):
        """Initialize the ingestion system."""
        self.ingestion.initialize()
        logger.info("✅ Individual Aspect Ingestion system initialized")
    
    def load_aspect_file(self, file_path: str) -> Dict[str, Any]:
        """
        Load a single aspect JSON file.
        
        Args:
            file_path: Path to the JSON file
            
        Returns:
            Dictionary containing the aspect data
        """
        try:
            logger.info(f"📁 Loading aspect file: {file_path}")
            with open(file_path, 'r') as f:
                data = json.load(f)
            
            logger.info(f"✅ Loaded aspect file: {file_path}")
            return data
            
        except FileNotFoundError:
            logger.error(f"❌ Aspect file not found: {file_path}")
            raise
        except json.JSONDecodeError as e:
            logger.error(f"❌ Invalid JSON in aspect file: {e}")
            raise
    
    def validate_aspect_data(self, aspect_data: Dict[str, Any]) -> bool:
        """
        Validate the structure of aspect data.
        
        Args:
            aspect_data: Aspect data dictionary
            
        Returns:
            True if valid, False otherwise
        """
        required_fields = ['aspect_name', 'aspect_type', 'entity_type', 'entity_key', 'payload']
        
        for field in required_fields:
            if field not in aspect_data:
                logger.error(f"❌ Missing required field: {field}")
                return False
        
        logger.info(f"✅ Aspect data validation passed")
        logger.info(f"   📊 Aspect: {aspect_data['aspect_name']}")
        logger.info(f"   📊 Type: {aspect_data['aspect_type']}")
        logger.info(f"   📊 Entity: {aspect_data['entity_type']}")
        logger.info(f"   📊 Entity Key: {list(aspect_data['entity_key'].keys())}")
        
        return True
    
    def ingest_aspect_file(self, file_path: str):
        """
        Ingest a single aspect JSON file.
        
        Args:
            file_path: Path to the JSON file to ingest
        """
        try:
            logger.info(f"🚀 Starting ingestion of aspect file: {file_path}")
            
            # Load the aspect file
            aspect_data = self.load_aspect_file(file_path)
            
            # Validate the aspect data
            if not self.validate_aspect_data(aspect_data):
                raise ValueError(f"Invalid aspect data in {file_path}")
            
            # Extract aspect information
            aspect_name = aspect_data['aspect_name']
            aspect_type = aspect_data['aspect_type']
            entity_type = aspect_data['entity_type']
            entity_key = aspect_data['entity_key']
            payload = aspect_data['payload']
            
            # Generate the method name
            method_name = f"upsert_{aspect_name.lower()}_aspect"
            
            if not hasattr(self.ingestion.writer, method_name):
                raise ValueError(f"Method {method_name} not found for aspect {aspect_name}")
            
            # Get the method
            method = getattr(self.ingestion.writer, method_name)
            
            # Call the method with independent ingestion parameters
            logger.info(f"📞 Calling {method_name} with independent ingestion")
            
            if aspect_type == 'versioned':
                version = method(payload=payload, **entity_key)
                logger.info(f"✅ Successfully ingested {aspect_name} aspect (version: {version})")
            else:  # timeseries
                method(payload=payload, **entity_key)
                logger.info(f"✅ Successfully ingested {aspect_name} aspect (timeseries)")
            
            logger.info(f"✅ Successfully ingested aspect file: {file_path}")
            
        except Exception as e:
            logger.error(f"❌ Failed to ingest aspect file {file_path}: {e}")
            raise
    
    def ingest_multiple_aspect_files(self, file_paths: list):
        """
        Ingest multiple aspect files in sequence.
        
        Args:
            file_paths: List of file paths to ingest
        """
        logger.info(f"📋 Starting batch ingestion of {len(file_paths)} aspect files")
        
        success_count = 0
        total_count = len(file_paths)
        
        for i, file_path in enumerate(file_paths, 1):
            logger.info(f"\n{'='*40}")
            logger.info(f"Processing aspect file {i}/{total_count}: {file_path}")
            try:
                self.ingest_aspect_file(file_path)
                success_count += 1
                logger.info(f"✅ Completed aspect file {i}/{total_count}")
            except Exception as e:
                logger.error(f"❌ Failed to process aspect file {i}/{total_count}: {e}")
                # Continue with next file
                continue
        
        logger.info(f"\n{'='*60}")
        logger.info("📊 BATCH INGESTION SUMMARY")
        logger.info(f"{'='*60}")
        logger.info(f"Total files processed: {total_count}")
        logger.info(f"Successful ingestions: {success_count}")
        logger.info(f"Failed ingestions: {total_count - success_count}")
        
        if success_count == total_count:
            logger.info("🎉 All aspect files ingested successfully!")
        else:
            logger.warning("⚠️ Some aspect files failed to ingest.")
        
        logger.info(f"{'='*60}")
    
    def cleanup(self):
        """Clean up resources."""
        self.ingestion.cleanup()
        logger.info("🔌 Individual Aspect Ingestion system cleaned up")


def main():
    """Main function to demonstrate individual aspect ingestion."""
    # Configuration
    registry_path = "enhanced_registry.yaml"
    neo4j_uri = "bolt://localhost:7687"
    neo4j_user = "neo4j"
    neo4j_password = "password"
    
    # Individual aspect files to ingest
    aspect_files = [
        "api_aspect_datasetproperties.json",
        "api_aspect_corpuserinfo.json",
        "api_aspect_schemametadata.json",
        "api_aspect_ownership.json",
        "api_aspect_datasetprofile.json"
    ]
    
    # Create individual aspect ingestion instance
    aspect_ingestion = IndividualAspectIngestion(registry_path, neo4j_uri, neo4j_user, neo4j_password)
    
    try:
        # Initialize
        aspect_ingestion.initialize()
        
        # Option 1: Ingest files one by one
        logger.info("=" * 60)
        logger.info("📁 INGESTING INDIVIDUAL ASPECT FILES")
        logger.info("=" * 60)
        
        for file_path in aspect_files:
            if os.path.exists(file_path):
                logger.info(f"\n🔄 Processing: {file_path}")
                aspect_ingestion.ingest_aspect_file(file_path)
                logger.info(f"✅ Completed: {file_path}")
            else:
                logger.warning(f"⚠️ File not found: {file_path}")
        
        # Option 2: Ingest all files in batch (uncomment to use)
        # logger.info("=" * 60)
        # logger.info("📁 INGESTING ASPECT FILES IN BATCH")
        # logger.info("=" * 60)
        # aspect_ingestion.ingest_multiple_aspect_files(aspect_files)
        
        logger.info("\n🎉 All individual aspect files processed successfully!")
        
    except Exception as e:
        logger.error(f"❌ Individual aspect ingestion failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
        
    finally:
        # Clean up
        aspect_ingestion.cleanup()


if __name__ == "__main__":
    main()
