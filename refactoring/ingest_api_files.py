#!/usr/bin/env python3
"""
API File Ingestion Script

This script demonstrates how to ingest individual JSON files that simulate
REST API responses using the updated ingestion.py with independent ingestion.
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


class APIFileIngestion:
    """
    Class to handle ingestion of individual API JSON files.
    """
    
    def __init__(self, registry_path: str, neo4j_uri: str, neo4j_user: str, neo4j_password: str):
        """
        Initialize the API file ingestion system.
        
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
        logger.info("✅ API File Ingestion system initialized")
    
    def load_api_file(self, file_path: str) -> Dict[str, Any]:
        """
        Load a single API JSON file.
        
        Args:
            file_path: Path to the JSON file
            
        Returns:
            Dictionary containing the API data
        """
        try:
            logger.info(f"📁 Loading API file: {file_path}")
            with open(file_path, 'r') as f:
                data = json.load(f)
            
            logger.info(f"✅ Loaded API file: {file_path}")
            return data
            
        except FileNotFoundError:
            logger.error(f"❌ API file not found: {file_path}")
            raise
        except json.JSONDecodeError as e:
            logger.error(f"❌ Invalid JSON in API file: {e}")
            raise
    
    def convert_api_to_record(self, api_data: Dict[str, Any]) -> Dict[str, Any]:
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
    
    def ingest_api_file(self, file_path: str):
        """
        Ingest a single API JSON file.
        
        Args:
            file_path: Path to the JSON file to ingest
        """
        try:
            logger.info(f"🚀 Starting ingestion of API file: {file_path}")
            
            # Load the API file
            api_data = self.load_api_file(file_path)
            
            # Convert to record format
            record = self.convert_api_to_record(api_data)
            
            # Create a records data structure expected by ingestion.py
            records_data = {
                'metadata': {
                    'version': '2.0',
                    'description': f'API file ingestion: {file_path}',
                    'source': 'rest_api'
                },
                'records': [record]
            }
            
            # Ingest using the existing ingestion system
            self.ingestion.ingest_all_records(records_data)
            
            logger.info(f"✅ Successfully ingested API file: {file_path}")
            
        except Exception as e:
            logger.error(f"❌ Failed to ingest API file {file_path}: {e}")
            raise
    
    def ingest_multiple_files(self, file_paths: list):
        """
        Ingest multiple API files in sequence.
        
        Args:
            file_paths: List of file paths to ingest
        """
        logger.info(f"📋 Starting batch ingestion of {len(file_paths)} API files")
        
        for i, file_path in enumerate(file_paths, 1):
            logger.info(f"Processing file {i}/{len(file_paths)}: {file_path}")
            try:
                self.ingest_api_file(file_path)
                logger.info(f"✅ Completed file {i}/{len(file_paths)}")
            except Exception as e:
                logger.error(f"❌ Failed to process file {i}/{len(file_paths)}: {e}")
                # Continue with next file
                continue
        
        logger.info("🎉 Batch ingestion completed")
    
    def cleanup(self):
        """Clean up resources."""
        self.ingestion.cleanup()
        logger.info("🔌 API File Ingestion system cleaned up")


def main():
    """Main function to demonstrate API file ingestion."""
    # Configuration
    registry_path = "enhanced_registry.yaml"
    neo4j_uri = "bolt://localhost:7687"
    neo4j_user = "neo4j"
    neo4j_password = "password"
    
    # API files to ingest
    api_files = [
        "api_entity_dataset.json",
        "api_aspect_datajob.json", 
        "api_relationship_ownership.json"
    ]
    
    # Create API ingestion instance
    api_ingestion = APIFileIngestion(registry_path, neo4j_uri, neo4j_user, neo4j_password)
    
    try:
        # Initialize
        api_ingestion.initialize()
        
        # Option 1: Ingest files one by one
        logger.info("=" * 60)
        logger.info("📁 INGESTING FILES ONE BY ONE")
        logger.info("=" * 60)
        
        for file_path in api_files:
            if os.path.exists(file_path):
                logger.info(f"\n🔄 Processing: {file_path}")
                api_ingestion.ingest_api_file(file_path)
                logger.info(f"✅ Completed: {file_path}")
            else:
                logger.warning(f"⚠️ File not found: {file_path}")
        
        # Option 2: Ingest all files in batch (uncomment to use)
        # logger.info("=" * 60)
        # logger.info("📁 INGESTING FILES IN BATCH")
        # logger.info("=" * 60)
        # api_ingestion.ingest_multiple_files(api_files)
        
        logger.info("\n🎉 All API files processed successfully!")
        
    except Exception as e:
        logger.error(f"❌ API file ingestion failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
        
    finally:
        # Clean up
        api_ingestion.cleanup()


if __name__ == "__main__":
    main()
