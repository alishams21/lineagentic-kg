#!/usr/bin/env python3
"""
Automated Data Ingestion Script

This script reads data from example_records.json and uses RegistryFactory
to dynamically ingest all entities, aspects, and relationships into Neo4j.
"""

import json
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime
import sys
import os

# Add the current directory to the path to import RegistryFactory
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from registry_factory import RegistryFactory

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('ingestion.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class AutomatedIngestion:
    """
    Automated ingestion class that reads from JSON and uses RegistryFactory
    to dynamically ingest data into Neo4j.
    """
    
    def __init__(self, registry_path: str, neo4j_uri: str, neo4j_user: str, neo4j_password: str):
        """
        Initialize the automated ingestion system.
        
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
        
        # Initialize RegistryFactory and writer
        self.factory = None
        self.writer = None
        self.ingestion_stats = {
            'entities_created': 0,
            'aspects_created': 0,
            'relationships_created': 0,
            'errors': 0
        }
        
    def initialize(self):
        """Initialize the RegistryFactory and Neo4j writer."""
        try:
            logger.info("Initializing RegistryFactory...")
            self.factory = RegistryFactory(self.registry_path)
            logger.info(f"✅ Registry loaded from: {self.registry_path}")
            
            logger.info("Creating Neo4j writer...")
            self.writer = self.factory.create_writer(
                self.neo4j_uri, 
                self.neo4j_user, 
                self.neo4j_password
            )
            logger.info("✅ Neo4j writer created successfully")
            
            # Log available methods
            logger.info("📋 Available dynamically generated methods:")
            generated_methods = [method for method in dir(self.writer) 
                               if not method.startswith('_') and callable(getattr(self.writer, method))]
            generated_methods.sort()
            for method in generated_methods:
                logger.debug(f"   🔧 writer.{method}()")
            logger.info(f"📊 Total generated methods: {len(generated_methods)}")
            
        except Exception as e:
            logger.error(f"❌ Failed to initialize: {e}")
            raise
    
    def load_records(self, records_file: str) -> Dict[str, Any]:
        """
        Load records from JSON file.
        
        Args:
            records_file: Path to the JSON records file
            
        Returns:
            Dictionary containing the records data
        """
        try:
            logger.info(f"Loading records from: {records_file}")
            with open(records_file, 'r') as f:
                records_data = json.load(f)
            
            logger.info(f"✅ Loaded {len(records_data.get('records', []))} records")
            return records_data
            
        except FileNotFoundError:
            logger.error(f"❌ Records file not found: {records_file}")
            raise
        except json.JSONDecodeError as e:
            logger.error(f"❌ Invalid JSON in records file: {e}")
            raise
    
    def ingest_entity(self, record: Dict[str, Any]) -> Optional[str]:
        """
        Ingest a single entity record.
        
        Args:
            record: Entity record dictionary
            
        Returns:
            Generated URN if successful, None otherwise
        """
        try:
            entity_type = record.get('entity_type')
            entity_key = record.get('key', {})
            
            if not entity_type or not entity_key:
                logger.warning(f"⚠️ Skipping record with missing entity_type or key: {record}")
                return None
            
            # Generate the upsert method name (exact pattern from RegistryFactory)
            method_name = f"upsert_{entity_type.lower()}"
            
            if not hasattr(self.writer, method_name):
                logger.warning(f"⚠️ Method {method_name} not found for entity type {entity_type}")
                return None
            
            # Get the method
            method = getattr(self.writer, method_name)
            
            # Call the method with the key parameters
            logger.debug(f"Calling {method_name} with params: {entity_key}")
            urn = method(**entity_key)
            
            logger.info(f"✅ Created {entity_type}: {urn}")
            self.ingestion_stats['entities_created'] += 1
            return urn
            
        except Exception as e:
            logger.error(f"❌ Failed to ingest entity {record.get('entity_type', 'unknown')}: {e}")
            self.ingestion_stats['errors'] += 1
            return None
    
    def ingest_aspects(self, record: Dict[str, Any], entity_urn: str):
        """
        Ingest aspects for an entity.
        
        Args:
            record: Entity record dictionary
            entity_urn: The entity's URN
        """
        try:
            entity_type = record.get('entity_type')
            aspects = record.get('aspects', {})
            
            if not aspects:
                logger.debug(f"No aspects to ingest for {entity_type}: {entity_urn}")
                return
            
            for aspect_name, aspect_data in aspects.items():
                try:
                    # Skip if aspect_data is empty or doesn't have required fields
                    if not aspect_data or not isinstance(aspect_data, dict):
                        continue
                    
                    # Extract aspect type and payload
                    aspect_type = aspect_data.get('type')
                    payload = {k: v for k, v in aspect_data.items() if k != 'type'}
                    
                    if not payload:
                        logger.debug(f"No payload for aspect {aspect_name}")
                        continue
                    
                    # Generate the upsert method name
                    method_name = f"upsert_{aspect_name.lower()}_aspect"
                    
                    if not hasattr(self.writer, method_name):
                        logger.warning(f"⚠️ Method {method_name} not found for aspect {aspect_name}")
                        continue
                    
                    # Get the method
                    method = getattr(self.writer, method_name)
                    
                    # Call the method
                    logger.debug(f"Calling {method_name} for {entity_type}: {entity_urn}")
                    version = method(entity_type, entity_urn, payload)
                    
                    logger.info(f"✅ Added {aspect_name} aspect to {entity_type}: {entity_urn} (version: {version})")
                    self.ingestion_stats['aspects_created'] += 1
                    
                except Exception as e:
                    logger.error(f"❌ Failed to ingest aspect {aspect_name} for {entity_type}: {entity_urn} - {e}")
                    self.ingestion_stats['errors'] += 1
                    
        except Exception as e:
            logger.error(f"❌ Failed to ingest aspects for {entity_type}: {entity_urn} - {e}")
            self.ingestion_stats['errors'] += 1
    
    def ingest_relationships(self, record: Dict[str, Any], entity_urn: str):
        """
        Ingest relationships for an entity.
        
        Args:
            record: Entity record dictionary
            entity_urn: The entity's URN
        """
        try:
            entity_type = record.get('entity_type')
            relationships = record.get('relationships', [])
            
            if not relationships:
                logger.debug(f"No relationships to ingest for {entity_type}: {entity_urn}")
                return
            
            for rel in relationships:
                try:
                    rel_type = rel.get('type')
                    target_urn = rel.get('target')
                    direction = rel.get('direction', 'outgoing')
                    properties = rel.get('properties', {})
                    
                    if not rel_type or not target_urn:
                        logger.warning(f"⚠️ Skipping relationship with missing type or target: {rel}")
                        continue
                    
                    # Determine method name based on the exact patterns from RegistryFactory
                    # Examples: create_has_column_dataset_to_column_relationship, create_tagged_dataset_to_tag_relationship
                    
                    # First try the entity-specific method name pattern
                    target_entity_type = self._get_target_entity_type(target_urn)
                    method_name = f"create_{rel_type.lower()}_{entity_type.lower()}_to_{target_entity_type.lower()}_relationship"
                    
                    # If that doesn't exist, try the generic pattern
                    if not hasattr(self.writer, method_name):
                        method_name = f"create_{rel_type.lower()}_relationship"
                    
                    if not hasattr(self.writer, method_name):
                        logger.warning(f"⚠️ Method {method_name} not found for relationship {rel_type}")
                        continue
                    
                    # Get the method
                    method = getattr(self.writer, method_name)
                    
                    # Call the method (always outgoing direction as per RegistryFactory pattern)
                    logger.debug(f"Calling {method_name}: {entity_urn} -> {target_urn}")
                    method(entity_urn, target_urn, properties)
                    
                    logger.info(f"✅ Created {rel_type} relationship: {entity_urn} -> {target_urn}")
                    self.ingestion_stats['relationships_created'] += 1
                    
                except Exception as e:
                    logger.error(f"❌ Failed to ingest relationship {rel.get('type', 'unknown')}: {e}")
                    self.ingestion_stats['errors'] += 1
                    
        except Exception as e:
            logger.error(f"❌ Failed to ingest relationships for {entity_type}: {entity_urn} - {e}")
            self.ingestion_stats['errors'] += 1
    
    def ingest_all_records(self, records_data: Dict[str, Any]):
        """
        Ingest all records from the loaded data.
        
        Args:
            records_data: Dictionary containing records data
        """
        records = records_data.get('records', [])
        
        if not records:
            logger.warning("⚠️ No records found in the data")
            return
        
        logger.info(f"🚀 Starting ingestion of {len(records)} records...")
        
        # First pass: Create all entities
        logger.info("📋 Phase 1: Creating entities...")
        entity_urns = {}
        
        for i, record in enumerate(records, 1):
            logger.info(f"Processing record {i}/{len(records)}: {record.get('entity_type', 'unknown')}")
            
            # Ingest the entity
            urn = self.ingest_entity(record)
            if urn:
                entity_urns[record.get('urn')] = urn
        
        # Second pass: Add aspects
        logger.info("📋 Phase 2: Adding aspects...")
        for i, record in enumerate(records, 1):
            urn = entity_urns.get(record.get('urn'))
            if urn:
                self.ingest_aspects(record, urn)
        
        # Third pass: Create relationships
        logger.info("📋 Phase 3: Creating relationships...")
        for i, record in enumerate(records, 1):
            urn = entity_urns.get(record.get('urn'))
            if urn:
                self.ingest_relationships(record, urn)
        
        logger.info("✅ Ingestion completed!")
    
    def print_stats(self):
        """Print ingestion statistics."""
        logger.info("=" * 60)
        logger.info("📊 INGESTION STATISTICS")
        logger.info("=" * 60)
        logger.info(f"Entities created: {self.ingestion_stats['entities_created']}")
        logger.info(f"Aspects created: {self.ingestion_stats['aspects_created']}")
        logger.info(f"Relationships created: {self.ingestion_stats['relationships_created']}")
        logger.info(f"Errors: {self.ingestion_stats['errors']}")
        logger.info("=" * 60)
    
    def _get_target_entity_type(self, target_urn: str) -> str:
        """
        Extract entity type from target URN.
        
        Args:
            target_urn: The target URN
            
        Returns:
            Entity type string
        """
        try:
            # Parse URN to extract entity type
            if target_urn.startswith("urn:li:dataset:"):
                return "dataset"
            elif target_urn.startswith("urn:li:dataflow:"):
                return "dataflow"
            elif target_urn.startswith("urn:li:datajob:"):
                return "datajob"
            elif target_urn.startswith("urn:li:corpuser:"):
                return "corpuser"
            elif target_urn.startswith("urn:li:corpgroup:"):
                return "corpgroup"
            elif target_urn.startswith("urn:li:tag:"):
                return "tag"
            elif "#" in target_urn:  # Column URN
                return "column"
            else:
                return "unknown"
        except Exception:
            return "unknown"
    
    def cleanup(self):
        """Clean up resources."""
        if self.writer:
            self.writer.close()
            logger.info("🔌 Writer connection closed")


def main():
    """Main function to run the automated ingestion."""
    # Configuration
    registry_path = "enhanced_registry.yaml"
    records_file = "example_records.json"
    neo4j_uri = "bolt://localhost:7687"
    neo4j_user = "neo4j"
    neo4j_password = "password"
    
    # Create ingestion instance
    ingestion = AutomatedIngestion(registry_path, neo4j_uri, neo4j_user, neo4j_password)
    
    try:
        # Initialize
        ingestion.initialize()
        
        # Load records
        records_data = ingestion.load_records(records_file)
        
        # Ingest all records
        ingestion.ingest_all_records(records_data)
        
        # Print statistics
        ingestion.print_stats()
        
        logger.info("🎉 Automated ingestion completed successfully!")
        
    except Exception as e:
        logger.error(f"❌ Ingestion failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
        
    finally:
        # Clean up
        ingestion.cleanup()


if __name__ == "__main__":
    main()
