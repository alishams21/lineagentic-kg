#!/usr/bin/env python3
"""
Data Ingestion Script for Column-Level Lineage Example

This script reads 3 JSON files containing customer data at different stages:
1. Raw customer data (from source system)
2. Staging customer data (cleaned and transformed)
3. Final customer data (business-ready)

It uses RegistryFactory to create entities, aspects, and relationships in Neo4j,
establishing complete column-level lineage tracking.
"""

import json
import os
import sys
from datetime import datetime
from typing import Dict, Any, List

# Add the current directory to Python path to import RegistryFactory
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from registry_factory import RegistryFactory


class DataIngestionPipeline:
    """Pipeline for ingesting customer data with column-level lineage"""
    
    def __init__(self, registry_path: str, neo4j_uri: str, neo4j_user: str, neo4j_password: str):
        """Initialize the ingestion pipeline"""
        self.registry_path = registry_path
        self.neo4j_uri = neo4j_uri
        self.neo4j_user = neo4j_user
        self.neo4j_password = neo4j_password
        
        # Initialize RegistryFactory
        print("Initializing RegistryFactory...")
        self.factory = RegistryFactory(registry_path)
        self.writer = self.factory.create_writer(neo4j_uri, neo4j_user, neo4j_password)
        
        # Store URNs for lineage creation
        self.dataset_urns = {}
        self.column_urns = {}
        
    def load_json_data(self, file_path: str) -> Dict[str, Any]:
        """Load data from JSON file"""
        print(f"Loading data from {file_path}...")
        with open(file_path, 'r') as f:
            return json.load(f)
    
    def ingest_raw_dataset(self, data: Dict[str, Any]) -> str:
        """Ingest raw customer dataset"""
        print("\n=== Ingesting Raw Customer Dataset ===")
        
        # Create dataset
        dataset_info = data['dataset_info']
        dataset_urn = self.writer.upsert_dataset(
            platform=dataset_info['platform'],
            name=dataset_info['name'],
            env=dataset_info['env']
        )
        self.dataset_urns['raw'] = dataset_urn
        print(f"Created raw dataset: {dataset_urn}")
        
        # Create aspects
        self._create_dataset_aspects(dataset_urn, data, 'raw')
        
        # Create columns
        self._create_columns(dataset_urn, data['schema']['fields'], 'raw')
        
        return dataset_urn
    
    def ingest_staging_dataset(self, data: Dict[str, Any]) -> str:
        """Ingest staging customer dataset"""
        print("\n=== Ingesting Staging Customer Dataset ===")
        
        # Create dataset
        dataset_info = data['dataset_info']
        dataset_urn = self.writer.upsert_dataset(
            platform=dataset_info['platform'],
            name=dataset_info['name'],
            env=dataset_info['env']
        )
        self.dataset_urns['staging'] = dataset_urn
        print(f"Created staging dataset: {dataset_urn}")
        
        # Create aspects
        self._create_dataset_aspects(dataset_urn, data, 'staging')
        
        # Create columns with transformations
        self._create_columns_with_transformations(dataset_urn, data, 'staging')
        
        return dataset_urn
    
    def ingest_final_dataset(self, data: Dict[str, Any]) -> str:
        """Ingest final customer dataset"""
        print("\n=== Ingesting Final Customer Dataset ===")
        
        # Create dataset
        dataset_info = data['dataset_info']
        dataset_urn = self.writer.upsert_dataset(
            platform=dataset_info['platform'],
            name=dataset_info['name'],
            env=dataset_info['env']
        )
        self.dataset_urns['final'] = dataset_urn
        print(f"Created final dataset: {dataset_urn}")
        
        # Create aspects
        self._create_dataset_aspects(dataset_urn, data, 'final')
        
        # Create columns with transformations
        self._create_columns_with_transformations(dataset_urn, data, 'final')
        
        return dataset_urn
    
    def _create_dataset_aspects(self, dataset_urn: str, data: Dict[str, Any], stage: str):
        """Create all aspects for a dataset"""
        print(f"Creating aspects for {stage} dataset...")
        
        # Dataset Properties
        dataset_info = data['dataset_info']
        properties_payload = {
            "description": dataset_info['description'],
            "customProperties": {
                "source": dataset_info['source'],
                "environment": dataset_info['env'],
                "data_type": dataset_info['data_type']
            }
        }
        self.writer.upsert_datasetproperties_aspect("Dataset", dataset_urn, properties_payload)
        
        # Schema Metadata
        schema_payload = data['schema']
        self.writer.upsert_schemametadata_aspect("Dataset", dataset_urn, schema_payload)
        
        # Dataset Profile
        profile_data = data['profile']
        profile_payload = {
            "rowCount": profile_data['rowCount'],
            "columnCount": profile_data['columnCount'],
            "sizeInBytes": profile_data['sizeInBytes'],
            "lastModified": self.writer.utc_now_ms()
        }
        self.writer.upsert_datasetprofile_aspect("Dataset", dataset_urn, profile_payload)
        
        # Ownership (assign to data engineering team)
        ownership_payload = {
            "owners": [
                {
                    "owner": "urn:li:corpuser:john.doe",
                    "type": "DATAOWNER",
                    "source": "ingestion"
                }
            ]
        }
        self.writer.upsert_ownership_aspect("Dataset", dataset_urn, ownership_payload)
        
        # Global Tags
        global_tags_payload = {
            "tags": [
                {
                    "tag": "urn:li:tag:PII=true",
                    "source": "ingestion"
                },
                {
                    "tag": "urn:li:tag:SENSITIVE",
                    "source": "ingestion"
                }
            ]
        }
        self.writer.upsert_globaltags_aspect("Dataset", dataset_urn, global_tags_payload)
        
        # Column Lineage
        self._create_column_lineage_aspect(dataset_urn, stage)
    
    def _create_columns(self, dataset_urn: str, fields: List[Dict[str, Any]], stage: str):
        """Create columns for a dataset"""
        print(f"Creating columns for {stage} dataset...")
        
        stage_columns = {}
        for field in fields:
            field_path = field['fieldPath']
            column_urn = self.writer.upsert_column(dataset_urn=dataset_urn, field_path=field_path)
            stage_columns[field_path] = column_urn
            
            # Create column properties
            col_props = {
                "description": field['description'],
                "dataType": field['type']['type'],
                "nullable": field.get('nullable', True),
                "defaultValue": None
            }
            self.writer.upsert_columnproperties_aspect("Column", column_urn, col_props)
            
            # Create HAS_COLUMN relationship
            self.writer.create_has_column_relationship(dataset_urn, column_urn)
            
            print(f"  Created column: {field_path} -> {column_urn}")
        
        self.column_urns[stage] = stage_columns
    
    def _create_columns_with_transformations(self, dataset_urn: str, data: Dict[str, Any], stage: str):
        """Create columns with transformation aspects"""
        print(f"Creating columns with transformations for {stage} dataset...")
        
        fields = data['schema']['fields']
        transformations = data.get('transformations', {})
        
        stage_columns = {}
        for field in fields:
            field_path = field['fieldPath']
            column_urn = self.writer.upsert_column(dataset_urn=dataset_urn, field_path=field_path)
            stage_columns[field_path] = column_urn
            
            # Create column properties
            col_props = {
                "description": field['description'],
                "dataType": field['type']['type'],
                "nullable": field.get('nullable', True),
                "defaultValue": None
            }
            self.writer.upsert_columnproperties_aspect("Column", column_urn, col_props)
            
            # Create transformation aspect if exists
            if field_path in transformations:
                transform_data = transformations[field_path]
                
                # Use aspect-driven transformation aspect generation
                transform_payload = self.factory.generate_transformation_aspect(
                    transform_data, 
                    source_dataset_urn=None,  # Will be set when lineage is created
                    target_dataset_urn=dataset_urn
                )
                
                self.writer.upsert_transformation_aspect("Column", column_urn, transform_payload)
                print(f"  Created column with transformation: {field_path} ({transform_data.get('type')})")
            else:
                print(f"  Created column: {field_path}")
            
            # Create HAS_COLUMN relationship
            self.writer.create_has_column_relationship(dataset_urn, column_urn)
        
        self.column_urns[stage] = stage_columns
    
    def _create_column_lineage_aspect(self, dataset_urn: str, stage: str):
        """Create column lineage aspect for dataset"""
        if stage == 'raw':
            # Raw dataset lineage
            lineage_payload = {
                "upstreams": [
                    {
                        "dataset": "urn:li:dataset:(urn:li:dataPlatform:crm,raw_customer_export,PROD)",
                        "columns": ["customer_id", "first_name", "last_name", "email_address", "phone_number", "registration_date"]
                    }
                ],
                "downstreams": [
                    {
                        "dataset": self.dataset_urns.get('staging', 'urn:li:dataset:(urn:li:dataPlatform:snowflake,staging_customer_data,PROD)'),
                        "columns": ["customer_id", "full_name", "email_hash", "phone_clean", "registration_year"]
                    }
                ]
            }
        elif stage == 'staging':
            # Staging dataset lineage
            lineage_payload = {
                "upstreams": [
                    {
                        "dataset": self.dataset_urns.get('raw', 'urn:li:dataset:(urn:li:dataPlatform:snowflake,raw_customer_data,PROD)'),
                        "columns": ["customer_id", "first_name", "last_name", "email_address", "phone_number", "registration_date"]
                    }
                ],
                "downstreams": [
                    {
                        "dataset": self.dataset_urns.get('final', 'urn:li:dataset:(urn:li:dataPlatform:snowflake,final_customer_data,PROD)'),
                        "columns": ["customer_id", "customer_name", "email_encrypted", "phone_masked", "customer_segment", "data_quality_score"]
                    }
                ]
            }
        else:  # final
            # Final dataset lineage
            lineage_payload = {
                "upstreams": [
                    {
                        "dataset": self.dataset_urns.get('staging', 'urn:li:dataset:(urn:li:dataPlatform:snowflake,staging_customer_data,PROD)'),
                        "columns": ["customer_id", "full_name", "email_hash", "phone_clean", "registration_year"]
                    }
                ],
                "downstreams": [
                    {
                        "dataset": "urn:li:dataset:(urn:li:dataPlatform:bi,customer_analytics,PROD)",
                        "columns": ["customer_id", "customer_name", "customer_segment", "data_quality_score"]
                    }
                ]
            }
        
        self.writer.upsert_columnlineage_aspect("Dataset", dataset_urn, lineage_payload)
    
    def create_column_lineage_relationships(self):
        """Create DERIVES_FROM relationships between columns using registry-driven approach"""
        print("\n=== Creating Column-Level Lineage Relationships ===")
        
        # Raw to Staging lineage
        if 'raw' in self.column_urns and 'staging' in self.column_urns:
            print("Creating Raw -> Staging column lineage...")
            
            # Get transformations from staging data
            staging_data = self.load_json_data(os.path.join("data", "staging_customer_data.json"))
            transformations = staging_data.get('transformations', {})
            
            # Get transformation statistics
            stats = self.factory.get_transformation_statistics(transformations)
            print(f"Staging transformations: {stats['total_transformations']} total, types: {stats['transformation_types']}")
            
            # Use registry-driven lineage creation
            self.factory.create_column_lineage_relationships(
                self.writer,
                transformations,
                self.column_urns['raw'],
                self.column_urns['staging'],
                self.dataset_urns['raw'],
                self.dataset_urns['staging']
            )
        
        # Staging to Final lineage
        if 'staging' in self.column_urns and 'final' in self.column_urns:
            print("Creating Staging -> Final column lineage...")
            
            # Get transformations from final data
            final_data = self.load_json_data(os.path.join("data", "final_customer_data.json"))
            transformations = final_data.get('transformations', {})
            
            # Get transformation statistics
            stats = self.factory.get_transformation_statistics(transformations)
            print(f"Final transformations: {stats['total_transformations']} total, types: {stats['transformation_types']}")
            
            # Use registry-driven lineage creation
            self.factory.create_column_lineage_relationships(
                self.writer,
                transformations,
                self.column_urns['staging'],
                self.column_urns['final'],
                self.dataset_urns['staging'],
                self.dataset_urns['final']
            )
    
    def create_dataset_lineage_relationships(self):
        """Create dataset-level lineage relationships using registry-driven approach"""
        print("\n=== Creating Dataset-Level Lineage Relationships ===")
        
        # Raw -> Staging
        if 'raw' in self.dataset_urns and 'staging' in self.dataset_urns:
            self.factory.create_dataset_lineage_relationship(
                self.writer,
                self.dataset_urns['raw'],
                self.dataset_urns['staging'],
                via_job="etl_job_1"
            )
        
        # Staging -> Final
        if 'staging' in self.dataset_urns and 'final' in self.dataset_urns:
            self.factory.create_dataset_lineage_relationship(
                self.writer,
                self.dataset_urns['staging'],
                self.dataset_urns['final'],
                via_job="etl_job_2"
            )
    
    def create_tags_and_relationships(self):
        """Create tags and tag relationships"""
        print("\n=== Creating Tags and Relationships ===")
        
        # Create tags
        tag1_urn = self.writer.upsert_tag(key="PII", value="true")
        tag2_urn = self.writer.upsert_tag(key="SENSITIVE", value="")
        
        # Create tagged relationships for all datasets
        for stage, dataset_urn in self.dataset_urns.items():
            self.writer.create_tagged_relationship(dataset_urn, tag1_urn, {"source": "ingestion"})
            self.writer.create_tagged_relationship(dataset_urn, tag2_urn, {"source": "ingestion"})
            print(f"Tagged {stage} dataset with PII and SENSITIVE tags")
    
    def run_ingestion(self, data_dir: str):
        """Run the complete ingestion pipeline"""
        print("🚀 Starting Data Ingestion Pipeline")
        print("=" * 60)
        
        try:
            # Load data from JSON files
            raw_data = self.load_json_data(os.path.join(data_dir, "raw_customer_data.json"))
            staging_data = self.load_json_data(os.path.join(data_dir, "staging_customer_data.json"))
            final_data = self.load_json_data(os.path.join(data_dir, "final_customer_data.json"))
            
            # Ingest datasets in order
            self.ingest_raw_dataset(raw_data)
            self.ingest_staging_dataset(staging_data)
            self.ingest_final_dataset(final_data)
            
            # Create lineage relationships
            self.create_column_lineage_relationships()
            self.create_dataset_lineage_relationships()
            
            # Create tags and relationships
            self.create_tags_and_relationships()
            
            print("\n" + "=" * 60)
            print("✅ Data Ingestion Pipeline Completed Successfully!")
            print("=" * 60)
            
            # Print summary
            print(f"\n📊 Ingestion Summary:")
            print(f"  • Raw Dataset: {self.dataset_urns.get('raw', 'N/A')}")
            print(f"  • Staging Dataset: {self.dataset_urns.get('staging', 'N/A')}")
            print(f"  • Final Dataset: {self.dataset_urns.get('final', 'N/A')}")
            print(f"  • Total Columns Created: {sum(len(cols) for cols in self.column_urns.values())}")
            print(f"  • Column Lineage Relationships: {len(self.column_urns.get('staging', {})) + len(self.column_urns.get('final', {}))}")
            
        except Exception as e:
            print(f"❌ Error during ingestion: {str(e)}")
            raise
        finally:
            # Close the writer
            self.writer.close()


def main():
    """Main function to run the ingestion pipeline"""
    
    # Configuration
    registry_path = "enhanced_registry.yaml"
    data_dir = "data"
    
    # Neo4j connection settings
    neo4j_uri = "bolt://localhost:7687"
    neo4j_user = "neo4j"
    neo4j_password = "password"
    
    # Validate files exist
    required_files = [
        "raw_customer_data.json",
        "staging_customer_data.json", 
        "final_customer_data.json"
    ]
    
    for file_name in required_files:
        file_path = os.path.join(data_dir, file_name)
        if not os.path.exists(file_path):
            print(f"❌ Error: Required file not found: {file_path}")
            sys.exit(1)
    
    if not os.path.exists(registry_path):
        print(f"❌ Error: Registry file not found: {registry_path}")
        sys.exit(1)
    
    # Create and run ingestion pipeline
    pipeline = DataIngestionPipeline(registry_path, neo4j_uri, neo4j_user, neo4j_password)
    pipeline.run_ingestion(data_dir)


if __name__ == "__main__":
    main()
