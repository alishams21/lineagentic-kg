#!/usr/bin/env python3
"""
Simple runner script for automated ingestion.

Usage:
    python run_ingestion.py [--registry REGISTRY_PATH] [--records RECORDS_FILE] [--neo4j-uri URI] [--user USER] [--password PASSWORD]
"""

import argparse
import sys
import os
from ingestion import AutomatedIngestion

def main():
    parser = argparse.ArgumentParser(description='Run automated data ingestion')
    parser.add_argument('--registry', default='enhanced_registry.yaml', 
                       help='Path to enhanced registry YAML file')
    parser.add_argument('--records', default='example_records.json', 
                       help='Path to JSON records file')
    parser.add_argument('--neo4j-uri', default='bolt://localhost:7687', 
                       help='Neo4j connection URI')
    parser.add_argument('--user', default='neo4j', 
                       help='Neo4j username')
    parser.add_argument('--password', default='password', 
                       help='Neo4j password')
    parser.add_argument('--verbose', '-v', action='store_true', 
                       help='Enable verbose logging')
    
    args = parser.parse_args()
    
    # Check if files exist
    if not os.path.exists(args.registry):
        print(f"❌ Registry file not found: {args.registry}")
        sys.exit(1)
    
    if not os.path.exists(args.records):
        print(f"❌ Records file not found: {args.records}")
        sys.exit(1)
    
    print("🚀 Starting Automated Data Ingestion")
    print("=" * 50)
    print(f"Registry: {args.registry}")
    print(f"Records: {args.records}")
    print(f"Neo4j URI: {args.neo4j_uri}")
    print(f"User: {args.user}")
    print("=" * 50)
    
    # Create ingestion instance
    ingestion = AutomatedIngestion(
        registry_path=args.registry,
        neo4j_uri=args.neo4j_uri,
        neo4j_user=args.user,
        neo4j_password=args.password
    )
    
    try:
        # Initialize
        ingestion.initialize()
        
        # Load records
        records_data = ingestion.load_records(args.records)
        
        # Ingest all records
        ingestion.ingest_all_records(records_data)
        
        # Print statistics
        ingestion.print_stats()
        
        print("🎉 Automated ingestion completed successfully!")
        
    except Exception as e:
        print(f"❌ Ingestion failed: {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)
        
    finally:
        # Clean up
        ingestion.cleanup()

if __name__ == "__main__":
    main()
