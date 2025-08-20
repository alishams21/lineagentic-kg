# Simple usage - same API as before
from registry import RegistryFactory

def main():
    """Demonstrate the fully generic, YAML-driven RegistryFactory"""
    print("🚀 Fully Generic RegistryFactory Demo")
    print("=" * 60)
    
    # Configuration
    registry_path = "config/main_registry.yaml"
    neo4j_uri = "bolt://localhost:7687"
    neo4j_user = "neo4j"
    neo4j_password = "password"
    
    try:
        # Create registry factory
        print("1. Creating RegistryFactory...")
        factory = RegistryFactory(registry_path)
        print(f"   ✅ Registry loaded from: {registry_path}")
        print(f"   📊 Entities: {list(factory.registry['entities'].keys())}")
        print(f"   📊 Aspects: {list(factory.registry['aspects'].keys())}")
        print(f"   📊 Aspect Relationships: {list(factory.registry.get('aspect_relationships', {}).keys())}")
        
        # Create writer instance
        print("\n2. Creating Neo4j writer...")
        writer = factory.create_writer(neo4j_uri, neo4j_user, neo4j_password)
        print("   ✅ Writer created successfully")
        
        # ============================================================================
        # GENERIC METHOD GENERATION SUMMARY
        # ============================================================================
        print("\n" + "="*60)
        print("GENERIC METHOD GENERATION")
        print("="*60)
        
        # Count generated methods
        generated_methods = [method for method in dir(writer) if not method.startswith('_') and callable(getattr(writer, method))]
        entity_methods = [m for m in generated_methods if m.startswith(('upsert_', 'get_', 'delete_')) and not m.endswith('_aspect')]
        aspect_methods = [m for m in generated_methods if m.endswith('_aspect')]
        discovery_methods = [m for m in generated_methods if m.startswith('discover_')]
        utility_methods = [m for m in generated_methods if m not in entity_methods + aspect_methods + discovery_methods]
        
        print(f"\n📊 Generated Methods Summary:")
        print(f"   🏗️ Entity Methods: {len(entity_methods)} (CRUD for all entities)")
        print(f"   📊 Aspect Methods: {len(aspect_methods)} (CRUD for all aspects)")
        print(f"   🔍 Discovery Methods: {len(discovery_methods)} (Automatic relationship building)")
        print(f"   🛠️ Utility Methods: {len(utility_methods)} (Helper functions)")
        print(f"   📈 Total: {len(generated_methods)} methods (100% from YAML)")
        
        print(f"\n🎯 Registry-Driven Features:")
        print(f"   • Independent Ingestion: {sum(1 for a in factory.registry['aspects'].values() if a.get('entity_creation'))} aspects")
        print(f"   • YAML Relationship Rules: {len(factory.registry.get('aspect_relationships', {}))} aspect types")
        print(f"   • URN Patterns: {len(factory.registry['urn_patterns'])} generators")
        
        print(f"\n✨ Key Method Examples:")
        print(f"   🔧 writer.upsert_dataset(platform, name, env)  # Entity creation")
        print(f"   🔧 writer.upsert_ownership_aspect(payload, platform, name, env)  # Independent ingestion")
        print(f"   🔧 writer.discover_relationships_from_aspect(entity_urn, entity_type, aspect_name, aspect_data)  # Aspect-driven")
        
        # ============================================================================
        # PRACTICAL DEMONSTRATION
        # ============================================================================
        print("\n" + "="*60)
        print("PRACTICAL DEMONSTRATION")
        print("="*60)
        
        print("\n3. Creating entities and aspects with automatic relationship discovery...")
        
        # Create entities
        dataset_urn = writer.upsert_dataset(platform="postgresql", name="customer_data", env="PROD")
        user_urn = writer.upsert_corpuser(username="data.engineer@company.com")
        tag_urn = writer.upsert_tag(key="SENSITIVE", value="true")
        print(f"   ✅ Created: Dataset, CorpUser, Tag")
        
        # Add aspects with automatic relationship creation
        ownership_payload = {
            "owners": [
                {"owner": "data.engineer@company.com", "type": "DATAOWNER", "source": "MANUAL"},
                {"owner": "analytics_team", "type": "DELEGATE", "source": "MANUAL"}
            ]
        }
        writer.upsert_ownership_aspect("Dataset", dataset_urn, ownership_payload)
        
        tags_payload = {"tags": [{"tag": "SENSITIVE"}, {"tag": "BUSINESS_CRITICAL"}]}
        writer.upsert_globaltags_aspect("Dataset", dataset_urn, tags_payload)
        
        schema_payload = {
            "schemaName": "customer_data_schema",
            "platform": "postgresql",
            "fields": [
                {"fieldPath": "customer_id", "type": "UUID"},
                {"fieldPath": "email", "type": "VARCHAR(255)"},
                {"fieldPath": "name", "type": "VARCHAR(100)"}
            ]
        }
        writer.upsert_schemametadata_aspect("Dataset", dataset_urn, schema_payload)
        print(f"   ✅ Added: ownership, globalTags, schemaMetadata aspects")
        
        # Demonstrate independent ingestion
        print("\n4. Independent aspect ingestion demonstration...")
        
        # Create dataset properties without pre-existing entity URN
        props_payload = {
            "description": "Customer data for analytics",
            "customProperties": {"retention_days": 365}
        }
        writer.upsert_datasetproperties_aspect(
            payload=props_payload,
            platform="postgresql",
            name="customer_data",
            env="PROD"
        )
        
        # Create user info without pre-existing entity URN
        user_info_payload = {
            "displayName": "Data Engineer",
            "email": "data.engineer@company.com",
            "title": "Senior Data Engineer"
        }
        writer.upsert_corpuserinfo_aspect(
            payload=user_info_payload,
            username="data.engineer@company.com"
        )
        print(f"   ✅ Independent ingestion: datasetProperties, corpUserInfo")
        
        # Demonstrate data lineage
        print("\n5. Data lineage demonstration...")
        
        # Create derived dataset
        derived_urn = writer.upsert_dataset(platform="postgresql", name="customer_analytics", env="PROD")
        
        # Add transformation aspect to show lineage
        transformation_payload = {
            "inputColumns": ["customer_id", "email"],
            "transformationType": "AGGREGATE",
            "sourceDataset": dataset_urn,
            "targetDataset": derived_urn
        }
        writer.upsert_transformation_aspect(
            "Column", 
            f"{derived_urn}#customer_count",
            transformation_payload
        )
        print(f"   ✅ Data lineage: transformation aspect with DERIVES_FROM relationships")
        
        # ============================================================================
        # VERIFICATION
        # ============================================================================
        print("\n" + "="*60)
        print("VERIFICATION")
        print("="*60)
        
        print("\n6. Verifying automatic relationship creation...")
        
        # Check that relationships were created automatically
        with writer._driver.session() as s:
            # Check ownership relationships
            ownership_count = s.run(
                "MATCH ()-[r:OWNS]->() RETURN count(r) as count"
            ).single()['count']
            
            # Check tag relationships  
            tag_count = s.run(
                "MATCH ()-[r:TAGGED]->() RETURN count(r) as count"
            ).single()['count']
            
            # Check column relationships
            column_count = s.run(
                "MATCH ()-[r:HAS_COLUMN]->() RETURN count(r) as count"
            ).single()['count']
            
            # Check lineage relationships
            lineage_count = s.run(
                "MATCH ()-[r:DERIVES_FROM]->() RETURN count(r) as count"
            ).single()['count']
        
        print(f"   🔗 OWNS relationships: {ownership_count}")
        print(f"   🔗 TAGGED relationships: {tag_count}")
        print(f"   🔗 HAS_COLUMN relationships: {column_count}")
        print(f"   🔗 DERIVES_FROM relationships: {lineage_count}")
        
        print("\n7. Retrieving data to verify ingestion...")
        
        # Retrieve some data
        dataset_data = writer.get_dataset(dataset_urn)
        ownership_data = writer.get_ownership_aspect("Dataset", dataset_urn)
        schema_data = writer.get_schemametadata_aspect("Dataset", dataset_urn)
        
        print(f"   📊 Dataset: {dataset_data['name']} ({dataset_data['platform']})")
        print(f"   📊 Ownership: {len(ownership_data['payload']['owners'])} owners")
        print(f"   📊 Schema: {len(schema_data['payload']['fields'])} columns")
        
        # ============================================================================
        # SUMMARY
        # ============================================================================
        print("\n" + "="*60)
        print("SUMMARY")
        print("="*60)
        
        print("\n🎉 RegistryFactory is now fully generic and YAML-driven!")
        print("\n✅ Key Achievements:")
        print("   • All methods generated from enhanced_registry.yaml")
        print("   • No hardcoded aspect names or relationship logic")
        print("   • Aspect-driven relationships defined in YAML rules")
        print("   • Independent ingestion for all aspects")
        print("   • Automatic relationship discovery and creation")
        print("   • Data lineage tracking with transformation aspects")
        
        print(f"\n📊 System Statistics:")
        print(f"   • {len(entity_methods)} entity methods generated")
        print(f"   • {len(aspect_methods)} aspect methods generated")
        print(f"   • {len(discovery_methods)} discovery methods generated")
        print(f"   • {len(utility_methods)} utility methods generated")
        print(f"   • {len(generated_methods)} total methods (100% dynamic)")
        
        print(f"\n🔧 Usage Pattern:")
        print(f"   • Define entities, aspects, and relationships in YAML")
        print(f"   • RegistryFactory generates all methods automatically")
        print(f"   • Ingest aspects independently (entities created automatically)")
        print(f"   • Relationships built automatically from aspect data")
        print(f"   • No manual relationship creation needed")
        
        print(f"\n🚀 The system is now completely declarative and configurable!")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        print("Make sure Neo4j is running and accessible at the specified URI")
        import traceback
        traceback.print_exc()
    
    finally:
        # Clean up
        if 'writer' in locals():
            writer.close()
            print("\n🔌 Writer connection closed")


if __name__ == "__main__":
    main()
