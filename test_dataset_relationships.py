#!/usr/bin/env python3

from lineagentic_kg.registry.factory import RegistryFactory

# Initialize the registry factory
registry_factory = RegistryFactory("lineagentic_kg/config/main_registry.yaml")

# Create a Neo4j writer instance
neo4j_writer = registry_factory.create_writer(
    uri="bolt://localhost:7687",
    user="neo4j",
    password="password"
)

print("🔍 Testing Dataset relationship creation")

# Create Dataset
print("\n1️⃣ Creating Dataset: customer_raw_data")
dataset_urn = neo4j_writer.upsert_dataset(
    platform="snowflake",
    name="customer_raw_data",
    env="PROD"
)
print(f"✅ Created Dataset: {dataset_urn}")

# Create Tag
print("\n2️⃣ Creating Tag: PII")
tag_urn = neo4j_writer.upsert_tag(key="PII", value="")
print(f"✅ Created Tag: {tag_urn}")

# Create CorpUser
print("\n3️⃣ Creating CorpUser: data_engineering_team")
user_urn = neo4j_writer.upsert_corpuser(username="data_engineering_team")
print(f"✅ Created CorpUser: {user_urn}")

# Create CorpGroup
print("\n4️⃣ Creating CorpGroup: data_engineering")
group_urn = neo4j_writer.upsert_corpgroup(name="data_engineering")
print(f"✅ Created CorpGroup: {group_urn}")

# Add globalTags aspect
print("\n5️⃣ Adding globalTags aspect")
result = neo4j_writer.upsert_globaltags_aspect(
    entity_label="Dataset",
    entity_urn=dataset_urn,
    tags=[
        {"tag": "urn:li:tag:PII", "context": "data_classification"}
    ]
)
print(f"✅ Added globalTags aspect: {result}")

# Add ownership aspect
print("\n6️⃣ Adding ownership aspect")
result = neo4j_writer.upsert_ownership_aspect(
    entity_label="Dataset",
    entity_urn=dataset_urn,
    owners=[
        {"owner": "urn:li:corpuser:data_engineering_team", "type": "TECHNICAL_OWNER"}
    ],
    groupOwners=[
        {"group": "urn:li:corpGroup:data_engineering", "type": "TECHNICAL_GROUP"}
    ],
    lastModified="2024-01-15T10:00:00Z"
)
print(f"✅ Added ownership aspect: {result}")

# Check if relationships were created
print("\n7️⃣ Checking for relationships")
with neo4j_writer._driver.session() as s:
    # Check TAGGED relationships
    tagged_result = s.run("""
        MATCH (d:Dataset)-[r:TAGGED]->(t:Tag)
        RETURN d.urn as dataset_urn, t.urn as tag_urn, type(r) as relationship_type
    """)
    
    tagged_relationships = list(tagged_result)
    if tagged_relationships:
        print("✅ Found TAGGED relationships:")
        for rel in tagged_relationships:
            print(f"   {rel['dataset_urn']} -> {rel['relationship_type']} -> {rel['tag_urn']}")
    else:
        print("❌ No TAGGED relationships found")
    
    # Check OWNS relationships
    owns_result = s.run("""
        MATCH (u:CorpUser)-[r:OWNS]->(d:Dataset)
        RETURN u.urn as user_urn, d.urn as dataset_urn, type(r) as relationship_type
    """)
    
    owns_relationships = list(owns_result)
    if owns_relationships:
        print("✅ Found OWNS relationships:")
        for rel in owns_relationships:
            print(f"   {rel['user_urn']} -> {rel['relationship_type']} -> {rel['dataset_urn']}")
    else:
        print("❌ No OWNS relationships found")
    
    # Check GROUP_OWNS relationships
    group_owns_result = s.run("""
        MATCH (g:CorpGroup)-[r:GROUP_OWNS]->(d:Dataset)
        RETURN g.urn as group_urn, d.urn as dataset_urn, type(r) as relationship_type
    """)
    
    group_owns_relationships = list(group_owns_result)
    if group_owns_relationships:
        print("✅ Found GROUP_OWNS relationships:")
        for rel in group_owns_relationships:
            print(f"   {rel['group_urn']} -> {rel['relationship_type']} -> {rel['dataset_urn']}")
    else:
        print("❌ No GROUP_OWNS relationships found")
    
    # Debug: Check what aspects exist
    print("\n🔍 Debug: Checking aspects on Dataset")
    aspects_result = s.run("""
        MATCH (d:Dataset)-[r:HAS_ASPECT]->(a:Aspect)
        RETURN d.urn as dataset_urn, r.name as aspect_name, a.json as aspect_data
    """)
    
    aspects = list(aspects_result)
    if aspects:
        print("✅ Found aspects on Dataset:")
        for aspect in aspects:
            print(f"   {aspect['dataset_urn']} -> {aspect['aspect_name']}")
            print(f"   Data: {aspect['aspect_data'][:100]}...")
    else:
        print("❌ No aspects found on Dataset")

neo4j_writer.close()
print("\n✅ Test completed")
