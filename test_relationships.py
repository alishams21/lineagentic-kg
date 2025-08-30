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

print("🔍 Testing relationship creation between CorpUser and CorpGroup")

# Create CorpUser
print("\n1️⃣ Creating CorpUser: data_engineering_team")
user_urn = neo4j_writer.upsert_corpuser(username="data_engineering_team")
print(f"✅ Created CorpUser: {user_urn}")

# Create CorpGroup
print("\n2️⃣ Creating CorpGroup: data_engineering")
group_urn = neo4j_writer.upsert_corpgroup(name="data_engineering")
print(f"✅ Created CorpGroup: {group_urn}")

# Add corpGroupInfo aspect with members
print("\n3️⃣ Adding corpGroupInfo aspect with members")
result = neo4j_writer.upsert_corpgroupinfo_aspect(
    entity_label="CorpGroup",
    entity_urn=group_urn,
    displayName="Data Engineering Team",
    description="Team responsible for data infrastructure and ETL pipelines",
    email="data-engineering@company.com",
    members=["data_engineering_team"]
)
print(f"✅ Added corpGroupInfo aspect: {result}")

# Check if relationships were created
print("\n4️⃣ Checking for MEMBER_OF relationships")
with neo4j_writer._driver.session() as s:
    result = s.run("""
        MATCH (u:CorpUser)-[r:MEMBER_OF]->(g:CorpGroup)
        RETURN u.urn as user_urn, g.urn as group_urn, type(r) as relationship_type
    """)
    
    relationships = list(result)
    if relationships:
        print("✅ Found MEMBER_OF relationships:")
        for rel in relationships:
            print(f"   {rel['user_urn']} -> {rel['relationship_type']} -> {rel['group_urn']}")
    else:
        print("❌ No MEMBER_OF relationships found")
        
        # Debug: Check what entities exist
        print("\n🔍 Debug: Checking existing entities")
        users = s.run("MATCH (u:CorpUser) RETURN u.urn as urn, u.username as username")
        groups = s.run("MATCH (g:CorpGroup) RETURN g.urn as urn, g.name as name")
        
        print("CorpUsers:")
        for user in users:
            print(f"   {user['urn']} (username: {user['username']})")
            
        print("CorpGroups:")
        for group in groups:
            print(f"   {group['urn']} (name: {group['name']})")

neo4j_writer.close()
print("\n✅ Test completed")
