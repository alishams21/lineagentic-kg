
<div align="center">
  <img src="images/logo2.jpg" alt="Lineagentic-catalog" width="880" height="300">
</div>

## Lineagentic-Catalog

Lineagentic-catalog is an graph based data catalog with generic model which can evolve without schema migrations support organisations demand for customization.

### Some of the important features of Lineagentic-Catalog are:
- **Native Relationships**: Neo4j's native graph structure provides better performance for relationship traversal.
- **Complex Queries**: Easier to write complex lineage queries
- **JSON Storage**: Simple JSON-based aspect storage
- **Flexibility**: Easier to evolve without schema migrations
- **Column-Level**: Advanced column-level lineage
- **Transformation Recipes**: Stores actual transformation logic
- **Bidirectional**: Easy to traverse both upstream and downstream
- **Real-time aspect validation**: Real-time aspect validation
- **Advanced versioning mechanism**: Advanced versioning mechanism for versioned aspects
- **Entity registry validation**: Entity registry validation

## Graph Model

The graph model is designed to be generic and can be extended supporting following concepts:

### 1- Entities

Entities are primary nodes in the metadata graph (e.g., Dataset, CorpUser, DataJob). Entity properties stored as node attributes. Example of entity creation:

```python
# Entity creation with URNs
writer.upsert_entity("DataJob", job_urn, {
    "name": j_name, "namespace": j_ns, "versionId": j_ver,
    "integration": integration, "processingType": processing_type, "jobType": job_type_name
})
```

### 2- Aspects

Aspects are collections of attributes describing facets of entities. Smallest atomic unit of write.

```python
# Versioned aspect creation
writer.upsert_versioned_aspect("DataJob", job_urn, "dataJobInfo", {
    "name": j_name, "namespace": j_ns, "versionId": j_ver,
    "integration": integration, "processingType": processing_type, "jobType": job_type_name
})
```

Aspect are changed over time and can be versioned.Versioned aspects are used to store static attributes of an entity. TimeSeries aspects are used to store dynamic and operational metrics of an entity. there are two types of aspects:

Versioned aspects (Schema-based):
```python
writer.upsert_versioned_aspect("DataJob", job_urn, "dataJobInfo", {
    "name": j_name, "namespace": j_ns, "versionId": j_ver,
    "integration": integration, "processingType": processing_type, "jobType": job_type_name
})
```
TimeSeries aspects (Event-based):
```python
writer.append_timeseries_aspect("DataJob", job_urn, "dataJobRun", {
    "eventType": event_type,
    "runId": run_id,
    "parent": parent_info
}, timestamp_ms=ts_ms)
```

Aspect Registry Validation:
```python
def _validate_aspect(self, entity_label: str, aspect_name: str, kind: str):
    ents = self.registry.get("entities", {})
    ent = ents.get(entity_label, {})
    aspects = ent.get("aspects", {})
    allowed = aspects.get(aspect_name)
    if allowed != kind:
        raise ValueError(f"Aspect '{aspect_name}' not allowed as '{kind}' on entity '{entity_label}'")
```

Lineagentic-catalog supports advanced versioning mechanism for versioned aspects. This is a key to track changes to entities and aspects over time supporting following features:

**Key Features**:
- **Automatic Version Increment**: New versions are auto-incremented
- **Latest Flag**: Only one version marked as `latest: true`
- **Version History**: All versions preserved with timestamps
- **Atomic Updates**: Version changes are atomic operations

```python
def upsert_versioned_aspect(self, entity_label: str, entity_urn: str,
                            aspect_name: str, payload: Dict[str, Any], version: int|None=None) -> int:
    # 1. Validate aspect is allowed for entity
    self._validate_aspect(entity_label, aspect_name, "versioned")
    
    # 2. Get current max version
    current_max = self._max_version(entity_label, entity_urn, aspect_name)
    new_version = current_max + 1 if version is None else version
    
    # 3. Mark previous version as not latest
    s.run("""
        MATCH (e:{entity_label} {{urn:$urn}})-[r:HAS_ASPECT {{name:$an, kind:'versioned', latest:true}}]->(:Aspect)
        SET r.latest=false
    """)
    
    # 4. Create new versioned aspect
    s.run("""
        MATCH (e:{entity_label} {{urn:$urn}})
        CREATE (a:Aspect:Versioned {{id:$id, name:$an, version:$ver, kind:'versioned', json:$json, createdAt:$now}})
        CREATE (e)-[:HAS_ASPECT {{name:$an, version:$ver, latest:true, kind:'versioned'}}]->(a)
    """)
```

### 3- Relationships

Relationships are edges between entities. Relationships are used to describe the relationships between entities and aspects.

```python
# Entity relationships
writer.create_relationship("DataFlow", flow_urn, "HAS_JOB", "DataJob", job_urn, {})
writer.create_relationship("DataJob", job_urn, "CONSUMES", "Dataset", in_urn, {})
writer.create_relationship("DataJob", job_urn, "PRODUCES", "Dataset", out_urn, {})

# Column lineage relationships
writer.create_relationship("Column", out_col_urn, "DERIVES_FROM", "Column", in_col_urn, {
    "type": t.get("type"), "subtype": t.get("subtype"),
    "description": t.get("description"), "masking": bool(t.get("masking"))
})