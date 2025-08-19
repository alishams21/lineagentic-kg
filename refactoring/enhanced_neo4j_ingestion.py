#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
from typing import Any, Dict

import yaml
from registry_factory import RegistryFactory
from ..models.models import EventIngestionRequest


def to_ms(iso: str) -> int:
    return int(dt.datetime.fromisoformat(iso.replace("Z","+00:00")).timestamp() * 1000)


def ingest_openlineage_event_enhanced(agent_result: Dict[str, Any], 
                                    event_ingestion_request: EventIngestionRequest, 
                                    writer: Any) -> None:
    """
    Enhanced ingestion function using registry-based writer
    """
    event_type = event_ingestion_request.event_type
    event_time = event_ingestion_request.event_time
    ts_ms = to_ms(event_time) if event_time else writer.utc_now_ms()

    run = event_ingestion_request.run
    run_id = run.run_id if hasattr(run, 'run_id') else f"run-{ts_ms}"
    parent = run.facets.parent.job if hasattr(run, 'facets') and hasattr(run.facets, 'parent') and hasattr(run.facets.parent, 'job') else None

    job = event_ingestion_request.job
    j_ns = job.namespace if hasattr(job, 'namespace') else "default_ns"
    j_name = job.name if hasattr(job, 'name') else "job"
    j_ver = job.version_id if hasattr(job, 'version_id') else "v0"
    j_facets = job.facets if hasattr(job, 'facets') else None

    job_type = j_facets.job_type if hasattr(j_facets, 'job_type') else None
    integration = job_type.integration if job_type and hasattr(job_type, 'integration') else "unknown"
    processing_type = job_type.processing_type if job_type and hasattr(job_type, 'processing_type') else None
    job_type_name = job_type.job_type if job_type and hasattr(job_type, 'job_type') else None

    flow_id = f"{j_ns}.{j_name}"
    
    # Use registry-generated URN functions
    flow_urn = writer.urn_generators['dataFlow'](platform=integration, flow_id=flow_id, env="PROD")
    job_urn = writer.urn_generators['dataJob'](flow_urn=flow_urn, job_name=j_name)

    # Use registry-generated entity methods
    writer.upsert_dataflow(platform=integration, flow_id=flow_id, namespace=j_ns, name=j_name, env="PROD")
    writer.upsert_datajob(name=j_name, namespace=j_ns, versionId=j_ver, 
                         integration=integration, processingType=processing_type, jobType=job_type_name)

    # Use registry-generated relationship methods
    writer.create_has_job_relationship(flow_urn, job_urn, {})

    # Handle aspects
    writer.upsert_versioned_aspect("DataJob", job_urn, "dataJobInfo", {
        "name": j_name, "namespace": j_ns, "versionId": j_ver,
        "integration": integration, "processingType": processing_type, "jobType": job_type_name
    })

    # Handle documentation
    if hasattr(j_facets, 'documentation') and j_facets.documentation:
        doc = j_facets.documentation
        writer.upsert_versioned_aspect("DataJob", job_urn, "documentation", {
            "description": doc.description if hasattr(doc, 'description') else None,
            "contentType": doc.content_type if hasattr(doc, 'content_type') else None
        })

    # Handle source code location
    if hasattr(j_facets, 'source_code_location') and j_facets.source_code_location:
        writer.upsert_versioned_aspect("DataJob", job_urn, "sourceCodeLocation", j_facets.source_code_location.__dict__)

    # Handle source code
    if hasattr(j_facets, 'source_code') and j_facets.source_code:
        sc = j_facets.source_code
        code = sc.source_code if hasattr(sc, 'source_code') else ""
        writer.upsert_versioned_aspect("DataJob", job_urn, "sourceCode", {
            "language": sc.language if hasattr(sc, 'language') else None,
            "snippet": code if len(code) < 4000 else code[:4000] + "..."
        })

    # Handle environment variables
    if hasattr(j_facets, 'environment_variables') and j_facets.environment_variables:
        envs = j_facets.environment_variables
        safe_envs = [{"name": e.name if hasattr(e, 'name') else "", 
                     "value": writer.mask_secret(e.name if hasattr(e, 'name') else "", 
                                               e.value if hasattr(e, 'value') else "")} for e in envs]
        writer.upsert_versioned_aspect("DataJob", job_urn, "environmentProperties", {"env": safe_envs})

    # Handle ownership
    if hasattr(j_facets, 'ownership') and j_facets.ownership:
        owners = j_facets.ownership.owners if hasattr(j_facets.ownership, 'owners') else []
        writer.upsert_versioned_aspect("DataJob", job_urn, "ownership", {"owners": [o.__dict__ for o in owners]})
        
        for o in owners:
            t = (o.type if hasattr(o, 'type') else "").upper()
            name = o.name if hasattr(o, 'name') else ""
            if t == "INDIVIDUAL":
                user_urn = writer.urn_generators['corpUser'](username=name)
                writer.upsert_corpuser(username=name)
                writer.create_owns_relationship(user_urn, job_urn, {"via": "aspect"})
            elif t == "TEAM":
                group_urn = writer.urn_generators['corpGroup'](groupname=name)
                writer.upsert_corpgroup(name=name)
                writer.create_owns_relationship(group_urn, job_urn, {"via": "aspect"})

    # Handle job run
    writer.append_timeseries_aspect("DataJob", job_urn, "dataJobRun", {
        "eventType": event_type,
        "runId": run_id,
        "parent": {"namespace": parent.namespace if parent and hasattr(parent, 'namespace') else None, 
                  "name": parent.name if parent and hasattr(parent, 'name') else None} if parent else None
    }, timestamp_ms=ts_ms)

    # Process inputs
    inputs = agent_result.get("inputs", []) or []
    input_dataset_urns = []

    for ds in inputs:
        ns = ds.get("namespace") or "unknown_platform"
        name = ds.get("name") or "unknown_name"
        version = event_ingestion_request.job.facets.schema.version_id if hasattr(event_ingestion_request.job.facets, 'schema') and hasattr(event_ingestion_request.job.facets.schema, 'version_id') else "v0"
        
        ds_urn = writer.urn_generators['dataset'](platform=ns, name=name, env="PROD")
        writer.upsert_dataset(platform=ns, name=name, env="PROD", versionId=version)

        facets = ds.get("facets", {}) or {}

        # Handle schema
        if "schema" in facets:
            fields = facets["schema"].get("fields", [])
            schema_payload = {
                "schemaName": f"{ns}.{name}",
                "platform": writer.urn_generators['dataPlatform'](platform=ns),
                "version": 0,
                "fields": [{
                    "fieldPath": f.get("name"),
                    "type": {"type": f.get("type")},
                    "description": f.get("description"),
                    "nullable": True,
                    "versionId": f.get("versionId")
                } for f in fields]
            }
            writer.upsert_versioned_aspect("Dataset", ds_urn, "schemaMetadata", schema_payload)

        # Handle tags
        if ds.get("facets", {}).get("tags"):
            for t in ds.get("facets", {}).get("tags", []):
                k, v, src = t.get("key"), t.get("value"), t.get("source")
                t_urn = writer.urn_generators['tag'](key=k, value=v)
                writer.upsert_entity("Tag", t_urn, {"key": k, "value": v})
                writer.create_relationship("Dataset", ds_urn, "TAGGED", "Tag", t_urn, {"source": src})

        # Handle ownership
        if ds.get("facets", {}).get("ownership"):
            owners = ds.get("facets", {}).get("ownership", {}).get("owners", [])
            writer.upsert_versioned_aspect("Dataset", ds_urn, "ownership", {"owners": owners})
            for o in owners:
                t = (o.get("type") or "").upper()
                name = o.get("name") or ""
                if t == "INDIVIDUAL":
                    user_urn = writer.urn_generators['corpUser'](username=name)
                    writer.upsert_corpuser(username=name)
                    writer.create_owns_relationship(user_urn, ds_urn, {"via": "aspect"})
                elif t == "TEAM":
                    group_urn = writer.urn_generators['corpGroup'](groupname=name)
                    writer.upsert_corpgroup(name=name)
                    writer.create_owns_relationship(group_urn, ds_urn, {"via": "aspect"})

        # Handle input statistics
        if ds.get("facets", {}).get("inputStatistics"):
            stats = ds.get("facets", {}).get("inputStatistics", {})
            writer.append_timeseries_aspect("Dataset", ds_urn, "datasetProfile", {
                "rowCount": stats.get("rowCount"),
                "fileCount": stats.get("fileCount"),
                "size": stats.get("size"),
                "kind": "input"
            }, timestamp_ms=ts_ms)

        input_dataset_urns.append(ds_urn)

    # Process outputs (similar to inputs but with column lineage)
    outputs = agent_result.get("outputs", []) or []
    output_dataset_urns = []

    for ds in outputs:
        ns = ds.get("namespace") or "unknown_platform"
        name = ds.get("name") or "unknown_name"
        version = getattr(event_ingestion_request.job.facets, 'schema', None)
        version = getattr(version, 'versionId', 'v0') if version else 'v0'
        
        ds_urn = writer.urn_generators['dataset'](platform=ns, name=name, env="PROD")
        writer.upsert_dataset(platform=ns, name=name, env="PROD", versionId=version)

        # Handle column lineage
        if ds.get("facets", {}).get("columnLineage"):
            cl = ds.get("facets", {}).get("columnLineage", {}).get("fields", {})
            for out_col, spec in cl.items():
                out_col_urn = writer.urn_generators['column'](dataset_urn=ds_urn, field_path=out_col)
                writer.upsert_entity("Column", out_col_urn, {"datasetUrn": ds_urn, "fieldPath": out_col})
                writer.create_relationship("Dataset", ds_urn, "HAS_COLUMN", "Column", out_col_urn, {})

                input_columns = []
                for infl in spec.get("inputFields", []):
                    in_ns = infl.get("namespace")
                    in_name = infl.get("name")
                    in_field = infl.get("field")
                    in_ds_urn = writer.urn_generators['dataset'](platform=in_ns, name=in_name, env="PROD")
                    in_col_urn = writer.urn_generators['column'](dataset_urn=in_ds_urn, field_path=in_field)

                    writer.upsert_dataset(platform=in_ns, name=in_name, env="PROD")
                    writer.upsert_entity("Column", in_col_urn, {"datasetUrn": in_ds_urn, "fieldPath": in_field})
                    writer.create_relationship("Dataset", in_ds_urn, "HAS_COLUMN", "Column", in_col_urn, {})

                    steps = []
                    for t in infl.get("transformations", []):
                        writer.create_relationship("Column", out_col_urn, "DERIVES_FROM", "Column", in_col_urn, {
                            "type": t.get("type"), "subtype": t.get("subtype"),
                            "description": t.get("description"), "masking": bool(t.get("masking"))
                        })
                        steps.append({
                            "type": t.get("type"),
                            "subtype": t.get("subtype"),
                            "description": t.get("description"),
                            "masking": bool(t.get("masking"))
                        })

                    input_columns.append({
                        "datasetUrn": in_ds_urn,
                        "fieldPath": in_field,
                        "steps": steps
                    })

                writer.upsert_versioned_aspect("Column", out_col_urn, "transformation", {
                    "inputColumns": input_columns,
                    "notes": "auto-generated from columnLineage facet"
                })

        output_dataset_urns.append(ds_urn)

    # Create job input/output relationships
    for in_urn in input_dataset_urns:
        writer.create_consumes_relationship(job_urn, in_urn, {})
    for out_urn in output_dataset_urns:
        writer.create_produces_relationship(job_urn, out_urn, {})

    # Create input/output aspect
    io_aspect = {"inputs": input_dataset_urns, "outputs": output_dataset_urns}
    writer.upsert_versioned_aspect("DataJob", job_urn, "dataJobInputOutput", io_aspect)

    # Create dataset lineage relationships
    for out_urn in output_dataset_urns:
        for in_urn in input_dataset_urns:
            writer.create_relationship("Dataset", in_urn, "UPSTREAM_OF", "Dataset", out_urn, {"via": "job"})


def main():
    parser = argparse.ArgumentParser(description="Enhanced OpenLineage ingestion with registry-based writer")
    parser.add_argument("--event", required=True, help="Path to JSON event file")
    parser.add_argument("--registry", default="refactoring/enhanced_registry.yaml", help="Path to enhanced registry YAML")
    args = parser.parse_args()

    # Create factory and writer
    factory = RegistryFactory(args.registry)
    
    uri = os.environ.get("NEO4J_URI", "bolt://localhost:7687")
    user = os.environ.get("NEO4J_USER", "neo4j")
    pwd = os.environ.get("NEO4J_PASSWORD", "password")

    writer = factory.create_writer(uri, user, pwd)
    
    try:
        with open(args.event, "r") as f:
            agent_result = json.load(f)

        # Note: You'll need to create event_ingestion_request from agent_result
        # This is a placeholder - you'll need to adapt this based on your actual data structure
        event_ingestion_request = EventIngestionRequest()  # Create from agent_result
        
        ingest_openlineage_event_enhanced(agent_result, event_ingestion_request, writer)
        print("Enhanced ingestion complete.")
        print("Try queries:")
        print("  MATCH (c:Column)-[r:DERIVES_FROM]->(i:Column) RETURN c,r,i LIMIT 25;")
        print("  MATCH (c:Column)-[:HAS_ASPECT{name:'transformation',latest:true}]->(a) RETURN c,a LIMIT 10;")
    finally:
        writer.close()


if __name__ == "__main__":
    main()
