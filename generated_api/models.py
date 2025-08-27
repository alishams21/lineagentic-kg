#!/usr/bin/env python3
"""
Pydantic models for the generated API
"""

from pydantic import BaseModel, Field
from typing import Any, Dict, List, Optional
from datetime import datetime


# Health check models
class HealthResponse(BaseModel):
    """Health check response"""
    status: str = Field(..., description="Service status")
    registry_loaded: bool = Field(..., description="Registry loaded status")
    available_entities: List[str] = Field(..., description="Available entity types")
    available_aspects: List[str] = Field(..., description="Available aspect types")
    available_utilities: List[str] = Field(..., description="Available utility functions")


# Entity models - generated dynamically from registry

class DataProductUpsertRequest(BaseModel):
    """Request model for upserting DataProduct entity"""

    name: Optional[str] = Field(None, description="name")
    domain: Optional[str] = Field(None, description="domain")
    purpose: Optional[str] = Field(None, description="purpose")
    owner: Optional[str] = Field(None, description="owner")
    upstream: Optional[str] = Field(None, description="upstream")
    datasets: Optional[List[str]] = Field(None, description="datasets (array)")
    additional_properties: Optional[Dict[str, Any]] = Field(None, description="Additional DataProduct properties")


class DataProductGetRequest(BaseModel):
    """Request model for getting DataProduct entity"""
    urn: str = Field(..., description="DataProduct URN")


class DataProductDeleteRequest(BaseModel):
    """Request model for deleting DataProduct entity"""
    urn: str = Field(..., description="DataProduct URN")


class DataProductResponse(BaseModel):
    """Response model for DataProduct entity"""
    urn: str = Field(..., description="DataProduct URN")
    properties: Dict[str, Any] = Field(..., description="DataProduct properties")
    last_updated: Optional[datetime] = Field(None, description="Last updated timestamp")

class DatasetUpsertRequest(BaseModel):
    """Request model for upserting Dataset entity"""

    platform: Optional[str] = Field(None, description="platform")
    name: Optional[str] = Field(None, description="name")
    env: Optional[str] = Field(None, description="env")
    versionId: Optional[str] = Field(None, description="versionId")
    additional_properties: Optional[Dict[str, Any]] = Field(None, description="Additional Dataset properties")


class DatasetGetRequest(BaseModel):
    """Request model for getting Dataset entity"""
    urn: str = Field(..., description="Dataset URN")


class DatasetDeleteRequest(BaseModel):
    """Request model for deleting Dataset entity"""
    urn: str = Field(..., description="Dataset URN")


class DatasetResponse(BaseModel):
    """Response model for Dataset entity"""
    urn: str = Field(..., description="Dataset URN")
    properties: Dict[str, Any] = Field(..., description="Dataset properties")
    last_updated: Optional[datetime] = Field(None, description="Last updated timestamp")

class DataFlowUpsertRequest(BaseModel):
    """Request model for upserting DataFlow entity"""

    platform: Optional[str] = Field(None, description="platform")
    flow_id: Optional[str] = Field(None, description="flow_id")
    namespace: Optional[str] = Field(None, description="namespace")
    name: Optional[str] = Field(None, description="name")
    env: Optional[str] = Field(None, description="env")
    additional_properties: Optional[Dict[str, Any]] = Field(None, description="Additional DataFlow properties")


class DataFlowGetRequest(BaseModel):
    """Request model for getting DataFlow entity"""
    urn: str = Field(..., description="DataFlow URN")


class DataFlowDeleteRequest(BaseModel):
    """Request model for deleting DataFlow entity"""
    urn: str = Field(..., description="DataFlow URN")


class DataFlowResponse(BaseModel):
    """Response model for DataFlow entity"""
    urn: str = Field(..., description="DataFlow URN")
    properties: Dict[str, Any] = Field(..., description="DataFlow properties")
    last_updated: Optional[datetime] = Field(None, description="Last updated timestamp")

class DataJobUpsertRequest(BaseModel):
    """Request model for upserting DataJob entity"""

    flow_urn: Optional[str] = Field(None, description="flow_urn")
    job_name: Optional[str] = Field(None, description="job_name")
    additional_properties: Optional[Dict[str, Any]] = Field(None, description="Additional DataJob properties")


class DataJobGetRequest(BaseModel):
    """Request model for getting DataJob entity"""
    urn: str = Field(..., description="DataJob URN")


class DataJobDeleteRequest(BaseModel):
    """Request model for deleting DataJob entity"""
    urn: str = Field(..., description="DataJob URN")


class DataJobResponse(BaseModel):
    """Response model for DataJob entity"""
    urn: str = Field(..., description="DataJob URN")
    properties: Dict[str, Any] = Field(..., description="DataJob properties")
    last_updated: Optional[datetime] = Field(None, description="Last updated timestamp")

class CorpUserUpsertRequest(BaseModel):
    """Request model for upserting CorpUser entity"""

    username: Optional[str] = Field(None, description="username")
    additional_properties: Optional[Dict[str, Any]] = Field(None, description="Additional CorpUser properties")


class CorpUserGetRequest(BaseModel):
    """Request model for getting CorpUser entity"""
    urn: str = Field(..., description="CorpUser URN")


class CorpUserDeleteRequest(BaseModel):
    """Request model for deleting CorpUser entity"""
    urn: str = Field(..., description="CorpUser URN")


class CorpUserResponse(BaseModel):
    """Response model for CorpUser entity"""
    urn: str = Field(..., description="CorpUser URN")
    properties: Dict[str, Any] = Field(..., description="CorpUser properties")
    last_updated: Optional[datetime] = Field(None, description="Last updated timestamp")

class CorpGroupUpsertRequest(BaseModel):
    """Request model for upserting CorpGroup entity"""

    name: Optional[str] = Field(None, description="name")
    additional_properties: Optional[Dict[str, Any]] = Field(None, description="Additional CorpGroup properties")


class CorpGroupGetRequest(BaseModel):
    """Request model for getting CorpGroup entity"""
    urn: str = Field(..., description="CorpGroup URN")


class CorpGroupDeleteRequest(BaseModel):
    """Request model for deleting CorpGroup entity"""
    urn: str = Field(..., description="CorpGroup URN")


class CorpGroupResponse(BaseModel):
    """Response model for CorpGroup entity"""
    urn: str = Field(..., description="CorpGroup URN")
    properties: Dict[str, Any] = Field(..., description="CorpGroup properties")
    last_updated: Optional[datetime] = Field(None, description="Last updated timestamp")

class TagUpsertRequest(BaseModel):
    """Request model for upserting Tag entity"""

    key: Optional[str] = Field(None, description="key")
    value: Optional[str] = Field(None, description="value")
    additional_properties: Optional[Dict[str, Any]] = Field(None, description="Additional Tag properties")


class TagGetRequest(BaseModel):
    """Request model for getting Tag entity"""
    urn: str = Field(..., description="Tag URN")


class TagDeleteRequest(BaseModel):
    """Request model for deleting Tag entity"""
    urn: str = Field(..., description="Tag URN")


class TagResponse(BaseModel):
    """Response model for Tag entity"""
    urn: str = Field(..., description="Tag URN")
    properties: Dict[str, Any] = Field(..., description="Tag properties")
    last_updated: Optional[datetime] = Field(None, description="Last updated timestamp")

class ColumnUpsertRequest(BaseModel):
    """Request model for upserting Column entity"""

    dataset_urn: Optional[str] = Field(None, description="dataset_urn")
    field_path: Optional[str] = Field(None, description="field_path")
    additional_properties: Optional[Dict[str, Any]] = Field(None, description="Additional Column properties")


class ColumnGetRequest(BaseModel):
    """Request model for getting Column entity"""
    urn: str = Field(..., description="Column URN")


class ColumnDeleteRequest(BaseModel):
    """Request model for deleting Column entity"""
    urn: str = Field(..., description="Column URN")


class ColumnResponse(BaseModel):
    """Response model for Column entity"""
    urn: str = Field(..., description="Column URN")
    properties: Dict[str, Any] = Field(..., description="Column properties")
    last_updated: Optional[datetime] = Field(None, description="Last updated timestamp")


# Aspect models - generated dynamically from registry

class DpcontractAspectUpsertRequest(BaseModel):
    """Request model for upserting dpContract aspect"""
    entity_label: Optional[str] = Field(None, description="Entity label (optional if entity_creation is configured)")
    entity_urn: Optional[str] = Field(None, description="Entity URN (optional if entity_creation is configured)")

    output_name: Optional[Any] = Field(None, description="output_name")
    output_type: Optional[Any] = Field(None, description="output_type")
    fields: Optional[Any] = Field(None, description="fields")
    sink_location: Optional[Any] = Field(None, description="sink_location")
    freshness: Optional[Any] = Field(None, description="freshness")
    version: Optional[int] = Field(None, description="Version (for versioned aspects)")

    entity_params: Optional[Dict[str, Any]] = Field(None, description="Entity creation parameters")


class DpcontractAspectGetRequest(BaseModel):
    """Request model for getting dpContract aspect"""
    entity_label: str = Field(..., description="Entity label")
    entity_urn: str = Field(..., description="Entity URN")


class DpcontractAspectDeleteRequest(BaseModel):
    """Request model for deleting dpContract aspect"""
    entity_label: str = Field(..., description="Entity label")
    entity_urn: str = Field(..., description="Entity URN")


class DpcontractAspectResponse(BaseModel):
    """Response model for dpContract aspect"""
    entity_label: str = Field(..., description="Entity label")
    entity_urn: str = Field(..., description="Entity URN")
    aspect_name: str = Field(..., description="Aspect name")
    payload: Dict[str, Any] = Field(..., description="Aspect payload")
    version: Optional[int] = Field(None, description="Version")

class DpobservabilityAspectUpsertRequest(BaseModel):
    """Request model for upserting dpObservability aspect"""
    entity_label: Optional[str] = Field(None, description="Entity label (optional if entity_creation is configured)")
    entity_urn: Optional[str] = Field(None, description="Entity URN (optional if entity_creation is configured)")

    metrics_config: Optional[Any] = Field(None, description="metrics_config")
    alerting_rules: Optional[Any] = Field(None, description="alerting_rules")
    dashboard_config: Optional[Any] = Field(None, description="dashboard_config")
    version: Optional[int] = Field(None, description="Version (for versioned aspects)")

    entity_params: Optional[Dict[str, Any]] = Field(None, description="Entity creation parameters")


class DpobservabilityAspectGetRequest(BaseModel):
    """Request model for getting dpObservability aspect"""
    entity_label: str = Field(..., description="Entity label")
    entity_urn: str = Field(..., description="Entity URN")


class DpobservabilityAspectDeleteRequest(BaseModel):
    """Request model for deleting dpObservability aspect"""
    entity_label: str = Field(..., description="Entity label")
    entity_urn: str = Field(..., description="Entity URN")


class DpobservabilityAspectResponse(BaseModel):
    """Response model for dpObservability aspect"""
    entity_label: str = Field(..., description="Entity label")
    entity_urn: str = Field(..., description="Entity URN")
    aspect_name: str = Field(..., description="Aspect name")
    payload: Dict[str, Any] = Field(..., description="Aspect payload")
    version: Optional[int] = Field(None, description="Version")

class DppolicyAspectUpsertRequest(BaseModel):
    """Request model for upserting dpPolicy aspect"""
    entity_label: Optional[str] = Field(None, description="Entity label (optional if entity_creation is configured)")
    entity_urn: Optional[str] = Field(None, description="Entity URN (optional if entity_creation is configured)")

    access_control: Optional[Any] = Field(None, description="access_control")
    data_masking: Optional[Any] = Field(None, description="data_masking")
    quality_gate: Optional[Any] = Field(None, description="quality_gate")
    retention_policy: Optional[Any] = Field(None, description="retention_policy")
    evaluation_points: Optional[Any] = Field(None, description="evaluation_points")
    version: Optional[int] = Field(None, description="Version (for versioned aspects)")

    entity_params: Optional[Dict[str, Any]] = Field(None, description="Entity creation parameters")


class DppolicyAspectGetRequest(BaseModel):
    """Request model for getting dpPolicy aspect"""
    entity_label: str = Field(..., description="Entity label")
    entity_urn: str = Field(..., description="Entity URN")


class DppolicyAspectDeleteRequest(BaseModel):
    """Request model for deleting dpPolicy aspect"""
    entity_label: str = Field(..., description="Entity label")
    entity_urn: str = Field(..., description="Entity URN")


class DppolicyAspectResponse(BaseModel):
    """Response model for dpPolicy aspect"""
    entity_label: str = Field(..., description="Entity label")
    entity_urn: str = Field(..., description="Entity URN")
    aspect_name: str = Field(..., description="Aspect name")
    payload: Dict[str, Any] = Field(..., description="Aspect payload")
    version: Optional[int] = Field(None, description="Version")

class DpprovisionerAspectUpsertRequest(BaseModel):
    """Request model for upserting dpProvisioner aspect"""
    entity_label: Optional[str] = Field(None, description="Entity label (optional if entity_creation is configured)")
    entity_urn: Optional[str] = Field(None, description="Entity URN (optional if entity_creation is configured)")

    platform: Optional[Any] = Field(None, description="platform")
    environment: Optional[Any] = Field(None, description="environment")
    compute_resource: Optional[Any] = Field(None, description="compute_resource")
    storage_resource: Optional[Any] = Field(None, description="storage_resource")
    deployment_strategy: Optional[Any] = Field(None, description="deployment_strategy")
    version: Optional[int] = Field(None, description="Version (for versioned aspects)")

    entity_params: Optional[Dict[str, Any]] = Field(None, description="Entity creation parameters")


class DpprovisionerAspectGetRequest(BaseModel):
    """Request model for getting dpProvisioner aspect"""
    entity_label: str = Field(..., description="Entity label")
    entity_urn: str = Field(..., description="Entity URN")


class DpprovisionerAspectDeleteRequest(BaseModel):
    """Request model for deleting dpProvisioner aspect"""
    entity_label: str = Field(..., description="Entity label")
    entity_urn: str = Field(..., description="Entity URN")


class DpprovisionerAspectResponse(BaseModel):
    """Response model for dpProvisioner aspect"""
    entity_label: str = Field(..., description="Entity label")
    entity_urn: str = Field(..., description="Entity URN")
    aspect_name: str = Field(..., description="Aspect name")
    payload: Dict[str, Any] = Field(..., description="Aspect payload")
    version: Optional[int] = Field(None, description="Version")

class CorpuserinfoAspectUpsertRequest(BaseModel):
    """Request model for upserting corpUserInfo aspect"""
    entity_label: Optional[str] = Field(None, description="Entity label (optional if entity_creation is configured)")
    entity_urn: Optional[str] = Field(None, description="Entity URN (optional if entity_creation is configured)")

    active: Any = Field(..., description="active")
    displayName: Any = Field(..., description="displayName")
    email: Any = Field(..., description="email")
    title: Optional[Any] = Field(None, description="title")
    department: Optional[Any] = Field(None, description="department")
    managerUrn: Optional[Any] = Field(None, description="managerUrn")
    skills: Optional[Any] = Field(None, description="skills")
    version: Optional[int] = Field(None, description="Version (for versioned aspects)")

    entity_params: Optional[Dict[str, Any]] = Field(None, description="Entity creation parameters")


class CorpuserinfoAspectGetRequest(BaseModel):
    """Request model for getting corpUserInfo aspect"""
    entity_label: str = Field(..., description="Entity label")
    entity_urn: str = Field(..., description="Entity URN")


class CorpuserinfoAspectDeleteRequest(BaseModel):
    """Request model for deleting corpUserInfo aspect"""
    entity_label: str = Field(..., description="Entity label")
    entity_urn: str = Field(..., description="Entity URN")


class CorpuserinfoAspectResponse(BaseModel):
    """Response model for corpUserInfo aspect"""
    entity_label: str = Field(..., description="Entity label")
    entity_urn: str = Field(..., description="Entity URN")
    aspect_name: str = Field(..., description="Aspect name")
    payload: Dict[str, Any] = Field(..., description="Aspect payload")
    version: Optional[int] = Field(None, description="Version")

class CorpgroupinfoAspectUpsertRequest(BaseModel):
    """Request model for upserting corpGroupInfo aspect"""
    entity_label: Optional[str] = Field(None, description="Entity label (optional if entity_creation is configured)")
    entity_urn: Optional[str] = Field(None, description="Entity URN (optional if entity_creation is configured)")

    name: Any = Field(..., description="name")
    description: Optional[Any] = Field(None, description="description")
    email: Optional[Any] = Field(None, description="email")
    slackChannel: Optional[Any] = Field(None, description="slackChannel")
    version: Optional[int] = Field(None, description="Version (for versioned aspects)")

    entity_params: Optional[Dict[str, Any]] = Field(None, description="Entity creation parameters")


class CorpgroupinfoAspectGetRequest(BaseModel):
    """Request model for getting corpGroupInfo aspect"""
    entity_label: str = Field(..., description="Entity label")
    entity_urn: str = Field(..., description="Entity URN")


class CorpgroupinfoAspectDeleteRequest(BaseModel):
    """Request model for deleting corpGroupInfo aspect"""
    entity_label: str = Field(..., description="Entity label")
    entity_urn: str = Field(..., description="Entity URN")


class CorpgroupinfoAspectResponse(BaseModel):
    """Response model for corpGroupInfo aspect"""
    entity_label: str = Field(..., description="Entity label")
    entity_urn: str = Field(..., description="Entity URN")
    aspect_name: str = Field(..., description="Aspect name")
    payload: Dict[str, Any] = Field(..., description="Aspect payload")
    version: Optional[int] = Field(None, description="Version")

class DatasetpropertiesAspectUpsertRequest(BaseModel):
    """Request model for upserting datasetProperties aspect"""
    entity_label: Optional[str] = Field(None, description="Entity label (optional if entity_creation is configured)")
    entity_urn: Optional[str] = Field(None, description="Entity URN (optional if entity_creation is configured)")

    description: Any = Field(..., description="description")
    customProperties: Optional[Any] = Field(None, description="customProperties")
    tags: Optional[Any] = Field(None, description="tags")
    externalUrl: Optional[Any] = Field(None, description="externalUrl")
    version: Optional[int] = Field(None, description="Version (for versioned aspects)")

    entity_params: Optional[Dict[str, Any]] = Field(None, description="Entity creation parameters")


class DatasetpropertiesAspectGetRequest(BaseModel):
    """Request model for getting datasetProperties aspect"""
    entity_label: str = Field(..., description="Entity label")
    entity_urn: str = Field(..., description="Entity URN")


class DatasetpropertiesAspectDeleteRequest(BaseModel):
    """Request model for deleting datasetProperties aspect"""
    entity_label: str = Field(..., description="Entity label")
    entity_urn: str = Field(..., description="Entity URN")


class DatasetpropertiesAspectResponse(BaseModel):
    """Response model for datasetProperties aspect"""
    entity_label: str = Field(..., description="Entity label")
    entity_urn: str = Field(..., description="Entity URN")
    aspect_name: str = Field(..., description="Aspect name")
    payload: Dict[str, Any] = Field(..., description="Aspect payload")
    version: Optional[int] = Field(None, description="Version")

class SchemametadataAspectUpsertRequest(BaseModel):
    """Request model for upserting schemaMetadata aspect"""
    entity_label: Optional[str] = Field(None, description="Entity label (optional if entity_creation is configured)")
    entity_urn: Optional[str] = Field(None, description="Entity URN (optional if entity_creation is configured)")

    schemaName: Any = Field(..., description="schemaName")
    platform: Any = Field(..., description="platform")
    version: Optional[Any] = Field(None, description="version")
    fields: Any = Field(..., description="fields")
    primaryKeys: Optional[Any] = Field(None, description="primaryKeys")
    foreignKeys: Optional[Any] = Field(None, description="foreignKeys")
    version: Optional[int] = Field(None, description="Version (for versioned aspects)")

    entity_params: Optional[Dict[str, Any]] = Field(None, description="Entity creation parameters")


class SchemametadataAspectGetRequest(BaseModel):
    """Request model for getting schemaMetadata aspect"""
    entity_label: str = Field(..., description="Entity label")
    entity_urn: str = Field(..., description="Entity URN")


class SchemametadataAspectDeleteRequest(BaseModel):
    """Request model for deleting schemaMetadata aspect"""
    entity_label: str = Field(..., description="Entity label")
    entity_urn: str = Field(..., description="Entity URN")


class SchemametadataAspectResponse(BaseModel):
    """Response model for schemaMetadata aspect"""
    entity_label: str = Field(..., description="Entity label")
    entity_urn: str = Field(..., description="Entity URN")
    aspect_name: str = Field(..., description="Aspect name")
    payload: Dict[str, Any] = Field(..., description="Aspect payload")
    version: Optional[int] = Field(None, description="Version")

class OwnershipAspectUpsertRequest(BaseModel):
    """Request model for upserting ownership aspect"""
    entity_label: Optional[str] = Field(None, description="Entity label (optional if entity_creation is configured)")
    entity_urn: Optional[str] = Field(None, description="Entity URN (optional if entity_creation is configured)")

    owners: Any = Field(..., description="owners")
    lastModified: Optional[Any] = Field(None, description="lastModified")
    version: Optional[int] = Field(None, description="Version (for versioned aspects)")

    entity_params: Optional[Dict[str, Any]] = Field(None, description="Entity creation parameters")


class OwnershipAspectGetRequest(BaseModel):
    """Request model for getting ownership aspect"""
    entity_label: str = Field(..., description="Entity label")
    entity_urn: str = Field(..., description="Entity URN")


class OwnershipAspectDeleteRequest(BaseModel):
    """Request model for deleting ownership aspect"""
    entity_label: str = Field(..., description="Entity label")
    entity_urn: str = Field(..., description="Entity URN")


class OwnershipAspectResponse(BaseModel):
    """Response model for ownership aspect"""
    entity_label: str = Field(..., description="Entity label")
    entity_urn: str = Field(..., description="Entity URN")
    aspect_name: str = Field(..., description="Aspect name")
    payload: Dict[str, Any] = Field(..., description="Aspect payload")
    version: Optional[int] = Field(None, description="Version")

class GlobaltagsAspectUpsertRequest(BaseModel):
    """Request model for upserting globalTags aspect"""
    entity_label: Optional[str] = Field(None, description="Entity label (optional if entity_creation is configured)")
    entity_urn: Optional[str] = Field(None, description="Entity URN (optional if entity_creation is configured)")

    tags: Any = Field(..., description="tags")
    version: Optional[int] = Field(None, description="Version (for versioned aspects)")

    entity_params: Optional[Dict[str, Any]] = Field(None, description="Entity creation parameters")


class GlobaltagsAspectGetRequest(BaseModel):
    """Request model for getting globalTags aspect"""
    entity_label: str = Field(..., description="Entity label")
    entity_urn: str = Field(..., description="Entity URN")


class GlobaltagsAspectDeleteRequest(BaseModel):
    """Request model for deleting globalTags aspect"""
    entity_label: str = Field(..., description="Entity label")
    entity_urn: str = Field(..., description="Entity URN")


class GlobaltagsAspectResponse(BaseModel):
    """Response model for globalTags aspect"""
    entity_label: str = Field(..., description="Entity label")
    entity_urn: str = Field(..., description="Entity URN")
    aspect_name: str = Field(..., description="Aspect name")
    payload: Dict[str, Any] = Field(..., description="Aspect payload")
    version: Optional[int] = Field(None, description="Version")

class DatasetprofileAspectUpsertRequest(BaseModel):
    """Request model for upserting datasetProfile aspect"""
    entity_label: Optional[str] = Field(None, description="Entity label (optional if entity_creation is configured)")
    entity_urn: Optional[str] = Field(None, description="Entity URN (optional if entity_creation is configured)")

    rowCount: Any = Field(..., description="rowCount")
    columnCount: Any = Field(..., description="columnCount")
    sizeInBytes: Optional[Any] = Field(None, description="sizeInBytes")
    lastModified: Optional[Any] = Field(None, description="lastModified")
    partitionCount: Optional[Any] = Field(None, description="partitionCount")
    timestamp_ms: Optional[int] = Field(None, description="Timestamp in milliseconds (for timeseries aspects)")

    entity_params: Optional[Dict[str, Any]] = Field(None, description="Entity creation parameters")


class DatasetprofileAspectGetRequest(BaseModel):
    """Request model for getting datasetProfile aspect"""
    entity_label: str = Field(..., description="Entity label")
    entity_urn: str = Field(..., description="Entity URN")
    limit: Optional[int] = Field(100, description="Limit for timeseries aspects")


class DatasetprofileAspectDeleteRequest(BaseModel):
    """Request model for deleting datasetProfile aspect"""
    entity_label: str = Field(..., description="Entity label")
    entity_urn: str = Field(..., description="Entity URN")


class DatasetprofileAspectResponse(BaseModel):
    """Response model for datasetProfile aspect"""
    entity_label: str = Field(..., description="Entity label")
    entity_urn: str = Field(..., description="Entity URN")
    aspect_name: str = Field(..., description="Aspect name")
    payload: List[Dict[str, Any]] = Field(..., description="Aspect payload")
    timestamp_ms: Optional[int] = Field(None, description="Timestamp")

class DataflowinfoAspectUpsertRequest(BaseModel):
    """Request model for upserting dataFlowInfo aspect"""
    entity_label: Optional[str] = Field(None, description="Entity label (optional if entity_creation is configured)")
    entity_urn: Optional[str] = Field(None, description="Entity URN (optional if entity_creation is configured)")

    name: Any = Field(..., description="name")
    namespace: Any = Field(..., description="namespace")
    description: Optional[Any] = Field(None, description="description")
    version: Optional[Any] = Field(None, description="version")
    version: Optional[int] = Field(None, description="Version (for versioned aspects)")

    entity_params: Optional[Dict[str, Any]] = Field(None, description="Entity creation parameters")


class DataflowinfoAspectGetRequest(BaseModel):
    """Request model for getting dataFlowInfo aspect"""
    entity_label: str = Field(..., description="Entity label")
    entity_urn: str = Field(..., description="Entity URN")


class DataflowinfoAspectDeleteRequest(BaseModel):
    """Request model for deleting dataFlowInfo aspect"""
    entity_label: str = Field(..., description="Entity label")
    entity_urn: str = Field(..., description="Entity URN")


class DataflowinfoAspectResponse(BaseModel):
    """Response model for dataFlowInfo aspect"""
    entity_label: str = Field(..., description="Entity label")
    entity_urn: str = Field(..., description="Entity URN")
    aspect_name: str = Field(..., description="Aspect name")
    payload: Dict[str, Any] = Field(..., description="Aspect payload")
    version: Optional[int] = Field(None, description="Version")

class DatajobinfoAspectUpsertRequest(BaseModel):
    """Request model for upserting dataJobInfo aspect"""
    entity_label: Optional[str] = Field(None, description="Entity label (optional if entity_creation is configured)")
    entity_urn: Optional[str] = Field(None, description="Entity URN (optional if entity_creation is configured)")

    name: Any = Field(..., description="name")
    namespace: Any = Field(..., description="namespace")
    versionId: Optional[Any] = Field(None, description="versionId")
    integration: Optional[Any] = Field(None, description="integration")
    processingType: Optional[Any] = Field(None, description="processingType")
    jobType: Optional[Any] = Field(None, description="jobType")
    description: Optional[Any] = Field(None, description="description")
    version: Optional[int] = Field(None, description="Version (for versioned aspects)")

    entity_params: Optional[Dict[str, Any]] = Field(None, description="Entity creation parameters")


class DatajobinfoAspectGetRequest(BaseModel):
    """Request model for getting dataJobInfo aspect"""
    entity_label: str = Field(..., description="Entity label")
    entity_urn: str = Field(..., description="Entity URN")


class DatajobinfoAspectDeleteRequest(BaseModel):
    """Request model for deleting dataJobInfo aspect"""
    entity_label: str = Field(..., description="Entity label")
    entity_urn: str = Field(..., description="Entity URN")


class DatajobinfoAspectResponse(BaseModel):
    """Response model for dataJobInfo aspect"""
    entity_label: str = Field(..., description="Entity label")
    entity_urn: str = Field(..., description="Entity URN")
    aspect_name: str = Field(..., description="Aspect name")
    payload: Dict[str, Any] = Field(..., description="Aspect payload")
    version: Optional[int] = Field(None, description="Version")

class DocumentationAspectUpsertRequest(BaseModel):
    """Request model for upserting documentation aspect"""
    entity_label: Optional[str] = Field(None, description="Entity label (optional if entity_creation is configured)")
    entity_urn: Optional[str] = Field(None, description="Entity URN (optional if entity_creation is configured)")

    description: Any = Field(..., description="description")
    contentType: Optional[Any] = Field(None, description="contentType")
    content: Optional[Any] = Field(None, description="content")
    version: Optional[int] = Field(None, description="Version (for versioned aspects)")

    entity_params: Optional[Dict[str, Any]] = Field(None, description="Entity creation parameters")


class DocumentationAspectGetRequest(BaseModel):
    """Request model for getting documentation aspect"""
    entity_label: str = Field(..., description="Entity label")
    entity_urn: str = Field(..., description="Entity URN")


class DocumentationAspectDeleteRequest(BaseModel):
    """Request model for deleting documentation aspect"""
    entity_label: str = Field(..., description="Entity label")
    entity_urn: str = Field(..., description="Entity URN")


class DocumentationAspectResponse(BaseModel):
    """Response model for documentation aspect"""
    entity_label: str = Field(..., description="Entity label")
    entity_urn: str = Field(..., description="Entity URN")
    aspect_name: str = Field(..., description="Aspect name")
    payload: Dict[str, Any] = Field(..., description="Aspect payload")
    version: Optional[int] = Field(None, description="Version")

class SourcecodelocationAspectUpsertRequest(BaseModel):
    """Request model for upserting sourceCodeLocation aspect"""
    entity_label: Optional[str] = Field(None, description="Entity label (optional if entity_creation is configured)")
    entity_urn: Optional[str] = Field(None, description="Entity URN (optional if entity_creation is configured)")

    type: Any = Field(..., description="type")
    url: Any = Field(..., description="url")
    repo: Optional[Any] = Field(None, description="repo")
    branch: Optional[Any] = Field(None, description="branch")
    path: Optional[Any] = Field(None, description="path")
    version: Optional[int] = Field(None, description="Version (for versioned aspects)")

    entity_params: Optional[Dict[str, Any]] = Field(None, description="Entity creation parameters")


class SourcecodelocationAspectGetRequest(BaseModel):
    """Request model for getting sourceCodeLocation aspect"""
    entity_label: str = Field(..., description="Entity label")
    entity_urn: str = Field(..., description="Entity URN")


class SourcecodelocationAspectDeleteRequest(BaseModel):
    """Request model for deleting sourceCodeLocation aspect"""
    entity_label: str = Field(..., description="Entity label")
    entity_urn: str = Field(..., description="Entity URN")


class SourcecodelocationAspectResponse(BaseModel):
    """Response model for sourceCodeLocation aspect"""
    entity_label: str = Field(..., description="Entity label")
    entity_urn: str = Field(..., description="Entity URN")
    aspect_name: str = Field(..., description="Aspect name")
    payload: Dict[str, Any] = Field(..., description="Aspect payload")
    version: Optional[int] = Field(None, description="Version")

class SourcecodeAspectUpsertRequest(BaseModel):
    """Request model for upserting sourceCode aspect"""
    entity_label: Optional[str] = Field(None, description="Entity label (optional if entity_creation is configured)")
    entity_urn: Optional[str] = Field(None, description="Entity URN (optional if entity_creation is configured)")

    language: Any = Field(..., description="language")
    snippet: Optional[Any] = Field(None, description="snippet")
    fullCode: Optional[Any] = Field(None, description="fullCode")
    version: Optional[int] = Field(None, description="Version (for versioned aspects)")

    entity_params: Optional[Dict[str, Any]] = Field(None, description="Entity creation parameters")


class SourcecodeAspectGetRequest(BaseModel):
    """Request model for getting sourceCode aspect"""
    entity_label: str = Field(..., description="Entity label")
    entity_urn: str = Field(..., description="Entity URN")


class SourcecodeAspectDeleteRequest(BaseModel):
    """Request model for deleting sourceCode aspect"""
    entity_label: str = Field(..., description="Entity label")
    entity_urn: str = Field(..., description="Entity URN")


class SourcecodeAspectResponse(BaseModel):
    """Response model for sourceCode aspect"""
    entity_label: str = Field(..., description="Entity label")
    entity_urn: str = Field(..., description="Entity URN")
    aspect_name: str = Field(..., description="Aspect name")
    payload: Dict[str, Any] = Field(..., description="Aspect payload")
    version: Optional[int] = Field(None, description="Version")

class EnvironmentpropertiesAspectUpsertRequest(BaseModel):
    """Request model for upserting environmentProperties aspect"""
    entity_label: Optional[str] = Field(None, description="Entity label (optional if entity_creation is configured)")
    entity_urn: Optional[str] = Field(None, description="Entity URN (optional if entity_creation is configured)")

    env: Any = Field(..., description="env")
    config: Optional[Any] = Field(None, description="config")
    version: Optional[int] = Field(None, description="Version (for versioned aspects)")

    entity_params: Optional[Dict[str, Any]] = Field(None, description="Entity creation parameters")


class EnvironmentpropertiesAspectGetRequest(BaseModel):
    """Request model for getting environmentProperties aspect"""
    entity_label: str = Field(..., description="Entity label")
    entity_urn: str = Field(..., description="Entity URN")


class EnvironmentpropertiesAspectDeleteRequest(BaseModel):
    """Request model for deleting environmentProperties aspect"""
    entity_label: str = Field(..., description="Entity label")
    entity_urn: str = Field(..., description="Entity URN")


class EnvironmentpropertiesAspectResponse(BaseModel):
    """Response model for environmentProperties aspect"""
    entity_label: str = Field(..., description="Entity label")
    entity_urn: str = Field(..., description="Entity URN")
    aspect_name: str = Field(..., description="Aspect name")
    payload: Dict[str, Any] = Field(..., description="Aspect payload")
    version: Optional[int] = Field(None, description="Version")

class DatajobinputoutputAspectUpsertRequest(BaseModel):
    """Request model for upserting dataJobInputOutput aspect"""
    entity_label: Optional[str] = Field(None, description="Entity label (optional if entity_creation is configured)")
    entity_urn: Optional[str] = Field(None, description="Entity URN (optional if entity_creation is configured)")

    inputs: Any = Field(..., description="inputs")
    outputs: Any = Field(..., description="outputs")
    version: Optional[int] = Field(None, description="Version (for versioned aspects)")

    entity_params: Optional[Dict[str, Any]] = Field(None, description="Entity creation parameters")


class DatajobinputoutputAspectGetRequest(BaseModel):
    """Request model for getting dataJobInputOutput aspect"""
    entity_label: str = Field(..., description="Entity label")
    entity_urn: str = Field(..., description="Entity URN")


class DatajobinputoutputAspectDeleteRequest(BaseModel):
    """Request model for deleting dataJobInputOutput aspect"""
    entity_label: str = Field(..., description="Entity label")
    entity_urn: str = Field(..., description="Entity URN")


class DatajobinputoutputAspectResponse(BaseModel):
    """Response model for dataJobInputOutput aspect"""
    entity_label: str = Field(..., description="Entity label")
    entity_urn: str = Field(..., description="Entity URN")
    aspect_name: str = Field(..., description="Aspect name")
    payload: Dict[str, Any] = Field(..., description="Aspect payload")
    version: Optional[int] = Field(None, description="Version")

class DatajobrunAspectUpsertRequest(BaseModel):
    """Request model for upserting dataJobRun aspect"""
    entity_label: Optional[str] = Field(None, description="Entity label (optional if entity_creation is configured)")
    entity_urn: Optional[str] = Field(None, description="Entity URN (optional if entity_creation is configured)")

    eventType: Any = Field(..., description="eventType")
    runId: Any = Field(..., description="runId")
    parent: Optional[Any] = Field(None, description="parent")
    status: Optional[Any] = Field(None, description="status")
    startTime: Optional[Any] = Field(None, description="startTime")
    endTime: Optional[Any] = Field(None, description="endTime")
    timestamp_ms: Optional[int] = Field(None, description="Timestamp in milliseconds (for timeseries aspects)")

    entity_params: Optional[Dict[str, Any]] = Field(None, description="Entity creation parameters")


class DatajobrunAspectGetRequest(BaseModel):
    """Request model for getting dataJobRun aspect"""
    entity_label: str = Field(..., description="Entity label")
    entity_urn: str = Field(..., description="Entity URN")
    limit: Optional[int] = Field(100, description="Limit for timeseries aspects")


class DatajobrunAspectDeleteRequest(BaseModel):
    """Request model for deleting dataJobRun aspect"""
    entity_label: str = Field(..., description="Entity label")
    entity_urn: str = Field(..., description="Entity URN")


class DatajobrunAspectResponse(BaseModel):
    """Response model for dataJobRun aspect"""
    entity_label: str = Field(..., description="Entity label")
    entity_urn: str = Field(..., description="Entity URN")
    aspect_name: str = Field(..., description="Aspect name")
    payload: List[Dict[str, Any]] = Field(..., description="Aspect payload")
    timestamp_ms: Optional[int] = Field(None, description="Timestamp")

class ColumnpropertiesAspectUpsertRequest(BaseModel):
    """Request model for upserting columnProperties aspect"""
    entity_label: Optional[str] = Field(None, description="Entity label (optional if entity_creation is configured)")
    entity_urn: Optional[str] = Field(None, description="Entity URN (optional if entity_creation is configured)")

    description: Optional[Any] = Field(None, description="description")
    dataType: Any = Field(..., description="dataType")
    nullable: Optional[Any] = Field(None, description="nullable")
    defaultValue: Optional[Any] = Field(None, description="defaultValue")
    version: Optional[int] = Field(None, description="Version (for versioned aspects)")

    entity_params: Optional[Dict[str, Any]] = Field(None, description="Entity creation parameters")


class ColumnpropertiesAspectGetRequest(BaseModel):
    """Request model for getting columnProperties aspect"""
    entity_label: str = Field(..., description="Entity label")
    entity_urn: str = Field(..., description="Entity URN")


class ColumnpropertiesAspectDeleteRequest(BaseModel):
    """Request model for deleting columnProperties aspect"""
    entity_label: str = Field(..., description="Entity label")
    entity_urn: str = Field(..., description="Entity URN")


class ColumnpropertiesAspectResponse(BaseModel):
    """Response model for columnProperties aspect"""
    entity_label: str = Field(..., description="Entity label")
    entity_urn: str = Field(..., description="Entity URN")
    aspect_name: str = Field(..., description="Aspect name")
    payload: Dict[str, Any] = Field(..., description="Aspect payload")
    version: Optional[int] = Field(None, description="Version")

class TransformationAspectUpsertRequest(BaseModel):
    """Request model for upserting transformation aspect"""
    entity_label: Optional[str] = Field(None, description="Entity label (optional if entity_creation is configured)")
    entity_urn: Optional[str] = Field(None, description="Entity URN (optional if entity_creation is configured)")

    inputColumns: Any = Field(..., description="inputColumns")
    steps: Optional[Any] = Field(None, description="steps")
    notes: Optional[Any] = Field(None, description="notes")
    sourceDataset: Optional[Any] = Field(None, description="sourceDataset")
    targetDataset: Optional[Any] = Field(None, description="targetDataset")
    transformationType: Any = Field(..., description="transformationType")
    version: Optional[int] = Field(None, description="Version (for versioned aspects)")

    entity_params: Optional[Dict[str, Any]] = Field(None, description="Entity creation parameters")


class TransformationAspectGetRequest(BaseModel):
    """Request model for getting transformation aspect"""
    entity_label: str = Field(..., description="Entity label")
    entity_urn: str = Field(..., description="Entity URN")


class TransformationAspectDeleteRequest(BaseModel):
    """Request model for deleting transformation aspect"""
    entity_label: str = Field(..., description="Entity label")
    entity_urn: str = Field(..., description="Entity URN")


class TransformationAspectResponse(BaseModel):
    """Response model for transformation aspect"""
    entity_label: str = Field(..., description="Entity label")
    entity_urn: str = Field(..., description="Entity URN")
    aspect_name: str = Field(..., description="Aspect name")
    payload: Dict[str, Any] = Field(..., description="Aspect payload")
    version: Optional[int] = Field(None, description="Version")


# Utility models
class UtilityRequest(BaseModel):
    """Request model for utility functions"""
    function_name: str = Field(..., description="Name of the utility function")
    parameters: Optional[Dict[str, Any]] = Field(None, description="Function parameters")


class UtilityResponse(BaseModel):
    """Response model for utility functions"""
    result: Any = Field(..., description="Function result")
    function_name: str = Field(..., description="Name of the utility function")


# Discovery models
class DiscoveryRequest(BaseModel):
    """Request model for relationship discovery"""
    entity_urn: str = Field(..., description="Entity URN")
    entity_type: str = Field(..., description="Entity type")
    aspect_name: str = Field(..., description="Aspect name")
    aspect_data: Dict[str, Any] = Field(..., description="Aspect data")


class DiscoveryResponse(BaseModel):
    """Response model for relationship discovery"""
    message: str = Field(..., description="Discovery result message")
    relationships_created: int = Field(..., description="Number of relationships created")
