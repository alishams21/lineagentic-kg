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
    groupOwners: Optional[Any] = Field(None, description="groupOwners")
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

class CorpuserinfoAspectUpsertRequest(BaseModel):
    """Request model for upserting corpUserInfo aspect"""
    entity_label: Optional[str] = Field(None, description="Entity label (optional if entity_creation is configured)")
    entity_urn: Optional[str] = Field(None, description="Entity URN (optional if entity_creation is configured)")

    active: Any = Field(..., description="active")
    displayName: Any = Field(..., description="displayName")
    email: Any = Field(..., description="email")
    title: Optional[Any] = Field(None, description="title")
    managerUrn: Optional[Any] = Field(None, description="managerUrn")
    departmentId: Optional[Any] = Field(None, description="departmentId")
    departmentName: Optional[Any] = Field(None, description="departmentName")
    firstName: Optional[Any] = Field(None, description="firstName")
    lastName: Optional[Any] = Field(None, description="lastName")
    fullName: Optional[Any] = Field(None, description="fullName")
    countryCode: Optional[Any] = Field(None, description="countryCode")
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

    displayName: Any = Field(..., description="displayName")
    description: Optional[Any] = Field(None, description="description")
    email: Optional[Any] = Field(None, description="email")
    admins: Optional[Any] = Field(None, description="admins")
    members: Optional[Any] = Field(None, description="members")
    groups: Optional[Any] = Field(None, description="groups")
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

class ColumnpropertiesAspectUpsertRequest(BaseModel):
    """Request model for upserting columnProperties aspect"""
    entity_label: Optional[str] = Field(None, description="Entity label (optional if entity_creation is configured)")
    entity_urn: Optional[str] = Field(None, description="Entity URN (optional if entity_creation is configured)")

    description: Optional[Any] = Field(None, description="description")
    dataType: Any = Field(..., description="dataType")
    nullable: Optional[Any] = Field(None, description="nullable")
    defaultValue: Optional[Any] = Field(None, description="defaultValue")
    customProperties: Optional[Any] = Field(None, description="customProperties")
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

class ColumntransformationAspectUpsertRequest(BaseModel):
    """Request model for upserting columnTransformation aspect"""
    entity_label: Optional[str] = Field(None, description="Entity label (optional if entity_creation is configured)")
    entity_urn: Optional[str] = Field(None, description="Entity URN (optional if entity_creation is configured)")

    inputColumns: Any = Field(..., description="inputColumns")
    transformType: Any = Field(..., description="transformType")
    transformScript: Optional[Any] = Field(None, description="transformScript")
    sourceDataset: Optional[Any] = Field(None, description="sourceDataset")
    version: Optional[int] = Field(None, description="Version (for versioned aspects)")

    entity_params: Optional[Dict[str, Any]] = Field(None, description="Entity creation parameters")


class ColumntransformationAspectGetRequest(BaseModel):
    """Request model for getting columnTransformation aspect"""
    entity_label: str = Field(..., description="Entity label")
    entity_urn: str = Field(..., description="Entity URN")


class ColumntransformationAspectDeleteRequest(BaseModel):
    """Request model for deleting columnTransformation aspect"""
    entity_label: str = Field(..., description="Entity label")
    entity_urn: str = Field(..., description="Entity URN")


class ColumntransformationAspectResponse(BaseModel):
    """Response model for columnTransformation aspect"""
    entity_label: str = Field(..., description="Entity label")
    entity_urn: str = Field(..., description="Entity URN")
    aspect_name: str = Field(..., description="Aspect name")
    payload: Dict[str, Any] = Field(..., description="Aspect payload")
    version: Optional[int] = Field(None, description="Version")

class DatasettransformationAspectUpsertRequest(BaseModel):
    """Request model for upserting datasetTransformation aspect"""
    entity_label: Optional[str] = Field(None, description="Entity label (optional if entity_creation is configured)")
    entity_urn: Optional[str] = Field(None, description="Entity URN (optional if entity_creation is configured)")

    sourceDataset: Any = Field(..., description="sourceDataset")
    targetDataset: Any = Field(..., description="targetDataset")
    transformationType: Optional[Any] = Field(None, description="transformationType")
    description: Optional[Any] = Field(None, description="description")
    version: Optional[int] = Field(None, description="Version (for versioned aspects)")

    entity_params: Optional[Dict[str, Any]] = Field(None, description="Entity creation parameters")


class DatasettransformationAspectGetRequest(BaseModel):
    """Request model for getting datasetTransformation aspect"""
    entity_label: str = Field(..., description="Entity label")
    entity_urn: str = Field(..., description="Entity URN")


class DatasettransformationAspectDeleteRequest(BaseModel):
    """Request model for deleting datasetTransformation aspect"""
    entity_label: str = Field(..., description="Entity label")
    entity_urn: str = Field(..., description="Entity URN")


class DatasettransformationAspectResponse(BaseModel):
    """Response model for datasetTransformation aspect"""
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
