#!/usr/bin/env python3
"""
UPSERT routes for the generated API
"""

from fastapi import APIRouter, HTTPException
from typing import Any, Dict
import models
import factory_wrapper


router = APIRouter(prefix="/api/v1", tags=["UPSERT Operations"])


# Entity UPSERT routes

@router.post("/entities/Column", response_model=models.ColumnResponse)
async def upsert_Column(request: models.ColumnUpsertRequest):
    """Upsert Column entity"""
    try:
        factory = factory_wrapper.get_factory_instance()
        writer = factory_wrapper.get_writer_instance()
        
        method_name = "upsert_column"
        if not hasattr(writer, method_name):
            raise HTTPException(status_code=400, detail=f"Entity type 'Column' not found")
        
        method = getattr(writer, method_name)
        
        # Extract parameters from request
        params = request.dict()
        additional_properties = params.pop('additional_properties', None)
        
        # Add additional properties if provided
        if additional_properties:
            params.update(additional_properties)
        
        # Call the generated method - URN will be generated automatically
        result_urn = method(**params)
        
        # Get the created/updated entity
        get_method_name = "get_column"
        get_method = getattr(writer, get_method_name)
        entity_data = get_method(result_urn)
        
        return models.ColumnResponse(
            urn=result_urn,
            properties=entity_data or {},
            last_updated=entity_data.get('lastUpdated') if entity_data else None
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/entities/CorpGroup", response_model=models.CorpGroupResponse)
async def upsert_CorpGroup(request: models.CorpGroupUpsertRequest):
    """Upsert CorpGroup entity"""
    try:
        factory = factory_wrapper.get_factory_instance()
        writer = factory_wrapper.get_writer_instance()
        
        method_name = "upsert_corpgroup"
        if not hasattr(writer, method_name):
            raise HTTPException(status_code=400, detail=f"Entity type 'CorpGroup' not found")
        
        method = getattr(writer, method_name)
        
        # Extract parameters from request
        params = request.dict()
        additional_properties = params.pop('additional_properties', None)
        
        # Add additional properties if provided
        if additional_properties:
            params.update(additional_properties)
        
        # Call the generated method - URN will be generated automatically
        result_urn = method(**params)
        
        # Get the created/updated entity
        get_method_name = "get_corpgroup"
        get_method = getattr(writer, get_method_name)
        entity_data = get_method(result_urn)
        
        return models.CorpGroupResponse(
            urn=result_urn,
            properties=entity_data or {},
            last_updated=entity_data.get('lastUpdated') if entity_data else None
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/entities/CorpUser", response_model=models.CorpUserResponse)
async def upsert_CorpUser(request: models.CorpUserUpsertRequest):
    """Upsert CorpUser entity"""
    try:
        factory = factory_wrapper.get_factory_instance()
        writer = factory_wrapper.get_writer_instance()
        
        method_name = "upsert_corpuser"
        if not hasattr(writer, method_name):
            raise HTTPException(status_code=400, detail=f"Entity type 'CorpUser' not found")
        
        method = getattr(writer, method_name)
        
        # Extract parameters from request
        params = request.dict()
        additional_properties = params.pop('additional_properties', None)
        
        # Add additional properties if provided
        if additional_properties:
            params.update(additional_properties)
        
        # Call the generated method - URN will be generated automatically
        result_urn = method(**params)
        
        # Get the created/updated entity
        get_method_name = "get_corpuser"
        get_method = getattr(writer, get_method_name)
        entity_data = get_method(result_urn)
        
        return models.CorpUserResponse(
            urn=result_urn,
            properties=entity_data or {},
            last_updated=entity_data.get('lastUpdated') if entity_data else None
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/entities/Dataset", response_model=models.DatasetResponse)
async def upsert_Dataset(request: models.DatasetUpsertRequest):
    """Upsert Dataset entity"""
    try:
        factory = factory_wrapper.get_factory_instance()
        writer = factory_wrapper.get_writer_instance()
        
        method_name = "upsert_dataset"
        if not hasattr(writer, method_name):
            raise HTTPException(status_code=400, detail=f"Entity type 'Dataset' not found")
        
        method = getattr(writer, method_name)
        
        # Extract parameters from request
        params = request.dict()
        additional_properties = params.pop('additional_properties', None)
        
        # Add additional properties if provided
        if additional_properties:
            params.update(additional_properties)
        
        # Call the generated method - URN will be generated automatically
        result_urn = method(**params)
        
        # Get the created/updated entity
        get_method_name = "get_dataset"
        get_method = getattr(writer, get_method_name)
        entity_data = get_method(result_urn)
        
        return models.DatasetResponse(
            urn=result_urn,
            properties=entity_data or {},
            last_updated=entity_data.get('lastUpdated') if entity_data else None
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/entities/Tag", response_model=models.TagResponse)
async def upsert_Tag(request: models.TagUpsertRequest):
    """Upsert Tag entity"""
    try:
        factory = factory_wrapper.get_factory_instance()
        writer = factory_wrapper.get_writer_instance()
        
        method_name = "upsert_tag"
        if not hasattr(writer, method_name):
            raise HTTPException(status_code=400, detail=f"Entity type 'Tag' not found")
        
        method = getattr(writer, method_name)
        
        # Extract parameters from request
        params = request.dict()
        additional_properties = params.pop('additional_properties', None)
        
        # Add additional properties if provided
        if additional_properties:
            params.update(additional_properties)
        
        # Call the generated method - URN will be generated automatically
        result_urn = method(**params)
        
        # Get the created/updated entity
        get_method_name = "get_tag"
        get_method = getattr(writer, get_method_name)
        entity_data = get_method(result_urn)
        
        return models.TagResponse(
            urn=result_urn,
            properties=entity_data or {},
            last_updated=entity_data.get('lastUpdated') if entity_data else None
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/aspects/columnProperties", response_model=models.ColumnpropertiesAspectResponse)
async def upsert_columnProperties_aspect(request: models.ColumnpropertiesAspectUpsertRequest):
    """Upsert columnProperties aspect"""
    try:
        factory = factory_wrapper.get_factory_instance()
        writer = factory_wrapper.get_writer_instance()
        
        method_name = "upsert_columnproperties_aspect"
        if not hasattr(writer, method_name):
            raise HTTPException(status_code=400, detail=f"Aspect 'columnProperties' not found")
        
        method = getattr(writer, method_name)
        
        # Prepare parameters - extract all fields except entity_label, entity_urn, entity_params, version, timestamp_ms
        params = {
            "entity_label": request.entity_label,
            "entity_urn": request.entity_urn
        }
        
        # Add all aspect-specific fields to payload
        aspect_config = factory.registry.get('aspects', {}).get('columnProperties', {})
        aspect_properties = aspect_config.get('properties', [])
        
        payload = {}
        for prop in aspect_properties:
            if hasattr(request, prop) and getattr(request, prop) is not None:
                payload[prop] = getattr(request, prop)
        
        params["payload"] = payload
        
        # Add optional parameters - only for versioned aspects
        aspect_config = factory.registry.get('aspects', {}).get('columnProperties', {})
        aspect_type = aspect_config.get('type', 'versioned')
        
        if aspect_type == 'versioned' and hasattr(request, 'version') and request.version is not None:
            params["version"] = request.version
        
        # Add entity creation parameters
        entity_params = request.entity_params
        if entity_params:
            params.update(entity_params)
        
        # Call the generated method
        result = method(**params)
        
        return models.ColumnpropertiesAspectResponse(
            entity_label=request.entity_label or "unknown",
            entity_urn=request.entity_urn or "unknown",
            aspect_name="columnProperties",
            payload=payload,
            version=request.version if aspect_type == 'versioned' and hasattr(request, 'version') else None
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/aspects/columnTransformation", response_model=models.ColumntransformationAspectResponse)
async def upsert_columnTransformation_aspect(request: models.ColumntransformationAspectUpsertRequest):
    """Upsert columnTransformation aspect"""
    try:
        factory = factory_wrapper.get_factory_instance()
        writer = factory_wrapper.get_writer_instance()
        
        method_name = "upsert_columntransformation_aspect"
        if not hasattr(writer, method_name):
            raise HTTPException(status_code=400, detail=f"Aspect 'columnTransformation' not found")
        
        method = getattr(writer, method_name)
        
        # Prepare parameters - extract all fields except entity_label, entity_urn, entity_params, version, timestamp_ms
        params = {
            "entity_label": request.entity_label,
            "entity_urn": request.entity_urn
        }
        
        # Add all aspect-specific fields to payload
        aspect_config = factory.registry.get('aspects', {}).get('columnTransformation', {})
        aspect_properties = aspect_config.get('properties', [])
        
        payload = {}
        for prop in aspect_properties:
            if hasattr(request, prop) and getattr(request, prop) is not None:
                payload[prop] = getattr(request, prop)
        
        params["payload"] = payload
        
        # Add optional parameters - only for versioned aspects
        aspect_config = factory.registry.get('aspects', {}).get('columnTransformation', {})
        aspect_type = aspect_config.get('type', 'versioned')
        
        if aspect_type == 'versioned' and hasattr(request, 'version') and request.version is not None:
            params["version"] = request.version
        
        # Add entity creation parameters
        entity_params = request.entity_params
        if entity_params:
            params.update(entity_params)
        
        # Call the generated method
        result = method(**params)
        
        return models.ColumntransformationAspectResponse(
            entity_label=request.entity_label or "unknown",
            entity_urn=request.entity_urn or "unknown",
            aspect_name="columnTransformation",
            payload=payload,
            version=request.version if aspect_type == 'versioned' and hasattr(request, 'version') else None
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/aspects/corpGroupInfo", response_model=models.CorpgroupinfoAspectResponse)
async def upsert_corpGroupInfo_aspect(request: models.CorpgroupinfoAspectUpsertRequest):
    """Upsert corpGroupInfo aspect"""
    try:
        factory = factory_wrapper.get_factory_instance()
        writer = factory_wrapper.get_writer_instance()
        
        method_name = "upsert_corpgroupinfo_aspect"
        if not hasattr(writer, method_name):
            raise HTTPException(status_code=400, detail=f"Aspect 'corpGroupInfo' not found")
        
        method = getattr(writer, method_name)
        
        # Prepare parameters - extract all fields except entity_label, entity_urn, entity_params, version, timestamp_ms
        params = {
            "entity_label": request.entity_label,
            "entity_urn": request.entity_urn
        }
        
        # Add all aspect-specific fields to payload
        aspect_config = factory.registry.get('aspects', {}).get('corpGroupInfo', {})
        aspect_properties = aspect_config.get('properties', [])
        
        payload = {}
        for prop in aspect_properties:
            if hasattr(request, prop) and getattr(request, prop) is not None:
                payload[prop] = getattr(request, prop)
        
        params["payload"] = payload
        
        # Add optional parameters - only for versioned aspects
        aspect_config = factory.registry.get('aspects', {}).get('corpGroupInfo', {})
        aspect_type = aspect_config.get('type', 'versioned')
        
        if aspect_type == 'versioned' and hasattr(request, 'version') and request.version is not None:
            params["version"] = request.version
        
        # Add entity creation parameters
        entity_params = request.entity_params
        if entity_params:
            params.update(entity_params)
        
        # Call the generated method
        result = method(**params)
        
        return models.CorpgroupinfoAspectResponse(
            entity_label=request.entity_label or "unknown",
            entity_urn=request.entity_urn or "unknown",
            aspect_name="corpGroupInfo",
            payload=payload,
            version=request.version if aspect_type == 'versioned' and hasattr(request, 'version') else None
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/aspects/corpUserInfo", response_model=models.CorpuserinfoAspectResponse)
async def upsert_corpUserInfo_aspect(request: models.CorpuserinfoAspectUpsertRequest):
    """Upsert corpUserInfo aspect"""
    try:
        factory = factory_wrapper.get_factory_instance()
        writer = factory_wrapper.get_writer_instance()
        
        method_name = "upsert_corpuserinfo_aspect"
        if not hasattr(writer, method_name):
            raise HTTPException(status_code=400, detail=f"Aspect 'corpUserInfo' not found")
        
        method = getattr(writer, method_name)
        
        # Prepare parameters - extract all fields except entity_label, entity_urn, entity_params, version, timestamp_ms
        params = {
            "entity_label": request.entity_label,
            "entity_urn": request.entity_urn
        }
        
        # Add all aspect-specific fields to payload
        aspect_config = factory.registry.get('aspects', {}).get('corpUserInfo', {})
        aspect_properties = aspect_config.get('properties', [])
        
        payload = {}
        for prop in aspect_properties:
            if hasattr(request, prop) and getattr(request, prop) is not None:
                payload[prop] = getattr(request, prop)
        
        params["payload"] = payload
        
        # Add optional parameters - only for versioned aspects
        aspect_config = factory.registry.get('aspects', {}).get('corpUserInfo', {})
        aspect_type = aspect_config.get('type', 'versioned')
        
        if aspect_type == 'versioned' and hasattr(request, 'version') and request.version is not None:
            params["version"] = request.version
        
        # Add entity creation parameters
        entity_params = request.entity_params
        if entity_params:
            params.update(entity_params)
        
        # Call the generated method
        result = method(**params)
        
        return models.CorpuserinfoAspectResponse(
            entity_label=request.entity_label or "unknown",
            entity_urn=request.entity_urn or "unknown",
            aspect_name="corpUserInfo",
            payload=payload,
            version=request.version if aspect_type == 'versioned' and hasattr(request, 'version') else None
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/aspects/datasetProfile", response_model=models.DatasetprofileAspectResponse)
async def upsert_datasetProfile_aspect(request: models.DatasetprofileAspectUpsertRequest):
    """Upsert datasetProfile aspect"""
    try:
        factory = factory_wrapper.get_factory_instance()
        writer = factory_wrapper.get_writer_instance()
        
        method_name = "upsert_datasetprofile_aspect"
        if not hasattr(writer, method_name):
            raise HTTPException(status_code=400, detail=f"Aspect 'datasetProfile' not found")
        
        method = getattr(writer, method_name)
        
        # Prepare parameters - extract all fields except entity_label, entity_urn, entity_params, version, timestamp_ms
        params = {
            "entity_label": request.entity_label,
            "entity_urn": request.entity_urn
        }
        
        # Add all aspect-specific fields to payload
        aspect_config = factory.registry.get('aspects', {}).get('datasetProfile', {})
        aspect_properties = aspect_config.get('properties', [])
        
        payload = {}
        for prop in aspect_properties:
            if hasattr(request, prop) and getattr(request, prop) is not None:
                payload[prop] = getattr(request, prop)
        
        params["payload"] = payload
        
        # Add optional parameters - only for versioned aspects
        aspect_config = factory.registry.get('aspects', {}).get('datasetProfile', {})
        aspect_type = aspect_config.get('type', 'versioned')
        
        if aspect_type == 'versioned' and hasattr(request, 'version') and request.version is not None:
            params["version"] = request.version
        
        # Add entity creation parameters
        entity_params = request.entity_params
        if entity_params:
            params.update(entity_params)
        
        # Call the generated method
        result = method(**params)
        
        return models.DatasetprofileAspectResponse(
            entity_label=request.entity_label or "unknown",
            entity_urn=request.entity_urn or "unknown",
            aspect_name="datasetProfile",
            payload=payload,
            version=request.version if aspect_type == 'versioned' and hasattr(request, 'version') else None
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/aspects/datasetProperties", response_model=models.DatasetpropertiesAspectResponse)
async def upsert_datasetProperties_aspect(request: models.DatasetpropertiesAspectUpsertRequest):
    """Upsert datasetProperties aspect"""
    try:
        factory = factory_wrapper.get_factory_instance()
        writer = factory_wrapper.get_writer_instance()
        
        method_name = "upsert_datasetproperties_aspect"
        if not hasattr(writer, method_name):
            raise HTTPException(status_code=400, detail=f"Aspect 'datasetProperties' not found")
        
        method = getattr(writer, method_name)
        
        # Prepare parameters - extract all fields except entity_label, entity_urn, entity_params, version, timestamp_ms
        params = {
            "entity_label": request.entity_label,
            "entity_urn": request.entity_urn
        }
        
        # Add all aspect-specific fields to payload
        aspect_config = factory.registry.get('aspects', {}).get('datasetProperties', {})
        aspect_properties = aspect_config.get('properties', [])
        
        payload = {}
        for prop in aspect_properties:
            if hasattr(request, prop) and getattr(request, prop) is not None:
                payload[prop] = getattr(request, prop)
        
        params["payload"] = payload
        
        # Add optional parameters - only for versioned aspects
        aspect_config = factory.registry.get('aspects', {}).get('datasetProperties', {})
        aspect_type = aspect_config.get('type', 'versioned')
        
        if aspect_type == 'versioned' and hasattr(request, 'version') and request.version is not None:
            params["version"] = request.version
        
        # Add entity creation parameters
        entity_params = request.entity_params
        if entity_params:
            params.update(entity_params)
        
        # Call the generated method
        result = method(**params)
        
        return models.DatasetpropertiesAspectResponse(
            entity_label=request.entity_label or "unknown",
            entity_urn=request.entity_urn or "unknown",
            aspect_name="datasetProperties",
            payload=payload,
            version=request.version if aspect_type == 'versioned' and hasattr(request, 'version') else None
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/aspects/datasetTransformation", response_model=models.DatasettransformationAspectResponse)
async def upsert_datasetTransformation_aspect(request: models.DatasettransformationAspectUpsertRequest):
    """Upsert datasetTransformation aspect"""
    try:
        factory = factory_wrapper.get_factory_instance()
        writer = factory_wrapper.get_writer_instance()
        
        method_name = "upsert_datasettransformation_aspect"
        if not hasattr(writer, method_name):
            raise HTTPException(status_code=400, detail=f"Aspect 'datasetTransformation' not found")
        
        method = getattr(writer, method_name)
        
        # Prepare parameters - extract all fields except entity_label, entity_urn, entity_params, version, timestamp_ms
        params = {
            "entity_label": request.entity_label,
            "entity_urn": request.entity_urn
        }
        
        # Add all aspect-specific fields to payload
        aspect_config = factory.registry.get('aspects', {}).get('datasetTransformation', {})
        aspect_properties = aspect_config.get('properties', [])
        
        payload = {}
        for prop in aspect_properties:
            if hasattr(request, prop) and getattr(request, prop) is not None:
                payload[prop] = getattr(request, prop)
        
        params["payload"] = payload
        
        # Add optional parameters - only for versioned aspects
        aspect_config = factory.registry.get('aspects', {}).get('datasetTransformation', {})
        aspect_type = aspect_config.get('type', 'versioned')
        
        if aspect_type == 'versioned' and hasattr(request, 'version') and request.version is not None:
            params["version"] = request.version
        
        # Add entity creation parameters
        entity_params = request.entity_params
        if entity_params:
            params.update(entity_params)
        
        # Call the generated method
        result = method(**params)
        
        return models.DatasettransformationAspectResponse(
            entity_label=request.entity_label or "unknown",
            entity_urn=request.entity_urn or "unknown",
            aspect_name="datasetTransformation",
            payload=payload,
            version=request.version if aspect_type == 'versioned' and hasattr(request, 'version') else None
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/aspects/globalTags", response_model=models.GlobaltagsAspectResponse)
async def upsert_globalTags_aspect(request: models.GlobaltagsAspectUpsertRequest):
    """Upsert globalTags aspect"""
    try:
        factory = factory_wrapper.get_factory_instance()
        writer = factory_wrapper.get_writer_instance()
        
        method_name = "upsert_globaltags_aspect"
        if not hasattr(writer, method_name):
            raise HTTPException(status_code=400, detail=f"Aspect 'globalTags' not found")
        
        method = getattr(writer, method_name)
        
        # Prepare parameters - extract all fields except entity_label, entity_urn, entity_params, version, timestamp_ms
        params = {
            "entity_label": request.entity_label,
            "entity_urn": request.entity_urn
        }
        
        # Add all aspect-specific fields to payload
        aspect_config = factory.registry.get('aspects', {}).get('globalTags', {})
        aspect_properties = aspect_config.get('properties', [])
        
        payload = {}
        for prop in aspect_properties:
            if hasattr(request, prop) and getattr(request, prop) is not None:
                payload[prop] = getattr(request, prop)
        
        params["payload"] = payload
        
        # Add optional parameters - only for versioned aspects
        aspect_config = factory.registry.get('aspects', {}).get('globalTags', {})
        aspect_type = aspect_config.get('type', 'versioned')
        
        if aspect_type == 'versioned' and hasattr(request, 'version') and request.version is not None:
            params["version"] = request.version
        
        # Add entity creation parameters
        entity_params = request.entity_params
        if entity_params:
            params.update(entity_params)
        
        # Call the generated method
        result = method(**params)
        
        return models.GlobaltagsAspectResponse(
            entity_label=request.entity_label or "unknown",
            entity_urn=request.entity_urn or "unknown",
            aspect_name="globalTags",
            payload=payload,
            version=request.version if aspect_type == 'versioned' and hasattr(request, 'version') else None
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/aspects/ownership", response_model=models.OwnershipAspectResponse)
async def upsert_ownership_aspect(request: models.OwnershipAspectUpsertRequest):
    """Upsert ownership aspect"""
    try:
        factory = factory_wrapper.get_factory_instance()
        writer = factory_wrapper.get_writer_instance()
        
        method_name = "upsert_ownership_aspect"
        if not hasattr(writer, method_name):
            raise HTTPException(status_code=400, detail=f"Aspect 'ownership' not found")
        
        method = getattr(writer, method_name)
        
        # Prepare parameters - extract all fields except entity_label, entity_urn, entity_params, version, timestamp_ms
        params = {
            "entity_label": request.entity_label,
            "entity_urn": request.entity_urn
        }
        
        # Add all aspect-specific fields to payload
        aspect_config = factory.registry.get('aspects', {}).get('ownership', {})
        aspect_properties = aspect_config.get('properties', [])
        
        payload = {}
        for prop in aspect_properties:
            if hasattr(request, prop) and getattr(request, prop) is not None:
                payload[prop] = getattr(request, prop)
        
        params["payload"] = payload
        
        # Add optional parameters - only for versioned aspects
        aspect_config = factory.registry.get('aspects', {}).get('ownership', {})
        aspect_type = aspect_config.get('type', 'versioned')
        
        if aspect_type == 'versioned' and hasattr(request, 'version') and request.version is not None:
            params["version"] = request.version
        
        # Add entity creation parameters
        entity_params = request.entity_params
        if entity_params:
            params.update(entity_params)
        
        # Call the generated method
        result = method(**params)
        
        return models.OwnershipAspectResponse(
            entity_label=request.entity_label or "unknown",
            entity_urn=request.entity_urn or "unknown",
            aspect_name="ownership",
            payload=payload,
            version=request.version if aspect_type == 'versioned' and hasattr(request, 'version') else None
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/aspects/schemaMetadata", response_model=models.SchemametadataAspectResponse)
async def upsert_schemaMetadata_aspect(request: models.SchemametadataAspectUpsertRequest):
    """Upsert schemaMetadata aspect"""
    try:
        factory = factory_wrapper.get_factory_instance()
        writer = factory_wrapper.get_writer_instance()
        
        method_name = "upsert_schemametadata_aspect"
        if not hasattr(writer, method_name):
            raise HTTPException(status_code=400, detail=f"Aspect 'schemaMetadata' not found")
        
        method = getattr(writer, method_name)
        
        # Prepare parameters - extract all fields except entity_label, entity_urn, entity_params, version, timestamp_ms
        params = {
            "entity_label": request.entity_label,
            "entity_urn": request.entity_urn
        }
        
        # Add all aspect-specific fields to payload
        aspect_config = factory.registry.get('aspects', {}).get('schemaMetadata', {})
        aspect_properties = aspect_config.get('properties', [])
        
        payload = {}
        for prop in aspect_properties:
            if hasattr(request, prop) and getattr(request, prop) is not None:
                payload[prop] = getattr(request, prop)
        
        params["payload"] = payload
        
        # Add optional parameters - only for versioned aspects
        aspect_config = factory.registry.get('aspects', {}).get('schemaMetadata', {})
        aspect_type = aspect_config.get('type', 'versioned')
        
        if aspect_type == 'versioned' and hasattr(request, 'version') and request.version is not None:
            params["version"] = request.version
        
        # Add entity creation parameters
        entity_params = request.entity_params
        if entity_params:
            params.update(entity_params)
        
        # Call the generated method
        result = method(**params)
        
        return models.SchemametadataAspectResponse(
            entity_label=request.entity_label or "unknown",
            entity_urn=request.entity_urn or "unknown",
            aspect_name="schemaMetadata",
            payload=payload,
            version=request.version if aspect_type == 'versioned' and hasattr(request, 'version') else None
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
