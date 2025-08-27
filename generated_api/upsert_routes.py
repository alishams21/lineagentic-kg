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

@router.post("/entities/DataFlow", response_model=models.DataFlowResponse)
async def upsert_DataFlow(request: models.DataFlowUpsertRequest):
    """Upsert DataFlow entity"""
    try:
        factory = factory_wrapper.get_factory_instance()
        writer = factory_wrapper.get_writer_instance()
        
        method_name = "upsert_dataflow"
        if not hasattr(writer, method_name):
            raise HTTPException(status_code=400, detail=f"Entity type 'DataFlow' not found")
        
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
        get_method_name = "get_dataflow"
        get_method = getattr(writer, get_method_name)
        entity_data = get_method(result_urn)
        
        return models.DataFlowResponse(
            urn=result_urn,
            properties=entity_data or {},
            last_updated=entity_data.get('lastUpdated') if entity_data else None
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/entities/DataJob", response_model=models.DataJobResponse)
async def upsert_DataJob(request: models.DataJobUpsertRequest):
    """Upsert DataJob entity"""
    try:
        factory = factory_wrapper.get_factory_instance()
        writer = factory_wrapper.get_writer_instance()
        
        method_name = "upsert_datajob"
        if not hasattr(writer, method_name):
            raise HTTPException(status_code=400, detail=f"Entity type 'DataJob' not found")
        
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
        get_method_name = "get_datajob"
        get_method = getattr(writer, get_method_name)
        entity_data = get_method(result_urn)
        
        return models.DataJobResponse(
            urn=result_urn,
            properties=entity_data or {},
            last_updated=entity_data.get('lastUpdated') if entity_data else None
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/entities/DataProduct", response_model=models.DataProductResponse)
async def upsert_DataProduct(request: models.DataProductUpsertRequest):
    """Upsert DataProduct entity"""
    try:
        factory = factory_wrapper.get_factory_instance()
        writer = factory_wrapper.get_writer_instance()
        
        method_name = "upsert_dataproduct"
        if not hasattr(writer, method_name):
            raise HTTPException(status_code=400, detail=f"Entity type 'DataProduct' not found")
        
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
        get_method_name = "get_dataproduct"
        get_method = getattr(writer, get_method_name)
        entity_data = get_method(result_urn)
        
        return models.DataProductResponse(
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

@router.post("/aspects/dataFlowInfo", response_model=models.DataflowinfoAspectResponse)
async def upsert_dataFlowInfo_aspect(request: models.DataflowinfoAspectUpsertRequest):
    """Upsert dataFlowInfo aspect"""
    try:
        factory = factory_wrapper.get_factory_instance()
        writer = factory_wrapper.get_writer_instance()
        
        method_name = "upsert_dataflowinfo_aspect"
        if not hasattr(writer, method_name):
            raise HTTPException(status_code=400, detail=f"Aspect 'dataFlowInfo' not found")
        
        method = getattr(writer, method_name)
        
        # Prepare parameters - extract all fields except entity_label, entity_urn, entity_params, version, timestamp_ms
        params = {
            "entity_label": request.entity_label,
            "entity_urn": request.entity_urn
        }
        
        # Add all aspect-specific fields to payload
        aspect_config = factory.registry.get('aspects', {}).get('dataFlowInfo', {})
        aspect_properties = aspect_config.get('properties', [])
        
        payload = {}
        for prop in aspect_properties:
            if hasattr(request, prop) and getattr(request, prop) is not None:
                payload[prop] = getattr(request, prop)
        
        params["payload"] = payload
        
        # Add optional parameters - only for versioned aspects
        aspect_config = factory.registry.get('aspects', {}).get('dataFlowInfo', {})
        aspect_type = aspect_config.get('type', 'versioned')
        
        if aspect_type == 'versioned' and hasattr(request, 'version') and request.version is not None:
            params["version"] = request.version
        
        # Add entity creation parameters
        entity_params = request.entity_params
        if entity_params:
            params.update(entity_params)
        
        # Call the generated method
        result = method(**params)
        
        return models.DataflowinfoAspectResponse(
            entity_label=request.entity_label or "unknown",
            entity_urn=request.entity_urn or "unknown",
            aspect_name="dataFlowInfo",
            payload=payload,
            version=request.version if aspect_type == 'versioned' and hasattr(request, 'version') else None
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/aspects/dataJobInfo", response_model=models.DatajobinfoAspectResponse)
async def upsert_dataJobInfo_aspect(request: models.DatajobinfoAspectUpsertRequest):
    """Upsert dataJobInfo aspect"""
    try:
        factory = factory_wrapper.get_factory_instance()
        writer = factory_wrapper.get_writer_instance()
        
        method_name = "upsert_datajobinfo_aspect"
        if not hasattr(writer, method_name):
            raise HTTPException(status_code=400, detail=f"Aspect 'dataJobInfo' not found")
        
        method = getattr(writer, method_name)
        
        # Prepare parameters - extract all fields except entity_label, entity_urn, entity_params, version, timestamp_ms
        params = {
            "entity_label": request.entity_label,
            "entity_urn": request.entity_urn
        }
        
        # Add all aspect-specific fields to payload
        aspect_config = factory.registry.get('aspects', {}).get('dataJobInfo', {})
        aspect_properties = aspect_config.get('properties', [])
        
        payload = {}
        for prop in aspect_properties:
            if hasattr(request, prop) and getattr(request, prop) is not None:
                payload[prop] = getattr(request, prop)
        
        params["payload"] = payload
        
        # Add optional parameters - only for versioned aspects
        aspect_config = factory.registry.get('aspects', {}).get('dataJobInfo', {})
        aspect_type = aspect_config.get('type', 'versioned')
        
        if aspect_type == 'versioned' and hasattr(request, 'version') and request.version is not None:
            params["version"] = request.version
        
        # Add entity creation parameters
        entity_params = request.entity_params
        if entity_params:
            params.update(entity_params)
        
        # Call the generated method
        result = method(**params)
        
        return models.DatajobinfoAspectResponse(
            entity_label=request.entity_label or "unknown",
            entity_urn=request.entity_urn or "unknown",
            aspect_name="dataJobInfo",
            payload=payload,
            version=request.version if aspect_type == 'versioned' and hasattr(request, 'version') else None
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/aspects/dataJobInputOutput", response_model=models.DatajobinputoutputAspectResponse)
async def upsert_dataJobInputOutput_aspect(request: models.DatajobinputoutputAspectUpsertRequest):
    """Upsert dataJobInputOutput aspect"""
    try:
        factory = factory_wrapper.get_factory_instance()
        writer = factory_wrapper.get_writer_instance()
        
        method_name = "upsert_datajobinputoutput_aspect"
        if not hasattr(writer, method_name):
            raise HTTPException(status_code=400, detail=f"Aspect 'dataJobInputOutput' not found")
        
        method = getattr(writer, method_name)
        
        # Prepare parameters - extract all fields except entity_label, entity_urn, entity_params, version, timestamp_ms
        params = {
            "entity_label": request.entity_label,
            "entity_urn": request.entity_urn
        }
        
        # Add all aspect-specific fields to payload
        aspect_config = factory.registry.get('aspects', {}).get('dataJobInputOutput', {})
        aspect_properties = aspect_config.get('properties', [])
        
        payload = {}
        for prop in aspect_properties:
            if hasattr(request, prop) and getattr(request, prop) is not None:
                payload[prop] = getattr(request, prop)
        
        params["payload"] = payload
        
        # Add optional parameters - only for versioned aspects
        aspect_config = factory.registry.get('aspects', {}).get('dataJobInputOutput', {})
        aspect_type = aspect_config.get('type', 'versioned')
        
        if aspect_type == 'versioned' and hasattr(request, 'version') and request.version is not None:
            params["version"] = request.version
        
        # Add entity creation parameters
        entity_params = request.entity_params
        if entity_params:
            params.update(entity_params)
        
        # Call the generated method
        result = method(**params)
        
        return models.DatajobinputoutputAspectResponse(
            entity_label=request.entity_label or "unknown",
            entity_urn=request.entity_urn or "unknown",
            aspect_name="dataJobInputOutput",
            payload=payload,
            version=request.version if aspect_type == 'versioned' and hasattr(request, 'version') else None
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/aspects/dataJobRun", response_model=models.DatajobrunAspectResponse)
async def upsert_dataJobRun_aspect(request: models.DatajobrunAspectUpsertRequest):
    """Upsert dataJobRun aspect"""
    try:
        factory = factory_wrapper.get_factory_instance()
        writer = factory_wrapper.get_writer_instance()
        
        method_name = "upsert_datajobrun_aspect"
        if not hasattr(writer, method_name):
            raise HTTPException(status_code=400, detail=f"Aspect 'dataJobRun' not found")
        
        method = getattr(writer, method_name)
        
        # Prepare parameters - extract all fields except entity_label, entity_urn, entity_params, version, timestamp_ms
        params = {
            "entity_label": request.entity_label,
            "entity_urn": request.entity_urn
        }
        
        # Add all aspect-specific fields to payload
        aspect_config = factory.registry.get('aspects', {}).get('dataJobRun', {})
        aspect_properties = aspect_config.get('properties', [])
        
        payload = {}
        for prop in aspect_properties:
            if hasattr(request, prop) and getattr(request, prop) is not None:
                payload[prop] = getattr(request, prop)
        
        params["payload"] = payload
        
        # Add optional parameters - only for versioned aspects
        aspect_config = factory.registry.get('aspects', {}).get('dataJobRun', {})
        aspect_type = aspect_config.get('type', 'versioned')
        
        if aspect_type == 'versioned' and hasattr(request, 'version') and request.version is not None:
            params["version"] = request.version
        
        # Add entity creation parameters
        entity_params = request.entity_params
        if entity_params:
            params.update(entity_params)
        
        # Call the generated method
        result = method(**params)
        
        return models.DatajobrunAspectResponse(
            entity_label=request.entity_label or "unknown",
            entity_urn=request.entity_urn or "unknown",
            aspect_name="dataJobRun",
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

@router.post("/aspects/documentation", response_model=models.DocumentationAspectResponse)
async def upsert_documentation_aspect(request: models.DocumentationAspectUpsertRequest):
    """Upsert documentation aspect"""
    try:
        factory = factory_wrapper.get_factory_instance()
        writer = factory_wrapper.get_writer_instance()
        
        method_name = "upsert_documentation_aspect"
        if not hasattr(writer, method_name):
            raise HTTPException(status_code=400, detail=f"Aspect 'documentation' not found")
        
        method = getattr(writer, method_name)
        
        # Prepare parameters - extract all fields except entity_label, entity_urn, entity_params, version, timestamp_ms
        params = {
            "entity_label": request.entity_label,
            "entity_urn": request.entity_urn
        }
        
        # Add all aspect-specific fields to payload
        aspect_config = factory.registry.get('aspects', {}).get('documentation', {})
        aspect_properties = aspect_config.get('properties', [])
        
        payload = {}
        for prop in aspect_properties:
            if hasattr(request, prop) and getattr(request, prop) is not None:
                payload[prop] = getattr(request, prop)
        
        params["payload"] = payload
        
        # Add optional parameters - only for versioned aspects
        aspect_config = factory.registry.get('aspects', {}).get('documentation', {})
        aspect_type = aspect_config.get('type', 'versioned')
        
        if aspect_type == 'versioned' and hasattr(request, 'version') and request.version is not None:
            params["version"] = request.version
        
        # Add entity creation parameters
        entity_params = request.entity_params
        if entity_params:
            params.update(entity_params)
        
        # Call the generated method
        result = method(**params)
        
        return models.DocumentationAspectResponse(
            entity_label=request.entity_label or "unknown",
            entity_urn=request.entity_urn or "unknown",
            aspect_name="documentation",
            payload=payload,
            version=request.version if aspect_type == 'versioned' and hasattr(request, 'version') else None
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/aspects/dpContract", response_model=models.DpcontractAspectResponse)
async def upsert_dpContract_aspect(request: models.DpcontractAspectUpsertRequest):
    """Upsert dpContract aspect"""
    try:
        factory = factory_wrapper.get_factory_instance()
        writer = factory_wrapper.get_writer_instance()
        
        method_name = "upsert_dpcontract_aspect"
        if not hasattr(writer, method_name):
            raise HTTPException(status_code=400, detail=f"Aspect 'dpContract' not found")
        
        method = getattr(writer, method_name)
        
        # Prepare parameters - extract all fields except entity_label, entity_urn, entity_params, version, timestamp_ms
        params = {
            "entity_label": request.entity_label,
            "entity_urn": request.entity_urn
        }
        
        # Add all aspect-specific fields to payload
        aspect_config = factory.registry.get('aspects', {}).get('dpContract', {})
        aspect_properties = aspect_config.get('properties', [])
        
        payload = {}
        for prop in aspect_properties:
            if hasattr(request, prop) and getattr(request, prop) is not None:
                payload[prop] = getattr(request, prop)
        
        params["payload"] = payload
        
        # Add optional parameters - only for versioned aspects
        aspect_config = factory.registry.get('aspects', {}).get('dpContract', {})
        aspect_type = aspect_config.get('type', 'versioned')
        
        if aspect_type == 'versioned' and hasattr(request, 'version') and request.version is not None:
            params["version"] = request.version
        
        # Add entity creation parameters
        entity_params = request.entity_params
        if entity_params:
            params.update(entity_params)
        
        # Call the generated method
        result = method(**params)
        
        return models.DpcontractAspectResponse(
            entity_label=request.entity_label or "unknown",
            entity_urn=request.entity_urn or "unknown",
            aspect_name="dpContract",
            payload=payload,
            version=request.version if aspect_type == 'versioned' and hasattr(request, 'version') else None
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/aspects/dpObservability", response_model=models.DpobservabilityAspectResponse)
async def upsert_dpObservability_aspect(request: models.DpobservabilityAspectUpsertRequest):
    """Upsert dpObservability aspect"""
    try:
        factory = factory_wrapper.get_factory_instance()
        writer = factory_wrapper.get_writer_instance()
        
        method_name = "upsert_dpobservability_aspect"
        if not hasattr(writer, method_name):
            raise HTTPException(status_code=400, detail=f"Aspect 'dpObservability' not found")
        
        method = getattr(writer, method_name)
        
        # Prepare parameters - extract all fields except entity_label, entity_urn, entity_params, version, timestamp_ms
        params = {
            "entity_label": request.entity_label,
            "entity_urn": request.entity_urn
        }
        
        # Add all aspect-specific fields to payload
        aspect_config = factory.registry.get('aspects', {}).get('dpObservability', {})
        aspect_properties = aspect_config.get('properties', [])
        
        payload = {}
        for prop in aspect_properties:
            if hasattr(request, prop) and getattr(request, prop) is not None:
                payload[prop] = getattr(request, prop)
        
        params["payload"] = payload
        
        # Add optional parameters - only for versioned aspects
        aspect_config = factory.registry.get('aspects', {}).get('dpObservability', {})
        aspect_type = aspect_config.get('type', 'versioned')
        
        if aspect_type == 'versioned' and hasattr(request, 'version') and request.version is not None:
            params["version"] = request.version
        
        # Add entity creation parameters
        entity_params = request.entity_params
        if entity_params:
            params.update(entity_params)
        
        # Call the generated method
        result = method(**params)
        
        return models.DpobservabilityAspectResponse(
            entity_label=request.entity_label or "unknown",
            entity_urn=request.entity_urn or "unknown",
            aspect_name="dpObservability",
            payload=payload,
            version=request.version if aspect_type == 'versioned' and hasattr(request, 'version') else None
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/aspects/dpPolicy", response_model=models.DppolicyAspectResponse)
async def upsert_dpPolicy_aspect(request: models.DppolicyAspectUpsertRequest):
    """Upsert dpPolicy aspect"""
    try:
        factory = factory_wrapper.get_factory_instance()
        writer = factory_wrapper.get_writer_instance()
        
        method_name = "upsert_dppolicy_aspect"
        if not hasattr(writer, method_name):
            raise HTTPException(status_code=400, detail=f"Aspect 'dpPolicy' not found")
        
        method = getattr(writer, method_name)
        
        # Prepare parameters - extract all fields except entity_label, entity_urn, entity_params, version, timestamp_ms
        params = {
            "entity_label": request.entity_label,
            "entity_urn": request.entity_urn
        }
        
        # Add all aspect-specific fields to payload
        aspect_config = factory.registry.get('aspects', {}).get('dpPolicy', {})
        aspect_properties = aspect_config.get('properties', [])
        
        payload = {}
        for prop in aspect_properties:
            if hasattr(request, prop) and getattr(request, prop) is not None:
                payload[prop] = getattr(request, prop)
        
        params["payload"] = payload
        
        # Add optional parameters - only for versioned aspects
        aspect_config = factory.registry.get('aspects', {}).get('dpPolicy', {})
        aspect_type = aspect_config.get('type', 'versioned')
        
        if aspect_type == 'versioned' and hasattr(request, 'version') and request.version is not None:
            params["version"] = request.version
        
        # Add entity creation parameters
        entity_params = request.entity_params
        if entity_params:
            params.update(entity_params)
        
        # Call the generated method
        result = method(**params)
        
        return models.DppolicyAspectResponse(
            entity_label=request.entity_label or "unknown",
            entity_urn=request.entity_urn or "unknown",
            aspect_name="dpPolicy",
            payload=payload,
            version=request.version if aspect_type == 'versioned' and hasattr(request, 'version') else None
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/aspects/dpProvisioner", response_model=models.DpprovisionerAspectResponse)
async def upsert_dpProvisioner_aspect(request: models.DpprovisionerAspectUpsertRequest):
    """Upsert dpProvisioner aspect"""
    try:
        factory = factory_wrapper.get_factory_instance()
        writer = factory_wrapper.get_writer_instance()
        
        method_name = "upsert_dpprovisioner_aspect"
        if not hasattr(writer, method_name):
            raise HTTPException(status_code=400, detail=f"Aspect 'dpProvisioner' not found")
        
        method = getattr(writer, method_name)
        
        # Prepare parameters - extract all fields except entity_label, entity_urn, entity_params, version, timestamp_ms
        params = {
            "entity_label": request.entity_label,
            "entity_urn": request.entity_urn
        }
        
        # Add all aspect-specific fields to payload
        aspect_config = factory.registry.get('aspects', {}).get('dpProvisioner', {})
        aspect_properties = aspect_config.get('properties', [])
        
        payload = {}
        for prop in aspect_properties:
            if hasattr(request, prop) and getattr(request, prop) is not None:
                payload[prop] = getattr(request, prop)
        
        params["payload"] = payload
        
        # Add optional parameters - only for versioned aspects
        aspect_config = factory.registry.get('aspects', {}).get('dpProvisioner', {})
        aspect_type = aspect_config.get('type', 'versioned')
        
        if aspect_type == 'versioned' and hasattr(request, 'version') and request.version is not None:
            params["version"] = request.version
        
        # Add entity creation parameters
        entity_params = request.entity_params
        if entity_params:
            params.update(entity_params)
        
        # Call the generated method
        result = method(**params)
        
        return models.DpprovisionerAspectResponse(
            entity_label=request.entity_label or "unknown",
            entity_urn=request.entity_urn or "unknown",
            aspect_name="dpProvisioner",
            payload=payload,
            version=request.version if aspect_type == 'versioned' and hasattr(request, 'version') else None
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/aspects/environmentProperties", response_model=models.EnvironmentpropertiesAspectResponse)
async def upsert_environmentProperties_aspect(request: models.EnvironmentpropertiesAspectUpsertRequest):
    """Upsert environmentProperties aspect"""
    try:
        factory = factory_wrapper.get_factory_instance()
        writer = factory_wrapper.get_writer_instance()
        
        method_name = "upsert_environmentproperties_aspect"
        if not hasattr(writer, method_name):
            raise HTTPException(status_code=400, detail=f"Aspect 'environmentProperties' not found")
        
        method = getattr(writer, method_name)
        
        # Prepare parameters - extract all fields except entity_label, entity_urn, entity_params, version, timestamp_ms
        params = {
            "entity_label": request.entity_label,
            "entity_urn": request.entity_urn
        }
        
        # Add all aspect-specific fields to payload
        aspect_config = factory.registry.get('aspects', {}).get('environmentProperties', {})
        aspect_properties = aspect_config.get('properties', [])
        
        payload = {}
        for prop in aspect_properties:
            if hasattr(request, prop) and getattr(request, prop) is not None:
                payload[prop] = getattr(request, prop)
        
        params["payload"] = payload
        
        # Add optional parameters - only for versioned aspects
        aspect_config = factory.registry.get('aspects', {}).get('environmentProperties', {})
        aspect_type = aspect_config.get('type', 'versioned')
        
        if aspect_type == 'versioned' and hasattr(request, 'version') and request.version is not None:
            params["version"] = request.version
        
        # Add entity creation parameters
        entity_params = request.entity_params
        if entity_params:
            params.update(entity_params)
        
        # Call the generated method
        result = method(**params)
        
        return models.EnvironmentpropertiesAspectResponse(
            entity_label=request.entity_label or "unknown",
            entity_urn=request.entity_urn or "unknown",
            aspect_name="environmentProperties",
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

@router.post("/aspects/sourceCode", response_model=models.SourcecodeAspectResponse)
async def upsert_sourceCode_aspect(request: models.SourcecodeAspectUpsertRequest):
    """Upsert sourceCode aspect"""
    try:
        factory = factory_wrapper.get_factory_instance()
        writer = factory_wrapper.get_writer_instance()
        
        method_name = "upsert_sourcecode_aspect"
        if not hasattr(writer, method_name):
            raise HTTPException(status_code=400, detail=f"Aspect 'sourceCode' not found")
        
        method = getattr(writer, method_name)
        
        # Prepare parameters - extract all fields except entity_label, entity_urn, entity_params, version, timestamp_ms
        params = {
            "entity_label": request.entity_label,
            "entity_urn": request.entity_urn
        }
        
        # Add all aspect-specific fields to payload
        aspect_config = factory.registry.get('aspects', {}).get('sourceCode', {})
        aspect_properties = aspect_config.get('properties', [])
        
        payload = {}
        for prop in aspect_properties:
            if hasattr(request, prop) and getattr(request, prop) is not None:
                payload[prop] = getattr(request, prop)
        
        params["payload"] = payload
        
        # Add optional parameters - only for versioned aspects
        aspect_config = factory.registry.get('aspects', {}).get('sourceCode', {})
        aspect_type = aspect_config.get('type', 'versioned')
        
        if aspect_type == 'versioned' and hasattr(request, 'version') and request.version is not None:
            params["version"] = request.version
        
        # Add entity creation parameters
        entity_params = request.entity_params
        if entity_params:
            params.update(entity_params)
        
        # Call the generated method
        result = method(**params)
        
        return models.SourcecodeAspectResponse(
            entity_label=request.entity_label or "unknown",
            entity_urn=request.entity_urn or "unknown",
            aspect_name="sourceCode",
            payload=payload,
            version=request.version if aspect_type == 'versioned' and hasattr(request, 'version') else None
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/aspects/sourceCodeLocation", response_model=models.SourcecodelocationAspectResponse)
async def upsert_sourceCodeLocation_aspect(request: models.SourcecodelocationAspectUpsertRequest):
    """Upsert sourceCodeLocation aspect"""
    try:
        factory = factory_wrapper.get_factory_instance()
        writer = factory_wrapper.get_writer_instance()
        
        method_name = "upsert_sourcecodelocation_aspect"
        if not hasattr(writer, method_name):
            raise HTTPException(status_code=400, detail=f"Aspect 'sourceCodeLocation' not found")
        
        method = getattr(writer, method_name)
        
        # Prepare parameters - extract all fields except entity_label, entity_urn, entity_params, version, timestamp_ms
        params = {
            "entity_label": request.entity_label,
            "entity_urn": request.entity_urn
        }
        
        # Add all aspect-specific fields to payload
        aspect_config = factory.registry.get('aspects', {}).get('sourceCodeLocation', {})
        aspect_properties = aspect_config.get('properties', [])
        
        payload = {}
        for prop in aspect_properties:
            if hasattr(request, prop) and getattr(request, prop) is not None:
                payload[prop] = getattr(request, prop)
        
        params["payload"] = payload
        
        # Add optional parameters - only for versioned aspects
        aspect_config = factory.registry.get('aspects', {}).get('sourceCodeLocation', {})
        aspect_type = aspect_config.get('type', 'versioned')
        
        if aspect_type == 'versioned' and hasattr(request, 'version') and request.version is not None:
            params["version"] = request.version
        
        # Add entity creation parameters
        entity_params = request.entity_params
        if entity_params:
            params.update(entity_params)
        
        # Call the generated method
        result = method(**params)
        
        return models.SourcecodelocationAspectResponse(
            entity_label=request.entity_label or "unknown",
            entity_urn=request.entity_urn or "unknown",
            aspect_name="sourceCodeLocation",
            payload=payload,
            version=request.version if aspect_type == 'versioned' and hasattr(request, 'version') else None
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/aspects/transformation", response_model=models.TransformationAspectResponse)
async def upsert_transformation_aspect(request: models.TransformationAspectUpsertRequest):
    """Upsert transformation aspect"""
    try:
        factory = factory_wrapper.get_factory_instance()
        writer = factory_wrapper.get_writer_instance()
        
        method_name = "upsert_transformation_aspect"
        if not hasattr(writer, method_name):
            raise HTTPException(status_code=400, detail=f"Aspect 'transformation' not found")
        
        method = getattr(writer, method_name)
        
        # Prepare parameters - extract all fields except entity_label, entity_urn, entity_params, version, timestamp_ms
        params = {
            "entity_label": request.entity_label,
            "entity_urn": request.entity_urn
        }
        
        # Add all aspect-specific fields to payload
        aspect_config = factory.registry.get('aspects', {}).get('transformation', {})
        aspect_properties = aspect_config.get('properties', [])
        
        payload = {}
        for prop in aspect_properties:
            if hasattr(request, prop) and getattr(request, prop) is not None:
                payload[prop] = getattr(request, prop)
        
        params["payload"] = payload
        
        # Add optional parameters - only for versioned aspects
        aspect_config = factory.registry.get('aspects', {}).get('transformation', {})
        aspect_type = aspect_config.get('type', 'versioned')
        
        if aspect_type == 'versioned' and hasattr(request, 'version') and request.version is not None:
            params["version"] = request.version
        
        # Add entity creation parameters
        entity_params = request.entity_params
        if entity_params:
            params.update(entity_params)
        
        # Call the generated method
        result = method(**params)
        
        return models.TransformationAspectResponse(
            entity_label=request.entity_label or "unknown",
            entity_urn=request.entity_urn or "unknown",
            aspect_name="transformation",
            payload=payload,
            version=request.version if aspect_type == 'versioned' and hasattr(request, 'version') else None
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
