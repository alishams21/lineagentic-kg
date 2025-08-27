#!/usr/bin/env python3
"""
GET routes for the generated API
"""

from fastapi import APIRouter, HTTPException
from typing import Any, Dict, List
import models
import factory_wrapper


router = APIRouter(prefix="/api/v1", tags=["GET Operations"])


# Health check
@router.get("/health", response_model=models.HealthResponse)
async def health_check():
    """Health check endpoint"""
    try:
        factory = factory_wrapper.get_factory_instance()
        return models.HealthResponse(
            status="healthy",
            registry_loaded=True,
            available_entities=list(factory.registry.get('entities', {}).keys()),
            available_aspects=list(factory.registry.get('aspects', {}).keys()),
            available_utilities=list(factory.registry.get('utility_functions', {}).keys())
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# Entity GET routes

@router.get("/entities/Column/{urn}", response_model=models.ColumnResponse)
async def get_Column(urn: str):
    """Get Column entity by URN"""
    try:
        factory = factory_wrapper.get_factory_instance()
        writer = factory_wrapper.get_writer_instance()
        
        method_name = "get_column"
        if not hasattr(writer, method_name):
            raise HTTPException(status_code=400, detail=f"Entity type 'Column' not found")
        
        method = getattr(writer, method_name)
        result = method(urn)
        
        if result is None:
            raise HTTPException(status_code=404, detail=f"Column with URN '{urn}' not found")
        
        return models.ColumnResponse(
            urn=urn,
            properties=result,
            last_updated=result.get('lastUpdated')
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/entities/CorpGroup/{urn}", response_model=models.CorpGroupResponse)
async def get_CorpGroup(urn: str):
    """Get CorpGroup entity by URN"""
    try:
        factory = factory_wrapper.get_factory_instance()
        writer = factory_wrapper.get_writer_instance()
        
        method_name = "get_corpgroup"
        if not hasattr(writer, method_name):
            raise HTTPException(status_code=400, detail=f"Entity type 'CorpGroup' not found")
        
        method = getattr(writer, method_name)
        result = method(urn)
        
        if result is None:
            raise HTTPException(status_code=404, detail=f"CorpGroup with URN '{urn}' not found")
        
        return models.CorpGroupResponse(
            urn=urn,
            properties=result,
            last_updated=result.get('lastUpdated')
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/entities/CorpUser/{urn}", response_model=models.CorpUserResponse)
async def get_CorpUser(urn: str):
    """Get CorpUser entity by URN"""
    try:
        factory = factory_wrapper.get_factory_instance()
        writer = factory_wrapper.get_writer_instance()
        
        method_name = "get_corpuser"
        if not hasattr(writer, method_name):
            raise HTTPException(status_code=400, detail=f"Entity type 'CorpUser' not found")
        
        method = getattr(writer, method_name)
        result = method(urn)
        
        if result is None:
            raise HTTPException(status_code=404, detail=f"CorpUser with URN '{urn}' not found")
        
        return models.CorpUserResponse(
            urn=urn,
            properties=result,
            last_updated=result.get('lastUpdated')
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/entities/DataFlow/{urn}", response_model=models.DataFlowResponse)
async def get_DataFlow(urn: str):
    """Get DataFlow entity by URN"""
    try:
        factory = factory_wrapper.get_factory_instance()
        writer = factory_wrapper.get_writer_instance()
        
        method_name = "get_dataflow"
        if not hasattr(writer, method_name):
            raise HTTPException(status_code=400, detail=f"Entity type 'DataFlow' not found")
        
        method = getattr(writer, method_name)
        result = method(urn)
        
        if result is None:
            raise HTTPException(status_code=404, detail=f"DataFlow with URN '{urn}' not found")
        
        return models.DataFlowResponse(
            urn=urn,
            properties=result,
            last_updated=result.get('lastUpdated')
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/entities/DataJob/{urn}", response_model=models.DataJobResponse)
async def get_DataJob(urn: str):
    """Get DataJob entity by URN"""
    try:
        factory = factory_wrapper.get_factory_instance()
        writer = factory_wrapper.get_writer_instance()
        
        method_name = "get_datajob"
        if not hasattr(writer, method_name):
            raise HTTPException(status_code=400, detail=f"Entity type 'DataJob' not found")
        
        method = getattr(writer, method_name)
        result = method(urn)
        
        if result is None:
            raise HTTPException(status_code=404, detail=f"DataJob with URN '{urn}' not found")
        
        return models.DataJobResponse(
            urn=urn,
            properties=result,
            last_updated=result.get('lastUpdated')
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/entities/DataProduct/{urn}", response_model=models.DataProductResponse)
async def get_DataProduct(urn: str):
    """Get DataProduct entity by URN"""
    try:
        factory = factory_wrapper.get_factory_instance()
        writer = factory_wrapper.get_writer_instance()
        
        method_name = "get_dataproduct"
        if not hasattr(writer, method_name):
            raise HTTPException(status_code=400, detail=f"Entity type 'DataProduct' not found")
        
        method = getattr(writer, method_name)
        result = method(urn)
        
        if result is None:
            raise HTTPException(status_code=404, detail=f"DataProduct with URN '{urn}' not found")
        
        return models.DataProductResponse(
            urn=urn,
            properties=result,
            last_updated=result.get('lastUpdated')
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/entities/Dataset/{urn}", response_model=models.DatasetResponse)
async def get_Dataset(urn: str):
    """Get Dataset entity by URN"""
    try:
        factory = factory_wrapper.get_factory_instance()
        writer = factory_wrapper.get_writer_instance()
        
        method_name = "get_dataset"
        if not hasattr(writer, method_name):
            raise HTTPException(status_code=400, detail=f"Entity type 'Dataset' not found")
        
        method = getattr(writer, method_name)
        result = method(urn)
        
        if result is None:
            raise HTTPException(status_code=404, detail=f"Dataset with URN '{urn}' not found")
        
        return models.DatasetResponse(
            urn=urn,
            properties=result,
            last_updated=result.get('lastUpdated')
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/entities/Tag/{urn}", response_model=models.TagResponse)
async def get_Tag(urn: str):
    """Get Tag entity by URN"""
    try:
        factory = factory_wrapper.get_factory_instance()
        writer = factory_wrapper.get_writer_instance()
        
        method_name = "get_tag"
        if not hasattr(writer, method_name):
            raise HTTPException(status_code=400, detail=f"Entity type 'Tag' not found")
        
        method = getattr(writer, method_name)
        result = method(urn)
        
        if result is None:
            raise HTTPException(status_code=404, detail=f"Tag with URN '{urn}' not found")
        
        return models.TagResponse(
            urn=urn,
            properties=result,
            last_updated=result.get('lastUpdated')
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/aspects/columnProperties/{entity_label}/{entity_urn}", response_model=models.ColumnpropertiesAspectResponse)
async def get_columnProperties_aspect(entity_label: str, entity_urn: str):
    """Get columnProperties aspect for entity"""
    try:
        factory = factory_wrapper.get_factory_instance()
        writer = factory_wrapper.get_writer_instance()
        
        method_name = "get_columnproperties_aspect"
        if not hasattr(writer, method_name):
            # Debug: list available methods
            available_methods = [m for m in dir(writer) if not m.startswith('_') and 'aspect' in m]
            raise HTTPException(status_code=400, detail=f"Aspect 'columnProperties' not found. Available aspect methods: {available_methods}")
        
        method = getattr(writer, method_name)
        result = method(entity_label, entity_urn)
        
        if result is None:
            raise HTTPException(status_code=404, detail=f"columnProperties aspect not found")
        
        return models.ColumnpropertiesAspectResponse(
            entity_label=entity_label,
            entity_urn=entity_urn,
            aspect_name="columnProperties",
            payload=result,
            version=result.get('version') if isinstance(result, dict) else None
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/aspects/corpGroupInfo/{entity_label}/{entity_urn}", response_model=models.CorpgroupinfoAspectResponse)
async def get_corpGroupInfo_aspect(entity_label: str, entity_urn: str):
    """Get corpGroupInfo aspect for entity"""
    try:
        factory = factory_wrapper.get_factory_instance()
        writer = factory_wrapper.get_writer_instance()
        
        method_name = "get_corpgroupinfo_aspect"
        if not hasattr(writer, method_name):
            # Debug: list available methods
            available_methods = [m for m in dir(writer) if not m.startswith('_') and 'aspect' in m]
            raise HTTPException(status_code=400, detail=f"Aspect 'corpGroupInfo' not found. Available aspect methods: {available_methods}")
        
        method = getattr(writer, method_name)
        result = method(entity_label, entity_urn)
        
        if result is None:
            raise HTTPException(status_code=404, detail=f"corpGroupInfo aspect not found")
        
        return models.CorpgroupinfoAspectResponse(
            entity_label=entity_label,
            entity_urn=entity_urn,
            aspect_name="corpGroupInfo",
            payload=result,
            version=result.get('version') if isinstance(result, dict) else None
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/aspects/corpUserInfo/{entity_label}/{entity_urn}", response_model=models.CorpuserinfoAspectResponse)
async def get_corpUserInfo_aspect(entity_label: str, entity_urn: str):
    """Get corpUserInfo aspect for entity"""
    try:
        factory = factory_wrapper.get_factory_instance()
        writer = factory_wrapper.get_writer_instance()
        
        method_name = "get_corpuserinfo_aspect"
        if not hasattr(writer, method_name):
            # Debug: list available methods
            available_methods = [m for m in dir(writer) if not m.startswith('_') and 'aspect' in m]
            raise HTTPException(status_code=400, detail=f"Aspect 'corpUserInfo' not found. Available aspect methods: {available_methods}")
        
        method = getattr(writer, method_name)
        result = method(entity_label, entity_urn)
        
        if result is None:
            raise HTTPException(status_code=404, detail=f"corpUserInfo aspect not found")
        
        return models.CorpuserinfoAspectResponse(
            entity_label=entity_label,
            entity_urn=entity_urn,
            aspect_name="corpUserInfo",
            payload=result,
            version=result.get('version') if isinstance(result, dict) else None
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/aspects/dataFlowInfo/{entity_label}/{entity_urn}", response_model=models.DataflowinfoAspectResponse)
async def get_dataFlowInfo_aspect(entity_label: str, entity_urn: str):
    """Get dataFlowInfo aspect for entity"""
    try:
        factory = factory_wrapper.get_factory_instance()
        writer = factory_wrapper.get_writer_instance()
        
        method_name = "get_dataflowinfo_aspect"
        if not hasattr(writer, method_name):
            # Debug: list available methods
            available_methods = [m for m in dir(writer) if not m.startswith('_') and 'aspect' in m]
            raise HTTPException(status_code=400, detail=f"Aspect 'dataFlowInfo' not found. Available aspect methods: {available_methods}")
        
        method = getattr(writer, method_name)
        result = method(entity_label, entity_urn)
        
        if result is None:
            raise HTTPException(status_code=404, detail=f"dataFlowInfo aspect not found")
        
        return models.DataflowinfoAspectResponse(
            entity_label=entity_label,
            entity_urn=entity_urn,
            aspect_name="dataFlowInfo",
            payload=result,
            version=result.get('version') if isinstance(result, dict) else None
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/aspects/dataJobInfo/{entity_label}/{entity_urn}", response_model=models.DatajobinfoAspectResponse)
async def get_dataJobInfo_aspect(entity_label: str, entity_urn: str):
    """Get dataJobInfo aspect for entity"""
    try:
        factory = factory_wrapper.get_factory_instance()
        writer = factory_wrapper.get_writer_instance()
        
        method_name = "get_datajobinfo_aspect"
        if not hasattr(writer, method_name):
            # Debug: list available methods
            available_methods = [m for m in dir(writer) if not m.startswith('_') and 'aspect' in m]
            raise HTTPException(status_code=400, detail=f"Aspect 'dataJobInfo' not found. Available aspect methods: {available_methods}")
        
        method = getattr(writer, method_name)
        result = method(entity_label, entity_urn)
        
        if result is None:
            raise HTTPException(status_code=404, detail=f"dataJobInfo aspect not found")
        
        return models.DatajobinfoAspectResponse(
            entity_label=entity_label,
            entity_urn=entity_urn,
            aspect_name="dataJobInfo",
            payload=result,
            version=result.get('version') if isinstance(result, dict) else None
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/aspects/dataJobInputOutput/{entity_label}/{entity_urn}", response_model=models.DatajobinputoutputAspectResponse)
async def get_dataJobInputOutput_aspect(entity_label: str, entity_urn: str):
    """Get dataJobInputOutput aspect for entity"""
    try:
        factory = factory_wrapper.get_factory_instance()
        writer = factory_wrapper.get_writer_instance()
        
        method_name = "get_datajobinputoutput_aspect"
        if not hasattr(writer, method_name):
            # Debug: list available methods
            available_methods = [m for m in dir(writer) if not m.startswith('_') and 'aspect' in m]
            raise HTTPException(status_code=400, detail=f"Aspect 'dataJobInputOutput' not found. Available aspect methods: {available_methods}")
        
        method = getattr(writer, method_name)
        result = method(entity_label, entity_urn)
        
        if result is None:
            raise HTTPException(status_code=404, detail=f"dataJobInputOutput aspect not found")
        
        return models.DatajobinputoutputAspectResponse(
            entity_label=entity_label,
            entity_urn=entity_urn,
            aspect_name="dataJobInputOutput",
            payload=result,
            version=result.get('version') if isinstance(result, dict) else None
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/aspects/dataJobRun/{entity_label}/{entity_urn}", response_model=models.DatajobrunAspectResponse)
async def get_dataJobRun_aspect(entity_label: str, entity_urn: str, limit: int = 100):
    """Get dataJobRun aspect for entity"""
    try:
        factory = factory_wrapper.get_factory_instance()
        writer = factory_wrapper.get_writer_instance()
        
        method_name = "get_datajobrun_aspect"
        if not hasattr(writer, method_name):
            # Debug: list available methods
            available_methods = [m for m in dir(writer) if not m.startswith('_') and 'aspect' in m]
            raise HTTPException(status_code=400, detail=f"Aspect 'dataJobRun' not found. Available aspect methods: {available_methods}")
        
        method = getattr(writer, method_name)
        result = method(entity_label, entity_urn, limit)
        
        if result is None:
            raise HTTPException(status_code=404, detail=f"dataJobRun aspect not found")
        
        return models.DatajobrunAspectResponse(
            entity_label=entity_label,
            entity_urn=entity_urn,
            aspect_name="dataJobRun",
            payload=result,
            timestamp_ms=result[0].get('timestamp_ms') if result and len(result) > 0 else None
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/aspects/datasetProfile/{entity_label}/{entity_urn}", response_model=models.DatasetprofileAspectResponse)
async def get_datasetProfile_aspect(entity_label: str, entity_urn: str, limit: int = 100):
    """Get datasetProfile aspect for entity"""
    try:
        factory = factory_wrapper.get_factory_instance()
        writer = factory_wrapper.get_writer_instance()
        
        method_name = "get_datasetprofile_aspect"
        if not hasattr(writer, method_name):
            # Debug: list available methods
            available_methods = [m for m in dir(writer) if not m.startswith('_') and 'aspect' in m]
            raise HTTPException(status_code=400, detail=f"Aspect 'datasetProfile' not found. Available aspect methods: {available_methods}")
        
        method = getattr(writer, method_name)
        result = method(entity_label, entity_urn, limit)
        
        if result is None:
            raise HTTPException(status_code=404, detail=f"datasetProfile aspect not found")
        
        return models.DatasetprofileAspectResponse(
            entity_label=entity_label,
            entity_urn=entity_urn,
            aspect_name="datasetProfile",
            payload=result,
            timestamp_ms=result[0].get('timestamp_ms') if result and len(result) > 0 else None
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/aspects/datasetProperties/{entity_label}/{entity_urn}", response_model=models.DatasetpropertiesAspectResponse)
async def get_datasetProperties_aspect(entity_label: str, entity_urn: str):
    """Get datasetProperties aspect for entity"""
    try:
        factory = factory_wrapper.get_factory_instance()
        writer = factory_wrapper.get_writer_instance()
        
        method_name = "get_datasetproperties_aspect"
        if not hasattr(writer, method_name):
            # Debug: list available methods
            available_methods = [m for m in dir(writer) if not m.startswith('_') and 'aspect' in m]
            raise HTTPException(status_code=400, detail=f"Aspect 'datasetProperties' not found. Available aspect methods: {available_methods}")
        
        method = getattr(writer, method_name)
        result = method(entity_label, entity_urn)
        
        if result is None:
            raise HTTPException(status_code=404, detail=f"datasetProperties aspect not found")
        
        return models.DatasetpropertiesAspectResponse(
            entity_label=entity_label,
            entity_urn=entity_urn,
            aspect_name="datasetProperties",
            payload=result,
            version=result.get('version') if isinstance(result, dict) else None
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/aspects/documentation/{entity_label}/{entity_urn}", response_model=models.DocumentationAspectResponse)
async def get_documentation_aspect(entity_label: str, entity_urn: str):
    """Get documentation aspect for entity"""
    try:
        factory = factory_wrapper.get_factory_instance()
        writer = factory_wrapper.get_writer_instance()
        
        method_name = "get_documentation_aspect"
        if not hasattr(writer, method_name):
            # Debug: list available methods
            available_methods = [m for m in dir(writer) if not m.startswith('_') and 'aspect' in m]
            raise HTTPException(status_code=400, detail=f"Aspect 'documentation' not found. Available aspect methods: {available_methods}")
        
        method = getattr(writer, method_name)
        result = method(entity_label, entity_urn)
        
        if result is None:
            raise HTTPException(status_code=404, detail=f"documentation aspect not found")
        
        return models.DocumentationAspectResponse(
            entity_label=entity_label,
            entity_urn=entity_urn,
            aspect_name="documentation",
            payload=result,
            version=result.get('version') if isinstance(result, dict) else None
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/aspects/dpContract/{entity_label}/{entity_urn}", response_model=models.DpcontractAspectResponse)
async def get_dpContract_aspect(entity_label: str, entity_urn: str):
    """Get dpContract aspect for entity"""
    try:
        factory = factory_wrapper.get_factory_instance()
        writer = factory_wrapper.get_writer_instance()
        
        method_name = "get_dpcontract_aspect"
        if not hasattr(writer, method_name):
            # Debug: list available methods
            available_methods = [m for m in dir(writer) if not m.startswith('_') and 'aspect' in m]
            raise HTTPException(status_code=400, detail=f"Aspect 'dpContract' not found. Available aspect methods: {available_methods}")
        
        method = getattr(writer, method_name)
        result = method(entity_label, entity_urn)
        
        if result is None:
            raise HTTPException(status_code=404, detail=f"dpContract aspect not found")
        
        return models.DpcontractAspectResponse(
            entity_label=entity_label,
            entity_urn=entity_urn,
            aspect_name="dpContract",
            payload=result,
            version=result.get('version') if isinstance(result, dict) else None
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/aspects/dpObservability/{entity_label}/{entity_urn}", response_model=models.DpobservabilityAspectResponse)
async def get_dpObservability_aspect(entity_label: str, entity_urn: str):
    """Get dpObservability aspect for entity"""
    try:
        factory = factory_wrapper.get_factory_instance()
        writer = factory_wrapper.get_writer_instance()
        
        method_name = "get_dpobservability_aspect"
        if not hasattr(writer, method_name):
            # Debug: list available methods
            available_methods = [m for m in dir(writer) if not m.startswith('_') and 'aspect' in m]
            raise HTTPException(status_code=400, detail=f"Aspect 'dpObservability' not found. Available aspect methods: {available_methods}")
        
        method = getattr(writer, method_name)
        result = method(entity_label, entity_urn)
        
        if result is None:
            raise HTTPException(status_code=404, detail=f"dpObservability aspect not found")
        
        return models.DpobservabilityAspectResponse(
            entity_label=entity_label,
            entity_urn=entity_urn,
            aspect_name="dpObservability",
            payload=result,
            version=result.get('version') if isinstance(result, dict) else None
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/aspects/dpPolicy/{entity_label}/{entity_urn}", response_model=models.DppolicyAspectResponse)
async def get_dpPolicy_aspect(entity_label: str, entity_urn: str):
    """Get dpPolicy aspect for entity"""
    try:
        factory = factory_wrapper.get_factory_instance()
        writer = factory_wrapper.get_writer_instance()
        
        method_name = "get_dppolicy_aspect"
        if not hasattr(writer, method_name):
            # Debug: list available methods
            available_methods = [m for m in dir(writer) if not m.startswith('_') and 'aspect' in m]
            raise HTTPException(status_code=400, detail=f"Aspect 'dpPolicy' not found. Available aspect methods: {available_methods}")
        
        method = getattr(writer, method_name)
        result = method(entity_label, entity_urn)
        
        if result is None:
            raise HTTPException(status_code=404, detail=f"dpPolicy aspect not found")
        
        return models.DppolicyAspectResponse(
            entity_label=entity_label,
            entity_urn=entity_urn,
            aspect_name="dpPolicy",
            payload=result,
            version=result.get('version') if isinstance(result, dict) else None
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/aspects/dpProvisioner/{entity_label}/{entity_urn}", response_model=models.DpprovisionerAspectResponse)
async def get_dpProvisioner_aspect(entity_label: str, entity_urn: str):
    """Get dpProvisioner aspect for entity"""
    try:
        factory = factory_wrapper.get_factory_instance()
        writer = factory_wrapper.get_writer_instance()
        
        method_name = "get_dpprovisioner_aspect"
        if not hasattr(writer, method_name):
            # Debug: list available methods
            available_methods = [m for m in dir(writer) if not m.startswith('_') and 'aspect' in m]
            raise HTTPException(status_code=400, detail=f"Aspect 'dpProvisioner' not found. Available aspect methods: {available_methods}")
        
        method = getattr(writer, method_name)
        result = method(entity_label, entity_urn)
        
        if result is None:
            raise HTTPException(status_code=404, detail=f"dpProvisioner aspect not found")
        
        return models.DpprovisionerAspectResponse(
            entity_label=entity_label,
            entity_urn=entity_urn,
            aspect_name="dpProvisioner",
            payload=result,
            version=result.get('version') if isinstance(result, dict) else None
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/aspects/environmentProperties/{entity_label}/{entity_urn}", response_model=models.EnvironmentpropertiesAspectResponse)
async def get_environmentProperties_aspect(entity_label: str, entity_urn: str):
    """Get environmentProperties aspect for entity"""
    try:
        factory = factory_wrapper.get_factory_instance()
        writer = factory_wrapper.get_writer_instance()
        
        method_name = "get_environmentproperties_aspect"
        if not hasattr(writer, method_name):
            # Debug: list available methods
            available_methods = [m for m in dir(writer) if not m.startswith('_') and 'aspect' in m]
            raise HTTPException(status_code=400, detail=f"Aspect 'environmentProperties' not found. Available aspect methods: {available_methods}")
        
        method = getattr(writer, method_name)
        result = method(entity_label, entity_urn)
        
        if result is None:
            raise HTTPException(status_code=404, detail=f"environmentProperties aspect not found")
        
        return models.EnvironmentpropertiesAspectResponse(
            entity_label=entity_label,
            entity_urn=entity_urn,
            aspect_name="environmentProperties",
            payload=result,
            version=result.get('version') if isinstance(result, dict) else None
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/aspects/globalTags/{entity_label}/{entity_urn}", response_model=models.GlobaltagsAspectResponse)
async def get_globalTags_aspect(entity_label: str, entity_urn: str):
    """Get globalTags aspect for entity"""
    try:
        factory = factory_wrapper.get_factory_instance()
        writer = factory_wrapper.get_writer_instance()
        
        method_name = "get_globaltags_aspect"
        if not hasattr(writer, method_name):
            # Debug: list available methods
            available_methods = [m for m in dir(writer) if not m.startswith('_') and 'aspect' in m]
            raise HTTPException(status_code=400, detail=f"Aspect 'globalTags' not found. Available aspect methods: {available_methods}")
        
        method = getattr(writer, method_name)
        result = method(entity_label, entity_urn)
        
        if result is None:
            raise HTTPException(status_code=404, detail=f"globalTags aspect not found")
        
        return models.GlobaltagsAspectResponse(
            entity_label=entity_label,
            entity_urn=entity_urn,
            aspect_name="globalTags",
            payload=result,
            version=result.get('version') if isinstance(result, dict) else None
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/aspects/ownership/{entity_label}/{entity_urn}", response_model=models.OwnershipAspectResponse)
async def get_ownership_aspect(entity_label: str, entity_urn: str):
    """Get ownership aspect for entity"""
    try:
        factory = factory_wrapper.get_factory_instance()
        writer = factory_wrapper.get_writer_instance()
        
        method_name = "get_ownership_aspect"
        if not hasattr(writer, method_name):
            # Debug: list available methods
            available_methods = [m for m in dir(writer) if not m.startswith('_') and 'aspect' in m]
            raise HTTPException(status_code=400, detail=f"Aspect 'ownership' not found. Available aspect methods: {available_methods}")
        
        method = getattr(writer, method_name)
        result = method(entity_label, entity_urn)
        
        if result is None:
            raise HTTPException(status_code=404, detail=f"ownership aspect not found")
        
        return models.OwnershipAspectResponse(
            entity_label=entity_label,
            entity_urn=entity_urn,
            aspect_name="ownership",
            payload=result,
            version=result.get('version') if isinstance(result, dict) else None
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/aspects/schemaMetadata/{entity_label}/{entity_urn}", response_model=models.SchemametadataAspectResponse)
async def get_schemaMetadata_aspect(entity_label: str, entity_urn: str):
    """Get schemaMetadata aspect for entity"""
    try:
        factory = factory_wrapper.get_factory_instance()
        writer = factory_wrapper.get_writer_instance()
        
        method_name = "get_schemametadata_aspect"
        if not hasattr(writer, method_name):
            # Debug: list available methods
            available_methods = [m for m in dir(writer) if not m.startswith('_') and 'aspect' in m]
            raise HTTPException(status_code=400, detail=f"Aspect 'schemaMetadata' not found. Available aspect methods: {available_methods}")
        
        method = getattr(writer, method_name)
        result = method(entity_label, entity_urn)
        
        if result is None:
            raise HTTPException(status_code=404, detail=f"schemaMetadata aspect not found")
        
        return models.SchemametadataAspectResponse(
            entity_label=entity_label,
            entity_urn=entity_urn,
            aspect_name="schemaMetadata",
            payload=result,
            version=result.get('version') if isinstance(result, dict) else None
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/aspects/sourceCode/{entity_label}/{entity_urn}", response_model=models.SourcecodeAspectResponse)
async def get_sourceCode_aspect(entity_label: str, entity_urn: str):
    """Get sourceCode aspect for entity"""
    try:
        factory = factory_wrapper.get_factory_instance()
        writer = factory_wrapper.get_writer_instance()
        
        method_name = "get_sourcecode_aspect"
        if not hasattr(writer, method_name):
            # Debug: list available methods
            available_methods = [m for m in dir(writer) if not m.startswith('_') and 'aspect' in m]
            raise HTTPException(status_code=400, detail=f"Aspect 'sourceCode' not found. Available aspect methods: {available_methods}")
        
        method = getattr(writer, method_name)
        result = method(entity_label, entity_urn)
        
        if result is None:
            raise HTTPException(status_code=404, detail=f"sourceCode aspect not found")
        
        return models.SourcecodeAspectResponse(
            entity_label=entity_label,
            entity_urn=entity_urn,
            aspect_name="sourceCode",
            payload=result,
            version=result.get('version') if isinstance(result, dict) else None
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/aspects/sourceCodeLocation/{entity_label}/{entity_urn}", response_model=models.SourcecodelocationAspectResponse)
async def get_sourceCodeLocation_aspect(entity_label: str, entity_urn: str):
    """Get sourceCodeLocation aspect for entity"""
    try:
        factory = factory_wrapper.get_factory_instance()
        writer = factory_wrapper.get_writer_instance()
        
        method_name = "get_sourcecodelocation_aspect"
        if not hasattr(writer, method_name):
            # Debug: list available methods
            available_methods = [m for m in dir(writer) if not m.startswith('_') and 'aspect' in m]
            raise HTTPException(status_code=400, detail=f"Aspect 'sourceCodeLocation' not found. Available aspect methods: {available_methods}")
        
        method = getattr(writer, method_name)
        result = method(entity_label, entity_urn)
        
        if result is None:
            raise HTTPException(status_code=404, detail=f"sourceCodeLocation aspect not found")
        
        return models.SourcecodelocationAspectResponse(
            entity_label=entity_label,
            entity_urn=entity_urn,
            aspect_name="sourceCodeLocation",
            payload=result,
            version=result.get('version') if isinstance(result, dict) else None
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/aspects/transformation/{entity_label}/{entity_urn}", response_model=models.TransformationAspectResponse)
async def get_transformation_aspect(entity_label: str, entity_urn: str):
    """Get transformation aspect for entity"""
    try:
        factory = factory_wrapper.get_factory_instance()
        writer = factory_wrapper.get_writer_instance()
        
        method_name = "get_transformation_aspect"
        if not hasattr(writer, method_name):
            # Debug: list available methods
            available_methods = [m for m in dir(writer) if not m.startswith('_') and 'aspect' in m]
            raise HTTPException(status_code=400, detail=f"Aspect 'transformation' not found. Available aspect methods: {available_methods}")
        
        method = getattr(writer, method_name)
        result = method(entity_label, entity_urn)
        
        if result is None:
            raise HTTPException(status_code=404, detail=f"transformation aspect not found")
        
        return models.TransformationAspectResponse(
            entity_label=entity_label,
            entity_urn=entity_urn,
            aspect_name="transformation",
            payload=result,
            version=result.get('version') if isinstance(result, dict) else None
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
