#!/usr/bin/env python3
"""
DELETE routes for the generated API
"""

from fastapi import APIRouter, HTTPException
from typing import Any, Dict
import models
import factory_wrapper


router = APIRouter(prefix="/api/v1", tags=["DELETE Operations"])


# Entity DELETE routes

@router.delete("/entities/Column/{urn}")
async def delete_Column(urn: str):
    """Delete Column entity by URN"""
    try:
        factory = factory_wrapper.get_factory_instance()
        writer = factory_wrapper.get_writer_instance()
        
        method_name = "delete_column"
        if not hasattr(writer, method_name):
            raise HTTPException(status_code=400, detail=f"Entity type 'Column' not found")
        
        method = getattr(writer, method_name)
        method(urn)
        
        return {"message": f"Column with URN '{urn}' deleted successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.delete("/entities/CorpGroup/{urn}")
async def delete_CorpGroup(urn: str):
    """Delete CorpGroup entity by URN"""
    try:
        factory = factory_wrapper.get_factory_instance()
        writer = factory_wrapper.get_writer_instance()
        
        method_name = "delete_corpgroup"
        if not hasattr(writer, method_name):
            raise HTTPException(status_code=400, detail=f"Entity type 'CorpGroup' not found")
        
        method = getattr(writer, method_name)
        method(urn)
        
        return {"message": f"CorpGroup with URN '{urn}' deleted successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.delete("/entities/CorpUser/{urn}")
async def delete_CorpUser(urn: str):
    """Delete CorpUser entity by URN"""
    try:
        factory = factory_wrapper.get_factory_instance()
        writer = factory_wrapper.get_writer_instance()
        
        method_name = "delete_corpuser"
        if not hasattr(writer, method_name):
            raise HTTPException(status_code=400, detail=f"Entity type 'CorpUser' not found")
        
        method = getattr(writer, method_name)
        method(urn)
        
        return {"message": f"CorpUser with URN '{urn}' deleted successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.delete("/entities/Dataset/{urn}")
async def delete_Dataset(urn: str):
    """Delete Dataset entity by URN"""
    try:
        factory = factory_wrapper.get_factory_instance()
        writer = factory_wrapper.get_writer_instance()
        
        method_name = "delete_dataset"
        if not hasattr(writer, method_name):
            raise HTTPException(status_code=400, detail=f"Entity type 'Dataset' not found")
        
        method = getattr(writer, method_name)
        method(urn)
        
        return {"message": f"Dataset with URN '{urn}' deleted successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.delete("/entities/Tag/{urn}")
async def delete_Tag(urn: str):
    """Delete Tag entity by URN"""
    try:
        factory = factory_wrapper.get_factory_instance()
        writer = factory_wrapper.get_writer_instance()
        
        method_name = "delete_tag"
        if not hasattr(writer, method_name):
            raise HTTPException(status_code=400, detail=f"Entity type 'Tag' not found")
        
        method = getattr(writer, method_name)
        method(urn)
        
        return {"message": f"Tag with URN '{urn}' deleted successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.delete("/aspects/columnProperties/{entity_label}/{entity_urn}")
async def delete_columnProperties_aspect(entity_label: str, entity_urn: str):
    """Delete columnProperties aspect for entity"""
    try:
        factory = factory_wrapper.get_factory_instance()
        writer = factory_wrapper.get_writer_instance()
        
        method_name = "delete_columnproperties_aspect"
        if not hasattr(writer, method_name):
            raise HTTPException(status_code=400, detail=f"Aspect 'columnProperties' not found")
        
        method = getattr(writer, method_name)
        method(entity_label, entity_urn)
        
        return {"message": f"columnProperties aspect deleted successfully for entity '{entity_urn}'"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.delete("/aspects/columnTransformation/{entity_label}/{entity_urn}")
async def delete_columnTransformation_aspect(entity_label: str, entity_urn: str):
    """Delete columnTransformation aspect for entity"""
    try:
        factory = factory_wrapper.get_factory_instance()
        writer = factory_wrapper.get_writer_instance()
        
        method_name = "delete_columntransformation_aspect"
        if not hasattr(writer, method_name):
            raise HTTPException(status_code=400, detail=f"Aspect 'columnTransformation' not found")
        
        method = getattr(writer, method_name)
        method(entity_label, entity_urn)
        
        return {"message": f"columnTransformation aspect deleted successfully for entity '{entity_urn}'"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.delete("/aspects/corpGroupInfo/{entity_label}/{entity_urn}")
async def delete_corpGroupInfo_aspect(entity_label: str, entity_urn: str):
    """Delete corpGroupInfo aspect for entity"""
    try:
        factory = factory_wrapper.get_factory_instance()
        writer = factory_wrapper.get_writer_instance()
        
        method_name = "delete_corpgroupinfo_aspect"
        if not hasattr(writer, method_name):
            raise HTTPException(status_code=400, detail=f"Aspect 'corpGroupInfo' not found")
        
        method = getattr(writer, method_name)
        method(entity_label, entity_urn)
        
        return {"message": f"corpGroupInfo aspect deleted successfully for entity '{entity_urn}'"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.delete("/aspects/corpUserInfo/{entity_label}/{entity_urn}")
async def delete_corpUserInfo_aspect(entity_label: str, entity_urn: str):
    """Delete corpUserInfo aspect for entity"""
    try:
        factory = factory_wrapper.get_factory_instance()
        writer = factory_wrapper.get_writer_instance()
        
        method_name = "delete_corpuserinfo_aspect"
        if not hasattr(writer, method_name):
            raise HTTPException(status_code=400, detail=f"Aspect 'corpUserInfo' not found")
        
        method = getattr(writer, method_name)
        method(entity_label, entity_urn)
        
        return {"message": f"corpUserInfo aspect deleted successfully for entity '{entity_urn}'"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.delete("/aspects/dataQuality/{entity_label}/{entity_urn}")
async def delete_dataQuality_aspect(entity_label: str, entity_urn: str):
    """Delete dataQuality aspect for entity"""
    try:
        factory = factory_wrapper.get_factory_instance()
        writer = factory_wrapper.get_writer_instance()
        
        method_name = "delete_dataquality_aspect"
        if not hasattr(writer, method_name):
            raise HTTPException(status_code=400, detail=f"Aspect 'dataQuality' not found")
        
        method = getattr(writer, method_name)
        method(entity_label, entity_urn)
        
        return {"message": f"dataQuality aspect deleted successfully for entity '{entity_urn}'"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.delete("/aspects/datasetProfile/{entity_label}/{entity_urn}")
async def delete_datasetProfile_aspect(entity_label: str, entity_urn: str):
    """Delete datasetProfile aspect for entity"""
    try:
        factory = factory_wrapper.get_factory_instance()
        writer = factory_wrapper.get_writer_instance()
        
        method_name = "delete_datasetprofile_aspect"
        if not hasattr(writer, method_name):
            raise HTTPException(status_code=400, detail=f"Aspect 'datasetProfile' not found")
        
        method = getattr(writer, method_name)
        method(entity_label, entity_urn)
        
        return {"message": f"datasetProfile aspect deleted successfully for entity '{entity_urn}'"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.delete("/aspects/datasetProperties/{entity_label}/{entity_urn}")
async def delete_datasetProperties_aspect(entity_label: str, entity_urn: str):
    """Delete datasetProperties aspect for entity"""
    try:
        factory = factory_wrapper.get_factory_instance()
        writer = factory_wrapper.get_writer_instance()
        
        method_name = "delete_datasetproperties_aspect"
        if not hasattr(writer, method_name):
            raise HTTPException(status_code=400, detail=f"Aspect 'datasetProperties' not found")
        
        method = getattr(writer, method_name)
        method(entity_label, entity_urn)
        
        return {"message": f"datasetProperties aspect deleted successfully for entity '{entity_urn}'"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.delete("/aspects/datasetTransformation/{entity_label}/{entity_urn}")
async def delete_datasetTransformation_aspect(entity_label: str, entity_urn: str):
    """Delete datasetTransformation aspect for entity"""
    try:
        factory = factory_wrapper.get_factory_instance()
        writer = factory_wrapper.get_writer_instance()
        
        method_name = "delete_datasettransformation_aspect"
        if not hasattr(writer, method_name):
            raise HTTPException(status_code=400, detail=f"Aspect 'datasetTransformation' not found")
        
        method = getattr(writer, method_name)
        method(entity_label, entity_urn)
        
        return {"message": f"datasetTransformation aspect deleted successfully for entity '{entity_urn}'"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.delete("/aspects/globalTags/{entity_label}/{entity_urn}")
async def delete_globalTags_aspect(entity_label: str, entity_urn: str):
    """Delete globalTags aspect for entity"""
    try:
        factory = factory_wrapper.get_factory_instance()
        writer = factory_wrapper.get_writer_instance()
        
        method_name = "delete_globaltags_aspect"
        if not hasattr(writer, method_name):
            raise HTTPException(status_code=400, detail=f"Aspect 'globalTags' not found")
        
        method = getattr(writer, method_name)
        method(entity_label, entity_urn)
        
        return {"message": f"globalTags aspect deleted successfully for entity '{entity_urn}'"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.delete("/aspects/ownership/{entity_label}/{entity_urn}")
async def delete_ownership_aspect(entity_label: str, entity_urn: str):
    """Delete ownership aspect for entity"""
    try:
        factory = factory_wrapper.get_factory_instance()
        writer = factory_wrapper.get_writer_instance()
        
        method_name = "delete_ownership_aspect"
        if not hasattr(writer, method_name):
            raise HTTPException(status_code=400, detail=f"Aspect 'ownership' not found")
        
        method = getattr(writer, method_name)
        method(entity_label, entity_urn)
        
        return {"message": f"ownership aspect deleted successfully for entity '{entity_urn}'"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.delete("/aspects/schemaMetadata/{entity_label}/{entity_urn}")
async def delete_schemaMetadata_aspect(entity_label: str, entity_urn: str):
    """Delete schemaMetadata aspect for entity"""
    try:
        factory = factory_wrapper.get_factory_instance()
        writer = factory_wrapper.get_writer_instance()
        
        method_name = "delete_schemametadata_aspect"
        if not hasattr(writer, method_name):
            raise HTTPException(status_code=400, detail=f"Aspect 'schemaMetadata' not found")
        
        method = getattr(writer, method_name)
        method(entity_label, entity_urn)
        
        return {"message": f"schemaMetadata aspect deleted successfully for entity '{entity_urn}'"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
