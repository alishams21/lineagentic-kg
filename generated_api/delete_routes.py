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

@router.delete("/entities/DataFlow/{urn}")
async def delete_DataFlow(urn: str):
    """Delete DataFlow entity by URN"""
    try:
        factory = factory_wrapper.get_factory_instance()
        writer = factory_wrapper.get_writer_instance()
        
        method_name = "delete_dataflow"
        if not hasattr(writer, method_name):
            raise HTTPException(status_code=400, detail=f"Entity type 'DataFlow' not found")
        
        method = getattr(writer, method_name)
        method(urn)
        
        return {"message": f"DataFlow with URN '{urn}' deleted successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.delete("/entities/DataJob/{urn}")
async def delete_DataJob(urn: str):
    """Delete DataJob entity by URN"""
    try:
        factory = factory_wrapper.get_factory_instance()
        writer = factory_wrapper.get_writer_instance()
        
        method_name = "delete_datajob"
        if not hasattr(writer, method_name):
            raise HTTPException(status_code=400, detail=f"Entity type 'DataJob' not found")
        
        method = getattr(writer, method_name)
        method(urn)
        
        return {"message": f"DataJob with URN '{urn}' deleted successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.delete("/entities/DataProduct/{urn}")
async def delete_DataProduct(urn: str):
    """Delete DataProduct entity by URN"""
    try:
        factory = factory_wrapper.get_factory_instance()
        writer = factory_wrapper.get_writer_instance()
        
        method_name = "delete_dataproduct"
        if not hasattr(writer, method_name):
            raise HTTPException(status_code=400, detail=f"Entity type 'DataProduct' not found")
        
        method = getattr(writer, method_name)
        method(urn)
        
        return {"message": f"DataProduct with URN '{urn}' deleted successfully"}
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

@router.delete("/aspects/dataFlowInfo/{entity_label}/{entity_urn}")
async def delete_dataFlowInfo_aspect(entity_label: str, entity_urn: str):
    """Delete dataFlowInfo aspect for entity"""
    try:
        factory = factory_wrapper.get_factory_instance()
        writer = factory_wrapper.get_writer_instance()
        
        method_name = "delete_dataflowinfo_aspect"
        if not hasattr(writer, method_name):
            raise HTTPException(status_code=400, detail=f"Aspect 'dataFlowInfo' not found")
        
        method = getattr(writer, method_name)
        method(entity_label, entity_urn)
        
        return {"message": f"dataFlowInfo aspect deleted successfully for entity '{entity_urn}'"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.delete("/aspects/dataJobInfo/{entity_label}/{entity_urn}")
async def delete_dataJobInfo_aspect(entity_label: str, entity_urn: str):
    """Delete dataJobInfo aspect for entity"""
    try:
        factory = factory_wrapper.get_factory_instance()
        writer = factory_wrapper.get_writer_instance()
        
        method_name = "delete_datajobinfo_aspect"
        if not hasattr(writer, method_name):
            raise HTTPException(status_code=400, detail=f"Aspect 'dataJobInfo' not found")
        
        method = getattr(writer, method_name)
        method(entity_label, entity_urn)
        
        return {"message": f"dataJobInfo aspect deleted successfully for entity '{entity_urn}'"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.delete("/aspects/dataJobInputOutput/{entity_label}/{entity_urn}")
async def delete_dataJobInputOutput_aspect(entity_label: str, entity_urn: str):
    """Delete dataJobInputOutput aspect for entity"""
    try:
        factory = factory_wrapper.get_factory_instance()
        writer = factory_wrapper.get_writer_instance()
        
        method_name = "delete_datajobinputoutput_aspect"
        if not hasattr(writer, method_name):
            raise HTTPException(status_code=400, detail=f"Aspect 'dataJobInputOutput' not found")
        
        method = getattr(writer, method_name)
        method(entity_label, entity_urn)
        
        return {"message": f"dataJobInputOutput aspect deleted successfully for entity '{entity_urn}'"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.delete("/aspects/dataJobRun/{entity_label}/{entity_urn}")
async def delete_dataJobRun_aspect(entity_label: str, entity_urn: str):
    """Delete dataJobRun aspect for entity"""
    try:
        factory = factory_wrapper.get_factory_instance()
        writer = factory_wrapper.get_writer_instance()
        
        method_name = "delete_datajobrun_aspect"
        if not hasattr(writer, method_name):
            raise HTTPException(status_code=400, detail=f"Aspect 'dataJobRun' not found")
        
        method = getattr(writer, method_name)
        method(entity_label, entity_urn)
        
        return {"message": f"dataJobRun aspect deleted successfully for entity '{entity_urn}'"}
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

@router.delete("/aspects/documentation/{entity_label}/{entity_urn}")
async def delete_documentation_aspect(entity_label: str, entity_urn: str):
    """Delete documentation aspect for entity"""
    try:
        factory = factory_wrapper.get_factory_instance()
        writer = factory_wrapper.get_writer_instance()
        
        method_name = "delete_documentation_aspect"
        if not hasattr(writer, method_name):
            raise HTTPException(status_code=400, detail=f"Aspect 'documentation' not found")
        
        method = getattr(writer, method_name)
        method(entity_label, entity_urn)
        
        return {"message": f"documentation aspect deleted successfully for entity '{entity_urn}'"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.delete("/aspects/dpContract/{entity_label}/{entity_urn}")
async def delete_dpContract_aspect(entity_label: str, entity_urn: str):
    """Delete dpContract aspect for entity"""
    try:
        factory = factory_wrapper.get_factory_instance()
        writer = factory_wrapper.get_writer_instance()
        
        method_name = "delete_dpcontract_aspect"
        if not hasattr(writer, method_name):
            raise HTTPException(status_code=400, detail=f"Aspect 'dpContract' not found")
        
        method = getattr(writer, method_name)
        method(entity_label, entity_urn)
        
        return {"message": f"dpContract aspect deleted successfully for entity '{entity_urn}'"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.delete("/aspects/dpObservability/{entity_label}/{entity_urn}")
async def delete_dpObservability_aspect(entity_label: str, entity_urn: str):
    """Delete dpObservability aspect for entity"""
    try:
        factory = factory_wrapper.get_factory_instance()
        writer = factory_wrapper.get_writer_instance()
        
        method_name = "delete_dpobservability_aspect"
        if not hasattr(writer, method_name):
            raise HTTPException(status_code=400, detail=f"Aspect 'dpObservability' not found")
        
        method = getattr(writer, method_name)
        method(entity_label, entity_urn)
        
        return {"message": f"dpObservability aspect deleted successfully for entity '{entity_urn}'"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.delete("/aspects/dpPolicy/{entity_label}/{entity_urn}")
async def delete_dpPolicy_aspect(entity_label: str, entity_urn: str):
    """Delete dpPolicy aspect for entity"""
    try:
        factory = factory_wrapper.get_factory_instance()
        writer = factory_wrapper.get_writer_instance()
        
        method_name = "delete_dppolicy_aspect"
        if not hasattr(writer, method_name):
            raise HTTPException(status_code=400, detail=f"Aspect 'dpPolicy' not found")
        
        method = getattr(writer, method_name)
        method(entity_label, entity_urn)
        
        return {"message": f"dpPolicy aspect deleted successfully for entity '{entity_urn}'"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.delete("/aspects/dpProvisioner/{entity_label}/{entity_urn}")
async def delete_dpProvisioner_aspect(entity_label: str, entity_urn: str):
    """Delete dpProvisioner aspect for entity"""
    try:
        factory = factory_wrapper.get_factory_instance()
        writer = factory_wrapper.get_writer_instance()
        
        method_name = "delete_dpprovisioner_aspect"
        if not hasattr(writer, method_name):
            raise HTTPException(status_code=400, detail=f"Aspect 'dpProvisioner' not found")
        
        method = getattr(writer, method_name)
        method(entity_label, entity_urn)
        
        return {"message": f"dpProvisioner aspect deleted successfully for entity '{entity_urn}'"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.delete("/aspects/environmentProperties/{entity_label}/{entity_urn}")
async def delete_environmentProperties_aspect(entity_label: str, entity_urn: str):
    """Delete environmentProperties aspect for entity"""
    try:
        factory = factory_wrapper.get_factory_instance()
        writer = factory_wrapper.get_writer_instance()
        
        method_name = "delete_environmentproperties_aspect"
        if not hasattr(writer, method_name):
            raise HTTPException(status_code=400, detail=f"Aspect 'environmentProperties' not found")
        
        method = getattr(writer, method_name)
        method(entity_label, entity_urn)
        
        return {"message": f"environmentProperties aspect deleted successfully for entity '{entity_urn}'"}
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

@router.delete("/aspects/sourceCode/{entity_label}/{entity_urn}")
async def delete_sourceCode_aspect(entity_label: str, entity_urn: str):
    """Delete sourceCode aspect for entity"""
    try:
        factory = factory_wrapper.get_factory_instance()
        writer = factory_wrapper.get_writer_instance()
        
        method_name = "delete_sourcecode_aspect"
        if not hasattr(writer, method_name):
            raise HTTPException(status_code=400, detail=f"Aspect 'sourceCode' not found")
        
        method = getattr(writer, method_name)
        method(entity_label, entity_urn)
        
        return {"message": f"sourceCode aspect deleted successfully for entity '{entity_urn}'"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.delete("/aspects/sourceCodeLocation/{entity_label}/{entity_urn}")
async def delete_sourceCodeLocation_aspect(entity_label: str, entity_urn: str):
    """Delete sourceCodeLocation aspect for entity"""
    try:
        factory = factory_wrapper.get_factory_instance()
        writer = factory_wrapper.get_writer_instance()
        
        method_name = "delete_sourcecodelocation_aspect"
        if not hasattr(writer, method_name):
            raise HTTPException(status_code=400, detail=f"Aspect 'sourceCodeLocation' not found")
        
        method = getattr(writer, method_name)
        method(entity_label, entity_urn)
        
        return {"message": f"sourceCodeLocation aspect deleted successfully for entity '{entity_urn}'"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.delete("/aspects/transformation/{entity_label}/{entity_urn}")
async def delete_transformation_aspect(entity_label: str, entity_urn: str):
    """Delete transformation aspect for entity"""
    try:
        factory = factory_wrapper.get_factory_instance()
        writer = factory_wrapper.get_writer_instance()
        
        method_name = "delete_transformation_aspect"
        if not hasattr(writer, method_name):
            raise HTTPException(status_code=400, detail=f"Aspect 'transformation' not found")
        
        method = getattr(writer, method_name)
        method(entity_label, entity_urn)
        
        return {"message": f"transformation aspect deleted successfully for entity '{entity_urn}'"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
