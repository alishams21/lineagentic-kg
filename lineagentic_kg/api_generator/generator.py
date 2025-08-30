#!/usr/bin/env python3
"""
API Generator that creates FastAPI files based on RegistryFactory generated methods
"""

import os
import sys
import inspect
from typing import Any, Dict, List, Set
from pathlib import Path

from lineagentic_kg.utils.logging_config import get_logger, log_function_call, log_function_result, log_error_with_context
from lineagentic_kg.registry.factory import RegistryFactory


class APIGenerator:
    """Generates FastAPI files based on RegistryFactory methods"""
    
    def __init__(self, registry_path: str, output_dir: str = "generated_api"):
        self.logger = get_logger("lineagentic.api.generator")
        self.registry_path = registry_path
        self.output_dir = Path(output_dir)
        
        self.logger.info("Initializing APIGenerator", registry_path=registry_path, output_dir=output_dir)
        
        try:
            self.factory = RegistryFactory(registry_path)
            
            # Create output directory
            self.output_dir.mkdir(exist_ok=True)
            self.logger.debug("Created output directory", output_dir=str(self.output_dir))
            
            # Create a temporary writer instance to get the actual method names
            self._temp_writer = None
            
            self.logger.info("APIGenerator initialized successfully")
            
        except Exception as e:
            log_error_with_context(self.logger, e, "APIGenerator initialization")
            raise
        
    def _get_temp_writer(self):
        """Get a temporary writer instance to inspect available methods"""
        if self._temp_writer is None:
            self.logger.debug("Creating temporary writer instance for method inspection")
            # Create a temporary writer with dummy connection
            self._temp_writer = self.factory.create_writer("bolt://localhost:7687", "neo4j", "password")
        return self._temp_writer
    
    def _get_method_name_for_entity(self, entity_name: str, operation: str) -> str:
        """Get method name for entity operation using the same logic as Neo4jWriterGenerator"""
        return f"{operation}_{entity_name.lower()}"
    
    def _get_method_name_for_aspect(self, aspect_name: str, operation: str) -> str:
        """Get method name for aspect operation using the same logic as Neo4jWriterGenerator"""
        return f"{operation}_{aspect_name.lower()}_aspect"
    
    def _sanitize_field_name(self, field_name: str) -> str:
        """Sanitize field name for Python syntax by removing invalid characters"""
        # Remove square brackets and other invalid characters for Python field names
        sanitized = field_name.replace('[]', '').replace('[', '').replace(']', '')
        # Replace any other invalid characters with underscore
        sanitized = ''.join(c if c.isalnum() or c == '_' else '_' for c in sanitized)
        # Ensure it doesn't start with a number
        if sanitized and sanitized[0].isdigit():
            sanitized = 'field_' + sanitized
        return sanitized
        
    def generate_all(self):
        """Generate all FastAPI files"""
        log_function_call(self.logger, "generate_all")
        
        try:
            self.logger.info("Generating FastAPI files from RegistryFactory...")
            
            # Copy config files first
            self.logger.debug("Copying config files")
            self._copy_config_files()
            
            # Generate files
            self.logger.debug("Generating models.py")
            self._generate_models()
            
            self.logger.debug("Generating get routes")
            self._generate_get_routes()
            
            self.logger.debug("Generating upsert routes")
            self._generate_upsert_routes()
            
            self.logger.debug("Generating delete routes")
            self._generate_delete_routes()
            
            self.logger.debug("Generating factory wrapper")
            self._generate_factory_wrapper()
            
            self.logger.debug("Generating main app")
            self._generate_main_app()
            
            self.logger.debug("Generating requirements")
            self._generate_requirements()
            
            self.logger.debug("Generating README")
            self._generate_readme()
            
            self.logger.info("Generated FastAPI files successfully", output_dir=str(self.output_dir))
            log_function_result(self.logger, "generate_all", output_dir=str(self.output_dir))
            
        except Exception as e:
            log_error_with_context(self.logger, e, "generate_all")
            raise
    
    def _generate_models(self):
        """Generate Pydantic models dynamically from registry configuration"""
        log_function_call(self.logger, "_generate_models")
        self.logger.debug("Generating models.py...")
        
        models_content = '''#!/usr/bin/env python3
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
'''
        
        # Generate entity models dynamically from registry
        entities = self.factory.registry.get('entities', {})
        for entity_name, entity_config in entities.items():
            properties = entity_config.get('properties', [])
            array_properties = entity_config.get('array_properties', [])
            
            # Generate upsert request model
            models_content += f'''
class {entity_name}UpsertRequest(BaseModel):
    """Request model for upserting {entity_name} entity"""
'''
            
            # Add entity-specific properties
            for prop in properties:
                sanitized_prop = self._sanitize_field_name(prop)
                if prop in array_properties:
                    models_content += f'''
    {sanitized_prop}: Optional[List[str]] = Field(None, description="{prop} (array)")'''
                else:
                    models_content += f'''
    {sanitized_prop}: Optional[str] = Field(None, description="{prop}")'''
            
            models_content += f'''
    additional_properties: Optional[Dict[str, Any]] = Field(None, description="Additional {entity_name} properties")
'''
            
            # Generate get request model
            models_content += f'''

class {entity_name}GetRequest(BaseModel):
    """Request model for getting {entity_name} entity"""
    urn: str = Field(..., description="{entity_name} URN")
'''
            
            # Generate delete request model
            models_content += f'''

class {entity_name}DeleteRequest(BaseModel):
    """Request model for deleting {entity_name} entity"""
    urn: str = Field(..., description="{entity_name} URN")
'''
            
            # Generate response model
            models_content += f'''

class {entity_name}Response(BaseModel):
    """Response model for {entity_name} entity"""
    urn: str = Field(..., description="{entity_name} URN")
    properties: Dict[str, Any] = Field(..., description="{entity_name} properties")
    last_updated: Optional[datetime] = Field(None, description="Last updated timestamp")
'''
        
        # Generate aspect models dynamically from registry
        models_content += '''

# Aspect models - generated dynamically from registry
'''
        
        aspects = self.factory.registry.get('aspects', {})
        for aspect_name, aspect_config in aspects.items():
            aspect_type = aspect_config.get('type', 'versioned')
            properties = aspect_config.get('properties', [])
            required_props = aspect_config.get('required', [])
            
            # Generate upsert request model
            models_content += f'''
class {aspect_name.title()}AspectUpsertRequest(BaseModel):
    """Request model for upserting {aspect_name} aspect"""
    entity_label: Optional[str] = Field(None, description="Entity label (optional if entity_creation is configured)")
    entity_urn: Optional[str] = Field(None, description="Entity URN (optional if entity_creation is configured)")
'''
            
            # Add aspect-specific properties
            for prop in properties:
                sanitized_prop = self._sanitize_field_name(prop)
                if prop in required_props:
                    models_content += f'''
    {sanitized_prop}: Any = Field(..., description="{prop}")'''
                else:
                    models_content += f'''
    {sanitized_prop}: Optional[Any] = Field(None, description="{prop}")'''
            
            # Add type-specific fields
            if aspect_type == 'versioned':
                models_content += '''
    version: Optional[int] = Field(None, description="Version (for versioned aspects)")
'''
            elif aspect_type == 'timeseries':
                models_content += '''
    timestamp_ms: Optional[int] = Field(None, description="Timestamp in milliseconds (for timeseries aspects)")
'''
            
            models_content += f'''
    entity_params: Optional[Dict[str, Any]] = Field(None, description="Entity creation parameters")
'''
            
            # Generate get request model
            models_content += f'''

class {aspect_name.title()}AspectGetRequest(BaseModel):
    """Request model for getting {aspect_name} aspect"""
    entity_label: str = Field(..., description="Entity label")
    entity_urn: str = Field(..., description="Entity URN")
'''
            
            if aspect_type == 'timeseries':
                models_content += '''    limit: Optional[int] = Field(100, description="Limit for timeseries aspects")
'''
            
            # Generate delete request model
            models_content += f'''

class {aspect_name.title()}AspectDeleteRequest(BaseModel):
    """Request model for deleting {aspect_name} aspect"""
    entity_label: str = Field(..., description="Entity label")
    entity_urn: str = Field(..., description="Entity URN")
'''
            
            # Generate response model
            payload_type = "List[Dict[str, Any]]" if aspect_type == 'timeseries' else "Dict[str, Any]"
            models_content += f'''

class {aspect_name.title()}AspectResponse(BaseModel):
    """Response model for {aspect_name} aspect"""
    entity_label: str = Field(..., description="Entity label")
    entity_urn: str = Field(..., description="Entity URN")
    aspect_name: str = Field(..., description="Aspect name")
    payload: {payload_type} = Field(..., description="Aspect payload")
'''
            
            if aspect_type == 'versioned':
                models_content += '''    version: Optional[int] = Field(None, description="Version")
'''
            elif aspect_type == 'timeseries':
                models_content += '''    timestamp_ms: Optional[int] = Field(None, description="Timestamp")
'''
        
        # Generate utility models
        models_content += '''

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
'''
        
        # Write models file
        with open(self.output_dir / "models.py", "w") as f:
            f.write(models_content)
        
        self.logger.debug("Generated models.py successfully")
        log_function_result(self.logger, "_generate_models")
    
    def _generate_get_routes(self):
        """Generate GET routes"""
        routes_content = '''#!/usr/bin/env python3
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
'''
        
        # Generate entity GET routes dynamically from registry
        entities = self.factory.registry.get('entities', {})
        for entity_name in sorted(entities.keys()):
            method_name = self._get_method_name_for_entity(entity_name, "get")
            routes_content += f'''
@router.get("/entities/{entity_name}/{{urn}}", response_model=models.{entity_name}Response)
async def get_{entity_name}(urn: str):
    """Get {entity_name} entity by URN"""
    try:
        factory = factory_wrapper.get_factory_instance()
        writer = factory_wrapper.get_writer_instance()
        
        method_name = "{method_name}"
        if not hasattr(writer, method_name):
            raise HTTPException(status_code=400, detail=f"Entity type '{entity_name}' not found")
        
        method = getattr(writer, method_name)
        result = method(urn)
        
        if result is None:
            raise HTTPException(status_code=404, detail=f"{entity_name} with URN '{{urn}}' not found")
        
        return models.{entity_name}Response(
            urn=urn,
            properties=result,
            last_updated=result.get('lastUpdated')
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
'''
        
        # Generate aspect GET routes dynamically from registry
        aspects = self.factory.registry.get('aspects', {})
        for aspect_name in sorted(aspects.keys()):
            aspect_config = aspects[aspect_name]
            aspect_type = aspect_config.get('type', 'versioned')
            method_name = self._get_method_name_for_aspect(aspect_name, "get")
            
            if aspect_type == 'timeseries':
                # Timeseries aspects need limit parameter and return list
                routes_content += f'''
@router.get("/aspects/{aspect_name}/{{entity_label}}/{{entity_urn}}", response_model=models.{aspect_name.title()}AspectResponse)
async def get_{aspect_name}_aspect(entity_label: str, entity_urn: str, limit: int = 100):
    """Get {aspect_name} aspect for entity"""
    try:
        factory = factory_wrapper.get_factory_instance()
        writer = factory_wrapper.get_writer_instance()
        
        method_name = "{method_name}"
        if not hasattr(writer, method_name):
            # Debug: list available methods
            available_methods = [m for m in dir(writer) if not m.startswith('_') and 'aspect' in m]
            raise HTTPException(status_code=400, detail=f"Aspect '{aspect_name}' not found. Available aspect methods: {{available_methods}}")
        
        method = getattr(writer, method_name)
        result = method(entity_label, entity_urn, limit)
        
        if result is None:
            raise HTTPException(status_code=404, detail=f"{aspect_name} aspect not found")
        
        return models.{aspect_name.title()}AspectResponse(
            entity_label=entity_label,
            entity_urn=entity_urn,
            aspect_name="{aspect_name}",
            payload=result,
            timestamp_ms=result[0].get('timestamp_ms') if result and len(result) > 0 else None
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
'''
            else:
                # Versioned aspects don't need limit parameter and return dict
                routes_content += f'''
@router.get("/aspects/{aspect_name}/{{entity_label}}/{{entity_urn}}", response_model=models.{aspect_name.title()}AspectResponse)
async def get_{aspect_name}_aspect(entity_label: str, entity_urn: str):
    """Get {aspect_name} aspect for entity"""
    try:
        factory = factory_wrapper.get_factory_instance()
        writer = factory_wrapper.get_writer_instance()
        
        method_name = "{method_name}"
        if not hasattr(writer, method_name):
            # Debug: list available methods
            available_methods = [m for m in dir(writer) if not m.startswith('_') and 'aspect' in m]
            raise HTTPException(status_code=400, detail=f"Aspect '{aspect_name}' not found. Available aspect methods: {{available_methods}}")
        
        method = getattr(writer, method_name)
        result = method(entity_label, entity_urn)
        
        if result is None:
            raise HTTPException(status_code=404, detail=f"{aspect_name} aspect not found")
        
        return models.{aspect_name.title()}AspectResponse(
            entity_label=entity_label,
            entity_urn=entity_urn,
            aspect_name="{aspect_name}",
            payload=result,
            version=result.get('version') if isinstance(result, dict) else None
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
'''
        
        # Write routes file
        with open(self.output_dir / "get_routes.py", "w") as f:
            f.write(routes_content)
        
        print(f"✅ Generated get_routes.py")
    
    def _generate_upsert_routes(self):
        """Generate UPSERT routes"""
        routes_content = '''#!/usr/bin/env python3
"""
UPSERT routes for the generated API
"""

from fastapi import APIRouter, HTTPException
from typing import Any, Dict
import models
import factory_wrapper


router = APIRouter(prefix="/api/v1", tags=["UPSERT Operations"])


# Entity UPSERT routes
'''
        
        # Generate entity UPSERT routes dynamically from registry
        entities = self.factory.registry.get('entities', {})
        for entity_name in sorted(entities.keys()):
            method_name = self._get_method_name_for_entity(entity_name, "upsert")
            routes_content += f'''
@router.post("/entities/{entity_name}", response_model=models.{entity_name}Response)
async def upsert_{entity_name}(request: models.{entity_name}UpsertRequest):
    """Upsert {entity_name} entity"""
    try:
        factory = factory_wrapper.get_factory_instance()
        writer = factory_wrapper.get_writer_instance()
        
        method_name = "{method_name}"
        if not hasattr(writer, method_name):
            raise HTTPException(status_code=400, detail=f"Entity type '{entity_name}' not found")
        
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
        get_method_name = "{self._get_method_name_for_entity(entity_name, 'get')}"
        get_method = getattr(writer, get_method_name)
        entity_data = get_method(result_urn)
        
        return models.{entity_name}Response(
            urn=result_urn,
            properties=entity_data or {{}},
            last_updated=entity_data.get('lastUpdated') if entity_data else None
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
'''
        
        # Generate aspect UPSERT routes dynamically from registry
        aspects = self.factory.registry.get('aspects', {})
        for aspect_name in sorted(aspects.keys()):
            method_name = self._get_method_name_for_aspect(aspect_name, "upsert")
            routes_content += f'''
@router.post("/aspects/{aspect_name}", response_model=models.{aspect_name.title()}AspectResponse)
async def upsert_{aspect_name}_aspect(request: models.{aspect_name.title()}AspectUpsertRequest):
    """Upsert {aspect_name} aspect"""
    try:
        factory = factory_wrapper.get_factory_instance()
        writer = factory_wrapper.get_writer_instance()
        
        method_name = "{method_name}"
        if not hasattr(writer, method_name):
            raise HTTPException(status_code=400, detail=f"Aspect '{aspect_name}' not found")
        
        method = getattr(writer, method_name)
        
        # Prepare parameters - extract all fields except entity_label, entity_urn, entity_params, version, timestamp_ms
        params = {{
            "entity_label": request.entity_label,
            "entity_urn": request.entity_urn
        }}
        
        # Add all aspect-specific fields to payload
        aspect_config = factory.registry.get('aspects', {{}}).get('{aspect_name}', {{}})
        aspect_properties = aspect_config.get('properties', [])
        
        payload = {{}}
        for prop in aspect_properties:
            if hasattr(request, prop) and getattr(request, prop) is not None:
                payload[prop] = getattr(request, prop)
        
        params["payload"] = payload
        
        # Add optional parameters - only for versioned aspects
        aspect_config = factory.registry.get('aspects', {{}}).get('{aspect_name}', {{}})
        aspect_type = aspect_config.get('type', 'versioned')
        
        if aspect_type == 'versioned' and hasattr(request, 'version') and request.version is not None:
            params["version"] = request.version
        
        # Add entity creation parameters
        entity_params = request.entity_params
        if entity_params:
            params.update(entity_params)
        
        # Call the generated method
        result = method(**params)
        
        return models.{aspect_name.title()}AspectResponse(
            entity_label=request.entity_label or "unknown",
            entity_urn=request.entity_urn or "unknown",
            aspect_name="{aspect_name}",
            payload=payload,
            version=request.version if aspect_type == 'versioned' and hasattr(request, 'version') else None
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
'''
        
        # Write routes file
        with open(self.output_dir / "upsert_routes.py", "w") as f:
            f.write(routes_content)
        
        print(f"✅ Generated upsert_routes.py")
    
    def _generate_delete_routes(self):
        """Generate DELETE routes"""
        routes_content = '''#!/usr/bin/env python3
"""
DELETE routes for the generated API
"""

from fastapi import APIRouter, HTTPException
from typing import Any, Dict
import models
import factory_wrapper


router = APIRouter(prefix="/api/v1", tags=["DELETE Operations"])


# Entity DELETE routes
'''
        
        # Generate entity DELETE routes dynamically from registry
        entities = self.factory.registry.get('entities', {})
        for entity_name in sorted(entities.keys()):
            method_name = self._get_method_name_for_entity(entity_name, "delete")
            routes_content += f'''
@router.delete("/entities/{entity_name}/{{urn}}")
async def delete_{entity_name}(urn: str):
    """Delete {entity_name} entity by URN"""
    try:
        factory = factory_wrapper.get_factory_instance()
        writer = factory_wrapper.get_writer_instance()
        
        method_name = "{method_name}"
        if not hasattr(writer, method_name):
            raise HTTPException(status_code=400, detail=f"Entity type '{entity_name}' not found")
        
        method = getattr(writer, method_name)
        method(urn)
        
        return {{"message": f"{entity_name} with URN '{{urn}}' deleted successfully"}}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
'''
        
        # Generate aspect DELETE routes dynamically from registry
        aspects = self.factory.registry.get('aspects', {})
        for aspect_name in sorted(aspects.keys()):
            method_name = self._get_method_name_for_aspect(aspect_name, "delete")
            routes_content += f'''
@router.delete("/aspects/{aspect_name}/{{entity_label}}/{{entity_urn}}")
async def delete_{aspect_name}_aspect(entity_label: str, entity_urn: str):
    """Delete {aspect_name} aspect for entity"""
    try:
        factory = factory_wrapper.get_factory_instance()
        writer = factory_wrapper.get_writer_instance()
        
        method_name = "{method_name}"
        if not hasattr(writer, method_name):
            raise HTTPException(status_code=400, detail=f"Aspect '{aspect_name}' not found")
        
        method = getattr(writer, method_name)
        method(entity_label, entity_urn)
        
        return {{"message": f"{aspect_name} aspect deleted successfully for entity '{{entity_urn}}'"}}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
'''
        
        # Write routes file
        with open(self.output_dir / "delete_routes.py", "w") as f:
            f.write(routes_content)
        
        print(f"✅ Generated delete_routes.py")
    
    def _generate_factory_wrapper(self):
        """Generate factory wrapper for dependency injection"""
        wrapper_content = '''#!/usr/bin/env python3
"""
Factory wrapper for dependency injection
"""

import os
import sys
from typing import Optional

# Add the parent directory to sys.path to import the registry module
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

try:
    from lineagentic_kg.registry.factory import RegistryFactory
except ImportError:
    # Fallback for when running as standalone
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
    from lineagentic_kg.registry.factory import RegistryFactory


class FactoryWrapper:
    """Wrapper for RegistryFactory with singleton pattern"""
    
    _instance: Optional['FactoryWrapper'] = None
    _factory: Optional[RegistryFactory] = None
    _writer = None
    
    def __init__(self):
        # Try multiple possible registry paths
        possible_paths = [
            os.getenv("REGISTRY_PATH"),
            "config/main_registry.yaml",  # Local config directory (copied during generation)
            os.path.join(os.path.dirname(__file__), "config", "main_registry.yaml"),  # Relative to this file
            "../config/main_registry.yaml",
            "../../config/main_registry.yaml",
            os.path.join(os.path.dirname(__file__), "..", "config", "main_registry.yaml"),
            os.path.join(os.path.dirname(__file__), "..", "..", "config", "main_registry.yaml")
        ]
        
        registry_path = None
        for path in possible_paths:
            if path and os.path.exists(path):
                registry_path = path
                break
        
        if not registry_path:
            raise FileNotFoundError(f"Registry file not found. Tried paths: {possible_paths}")
        
        self._factory = RegistryFactory(registry_path)
        
        # Set methods based on registry configuration
        self._factory.entity_methods = list(self._factory.registry.get('entities', {}).keys())
        self._factory.aspect_methods = list(self._factory.registry.get('aspects', {}).keys())
        self._factory.utility_methods = list(self._factory.registry.get('utility_functions', {}).keys())
    

    
    def get_writer_instance(self):
        """Get or create writer instance"""
        if self._writer is None:
            uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
            user = os.getenv("NEO4J_USER", "neo4j")
            password = os.getenv("NEO4J_PASSWORD", "password")
            self._writer = self._factory.create_writer(uri, user, password)
        return self._writer
    
    @classmethod
    def get_instance(cls) -> 'FactoryWrapper':
        """Get singleton instance"""
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance


def get_factory_instance() -> RegistryFactory:
    """Get factory instance for dependency injection"""
    return FactoryWrapper.get_instance()._factory


def get_writer_instance():
    """Get writer instance for dependency injection"""
    return FactoryWrapper.get_instance().get_writer_instance()
'''
        
        # Write factory wrapper file
        with open(self.output_dir / "factory_wrapper.py", "w") as f:
            f.write(wrapper_content)
        
        print(f"✅ Generated factory_wrapper.py")
    
    def _generate_main_app(self):
        """Generate main FastAPI application"""
        app_content = '''#!/usr/bin/env python3
"""
Main FastAPI application
"""

import os
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import models
import get_routes
import upsert_routes
import delete_routes


def create_app() -> FastAPI:
    """Create and configure FastAPI application"""
    app = FastAPI(
        title="RegistryFactory Generated API",
        description="Auto-generated API from RegistryFactory methods",
        version="1.0.0"
    )
    
    # Add CORS middleware
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    
    # Include routers
    app.include_router(get_routes.router)
    app.include_router(upsert_routes.router)
    app.include_router(delete_routes.router)
    
    return app


# Create app instance
app = create_app()


if __name__ == "__main__":
    import uvicorn
    
    # Get configuration from environment
    host = os.getenv("API_HOST", "0.0.0.0")
    port = int(os.getenv("API_PORT", "8000"))
    
    print(f"🚀 Starting RegistryFactory API server on {host}:{port}")
    print(f" API Documentation: http://{host}:{port}/docs")
    
    uvicorn.run(app, host=host, port=port)
'''
        
        # Write main app file
        with open(self.output_dir / "main.py", "w") as f:
            f.write(app_content)
        
        print(f"✅ Generated main.py")
    
    def _generate_requirements(self):
        """Generate requirements.txt"""
        requirements_content = '''fastapi==0.116.1
uvicorn[standard]==0.35.0
pydantic==2.11.7
neo4j==5.28.2
python-dotenv==1.1.1
PyYAML==6.0.2
'''
        
        with open(self.output_dir / "requirements.txt", "w") as f:
            f.write(requirements_content)
        
        print(f"✅ Generated requirements.txt")
    
    def _generate_readme(self):
        """Generate README.md"""
        readme_content = f'''# RegistryFactory Generated API

This is an auto-generated FastAPI application based on the RegistryFactory methods.

## Generated Endpoints

### Entities
'''
        
        # Add entity endpoints dynamically from registry
        entities = self.factory.registry.get('entities', {})
        for entity_name in sorted(entities.keys()):
            readme_content += f'''
#### {entity_name.title()}
- `GET /api/v1/entities/{entity_name}/{{urn}}` - Get {entity_name} by URN
- `POST /api/v1/entities/{entity_name}` - Upsert {entity_name}
- `DELETE /api/v1/entities/{entity_name}/{{urn}}` - Delete {entity_name} by URN
'''
        
        readme_content += '''
### Aspects
'''
        
        # Add aspect endpoints dynamically from registry
        aspects = self.factory.registry.get('aspects', {})
        for aspect_name in sorted(aspects.keys()):
            readme_content += f'''
#### {aspect_name.title()}
- `GET /api/v1/aspects/{aspect_name}/{{entity_label}}/{{entity_urn}}` - Get {aspect_name} aspect
- `POST /api/v1/aspects/{aspect_name}` - Upsert {aspect_name} aspect
- `DELETE /api/v1/aspects/{aspect_name}/{{entity_label}}/{{entity_urn}}` - Delete {aspect_name} aspect
'''
        
        readme_content += '''
### Health Check
- `GET /api/v1/health` - Health check endpoint

## Setup

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Set environment variables (optional - the API will auto-detect the registry path):
```bash
export REGISTRY_PATH="config/main_registry.yaml"  # Optional - auto-detected if not set
export NEO4J_URI="bolt://localhost:7687"
export NEO4J_USER="neo4j"
export NEO4J_PASSWORD="password"
export API_HOST="0.0.0.0"
export API_PORT="8000"
```

3. Run the API:
```bash
python main.py
```

4. Access the API:
- API Documentation: http://localhost:8000/docs
- Health Check: http://localhost:8000/api/v1/health

## Generated Files

- `main.py` - Main FastAPI application
- `models.py` - Pydantic models for requests/responses
- `get_routes.py` - GET operation routes
- `upsert_routes.py` - POST/UPSERT operation routes
- `delete_routes.py` - DELETE operation routes
- `factory_wrapper.py` - RegistryFactory wrapper for dependency injection
- `requirements.txt` - Python dependencies
- `README.md` - This file
- `config/` - Directory containing all required YAML configuration files
'''
        
        with open(self.output_dir / "README.md", "w") as f:
            f.write(readme_content)
        
        print(f"✅ Generated README.md")

    def _copy_config_files(self):
        """Copy required YAML config files to the generated API directory"""
        log_function_call(self.logger, "_copy_config_files")
        
        try:
            # Create config directory in output
            config_dir = self.output_dir / "config"
            config_dir.mkdir(exist_ok=True)
            
            # Get the source config directory path
            # Try to find the config directory relative to the registry path
            registry_path = Path(self.registry_path)
            source_config_dir = None
            
            # Try different possible locations for the config directory
            possible_paths = [
                registry_path.parent,  # Same directory as registry file
                Path("lineagentic_kg/config"),  # Relative to current working directory
                Path(__file__).parent.parent / "config",  # Relative to this file
            ]
            
            for path in possible_paths:
                if path.exists() and (path / "main_registry.yaml").exists():
                    source_config_dir = path
                    break
            
            if not source_config_dir:
                # If we can't find the config directory, try to copy from the package
                import lineagentic_kg
                package_dir = Path(lineagentic_kg.__file__).parent
                source_config_dir = package_dir / "config"
            
            if not source_config_dir.exists():
                self.logger.warning("Could not find source config directory", 
                                  source_config_dir=str(source_config_dir))
                return
            
            self.logger.info("Copying config files", 
                           source_dir=str(source_config_dir), 
                           target_dir=str(config_dir))
            
            # List of required YAML files to copy
            required_files = [
                "main_registry.yaml",
                "entities.yaml", 
                "aspects.yaml",
                "relationships.yaml",
                "urn_patterns.yaml",
                "utilities.yaml",
                "core.yaml"
            ]
            
            copied_files = []
            for filename in required_files:
                source_file = source_config_dir / filename
                target_file = config_dir / filename
                
                if source_file.exists():
                    import shutil
                    shutil.copy2(source_file, target_file)
                    copied_files.append(filename)
                    self.logger.debug("Copied config file", 
                                    source=str(source_file), 
                                    target=str(target_file))
                else:
                    self.logger.warning("Config file not found", 
                                      filename=filename, 
                                      source_dir=str(source_config_dir))
            
            self.logger.info("Config files copied successfully", 
                           copied_files=copied_files,
                           total_files=len(copied_files))
            
            print(f"✅ Copied {len(copied_files)} config files to {config_dir}")
            
        except Exception as e:
            log_error_with_context(self.logger, e, "_copy_config_files")
            self.logger.warning("Failed to copy config files, continuing with generation")


def main():
    """Main function to generate API files"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Generate FastAPI files from RegistryFactory")
    parser.add_argument("--registry-path", default="config/main_registry.yaml", 
                       help="Path to registry configuration file")
    parser.add_argument("--output-dir", default="generated_api", 
                       help="Output directory for generated files")
    
    args = parser.parse_args()
    
    generator = APIGenerator(args.registry_path, args.output_dir)
    generator.generate_all()


if __name__ == "__main__":
    main()