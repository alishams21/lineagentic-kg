#!/usr/bin/env python3
from __future__ import annotations

import datetime as dt
import json
import re
from typing import Any, Dict, List, Optional, Type, Callable
import yaml
from neo4j import GraphDatabase


class RegistryFactory:
    """Factory class that reads registry and generates dynamic Neo4jMetadataWriter"""
    
    def __init__(self, registry_path: str):
        self.registry_path = registry_path
        self.registry = self._load_registry()
        self.utility_functions = self._create_utility_functions()
        self.urn_generators = self._create_urn_generators()
    
    def _load_registry(self) -> Dict[str, Any]:
        """Load and validate registry YAML"""
        with open(self.registry_path, 'r') as f:
            registry = yaml.safe_load(f)
        self._validate_registry(registry)
        return registry
    
    def _validate_registry(self, registry: Dict[str, Any]) -> None:
        """Validate registry structure"""
        required_sections = ['entities', 'urn_patterns', 'aspect_types', 'aspects']
        for section in required_sections:
            if section not in registry:
                raise ValueError(f"Registry missing required section: {section}")
    
    def _create_utility_functions(self) -> Dict[str, Callable]:
        """Create utility functions"""
        def sanitize_id(raw: str) -> str:
            return re.sub(r"[^a-zA-Z0-9_.-]+", "_", raw.strip())
        
        def email_to_username(email: str) -> str:
            if "@" in email:
                return sanitize_id(email.split("@",1)[0])
            return sanitize_id(email)
        
        def mask_secret(k: str, v: str) -> str:
            if re.search(r"(pass|secret|key|token)", k, re.IGNORECASE):
                return "****"
            return v
        
        def utc_now_ms() -> int:
            return int(dt.datetime.now(dt.UTC).timestamp() * 1000)
        
        return {
            'sanitize_id': sanitize_id,
            'email_to_username': email_to_username,
            'mask_secret': mask_secret,
            'utc_now_ms': utc_now_ms
        }
    
    def _create_urn_generators(self) -> Dict[str, Callable]:
        """Create URN generator functions from registry patterns"""
        generators = {}
        
        for name, pattern in self.registry['urn_patterns'].items():
            template = pattern['template']
            parameters = pattern['parameters']
            sanitize_params = pattern.get('sanitize', [])
            defaults = pattern.get('defaults', {})
            transformations = pattern.get('transformations', {})
            conditional_logic = pattern.get('conditional_logic')
            conditional_rules = pattern.get('conditional_rules', {})
            dependencies = pattern.get('dependencies', [])
            
            def create_urn_generator(template, params, sanitize, defaults, transforms, 
                                   cond_logic, cond_rules, deps, pattern_name, utils):
                def urn_generator(**kwargs):
                    # Apply defaults
                    for param, default_val in defaults.items():
                        if param not in kwargs:
                            kwargs[param] = default_val
                    
                    # Apply transformations
                    for param, transform_func in transforms.items():
                        if param in kwargs:
                            kwargs[param] = utils[transform_func](kwargs[param])
                    
                    # Apply sanitization
                    for param in sanitize:
                        if param in kwargs and kwargs[param]:
                            kwargs[param] = utils['sanitize_id'](kwargs[param])
                    
                    # Handle dependencies (like data_platform_urn)
                    if deps:
                        for dep in deps:
                            if dep == 'dataPlatform' and 'platform' in kwargs:
                                kwargs['data_platform_urn'] = generators['dataPlatform'](platform=kwargs['platform'])
                    
                    # Handle conditional logic
                    if cond_logic and cond_logic in cond_rules:
                        rules = cond_rules[cond_logic]
                        if 'value' in kwargs and kwargs['value']:
                            kwargs[cond_logic] = rules['when_value_present'].format(value=kwargs['value'])
                        else:
                            kwargs[cond_logic] = rules['when_value_absent']
                    
                    # Format template
                    formatted = template.format(
                        prefix=self.registry['metadata']['urn_prefix'],
                        **kwargs
                    )
                    return formatted
                
                return urn_generator
            
            generators[name] = create_urn_generator(
                template, parameters, sanitize_params, defaults, transformations, 
                conditional_logic, conditional_rules, dependencies, name, self.utility_functions
            )
        
        return generators
    
    def validate_aspect_payload(self, aspect_name: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Validate and enrich aspect payload based on registry definition"""
        if aspect_name not in self.registry['aspects']:
            raise ValueError(f"Aspect '{aspect_name}' not defined in registry")
        
        aspect_def = self.registry['aspects'][aspect_name]
        aspect_type = aspect_def['type']
        properties = aspect_def.get('properties', [])
        required = aspect_def.get('required', [])
        defaults = aspect_def.get('defaults', {})
        
        # Apply defaults
        enriched_payload = payload.copy()
        for prop, default_val in defaults.items():
            if prop not in enriched_payload:
                enriched_payload[prop] = default_val
        
        # Validate required properties
        missing_props = [prop for prop in required if prop not in enriched_payload or enriched_payload[prop] is None]
        if missing_props:
            raise ValueError(f"Aspect '{aspect_name}' missing required properties: {missing_props}")
        
        # Filter to only include defined properties
        filtered_payload = {k: v for k, v in enriched_payload.items() if k in properties}
        
        return filtered_payload
    
    def generate_neo4j_writer_class(self) -> Type:
        """Generate Neo4jMetadataWriter class dynamically from registry"""
        
        class DynamicNeo4jMetadataWriter:
            def __init__(self, uri: str, user: str, password: str, registry: Dict[str, Any], 
                         urn_generators: Dict[str, Callable], utility_functions: Dict[str, Callable], registry_factory):
                self._driver = GraphDatabase.driver(uri, auth=(user, password))
                self.registry = registry
                self.urn_generators = urn_generators
                self.utility_functions = utility_functions
                self.registry_factory = registry_factory
                
                # Generate all methods from registry - fully generic approach
                self._generate_entity_methods()
                self._generate_relationship_methods()
                self._generate_aspect_methods()
                self._generate_utility_methods()
            
            def close(self):
                self._driver.close()
            
            def _generate_entity_methods(self):
                """Generate entity-specific methods from registry"""
                for entity_name, entity_def in self.registry.get('entities', {}).items():
                    urn_gen_name = entity_def.get('urn_generator')
                    if urn_gen_name and urn_gen_name in self.urn_generators:
                        urn_gen = self.urn_generators[urn_gen_name]
                        
                        # Generate upsert method
                        def create_upsert_method(entity_name, urn_gen, entity_def):
                            def upsert_method(**kwargs):
                                urn = urn_gen(**kwargs)
                                props = {k: v for k, v in kwargs.items() if k in entity_def.get('properties', [])}
                                self._upsert_entity_generic(entity_name, urn, props)
                                return urn
                            return upsert_method
                        
                        method_name = f"upsert_{entity_name.lower()}"
                        setattr(self, method_name, create_upsert_method(entity_name, urn_gen, entity_def))
                        
                        # Generate get method
                        def create_get_method(entity_name):
                            def get_method(urn: str):
                                return self._get_entity_generic(entity_name, urn)
                            return get_method
                        
                        method_name = f"get_{entity_name.lower()}"
                        setattr(self, method_name, create_get_method(entity_name))
                        
                        # Generate delete method
                        def create_delete_method(entity_name):
                            def delete_method(urn: str):
                                return self._delete_entity_generic(entity_name, urn)
                            return delete_method
                        
                        method_name = f"delete_{entity_name.lower()}"
                        setattr(self, method_name, create_delete_method(entity_name))
            
            def _generate_relationship_methods(self):
                """Generate relationship-specific methods from registry"""
                for entity_name, entity_def in self.registry.get('entities', {}).items():
                    for rel in entity_def.get('relationships', []):
                        rel_type = rel['type']
                        target = rel['target']
                        direction = rel.get('direction', 'outgoing')
                        properties = rel.get('properties', [])
                        entity_creation = rel.get('entity_creation')
                        
                        # Create a unique method name for each relationship
                        method_name = f"create_{rel_type.lower()}_relationship"
                        
                        # Check if method already exists (for relationships with same type but different entities)
                        if hasattr(self, method_name):
                            # If method exists, create a more specific name
                            method_name = f"create_{rel_type.lower()}_{entity_name.lower()}_to_{target.lower()}_relationship"
                        
                        def create_rel_method(rel_type, entity_name, target, direction, properties, method_name, entity_creation):
                            def rel_method(from_urn: str = None, to_urn: str = None, props: Dict[str, Any]|None=None, **entity_params):
                                # If URNs are not provided but entity_creation is defined, create entities first
                                if from_urn is None and entity_creation and 'source_entity' in entity_creation:
                                    from_urn = self._create_entity_if_needed(entity_creation['source_entity'], entity_params)
                                elif from_urn is None:
                                    raise ValueError(f"from_urn is required for relationship {rel_type}")
                                
                                if to_urn is None and entity_creation and 'target_entity' in entity_creation:
                                    to_urn = self._create_entity_if_needed(entity_creation['target_entity'], entity_params)
                                elif to_urn is None:
                                    raise ValueError(f"to_urn is required for relationship {rel_type}")
                                
                                # Filter properties to only include those defined in registry
                                if props:
                                    filtered_props = {k: v for k, v in props.items() if k in properties}
                                else:
                                    filtered_props = {}
                                
                                if direction == 'outgoing':
                                    self._create_relationship_generic(entity_name, from_urn, rel_type, target, to_urn, filtered_props)
                                else:
                                    self._create_relationship_generic(target, to_urn, rel_type, entity_name, from_urn, filtered_props)
                            return rel_method
                        
                        setattr(self, method_name, create_rel_method(rel_type, entity_name, target, direction, properties, method_name, entity_creation))
            
            def _generate_aspect_methods(self):
                """Generate aspect-specific methods from registry"""
                for aspect_name, aspect_def in self.registry.get('aspects', {}).items():
                    aspect_type = aspect_def['type']
                    entity_creation = aspect_def.get('entity_creation')
                    
                    # Generate upsert method with independent ingestion support
                    def create_upsert_aspect_method(aspect_name, aspect_type, entity_creation):
                        if aspect_type == 'versioned':
                            def aspect_method(entity_label: str = None, entity_urn: str = None, payload: Dict[str, Any] = None, version: int|None=None, **entity_params) -> int:
                                # If entity_urn is not provided but entity_creation is defined, create entity first
                                if entity_urn is None and entity_creation:
                                    entity_urn = self._create_entity_if_needed(entity_creation, entity_params)
                                    entity_label = entity_creation['entity_type']
                                elif entity_urn is None:
                                    raise ValueError(f"entity_urn is required for aspect {aspect_name}")
                                
                                return self._upsert_versioned_aspect_generic(entity_label, entity_urn, aspect_name, payload, version)
                        else:  # timeseries
                            def aspect_method(entity_label: str = None, entity_urn: str = None, payload: Dict[str, Any] = None, timestamp_ms: int|None=None, **entity_params) -> None:
                                # If entity_urn is not provided but entity_creation is defined, create entity first
                                if entity_urn is None and entity_creation:
                                    entity_urn = self._create_entity_if_needed(entity_creation, entity_params)
                                    entity_label = entity_creation['entity_type']
                                elif entity_urn is None:
                                    raise ValueError(f"entity_urn is required for aspect {aspect_name}")
                                
                                self._append_timeseries_aspect_generic(entity_label, entity_urn, aspect_name, payload, timestamp_ms)
                        return aspect_method
                    
                    method_name = f"upsert_{aspect_name.lower()}_aspect"
                    setattr(self, method_name, create_upsert_aspect_method(aspect_name, aspect_type, entity_creation))
                    
                    # Generate get method
                    def create_get_aspect_method(aspect_name, aspect_type):
                        if aspect_type == 'versioned':
                            def aspect_method(entity_label: str, entity_urn: str) -> Dict[str, Any]:
                                return self._get_latest_aspect_generic(entity_label, entity_urn, aspect_name)
                        else:  # timeseries
                            def aspect_method(entity_label: str, entity_urn: str, limit: int = 100) -> List[Dict[str, Any]]:
                                return self._get_timeseries_aspect_generic(entity_label, entity_urn, aspect_name, limit)
                        return aspect_method
                    
                    method_name = f"get_{aspect_name.lower()}_aspect"
                    setattr(self, method_name, create_get_aspect_method(aspect_name, aspect_type))
                    
                    # Generate delete method
                    def create_delete_aspect_method(aspect_name):
                        def aspect_method(entity_label: str, entity_urn: str) -> None:
                            return self._delete_aspect_generic(entity_label, entity_urn, aspect_name)
                        return aspect_method
                    
                    method_name = f"delete_{aspect_name.lower()}_aspect"
                    setattr(self, method_name, create_delete_aspect_method(aspect_name))
            
            def _generate_utility_methods(self):
                """Generate utility methods from utility functions"""
                for func_name, func in self.utility_functions.items():
                    setattr(self, func_name, func)
            
            # Generic core methods - these are the foundation for all generated methods
            def _upsert_entity_generic(self, label: str, urn: str, props: Dict[str, Any]) -> None:
                """Generic entity upsert method"""
                props = {k: v for k, v in props.items() if v is not None}
                with self._driver.session() as s:
                    s.run(
                        f"""
                        MERGE (e:{label} {{urn:$urn}})
                        SET e += $props, e.lastUpdated=$now
                        """,
                        urn=urn, props=props, now=self.utility_functions['utc_now_ms']()
                    )
            
            def _get_entity_generic(self, label: str, urn: str) -> Dict[str, Any]:
                """Generic entity get method"""
                with self._driver.session() as s:
                    result = s.run(
                        f"""
                        MATCH (e:{label} {{urn:$urn}})
                        RETURN e
                        """,
                        urn=urn
                    )
                    record = result.single()
                    return dict(record['e']) if record else None
            
            def _delete_entity_generic(self, label: str, urn: str) -> None:
                """Generic entity delete method"""
                with self._driver.session() as s:
                    s.run(
                        f"""
                        MATCH (e:{label} {{urn:$urn}})
                        DETACH DELETE e
                        """,
                        urn=urn
                    )
            
            def _create_relationship_generic(self, from_label: str, from_urn: str, rel: str,
                                          to_label: str, to_urn: str, props: Dict[str, Any]|None=None) -> None:
                """Generic relationship creation method"""
                props = props or {}
                with self._driver.session() as s:
                    s.run(
                        f"""
                        MATCH (a:{from_label} {{urn:$from_urn}})
                        MATCH (b:{to_label} {{urn:$to_urn}})
                        MERGE (a)-[r:{rel}]->(b)
                        SET r += $props
                        """,
                        from_urn=from_urn, to_urn=to_urn, props=props
                    )
            
            def _validate_aspect_generic(self, entity_label: str, aspect_name: str, kind: str):
                """Validate aspect against registry"""
                ents = self.registry.get("entities", {})
                ent = ents.get(entity_label, {})
                aspects = ent.get("aspects", {})
                allowed = aspects.get(aspect_name)
                if allowed != kind:
                    raise ValueError(f"Aspect '{aspect_name}' not allowed as '{kind}' on entity '{entity_label}' (registry says: {allowed})")
                
                # Also validate that aspect is defined in aspects section
                if aspect_name not in self.registry.get("aspects", {}):
                    raise ValueError(f"Aspect '{aspect_name}' not defined in registry aspects section")
            
            def _max_version_generic(self, entity_label: str, entity_urn: str, aspect_name: str) -> int:
                """Get max version for versioned aspect"""
                with self._driver.session() as s:
                    res = s.run(
                        f"""
                        MATCH (e:{entity_label} {{urn:$urn}})-[:HAS_ASPECT {{name:$an}}]->(a:Aspect:Versioned)
                        RETURN coalesce(max(a.version), -1) AS maxv
                        """,
                        urn=entity_urn, an=aspect_name
                    )
                    rec = res.single()
                    return rec["maxv"] if rec else -1
            
            def _upsert_versioned_aspect_generic(self, entity_label: str, entity_urn: str,
                                               aspect_name: str, payload: Dict[str, Any], version: int|None=None) -> int:
                """Generic versioned aspect upsert method"""
                self._validate_aspect_generic(entity_label, aspect_name, "versioned")
                
                # Validate and enrich payload using the factory's method
                validated_payload = self.registry_factory.validate_aspect_payload(aspect_name, payload)
                
                current_max = self._max_version_generic(entity_label, entity_urn, aspect_name)
                new_version = current_max + 1 if version is None else version
                aspect_id = f"{entity_urn}|{aspect_name}|{new_version}"
                
                with self._driver.session() as s:
                    s.run(
                        f"""
                        MATCH (e:{entity_label} {{urn:$urn}})-[r:HAS_ASPECT {{name:$an, kind:'versioned', latest:true}}]->(:Aspect)
                        SET r.latest=false
                        """,
                        urn=entity_urn, an=aspect_name
                    )
                    s.run(
                        f"""
                        MATCH (e:{entity_label} {{urn:$urn}})
                        CREATE (a:Aspect:Versioned {{id:$id, name:$an, version:$ver, kind:'versioned', json:$json, createdAt:$now}})
                        CREATE (e)-[:HAS_ASPECT {{name:$an, version:$ver, latest:true, kind:'versioned'}}]->(a)
                        """,
                        urn=entity_urn, id=aspect_id, an=aspect_name, ver=new_version,
                        json=json.dumps(validated_payload, ensure_ascii=False), now=self.utility_functions['utc_now_ms']()
                    )
                return new_version
            
            def _append_timeseries_aspect_generic(self, entity_label: str, entity_urn: str,
                                                aspect_name: str, payload: Dict[str, Any], timestamp_ms: int|None=None) -> None:
                """Generic timeseries aspect append method"""
                self._validate_aspect_generic(entity_label, aspect_name, "timeseries")
                
                # Validate and enrich payload using the factory's method
                validated_payload = self.registry_factory.validate_aspect_payload(aspect_name, payload)
                
                ts = timestamp_ms or self.utility_functions['utc_now_ms']()
                aspect_id = f"{entity_urn}|{aspect_name}|{ts}"
                
                with self._driver.session() as s:
                    s.run(
                        f"""
                        MATCH (e:{entity_label} {{urn:$urn}})
                        CREATE (a:Aspect:TimeSeries {{id:$id, name:$an, ts:$ts, kind:'timeseries', json:$json, createdAt:$now}})
                        CREATE (e)-[:HAS_ASPECT {{name:$an, ts:$ts, kind:'timeseries'}}]->(a)
                        """,
                        urn=entity_urn, id=aspect_id, an=aspect_name, ts=ts,
                        json=json.dumps(validated_payload, ensure_ascii=False), now=self.utility_functions['utc_now_ms']()
                    )
            
            def _get_latest_aspect_generic(self, entity_label: str, entity_urn: str, aspect_name: str) -> Dict[str, Any]:
                """Generic method to get latest version of an aspect"""
                with self._driver.session() as s:
                    result = s.run(
                        f"""
                        MATCH (e:{entity_label} {{urn:$urn}})-[r:HAS_ASPECT {{name:$an, kind:'versioned', latest:true}}]->(a:Aspect:Versioned)
                        RETURN a.version as version, 
                               a.json as payload, 
                               a.createdAt as created_at
                        """,
                        urn=entity_urn, an=aspect_name
                    )
                    
                    record = result.single()
                    if record:
                        return {
                            'version': record['version'],
                            'payload': json.loads(record['payload']) if record['payload'] else {},
                            'created_at': record['created_at']
                        }
                    return None
            
            def _get_timeseries_aspect_generic(self, entity_label: str, entity_urn: str, aspect_name: str, limit: int = 100) -> List[Dict[str, Any]]:
                """Generic method to get timeseries aspect data"""
                with self._driver.session() as s:
                    result = s.run(
                        f"""
                        MATCH (e:{entity_label} {{urn:$urn}})-[r:HAS_ASPECT {{name:$an, kind:'timeseries'}}]->(a:Aspect:TimeSeries)
                        RETURN a.ts as timestamp, 
                               a.json as payload, 
                               a.createdAt as created_at
                        ORDER BY a.ts DESC
                        LIMIT $limit
                        """,
                        urn=entity_urn, an=aspect_name, limit=limit
                    )
                    
                    timeseries_data = []
                    for record in result:
                        timeseries_data.append({
                            'timestamp': record['timestamp'],
                            'payload': json.loads(record['payload']) if record['payload'] else {},
                            'created_at': record['created_at']
                        })
                    return timeseries_data
            
            def _delete_aspect_generic(self, entity_label: str, entity_urn: str, aspect_name: str) -> None:
                """Generic method to delete an aspect"""
                with self._driver.session() as s:
                    s.run(
                        f"""
                        MATCH (e:{entity_label} {{urn:$urn}})-[r:HAS_ASPECT {{name:$an}}]->(a:Aspect)
                        DELETE r, a
                        """,
                        urn=entity_urn, an=aspect_name
                    )
            
            def _create_entity_if_needed(self, entity_creation: Dict[str, Any], entity_params: Dict[str, Any]) -> str:
                """Create entity if it doesn't exist and return its URN"""
                entity_type = entity_creation['entity_type']
                urn_generator_name = entity_creation['urn_generator']
                required_params = entity_creation['required_params']
                optional_params = entity_creation.get('optional_params', [])
                
                # Validate required parameters
                missing_params = [param for param in required_params if param not in entity_params]
                if missing_params:
                    raise ValueError(f"Missing required parameters for {entity_type}: {missing_params}")
                
                # Extract parameters for URN generation
                urn_params = {}
                for param in required_params + optional_params:
                    if param in entity_params:
                        urn_params[param] = entity_params[param]
                
                # Generate URN
                urn_generator = self.urn_generators[urn_generator_name]
                entity_urn = urn_generator(**urn_params)
                
                # Create entity if it doesn't exist
                entity_props = {k: v for k, v in entity_params.items() if v is not None}
                self._upsert_entity_generic(entity_type, entity_urn, entity_props)
                
                return entity_urn
        
        return DynamicNeo4jMetadataWriter
    
    def create_writer(self, uri: str, user: str, password: str) -> Any:
        """Create Neo4jMetadataWriter instance"""
        writer_class = self.generate_neo4j_writer_class()
        return writer_class(uri, user, password, self.registry, self.urn_generators, self.utility_functions, self)


def main():
    """Comprehensive example demonstrating all entities, aspects, and relationships from enhanced_registry.yaml"""
    print("🚀 Comprehensive RegistryFactory Example")
    print("=" * 60)
    
    # Configuration
    registry_path = "enhanced_registry.yaml"
    neo4j_uri = "bolt://localhost:7687"
    neo4j_user = "neo4j"
    neo4j_password = "password"
    
    try:
        # Create registry factory
        print("1. Creating RegistryFactory...")
        factory = RegistryFactory(registry_path)
        print(f"   ✅ Registry loaded from: {registry_path}")
        print(f"   📊 Entities found: {list(factory.registry['entities'].keys())}")
        print(f"   📊 Aspects found: {list(factory.registry['aspects'].keys())}")
        
        # Create writer instance
        print("\n2. Creating Neo4j writer...")
        writer = factory.create_writer(neo4j_uri, neo4j_user, neo4j_password)
        print("   ✅ Writer created successfully")
        
        # ============================================================================
        # DYNAMIC METHOD GENERATION DEMONSTRATION
        # ============================================================================
        print("\n" + "="*60)
        print("DYNAMIC METHOD GENERATION DEMONSTRATION")
        print("="*60)
        
        print("\n📋 Generated Entity Methods:")
        print("   Entity CRUD Operations:")
        for entity_name in factory.registry['entities'].keys():
            entity_def = factory.registry['entities'][entity_name]
            properties = entity_def.get('properties', [])
            urn_generator = entity_def.get('urn_generator')
            
            # Get URN generator parameters
            urn_params = []
            if urn_generator and urn_generator in factory.registry['urn_patterns']:
                urn_params = factory.registry['urn_patterns'][urn_generator].get('parameters', [])
            
            # Show upsert method with actual parameters
            param_str = ", ".join(urn_params) if urn_params else "**kwargs"
            print(f"     ✅ writer.upsert_{entity_name.lower()}({param_str})")
            print(f"     ✅ writer.get_{entity_name.lower()}(urn: str)")
            print(f"     ✅ writer.delete_{entity_name.lower()}(urn: str)")
        
        print("\n📋 Generated Relationship Methods:")
        print("   Relationship Creation:")
        for entity_name, entity_def in factory.registry['entities'].items():
            for rel in entity_def.get('relationships', []):
                rel_type = rel['type']
                target = rel['target']
                direction = rel.get('direction', 'outgoing')
                properties = rel.get('properties', [])
                entity_creation = rel.get('entity_creation')
                
                # Create method name
                method_name = f"create_{rel_type.lower()}_relationship"
                if hasattr(writer, method_name):
                    method_name = f"create_{rel_type.lower()}_{entity_name.lower()}_to_{target.lower()}_relationship"
                
                # Show parameters based on entity creation configuration
                if entity_creation:
                    source_params = []
                    target_params = []
                    
                    if 'source_entity' in entity_creation:
                        source_params = entity_creation['source_entity'].get('required_params', [])
                    if 'target_entity' in entity_creation:
                        target_params = entity_creation['target_entity'].get('required_params', [])
                    
                    # Combine parameters for independent ingestion
                    all_params = list(set(source_params + target_params))
                    param_str = ", ".join([f"{p}=None" for p in all_params])
                    if properties:
                        param_str += f", props: Dict[str, Any] = None"
                    
                    print(f"     ✅ writer.{method_name}({param_str})  # Independent ingestion")
                else:
                    # Traditional parameters
                    param_str = "from_urn: str, to_urn: str"
                    if properties:
                        param_str += ", props: Dict[str, Any] = None"
                    print(f"     ✅ writer.{method_name}({param_str})  # Traditional")
        
        print("\n📋 Generated Aspect Methods:")
        print("   Aspect CRUD Operations:")
        for aspect_name, aspect_def in factory.registry['aspects'].items():
            aspect_type = aspect_def['type']
            entity_creation = aspect_def.get('entity_creation')
            
            # Show parameters based on entity creation configuration
            if entity_creation:
                required_params = entity_creation.get('required_params', [])
                optional_params = entity_creation.get('optional_params', [])
                all_params = required_params + optional_params
                
                param_str = "payload: Dict[str, Any]"
                if all_params:
                    param_str += ", " + ", ".join([f"{p}=None" for p in all_params])
                if aspect_type == 'versioned':
                    param_str += ", version: int = None"
                elif aspect_type == 'timeseries':
                    param_str += ", timestamp_ms: int = None"
                
                print(f"     ✅ writer.upsert_{aspect_name.lower()}_aspect({param_str})  # Independent ingestion")
            else:
                # Traditional parameters
                param_str = "entity_label: str, entity_urn: str, payload: Dict[str, Any]"
                if aspect_type == 'versioned':
                    param_str += ", version: int = None"
                elif aspect_type == 'timeseries':
                    param_str += ", timestamp_ms: int = None"
                print(f"     ✅ writer.upsert_{aspect_name.lower()}_aspect({param_str})  # Traditional")
            
            # Get and delete methods
            if aspect_type == 'versioned':
                print(f"     ✅ writer.get_{aspect_name.lower()}_aspect(entity_label: str, entity_urn: str) -> Dict[str, Any]")
            else:  # timeseries
                print(f"     ✅ writer.get_{aspect_name.lower()}_aspect(entity_label: str, entity_urn: str, limit: int = 100) -> List[Dict[str, Any]]")
            print(f"     ✅ writer.delete_{aspect_name.lower()}_aspect(entity_label: str, entity_urn: str)")
        
        print("\n📋 Generated Utility Methods:")
        print("   Utility Functions:")
        for func_name in factory.utility_functions.keys():
            print(f"     ✅ writer.{func_name}()")
        
        print("\n📋 Available Methods on Writer Instance:")
        print("   All dynamically generated methods:")
        generated_methods = [method for method in dir(writer) if not method.startswith('_') and callable(getattr(writer, method))]
        generated_methods.sort()
        for method in generated_methods:
            print(f"     🔧 writer.{method}()")
        
        print(f"\n📊 Total Generated Methods: {len(generated_methods)}")
        print("   (All methods are dynamically generated from registry configuration!)")
        
        print("\n📋 Usage Examples:")
        print("   Independent Aspect Ingestion:")
        print("     ✅ writer.upsert_datasetproperties_aspect(payload=props, platform='mysql', name='dataset')")
        print("     ✅ writer.upsert_corpuserinfo_aspect(payload=user_info, username='user@company.com')")
        print("     ✅ writer.upsert_schemametadata_aspect(payload=schema, platform='mysql', name='dataset')")
        
        print("\n   Independent Relationship Creation:")
        print("     ✅ writer.create_owns_corpuser_to_dataset_relationship(props={'via': 'ownership'}, username='user@company.com', platform='mysql', name='dataset')")
        print("     ✅ writer.create_upstream_of_relationship(props={'via': 'etl'}, platform='source', name='source_dataset', platform='target', name='target_dataset')")
        
        print("\n   Traditional Methods (still supported):")
        print("     ✅ writer.upsert_datasetproperties_aspect('Dataset', dataset_urn, payload)")
        print("     ✅ writer.create_owns_corpuser_to_dataset_relationship(user_urn, dataset_urn, props)")
        
        print("\n📋 Actual Generated Method Signatures:")
        print("   (Inspecting the writer object to show real method signatures)")
        
        # Show some key method signatures
        key_methods = [
            'upsert_dataset',
            'upsert_corpuser', 
            'upsert_datasetproperties_aspect',
            'upsert_corpuserinfo_aspect',
            'create_owns_corpuser_to_dataset_relationship',
            'create_upstream_of_relationship'
        ]
        
        for method_name in key_methods:
            if hasattr(writer, method_name):
                method = getattr(writer, method_name)
                import inspect
                try:
                    sig = inspect.signature(method)
                    print(f"     🔧 writer.{method_name}{sig}")
                except:
                    print(f"     🔧 writer.{method_name}(...)")
            else:
                print(f"     ❌ writer.{method_name} (not found)")
        
        print("\n📋 Registry Configuration Summary:")
        print(f"   • Entities: {len(factory.registry['entities'])}")
        print(f"   • Aspects: {len(factory.registry['aspects'])}")
        print(f"   • URN Patterns: {len(factory.registry['urn_patterns'])}")
        
        # Count independent ingestion capabilities
        independent_aspects = sum(1 for aspect in factory.registry['aspects'].values() if aspect.get('entity_creation'))
        independent_relationships = sum(1 for entity in factory.registry['entities'].values() 
                                      for rel in entity.get('relationships', []) if rel.get('entity_creation'))
        
        print(f"   • Aspects with Independent Ingestion: {independent_aspects}")
        print(f"   • Relationships with Independent Creation: {independent_relationships}")
        
        
        # ============================================================================
        # DATASET ENTITY EXAMPLES
        # ============================================================================
        print("\n" + "="*60)
        print("DATASET ENTITY EXAMPLES")
        print("="*60)
        
        # Create datasets
        print("\n3. Creating datasets...")
        raw_dataset_urn = writer.upsert_dataset(
            platform="mysql",
            name="raw_customer_data",
            env="PROD"
        )
        staging_dataset_urn = writer.upsert_dataset(
            platform="mysql",
            name="staging_customer_data",
            env="PROD"
        )
        final_dataset_urn = writer.upsert_dataset(
            platform="mysql",
            name="final_customer_data",
            env="PROD"
        )
        print(f"   ✅ Raw dataset: {raw_dataset_urn}")
        print(f"   ✅ Staging dataset: {staging_dataset_urn}")
        print(f"   ✅ Final dataset: {final_dataset_urn}")
        
        # Add schema metadata aspect
        print("\n4. Adding schema metadata...")
        schema_payload = {
            "schemaName": "customer_schema",
            "platform": "mysql",
            "version": 1,
            "fields": [
                {"fieldPath": "customer_id", "type": "INTEGER", "nullable": False},
                {"fieldPath": "customer_name", "type": "VARCHAR(255)", "nullable": True},
                {"fieldPath": "email", "type": "VARCHAR(255)", "nullable": True},
                {"fieldPath": "created_date", "type": "TIMESTAMP", "nullable": True}
            ],
            "primaryKeys": ["customer_id"]
        }
        version = writer.upsert_schemametadata_aspect("Dataset", raw_dataset_urn, schema_payload)
        print(f"   ✅ Schema metadata added (version: {version})")
        
        # Add dataset properties aspect
        print("\n5. Adding dataset properties...")
        properties_payload = {
            "description": "Raw customer data from MySQL database",
            "customProperties": {
                "source_system": "CRM",
                "refresh_frequency": "daily",
                "data_retention_days": 365
            },
            "tags": ["customer", "pii", "business_critical"],
            "externalUrl": "https://company.com/data/customer"
        }
        version = writer.upsert_datasetproperties_aspect("Dataset", raw_dataset_urn, properties_payload)
        print(f"   ✅ Dataset properties added (version: {version})")
        
        # Add dataset profile (timeseries aspect)
        print("\n6. Adding dataset profile...")
        profile_payload = {
            "rowCount": 10000,
            "columnCount": 4,
            "sizeInBytes": 2048000,
            "lastModified": factory.utility_functions['utc_now_ms'](),
            "partitionCount": 1
        }
        writer.upsert_datasetprofile_aspect("Dataset", raw_dataset_urn, profile_payload)
        print("   ✅ Dataset profile added")
        
        # ============================================================================
        # CORPUSER ENTITY EXAMPLES
        # ============================================================================
        print("\n" + "="*60)
        print("CORPUSER ENTITY EXAMPLES")
        print("="*60)
        
        # Create users
        print("\n7. Creating users...")
        user1_urn = writer.upsert_corpuser(username="john.doe@company.com")
        user2_urn = writer.upsert_corpuser(username="jane.smith@company.com")
        manager_urn = writer.upsert_corpuser(username="bob.manager@company.com")
        print(f"   ✅ User 1: {user1_urn}")
        print(f"   ✅ User 2: {user2_urn}")
        print(f"   ✅ Manager: {manager_urn}")
        
        # Add user info aspect
        print("\n8. Adding user info...")
        user_info_payload = {
            "active": True,
            "displayName": "John Doe",
            "email": "john.doe@company.com",
            "title": "Data Engineer",
            "department": "Engineering",
            "managerUrn": manager_urn,
            "skills": ["Python", "SQL", "Apache Spark"]
        }
        version = writer.upsert_corpuserinfo_aspect("CorpUser", user1_urn, user_info_payload)
        print(f"   ✅ User info added (version: {version})")
        
        # ============================================================================
        # CORPGROUP ENTITY EXAMPLES
        # ============================================================================
        print("\n" + "="*60)
        print("CORPGROUP ENTITY EXAMPLES")
        print("="*60)
        
        # Create groups
        print("\n9. Creating groups...")
        data_team_urn = writer.upsert_corpgroup(groupname="data_team")
        engineering_urn = writer.upsert_corpgroup(groupname="engineering")
        print(f"   ✅ Data team: {data_team_urn}")
        print(f"   ✅ Engineering: {engineering_urn}")
        
        # Add group info aspect
        print("\n10. Adding group info...")
        group_info_payload = {
            "name": "Data Team",
            "description": "Data engineering and analytics team",
            "email": "data-team@company.com",
            "slackChannel": "#data-team"
        }
        version = writer.upsert_corpgroupinfo_aspect("CorpGroup", data_team_urn, group_info_payload)
        print(f"   ✅ Group info added (version: {version})")
        
        # ============================================================================
        # TAG ENTITY EXAMPLES
        # ============================================================================
        print("\n" + "="*60)
        print("TAG ENTITY EXAMPLES")
        print("="*60)
        
        # Create tags
        print("\n11. Creating tags...")
        pii_tag_urn = writer.upsert_tag(key="PII", value="true")
        sensitive_tag_urn = writer.upsert_tag(key="SENSITIVE", value="")
        business_critical_tag_urn = writer.upsert_tag(key="BUSINESS_CRITICAL", value="true")
        print(f"   ✅ PII tag: {pii_tag_urn}")
        print(f"   ✅ Sensitive tag: {sensitive_tag_urn}")
        print(f"   ✅ Business critical tag: {business_critical_tag_urn}")
        
        # ============================================================================
        # COLUMN ENTITY EXAMPLES
        # ============================================================================
        print("\n" + "="*60)
        print("COLUMN ENTITY EXAMPLES")
        print("="*60)
        
        # Create columns
        print("\n12. Creating columns...")
        customer_id_col_urn = writer.upsert_column(dataset_urn=raw_dataset_urn, field_path="customer_id")
        customer_name_col_urn = writer.upsert_column(dataset_urn=raw_dataset_urn, field_path="customer_name")
        email_col_urn = writer.upsert_column(dataset_urn=raw_dataset_urn, field_path="email")
        print(f"   ✅ Customer ID column: {customer_id_col_urn}")
        print(f"   ✅ Customer name column: {customer_name_col_urn}")
        print(f"   ✅ Email column: {email_col_urn}")
        
        # Add column properties aspect
        print("\n13. Adding column properties...")
        column_props_payload = {
            "description": "Unique customer identifier",
            "dataType": "INTEGER",
            "nullable": False,
            "defaultValue": None
        }
        version = writer.upsert_columnproperties_aspect("Column", customer_id_col_urn, column_props_payload)
        print(f"   ✅ Column properties added (version: {version})")
        
        # Add transformation aspect
        print("\n14. Adding transformation aspect...")
        transformation_payload = {
            "inputColumns": ["raw_customer_id"],
            "transformationType": "IDENTITY",
            "sourceDataset": raw_dataset_urn,
            "targetDataset": staging_dataset_urn,
            "steps": [
                {
                    "type": "IDENTITY",
                    "description": "Direct mapping from raw to staging",
                    "config": {"mapping": "1:1"}
                }
            ],
            "notes": "Identity transformation for customer ID"
        }
        version = writer.upsert_transformation_aspect("Column", customer_id_col_urn, transformation_payload)
        print(f"   ✅ Transformation aspect added (version: {version})")
        
        # ============================================================================
        # DATAFLOW ENTITY EXAMPLES
        # ============================================================================
        print("\n" + "="*60)
        print("DATAFLOW ENTITY EXAMPLES")
        print("="*60)
        
        # Create data flow
        print("\n15. Creating data flow...")
        dataflow_urn = writer.upsert_dataflow(
            platform="airflow",
            flow_id="customer_etl_pipeline",
            namespace="customer_data",
            name="Customer ETL Pipeline",
            env="PROD"
        )
        print(f"   ✅ Data flow: {dataflow_urn}")
        
        # Add data flow info aspect
        print("\n16. Adding data flow info...")
        flow_info_payload = {
            "name": "Customer ETL Pipeline",
            "namespace": "customer_data",
            "description": "ETL pipeline for customer data processing",
            "version": "1.0.0"
        }
        version = writer.upsert_dataflowinfo_aspect("DataFlow", dataflow_urn, flow_info_payload)
        print(f"   ✅ Data flow info added (version: {version})")
        
        # ============================================================================
        # DATAJOB ENTITY EXAMPLES
        # ============================================================================
        print("\n" + "="*60)
        print("DATAJOB ENTITY EXAMPLES")
        print("="*60)
        
        # Create data job
        print("\n17. Creating data job...")
        datajob_urn = writer.upsert_datajob(
            flow_urn=dataflow_urn,
            job_name="customer_data_processing"
        )
        print(f"   ✅ Data job: {datajob_urn}")
        
        # Add data job info aspect (independent ingestion example)
        print("\n18. Adding data job info (independent ingestion)...")
        job_info_payload = {
            "name": "Customer Data Processing",
            "namespace": "customer_data",
            "versionId": "1.0.0",
            "integration": "airflow",
            "processingType": "batch",
            "jobType": "etl",
            "description": "Process customer data from raw to final"
        }
        # Method 1: Traditional way (requires entity to exist)
        version = writer.upsert_datajobinfo_aspect("DataJob", datajob_urn, job_info_payload)
        print(f"   ✅ Data job info added (version: {version})")
        
        # Method 2: Independent ingestion (creates entity if needed)
        print("\n18b. Adding data job info with independent ingestion...")
        independent_job_info_payload = {
            "name": "Independent Data Processing",
            "namespace": "independent_data",
            "versionId": "2.0.0",
            "integration": "spark",
            "processingType": "streaming",
            "jobType": "etl",
            "description": "Independent data processing job"
        }
        # This will create the DataJob entity automatically if it doesn't exist
        version = writer.upsert_datajobinfo_aspect(
            payload=independent_job_info_payload,
            flow_urn="urn:li:dataFlow:(airflow,independent_flow,PROD)",
            job_name="independent_processing"
        )
        print(f"   ✅ Independent data job info added (version: {version})")
        
        # Add documentation aspect
        print("\n19. Adding documentation...")
        doc_payload = {
            "description": "Customer data processing job documentation",
            "contentType": "markdown",
            "content": "# Customer Data Processing\n\nThis job processes customer data..."
        }
        version = writer.upsert_documentation_aspect("DataJob", datajob_urn, doc_payload)
        print(f"   ✅ Documentation added (version: {version})")
        
        # Add source code location aspect
        print("\n20. Adding source code location...")
        source_location_payload = {
            "type": "github",
            "url": "https://github.com/company/data-pipelines",
            "repo": "data-pipelines",
            "branch": "main",
            "path": "jobs/customer_processing.py"
        }
        version = writer.upsert_sourcecodelocation_aspect("DataJob", datajob_urn, source_location_payload)
        print(f"   ✅ Source code location added (version: {version})")
        
        # Add source code aspect
        print("\n21. Adding source code...")
        source_code_payload = {
            "language": "python",
            "snippet": "def process_customer_data():\n    # Process customer data\n    pass",
            "fullCode": "import pandas as pd\n\ndef process_customer_data():\n    # Full implementation\n    pass"
        }
        version = writer.upsert_sourcecode_aspect("DataJob", datajob_urn, source_code_payload)
        print(f"   ✅ Source code added (version: {version})")
        
        # Add environment properties aspect
        print("\n22. Adding environment properties...")
        env_props_payload = {
            "env": "PROD",
            "config": {
                "memory": "4GB",
                "cpu": "2 cores",
                "timeout": "3600s"
            }
        }
        version = writer.upsert_environmentproperties_aspect("DataJob", datajob_urn, env_props_payload)
        print(f"   ✅ Environment properties added (version: {version})")
        
        # Add data job input/output aspect
        print("\n23. Adding data job input/output...")
        io_payload = {
            "inputs": [raw_dataset_urn],
            "outputs": [final_dataset_urn]
        }
        version = writer.upsert_datajobinputoutput_aspect("DataJob", datajob_urn, io_payload)
        print(f"   ✅ Data job I/O added (version: {version})")
        
        # Add data job run (timeseries aspect)
        print("\n24. Adding data job run...")
        job_run_payload = {
            "eventType": "STARTED",
            "runId": "run_12345",
            "parent": None,
            "status": "RUNNING",
            "startTime": factory.utility_functions['utc_now_ms'](),
            "endTime": None
        }
        writer.upsert_datajobrun_aspect("DataJob", datajob_urn, job_run_payload)
        print("   ✅ Data job run added")
        
        # ============================================================================
        # RELATIONSHIP EXAMPLES
        # ============================================================================
        print("\n" + "="*60)
        print("RELATIONSHIP EXAMPLES")
        print("="*60)
        
        # Dataset relationships
        print("\n25. Creating dataset relationships...")
        writer.create_has_column_relationship(raw_dataset_urn, customer_id_col_urn)
        writer.create_has_column_relationship(raw_dataset_urn, customer_name_col_urn)
        writer.create_has_column_relationship(raw_dataset_urn, email_col_urn)
        print("   ✅ HAS_COLUMN relationships created")
        
        # Tag relationships
        print("\n26. Creating tag relationships...")
        writer.create_tagged_relationship(raw_dataset_urn, pii_tag_urn, {"source": "auto_detection"})
        writer.create_tagged_relationship(raw_dataset_urn, business_critical_tag_urn, {"source": "manual"})
        print("   ✅ TAGGED relationships created")
        
        # CorpUser relationships
        print("\n27. Creating corpuser relationships...")
        writer.create_owns_corpuser_to_dataset_relationship(user1_urn, raw_dataset_urn, {"via": "data_ownership"})
        writer.create_owns_corpgroup_to_dataset_relationship(data_team_urn, raw_dataset_urn, {"via": "team_ownership"})
        print("   ✅ OWNS relationships created")
        
        # Independent relationship creation example
        print("\n27b. Creating independent relationships...")
        # This will create both entities if they don't exist
        writer.create_owns_corpuser_to_dataset_relationship(
            props={"via": "independent_ownership"},
            username="independent.user@company.com",
            platform="mysql",
            name="independent_dataset"
        )
        print("   ✅ Independent OWNS relationship created")
        
        # Dataset lineage relationships
        print("\n27. Creating dataset lineage...")
        writer.create_upstream_of_relationship(raw_dataset_urn, staging_dataset_urn, {"via": "etl_job_1"})
        writer.create_upstream_of_relationship(staging_dataset_urn, final_dataset_urn, {"via": "etl_job_2"})
        print("   ✅ UPSTREAM_OF relationships created")
        
        # Ownership relationships
        print("\n28. Creating ownership relationships...")
        writer.create_owns_relationship(user1_urn, raw_dataset_urn, {"via": "data_ownership"})
        writer.create_owns_relationship(data_team_urn, raw_dataset_urn, {"via": "team_ownership"})
        print("   ✅ OWNS relationships created")
        
        # Data flow relationships
        print("\n29. Creating data flow relationships...")
        writer.create_has_job_relationship(dataflow_urn, datajob_urn)
        print("   ✅ HAS_JOB relationship created")
        
        # Data job relationships
        print("\n30. Creating data job relationships...")
        writer.create_consumes_relationship(datajob_urn, raw_dataset_urn)
        writer.create_produces_relationship(datajob_urn, final_dataset_urn)
        writer.create_owns_relationship(user1_urn, datajob_urn, {"via": "job_ownership"})
        print("   ✅ Data job relationships created")
        
        # Column lineage relationships
        print("\n31. Creating column lineage...")
        writer.create_derives_from_relationship(
            customer_id_col_urn, 
            customer_id_col_urn, 
            {
                "type": "TRANSFORM",
                "subtype": "IDENTITY",
                "description": "Direct mapping",
                "transformation": "IDENTITY",
                "source_dataset": raw_dataset_urn,
                "target_dataset": staging_dataset_urn
            }
        )
        print("   ✅ DERIVES_FROM relationship created")
        
        # ============================================================================
        # DATA RETRIEVAL EXAMPLES
        # ============================================================================
        print("\n" + "="*60)
        print("DATA RETRIEVAL EXAMPLES")
        print("="*60)
        
        # Retrieve entities
        print("\n32. Retrieving entities...")
        dataset_data = writer.get_dataset(raw_dataset_urn)
        user_data = writer.get_corpuser(user1_urn)
        group_data = writer.get_corpgroup(data_team_urn)
        print(f"   📊 Dataset: {dataset_data}")
        print(f"   📊 User: {user_data}")
        print(f"   📊 Group: {group_data}")
        
        # Retrieve aspects
        print("\n33. Retrieving aspects...")
        schema_data = writer.get_schemametadata_aspect("Dataset", raw_dataset_urn)
        user_info_data = writer.get_corpuserinfo_aspect("CorpUser", user1_urn)
        group_info_data = writer.get_corpgroupinfo_aspect("CorpGroup", data_team_urn)
        print(f"   📊 Schema: {schema_data}")
        print(f"   📊 User info: {user_info_data}")
        print(f"   📊 Group info: {group_info_data}")
        
        # Retrieve timeseries data
        print("\n34. Retrieving timeseries data...")
        profile_data = writer.get_datasetprofile_aspect("Dataset", raw_dataset_urn, limit=5)
        job_run_data = writer.get_datajobrun_aspect("DataJob", datajob_urn, limit=5)
        print(f"   📊 Profile data: {profile_data}")
        print(f"   📊 Job run data: {job_run_data}")
        
        # ============================================================================
        # INDEPENDENT INGESTION DEMONSTRATION
        # ============================================================================
        print("\n" + "="*60)
        print("INDEPENDENT INGESTION DEMONSTRATION")
        print("="*60)
        
        print("\n35. Independent Aspect Ingestion Examples:")
        
        # Independent dataset properties ingestion
        print("\n35a. Independent dataset properties ingestion...")
        dataset_props_payload = {
            "description": "Dataset created via independent aspect ingestion",
            "customProperties": {"source": "independent_ingestion"},
            "tags": ["independent", "demo"]
        }
        version = writer.upsert_datasetproperties_aspect(
            payload=dataset_props_payload,
            platform="postgresql",
            name="independent_dataset"
        )
        print(f"   ✅ Independent dataset properties added (version: {version})")
        
        # Independent schema metadata ingestion
        print("\n35b. Independent schema metadata ingestion...")
        schema_payload = {
            "schemaName": "independent_schema",
            "platform": "postgresql",
            "version": 1,
            "fields": [
                {"fieldPath": "id", "type": "INTEGER", "nullable": False},
                {"fieldPath": "name", "type": "VARCHAR(100)", "nullable": True}
            ],
            "primaryKeys": ["id"]
        }
        version = writer.upsert_schemametadata_aspect(
            payload=schema_payload,
            platform="postgresql",
            name="independent_dataset"
        )
        print(f"   ✅ Independent schema metadata added (version: {version})")
        
        # Independent corpuser info ingestion
        print("\n35c. Independent corpuser info ingestion...")
        user_info_payload = {
            "active": True,
            "displayName": "Independent User",
            "email": "independent.user@company.com",
            "title": "Data Engineer",
            "department": "Engineering"
        }
        version = writer.upsert_corpuserinfo_aspect(
            payload=user_info_payload,
            username="independent.user@company.com"
        )
        print(f"   ✅ Independent corpuser info added (version: {version})")
        
        print("\n36. Independent Relationship Creation Examples:")
        
        # Independent ownership relationship
        print("\n36a. Independent ownership relationship...")
        writer.create_owns_corpuser_to_dataset_relationship(
            props={"via": "independent_ownership"},
            username="independent.user@company.com",
            platform="postgresql",
            name="independent_dataset"
        )
        print("   ✅ Independent ownership relationship created")
        
        # Independent dataset lineage
        print("\n36b. Independent dataset lineage...")
        # Create source dataset first
        source_dataset_urn = writer.upsert_dataset(
            platform="mysql",
            name="source_dataset"
        )
        # Create relationship with target dataset
        writer.create_upstream_of_relationship(
            from_urn=source_dataset_urn,
            props={"via": "independent_etl"},
            platform="postgresql",
            name="target_dataset"
        )
        print("   ✅ Independent dataset lineage created")
        
        # Independent column relationship
        print("\n36c. Independent column relationship...")
        writer.create_has_column_relationship(
            platform="postgresql",
            name="independent_dataset",
            dataset_urn="urn:li:dataset:(postgresql,independent_dataset,PROD)",
            field_path="id"
        )
        print("   ✅ Independent column relationship created")
        
        print("\n✅ Comprehensive example completed successfully!")
        print("=" * 60)
        print("🎉 All entities, aspects, and relationships from enhanced_registry.yaml demonstrated!")
        print("\n🚀 NEW FEATURES:")
        print("   • Independent aspect ingestion: Create entities automatically when ingesting aspects")
        print("   • Independent relationship creation: Create both source and target entities automatically")
        print("   • Flexible parameter passing: Support both URN-based and parameter-based ingestion")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        print("Make sure Neo4j is running and accessible at the specified URI")
        import traceback
        traceback.print_exc()
    
    finally:
        # Clean up
        if 'writer' in locals():
            writer.close()
            print("🔌 Writer connection closed")


if __name__ == "__main__":
    main()

