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
                self.registry_factory = registry_factory  # Add this line
                
                # Generate entity and relationship methods
                self._generate_entity_methods()
                self._generate_relationship_methods()
                self._generate_aspect_methods()
            
            def close(self):
                self._driver.close()
            
            def upsert_entity(self, label: str, urn: str, props: Dict[str, Any]) -> None:
                """Generic entity upsert method"""
                props = {k: v for k, v in props.items() if v is not None}
                with self._driver.session() as s:
                    s.run(
                        f"""
                        MERGE (e:{label} {{urn:$urn}})
                        SET e += $props, e.lastUpdated=$now
                        """,
                        urn=urn, props=props, now=self.utc_now_ms()
                    )
            
            def create_relationship(self, from_label: str, from_urn: str, rel: str,
                                  to_label: str, to_urn: str, props: Dict[str, Any]|None=None) -> None:
                """Generic relationship creation method"""
                props = props or {}
                print(f"DEBUG: Creating relationship {from_label}({from_urn}) -[{rel}]-> {to_label}({to_urn}) with props: {props}")
                with self._driver.session() as s:
                    result = s.run(
                        f"""
                        MATCH (a:{from_label} {{urn:$from_urn}})
                        MATCH (b:{to_label} {{urn:$to_urn}})
                        MERGE (a)-[r:{rel}]->(b)
                        SET r += $props
                        RETURN count(r) as created
                        """,
                        from_urn=from_urn, to_urn=to_urn, props=props
                    )
                    record = result.single()
                    print(f"DEBUG: Relationship creation result: {record}")
            
            def _validate_aspect(self, entity_label: str, aspect_name: str, kind: str):
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
            
            def _max_version(self, entity_label: str, entity_urn: str, aspect_name: str) -> int:
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
            
            def upsert_versioned_aspect(self, entity_label: str, entity_urn: str,
                                      aspect_name: str, payload: Dict[str, Any], version: int|None=None) -> int:
                """Upsert versioned aspect with validation"""
                self._validate_aspect(entity_label, aspect_name, "versioned")
                
                # Validate and enrich payload using the factory's method
                validated_payload = self.registry_factory.validate_aspect_payload(aspect_name, payload)
                
                current_max = self._max_version(entity_label, entity_urn, aspect_name)
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
                        json=json.dumps(validated_payload, ensure_ascii=False), now=self.utc_now_ms()
                    )
                return new_version
            
            def append_timeseries_aspect(self, entity_label: str, entity_urn: str,
                                       aspect_name: str, payload: Dict[str, Any], timestamp_ms: int|None=None) -> None:
                """Append timeseries aspect with validation"""
                self._validate_aspect(entity_label, aspect_name, "timeseries")
                
                # Validate and enrich payload using the factory's method
                validated_payload = self.registry_factory.validate_aspect_payload(aspect_name, payload)
                
                ts = timestamp_ms or self.utc_now_ms()
                aspect_id = f"{entity_urn}|{aspect_name}|{ts}"
                
                with self._driver.session() as s:
                    s.run(
                        f"""
                        MATCH (e:{entity_label} {{urn:$urn}})
                        CREATE (a:Aspect:TimeSeries {{id:$id, name:$an, ts:$ts, kind:'timeseries', json:$json, createdAt:$now}})
                        CREATE (e)-[:HAS_ASPECT {{name:$an, ts:$ts, kind:'timeseries'}}]->(a)
                        """,
                        urn=entity_urn, id=aspect_id, an=aspect_name, ts=ts,
                        json=json.dumps(validated_payload, ensure_ascii=False), now=self.utc_now_ms()
                    )
            
            def _generate_entity_methods(self):
                """Generate entity-specific convenience methods"""
                for entity_name, entity_def in self.registry.get('entities', {}).items():
                    urn_gen_name = entity_def.get('urn_generator')
                    if urn_gen_name and urn_gen_name in self.urn_generators:
                        urn_gen = self.urn_generators[urn_gen_name]
                        
                        def create_entity_method(entity_name, urn_gen, entity_def):
                            def entity_method(**kwargs):
                                urn = urn_gen(**kwargs)
                                props = {k: v for k, v in kwargs.items() if k in entity_def.get('properties', [])}
                                self.upsert_entity(entity_name, urn, props)
                                return urn
                            return entity_method
                        
                        method_name = f"upsert_{entity_name.lower()}"
                        setattr(self, method_name, create_entity_method(entity_name, urn_gen, entity_def))
            
            def _generate_relationship_methods(self):
                """Generate relationship-specific convenience methods"""
                for entity_name, entity_def in self.registry.get('entities', {}).items():
                    for rel in entity_def.get('relationships', []):
                        rel_type = rel['type']
                        target = rel['target']
                        direction = rel.get('direction', 'outgoing')
                        
                        # Create a unique method name for each relationship
                        method_name = f"create_{rel_type.lower()}_relationship"
                        
                        # Check if method already exists (for relationships with same type but different entities)
                        if hasattr(self, method_name):
                            # If method exists, create a more specific name
                            method_name = f"create_{rel_type.lower()}_{entity_name.lower()}_to_{target.lower()}_relationship"
                        
                        def create_rel_method(rel_type, entity_name, target, direction, rel, method_name):
                            def rel_method(from_urn: str, to_urn: str, props: Dict[str, Any]|None=None):
                                print(f"DEBUG: Called {rel_type} relationship method with from_urn={from_urn}, to_urn={to_urn}, direction={direction}")
                                print(f"DEBUG: Entity name: {entity_name}, Target: {target}")
                                if direction == 'outgoing':
                                    self.create_relationship(entity_name, from_urn, rel_type, target, to_urn, props)
                                else:
                                    self.create_relationship(target, to_urn, rel_type, entity_name, from_urn, props)
                            return rel_method
                        
                        setattr(self, method_name, create_rel_method(rel_type, entity_name, target, direction, rel, method_name))
            
            def _generate_aspect_methods(self):
                """Generate aspect-specific convenience methods"""
                for aspect_name, aspect_def in self.registry.get('aspects', {}).items():
                    aspect_type = aspect_def['type']
                    
                    def create_aspect_method(aspect_name, aspect_type, aspect_def):
                        if aspect_type == 'versioned':
                            def aspect_method(entity_label: str, entity_urn: str, payload: Dict[str, Any], version: int|None=None) -> int:
                                return self.upsert_versioned_aspect(entity_label, entity_urn, aspect_name, payload, version)
                        else:  # timeseries
                            def aspect_method(entity_label: str, entity_urn: str, payload: Dict[str, Any], timestamp_ms: int|None=None) -> None:
                                self.append_timeseries_aspect(entity_label, entity_urn, aspect_name, payload, timestamp_ms)
                        return aspect_method
                    
                    method_name = f"upsert_{aspect_name.lower()}_aspect"
                    setattr(self, method_name, create_aspect_method(aspect_name, aspect_type, aspect_def))
            
            # Add utility functions as instance methods
            def utc_now_ms(self) -> int:
                return self.utility_functions['utc_now_ms']()
            
            def sanitize_id(self, raw: str) -> str:
                return self.utility_functions['sanitize_id'](raw)
            
            def email_to_username(self, email: str) -> str:
                return self.utility_functions['email_to_username'](email)
            
            def mask_secret(self, k: str, v: str) -> str:
                return self.utility_functions['mask_secret'](k, v)
            
            # Add aspect validation method
            def validate_aspect_payload(self, aspect_name: str, payload: Dict[str, Any]) -> Dict[str, Any]:
                return self.registry_factory.validate_aspect_payload(aspect_name, payload)

            def get_aspect_version_history(self, entity_label: str, entity_urn: str, aspect_name: str) -> List[Dict[str, Any]]:
                """Get complete version history for a specific aspect"""
                with self._driver.session() as s:
                    result = s.run("""
                        MATCH (e:{entity_label} {{urn:$urn}})-[r:HAS_ASPECT {{name:$an, kind:'versioned'}}]->(a:Aspect:Versioned)
                        RETURN a.version as version, 
                               a.json as payload, 
                               a.createdAt as created_at,
                               r.latest as is_latest
                        ORDER BY a.version ASC
                    """.format(entity_label=entity_label), urn=entity_urn, an=aspect_name)
                    
                    history = []
                    for record in result:
                        history.append({
                            'version': record['version'],
                            'payload': json.loads(record['payload']) if record['payload'] else {},
                            'created_at': record['created_at'],
                            'is_latest': record['is_latest']
                        })
                    return history

            def get_latest_aspect_version(self, entity_label: str, entity_urn: str, aspect_name: str) -> Dict[str, Any]:
                """Get the latest version of a specific aspect"""
                with self._driver.session() as s:
                    result = s.run("""
                        MATCH (e:{entity_label} {{urn:$urn}})-[r:HAS_ASPECT {{name:$an, kind:'versioned', latest:true}}]->(a:Aspect:Versioned)
                        RETURN a.version as version, 
                               a.json as payload, 
                               a.createdAt as created_at
                    """.format(entity_label=entity_label), urn=entity_urn, an=aspect_name)
                    
                    record = result.single()
                    if record:
                        return {
                            'version': record['version'],
                            'payload': json.loads(record['payload']) if record['payload'] else {},
                            'created_at': record['created_at']
                        }
                    return None

            def get_aspect_version(self, entity_label: str, entity_urn: str, aspect_name: str, version: int) -> Dict[str, Any]:
                """Get a specific version of an aspect"""
                with self._driver.session() as s:
                    result = s.run("""
                        MATCH (e:{entity_label} {{urn:$urn}})-[r:HAS_ASPECT {{name:$an, kind:'versioned'}}]->(a:Aspect:Versioned {{version:$version}})
                        RETURN a.version as version, 
                               a.json as payload, 
                               a.createdAt as created_at,
                               r.latest as is_latest
                    """.format(entity_label=entity_label), urn=entity_urn, an=aspect_name, version=version)
                    
                    record = result.single()
                    if record:
                        return {
                            'version': record['version'],
                            'payload': json.loads(record['payload']) if record['payload'] else {},
                            'created_at': record['created_at'],
                            'is_latest': record['is_latest']
                        }
                    return None
        
        return DynamicNeo4jMetadataWriter
    
    def create_writer(self, uri: str, user: str, password: str) -> Any:
        """Create Neo4jMetadataWriter instance"""
        writer_class = self.generate_neo4j_writer_class()
        return writer_class(uri, user, password, self.registry, self.urn_generators, self.utility_functions, self)

