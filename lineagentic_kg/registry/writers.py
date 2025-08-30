#!/usr/bin/env python3
from __future__ import annotations

import json
import re
from typing import Any, Dict, Type, Callable, List
from neo4j import GraphDatabase


class Neo4jWriterGenerator:
    """Generates dynamic Neo4jMetadataWriter class from registry"""
    
    def __init__(self, registry: Dict[str, Any], urn_generators: Dict[str, Callable], 
                 utility_functions: Dict[str, Callable], registry_factory: Any):
        self.registry = registry
        self.urn_generators = urn_generators
        self.utility_functions = utility_functions
        self.registry_factory = registry_factory
    
    def generate_class(self) -> Type:
        """Generate the complete Neo4jMetadataWriter class"""
        
        class DynamicNeo4jMetadataWriter:
            def __init__(self, uri: str, user: str, password: str, registry: Dict[str, Any], 
                         urn_generators: Dict[str, Callable], utility_functions: Dict[str, Callable], registry_factory):
                self._driver = GraphDatabase.driver(uri, auth=(user, password))
                self.registry = registry
                self.urn_generators = urn_generators
                self.utility_functions = utility_functions
                self.registry_factory = registry_factory
                
                # Generate all methods from registry
                self._generate_entity_methods()
                self._generate_relationship_discovery_methods()
                self._generate_aspect_methods()
                self._generate_utility_methods()
            
            def close(self):
                self._driver.close()
            
            def _generate_entity_methods(self):
                """Generate entity-specific methods from registry"""
                section_config = self.registry.get('section_config', {})
                entities_section = section_config.get('entities_section', 'entities')
                entity_config = self.registry.get('entity_config', {})
                urn_generator_field = entity_config.get('urn_generator_field', 'urn_generator')
                properties_field = entity_config.get('properties_field', 'properties')
                
                for entity_name, entity_def in self.registry.get(entities_section, {}).items():
                    urn_gen_name = entity_def.get(urn_generator_field)
                    if urn_gen_name and urn_gen_name in self.urn_generators:
                        urn_gen = self.urn_generators[urn_gen_name]
                        
                        # Generate upsert method
                        def create_upsert_method(entity_name, urn_gen, entity_def):
                            def upsert_method(**kwargs):
                                urn = urn_gen(**kwargs)
                                props = {k: v for k, v in kwargs.items() if k in entity_def.get(properties_field, [])}
                                self._upsert_entity_generic(entity_name, urn, props)
                                # Discover relationships from entity creation
                                self.discover_relationships_from_entity(entity_name, urn, props)
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
            
            def _generate_relationship_discovery_methods(self):
                """Generate relationship discovery methods from registry"""
                section_config = self.registry.get('section_config', {})
                aspect_relationships_section = section_config.get('aspect_relationships_section', 'aspect_relationships')
                relationship_config = self.registry.get('relationship_config', {})
                rules_field = relationship_config.get('rules_field', 'rules')
                entity_type_field = relationship_config.get('entity_type_field', 'entity_type')
                
                def discover_relationships_from_aspect(self, entity_urn: str, entity_type: str, aspect_name: str, aspect_data: Dict[str, Any]):
                    """Discover and create relationships from aspect data using YAML-driven rules"""
                    aspect_rules = self.registry.get(aspect_relationships_section, {}).get(aspect_name)
                    if not aspect_rules:
                        return
                    
                    # Get entity properties to include in relationship discovery
                    entity_props = self._get_entity_generic(entity_type, entity_urn)
                    if entity_props:
                        # Merge aspect data with entity properties for relationship discovery
                        combined_data = {**aspect_data, **entity_props}
                    else:
                        combined_data = aspect_data
                    
                    for rule in aspect_rules.get(rules_field, []):
                        if rule.get(entity_type_field) == entity_type:
                            self._apply_relationship_rule_generic(entity_urn, entity_type, combined_data, rule)
                
                setattr(self, 'discover_relationships_from_aspect', discover_relationships_from_aspect.__get__(self))
                
                def discover_relationships_from_entity(self, entity_type: str, entity_urn: str, entity_props: Dict[str, Any]):
                    """Discover and create relationships from entity properties using YAML-driven rules"""
                    print(f"🔍 Discovering relationships for entity {entity_type}:{entity_urn}")
                    print(f"🔍 Entity properties: {entity_props}")
                    
                    # Look for entity creation relationships (not aspect-driven)
                    for aspect_name, aspect_rules in self.registry.get(aspect_relationships_section, {}).items():
                        print(f"🔍 Checking aspect: {aspect_name}, ends with 'Creation': {aspect_name.endswith('Creation')}")
                        if aspect_name.endswith('Creation'):  # Only process entity creation relationships
                            print(f"🔍 Processing entity creation rule: {aspect_name}")
                            for rule in aspect_rules.get(rules_field, []):
                                print(f"🔍 Rule entity_type: {rule.get(entity_type_field)}, target: {entity_type}")
                                if rule.get(entity_type_field) == entity_type:
                                    print(f"🔍 Applying entity relationship rule for {entity_type}")
                                    self._apply_relationship_rule_generic(entity_urn, entity_type, entity_props, rule)
                
                setattr(self, 'discover_relationships_from_entity', discover_relationships_from_entity.__get__(self))
            
            def _generate_aspect_methods(self):
                """Generate aspect-specific methods from registry"""
                section_config = self.registry.get('section_config', {})
                aspects_section = section_config.get('aspects_section', 'aspects')
                aspect_config = self.registry.get('aspect_config', {})
                type_field = aspect_config.get('type_field', 'type')
                entity_creation_field = aspect_config.get('entity_creation_field', 'entity_creation')
                
                for aspect_name, aspect_def in self.registry.get(aspects_section, {}).items():
                    aspect_type = aspect_def[type_field]
                    entity_creation = aspect_def.get(entity_creation_field)
                    
                    # Generate upsert method with independent ingestion support
                    def create_upsert_aspect_method(aspect_name, aspect_type, entity_creation):
                        if aspect_type == 'versioned':
                            def aspect_method(entity_label: str = None, entity_urn: str = None, payload: Dict[str, Any] = None, version: int|None=None, **entity_params) -> int:
                                if entity_urn is None and entity_creation:
                                    entity_urn = self._create_entity_if_needed(entity_creation, entity_params)
                                    entity_label = entity_creation['entity_type']
                                elif entity_urn is None:
                                    raise ValueError(f"entity_urn is required for aspect {aspect_name}")
                                
                                # If payload is None, extract it from entity_params
                                if payload is None:
                                    # Get aspect properties from registry
                                    aspect_properties = self.registry.get('aspects', {}).get(aspect_name, {}).get('properties', [])
                                    payload = {}
                                    for prop in aspect_properties:
                                        if prop in entity_params:
                                            payload[prop] = entity_params[prop]
                                
                                result = self._upsert_versioned_aspect_generic(entity_label, entity_urn, aspect_name, payload, version)
                                self.discover_relationships_from_aspect(entity_urn, entity_label, aspect_name, payload)
                                return result
                        else:  # timeseries
                            def aspect_method(entity_label: str = None, entity_urn: str = None, payload: Dict[str, Any] = None, timestamp_ms: int|None=None, **entity_params) -> None:
                                if entity_urn is None and entity_creation:
                                    entity_urn = self._create_entity_if_needed(entity_creation, entity_params)
                                    entity_label = entity_creation['entity_type']
                                elif entity_urn is None:
                                    raise ValueError(f"entity_urn is required for aspect {aspect_name}")
                                
                                # If payload is None, extract it from entity_params
                                if payload is None:
                                    # Get aspect properties from registry
                                    aspect_properties = self.registry.get('aspects', {}).get(aspect_name, {}).get('properties', [])
                                    payload = {}
                                    for prop in aspect_properties:
                                        if prop in entity_params:
                                            payload[prop] = entity_params[prop]
                                
                                self._append_timeseries_aspect_generic(entity_label, entity_urn, aspect_name, payload, timestamp_ms)
                                self.discover_relationships_from_aspect(entity_urn, entity_label, aspect_name, payload)
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
            
            # Core generic methods
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
                
                missing_params = [param for param in required_params if param not in entity_params]
                if missing_params:
                    raise ValueError(f"Missing required parameters for {entity_type}: {missing_params}")
                
                urn_params = {}
                for param in required_params + optional_params:
                    if param in entity_params:
                        urn_params[param] = entity_params[param]
                
                urn_generator = self.urn_generators[urn_generator_name]
                entity_urn = urn_generator(**urn_params)
                
                entity_props = {k: v for k, v in entity_params.items() if v is not None}
                self._upsert_entity_generic(entity_type, entity_urn, entity_props)
                
                return entity_urn
            
            def _apply_relationship_rule_generic(self, entity_urn: str, entity_type: str, data: Dict[str, Any], rule: Dict[str, Any]):
                """Apply a single relationship rule to create relationships using generic configuration"""
                rule_config = self.registry.get('relationship_rule_config', {})
                relationship_type_field = rule_config.get('relationship_type_field', 'relationship_type')
                source_entity_field = rule_config.get('source_entity_field', 'source_entity')
                target_entity_field = rule_config.get('target_entity_field', 'target_entity')
                direction_field = rule_config.get('direction_field', 'direction')
                field_mapping_field = rule_config.get('field_mapping_field', 'field_mapping')
                additional_relationships_field = rule_config.get('additional_relationships_field', 'additional_relationships')
                
                relationship_type = rule[relationship_type_field]
                source_entity = rule[source_entity_field]
                target_entity = rule[target_entity_field]
                direction = rule.get(direction_field, rule_config.get('default_direction', 'outgoing'))
                field_mapping = rule[field_mapping_field]
                
                source_field = field_mapping[rule_config.get('source_field_name', 'source_field')]
                target_field = field_mapping.get(rule_config.get('target_field_name', 'target_field'), rule_config.get('default_target_field', 'urn'))
                
                # Generic field value extraction
                field_values = self._extract_field_values_generic(source_field, data)
                
                for field_value in field_values:
                    if field_value:
                        self._create_relationship_from_field_mapping_generic(
                            entity_urn, entity_type, data, rule, field_value
                        )
                
                # Handle additional relationships
                additional_relationships = rule.get(additional_relationships_field, [])
                for additional_rule in additional_relationships:
                    self._create_additional_relationship_generic(entity_urn, entity_type, data, additional_rule)
            
            def _extract_field_values_generic(self, source_field: str, data: Dict[str, Any]) -> List[Any]:
                """Generic field value extraction supporting arrays and direct fields"""
                array_separator = '[]'
                
                if array_separator in source_field:
                    base_field, sub_field = source_field.split(array_separator)
                    base_field = base_field.strip('.')
                    sub_field = sub_field.strip('.')
                    
                    array_data = data.get(base_field, [])
                    if not isinstance(array_data, list):
                        if isinstance(array_data, str) and ',' in array_data:
                            array_data = [item.strip() for item in array_data.split(',') if item.strip()]
                        elif array_data:
                            array_data = [array_data]
                        else:
                            array_data = []
                    
                    values = []
                    for item in array_data:
                        if sub_field:
                            if isinstance(item, dict) and sub_field in item:
                                values.append(item[sub_field])
                        else:
                            values.append(item)
                    return values
                else:
                    field_value = data.get(source_field)
                    if field_value is None:
                        return []
                    
                    # Handle comma-separated values generically
                    if isinstance(field_value, str) and ',' in field_value and not field_value.startswith('urn:'):
                        return [v.strip() for v in field_value.split(",") if v.strip()]
                    else:
                        return [field_value]
            
            def _create_relationship_from_field_mapping_generic(self, entity_urn: str, entity_type: str, data: Dict[str, Any], rule: Dict[str, Any], field_value: Any):
                """Create a relationship based on generic field mapping rule"""
                rule_config = self.registry.get('relationship_rule_config', {})
                relationship_type_field = rule_config.get('relationship_type_field', 'relationship_type')
                source_entity_field = rule_config.get('source_entity_field', 'source_entity')
                target_entity_field = rule_config.get('target_entity_field', 'target_entity')
                direction_field = rule_config.get('direction_field', 'direction')
                field_mapping_field = rule_config.get('field_mapping_field', 'field_mapping')
                
                relationship_type = rule[relationship_type_field]
                source_entity = rule[source_entity_field]
                target_entity = rule[target_entity_field]
                direction = rule.get(direction_field, rule_config.get('default_direction', 'outgoing'))
                field_mapping = rule[field_mapping_field]
                
                source_entity_type = field_mapping[rule_config.get('source_entity_type_name', 'source_entity_type')]
                target_entity_type = field_mapping[rule_config.get('target_entity_type_name', 'target_entity_type')]
                source_urn_field = field_mapping[rule_config.get('source_urn_field_name', 'source_urn_field')]
                target_urn_field = field_mapping[rule_config.get('target_urn_field_name', 'target_urn_field')]
                
                if direction == rule_config.get('outgoing_direction', 'outgoing'):
                    source_urn = entity_urn
                    target_urn = self._resolve_urn_generic(field_value, target_entity_type, target_urn_field, data, field_mapping, entity_urn)
                else:  # incoming
                    source_urn = self._resolve_urn_generic(field_value, source_entity_type, source_urn_field, data, field_mapping, entity_urn)
                    target_urn = entity_urn
                
                if source_urn and target_urn:
                    # Prevent self-relationships
                    if source_urn == target_urn:
                        print(f"   ⚠️ Skipped self-relationship: {source_urn} -> {relationship_type} -> {target_urn}")
                        return
                    
                    # Extract relationship properties from data if specified
                    relationship_props = {}
                    if 'relationship_properties' in field_mapping:
                        for prop_name, data_field in field_mapping['relationship_properties'].items():
                            if data_field in data:
                                relationship_props[prop_name] = data[data_field]
                    
                    # Check if relationship already exists to prevent duplicates
                    with self._driver.session() as s:
                        result = s.run(
                            f"MATCH (a:{source_entity_type} {{urn: $source_urn}})-[r:{relationship_type}]->(b:{target_entity_type} {{urn: $target_urn}}) RETURN r",
                            source_urn=source_urn, target_urn=target_urn
                        )
                        if not result.single():
                            self._create_relationship_generic(source_entity_type, source_urn, relationship_type, target_entity_type, target_urn, relationship_props)
                else:
                    print(f"   ⚠️ Skipped {relationship_type}: source_urn={source_urn}, target_urn={target_urn}")
            
            def _create_additional_relationship_generic(self, entity_urn: str, entity_type: str, data: Dict[str, Any], rule: Dict[str, Any]):
                """Create additional relationships using generic configuration"""
                rule_config = self.registry.get('relationship_rule_config', {})
                relationship_type_field = rule_config.get('relationship_type_field', 'relationship_type')
                source_entity_field = rule_config.get('source_entity_field', 'source_entity')
                target_entity_field = rule_config.get('target_entity_field', 'target_entity')
                field_mapping_field = rule_config.get('field_mapping_field', 'field_mapping')
                source_field_name = rule_config.get('source_field_name', 'source_field')
                target_field_name = rule_config.get('target_field_name', 'target_field')
                
                relationship_type = rule[relationship_type_field]
                source_entity = rule[source_entity_field]
                target_entity = rule[target_entity_field]
                field_mapping = rule[field_mapping_field]
                
                source_field = field_mapping[source_field_name]
                target_field = field_mapping[target_field_name]
                
                source_urn = data.get(source_field)
                target_urn = data.get(target_field)
                
                if source_urn and target_urn:
                    self._create_relationship_generic(source_entity, source_urn, relationship_type, target_entity, target_urn, {})
            
            def _resolve_urn_generic(self, field_value: Any, entity_type: str, urn_field: str, 
                                   data: Dict[str, Any], field_mapping: Dict[str, Any], entity_urn: str = None) -> str:
                """Generic URN resolution using configuration"""
                
                # If field_value is already a complete URN, return it as-is
                if isinstance(field_value, str) and field_value.startswith('urn:'):
                    return field_value
                
                # Get entity definition from registry
                entity_def = self.registry.get('entities', {}).get(entity_type)
                if not entity_def:
                    return field_value
                
                # Get URN pattern configuration
                urn_generator_name = entity_def.get('urn_generator')
                urn_pattern = self.registry.get('urn_patterns', {}).get(urn_generator_name)
                if not urn_pattern:
                    return field_value
                
                # Use configuration-driven resolution
                resolution_strategy = urn_pattern.get('resolution_strategy', 'template')
                
                if resolution_strategy == 'direct':
                    result = self._resolve_direct_urn(field_value, entity_type, urn_pattern, entity_urn)
                elif resolution_strategy == 'template':
                    result = self._resolve_template_urn(field_value, entity_type, urn_pattern, data, entity_urn)
                elif resolution_strategy == 'lookup':
                    result = self._resolve_lookup_urn(field_value, entity_type, urn_field, data)
                else:
                    result = field_value
                
                return result
            
            def _resolve_direct_urn(self, field_value: Any, entity_type: str, urn_pattern: Dict[str, Any], entity_urn: str = None) -> str:
                """Resolve URN using direct construction strategy"""
                direct_config = urn_pattern.get('direct_construction', {})
                pattern = direct_config.get('pattern')
                
                if pattern:
                    parent_urn_source = direct_config.get('parent_urn_source')
                    field_value_source = direct_config.get('field_value_source')
                    
                    if parent_urn_source == 'entity_urn' and entity_urn:
                        return pattern.format(parent_urn=entity_urn, field_value=field_value)
                    elif field_value_source == 'field_value':
                        return pattern.format(field_value=field_value)
                
                return field_value
            
            def _resolve_template_urn(self, field_value: Any, entity_type: str, urn_pattern: Dict[str, Any], 
                                    data: Dict[str, Any], entity_urn: str = None) -> str:
                """Resolve URN using template strategy"""
                entity_def = self.registry.get('entities', {}).get(entity_type)
                if not entity_def:
                    return field_value
                
                urn_generator_name = entity_def.get('urn_generator')
                if urn_generator_name and urn_generator_name in self.urn_generators:
                    urn_generator = self.urn_generators[urn_generator_name]
                    
                    # Build parameters for URN generation based on URN pattern configuration
                    urn_params = {}
                    
                    # Get the parameters from the URN pattern
                    pattern_params = urn_pattern.get('parameters', [])
                    
                    # Map field_value to the appropriate parameter based on entity type
                    if entity_type == 'CorpUser':
                        urn_params['username'] = field_value
                    elif entity_type == 'CorpGroup':
                        urn_params['name'] = field_value
                    elif entity_type == 'Column':
                        urn_params['field_path'] = field_value
                        if entity_urn:
                            urn_params['dataset_urn'] = entity_urn
                    else:
                        # Default mapping for other entity types
                        if pattern_params and len(pattern_params) > 0:
                            urn_params[pattern_params[0]] = field_value
                    
                    # Add any additional parameters from data that match the pattern parameters
                    for param in pattern_params:
                        if param in data and param not in urn_params:
                            urn_params[param] = data[param]
                    
                    try:
                        return urn_generator(**urn_params)
                    except Exception as e:
                        print(f"   ⚠️ URN generation failed for {entity_type}: {e}")
                        return field_value
                
                return field_value
            
            def _resolve_lookup_urn(self, field_value: Any, entity_type: str, urn_field: str, data: Dict[str, Any]) -> str:
                """Resolve URN using database lookup strategy"""
                rule_config = self.registry.get('relationship_rule_config', {})
                
                if urn_field != rule_config.get('urn_field_name', 'urn'):
                    with self._driver.session() as s:
                        result = s.run(
                            f"MATCH (e:{entity_type} {{{urn_field}: $value}}) WHERE e.urn IS NOT NULL RETURN e.urn as urn ORDER BY e.urn LIMIT 1",
                            value=field_value
                        )
                        record = result.single()
                        if record and record['urn']:
                            return record['urn']
                        else:
                            print(f"   ⚠️ Could not find {entity_type} with {urn_field}={field_value}")
                            return None
                
                return field_value
            
            def _ensure_entity_exists_generic(self, entity_type: str, entity_urn: str, field_value: Any, urn_field: str):
                """Ensure target entity exists using generic property extraction"""
                entity_def = self.registry.get('entities', {}).get(entity_type)
                if not entity_def:
                    print(f"   ⚠️ Entity type '{entity_type}' not found in registry")
                    return
                
                # Extract properties using configuration-driven parsing
                props = self._extract_entity_properties_generic(entity_urn, entity_def)
                
                # Add the field value if it's a valid property
                if urn_field in entity_def.get('properties', []):
                    props[urn_field] = field_value
                
                self._upsert_entity_generic(entity_type, entity_urn, props)
            
            def _extract_entity_properties_generic(self, entity_urn: str, entity_def: Dict[str, Any]) -> Dict[str, Any]:
                """Extract entity properties using configuration-driven parsing"""
                props = {'urn': entity_urn}
                
                property_extraction = entity_def.get('property_extraction', {})
                urn_parsing = property_extraction.get('urn_parsing', {})
                
                if urn_parsing:
                    pattern = urn_parsing.get('pattern')
                    mappings = urn_parsing.get('mappings', [])
                    
                    if pattern and mappings:
                        import re
                        match = re.search(pattern, entity_urn)
                        if match:
                            for mapping in mappings:
                                source_group = mapping.get('source_group')
                                target_property = mapping.get('target_property')
                                if source_group and target_property:
                                    props[target_property] = match.group(source_group)
                
                return props
        
        return DynamicNeo4jMetadataWriter
