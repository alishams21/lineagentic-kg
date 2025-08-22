#!/usr/bin/env python3
from __future__ import annotations

from typing import Any, Dict, List


class RegistryValidator:
    """Validates registry structure and configuration"""
    
    def validate(self, registry: Dict[str, Any]) -> None:
        """Validate complete registry structure"""
        self._validate_required_sections(registry)
        self._validate_urn_patterns(registry)
        self._validate_aspects(registry)
        self._validate_entities(registry)
        self._validate_utility_functions(registry)
        self._validate_relationships(registry)
    
    def _validate_required_sections(self, registry: Dict[str, Any]) -> None:
        """Validate that all required sections are present"""
        required_sections = registry.get('registry_config', {}).get('required_sections', 
            ['entities', 'urn_patterns', 'aspect_types', 'aspects'])
        
        for section in required_sections:
            if section not in registry:
                raise ValueError(f"Registry missing required section: {section}")
    
    def _validate_urn_patterns(self, registry: Dict[str, Any]) -> None:
        """Validate URN pattern definitions"""
        urn_patterns = registry.get('urn_patterns', {})
        
        for pattern_name, pattern_def in urn_patterns.items():
            if 'template' not in pattern_def:
                raise ValueError(f"URN pattern '{pattern_name}' missing template")
            
            # Validate that all parameters in template are defined
            template = pattern_def['template']
            parameters = pattern_def.get('parameters', [])
            dependencies = pattern_def.get('dependencies', [])
            
            # Simple template parameter validation
            import re
            template_params = re.findall(r'\{([^}]+)\}', template)
            for param in template_params:
                if param not in parameters and param != 'prefix':
                    # Check if this is a conditional logic field
                    conditional_logic = pattern_def.get('conditional_logic')
                    if param == conditional_logic:
                        continue  # Skip validation for conditional logic fields
                    
                    # Check if this is a dependency-generated parameter
                    is_dependency_param = False
                    for dep in dependencies:
                        if dep in urn_patterns:
                            dep_params = urn_patterns[dep].get('parameters', [])
                            for dep_param in dep_params:
                                # Convert dependency parameter to expected format (e.g., platform -> data_platform_urn)
                                if param == f"{dep_param}_urn" or param == f"{dep}_urn":
                                    is_dependency_param = True
                                    break
                            # Also check for camelCase to snake_case conversion (e.g., dataPlatform -> data_platform_urn)
                            if not is_dependency_param:
                                import re
                                # Convert camelCase to snake_case
                                snake_case_dep = re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', dep).lower()
                                if param == f"{snake_case_dep}_urn":
                                    is_dependency_param = True
                                    break
                    
                    if not is_dependency_param:
                        raise ValueError(f"Template parameter '{param}' not defined in parameters for pattern '{pattern_name}'")
    
    def _validate_aspects(self, registry: Dict[str, Any]) -> None:
        """Validate aspect definitions"""
        aspects = registry.get('aspects', {})
        aspect_types = registry.get('aspect_types', {})
        
        for aspect_name, aspect_def in aspects.items():
            aspect_type = aspect_def.get('type')
            if not aspect_type:
                raise ValueError(f"Aspect '{aspect_name}' missing type")
            
            if aspect_type not in aspect_types:
                raise ValueError(f"Aspect '{aspect_name}' has unknown type: {aspect_type}")
            
            # Validate properties
            properties = aspect_def.get('properties', [])
            if not properties:
                raise ValueError(f"Aspect '{aspect_name}' has no properties defined")
    
    def _validate_entities(self, registry: Dict[str, Any]) -> None:
        """Validate entity definitions"""
        entities = registry.get('entities', {})
        urn_patterns = registry.get('urn_patterns', {})
        
        for entity_name, entity_def in entities.items():
            urn_generator = entity_def.get('urn_generator')
            if urn_generator and urn_generator not in urn_patterns:
                raise ValueError(f"Entity '{entity_name}' references unknown URN generator: {urn_generator}")
    
    def _validate_utility_functions(self, registry: Dict[str, Any]) -> None:
        """Validate utility function definitions"""
        utility_functions = registry.get('utility_functions', {})
        enabled_functions = registry.get('registry_config', {}).get('enabled_utility_functions', [])
        
        for func_name in enabled_functions:
            if func_name not in utility_functions:
                raise ValueError(f"Enabled utility function '{func_name}' not defined in utility_functions")
    
    def _validate_relationships(self, registry: Dict[str, Any]) -> None:
        """Validate relationship rule definitions"""
        relationships = registry.get('aspect_relationships', {})
        entities = registry.get('entities', {})
        
        for aspect_name, relationship_def in relationships.items():
            rules = relationship_def.get('rules', [])
            
            for rule in rules:
                entity_type = rule.get('entity_type')
                if entity_type and entity_type not in entities:
                    raise ValueError(f"Relationship rule references unknown entity type: {entity_type}")
                
                # Validate field mappings
                field_mapping = rule.get('field_mapping', {})
                if not field_mapping:
                    raise ValueError(f"Relationship rule missing field_mapping")
