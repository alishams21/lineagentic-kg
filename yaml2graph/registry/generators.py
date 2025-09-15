#!/usr/bin/env python3
from __future__ import annotations

import datetime as dt
import re
from typing import Any, Dict, Callable


class UtilityFunctionBuilder:
    """Builds utility functions from registry definitions"""
    
    def __init__(self, registry: Dict[str, Any]):
        self.registry = registry
    
    def create_functions(self) -> Dict[str, Callable]:
        """Create utility functions from registry configuration"""
        utility_functions_def = self.registry.get('utility_functions', {})
        utility_config = self.registry.get('utility_config', {})
        # Fix the path to match the new configuration structure
        core_config = self.registry.get('core_config', {})
        utility_functions_config = core_config.get('utility_functions', {})
        enabled_functions = utility_functions_config.get('enabled_utility_functions', [])
        
        created_functions = {}
        
        for func_name in enabled_functions:
            if func_name in utility_functions_def:
                func_def = utility_functions_def[func_name]
                created_functions[func_name] = self._build_utility_function(func_def, utility_config)
        
        return created_functions
    
    def _build_utility_function(self, func_def: Dict[str, Any], utility_config: Dict[str, Any]) -> Callable:
        """Build a utility function from its YAML definition"""
        func_type = func_def.get('type', '')
        implementation = func_def.get('implementation', {})
        
        if func_type == 'string_processing':
            return self._build_string_processing_function(implementation, utility_config)
        elif func_type == 'data_masking':
            return self._build_data_masking_function(implementation, utility_config)
        elif func_type == 'timestamp':
            return self._build_timestamp_function(implementation, utility_config)
        elif func_type == 'urn_generation':
            return self._build_urn_generation_function(implementation, utility_config)
        else:
            raise ValueError(f"Unknown utility function type: {func_type}")
    
    def _build_string_processing_function(self, implementation: Dict[str, Any], utility_config: Dict[str, Any]) -> Callable:
        """Build a string processing function"""
        operation = implementation.get('operation', '')
        
        if operation == 'regex_replace':
            pattern = utility_config.get(implementation.get('pattern_config', ''), r"[^a-zA-Z0-9_.-]+")
            replacement = utility_config.get(implementation.get('replacement_config', ''), "_")
            strip_method = utility_config.get(implementation.get('strip_method_config', ''), 'strip')
            
            def func(raw):
                if implementation.get('pre_processing') == 'strip':
                    raw = getattr(raw, strip_method)()
                return re.sub(pattern, replacement, raw)
            return func
            
        elif operation == 'split_and_extract':
            separator = utility_config.get(implementation.get('separator_config', ''), "@")
            split_limit = utility_config.get(implementation.get('split_limit_config', ''), 1)
            split_index = utility_config.get(implementation.get('split_index_config', ''), 0)
            
            post_processing = implementation.get('post_processing')
            if post_processing == 'regex_replace':
                pattern = utility_config.get(implementation.get('pattern_config', ''), r"[^a-zA-Z0-9_.-]+")
                replacement = utility_config.get(implementation.get('replacement_config', ''), "_")
                
                def func(email):
                    if separator in email:
                        username = email.split(separator, split_limit)[split_index]
                        return re.sub(pattern, replacement, username)
                    return email
                return func
            else:
                def func(email):
                    if separator in email:
                        return email.split(separator, split_limit)[split_index]
                    return email
                return func
        
        else:
            raise ValueError(f"Unknown string processing operation: {operation}")
    
    def _build_data_masking_function(self, implementation: Dict[str, Any], utility_config: Dict[str, Any]) -> Callable:
        """Build a data masking function"""
        operation = implementation.get('operation', '')
        
        if operation == 'conditional_replace':
            condition = implementation.get('condition', '')
            
            if condition == 'regex_match':
                pattern = utility_config.get(implementation.get('pattern_config', ''), r"(pass|secret|key|token)")
                regex_flag = utility_config.get(implementation.get('regex_flag_config', ''), 'IGNORECASE')
                replacement = utility_config.get(implementation.get('replacement_config', ''), "****")
                
                def func(k, v):
                    if re.search(pattern, k, getattr(re, regex_flag)):
                        return replacement
                    return v
                return func
        
        else:
            raise ValueError(f"Unknown data masking operation: {operation}")
    
    def _build_timestamp_function(self, implementation: Dict[str, Any], utility_config: Dict[str, Any]) -> Callable:
        """Build a timestamp function"""
        operation = implementation.get('operation', '')
        
        if operation == 'datetime_now':
            method = utility_config.get(implementation.get('method_config', ''), 'now')
            timezone = utility_config.get(implementation.get('timezone_config', ''), 'UTC')
            post_processing = implementation.get('post_processing', '')
            
            if post_processing == 'timestamp_multiply':
                multiplier = utility_config.get(implementation.get('multiplier_config', ''), 1000)
                
                def func():
                    if timezone == 'UTC':
                        return int(getattr(dt.datetime, method)(dt.timezone.utc).timestamp() * multiplier)
                    else:
                        return int(getattr(dt.datetime, method)(getattr(dt, timezone)).timestamp() * multiplier)
                return func
            else:
                def func():
                    if timezone == 'UTC':
                        return getattr(dt.datetime, method)(dt.timezone.utc)
                    else:
                        return getattr(dt.datetime, method)(getattr(dt, timezone))
                return func
        
        else:
            raise ValueError(f"Unknown timestamp operation: {operation}")

    def _build_urn_generation_function(self, implementation: Dict[str, Any], utility_config: Dict[str, Any]) -> Callable:
        """Build a URN generation function"""
        operation = implementation.get('operation', '')
        
        if operation == 'string_format':
            template = implementation.get('template', '')
            
            def func(**kwargs):
                return template.format(**kwargs)
            return func
        else:
            raise ValueError(f"Unknown URN generation operation: {operation}")


class URNGenerator:
    """Generates URN generator functions from registry patterns"""
    
    def __init__(self, registry: Dict[str, Any], utility_builder: UtilityFunctionBuilder):
        self.registry = registry
        self.utility_builder = utility_builder
    
    def create_generators(self) -> Dict[str, Callable]:
        """Create URN generator functions from registry patterns"""
        def process_urn_pattern(name: str, pattern: Dict[str, Any]) -> Callable:
            def create_urn_generator(pattern, pattern_name, utils):
                def urn_generator(**kwargs):
                    context_config = self.registry.get('context_config', {})
                    pattern_field = context_config.get('pattern_field', 'pattern')
                    generators_field = context_config.get('generators_field', 'generators')
                    
                    context = kwargs.copy()
                    context[pattern_field] = pattern
                    context[generators_field] = generators
                    
                    field_config = self.registry.get('field_processing', {})
                    skip_fields = field_config.get('skip_fields', ['template'])
                    context_fields = field_config.get('context_fields', ['pattern', 'generators'])
                    prefix_config = field_config.get('prefix_config', {'section': 'metadata', 'field': 'urn_prefix'})
                    
                    for field_name, field_value in pattern.items():
                        if field_name in skip_fields:
                            continue
                        
                        self._process_field_generically(field_name, field_value, context, utils)
                    
                    prefix_section = self.registry.get(prefix_config['section'], {})
                    prefix_value = prefix_section.get(prefix_config['field'], '')
                    formatted = pattern['template'].format(
                        prefix=prefix_value,
                        **{k: v for k, v in context.items() if k not in context_fields}
                    )
                    return formatted
                
                return urn_generator
            
            return create_urn_generator(pattern, name, self.utility_builder.create_functions())
        
        section_config = self.registry.get('section_config', {})
        urn_patterns_section = section_config.get('urn_patterns_section', 'urn_patterns')
        
        generators = {}
        if urn_patterns_section in self.registry:
            for name, config in self.registry[urn_patterns_section].items():
                generators[name] = process_urn_pattern(name, config)
        
        return generators
    
    def _process_field_generically(self, field_name: str, field_value: Any, context: Dict[str, Any], utils: Dict[str, Callable]) -> None:
        """Generic field processor"""
        try:
            if isinstance(field_value, dict):
                for param, val in field_value.items():
                    if param not in context:
                        context[param] = val
                    elif param in context and isinstance(val, str) and val in utils:
                        context[param] = utils[val](context[param])
            
            elif isinstance(field_value, list):
                for item in field_value:
                    if isinstance(item, str):
                        if item in context and context[item]:
                            if isinstance(context[item], str):
                                for util_name, util_func in utils.items():
                                    # Skip sanitization entirely - no sanitize_id usage
                                    if util_name != 'sanitize_id':
                                        try:
                                            context[item] = util_func(context[item])
                                            break
                                        except:
                                            continue
                        context[f'list_{field_name}'] = field_value
            
            elif isinstance(field_value, str):
                string_config = self.registry.get('string_processing', {})
                pattern_field = string_config.get('pattern_field', 'pattern')
                value_field = string_config.get('value_field', 'value')
                when_value_present_field = string_config.get('when_value_present_field', 'when_value_present')
                when_value_absent_field = string_config.get('when_value_absent_field', 'when_value_absent')
                
                pattern = context.get(pattern_field, {})
                for rule_key, rule_value in pattern.items():
                    if isinstance(rule_value, dict) and field_value in rule_value:
                        rules = rule_value[field_value]
                        if value_field in context and context[value_field]:
                            context[field_value] = rules[when_value_present_field].format(value=context[value_field])
                        else:
                            context[field_value] = rules[when_value_absent_field]
                        break
                else:
                    context[f'field_{field_name}'] = field_value
            
            else:
                context[f'field_{field_name}'] = field_value
        except Exception as e:
            print(f"Error processing field '{field_name}' with value {field_value}: {e}")
            raise


class AspectProcessor:
    """Processes aspect definitions and creates processors"""
    
    def __init__(self, registry: Dict[str, Any], utility_builder: UtilityFunctionBuilder):
        self.registry = registry
        self.utility_builder = utility_builder
    
    def create_processors(self) -> Dict[str, Callable]:
        """Create aspect processor functions from registry definitions"""
        def process_aspect(name: str, aspect: Dict[str, Any]) -> Callable:
            def aspect_processor(payload: Dict[str, Any]) -> Dict[str, Any]:
                aspect_context_config = self.registry.get('aspect_context_config', {})
                aspect_field = aspect_context_config.get('aspect_field', 'aspect')
                
                context = payload.copy()
                context[aspect_field] = aspect
                
                for field_name, field_value in aspect.items():
                    self._process_field_generically(field_name, field_value, context, self.utility_builder.create_functions())
                
                aspect_config = self.registry.get('aspect_processing', {})
                properties_field = aspect_config.get('properties_field', 'properties')
                context_exclude_fields = aspect_config.get('context_exclude_fields', ['aspect'])
                
                properties = aspect.get(properties_field, [])
                filtered_payload = {k: v for k, v in context.items() 
                                  if k in properties and k not in context_exclude_fields}
                
                return filtered_payload
            
            return aspect_processor
        
        section_config = self.registry.get('section_config', {})
        aspects_section = section_config.get('aspects_section', 'aspects')
        
        processors = {}
        if aspects_section in self.registry:
            for name, config in self.registry[aspects_section].items():
                processors[name] = process_aspect(name, config)
        
        return processors
    
    def _process_field_generically(self, field_name: str, field_value: Any, context: Dict[str, Any], utils: Dict[str, Callable]) -> None:
        """Generic field processor for aspects"""
        # Similar to URNGenerator._process_field_generically but simplified for aspects
        try:
            if isinstance(field_value, dict):
                for param, val in field_value.items():
                    if param not in context:
                        context[param] = val
                    elif param in context and isinstance(val, str) and val in utils:
                        # Skip sanitization entirely - no sanitize_id usage
                        if val != 'sanitize_id':
                            context[param] = utils[val](context[param])
            
            elif isinstance(field_value, list):
                for item in field_value:
                    if isinstance(item, str):
                        if item in context and context[item]:
                            if isinstance(context[item], str):
                                for util_name, util_func in utils.items():
                                    # Skip sanitization entirely - no sanitize_id usage
                                    if util_name != 'sanitize_id':
                                        try:
                                            context[item] = util_func(context[item])
                                            break
                                        except:
                                            continue
                        context[f'list_{field_name}'] = field_value
            
            else:
                context[f'field_{field_name}'] = field_value
        except Exception as e:
            print(f"Error processing aspect field '{field_name}' with value {field_value}: {e}")
            raise 