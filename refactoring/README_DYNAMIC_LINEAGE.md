# Dynamic Transformation Type Discovery

This document explains the enhanced **dynamic transformation type discovery** system that automatically discovers and handles transformation types from data during ingestion, eliminating the need for hardcoded transformation type definitions.

## Overview

The system now supports **completely generic transformation handling** where transformation types are discovered dynamically from the data during ingestion, rather than being predefined in the registry. This makes the system truly flexible and extensible.

## Key Features

### 🚀 **Dynamic Discovery**
- **Automatic Type Detection**: Transformation types are discovered from JSON data during ingestion
- **No Hardcoded Types**: No need to predefine transformation types in registry
- **Extensible**: Supports any transformation type without code changes

### 🔧 **Generic Templates**
- **Default Template**: Works with any transformation type automatically
- **Pattern Matching**: Optional patterns for common transformation types
- **Fallback Handling**: Graceful handling of unknown transformation types

### 📊 **Statistics & Validation**
- **Transformation Statistics**: Automatic counting and analysis of transformations
- **Data Validation**: Validates transformation data structure
- **Error Handling**: Graceful handling of invalid transformations

## Architecture Changes

### 1. Enhanced Registry Configuration (`enhanced_registry.yaml`)

#### Dynamic Discovery Configuration
```yaml
lineage_config:
  transformation_discovery:
    enabled: true
    auto_generate_properties: true
    default_relationship_properties:
      type: "TRANSFORM"
      source_dataset: null
      target_dataset: null
      description: null
```

#### Generic Templates
```yaml
transformation_templates:
  # Default template for ANY transformation type
  default:
    description_template: "Transform {input_columns} using {transformation_type}"
    default_config: {}
    relationship_properties:
      type: "TRANSFORM"
      subtype: "{transformation_type}"
      transformation: "{transformation_type}"
  
  # Optional patterns for common types (not required)
  patterns:
    CONCATENATION:
      description_template: "Combine {input_columns} with separator"
      relationship_properties:
        subtype: "CONCATENATION"
        transformation: "CONCAT"
```

### 2. RegistryFactory Enhancements (`registry_factory.py`)

#### New Dynamic Methods

1. **`discover_transformation_types()`**
   ```python
   def discover_transformation_types(self, transformations: Dict[str, Any]) -> List[str]:
       """Discover transformation types from transformation data"""
   ```

2. **`validate_transformation_data()`**
   ```python
   def validate_transformation_data(self, transformations: Dict[str, Any]) -> Dict[str, Any]:
       """Validate and enrich transformation data"""
   ```

3. **`get_transformation_statistics()`**
   ```python
   def get_transformation_statistics(self, transformations: Dict[str, Any]) -> Dict[str, Any]:
       """Get statistics about transformations"""
   ```

## How It Works

### 1. **Data-Driven Discovery**
Transformations are defined in JSON data files:
```json
{
  "transformations": {
    "full_name": {
      "type": "CONCATENATION",  // ← Discovered automatically
      "input_columns": ["first_name", "last_name"],
      "description": "Combine first and last name with space separator",
      "config": {"separator": " ", "trim": true}
    },
    "custom_field": {
      "type": "CUSTOM_TRANSFORMATION",  // ← Any type works!
      "input_columns": ["field1", "field2"],
      "description": "Custom transformation for testing",
      "config": {"custom_param": "value"}
    }
  }
}
```

### 2. **Automatic Processing**
The system automatically:
1. **Discovers** transformation types from data
2. **Validates** transformation structure
3. **Generates** aspects using generic templates
4. **Creates** lineage relationships with appropriate properties
5. **Reports** statistics and validation results

### 3. **Template Resolution**
For each transformation type:
1. **Check patterns**: Look for specific pattern in registry
2. **Use default**: Fall back to generic default template
3. **Apply placeholders**: Replace `{transformation_type}`, `{input_columns}` etc.
4. **Generate properties**: Create consistent relationship properties

## Usage Examples

### Basic Usage
```python
# Load transformations from data
transformations = data.get('transformations', {})

# Discover types automatically
discovered_types = factory.discover_transformation_types(transformations)
print(f"Discovered types: {discovered_types}")

# Get statistics
stats = factory.get_transformation_statistics(transformations)
print(f"Total transformations: {stats['total_transformations']}")

# Create lineage automatically
factory.create_column_lineage_relationships(
    writer, transformations, source_columns, target_columns,
    source_dataset_urn, target_dataset_urn
)
```

### Custom Transformation Types
```python
# Any transformation type works without predefinition
custom_transformations = {
    "ai_enhanced": {
        "type": "AI_ENHANCEMENT",  # ← New type, no registry changes needed
        "input_columns": ["text_field"],
        "description": "AI-powered text enhancement",
        "config": {"model": "gpt-4", "temperature": 0.7}
    },
    "ml_prediction": {
        "type": "ML_PREDICTION",  # ← Another new type
        "input_columns": ["features"],
        "description": "Machine learning prediction",
        "config": {"algorithm": "random_forest"}
    }
}

# Process automatically
factory.create_column_lineage_relationships(
    writer, custom_transformations, source_columns, target_columns,
    source_dataset_urn, target_dataset_urn
)
```

## Benefits

### 1. **True Genericity**
- **No Hardcoded Types**: Works with any transformation type
- **Data-Driven**: Types discovered from actual data
- **Zero Configuration**: No registry updates for new types

### 2. **Extensibility**
- **Add New Types**: Just use them in data, no code changes
- **Custom Transformations**: Support for domain-specific transformations
- **Future-Proof**: Adapts to new transformation patterns

### 3. **Maintainability**
- **Centralized Logic**: All transformation handling in one place
- **Consistent Behavior**: Same processing for all transformation types
- **Error Handling**: Graceful handling of invalid data

### 4. **Analytics**
- **Transformation Statistics**: Automatic counting and analysis
- **Usage Patterns**: Track which transformation types are used
- **Validation Reports**: Identify data quality issues

## Testing

### Run Dynamic Discovery Test
```bash
python test_dynamic_lineage.py
```

This tests:
- ✅ Transformation type discovery
- ✅ Statistics generation
- ✅ Dynamic aspect generation
- ✅ Relationship property generation
- ✅ Custom transformation types
- ✅ Data validation
- ✅ Registry configuration

### Sample Output
```
🧪 Testing Dynamic Transformation Type Discovery
============================================================

1. Testing Transformation Type Discovery:
----------------------------------------
Staging transformation types: ['EXTRACTION', 'CONCATENATION', 'CLEANING', 'HASHING']
Final transformation types: ['STANDARDIZATION', 'ENCRYPTION', 'MASKING', 'SCORING', 'CLASSIFICATION']
All unique transformation types: ['CLEANING', 'ENCRYPTION', 'MASKING', 'EXTRACTION', 'SCORING', 'HASHING', 'CONCATENATION', 'CLASSIFICATION', 'STANDARDIZATION']

2. Testing Transformation Statistics:
----------------------------------------
Staging transformations statistics:
  Total transformations: 4
  Transformation types: {'CONCATENATION': 1, 'HASHING': 1, 'CLEANING': 1, 'EXTRACTION': 1}
  Input column usage: {'first_name': 1, 'last_name': 1, 'email_address': 1, 'phone_number': 1, 'registration_date': 1}
  Target columns: ['full_name', 'email_hash', 'phone_clean', 'registration_year']
```

## Migration from Hardcoded Types

### Before (Hardcoded)
```yaml
# Required: Predefine all transformation types
transformation_types:
  CONCATENATION:
    description_template: "Combine {input_columns} with {separator} separator"
    # ... hardcoded configuration
  HASHING:
    description_template: "Hash {input_columns} using {algorithm} for privacy"
    # ... hardcoded configuration
  # Must add new types here before using them
```

### After (Dynamic)
```yaml
# Optional: Only common patterns, not required
transformation_templates:
  default:
    description_template: "Transform {input_columns} using {transformation_type}"
    # Works with ANY transformation type
  patterns:
    CONCATENATION:
      description_template: "Combine {input_columns} with separator"
      # Optional optimization for common types
```

## Advanced Features

### 1. **Pattern Matching**
Optional patterns for common transformation types provide better defaults:
```yaml
patterns:
  CONCATENATION:
    description_template: "Combine {input_columns} with separator"
    relationship_properties:
      subtype: "CONCATENATION"
      transformation: "CONCAT"
```

### 2. **Validation**
Automatic validation of transformation data:
```python
validated = factory.validate_transformation_data(transformations)
# Filters out invalid transformations and reports warnings
```

### 3. **Statistics**
Comprehensive transformation analytics:
```python
stats = factory.get_transformation_statistics(transformations)
# Returns: total count, type distribution, input usage, target columns
```

## Future Enhancements

1. **Transformation Templates**: Pre-built templates for common patterns
2. **Validation Rules**: Custom validation rules for specific transformation types
3. **Performance Optimization**: Caching for frequently used transformation types
4. **Visualization**: Generate transformation flow diagrams
5. **Impact Analysis**: Track downstream impact of transformations

## Conclusion

The dynamic transformation type discovery system provides:

- **🎯 True Genericity**: Works with any transformation type without configuration
- **🚀 Extensibility**: Easy to add new transformation types
- **🔧 Maintainability**: Centralized, consistent processing
- **📊 Analytics**: Built-in statistics and validation
- **🛡️ Reliability**: Graceful error handling and validation

This makes the column-level lineage system completely **data-driven** and **future-proof**! 🎉
