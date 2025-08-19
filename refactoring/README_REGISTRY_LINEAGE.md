# Registry-Driven Column-Level Lineage

This document explains the enhanced registry-driven approach for generating column-level lineage in the data catalog system.

## Overview

The system now supports **automatic column-level lineage generation** based on registry configuration, making it generic and reusable across different entities and transformation types.

## Key Changes

### 1. Enhanced Registry Configuration (`enhanced_registry.yaml`)

#### New Lineage Configuration Section
```yaml
lineage_config:
  transformation_types:
    CONCATENATION:
      description_template: "Combine {input_columns} with {separator} separator"
      default_config: {"separator": " ", "trim": true}
      relationship_properties:
        type: "TRANSFORM"
        subtype: "CONCATENATION"
        transformation: "CONCAT"
    
    HASHING:
      description_template: "Hash {input_columns} using {algorithm} for privacy"
      default_config: {"algorithm": "SHA-256", "salt": "default_salt"}
      relationship_properties:
        type: "TRANSFORM"
        subtype: "HASHING"
        transformation: "SHA256_HASH"
    
    # ... more transformation types
```

#### Enhanced Transformation Aspect
```yaml
transformation:
  type: versioned
  properties:
    - inputColumns
    - steps
    - notes
    - sourceDataset      # NEW: Track source dataset
    - targetDataset      # NEW: Track target dataset
    - transformationType # NEW: Track transformation type
  required: ["inputColumns", "transformationType"]
```

### 2. RegistryFactory Enhancements (`registry_factory.py`)

#### New Methods for Lineage Generation

1. **`generate_transformation_aspect()`**
   - Generates transformation aspect payload from transformation data
   - Uses registry configuration for validation and defaults
   - Automatically includes source/target dataset information

2. **`generate_lineage_relationship_properties()`**
   - Generates relationship properties for DERIVES_FROM relationships
   - Uses registry templates for consistent property generation
   - Includes transformation metadata

3. **`create_column_lineage_relationships()`**
   - Automatically creates column-level lineage relationships
   - Processes transformation data from JSON files
   - Creates DERIVES_FROM relationships for each input column

4. **`create_dataset_lineage_relationship()`**
   - Creates dataset-level lineage relationships
   - Uses registry configuration for relationship properties

## How It Works

### 1. Transformation Data Structure
Transformations are defined in JSON data files:
```json
{
  "transformations": {
    "full_name": {
      "type": "CONCATENATION",
      "input_columns": ["first_name", "last_name"],
      "description": "Combine first and last name with space separator",
      "config": {"separator": " ", "trim": true}
    }
  }
}
```

### 2. Automatic Lineage Generation
The system automatically:
1. Reads transformation data from JSON files
2. Generates transformation aspects using registry templates
3. Creates DERIVES_FROM relationships between columns
4. Applies consistent metadata from registry configuration

### 3. Registry-Driven Approach
- **No hardcoded lineage logic** in ingestion scripts
- **Configurable transformation types** in registry
- **Consistent relationship properties** across all transformations
- **Extensible** for new transformation types

## Supported Transformation Types

| Type | Description | Example Use Case |
|------|-------------|------------------|
| `CONCATENATION` | Combine multiple columns | Full name from first + last |
| `HASHING` | Hash values for privacy | Email hashing |
| `CLEANING` | Clean and format data | Phone number formatting |
| `EXTRACTION` | Extract parts of data | Year from date |
| `VALIDATION` | Validate data quality | ID validation |
| `PASSTHROUGH` | Pass through unchanged | ID passthrough |
| `STANDARDIZATION` | Standardize format | Name standardization |
| `ENCRYPTION` | Encrypt sensitive data | Email encryption |
| `MASKING` | Mask sensitive data | Phone masking |
| `CLASSIFICATION` | Classify into categories | Customer segments |
| `SCORING` | Calculate scores | Data quality scores |

## Usage Example

### Before (Hardcoded)
```python
# Hardcoded lineage creation
self.writer.create_derives_from_relationship(
    self.column_urns['staging']['full_name'],
    self.column_urns['raw']['first_name'],
    {
        "type": "TRANSFORM",
        "subtype": "CONCATENATION",
        "description": "Full name concatenation",
        "transformation": "CONCAT"
    }
)
```

### After (Registry-Driven)
```python
# Registry-driven lineage creation
self.factory.create_column_lineage_relationships(
    self.writer,
    transformations,  # From JSON file
    self.column_urns['raw'],
    self.column_urns['staging'],
    self.dataset_urns['raw'],
    self.dataset_urns['staging']
)
```

## Benefits

### 1. **Generic and Reusable**
- Works with any entity type defined in registry
- Supports any transformation type configuration
- No code changes needed for new transformation types

### 2. **Consistent Metadata**
- All transformations use consistent property structure
- Registry ensures standardization across the system
- Easy to extend with new transformation types

### 3. **Maintainable**
- Lineage logic centralized in registry
- Easy to modify transformation behavior
- Clear separation of configuration and code

### 4. **Extensible**
- Add new transformation types by updating registry
- Support new relationship properties
- Easy to add new lineage patterns

## Testing

Run the test script to verify functionality:
```bash
python test_registry_lineage.py
```

This will test:
- Transformation aspect generation
- Lineage relationship properties generation
- Different transformation types
- Registry configuration access
- Column URN generation

## Future Enhancements

1. **Dynamic Transformation Types**: Add support for custom transformation types
2. **Complex Lineage Patterns**: Support multi-step transformations
3. **Lineage Validation**: Add validation rules for lineage consistency
4. **Lineage Visualization**: Generate lineage diagrams from registry
5. **Transformation Templates**: Pre-built transformation templates for common patterns

## Migration Guide

To migrate existing hardcoded lineage to registry-driven approach:

1. **Update Registry**: Add transformation types to `lineage_config`
2. **Update JSON Data**: Ensure transformation data includes required fields
3. **Update Ingestion Scripts**: Replace hardcoded lineage with registry calls
4. **Test**: Run test script to verify functionality
5. **Deploy**: Deploy updated registry and code

This approach makes the system much more flexible and maintainable for column-level lineage tracking!
