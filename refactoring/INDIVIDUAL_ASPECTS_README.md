# Individual Aspect JSON Files

This directory contains individual aspect JSON files that simulate REST API responses for independent aspect ingestion. Each file contains a single aspect with the necessary entity parameters for automatic entity creation.

## Overview

Since the `registry_factory.py` now supports independent aspect ingestion, these files demonstrate how to ingest aspects without requiring pre-existing entity URNs. The system will automatically create the associated entities if they don't exist.

## File Structure

Each aspect file follows this structure:

```json
{
  "metadata": {
    "source": "rest_api",
    "endpoint": "/api/v1/aspects/{aspectName}",
    "timestamp": "2024-01-15T13:00:00Z",
    "request_id": "req_22222"
  },
  "aspect_name": "datasetProperties",
  "aspect_type": "versioned",
  "entity_type": "Dataset",
  "entity_key": {
    "platform": "postgresql",
    "name": "user_profiles",
    "env": "PROD"
  },
  "payload": {
    // Aspect-specific data
  }
}
```

## Available Aspect Files

### 1. `api_aspect_datasetproperties.json`
- **Aspect**: `datasetProperties`
- **Type**: `versioned`
- **Entity**: `Dataset`
- **Entity Key**: `platform`, `name`, `env`
- **Description**: Dataset properties including description, custom properties, tags, and external URL

### 2. `api_aspect_corpuserinfo.json`
- **Aspect**: `corpUserInfo`
- **Type**: `versioned`
- **Entity**: `CorpUser`
- **Entity Key**: `username`
- **Description**: User information including display name, email, title, department, skills, etc.

### 3. `api_aspect_schemametadata.json`
- **Aspect**: `schemaMetadata`
- **Type**: `versioned`
- **Entity**: `Dataset`
- **Entity Key**: `platform`, `name`, `env`
- **Description**: Schema metadata including fields, data types, primary keys, etc.

### 4. `api_aspect_ownership.json`
- **Aspect**: `ownership`
- **Type**: `versioned`
- **Entity**: `Dataset`
- **Entity Key**: `platform`, `name`, `env`
- **Description**: Ownership information including owners, types, and modification history

### 5. `api_aspect_datasetprofile.json`
- **Aspect**: `datasetProfile`
- **Type**: `timeseries`
- **Entity**: `Dataset`
- **Entity Key**: `platform`, `name`, `env`
- **Description**: Dataset profile data including row count, column count, size, and column profiles

## Key Features

### Independent Ingestion
- **No URNs Required**: Entity URNs are automatically generated from entity parameters
- **Automatic Entity Creation**: Entities are created if they don't exist
- **Flexible Parameters**: Support for both required and optional entity parameters

### REST API Simulation
- **Realistic Structure**: Files simulate actual REST API responses
- **Metadata Included**: Each file includes source, endpoint, timestamp, and request ID
- **Proper Formatting**: JSON structure matches expected API response format

### Validation Support
- **Structure Validation**: All files include required fields
- **Type Validation**: Aspect types are properly specified (versioned/timeseries)
- **Parameter Validation**: Entity keys contain all necessary parameters

## Usage

### Testing Files
```bash
python test_individual_aspects.py
```

### Ingesting Individual Files
```bash
python ingest_individual_aspects.py
```

### Programmatic Usage
```python
from ingest_individual_aspects import IndividualAspectIngestion

# Initialize
ingestion = IndividualAspectIngestion(registry_path, neo4j_uri, neo4j_user, neo4j_password)
ingestion.initialize()

# Ingest single file
ingestion.ingest_aspect_file("api_aspect_datasetproperties.json")

# Ingest multiple files
files = ["api_aspect_datasetproperties.json", "api_aspect_corpuserinfo.json"]
ingestion.ingest_multiple_aspect_files(files)
```

## Method Generation

The `registry_factory.py` automatically generates methods for each aspect:

- `upsert_datasetproperties_aspect(payload, platform, name, env)`
- `upsert_corpuserinfo_aspect(payload, username)`
- `upsert_schemametadata_aspect(payload, platform, name, env)`
- `upsert_ownership_aspect(payload, platform, name, env)`
- `upsert_datasetprofile_aspect(payload, platform, name, env)`

## Benefits

1. **Simplified Ingestion**: No need to create entities first
2. **REST API Ready**: Files can be directly used as API response examples
3. **Independent Processing**: Each aspect can be ingested separately
4. **Automatic Entity Management**: Entities are created on-demand
5. **Flexible Workflows**: Support for both batch and individual processing

## Example Workflow

1. **Receive Aspect Data**: REST API receives aspect data from client
2. **Validate Structure**: Ensure all required fields are present
3. **Extract Parameters**: Get entity_key and payload from request
4. **Call Generated Method**: Use the appropriate upsert method
5. **Automatic Entity Creation**: Entity is created if it doesn't exist
6. **Aspect Ingestion**: Aspect is ingested with the entity

This approach eliminates the need for complex entity-aspect relationship management and provides a clean, RESTful interface for metadata ingestion.
