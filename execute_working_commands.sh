#!/bin/bash

# Execute Working Curl Commands for LineAgentic Catalog API
# This script creates a raw and processed dataset scenario with proper data lineage

echo "🚀 Executing Raw to Processed Data Lineage Scenario"
echo "=================================================="

# Set the registry path environment variable
export REGISTRY_PATH="lineagentic_catalog/config/main_registry.yaml"

# Wait for API to be ready
echo "⏳ Waiting for API to be ready..."
sleep 3

echo ""
echo "📋 Executing Data Lineage Scenario:"
echo "==================================="

echo ""
echo "1️⃣ Creating Raw Dataset (customer_raw_data):"
echo "--------------------------------------------"
curl -X POST "http://localhost:8000/api/v1/entities/Dataset" \
  -H "Content-Type: application/json" \
  -d '{
    "platform": "snowflake",
    "name": "customer_raw_data",
    "env": "PROD",
    "versionId": "v1.0"
  }' | jq '.'

echo ""
echo "2️⃣ Creating Processed Dataset (customer_processed_data):"
echo "------------------------------------------------------"
curl -X POST "http://localhost:8000/api/v1/entities/Dataset" \
  -H "Content-Type: application/json" \
  -d '{
    "platform": "snowflake",
    "name": "customer_processed_data",
    "env": "PROD",
    "versionId": "v1.0"
  }' | jq '.'

echo ""
echo "3️⃣ Adding datasetProperties to Raw Dataset:"
echo "-------------------------------------------"
curl -X POST "http://localhost:8000/api/v1/aspects/datasetProperties" \
  -H "Content-Type: application/json" \
  -d '{
    "entity_label": "Dataset",
    "entity_urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,customer_raw_data,PROD)",
    "description": "Raw customer data from source systems",
    "customProperties": {
      "data_source": "CRM System",
      "ingestion_frequency": "daily",
      "data_quality": "raw"
    },
    "tags": ["raw", "customer", "crm"],
    "externalUrl": "https://snowflake.company.com/raw/customer_raw_data"
  }' | jq '.'

echo ""
echo "4️⃣ Adding datasetProperties to Processed Dataset:"
echo "------------------------------------------------"
curl -X POST "http://localhost:8000/api/v1/aspects/datasetProperties" \
  -H "Content-Type: application/json" \
  -d '{
    "entity_label": "Dataset",
    "entity_urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,customer_processed_data,PROD)",
    "description": "Processed and cleaned customer data for analytics",
    "customProperties": {
      "data_source": "ETL Pipeline",
      "ingestion_frequency": "daily",
      "data_quality": "processed"
    },
    "tags": ["processed", "customer", "analytics"],
    "externalUrl": "https://snowflake.company.com/processed/customer_processed_data"
  }' | jq '.'

echo ""
echo "5️⃣ Adding schemaMetadata to Raw Dataset:"
echo "----------------------------------------"
curl -X POST "http://localhost:8000/api/v1/aspects/schemaMetadata" \
  -H "Content-Type: application/json" \
  -d '{
    "entity_label": "Dataset",
    "entity_urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,customer_raw_data,PROD)",
    "schemaName": "customer_raw_schema",
    "platform": "snowflake",
    "version": "1.0",
    "fields": [
      {"fieldPath": "customer_email", "type": "VARCHAR", "description": "Raw customer email address"}
    ],
    "primaryKeys": ["customer_email"]
  }' | jq '.'

echo ""
echo "6️⃣ Adding schemaMetadata to Processed Dataset:"
echo "---------------------------------------------"
curl -X POST "http://localhost:8000/api/v1/aspects/schemaMetadata" \
  -H "Content-Type: application/json" \
  -d '{
    "entity_label": "Dataset",
    "entity_urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,customer_processed_data,PROD)",
    "schemaName": "customer_processed_schema",
    "platform": "snowflake",
    "version": "1.0",
    "fields": [
      {"fieldPath": "customer_id", "type": "VARCHAR", "description": "Hashed customer identifier"}
    ],
    "primaryKeys": ["customer_id"]
  }' | jq '.'

echo ""
echo "7️⃣ Adding ownership to Raw Dataset:"
echo "----------------------------------"
curl -X POST "http://localhost:8000/api/v1/aspects/ownership" \
  -H "Content-Type: application/json" \
  -d '{
    "entity_label": "Dataset",
    "entity_urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,customer_raw_data,PROD)",
    "owners": [
      {"owner": "data_engineering_team", "type": "TECHNICAL_OWNER"},
      {"owner": "crm_team", "type": "BUSINESS_OWNER"}
    ],
    "lastModified": "2024-01-15T10:00:00Z"
  }' | jq '.'

echo ""
echo "8️⃣ Adding ownership to Processed Dataset:"
echo "----------------------------------------"
curl -X POST "http://localhost:8000/api/v1/aspects/ownership" \
  -H "Content-Type: application/json" \
  -d '{
    "entity_label": "Dataset",
    "entity_urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,customer_processed_data,PROD)",
    "owners": [
      {"owner": "data_engineering_team", "type": "TECHNICAL_OWNER"},
      {"owner": "analytics_team", "type": "BUSINESS_OWNER"}
    ],
    "lastModified": "2024-01-15T10:00:00Z"
  }' | jq '.'

echo ""
echo "9️⃣ Adding globalTags to Raw Dataset:"
echo "-----------------------------------"
curl -X POST "http://localhost:8000/api/v1/aspects/globalTags" \
  -H "Content-Type: application/json" \
  -d '{
    "entity_label": "Dataset",
    "entity_urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,customer_raw_data,PROD)",
    "tags": [
      {"tag": "PII", "context": "data_classification"},
      {"tag": "RAW_DATA", "context": "data_layer"}
    ]
  }' | jq '.'

echo ""
echo "🔟 Adding globalTags to Processed Dataset:"
echo "----------------------------------------"
curl -X POST "http://localhost:8000/api/v1/aspects/globalTags" \
  -H "Content-Type: application/json" \
  -d '{
    "entity_label": "Dataset",
    "entity_urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,customer_processed_data,PROD)",
    "tags": [
      {"tag": "ANONYMIZED", "context": "data_classification"},
      {"tag": "PROCESSED_DATA", "context": "data_layer"}
    ]
  }' | jq '.'

# echo ""
# echo "1️⃣1️⃣ Adding datasetProfile to Raw Dataset:"
# echo "-----------------------------------------"
# curl -X POST "http://localhost:8000/api/v1/aspects/datasetProfile" \
#   -H "Content-Type: application/json" \
#   -d '{
#     "entity_label": "Dataset",
#     "entity_urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,customer_raw_data,PROD)",
#     "rowCount": 100000,
#     "columnCount": 1,
#     "sizeInBytes": 5000000,
#     "lastModified": "2024-01-15T10:00:00Z",
#     "partitionCount": 1
#   }' | jq '.'

# echo ""
# echo "1️⃣2️⃣ Adding datasetProfile to Processed Dataset:"
# echo "-----------------------------------------------"
# curl -X POST "http://localhost:8000/api/v1/aspects/datasetProfile" \
#   -H "Content-Type: application/json" \
#   -d '{
#     "entity_label": "Dataset",
#     "entity_urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,customer_processed_data,PROD)",
#     "rowCount": 100000,
#     "columnCount": 1,
#     "sizeInBytes": 3000000,
#     "lastModified": "2024-01-15T10:00:00Z",
#     "partitionCount": 1
#   }' | jq '.'

echo ""
echo "1️⃣3️⃣ Adding datasetTransformation to Raw Dataset:"
echo "------------------------------------------------"
curl -X POST "http://localhost:8000/api/v1/aspects/datasetTransformation" \
  -H "Content-Type: application/json" \
  -d '{
    "entity_label": "Dataset",
    "entity_urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,customer_raw_data,PROD)",
    "sourceDataset": "urn:li:dataset:(urn:li:dataPlatform:snowflake,customer_raw_data,PROD)",
    "targetDataset": "urn:li:dataset:(urn:li:dataPlatform:snowflake,customer_processed_data,PROD)",
    "transformationType": "data_cleaning_and_hashing",
    "description": "Raw customer data is cleaned and customer emails are hashed to create anonymized customer IDs"
  }' | jq '.'

echo ""
echo "1️⃣3️⃣b Adding datasetTransformation to Processed Dataset (for testing):"
echo "--------------------------------------------------------------------"
curl -X POST "http://localhost:8000/api/v1/aspects/datasetTransformation" \
  -H "Content-Type: application/json" \
  -d '{
    "entity_label": "Dataset",
    "entity_urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,customer_processed_data,PROD)",
    "sourceDataset": "urn:li:dataset:(urn:li:dataPlatform:snowflake,customer_raw_data,PROD)",
    "targetDataset": "urn:li:dataset:(urn:li:dataPlatform:snowflake,customer_processed_data,PROD)",
    "transformationType": "data_cleaning_and_hashing",
    "description": "Raw customer data is cleaned and customer emails are hashed to create anonymized customer IDs"
  }' | jq '.'

echo ""
echo "1️⃣4️⃣ Creating Column for Raw Dataset (customer_email):"
echo "-----------------------------------------------------"
curl -X POST "http://localhost:8000/api/v1/entities/Column" \
  -H "Content-Type: application/json" \
  -d '{
    "dataset_urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,customer_raw_data,PROD)",
    "field_path": "customer_email"
  }' | jq '.'

echo ""
echo "1️⃣5️⃣ Creating Column for Processed Dataset (customer_id):"
echo "--------------------------------------------------------"
curl -X POST "http://localhost:8000/api/v1/entities/Column" \
  -H "Content-Type: application/json" \
  -d '{
    "dataset_urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,customer_processed_data,PROD)",
    "field_path": "customer_id"
  }' | jq '.'

echo ""
echo "1️⃣6️⃣ Adding columnProperties to Raw Column:"
echo "-------------------------------------------"
curl -X POST "http://localhost:8000/api/v1/aspects/columnProperties" \
  -H "Content-Type: application/json" \
  -d '{
    "entity_label": "Column",
    "entity_urn": "urn:li:column:(urn:li:dataset:(urn:li:dataPlatform:snowflake,customer_raw_data,PROD),customer_email)",
    "description": "Raw customer email address from source system",
    "dataType": "VARCHAR(255)",
    "nullable": false,
    "defaultValue": null,
    "customProperties": {
      "contains_pii": true,
      "source_field": "email"
    }
  }' | jq '.'

echo ""
echo "1️⃣7️⃣ Adding columnProperties to Processed Column:"
echo "------------------------------------------------"
curl -X POST "http://localhost:8000/api/v1/aspects/columnProperties" \
  -H "Content-Type: application/json" \
  -d '{
    "entity_label": "Column",
    "entity_urn": "urn:li:column:(urn:li:dataset:(urn:li:dataPlatform:snowflake,customer_processed_data,PROD),customer_id)",
    "description": "Hashed customer identifier for privacy",
    "dataType": "VARCHAR(64)",
    "nullable": false,
    "defaultValue": null,
    "customProperties": {
      "contains_pii": false,
      "hash_algorithm": "sha256"
    }
  }' | jq '.'

echo ""
echo "1️⃣8️⃣ Adding columnTransformation to Processed Column:"
echo "---------------------------------------------------"
curl -X POST "http://localhost:8000/api/v1/aspects/columnTransformation" \
  -H "Content-Type: application/json" \
  -d '{
    "entity_label": "Column",
    "entity_urn": "urn:li:column:(urn:li:dataset:(urn:li:dataPlatform:snowflake,customer_processed_data,PROD),customer_id)",
    "inputColumns": ["customer_email"],
    "transformType": "hash_transformation",
    "transformScript": "SHA256(customer_email)",
    "sourceDataset": "urn:li:dataset:(urn:li:dataPlatform:snowflake,customer_raw_data,PROD)"
  }' | jq '.'

echo ""
echo "1️⃣9️⃣ Getting Raw Dataset:"
echo "------------------------"
curl -X GET "http://localhost:8000/api/v1/entities/Dataset/urn:li:dataset:(urn:li:dataPlatform:snowflake,customer_raw_data,PROD)" | jq '.'

echo ""
echo "2️⃣0️⃣ Getting Processed Dataset:"
echo "------------------------------"
curl -X GET "http://localhost:8000/api/v1/entities/Dataset/urn:li:dataset:(urn:li:dataPlatform:snowflake,customer_processed_data,PROD)" | jq '.'

echo ""
echo "✅ DATA LINEAGE SCENARIO COMPLETED!"
echo "=================================="
echo "✅ Raw Dataset created: customer_raw_data"
echo "✅ Processed Dataset created: customer_processed_data"
echo "✅ All aspects added to both datasets:"
echo "   - datasetProperties (description, tags, custom properties)"
echo "   - schemaMetadata (schema definition with fields)"
echo "   - ownership (technical and business owners)"
echo "   - globalTags (data classification tags)"
echo "✅ datasetTransformation aspect added to processed dataset"
echo "✅ Columns created for both datasets:"
echo "   - Raw: customer_email (contains PII)"
echo "   - Processed: customer_id (anonymized)"
echo "✅ columnProperties added to both columns"
echo "✅ columnTransformation added to processed column"
echo "✅ Relationships should be automatically created:"
echo "   - HAS_COLUMN: Dataset → Column"
echo "   - TRANSFORMS: Column → Column (customer_email → customer_id)"
echo "   - DOWNSTREAM_OF: Dataset → Dataset (raw → processed)"
echo "✅ All URNs auto-generated by backend"
echo ""
echo "🌐 API Documentation: http://localhost:8000/docs"
echo "📁 Generated API Location: generated_api/"
echo ""
echo "🎯 Raw to Processed Data Lineage Successfully Created!"
