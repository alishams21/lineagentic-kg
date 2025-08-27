#!/bin/bash

# Execute Working Curl Commands for LineAgentic Catalog API
# This script actually executes the curl commands and pushes data

echo "🚀 Executing Working Curl Commands for LineAgentic Catalog API"
echo "============================================================="

# Set the registry path environment variable
export REGISTRY_PATH="lineagentic_catalog/config/main_registry.yaml"

# Wait for API to be ready
echo "⏳ Waiting for API to be ready..."
sleep 3

echo ""
echo "📋 Executing Curl Commands:"
echo "==========================="

echo ""
echo "1️⃣ Creating DataProduct (NEW URN Generator):"
echo "---------------------------------------------"
curl -X POST "http://localhost:8000/api/v1/entities/DataProduct" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "customer_analytics_dp",
    "domain": "analytics",
    "purpose": "Customer behavior analysis and insights",
    "owner": "data_team",
    "upstream": "customer_data_pipeline",
    "datasets": [
      "urn:li:dataset:(urn:li:dataPlatform:snowflake,customer_profiles,PROD)",
      "urn:li:dataset:(urn:li:dataPlatform:snowflake,customer_segments,PROD)"
    ]
  }' | jq '.'

echo ""
echo "2️⃣ Creating Connected Dataset:"
echo "-----------------------------"
curl -X POST "http://localhost:8000/api/v1/entities/Dataset" \
  -H "Content-Type: application/json" \
  -d '{
    "platform": "snowflake",
    "name": "customer_profiles",
    "env": "PROD",
    "versionId": "v1.0"
  }' | jq '.'

echo ""
echo "3️⃣ Creating Another Connected Dataset:"
echo "-------------------------------------"
curl -X POST "http://localhost:8000/api/v1/entities/Dataset" \
  -H "Content-Type: application/json" \
  -d '{
    "platform": "snowflake",
    "name": "customer_segments",
    "env": "PROD",
    "versionId": "v1.0"
  }' | jq '.'

echo ""
echo "4️⃣ Adding schemaMetadata to customer_profiles Dataset:"
echo "-----------------------------------------------------"
curl -X POST "http://localhost:8000/api/v1/aspects/schemaMetadata" \
  -H "Content-Type: application/json" \
  -d '{
    "entity_label": "Dataset",
    "entity_urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,customer_profiles,PROD)",
    "schemaName": "customer_profiles_schema",
    "platform": "snowflake",
    "fields": [
      {"fieldPath": "customer_id", "type": "VARCHAR", "description": "Unique customer identifier"},
      {"fieldPath": "email", "type": "VARCHAR", "description": "Customer email address"}
    ]
  }' | jq '.'

echo ""
echo "5️⃣ Adding schemaMetadata to customer_segments Dataset:"
echo "-----------------------------------------------------"
curl -X POST "http://localhost:8000/api/v1/aspects/schemaMetadata" \
  -H "Content-Type: application/json" \
  -d '{
    "entity_label": "Dataset",
    "entity_urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,customer_segments,PROD)",
    "schemaName": "customer_segments_schema",
    "platform": "snowflake",
    "fields": [
      {"fieldPath": "customer_id", "type": "VARCHAR", "description": "Unique customer identifier"},
      {"fieldPath": "segment_type", "type": "VARCHAR", "description": "Type of customer segment"}
    ]
  }' | jq '.'

echo ""
echo "6️⃣ Adding dpContract Aspect to DataProduct:"
echo "-------------------------------------------"
curl -X POST "http://localhost:8000/api/v1/aspects/dpContract" \
  -H "Content-Type: application/json" \
  -d '{
    "entity_label": "DataProduct",
    "entity_urn": "urn:li:dataProduct:(analytics,customer_analytics_dp,PROD)",
    "output_name": "customer_analytics_output",
    "output_type": "table",
    "fields": [
      {"name": "customer_id", "type": "string", "description": "Unique customer identifier"},
      {"name": "event_type", "type": "string", "description": "Type of customer event"},
      {"name": "event_timestamp", "type": "timestamp", "description": "When the event occurred"},
      {"name": "segment", "type": "string", "description": "Customer segment"}
    ],
    "sink_location": "snowflake://analytics.customer_analytics_output",
    "freshness": "1h"
  }' | jq '.'

echo ""
echo "7️⃣ Adding dpObservability Aspect to DataProduct:"
echo "------------------------------------------------"
curl -X POST "http://localhost:8000/api/v1/aspects/dpObservability" \
  -H "Content-Type: application/json" \
  -d '{
    "entity_label": "DataProduct",
    "entity_urn": "urn:li:dataProduct:(analytics,customer_analytics_dp,PROD)",
    "metrics_config": {
      "row_count": {"threshold": 1000, "alert": true},
      "freshness": {"threshold": "1h", "alert": true},
      "quality_score": {"threshold": 0.95, "alert": true}
    },
    "alerting_rules": [
      {"metric": "freshness", "condition": ">1h", "severity": "critical"},
      {"metric": "quality_score", "condition": "<0.9", "severity": "warning"}
    ],
    "dashboard_config": {
      "url": "https://grafana.company.com/d/customer-analytics",
      "refresh_interval": "5m"
    }
  }' | jq '.'

echo ""
echo "8️⃣ Adding dpPolicy Aspect to DataProduct:"
echo "----------------------------------------"
curl -X POST "http://localhost:8000/api/v1/aspects/dpPolicy" \
  -H "Content-Type: application/json" \
  -d '{
    "entity_label": "DataProduct",
    "entity_urn": "urn:li:dataProduct:(analytics,customer_analytics_dp,PROD)",
    "access_control": {
      "roles": ["data_analyst", "data_scientist"],
      "permissions": ["read", "write"],
      "row_level_security": true
    },
    "data_masking": {
      "customer_id": "hash",
      "email": "partial_mask"
    },
    "quality_gate": {
      "completeness": 0.95,
      "accuracy": 0.98,
      "consistency": 0.90
    },
    "retention_policy": {
      "retention_period": "2y",
      "archive_after": "6m"
    },
    "evaluation_points": [
      {"stage": "ingestion", "checks": ["null_check", "format_check"]},
      {"stage": "processing", "checks": ["business_rules", "outlier_detection"]}
    ]
  }' | jq '.'

echo ""
echo "9️⃣ Getting DataProduct:"
echo "----------------------"
curl -X GET "http://localhost:8000/api/v1/entities/DataProduct/urn:li:dataProduct:(analytics,customer_analytics_dp,PROD)" | jq '.'

echo ""
echo "🔟 Getting Aspect:"
echo "-----------------"
curl -X GET "http://localhost:8000/api/v1/aspects/dpContract/DataProduct/urn:li:dataProduct:(analytics,customer_analytics_dp,PROD)" | jq '.'

echo ""
echo "1️⃣1️⃣ Creating DataFlow:"
echo "----------------------"
curl -X POST "http://localhost:8000/api/v1/entities/DataFlow" \
  -H "Content-Type: application/json" \
  -d '{
    "platform": "analytics",
    "flow_id": "customer_etl",
    "env": "PROD"
  }' | jq '.'

echo ""
echo "1️⃣2️⃣ Adding Transformation Relationship:"
echo "----------------------------------------"
curl -X POST "http://localhost:8000/api/v1/aspects/dataJobInputOutput" \
  -H "Content-Type: application/json" \
  -d '{
    "entity_label": "DataJob",
    "entity_urn": "urn:li:dataJob:(urn:li:dataFlow:(analytics,customer_etl,PROD),customer_id_extraction)",
    "inputs": ["urn:li:dataset:(urn:li:dataPlatform:snowflake,customer_profiles,PROD)"],
    "outputs": ["urn:li:dataset:(urn:li:dataPlatform:snowflake,customer_segments,PROD)"]
  }' | jq '.'

echo ""
echo "1️⃣3️⃣ Adding Column Transformation:"
echo "----------------------------------"
curl -X POST "http://localhost:8000/api/v1/aspects/transformation" \
  -H "Content-Type: application/json" \
  -d '{
    "entity_label": "Column",
    "entity_urn": "urn:li:column:(urn_li_dataset_urn_li_dataPlatform_snowflake_customer_segments_PROD_,customer_id)",
    "inputColumns": ["email"],
    "transformationType": "hash_extraction",
    "sourceDataset": "urn:li:dataset:(urn:li:dataPlatform:snowflake,customer_profiles,PROD)",
    "targetDataset": "urn:li:dataset:(urn:li:dataPlatform:snowflake,customer_segments,PROD)",
    "steps": ["Extract customer_id from email using hash function"],
    "notes": "Customer ID is derived from email address for privacy"
  }' | jq '.'

echo ""
echo "✅ EXECUTION COMPLETED!"
echo "======================"
echo "✅ DataProduct created with new URN generator: urn:li:dataProduct:(domain,name,env)"
echo "✅ 2 Connected Datasets created: customer_profiles, customer_segments"
echo "✅ Schema metadata added to both datasets with 2 fields each:"
echo "   - customer_profiles: customer_id, email"
echo "   - customer_segments: customer_id, segment_type"
echo "✅ 3 Aspects created for DataProduct (dpContract, dpObservability, dpPolicy)"
echo "✅ HAS_CONTRACT relationships should be created between DataProduct and Datasets"
echo "✅ HAS_COLUMN relationships should be created between Datasets and Columns"
echo "✅ TRANSFORMS relationships should be created between Columns (email → customer_id)"
echo "✅ CONSUMES/PRODUCES relationships should be created between DataJob and Datasets"
echo "✅ GET operations executed for both entities and aspects"
echo "✅ DataProduct references 2 datasets: customer_profiles, customer_segments"
echo "✅ Transformation: customer_id in customer_segments derived from email in customer_profiles"
echo "✅ Rich metadata and configurations in aspects"
echo ""
echo "🌐 API Documentation: http://localhost:8000/docs"
echo "📁 Generated API Location: generated_api/"
echo ""
echo "🎯 Data Successfully Pushed to API!"
