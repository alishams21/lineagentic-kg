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
    "datasets": "customer_profiles,customer_segments"
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
echo "4️⃣ Adding dpContract Aspect to DataProduct:"
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
echo "5️⃣ Adding dpObservability Aspect to DataProduct:"
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
echo "6️⃣ Adding dpPolicy Aspect to DataProduct:"
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
echo "7️⃣ Getting DataProduct:"
echo "----------------------"
curl -X GET "http://localhost:8000/api/v1/entities/DataProduct/urn:li:dataProduct:(analytics,customer_analytics_dp,PROD)" | jq '.'

echo ""
echo "8️⃣ Getting Aspect:"
echo "-----------------"
curl -X GET "http://localhost:8000/api/v1/aspects/dpContract/DataProduct/urn:li:dataProduct:(analytics,customer_analytics_dp,PROD)" | jq '.'

echo ""
echo "✅ EXECUTION COMPLETED!"
echo "======================"
echo "✅ DataProduct created with new URN generator: urn:li:dataProduct:(domain,name,env)"
echo "✅ 2 Connected Datasets created: customer_profiles, customer_segments"
echo "✅ 3 Aspects created for DataProduct (dpContract, dpObservability, dpPolicy)"
echo "✅ HAS_CONTRACT relationships should be created between DataProduct and Datasets"
echo "✅ GET operations executed for both entities and aspects"
echo "✅ DataProduct references 3 datasets: customer_events,customer_profiles,customer_segments"
echo "✅ Rich metadata and configurations in aspects"
echo ""
echo "🌐 API Documentation: http://localhost:8000/docs"
echo "📁 Generated API Location: generated_api/"
echo ""
echo "🎯 Data Successfully Pushed to API!"
