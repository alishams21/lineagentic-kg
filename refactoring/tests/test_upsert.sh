#!/bin/bash

# RegistryFactory API Test Script
# This script tests all upsert endpoints for entities and aspects

echo "🚀 Starting RegistryFactory API Tests"
echo "======================================"

# Base URL for the API
BASE_URL="http://localhost:8000/api/v1"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    local status=$1
    local message=$2
    case $status in
        "SUCCESS")
            echo -e "${GREEN}✅ $message${NC}"
            ;;
        "ERROR")
            echo -e "${RED}❌ $message${NC}"
            ;;
        "INFO")
            echo -e "${BLUE}ℹ️  $message${NC}"
            ;;
        "WARNING")
            echo -e "${YELLOW}⚠️  $message${NC}"
            ;;
    esac
}

# Function to make curl request and check response
make_request() {
    local method=$1
    local endpoint=$2
    local data=$3
    local description=$4
    
    print_status "INFO" "Testing: $description"
    
    if [ -n "$data" ]; then
        response=$(curl -s -w "\n%{http_code}" -X $method "$BASE_URL$endpoint" \
            -H "Content-Type: application/json" \
            -d "$data")
    else
        response=$(curl -s -w "\n%{http_code}" -X $method "$BASE_URL$endpoint")
    fi
    
    # Extract status code (last line)
    status_code=$(echo "$response" | tail -n1)
    # Extract response body (all lines except last) - macOS compatible
    body=$(echo "$response" | sed '$d')
    
    if [ "$status_code" -ge 200 ] && [ "$status_code" -lt 300 ]; then
        print_status "SUCCESS" "$description - Status: $status_code"
        echo "Response: $body" | jq '.' 2>/dev/null || echo "Response: $body"
    else
        print_status "ERROR" "$description - Status: $status_code"
        echo "Error: $body"
    fi
    
    echo ""
    sleep 1  # Small delay between requests
}

# Check if API is running
print_status "INFO" "Checking if API is running..."
health_response=$(curl -s "$BASE_URL/health")
if [ $? -ne 0 ]; then
    print_status "ERROR" "API is not running. Please start the API first:"
    echo "cd test_generated_api && python main.py"
    exit 1
fi
print_status "SUCCESS" "API is running"

echo ""
print_status "INFO" "Starting Entity Upsert Tests"
echo "======================================"

# 1. Dataset Entity
make_request "POST" "/entities/Dataset" '{
  "platform": "mysql",
  "name": "test_db.test_table",
  "env": "PROD",
  "additional_properties": {
    "description": "Test table for API examples",
    "owner": "data-team"
  }
}' "Dataset Entity Upsert"

# 2. DataFlow Entity
make_request "POST" "/entities/DataFlow" '{
  "platform": "airflow",
  "flow_id": "test_flow_001",
  "namespace": "default",
  "name": "test_data_pipeline",
  "env": "PROD",
  "additional_properties": {
    "description": "Test data flow for API examples",
    "owner": "data-team"
  }
}' "DataFlow Entity Upsert"

# 3. DataJob Entity
make_request "POST" "/entities/DataJob" '{
  "flow_urn": "urn:li:dataFlow:(airflow,test_flow_001,PROD)",
  "job_name": "test_job_001",
  "additional_properties": {
    "description": "Test data job for API examples",
    "owner": "data-team"
  }
}' "DataJob Entity Upsert"

# 4. CorpUser Entity
make_request "POST" "/entities/CorpUser" '{
  "username": "test.user",
  "additional_properties": {
    "description": "Test user for API examples",
    "department": "Engineering"
  }
}' "CorpUser Entity Upsert"

# 5. CorpGroup Entity
make_request "POST" "/entities/CorpGroup" '{
  "name": "data-engineers",
  "additional_properties": {
    "description": "Data engineering team",
    "slack_channel": "#data-eng"
  }
}' "CorpGroup Entity Upsert"

# 6. Tag Entity
make_request "POST" "/entities/Tag" '{
  "key": "data-quality",
  "value": "high",
  "additional_properties": {
    "description": "Data quality tag",
    "category": "quality"
  }
}' "Tag Entity Upsert"

# 7. Column Entity
make_request "POST" "/entities/Column" '{
  "dataset_urn": "urn:li:dataset:(urn:li:dataPlatform:mysql,test_db.test_table,PROD)",
  "field_path": "id",
  "additional_properties": {
    "description": "Primary key column",
    "data_type": "INTEGER"
  }
}' "Column Entity Upsert"

echo ""
print_status "INFO" "Starting Aspect Upsert Tests"
echo "======================================"

# 1. CorpUserInfo Aspect
make_request "POST" "/aspects/corpUserInfo" '{
  "entity_label": "CorpUser",
  "entity_params": {
    "username": "test.user"
  },
  "active": true,
  "displayName": "Test User",
  "email": "test.user@company.com",
  "title": "Data Engineer",
  "department": "Engineering",
  "managerUrn": "urn:li:corpuser:manager.user",
  "skills": ["python", "sql", "airflow"]
}' "CorpUserInfo Aspect Upsert"

# 2. CorpGroupInfo Aspect
make_request "POST" "/aspects/corpGroupInfo" '{
  "entity_label": "CorpGroup",
  "entity_params": {
    "name": "data-engineers"
  },
  "name": "Data Engineers",
  "description": "Data engineering team responsible for data pipelines",
  "email": "data-eng@company.com",
  "slackChannel": "#data-eng"
}' "CorpGroupInfo Aspect Upsert"

# 3. DatasetProperties Aspect
make_request "POST" "/aspects/datasetProperties" '{
  "entity_label": "Dataset",
  "entity_params": {
    "platform": "mysql",
    "name": "test_db.test_table"
  },
  "description": "Test table for API examples and testing",
  "customProperties": {
    "owner": "data-team",
    "refresh_frequency": "daily"
  },
  "tags": ["test", "api"],
  "externalUrl": "https://company.com/docs/test_table"
}' "DatasetProperties Aspect Upsert"

# 4. SchemaMetadata Aspect
make_request "POST" "/aspects/schemaMetadata" '{
  "entity_label": "Dataset",
  "entity_params": {
    "platform": "mysql",
    "name": "test_db.test_table"
  },
  "schemaName": "test_table",
  "platform": "mysql",
  "version": "1.0",
  "fields": [
    {
      "fieldPath": "id",
      "type": "INTEGER",
      "nullable": false,
      "description": "Primary key"
    },
    {
      "fieldPath": "name",
      "type": "VARCHAR(255)",
      "nullable": true,
      "description": "User name"
    }
  ],
  "primaryKeys": ["id"],
  "foreignKeys": []
}' "SchemaMetadata Aspect Upsert"

# 5. Ownership Aspect
make_request "POST" "/aspects/ownership" '{
  "entity_label": "Dataset",
  "entity_params": {
    "platform": "mysql",
    "name": "test_db.test_table"
  },
  "owners": [
    {
      "owner": "urn:li:corpuser:test.user",
      "type": "DATAOWNER"
    },
    {
      "owner": "urn:li:corpgroup:data-engineers",
      "type": "DELEGATE"
    }
  ],
  "lastModified": {
    "time": 1640995200000,
    "actor": "urn:li:corpuser:admin.user"
  }
}' "Ownership Aspect Upsert"

# 6. GlobalTags Aspect
make_request "POST" "/aspects/globalTags" '{
  "entity_label": "Dataset",
  "entity_params": {
    "platform": "mysql",
    "name": "test_db.test_table"
  },
  "tags": [
    {
      "tag": "urn:li:tag:data-quality",
      "associatedUrn": "urn:li:dataset:(urn:li:dataPlatform:mysql,test_db.test_table,PROD)"
    },
    {
      "tag": "urn:li:tag:test",
      "associatedUrn": "urn:li:dataset:(urn:li:dataPlatform:mysql,test_db.test_table,PROD)"
    }
  ]
}' "GlobalTags Aspect Upsert"

# 7. DatasetProfile Aspect
make_request "POST" "/aspects/datasetProfile" '{
  "entity_label": "Dataset",
  "entity_params": {
    "platform": "mysql",
    "name": "test_db.test_table"
  },
  "rowCount": 1000000,
  "columnCount": 5,
  "sizeInBytes": 52428800,
  "lastModified": 1640995200000,
  "partitionCount": 1
}' "DatasetProfile Aspect Upsert"

# 8. DataFlowInfo Aspect
make_request "POST" "/aspects/dataFlowInfo" '{
  "entity_label": "DataFlow",
  "entity_params": {
    "platform": "airflow",
    "flow_id": "test_flow_001"
  },
  "name": "Test Data Pipeline",
  "namespace": "default",
  "description": "Test data flow for API examples",
  "version": "1.0"
}' "DataFlowInfo Aspect Upsert"

# 9. DataJobInfo Aspect
make_request "POST" "/aspects/dataJobInfo" '{
  "entity_label": "DataJob",
  "entity_params": {
    "flow_urn": "urn:li:dataFlow:(airflow,test_flow_001,PROD)",
    "job_name": "test_job_001"
  },
  "name": "Test Job 001",
  "namespace": "default",
  "versionId": "1.0",
  "integration": "airflow",
  "processingType": "BATCH",
  "jobType": "ETL",
  "description": "Test data job for API examples"
}' "DataJobInfo Aspect Upsert"

# 10. Documentation Aspect
make_request "POST" "/aspects/documentation" '{
  "entity_label": "DataJob",
  "entity_params": {
    "flow_urn": "urn:li:dataFlow:(airflow,test_flow_001,PROD)",
    "job_name": "test_job_001"
  },
  "description": "Documentation for test job",
  "contentType": "text/markdown",
  "content": "# Test Job Documentation\n\nThis is a test job for API examples."
}' "Documentation Aspect Upsert"

# 11. SourceCodeLocation Aspect
make_request "POST" "/aspects/sourceCodeLocation" '{
  "entity_label": "DataJob",
  "entity_params": {
    "flow_urn": "urn:li:dataFlow:(airflow,test_flow_001,PROD)",
    "job_name": "test_job_001"
  },
  "type": "GITHUB",
  "url": "https://github.com/company/data-pipelines",
  "repo": "data-pipelines",
  "branch": "main",
  "path": "jobs/test_job_001.py"
}' "SourceCodeLocation Aspect Upsert"

# 12. SourceCode Aspect
make_request "POST" "/aspects/sourceCode" '{
  "entity_label": "DataJob",
  "entity_params": {
    "flow_urn": "urn:li:dataFlow:(airflow,test_flow_001,PROD)",
    "job_name": "test_job_001"
  },
  "language": "python",
  "snippet": "def process_data():\n    return \"Hello World\"",
  "fullCode": "import pandas as pd\n\ndef process_data():\n    df = pd.read_csv(\"data.csv\")\n    return df.head()"
}' "SourceCode Aspect Upsert"

# 13. EnvironmentProperties Aspect
make_request "POST" "/aspects/environmentProperties" '{
  "entity_label": "DataJob",
  "entity_params": {
    "flow_urn": "urn:li:dataFlow:(airflow,test_flow_001,PROD)",
    "job_name": "test_job_001"
  },
  "env": "PROD",
  "config": {
    "memory": "4GB",
    "cpu": "2",
    "timeout": "3600"
  }
}' "EnvironmentProperties Aspect Upsert"

# 14. DataJobInputOutput Aspect
make_request "POST" "/aspects/dataJobInputOutput" '{
  "entity_label": "DataJob",
  "entity_params": {
    "flow_urn": "urn:li:dataFlow:(airflow,test_flow_001,PROD)",
    "job_name": "test_job_001"
  },
  "inputs": [
    "urn:li:dataset:(urn:li:dataPlatform:mysql,source_db.source_table,PROD)"
  ],
  "outputs": [
    "urn:li:dataset:(urn:li:dataPlatform:mysql,test_db.test_table,PROD)"
  ]
}' "DataJobInputOutput Aspect Upsert"

# 15. DataJobRun Aspect
make_request "POST" "/aspects/dataJobRun" '{
  "entity_label": "DataJob",
  "entity_params": {
    "flow_urn": "urn:li:dataFlow:(airflow,test_flow_001,PROD)",
    "job_name": "test_job_001"
  },
  "eventType": "START",
  "runId": "test_run_001",
  "parent": "urn:li:dataFlow:(airflow,test_flow_001,PROD)",
  "status": "RUNNING",
  "startTime": 1640995200000,
  "endTime": null
}' "DataJobRun Aspect Upsert"

# 16. ColumnProperties Aspect
make_request "POST" "/aspects/columnProperties" '{
  "entity_label": "Column",
  "entity_params": {
    "dataset_urn": "urn:li:dataset:(urn:li:dataPlatform:mysql,test_db.test_table,PROD)",
    "field_path": "id"
  },
  "description": "Primary key column for the test table",
  "dataType": "INTEGER",
  "nullable": false,
  "defaultValue": null
}' "ColumnProperties Aspect Upsert"

# 17. Transformation Aspect
make_request "POST" "/aspects/transformation" '{
  "entity_label": "Column",
  "entity_params": {
    "dataset_urn": "urn:li:dataset:(urn:li:dataPlatform:mysql,test_db.test_table,PROD)",
    "field_path": "id"
  },
  "inputColumns": [
    "urn:li:column:(urn:li:dataset:(urn:li:dataPlatform:mysql,source_db.source_table,PROD),source_id)"
  ],
  "steps": [
    {
      "operation": "IDENTITY",
      "description": "Direct mapping from source_id"
    }
  ],
  "notes": "Simple identity transformation",
  "sourceDataset": "urn:li:dataset:(urn:li:dataPlatform:mysql,source_db.source_table,PROD)",
  "targetDataset": "urn:li:dataset:(urn:li:dataPlatform:mysql,test_db.test_table,PROD)",
  "transformationType": "IDENTITY"
}' "Transformation Aspect Upsert"

echo ""
print_status "INFO" "All tests completed!"
echo "======================================"
print_status "SUCCESS" "Test script finished. Check the output above for any errors."
