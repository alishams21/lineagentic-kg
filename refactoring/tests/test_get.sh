#!/bin/bash

# RegistryFactory API Test Script
# This script tests all GET endpoints for entities and aspects

echo "🔍 Starting RegistryFactory API GET Tests"
echo "=========================================="

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
    local description=$3
    
    print_status "INFO" "Testing: $description"
    
    response=$(curl -s -w "\n%{http_code}" -X $method "$BASE_URL$endpoint")
    
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
    echo "cd generated_api && python main.py"
    exit 1
fi
print_status "SUCCESS" "API is running"

echo ""
print_status "INFO" "Starting Entity GET Tests"
echo "======================================"

# Test URNs (using actual generated URN formats from factory)
# These match what the factory actually generates based on the entity_params
TEST_DATASET_URN="urn:li:dataset:(urn:li:dataPlatform:mysql,test_db.test_table,PROD)"
TEST_DATAFLOW_URN="urn:li:dataFlow:(airflow,test_flow_001,PROD)"
TEST_DATAJOB_URN="urn:li:dataJob:(urn_li_dataFlow_airflow_test_flow_001_PROD_,test_job_001)"  # Actual generated format
TEST_CORPUSER_URN="urn:li:corpuser:test.user"
TEST_CORPGROUP_URN="urn:li:corpGroup:data-engineers"  # Fixed: uppercase 'G' in corpGroup
TEST_TAG_URN="urn:li:tag:data-quality=high"  # Actual generated format with value
TEST_COLUMN_URN="urn:li:column:(urn_li_dataset_urn_li_dataPlatform_mysql_test_db.test_table_PROD_,id)"  # Actual generated format

# 1. Dataset Entity
make_request "GET" "/entities/Dataset/$TEST_DATASET_URN" "Dataset Entity GET"

# 2. DataFlow Entity
make_request "GET" "/entities/DataFlow/$TEST_DATAFLOW_URN" "DataFlow Entity GET"

# 3. DataJob Entity
make_request "GET" "/entities/DataJob/$TEST_DATAJOB_URN" "DataJob Entity GET"

# 4. CorpUser Entity
make_request "GET" "/entities/CorpUser/$TEST_CORPUSER_URN" "CorpUser Entity GET"

# 5. CorpGroup Entity
make_request "GET" "/entities/CorpGroup/$TEST_CORPGROUP_URN" "CorpGroup Entity GET"

# 6. Tag Entity
make_request "GET" "/entities/Tag/$TEST_TAG_URN" "Tag Entity GET"

# 7. Column Entity
make_request "GET" "/entities/Column/$TEST_COLUMN_URN" "Column Entity GET"

echo ""
print_status "INFO" "Starting Aspect GET Tests (Versioned Aspects Only)"
echo "================================================================"

# Only test versioned aspects for now (timeseries aspects have API issues)
# Versioned aspects: corpUserInfo, corpGroupInfo, datasetProperties, schemaMetadata, 
# ownership, globalTags, dataFlowInfo, dataJobInfo, documentation, sourceCodeLocation, 
# sourceCode, environmentProperties, dataJobInputOutput, columnProperties, transformation

# 1. CorpUserInfo Aspect (versioned)
make_request "GET" "/aspects/corpUserInfo/CorpUser/$TEST_CORPUSER_URN" "CorpUserInfo Aspect GET"

# 2. CorpGroupInfo Aspect (versioned)
make_request "GET" "/aspects/corpGroupInfo/CorpGroup/$TEST_CORPGROUP_URN" "CorpGroupInfo Aspect GET"

# 3. DatasetProperties Aspect (versioned)
make_request "GET" "/aspects/datasetProperties/Dataset/$TEST_DATASET_URN" "DatasetProperties Aspect GET"

# 4. SchemaMetadata Aspect (versioned)
make_request "GET" "/aspects/schemaMetadata/Dataset/$TEST_DATASET_URN" "SchemaMetadata Aspect GET"

# 5. Ownership Aspect (versioned)
make_request "GET" "/aspects/ownership/Dataset/$TEST_DATASET_URN" "Ownership Aspect GET"

# 6. GlobalTags Aspect (versioned)
make_request "GET" "/aspects/globalTags/Dataset/$TEST_DATASET_URN" "GlobalTags Aspect GET"

# 7. DataFlowInfo Aspect (versioned)
make_request "GET" "/aspects/dataFlowInfo/DataFlow/$TEST_DATAFLOW_URN" "DataFlowInfo Aspect GET"

# 8. DataJobInfo Aspect (versioned)
make_request "GET" "/aspects/dataJobInfo/DataJob/$TEST_DATAJOB_URN" "DataJobInfo Aspect GET"

# 9. Documentation Aspect (versioned)
make_request "GET" "/aspects/documentation/DataJob/$TEST_DATAJOB_URN" "Documentation Aspect GET"

# 10. SourceCodeLocation Aspect (versioned)
make_request "GET" "/aspects/sourceCodeLocation/DataJob/$TEST_DATAJOB_URN" "SourceCodeLocation Aspect GET"

# 11. SourceCode Aspect (versioned)
make_request "GET" "/aspects/sourceCode/DataJob/$TEST_DATAJOB_URN" "SourceCode Aspect GET"

# 12. EnvironmentProperties Aspect (versioned)
make_request "GET" "/aspects/environmentProperties/DataJob/$TEST_DATAJOB_URN" "EnvironmentProperties Aspect GET"

# 13. DataJobInputOutput Aspect (versioned)
make_request "GET" "/aspects/dataJobInputOutput/DataJob/$TEST_DATAJOB_URN" "DataJobInputOutput Aspect GET"

# 14. ColumnProperties Aspect (versioned)
make_request "GET" "/aspects/columnProperties/Column/$TEST_COLUMN_URN" "ColumnProperties Aspect GET"

# 15. Transformation Aspect (versioned)
make_request "GET" "/aspects/transformation/Column/$TEST_COLUMN_URN" "Transformation Aspect GET"

# 16. Dataset Profile Aspect (timeseries)
make_request "GET" "/aspects/datasetProfile/Dataset/$TEST_DATASET_URN" "DatasetProfile Aspect GET (timeseries)"

# 17. Data Job Run Aspect (timeseries)
make_request "GET" "/aspects/dataJobRun/DataJob/$TEST_DATAJOB_URN" "DataJobRun Aspect GET (timeseries)"

echo ""
print_status "INFO" "All GET tests completed!"
echo "======================================"
print_status "SUCCESS" "GET test script finished. Check the output above for any errors."
