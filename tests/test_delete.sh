#!/bin/bash

# RegistryFactory API Test Script
# This script tests all DELETE endpoints for entities and aspects

echo "🗑️  Starting RegistryFactory API DELETE Tests"
echo "=============================================="

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
print_status "WARNING" "⚠️  WARNING: This script will DELETE data from your registry!"
print_status "WARNING" "Make sure you have backed up any important data before proceeding."
echo ""
read -p "Do you want to continue? (y/N): " -n 1 -r
echo ""
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    print_status "INFO" "Delete tests cancelled by user."
    exit 0
fi

# Test URNs (these should match what was created in test_upsert.sh)
TEST_DATASET_URN="urn:li:dataset:(urn:li:dataPlatform:mysql,test_db.test_table,PROD)"
TEST_DATAFLOW_URN="urn:li:dataFlow:(urn:li:dataPlatform:airflow,test_flow_001,PROD)"
TEST_DATAJOB_URN="urn:li:dataJob:(urn:li:dataFlow:(urn:li:dataPlatform:airflow,test_flow_001,PROD),test_job_001)"
TEST_CORPUSER_URN="urn:li:corpuser:test.user"
TEST_CORPGROUP_URN="urn:li:corpgroup:data-engineers"
TEST_TAG_URN="urn:li:tag:data-quality"
TEST_COLUMN_URN="urn:li:column:(urn:li:dataset:(urn:li:dataPlatform:mysql,test_db.test_table,PROD),id)"

echo ""
print_status "INFO" "Starting Aspect DELETE Tests (delete aspects first)"
echo "================================================================"

# Delete aspects first (before entities)

# 1. CorpUserInfo Aspect
make_request "DELETE" "/aspects/corpUserInfo/CorpUser/$TEST_CORPUSER_URN" "CorpUserInfo Aspect DELETE"

# 2. CorpGroupInfo Aspect
make_request "DELETE" "/aspects/corpGroupInfo/CorpGroup/$TEST_CORPGROUP_URN" "CorpGroupInfo Aspect DELETE"

# 3. DatasetProperties Aspect
make_request "DELETE" "/aspects/datasetProperties/Dataset/$TEST_DATASET_URN" "DatasetProperties Aspect DELETE"

# 4. SchemaMetadata Aspect
make_request "DELETE" "/aspects/schemaMetadata/Dataset/$TEST_DATASET_URN" "SchemaMetadata Aspect DELETE"

# 5. Ownership Aspect
make_request "DELETE" "/aspects/ownership/Dataset/$TEST_DATASET_URN" "Ownership Aspect DELETE"

# 6. GlobalTags Aspect
make_request "DELETE" "/aspects/globalTags/Dataset/$TEST_DATASET_URN" "GlobalTags Aspect DELETE"

# 7. DatasetProfile Aspect
make_request "DELETE" "/aspects/datasetProfile/Dataset/$TEST_DATASET_URN" "DatasetProfile Aspect DELETE"

# 8. DataFlowInfo Aspect
make_request "DELETE" "/aspects/dataFlowInfo/DataFlow/$TEST_DATAFLOW_URN" "DataFlowInfo Aspect DELETE"

# 9. DataJobInfo Aspect
make_request "DELETE" "/aspects/dataJobInfo/DataJob/$TEST_DATAJOB_URN" "DataJobInfo Aspect DELETE"

# 10. Documentation Aspect
make_request "DELETE" "/aspects/documentation/DataJob/$TEST_DATAJOB_URN" "Documentation Aspect DELETE"

# 11. SourceCodeLocation Aspect
make_request "DELETE" "/aspects/sourceCodeLocation/DataJob/$TEST_DATAJOB_URN" "SourceCodeLocation Aspect DELETE"

# 12. SourceCode Aspect
make_request "DELETE" "/aspects/sourceCode/DataJob/$TEST_DATAJOB_URN" "SourceCode Aspect DELETE"

# 13. EnvironmentProperties Aspect
make_request "DELETE" "/aspects/environmentProperties/DataJob/$TEST_DATAJOB_URN" "EnvironmentProperties Aspect DELETE"

# 14. DataJobInputOutput Aspect
make_request "DELETE" "/aspects/dataJobInputOutput/DataJob/$TEST_DATAJOB_URN" "DataJobInputOutput Aspect DELETE"

# 15. DataJobRun Aspect
make_request "DELETE" "/aspects/dataJobRun/DataJob/$TEST_DATAJOB_URN" "DataJobRun Aspect DELETE"

# 16. ColumnProperties Aspect
make_request "DELETE" "/aspects/columnProperties/Column/$TEST_COLUMN_URN" "ColumnProperties Aspect DELETE"

# 17. Transformation Aspect
make_request "DELETE" "/aspects/transformation/Column/$TEST_COLUMN_URN" "Transformation Aspect DELETE"

echo ""
print_status "INFO" "Starting Entity DELETE Tests (delete entities last)"
echo "================================================================"

# Delete entities last (after aspects)

# 1. Column Entity (delete first as it depends on Dataset)
make_request "DELETE" "/entities/Column/$TEST_COLUMN_URN" "Column Entity DELETE"

# 2. DataJob Entity (delete before DataFlow)
make_request "DELETE" "/entities/DataJob/$TEST_DATAJOB_URN" "DataJob Entity DELETE"

# 3. DataFlow Entity
make_request "DELETE" "/entities/DataFlow/$TEST_DATAFLOW_URN" "DataFlow Entity DELETE"

# 4. Dataset Entity
make_request "DELETE" "/entities/Dataset/$TEST_DATASET_URN" "Dataset Entity DELETE"

# 5. CorpUser Entity
make_request "DELETE" "/entities/CorpUser/$TEST_CORPUSER_URN" "CorpUser Entity DELETE"

# 6. CorpGroup Entity
make_request "DELETE" "/entities/CorpGroup/$TEST_CORPGROUP_URN" "CorpGroup Entity DELETE"

# 7. Tag Entity
make_request "DELETE" "/entities/Tag/$TEST_TAG_URN" "Tag Entity DELETE"

echo ""
print_status "INFO" "All DELETE tests completed!"
echo "======================================"
print_status "SUCCESS" "DELETE test script finished. Check the output above for any errors."
print_status "INFO" "Note: All test data has been removed from the registry."
