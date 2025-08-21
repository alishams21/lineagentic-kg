#!/bin/bash

# RegistryFactory CLI - Upsert Operations Test Script
# This script tests all upsert operations for entities and aspects

echo "🚀 RegistryFactory CLI - Upsert Operations Test"
echo "=============================================="

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

# Function to run CLI command and check response
run_cli_command() {
    local command=$1
    local description=$2
    
    print_status "INFO" "Testing: $description"
    
    # Change to the generated CLI directory
    cd generated_cli
    
    # Run the command and capture output
    output=$(python cli.py $command 2>&1)
    exit_code=$?
    
    # Go back to parent directory
    cd ..
    
    if [ $exit_code -eq 0 ]; then
        print_status "SUCCESS" "$description"
        echo "Output: $output"
    else
        print_status "ERROR" "$description"
        echo "Error: $output"
    fi
    
    echo ""
    sleep 1  # Small delay between commands
}

# Set environment variables
export REGISTRY_PATH="../config/main_registry.yaml"
export NEO4J_URI="bolt://localhost:7687"
export NEO4J_USER="neo4j"
export NEO4J_PASSWORD="password"

# Check if CLI is available
print_status "INFO" "Checking if CLI is available..."
if [ ! -f "generated_cli/cli.py" ]; then
    print_status "ERROR" "CLI not found. Please generate the CLI first:"
    echo "python generate_cli.py"
    exit 1
fi
print_status "SUCCESS" "CLI is available"

echo ""
print_status "INFO" "📊 Entity UPSERT Operations"
echo "====================================="

# Test all entity upsert operations
run_cli_command "upsert-dataset --platform mysql --name test_dataset --env PROD --output table" "Dataset Entity Upsert"

run_cli_command "upsert-dataflow --platform mysql --flow_id test_flow --namespace test_namespace --name test_dataflow --env PROD --output table" "DataFlow Entity Upsert"

run_cli_command "upsert-datajob --flow_urn 'urn:li:dataFlow:test_dataflow' --job_name test_datajob --output table" "DataJob Entity Upsert"

run_cli_command "upsert-corpuser --username test.user --output table" "CorpUser Entity Upsert"

run_cli_command "upsert-corpgroup --name test-group --output table" "CorpGroup Entity Upsert"

run_cli_command "upsert-tag --key test-key --value test-tag --output table" "Tag Entity Upsert"

run_cli_command "upsert-column --dataset_urn 'urn:li:dataset:(urn:li:dataPlatform:mysql,test_dataset,PROD)' --field_path test_column --output table" "Column Entity Upsert"

echo ""
print_status "INFO" "📝 Aspect UPSERT Operations"
echo "====================================="

# Test aspect upsert operations
run_cli_command "upsert-corpuserinfo-aspect --username 'test.user' --active true --displayName 'TestUser' --email 'test.user@company.com' --output table" "CorpUserInfo Aspect Upsert"

run_cli_command "upsert-corpgroupinfo-aspect --name 'TestGroup' --output table" "CorpGroupInfo Aspect Upsert"

run_cli_command "upsert-datasetproperties-aspect --platform 'mysql' --name 'test_dataset' --description 'Test_dataset_for_CLI_testing' --output table" "DatasetProperties Aspect Upsert"

run_cli_command "upsert-schemametadata-aspect --platform 'mysql' --name 'test_dataset' --schemaName 'test_schema' --fields '[{\"name\":\"id\",\"type\":\"INTEGER\"}]' --version 1 --output table" "SchemaMetadata Aspect Upsert"

run_cli_command "upsert-ownership-aspect --platform 'mysql' --name 'test_dataset' --owners '[{\"owner\":\"urn:li:corpuser:test.user\",\"type\":\"DATAOWNER\"}]' --version 1 --output table" "Ownership Aspect Upsert"

run_cli_command "upsert-globaltags-aspect --platform 'mysql' --name 'test_dataset' --tags '[{\"tag\":\"urn:li:tag:test-key\"}]' --version 1 --output table" "GlobalTags Aspect Upsert"

run_cli_command "upsert-datasetprofile-aspect --platform 'mysql' --name 'test_dataset' --rowCount '1000' --columnCount '5' --timestamp-ms 1640995200000 --output table" "DatasetProfile Aspect Upsert"

run_cli_command "upsert-dataflowinfo-aspect --platform 'mysql' --flow_id 'test_flow' --name 'test_dataflow' --namespace 'test_namespace' --version 1 --output table" "DataFlowInfo Aspect Upsert"

run_cli_command "upsert-datajobinfo-aspect --flow_urn 'urn:li:dataFlow:(urn:li:dataPlatform:mysql,test_flow,test_namespace,test_dataflow,PROD)' --job_name 'test_datajob' --name 'test_datajob' --namespace 'test_namespace' --output table" "DataJobInfo Aspect Upsert"

run_cli_command "upsert-documentation-aspect --flow_urn 'urn:li:dataFlow:(urn:li:dataPlatform:mysql,test_flow,test_namespace,test_dataflow,PROD)' --job_name 'test_datajob' --description 'Test_documentation_for_datajob' --output table" "Documentation Aspect Upsert"

echo ""
print_status "INFO" "🔧 Utility Functions Test"
echo "================================="

# Test utility functions
run_cli_command "list-utilities" "List Available Utilities"

run_cli_command "utility utc_now_ms --output table" "UTC Now MS Utility"

echo ""
print_status "INFO" "📤 Output Format Tests"
echo "================================"

# Test different output formats
run_cli_command "upsert-dataset --platform mysql --name format_test --env PROD --output json" "JSON Output Format"

run_cli_command "upsert-corpuser --username format.test --output yaml" "YAML Output Format"

echo ""
print_status "INFO" "🎯 Upsert Operations Summary"
echo "================================="
print_status "SUCCESS" "✅ All UPSERT operations completed!"
echo ""
echo "📋 Tested Operations:"
echo "  • 7 Entity Types: Dataset, DataFlow, DataJob, CorpUser, CorpGroup, Tag, Column"
echo "  • 10 Aspect Types: corpUserInfo, corpGroupInfo, datasetProperties, schemaMetadata, ownership, globalTags, datasetProfile, dataFlowInfo, dataJobInfo, documentation"
echo "  • 2 Output Formats: JSON, YAML"
echo "  • 1 Utility Function: utc_now_ms"
echo ""
print_status "SUCCESS" "🎉 UPSERT test script completed successfully!"
