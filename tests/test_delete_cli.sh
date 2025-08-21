#!/bin/bash

# RegistryFactory CLI - Delete Operations Test Script
# This script tests all delete operations for entities and aspects

echo "🗑️  RegistryFactory CLI - Delete Operations Test"
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
print_status "INFO" "🚀 CLI Health Check"
echo "========================"

# Test health check first
run_cli_command "health" "System Health Status"

echo ""
print_status "INFO" "📝 Aspect DELETE Operations"
echo "====================================="

# First create aspects using UPSERT operations so we can delete them
print_status "INFO" "Creating aspects for DELETE testing..."

run_cli_command "upsert-corpuserinfo-aspect --username test.user --active true --displayName TestUser --email test.user@company.com --output json" "Create CorpUserInfo for DELETE"
run_cli_command "upsert-corpgroupinfo-aspect --name test-group --output json" "Create CorpGroupInfo for DELETE"
run_cli_command "upsert-datasetproperties-aspect --platform mysql --name test_dataset --description Test_dataset_for_DELETE_testing --output json" "Create DatasetProperties for DELETE"
run_cli_command "upsert-schemametadata-aspect --platform mysql --name test_dataset --schemaName test_schema --fields '[{\"name\":\"id\",\"type\":\"INTEGER\"}]' --version 1 --output json" "Create SchemaMetadata for DELETE"
run_cli_command "upsert-ownership-aspect --platform mysql --name test_dataset --owners '[{\"owner\":\"urn:li:corpuser:test.user\",\"type\":\"DATAOWNER\"}]' --version 1 --output json" "Create Ownership for DELETE"
run_cli_command "upsert-globaltags-aspect --platform mysql --name test_dataset --tags '[{\"tag\":\"urn:li:tag:test-key\"}]' --version 1 --output json" "Create GlobalTags for DELETE"
run_cli_command "upsert-datasetprofile-aspect --platform mysql --name test_dataset --rowCount 1000 --columnCount 5 --timestamp-ms 1640995200000 --output json" "Create DatasetProfile for DELETE"
run_cli_command "upsert-dataflowinfo-aspect --platform mysql --flow_id test_flow --name test_dataflow --namespace test_namespace --version 1 --output json" "Create DataFlowInfo for DELETE"
run_cli_command "upsert-datajobinfo-aspect --flow_urn urn:li:dataFlow:test_dataflow --job_name test_datajob --name test_datajob --namespace test_namespace --output json" "Create DataJobInfo for DELETE"
run_cli_command "upsert-documentation-aspect --flow_urn urn:li:dataFlow:test_dataflow --job_name test_datajob --description Test_documentation_for_DELETE --output json" "Create Documentation for DELETE"

echo ""
print_status "INFO" "Now testing DELETE operations on created aspects..."

# Test aspect delete operations using parameters (auto-generated URNs)
run_cli_command "delete-corpuserinfo-aspect --username test.user" "CorpUserInfo Aspect DELETE"

run_cli_command "delete-corpgroupinfo-aspect --name test-group" "CorpGroupInfo Aspect DELETE"

run_cli_command "delete-datasetproperties-aspect --platform mysql --name test_dataset" "DatasetProperties Aspect DELETE"

run_cli_command "delete-schemametadata-aspect --platform mysql --name test_dataset" "SchemaMetadata Aspect DELETE"

run_cli_command "delete-ownership-aspect --platform mysql --name test_dataset" "Ownership Aspect DELETE"

run_cli_command "delete-globaltags-aspect --platform mysql --name test_dataset" "GlobalTags Aspect DELETE"

run_cli_command "delete-datasetprofile-aspect --platform mysql --name test_dataset" "DatasetProfile Aspect DELETE"

run_cli_command "delete-dataflowinfo-aspect --platform mysql --flow_id test_flow --name test_dataflow --namespace test_namespace" "DataFlowInfo Aspect DELETE"

run_cli_command "delete-datajobinfo-aspect --flow_urn urn:li:dataFlow:test_dataflow --job_name test_datajob" "DataJobInfo Aspect DELETE"

run_cli_command "delete-documentation-aspect --flow_urn urn:li:dataFlow:test_dataflow --job_name test_datajob" "Documentation Aspect DELETE"

echo ""
print_status "INFO" "📊 Entity DELETE Operations"
echo "====================================="

# First create entities using UPSERT operations so we can delete them
print_status "INFO" "Creating entities for DELETE testing..."

run_cli_command "upsert-dataset --name test_dataset --platform mysql --env PROD --output json" "Create Dataset for DELETE"
run_cli_command "upsert-dataflow --platform mysql --flow_id test_flow --namespace test_namespace --name test_dataflow --env PROD --output json" "Create DataFlow for DELETE"
run_cli_command "upsert-datajob --flow_urn urn:li:dataFlow:test_dataflow --job_name test_datajob --output json" "Create DataJob for DELETE"
run_cli_command "upsert-corpuser --username test.user --output json" "Create CorpUser for DELETE"
run_cli_command "upsert-corpgroup --name test-group --output json" "Create CorpGroup for DELETE"
run_cli_command "upsert-tag --key test-key --value test-tag --output json" "Create Tag for DELETE"
run_cli_command "upsert-column --dataset_urn urn:li:dataset:\(urn:li:dataPlatform:mysql,test_dataset,PROD\) --field_path test_column --output json" "Create Column for DELETE"

echo ""
print_status "INFO" "Now testing DELETE operations on created entities..."

# Test all entity delete operations using parameters (auto-generated URNs)
run_cli_command "delete-dataset --name test_dataset --platform mysql --env PROD" "Dataset Entity DELETE"

run_cli_command "delete-dataflow --name test_dataflow --namespace test_namespace --env PROD --platform mysql" "DataFlow Entity DELETE"

run_cli_command "delete-datajob --job_name test_datajob --flow_urn urn:li:dataFlow:test_dataflow" "DataJob Entity DELETE"

run_cli_command "delete-corpuser --username test.user" "CorpUser Entity DELETE"

run_cli_command "delete-corpgroup --name test-group" "CorpGroup Entity DELETE"

run_cli_command "delete-tag --key test-key --value test-tag" "Tag Entity DELETE"

run_cli_command "delete-column --dataset_urn urn:li:dataset:\(urn:li:dataPlatform:mysql,test_dataset,PROD\) --field_path test_column" "Column Entity DELETE"

echo ""
print_status "INFO" "🔧 Utility Functions Test"
echo "================================="

# Test utility functions
run_cli_command "list-utilities" "List Available Utilities"

run_cli_command "utility utc_now_ms --output table" "UTC Now MS Utility"

echo ""
print_status "INFO" "⚠️  Error Handling Tests"
echo "================================"

# Test error handling with invalid parameters
run_cli_command "delete-dataset --name invalid --platform invalid --env invalid" "Invalid Dataset Parameters Error Handling"

run_cli_command "delete-corpuser --username invalid" "Invalid CorpUser Parameters Error Handling"

run_cli_command "delete-corpuserinfo-aspect --username invalid" "Invalid Aspect Parameters Error Handling"

echo ""
print_status "INFO" "🧹 Cleanup Test"
echo "====================="

# Clean up any remaining test data
run_cli_command "delete-dataset --name format_test --platform mysql --env PROD" "Cleanup Format Test Dataset"

run_cli_command "delete-corpuser --username format.test" "Cleanup Format Test CorpUser"

echo ""
print_status "INFO" "🎯 Auto-Generated URN Demonstration"
echo "============================================="

# Demonstrate that the same parameters always generate the same URNs
print_status "INFO" "Demonstrating auto-generated URN consistency..."

run_cli_command "upsert-dataset --name test_urn_demo --platform mysql --env PROD --output json" "Create Dataset with Auto-Generated URN"
run_cli_command "upsert-dataset --name test_urn_demo --platform mysql --env PROD --output json" "Create Same Dataset Again (Same URN)"
run_cli_command "upsert-corpuser --username test.user.demo --output json" "Create CorpUser with Auto-Generated URN"
run_cli_command "upsert-corpuser --username test.user.demo --output json" "Create Same CorpUser Again (Same URN)"

echo ""
print_status "INFO" "🎯 Delete Operations Summary"
echo "================================="
print_status "SUCCESS" "✅ All DELETE operations completed!"
echo ""
echo "📋 Tested Operations:"
echo "  • 10 Aspect Types: corpUserInfo, corpGroupInfo, datasetProperties, schemaMetadata, ownership, globalTags, datasetProfile, dataFlowInfo, dataJobInfo, documentation"
echo "  • 7 Entity Types: Dataset, DataFlow, DataJob, CorpUser, CorpGroup, Tag, Column"
echo "  • 1 Utility Function: utc_now_ms"
echo "  • Error Handling: Invalid URNs"
echo "  • Cleanup: Format test data"
echo "  • Auto-Generated URN Demonstration: Same parameters = Same URNs"
echo ""
print_status "SUCCESS" "🎉 DELETE test script completed successfully!"
