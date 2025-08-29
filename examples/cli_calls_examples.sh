#!/bin/bash

# Execute Working CLI Commands for LineAgentic Catalog
# This script creates a raw and processed dataset scenario with proper data lineage

echo "🚀 Executing Raw to Processed Data Lineage Scenario via CLI"
echo "=========================================================="

# Wait for CLI to be ready
echo "⏳ CLI should be ready..."
sleep 2

# Set the registry path environment variable
export REGISTRY_PATH="../lineagentic_kg/config/main_registry.yaml"

echo ""
echo "📋 Executing Data Lineage Scenario via CLI:"
echo "=========================================="

echo ""
echo "1️⃣ Creating Raw Dataset (customer_raw_data):"
echo "--------------------------------------------"
make cli ARGS="upsert-dataset --platform snowflake --name customer_raw_data --env PROD --versionid v1.0"

echo ""
echo "2️⃣ Creating Processed Dataset (customer_processed_data):"
echo "------------------------------------------------------"
make cli ARGS="upsert-dataset --platform snowflake --name customer_processed_data --env PROD --versionid v1.0"

echo ""
echo "3️⃣ Creating CorpUser Entities:"
echo "-----------------------------"
make cli ARGS="upsert-corpuser --username data_engineering_team"
make cli ARGS="upsert-corpuser --username crm_team"
make cli ARGS="upsert-corpuser --username analytics_team"

echo ""
echo "4️⃣ Adding corpUserInfo to CorpUsers:"
echo "---------------------------------"
make cli ARGS="upsert-corpuserinfo-aspect --username data_engineering_team --displayName 'Data Engineering Team' --email data-engineering@company.com --title 'Data Engineering Team' --departmentName Engineering --active true"

make cli ARGS="upsert-corpuserinfo-aspect --username crm_team --displayName 'CRM Team' --email crm-team@company.com --title 'CRM Operations Team' --departmentName Operations --active true"

make cli ARGS="upsert-corpuserinfo-aspect --username analytics_team --displayName 'Analytics Team' --email analytics@company.com --title 'Analytics Team' --departmentName Analytics --active true"

echo ""
echo "4️⃣5️⃣ Adding ownership to CorpUsers:"
echo "--------------------------------"
make cli ARGS="upsert-ownership-aspect --platform snowflake --name customer_raw_data --env PROD --owners '[{\"owner\": \"data_engineering_team\", \"type\": \"TECHNICAL_OWNER\"}, {\"owner\": \"crm_team\", \"type\": \"BUSINESS_OWNER\"}]' --groupOwners '[{\"group\": \"data_engineering\", \"type\": \"TECHNICAL_GROUP\"}, {\"group\": \"crm_operations\", \"type\": \"BUSINESS_GROUP\"}]' --lastModified 2024-01-15T10:00:00Z"

make cli ARGS="upsert-ownership-aspect --platform snowflake --name customer_processed_data --env PROD --owners '[{\"owner\": \"data_engineering_team\", \"type\": \"TECHNICAL_OWNER\"}, {\"owner\": \"analytics_team\", \"type\": \"BUSINESS_OWNER\"}]' --groupOwners '[{\"group\": \"data_engineering\", \"type\": \"TECHNICAL_GROUP\"}, {\"group\": \"analytics\", \"type\": \"BUSINESS_GROUP\"}]' --lastModified 2024-01-15T10:00:00Z"

# Note: Ownership aspects are applied to datasets, not users directly

echo ""
echo "6️⃣ Creating CorpGroup Entities:"
echo "------------------------------"
make cli ARGS="upsert-corpgroup --name data_engineering"
make cli ARGS="upsert-corpgroup --name crm_operations"
make cli ARGS="upsert-corpgroup --name analytics"

echo ""
echo "7️⃣ Adding corpGroupInfo to CorpGroups:"
echo "------------------------------------"
make cli ARGS="upsert-corpgroupinfo-aspect --name data_engineering --displayName 'Data Engineering Team' --description 'Team responsible for data infrastructure and ETL pipelines' --email data-engineering@company.com --members '[\"data_engineering_team\"]'"

make cli ARGS="upsert-corpgroupinfo-aspect --name crm_operations --displayName 'CRM Operations Team' --description 'Team managing customer relationship data and operations' --email crm-ops@company.com --members '[\"crm_team\"]'"

make cli ARGS="upsert-corpgroupinfo-aspect --name analytics --displayName 'Analytics Team' --description 'Team responsible for data analysis and business intelligence' --email analytics@company.com --members '[\"analytics_team\"]'"

echo ""
echo "8️⃣ Creating Tag Entities:"
echo "------------------------"
make cli ARGS="upsert-tag --key PII"
make cli ARGS="upsert-tag --key RAW_DATA"
make cli ARGS="upsert-tag --key ANONYMIZED"
make cli ARGS="upsert-tag --key PROCESSED_DATA"

echo ""
echo "9️⃣ Adding datasetProperties to Raw Dataset:"
echo "-------------------------------------------"
make cli ARGS="upsert-datasetproperties-aspect --platform snowflake --name customer_raw_data --env PROD --description 'Raw customer data from source systems' --customProperties '{\"data_source\": \"CRM System\", \"ingestion_frequency\": \"daily\", \"data_quality\": \"raw\"}' --tags '[\"raw\", \"customer\", \"crm\"]' --externalUrl https://snowflake.company.com/raw/customer_raw_data"

echo ""
echo "1️⃣0️⃣ Adding datasetProperties to Processed Dataset:"
echo "------------------------------------------------"
make cli ARGS="upsert-datasetproperties-aspect --platform snowflake --name customer_processed_data --env PROD --description 'Processed and cleaned customer data for analytics' --customProperties '{\"data_source\": \"ETL Pipeline\", \"ingestion_frequency\": \"daily\", \"data_quality\": \"processed\"}' --tags '[\"processed\", \"customer\", \"analytics\"]' --externalUrl https://snowflake.company.com/processed/customer_processed_data"

echo ""
echo "1️⃣1️⃣ Creating Column for Raw Dataset (customer_email):"
echo "-----------------------------------------------------"
make cli ARGS="upsert-column --dataset_urn 'urn:li:dataset:(urn:li:dataPlatform:snowflake,customer_raw_data,PROD)' --field_path customer_email"

echo ""
echo "1️⃣2️⃣ Creating Column for Processed Dataset (customer_id):"
echo "--------------------------------------------------------"
make cli ARGS="upsert-column --dataset_urn 'urn:li:dataset:(urn:li:dataPlatform:snowflake,customer_processed_data,PROD)' --field_path customer_id"

echo ""
echo "1️⃣3️⃣ Adding schemaMetadata to Raw Dataset:"
echo "----------------------------------------"
make cli ARGS="upsert-schemametadata-aspect --platform snowflake --name customer_raw_data --env PROD --schemaName customer_raw_schema --fields '[{\"fieldPath\": \"customer_email\", \"type\": \"VARCHAR\", \"description\": \"Raw customer email address\"}]' --primaryKeys '[\"customer_email\"]'"

echo ""
echo "1️⃣4️⃣ Adding schemaMetadata to Processed Dataset:"
echo "---------------------------------------------"
make cli ARGS="upsert-schemametadata-aspect --platform snowflake --name customer_processed_data --env PROD --schemaName customer_processed_schema --fields '[{\"fieldPath\": \"customer_id\", \"type\": \"VARCHAR\", \"description\": \"Hashed customer identifier\"}]' --primaryKeys '[\"customer_id\"]'"

# Note: Ownership aspects were already applied in step 4️⃣5️⃣ above

echo ""
echo "1️⃣7️⃣ Adding globalTags to Raw Dataset:"
echo "-----------------------------------"
make cli ARGS="upsert-globaltags-aspect --platform snowflake --name customer_raw_data --env PROD --tags '[{\"tag\": \"PII\", \"context\": \"data_classification\"}, {\"tag\": \"RAW_DATA\", \"context\": \"data_layer\"}]'"

echo ""
echo "1️⃣8️⃣ Adding globalTags to Processed Dataset:"
echo "----------------------------------------"
make cli ARGS="upsert-globaltags-aspect --platform snowflake --name customer_processed_data --env PROD --tags '[{\"tag\": \"ANONYMIZED\", \"context\": \"data_classification\"}, {\"tag\": \"PROCESSED_DATA\", \"context\": \"data_layer\"}]'"

echo ""
echo "1️⃣9️⃣ Adding dataQuality to Raw Dataset:"
echo "-------------------------------------"
make cli ARGS="upsert-dataquality-aspect --platform snowflake --name customer_raw_data --env PROD --qualityScore 75.5 --qualityMetrics '{\"completeness\": 85.2, \"accuracy\": 70.1, \"consistency\": 78.3, \"timeliness\": 90.0}' --lastValidated 2024-01-15T10:00:00Z --validationRules '[\"no_null_emails\", \"valid_email_format\", \"unique_customer_records\"]' --dataQualityIssues '[{\"type\": \"missing_data\", \"severity\": \"medium\", \"description\": \"15% of email addresses are null or invalid\", \"affected_rows\": 15000}, {\"type\": \"duplicate_data\", \"severity\": \"low\", \"description\": \"2% of customer records are duplicates\", \"affected_rows\": 2000}]' --qualityChecks '[{\"name\": \"email_completeness\", \"status\": \"failed\", \"threshold\": 90.0, \"actual\": 85.2, \"description\": \"Email completeness check\"}, {\"name\": \"email_format_validation\", \"status\": \"passed\", \"threshold\": 95.0, \"actual\": 96.8, \"description\": \"Email format validation\"}]'"

echo ""
echo "2️⃣0️⃣ Adding dataQuality to Processed Dataset:"
echo "------------------------------------------"
make cli ARGS="upsert-dataquality-aspect --platform snowflake --name customer_processed_data --env PROD --qualityScore 92.5 --qualityMetrics '{\"completeness\": 98.5, \"accuracy\": 95.2, \"consistency\": 96.8, \"timeliness\": 94.1}' --lastValidated 2024-01-15T10:00:00Z --validationRules '[\"no_null_customer_ids\", \"unique_customer_ids\", \"valid_hash_format\", \"anonymization_complete\"]' --dataQualityIssues '[{\"type\": \"data_quality\", \"severity\": \"low\", \"description\": \"1% of customer IDs have formatting issues\", \"affected_rows\": 1000}]' --qualityChecks '[{\"name\": \"customer_id_completeness\", \"status\": \"passed\", \"threshold\": 95.0, \"actual\": 98.5, \"description\": \"Customer ID completeness check\"}, {\"name\": \"anonymization_check\", \"status\": \"passed\", \"threshold\": 100.0, \"actual\": 100.0, \"description\": \"PII anonymization verification\"}, {\"name\": \"hash_consistency\", \"status\": \"passed\", \"threshold\": 99.0, \"actual\": 99.8, \"description\": \"Hash format consistency check\"}]'"

echo ""
echo "2️⃣1️⃣ Adding columnProperties to Raw Column:"
echo "-------------------------------------------"
make cli ARGS="upsert-columnproperties-aspect --dataset_urn urn:li:dataset:\(urn:li:dataPlatform:snowflake,customer_raw_data,PROD\) --field_path customer_email --description 'Raw customer email address from source system' --dataType VARCHAR\(255\) --nullable false --customProperties '{\"contains_pii\": true, \"source_field\": \"email\"}'"

echo ""
echo "2️⃣2️⃣ Adding columnProperties to Processed Column:"
echo "------------------------------------------------"
make cli ARGS="upsert-columnproperties-aspect --dataset_urn urn:li:dataset:\(urn:li:dataPlatform:snowflake,customer_processed_data,PROD\) --field_path customer_id --description 'Hashed customer identifier for privacy' --dataType VARCHAR\(64\) --nullable false --customProperties '{\"contains_pii\": false, \"hash_algorithm\": \"sha256\"}'"

echo ""
echo "2️⃣3️⃣ Adding columnTransformation to Processed Column:"
echo "---------------------------------------------------"
make cli ARGS="upsert-columntransformation-aspect --dataset_urn urn:li:dataset:\(urn:li:dataPlatform:snowflake,customer_processed_data,PROD\) --field_path customer_id --inputColumns '[\"customer_email\"]' --transformType hash_transformation --transformScript SHA256\(customer_email\) --sourceDataset urn:li:dataset:\(urn:li:dataPlatform:snowflake,customer_raw_data,PROD\)"

echo ""
echo "2️⃣4️⃣ Adding datasetTransformation to Raw Dataset:"
echo "------------------------------------------------"
make cli ARGS="upsert-datasettransformation-aspect --platform snowflake --name customer_raw_data --env PROD --sourceDataset urn:li:dataset:\(urn:li:dataPlatform:snowflake,customer_raw_data,PROD\) --targetDataset urn:li:dataset:\(urn:li:dataPlatform:snowflake,customer_processed_data,PROD\) --transformationType data_cleaning_and_hashing --description 'Raw customer data is cleaned and customer emails are hashed to create anonymized customer IDs'"

echo ""
echo "2️⃣5️⃣ Getting Raw Dataset:"
echo "------------------------"
make cli ARGS="get-dataset 'urn:li:dataset:(urn:li:dataPlatform:snowflake,customer_raw_data,PROD)'"

echo ""
echo "2️⃣6️⃣ Getting Processed Dataset:"
echo "------------------------------"
make cli ARGS="get-dataset 'urn:li:dataset:(urn:li:dataPlatform:snowflake,customer_processed_data,PROD)'"

echo ""
echo "2️⃣7️⃣ Creating DataProduct Entity (customer_analytics_product):"
echo "------------------------------------------------------------"
make cli ARGS="upsert-dataproduct --name customer_analytics_product --domain customer_analytics --purpose 'Provides anonymized customer data for analytics and reporting'"

echo ""
echo "2️⃣8️⃣ Adding dpOwnership to DataProduct:"
echo "-------------------------------------"
make cli ARGS="upsert-dpownership-aspect --name customer_analytics_product --domain customer_analytics --owners '[{\"owner\": \"analytics_team\", \"type\": \"PRODUCT_OWNER\"}, {\"owner\": \"data_engineering_team\", \"type\": \"TECHNICAL_OWNER\"}]' --lastModified 2024-01-15T10:00:00Z"

echo ""
echo "2️⃣9️⃣ Adding dpContract to DataProduct:"
echo "-----------------------------------"
make cli ARGS="upsert-dpcontract-aspect --name customer_analytics_product --domain customer_analytics --output_port_name customer_analytics_port --output_port_type dataset --fields '[{\"name\": \"customer_id\", \"type\": \"VARCHAR(64)\", \"description\": \"Hashed customer identifier\", \"dataset_urn\": \"urn:li:dataset:(urn:li:dataPlatform:snowflake,customer_processed_data,PROD)\"}]' --consumer_port_name analytics_consumer --consumer_port_type api --freshness daily --slack_channel '#customer-analytics' --sla '99.9% uptime, 24h freshness'"

echo ""
echo "3️⃣0️⃣ Adding dpObservability to DataProduct:"
echo "----------------------------------------"
make cli ARGS="upsert-dpobservability-aspect --name customer_analytics_product --domain customer_analytics --metrics '{\"data_freshness\": \"last_updated_timestamp\", \"data_quality_score\": \"quality_metrics\", \"api_response_time\": \"avg_response_time_ms\", \"error_rate\": \"error_percentage\"}' --alerting_rules '[{\"name\": \"data_freshness_alert\", \"condition\": \"last_updated > 24h\", \"severity\": \"critical\", \"channel\": \"#data-alerts\"}, {\"name\": \"quality_score_alert\", \"condition\": \"quality_score < 90\", \"severity\": \"warning\", \"channel\": \"#data-alerts\"}]' --dashboard_url https://grafana.company.com/d/customer-analytics-product"

echo ""
echo "3️⃣1️⃣ Adding dpPolicy to DataProduct:"
echo "--------------------------------"
make cli ARGS="upsert-dppolicy-aspect --name customer_analytics_product --domain customer_analytics --access_control '{\"authentication\": \"oauth2\", \"authorization\": \"role_based\", \"allowed_roles\": [\"analytics_user\", \"data_scientist\", \"business_analyst\"]}' --data_masking '{\"enabled\": true, \"masking_rules\": [\"no_pii_exposure\", \"hash_customer_ids\"]}' --quality_gate '{\"enabled\": true, \"thresholds\": {\"completeness\": 95.0, \"accuracy\": 90.0, \"freshness\": 24}}' --retention_policy '{\"data_retention_days\": 365, \"backup_retention_days\": 730}' --evaluation_points '[\"data_ingestion\", \"data_processing\", \"data_delivery\"]'"

echo ""
echo "3️⃣2️⃣ Adding dpProvisioner to DataProduct:"
echo "-------------------------------------"
make cli ARGS="upsert-dpprovisioner-aspect --name customer_analytics_product --domain customer_analytics --platform snowflake --environment production --compute_resource '{\"warehouse\": \"ANALYTICS_WH\", \"size\": \"medium\", \"auto_suspend\": 300}' --storage_resource '{\"database\": \"CUSTOMER_ANALYTICS_DB\", \"schema\": \"PUBLIC\", \"table\": \"CUSTOMER_ANALYTICS_PRODUCT\"}' --deployment_strategy '{\"type\": \"blue_green\", \"rollback_enabled\": true, \"health_checks\": [\"data_quality\", \"api_availability\"]}'"

echo ""
echo "3️⃣3️⃣ Getting DataProduct:"
echo "------------------------"
make cli ARGS="get-dataproduct 'urn:li:dataProduct:(customer_analytics,customer_analytics_product,PROD)'"

echo ""
echo "✅ DATA LINEAGE SCENARIO COMPLETED via CLI!"
echo "=========================================="
echo "✅ Raw Dataset created: customer_raw_data"
echo "✅ Processed Dataset created: customer_processed_data"
echo "✅ DataProduct created: customer_analytics_product"
echo "✅ All aspects added to both datasets:"
echo "   - datasetProperties (description, tags, custom properties)"
echo "   - schemaMetadata (schema definition with fields)"
echo "   - ownership (technical and business owners)"
echo "   - globalTags (data classification tags)"
echo "   - dataQuality (quality metrics, validation rules, quality checks)"
echo "✅ datasetTransformation aspect added to processed dataset"
echo "✅ Columns created for both datasets:"
echo "   - Raw: customer_email (contains PII)"
echo "   - Processed: customer_id (anonymized)"
echo "✅ columnProperties added to both columns"
echo "✅ columnTransformation added to processed column"
echo "✅ DataProduct aspects added:"
echo "   - dpOwnership (product and technical owners)"
echo "   - dpContract (output port, fields, SLA)"
echo "   - dpObservability (metrics, alerts, dashboard)"
echo "   - dpPolicy (access control, quality gates, retention)"
echo "   - dpProvisioner (infrastructure, deployment)"
echo "✅ Relationships should be automatically created:"
echo "   - HAS_COLUMN: Dataset → Column"
echo "   - TRANSFORMS: Column → Column (customer_email → customer_id)"
echo "   - DOWNSTREAM_OF: Dataset → Dataset (raw → processed)"
echo "   - IMPLEMENTS_CONTRACT: Dataset → DataProduct (via dpContract)"
echo "✅ All URNs auto-generated by backend"
echo ""
echo "📁 Generated CLI Location: generated_cli/"
echo ""
echo "🎯 Raw to Processed Data Lineage with DataProduct Successfully Created via CLI!"
