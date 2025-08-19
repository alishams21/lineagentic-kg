# Data Ingestion Pipeline for Column-Level Lineage

This directory contains a complete data ingestion pipeline that demonstrates column-level lineage tracking using the RegistryFactory and Neo4j.

## 📁 File Structure

```
refactoring/
├── data/                          # JSON data files
│   ├── raw_customer_data.json     # Raw customer data from source
│   ├── staging_customer_data.json # Staging data with transformations
│   └── final_customer_data.json   # Final business-ready data
├── ingestion.py                   # Main ingestion script
├── registry_factory.py            # RegistryFactory implementation
├── enhanced_registry.yaml         # Metadata registry configuration
└── README_INGESTION.md           # This file
```

## 🎯 Overview

The ingestion pipeline creates a complete column-level lineage example with 3 datasets:

1. **Raw Dataset**: Contains unprocessed customer data from source system
2. **Staging Dataset**: Contains cleaned and transformed data with privacy protections
3. **Final Dataset**: Contains business-ready data with encryption and classification

## 📊 Data Flow

```
Raw Data (CRM) → Staging (ETL) → Final (Data Warehouse)
     ↓              ↓              ↓
6 columns      5 columns      6 columns
(raw format)   (cleaned)      (business-ready)
```

### Column Transformations

**Raw → Staging:**
- `customer_id`: Cleaning & validation
- `full_name`: Concatenation of `first_name + last_name`
- `email_hash`: SHA-256 hashing for privacy
- `phone_clean`: Formatting and cleaning
- `registration_year`: Year extraction from date

**Staging → Final:**
- `customer_id`: Passthrough unchanged
- `customer_name`: Standardization of `full_name`
- `email_encrypted`: AES-256 encryption of hashed email
- `phone_masked`: Masking showing only last 4 digits
- `customer_segment`: Classification based on registration year
- `data_quality_score`: Multi-input scoring from 5 staging columns

## 🚀 Usage

### Prerequisites

1. **Neo4j Database**: Running Neo4j instance
2. **Python Dependencies**: Install required packages
3. **RegistryFactory**: Ensure `registry_factory.py` is available

### Configuration

Update the Neo4j connection settings in `ingestion.py`:

```python
neo4j_uri = "bolt://localhost:7687"
neo4j_user = "neo4j"
neo4j_password = "your_password"
```

### Running the Pipeline

```bash
cd refactoring
python ingestion.py
```

### Expected Output

```
🚀 Starting Data Ingestion Pipeline
============================================================
Initializing RegistryFactory...
Loading data from refactoring/data/raw_customer_data.json...
Loading data from refactoring/data/staging_customer_data.json...
Loading data from refactoring/data/final_customer_data.json...

=== Ingesting Raw Customer Dataset ===
Created raw dataset: urn:li:dataset:(urn:li:dataPlatform:snowflake,raw_customer_data,PROD)
Creating aspects for raw dataset...
Creating columns for raw dataset...
  Created column: customer_id -> urn:li:dataset:(urn:li:dataPlatform:snowflake,raw_customer_data,PROD)#customer_id
  ...

=== Ingesting Staging Customer Dataset ===
Created staging dataset: urn:li:dataset:(urn:li:dataPlatform:snowflake,staging_customer_data,PROD)
Creating aspects for staging dataset...
Creating columns with transformations for staging dataset...
  Created column with transformation: full_name
  ...

=== Ingesting Final Customer Dataset ===
Created final dataset: urn:li:dataset:(urn:li:dataPlatform:snowflake,final_customer_data,PROD)
Creating aspects for final dataset...
Creating columns with transformations for final dataset...
  Created column with transformation: customer_name
  ...

=== Creating Column-Level Lineage Relationships ===
Creating Raw -> Staging column lineage...
Creating Staging -> Final column lineage...

=== Creating Dataset-Level Lineage Relationships ===
Created lineage: Raw -> Staging
Created lineage: Staging -> Final

=== Creating Tags and Relationships ===
Tagged raw dataset with PII and SENSITIVE tags
Tagged staging dataset with PII and SENSITIVE tags
Tagged final dataset with PII and SENSITIVE tags

============================================================
✅ Data Ingestion Pipeline Completed Successfully!
============================================================

📊 Ingestion Summary:
  • Raw Dataset: urn:li:dataset:(urn:li:dataPlatform:snowflake,raw_customer_data,PROD)
  • Staging Dataset: urn:li:dataset:(urn:li:dataPlatform:snowflake,staging_customer_data,PROD)
  • Final Dataset: urn:li:dataset:(urn:li:dataPlatform:snowflake,final_customer_data,PROD)
  • Total Columns Created: 17
  • Column Lineage Relationships: 11
```

## 📋 What Gets Created

### Entities
- **3 Datasets**: Raw, Staging, Final
- **17 Columns**: Across all 3 datasets
- **2 Tags**: PII and SENSITIVE

### Aspects
- **Dataset Properties**: Description, source, environment info
- **Schema Metadata**: Column definitions and types
- **Dataset Profile**: Row count, size, last modified
- **Ownership**: Data ownership assignments
- **Global Tags**: PII and sensitive data tags
- **Column Properties**: Column descriptions and data types
- **Transformations**: Column transformation definitions
- **Column Lineage**: Upstream/downstream column relationships

### Relationships
- **HAS_COLUMN**: Dataset to column relationships
- **DERIVES_FROM**: Column-level lineage with transformation details
- **UPSTREAM_OF**: Dataset-level lineage
- **TAGGED**: Dataset tagging relationships
- **OWNS**: Ownership relationships

## 🔍 Querying the Data

### View Column-Level Lineage

```cypher
// Show complete column-level lineage graph
MATCH (raw:Dataset {urn: "urn:li:dataset:(urn:li:dataPlatform:snowflake,raw_customer_data,PROD)"})
MATCH (staging:Dataset {urn: "urn:li:dataset:(urn:li:dataPlatform:snowflake,staging_customer_data,PROD)"})
MATCH (final:Dataset {urn: "urn:li:dataset:(urn:li:dataPlatform:snowflake,final_customer_data,PROD)"})

OPTIONAL MATCH (raw)-[:HAS_COLUMN]->(raw_col:Column)
OPTIONAL MATCH (staging)-[:HAS_COLUMN]->(staging_col:Column)
OPTIONAL MATCH (final)-[:HAS_COLUMN]->(final_col:Column)

OPTIONAL MATCH (staging_col)-[derives1:DERIVES_FROM]->(raw_col)
OPTIONAL MATCH (final_col)-[derives2:DERIVES_FROM]->(staging_col)

RETURN raw, staging, final, raw_col, staging_col, final_col, derives1, derives2
```

### View Transformation Details

```cypher
// Show transformation aspects for columns
MATCH (col:Column)-[:HAS_ASPECT]->(aspect:Aspect {name: "transformation"})
WHERE aspect.latest = true
RETURN col.urn as Column, aspect.json as Transformation
```

### View Dataset Lineage

```cypher
// Show dataset-level lineage
MATCH (upstream:Dataset)-[r:UPSTREAM_OF]->(downstream:Dataset)
RETURN upstream.urn as Upstream, downstream.urn as Downstream, r.via as Via
```

## 🛠️ Customization

### Adding New Datasets

1. Create a new JSON file in the `data/` directory
2. Follow the same structure as existing files
3. Add the dataset to the ingestion pipeline

### Modifying Transformations

1. Update the `transformations` section in the JSON files
2. Modify the lineage creation logic in `ingestion.py`
3. Update the column lineage relationships

### Adding New Aspects

1. Define the aspect in `enhanced_registry.yaml`
2. Add aspect creation logic in the ingestion pipeline
3. Update the RegistryFactory if needed

## 🔧 Troubleshooting

### Common Issues

1. **Neo4j Connection**: Ensure Neo4j is running and credentials are correct
2. **File Paths**: Verify JSON files exist in the `data/` directory
3. **Registry**: Ensure `enhanced_registry.yaml` is properly configured
4. **Dependencies**: Install required Python packages

### Debug Mode

Enable debug logging by modifying the RegistryFactory initialization:

```python
self.factory = RegistryFactory(registry_path, debug=True)
```

## 📈 Next Steps

1. **Extend the Pipeline**: Add more datasets and transformations
2. **Add Data Quality**: Implement data quality scoring and validation
3. **Real-time Ingestion**: Modify for real-time data streaming
4. **Monitoring**: Add monitoring and alerting capabilities
5. **API Integration**: Create REST API for programmatic access

## 📚 Related Files

- `registry_factory.py`: Core RegistryFactory implementation
- `enhanced_registry.yaml`: Metadata registry configuration
- `examples/`: Additional usage examples
- `cypher/`: Sample Cypher queries for analysis
