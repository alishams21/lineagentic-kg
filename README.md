<div align="center">
  <img src="https://raw.githubusercontent.com/lineagentic/lineagentic-catalog/main/images/lineagenticcatalog.jpg" alt="Lineagentic Logo" width="880" height="300">
</div>

# Lineagentic-Catalog

Lineagentic-Catalog is more than just a data catalog—it's a graph-native metadata platform that turns simple YAML definitions into a fully operational, customizable, and governed data model. With extendability built in, automatic generation of REST APIs, automatic generation of CLI tooling it delivers a "batteries included" experience for modern data teams.

## Features
- **Generic Metadata Model Generator**: Highly customizable metadata model that can be extended to support new entities, aspects, and relationships. 
- **REST APIs Generator**: Generate FastAPI endpoints from registry system.
- **CLI Tooling Generator**: Generate CLI commands from registry system.
- **Type Safety**: Generated code ensures proper data handling


## Quick Start

## You can use Lineagentic-Catalog as a library

Lineagentic-Catalog is data catalogue as code factory. Build a Yaml file in "lineagentic_catalog/config" folder for your desired architecture and taxonomy and Lineagentic-Catalog will generate all the code for you. You will get a python library with all the methods required for your data catalouge which is graph native

```
1- pip install lineagentic-catalog
2. Create a YAML file  /lineagentic_catalog/config/ # for your graph data model.
3. uv sync . # to install dependencies.
4. Run the example:
```
python test/tt.py
```
```python
from lineagentic_catalog.registry.factory import RegistryFactory
from lineagentic_catalog.utils import get_logger, setup_logging, log_function_call, log_function_result

# Setup logging with custom configuration
setup_logging(
    default_level="INFO",
    log_file="logs/lineagentic_catalog.log"
)

# Get logger for this application
logger = get_logger("lineagentic.example")

logger.info("Starting LineAgentic Catalog example", config_path="lineagentic_catalog/config/main_registry.yaml")

try:
    # 1. Initialize the registry factory with your config file
    log_function_call(logger, "RegistryFactory initialization", config_path="lineagentic_catalog/config/main_registry.yaml")
    registry_factory = RegistryFactory("lineagentic_catalog/config/main_registry.yaml")
    log_function_result(logger, "RegistryFactory initialization", 
                       factory_created=True, registry_path="lineagentic_catalog/config/main_registry.yaml")
    
    # 2. Create a Neo4j writer instance
    logger.info("Creating Neo4j writer instance", uri="bolt://localhost:7687")
    neo4j_writer = registry_factory.create_writer(
        uri="bolt://localhost:7687",
        user="neo4j",
        password="password"
    )
    logger.info("Neo4j writer instance created successfully")
    
    # 3. Create entities using the dynamically generated methods
    # The methods are generated based on your YAML configuration
    
    # Create a dataset
    logger.info("Creating dataset", platform="snowflake", name="customer_data", env="PROD")
    dataset_urn = neo4j_writer.upsert_dataset(
        platform="snowflake",
        name="customer_data",
        env="PROD"
    )
    logger.info("Dataset created successfully", dataset_urn=dataset_urn)
    
    # Create a data flow
    logger.info("Creating data flow", 
                platform="airflow", 
                flow_id="customer_etl", 
                namespace="data_engineering")
    flow_urn = neo4j_writer.upsert_dataflowinfo_aspect(
        payload={
            "name": "Customer ETL Pipeline",
            "namespace": "data_engineering",
            "description": "Customer data ETL pipeline"
        },
        platform="airflow",
        flow_id="customer_etl",
        env="PROD"
    )
    logger.info("Data flow created successfully", flow_urn=flow_urn)
    
    # Create a data job
    logger.info("Creating data job", flow_urn=flow_urn, job_name="transform_customer_data")
    job_urn = neo4j_writer.upsert_datajobinfo_aspect(
        payload={
            "name": "Transform Customer Data",
            "namespace": "data_engineering",
            "description": "Transform customer data job"
        },
        flow_urn=flow_urn,
        job_name="transform_customer_data"
    )
    logger.info("Data job created successfully", job_urn=job_urn)
    
    # 4. Retrieve entities
    logger.info("Retrieving dataset", dataset_urn=dataset_urn)
    dataset = neo4j_writer.get_dataset(dataset_urn)
    logger.info("Dataset retrieved successfully", dataset_urn=dataset_urn, dataset_size=len(str(dataset)))
    print(f"Retrieved dataset: {dataset}")
    
    # 5. Clean up
    logger.info("Closing Neo4j writer connection")
    neo4j_writer.close()
    logger.info("Neo4j writer connection closed successfully")
    
    logger.info("LineAgentic Catalog example completed successfully", 
                entities_created=3, 
                dataset_urn=dataset_urn,
                flow_urn=flow_urn,
                job_urn=job_urn)

except Exception as e:
    logger.error("Error in LineAgentic Catalog example", 
                error_type=type(e).__name__,
                error_message=str(e))
    raise
```

The library automatically generates methods like `upsert_dataset()`, `upsert_dataflow_aspect()`, `upsert_datajob()`, etc based on your YAML configuration files. Each entity type defined in your `entities.yaml` and `aspects.yaml` gets its own set of CRUD operations.

## Out of the box Restful apis:

You can also use auto-generated restful apis out of the box to save huge time. Just run following commands and you will have a FastAPI server running on your local machine with all endpoints for your graph data model.

```
1. Create a YAML file  /lineagentic_catalog/config/ # for your graph data model.
2. uv sync . # to install dependencies.
3- uv run generate-api # to generate FastAPI endpoints.
```

After generating the API, you can use curl commands to interact with your metadata:

```bash
# 1. Create a dataset
curl -X POST "http://localhost:8000/api/v1/entities/Dataset" \
  -H "Content-Type: application/json" \
  -d '{
    "platform": "snowflake",
    "name": "customer_data",
    "env": "PROD"
  }'

# 2. Get a dataset by URN
curl -X GET "http://localhost:8000/api/v1/entities/Dataset/urn:li:dataset:(urn:li:dataPlatform:snowflake,customer_data,PROD)"
```

## Out of the box CLI tooling:

In Lineagentic-Catalog, you can also auto-generate CLI tooling. Just run following commands and you will have a CLI tooling running on your local machine with all commands for your graph data model.

```
1. Create a YAML file  /lineagentic_catalog/config/ # for your graph data model.
2. uv sync . # to install dependencies.
3. uv run generate-cli # to generate CLI commands.
```

### Example: Using the Generated CLI

After generating the CLI, you can use command-line tools to manage your metadata:

```bash
# 1. Create a dataset
registry-cli upsert-dataset --platform "snowflake" --name "customer_data" --env "PROD"

# 2. Get dataset information
registry-cli get-dataset "urn:li:dataset:(urn:li:dataPlatform:snowflake,customer_data,PROD)" --output table

# 3. Add ownership aspect to the dataset
registry-cli upsert-ownership-aspect --entity-label "Dataset" --entity-urn "urn:li:dataset:(urn:li:dataPlatform:snowflake,customer_data,PROD)" --owners '[{"owner": "urn:li:corpuser:john.doe", "type": "DATAOWNER"}]'

# 4. Health check
registry-cli health
```


## How Lineagentic-Catalog Works - Detailed Flow Diagrams

Lineagentic-Catalog is a **dynamic code generation system** that creates graph database writers from YAML configuration files. 

1- Registry module:Core part of the Lineagentic-Catalog is Registry module which is developed based on registry design pattern. Think of it as a code factory that reads configuration which is in this case is Yaml file for your graph data model and builds Python classes automatically. 

2- API-Generator: This module is responsible for generating RESTful APIs for your graph data model. It is developed based on FastAPI framework and leverages methods generated by Registry module.

3- CLI-Generator: This module is responsible for generating CLI commands for your graph data model. It is developed based on Click framework and leverages methods generated by Registry module to build CLI commands.

### Why This Architecture is Powerful

This system essentially turns YAML configuration into working Python code at runtime! It provides:

1. **Flexibility**: Change data models without code changes
2. **Consistency**: All entities follow the same patterns
3. **Maintainability**: Business logic is separated from implementation
4. **Extensibility**: Easy to add new entity types and relationships
5. **Type Safety**: Generated code ensures proper data handling

The registry system transforms declarative configuration into executable code, making it easy to adapt to changing business requirements while maintaining code quality and consistency.




## Detailed Flow Diagrams

### 1. Bootstrap Phase: RegistryFactory Initialization
This diagram shows the complete initialization flow from YAML configuration to generated class.

<p align="center">
  <img src="images/01_bootstrap_phase.png" alt="Bootstrap Phase Diagram" width="800">
</p>
*Shows how RegistryFactory loads config, validates it, generates functions, and creates the final writer class.*

### 2. Runtime Phase: Using Generated Methods
This diagram shows what happens when you use the generated methods.

<p align="center">
  <img src="images/02_runtime_phase.png" alt="Runtime Phase Diagram" width="800">
</p>
*Flow when calling `upsert_dataset()` and `add_aspect()`.*

### 3. Configuration Loading Flow
<p align="center">
  <img src="images/03_config_loading.png" alt="Configuration Loading Diagram" width="800">
</p>

### 4. Method Generation Flow
<p align="center">
  <img src="images/04_method_generation.png" alt="Method Generation Diagram" width="800">
</p>

### 5. Overall System Architecture
<p align="center">
  <img src="images/05_system_architecture.png" alt="System Architecture Diagram" width="800">
</p>

## 6. Data Flow Overview
<p align="center">
  <img src="images/06_data_flow.png" alt="Data Flow Diagram" width="800">
</p>

## Step-by-Step Process

1. Configuration Files (`lineagentic_catalog/config/` folder)

The system starts with YAML configuration files that define the data model.

- **`main_registry.yaml`**: The main entry point that includes all other config files
- **`entities.yaml`**: Defines what types of data objects exist (Dataset, DataFlow, CorpUser, etc.) 
- **`urn_patterns.yaml`**: Defines how to create unique identifiers (URNs) for each entity
- **`aspects.yaml`**: Defines properties and metadata for entities (e.g. datasetProperties, dataflowProperties, etc.)
- **`relationships.yaml`**: Defines how entities connect to each other (e.g. dataset -> dataflow)
- **`utilities.yaml`**: Defines helper functions for data processing (e.g. data cleaning, data transformation, etc.)

2. Registry Loading (`lineagentic_catalog/registry/loaders.py`)

- Reads the main registry file
- Merges all included YAML files into one big configuration
- Handles file dependencies and deep merging

3. Validation (`lineagentic_catalog/registry/validators.py`)

- Checks that all required sections exist
- Validates configuration structure
- Ensures everything is properly configured

4. Code Generation (`lineagentic_catalog/registry/generators.py`)

- **URNGenerator**: Creates functions that generate unique identifiers
- **AspectProcessor**: Creates functions that process entity metadata
- **UtilityFunctionBuilder**: Creates helper functions for data cleaning/processing

5. Class Generation (`lineagentic_catalog/registry/writers.py`)

- Takes all the generated functions and configuration
- Dynamically creates a Python class called `Neo4jMetadataWriter`
- This class has methods like:
  - `upsert_dataset()`, `get_dataset()`, `delete_dataset()`
  - `upsert_dataflow()`, `get_dataflow()`, `delete_dataflow()`
  - And so on for each entity type

6. Factory (`lineagentic_catalog/registry/factory.py`)

- Orchestrates the entire process
- Creates the final writer class
- Provides a simple interface to use the generated code

## Example in fine grained way: How a Dataset Gets Created

1. **Config says**: "Dataset entities need platform, name, env, versionId properties"
2. **URN Pattern says**: "Dataset URNs should look like: `urn:li:dataset:(platform,name,env)`"
3. **Generator creates**: A function that builds URNs from the input data
4. **Writer gets**: A method `upsert_dataset(platform="mysql", name="users", env="PROD")`
5. **Result**: Creates a dataset node in Neo4j with the URN `urn:li:dataset:(mysql,users,PROD)`

## Key Benefits

- **No hardcoded entity types**: Add new entities by just editing YAML
- **Flexible URN patterns**: Change how IDs are generated without touching code
- **Dynamic methods**: New entity types automatically get create/read/delete methods
- **Configuration-driven**: Business logic is in config files, not code
- **Maintainable**: Changes to data model only require config updates
