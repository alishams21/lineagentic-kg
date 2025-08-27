# RegistryFactory Generated API

This is an auto-generated FastAPI application based on the RegistryFactory methods.

## Generated Endpoints

### Entities

#### Column
- `GET /api/v1/entities/Column/{urn}` - Get Column by URN
- `POST /api/v1/entities/Column` - Upsert Column
- `DELETE /api/v1/entities/Column/{urn}` - Delete Column by URN

#### Corpgroup
- `GET /api/v1/entities/CorpGroup/{urn}` - Get CorpGroup by URN
- `POST /api/v1/entities/CorpGroup` - Upsert CorpGroup
- `DELETE /api/v1/entities/CorpGroup/{urn}` - Delete CorpGroup by URN

#### Corpuser
- `GET /api/v1/entities/CorpUser/{urn}` - Get CorpUser by URN
- `POST /api/v1/entities/CorpUser` - Upsert CorpUser
- `DELETE /api/v1/entities/CorpUser/{urn}` - Delete CorpUser by URN

#### Dataflow
- `GET /api/v1/entities/DataFlow/{urn}` - Get DataFlow by URN
- `POST /api/v1/entities/DataFlow` - Upsert DataFlow
- `DELETE /api/v1/entities/DataFlow/{urn}` - Delete DataFlow by URN

#### Datajob
- `GET /api/v1/entities/DataJob/{urn}` - Get DataJob by URN
- `POST /api/v1/entities/DataJob` - Upsert DataJob
- `DELETE /api/v1/entities/DataJob/{urn}` - Delete DataJob by URN

#### Dataproduct
- `GET /api/v1/entities/DataProduct/{urn}` - Get DataProduct by URN
- `POST /api/v1/entities/DataProduct` - Upsert DataProduct
- `DELETE /api/v1/entities/DataProduct/{urn}` - Delete DataProduct by URN

#### Dataset
- `GET /api/v1/entities/Dataset/{urn}` - Get Dataset by URN
- `POST /api/v1/entities/Dataset` - Upsert Dataset
- `DELETE /api/v1/entities/Dataset/{urn}` - Delete Dataset by URN

#### Tag
- `GET /api/v1/entities/Tag/{urn}` - Get Tag by URN
- `POST /api/v1/entities/Tag` - Upsert Tag
- `DELETE /api/v1/entities/Tag/{urn}` - Delete Tag by URN

### Aspects

#### Columnproperties
- `GET /api/v1/aspects/columnProperties/{entity_label}/{entity_urn}` - Get columnProperties aspect
- `POST /api/v1/aspects/columnProperties` - Upsert columnProperties aspect
- `DELETE /api/v1/aspects/columnProperties/{entity_label}/{entity_urn}` - Delete columnProperties aspect

#### Corpgroupinfo
- `GET /api/v1/aspects/corpGroupInfo/{entity_label}/{entity_urn}` - Get corpGroupInfo aspect
- `POST /api/v1/aspects/corpGroupInfo` - Upsert corpGroupInfo aspect
- `DELETE /api/v1/aspects/corpGroupInfo/{entity_label}/{entity_urn}` - Delete corpGroupInfo aspect

#### Corpuserinfo
- `GET /api/v1/aspects/corpUserInfo/{entity_label}/{entity_urn}` - Get corpUserInfo aspect
- `POST /api/v1/aspects/corpUserInfo` - Upsert corpUserInfo aspect
- `DELETE /api/v1/aspects/corpUserInfo/{entity_label}/{entity_urn}` - Delete corpUserInfo aspect

#### Dataflowinfo
- `GET /api/v1/aspects/dataFlowInfo/{entity_label}/{entity_urn}` - Get dataFlowInfo aspect
- `POST /api/v1/aspects/dataFlowInfo` - Upsert dataFlowInfo aspect
- `DELETE /api/v1/aspects/dataFlowInfo/{entity_label}/{entity_urn}` - Delete dataFlowInfo aspect

#### Datajobinfo
- `GET /api/v1/aspects/dataJobInfo/{entity_label}/{entity_urn}` - Get dataJobInfo aspect
- `POST /api/v1/aspects/dataJobInfo` - Upsert dataJobInfo aspect
- `DELETE /api/v1/aspects/dataJobInfo/{entity_label}/{entity_urn}` - Delete dataJobInfo aspect

#### Datajobinputoutput
- `GET /api/v1/aspects/dataJobInputOutput/{entity_label}/{entity_urn}` - Get dataJobInputOutput aspect
- `POST /api/v1/aspects/dataJobInputOutput` - Upsert dataJobInputOutput aspect
- `DELETE /api/v1/aspects/dataJobInputOutput/{entity_label}/{entity_urn}` - Delete dataJobInputOutput aspect

#### Datajobrun
- `GET /api/v1/aspects/dataJobRun/{entity_label}/{entity_urn}` - Get dataJobRun aspect
- `POST /api/v1/aspects/dataJobRun` - Upsert dataJobRun aspect
- `DELETE /api/v1/aspects/dataJobRun/{entity_label}/{entity_urn}` - Delete dataJobRun aspect

#### Datasetprofile
- `GET /api/v1/aspects/datasetProfile/{entity_label}/{entity_urn}` - Get datasetProfile aspect
- `POST /api/v1/aspects/datasetProfile` - Upsert datasetProfile aspect
- `DELETE /api/v1/aspects/datasetProfile/{entity_label}/{entity_urn}` - Delete datasetProfile aspect

#### Datasetproperties
- `GET /api/v1/aspects/datasetProperties/{entity_label}/{entity_urn}` - Get datasetProperties aspect
- `POST /api/v1/aspects/datasetProperties` - Upsert datasetProperties aspect
- `DELETE /api/v1/aspects/datasetProperties/{entity_label}/{entity_urn}` - Delete datasetProperties aspect

#### Documentation
- `GET /api/v1/aspects/documentation/{entity_label}/{entity_urn}` - Get documentation aspect
- `POST /api/v1/aspects/documentation` - Upsert documentation aspect
- `DELETE /api/v1/aspects/documentation/{entity_label}/{entity_urn}` - Delete documentation aspect

#### Dpcontract
- `GET /api/v1/aspects/dpContract/{entity_label}/{entity_urn}` - Get dpContract aspect
- `POST /api/v1/aspects/dpContract` - Upsert dpContract aspect
- `DELETE /api/v1/aspects/dpContract/{entity_label}/{entity_urn}` - Delete dpContract aspect

#### Dpobservability
- `GET /api/v1/aspects/dpObservability/{entity_label}/{entity_urn}` - Get dpObservability aspect
- `POST /api/v1/aspects/dpObservability` - Upsert dpObservability aspect
- `DELETE /api/v1/aspects/dpObservability/{entity_label}/{entity_urn}` - Delete dpObservability aspect

#### Dppolicy
- `GET /api/v1/aspects/dpPolicy/{entity_label}/{entity_urn}` - Get dpPolicy aspect
- `POST /api/v1/aspects/dpPolicy` - Upsert dpPolicy aspect
- `DELETE /api/v1/aspects/dpPolicy/{entity_label}/{entity_urn}` - Delete dpPolicy aspect

#### Dpprovisioner
- `GET /api/v1/aspects/dpProvisioner/{entity_label}/{entity_urn}` - Get dpProvisioner aspect
- `POST /api/v1/aspects/dpProvisioner` - Upsert dpProvisioner aspect
- `DELETE /api/v1/aspects/dpProvisioner/{entity_label}/{entity_urn}` - Delete dpProvisioner aspect

#### Environmentproperties
- `GET /api/v1/aspects/environmentProperties/{entity_label}/{entity_urn}` - Get environmentProperties aspect
- `POST /api/v1/aspects/environmentProperties` - Upsert environmentProperties aspect
- `DELETE /api/v1/aspects/environmentProperties/{entity_label}/{entity_urn}` - Delete environmentProperties aspect

#### Globaltags
- `GET /api/v1/aspects/globalTags/{entity_label}/{entity_urn}` - Get globalTags aspect
- `POST /api/v1/aspects/globalTags` - Upsert globalTags aspect
- `DELETE /api/v1/aspects/globalTags/{entity_label}/{entity_urn}` - Delete globalTags aspect

#### Ownership
- `GET /api/v1/aspects/ownership/{entity_label}/{entity_urn}` - Get ownership aspect
- `POST /api/v1/aspects/ownership` - Upsert ownership aspect
- `DELETE /api/v1/aspects/ownership/{entity_label}/{entity_urn}` - Delete ownership aspect

#### Schemametadata
- `GET /api/v1/aspects/schemaMetadata/{entity_label}/{entity_urn}` - Get schemaMetadata aspect
- `POST /api/v1/aspects/schemaMetadata` - Upsert schemaMetadata aspect
- `DELETE /api/v1/aspects/schemaMetadata/{entity_label}/{entity_urn}` - Delete schemaMetadata aspect

#### Sourcecode
- `GET /api/v1/aspects/sourceCode/{entity_label}/{entity_urn}` - Get sourceCode aspect
- `POST /api/v1/aspects/sourceCode` - Upsert sourceCode aspect
- `DELETE /api/v1/aspects/sourceCode/{entity_label}/{entity_urn}` - Delete sourceCode aspect

#### Sourcecodelocation
- `GET /api/v1/aspects/sourceCodeLocation/{entity_label}/{entity_urn}` - Get sourceCodeLocation aspect
- `POST /api/v1/aspects/sourceCodeLocation` - Upsert sourceCodeLocation aspect
- `DELETE /api/v1/aspects/sourceCodeLocation/{entity_label}/{entity_urn}` - Delete sourceCodeLocation aspect

#### Transformation
- `GET /api/v1/aspects/transformation/{entity_label}/{entity_urn}` - Get transformation aspect
- `POST /api/v1/aspects/transformation` - Upsert transformation aspect
- `DELETE /api/v1/aspects/transformation/{entity_label}/{entity_urn}` - Delete transformation aspect

### Health Check
- `GET /api/v1/health` - Health check endpoint

## Setup

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Set environment variables (optional - the API will auto-detect the registry path):
```bash
export REGISTRY_PATH="config/main_registry.yaml"  # Optional - auto-detected if not set
export NEO4J_URI="bolt://localhost:7687"
export NEO4J_USER="neo4j"
export NEO4J_PASSWORD="password"
export API_HOST="0.0.0.0"
export API_PORT="8000"
```

3. Run the API:
```bash
python main.py
```

4. Access the API:
- API Documentation: http://localhost:8000/docs
- Health Check: http://localhost:8000/api/v1/health

## Generated Files

- `main.py` - Main FastAPI application
- `models.py` - Pydantic models for requests/responses
- `get_routes.py` - GET operation routes
- `upsert_routes.py` - POST/UPSERT operation routes
- `delete_routes.py` - DELETE operation routes
- `factory_wrapper.py` - RegistryFactory wrapper for dependency injection
- `requirements.txt` - Python dependencies
- `README.md` - This file
- `config/` - Directory containing all required YAML configuration files
