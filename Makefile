# LineAgent Project Makefile
# Centralized build and development commands

.PHONY: help start-databases stop-databases stop-databases-and-clean-data clean-all-stack generate-mermaid-diagram generate-and-run-api generate-cli run-cli test test-registry test-api test-cli test-coverage

help:
	@echo "🚀 Lineagentic Project"
	@echo ""
	@echo "🚀 Available commands:"
	@echo "  - start-databases: Start databases"
	@echo "  - stop-databases: Stop databases"
	@echo "  - stop-databases-and-clean-data: Stop databases and clean data"
	@echo "  - clean-all-stack: Clean all stack"
	@echo "  - generate-mermaid-diagram: Generate Mermaid diagram"
	@echo "  - install-deps: Install dependencies with uv"
	@echo "  - install-dev: Install development dependencies with uv"
	@echo "  - lock-deps: Lock dependencies with uv"
	@echo "  - sync-deps: Sync dependencies with uv"
	@echo "  - generate-and-run-api: Generate and run the API server"
	@echo "  - generate-cli: Generate the CLI"
	@echo "  - run-cli: Run CLI command (e.g., 'make run-cli ARGS=\"health\"')"
	@echo ""
	@echo "🧪 Testing Commands:"
	@echo "  - test: Run all tests"
	@echo "  - test-registry: Run registry tests only"
	@echo "  - test-api: Run API generator tests only"
	@echo "  - test-cli: Run CLI generator tests only"
	@echo "  - test-coverage: Run tests with coverage report"

	@echo ""
	@echo "📦 PyPI Publishing Commands:"
	@echo "  - build-package: Build the PyPI package"
	@echo "  - publish-testpypi: Publish to TestPyPI (testing)"
	@echo "  - publish-pypi: Publish to PyPI (production)"

# Load environment variables from .env file
ifneq (,$(wildcard .env))
    include .env
    export
endif

# =============================================================================
# DATABASES SERVERS

# Start all databases with docker-compose
start-databases:
	@echo "🚀 Starting databases with docker-compose..."
	@docker-compose up -d
	@echo "✅ Databases started!"
	@echo " Databases available at:"
	@echo "  - Neo4j Database: localhost:7474 (HTTP) / localhost:7687 (Bolt)"
	@echo ""
	@echo "⏳ Waiting for Neo4j to be ready..."
	@until docker exec neo4j-lineage cypher-shell -u neo4j -p password "RETURN 1" > /dev/null 2>&1; do \
		echo "   Waiting for Neo4j to be ready..."; \
		sleep 3; \
	done
	@echo "✅ Neo4j is ready!"
	@echo ""

# Stop all databases with docker-compose
stop-databases:
	@echo "🛑 Stopping all services with docker-compose..."
	@docker-compose down
	@echo "✅ All services stopped!"

# Stop all databases with docker-compose and remove volumes (CLEANS DATA)
stop-databases-and-clean-data:
	@echo "🛑 Stopping all databases and removing volumes (WILL DELETE ALL DATA)..."
	@docker-compose down -v
	@echo "✅ All databases stopped and data cleaned!"


# =============================================================================
# CLEANUP COMMANDS ############################################################
# =============================================================================

# Remove all __pycache__ directories
clean-pycache:
	@echo "🗑️  Removing all __pycache__ directories..."
	@echo " Searching for __pycache__ directories..."
	@find . -type d -name "__pycache__" -print
	@echo "🗑️  Removing found directories..."
	@find . -type d -name "__pycache__" -exec rm -rf {} + || echo "Error removing some directories"
	@echo "🔍 Verifying removal..."
	@if find . -type d -name "__pycache__" 2>/dev/null | grep -q .; then \
		echo "⚠️  Some __pycache__ directories still exist:"; \
		find . -type d -name "__pycache__" 2>/dev/null; \
	else \
		echo "✅ All __pycache__ directories removed successfully!"; \
	fi

# Clean up temporary files and kill processes
clean-all:
	@echo "🧹 Cleaning up temporary files and processes..."
	@echo "🛑 Killing processes on ports 8000, 7860..."
	@lsof -ti:8000 | xargs kill -9 2>/dev/null || echo "No process on port 8000"
	@lsof -ti:7860 | xargs kill -9 2>/dev/null || echo "No process on port 7860"
	@echo "🗑️  Cleaning up temporary files..."
	@find . -name "*.log" -type f -delete
	@find . -name "temp_*.json" -type f -delete
	@find . -name "generated-*.json" -type f -delete
	@echo "🗑️  Removing data folders..."
	@rm -rf agents_log 2>/dev/null || echo "No agents_log folder found"
	@rm -rf lineage_extraction_dumps 2>/dev/null || echo "No lineage_extraction_dumps folder found"
	@rm -rf .venv 2>/dev/null || echo "No .venv folder found"
	@rm -rf lineagentic-kg.egg-info 2>/dev/null || echo "No lineagentic-kg.egg-info folder found"
	@rm -rf .pytest_cache 2>/dev/null || echo "No .pytest_cache folder found"
	@rm -rf .mypy_cache 2>/dev/null || echo "No .mypy_cache folder found"
	@rm -rf generated_api 2>/dev/null || echo "No generated_api folder found"
	@rm -rf generated_cli 2>/dev/null || echo "No generated_cli folder found"
	@rm -rf logs 2>/dev/null || echo "No logs folder found"
	@rm -rf generated_cli 2>/dev/null || echo "No generated_cli folder found"
	@rm -rf dist 2>/dev/null || echo "No dist folder found"
	@rm -rf lineagentic_kg.egg-info 2>/dev/null || echo "No lineagentic_kg.egg-info folder found"
	@rm -rf .pytest_cache 2>/dev/null || echo "No .pytest_cache folder found"
	@$(MAKE) clean-pycache
	@echo "✅ Cleanup completed!"



# =============================================================================
# UV Package Management #######################################################
# =============================================================================

# Install dependencies with uv
install-deps:
	@echo "📦 Installing dependencies with uv..."
	@uv pip install -e .
	@echo "✅ Dependencies installed!"

# Install development dependencies with uv
install-dev:
	@echo "📦 Installing development dependencies with uv..."
	@uv pip install -e ".[dev]"
	@echo "✅ Development dependencies installed!"

# Lock dependencies with uv
lock-deps:
	@echo "🔒 Locking dependencies with uv..."
	@uv lock
	@echo "✅ Dependencies locked!"

# Sync dependencies with uv
sync-deps:
	@echo "🔄 Syncing dependencies with uv..."
	@uv sync
	@echo "✅ Dependencies synced!"

# =============================================================================
# API Generation and Execution ################################################
# =============================================================================

# Generate and run the API server
generate-and-run-api:
	@echo "🚀 Generating and running API server..."
	@echo "1️⃣ Syncing dependencies with uv..."
	@uv sync
	@echo "✅ Dependencies synced!"
	@echo ""
	@echo "2️⃣ Activating virtual environment..."
	@source .venv/bin/activate || echo "⚠️  Virtual environment activation failed, continuing..."
	@echo "✅ Environment activated!"
	@echo ""
	@echo "3️⃣ Generating API from registry..."
	@generate-api
	@echo "✅ API generated!"
	@echo ""
	@echo "4️⃣ Installing API dependencies..."
	@cd generated_api && pip install -r requirements.txt
	@echo "✅ API dependencies installed!"
	@echo ""
	@echo "5️⃣ Starting API server..."
	@cd generated_api && python main.py
	@echo "✅ API server started!"

# =============================================================================
# CLI Generation and Execution #################################################
# =============================================================================

# Generate and run the CLI
generate-cli:
	@echo "🚀 Generating and running CLI..."
	@echo "1️⃣ Syncing dependencies with uv..."
	@uv sync
	@echo "✅ Dependencies synced!"
	@echo ""
	@echo "2️⃣ Activating virtual environment..."
	@source .venv/bin/activate || echo "⚠️  Virtual environment activation failed, continuing..."
	@echo "✅ Environment activated!"
	@echo ""
	@echo "3️⃣ Generating CLI from registry..."
	@generate-cli
	@echo "✅ CLI generated!"
	@echo ""
	@echo "4️⃣ Installing CLI dependencies..."
	@cd generated_cli && pip install -r requirements.txt
	@echo "✅ CLI dependencies installed!"
	@echo ""
	@echo "5️⃣ Starting CLI..."
	@cd generated_cli && python lineagentic_cli.py --help
	@echo "✅ CLI is ready! Use 'make cli <command>' to run commands"

# Run CLI command with proper environment
run-cli:
	@echo "🚀 Running CLI command..."
	@cd generated_cli && python lineagentic_cli.py $(ARGS)



# =============================================================================
# Generate Mermaid Diagram #####################################################
# =============================================================================

# Generate Mermaid diagrams from source files
generate-mermaid-diagram:
	@echo "🎨 Generating Mermaid diagrams..."
	@if [ -f "docs/generate_diagrams.sh" ]; then \
		cd docs && chmod +x generate_diagrams.sh && ./generate_diagrams.sh; \
	else \
		echo "❌ generate_diagrams.sh not found in docs/ directory"; \
		exit 1; \
	fi
	@echo "✅ Mermaid diagrams generated successfully!"



# =============================================================================
# PYPI PACKAGE COMMANDS ######################################################
# =============================================================================

# Build the PyPI package
build-package:
	@echo "📦 Building PyPI package..."
	@echo "🧹 Cleaning previous builds..."
	@rm -rf dist build *.egg-info
	@echo "🔨 Building package..."
	@python -m build
	@echo "Package built successfully!"
	@echo "Package files created in dist/ directory"
	@echo "Next steps:"
	@echo "  - Test locally: pip install dist/lineagentic_kg-0.1.0.tar.gz"
	@echo "  - Publish to TestPyPI: make publish-testpypi"
	@echo "  - Publish to PyPI: make publish-pypi"

# Publish to TestPyPI (testing)
publish-testpypi:
	@echo "Publishing to TestPyPI (testing)..."
	@if [ ! -d "dist" ]; then \
		echo " No dist/ directory found. Run 'make build-package' first."; \
		exit 1; \
	fi
	@echo " Checking package..."
	@python -m twine check dist/*
	@echo " Uploading to TestPyPI..."
	@python -m twine upload --repository testpypi dist/*
	@echo "Package published to TestPyPI!"
	@echo "View at: https://test.pypi.org/project/lineagentic-kg/"
	@echo "Test install: pip install --index-url https://test.pypi.org/simple/ lineagentic-kg"

# Publish to PyPI (production)
publish-pypi:
	@echo "Publishing to PyPI (production)..."
	@if [ ! -d "dist" ]; then \
		echo " No dist/ directory found. Run 'make build-package' first."; \
		exit 1; \
	fi
	@echo "WARNING: This will publish to production PyPI!"
	@echo "   Make sure you have tested on TestPyPI first."
	@read -p "Continue? (y/N): " confirm && [ "$$confirm" = "y" ] || exit 1
	@echo " Checking package..."
	@python -m twine check dist/*
	@echo " Uploading to PyPI..."
	@python -m twine upload dist/*
	@echo "Package published to PyPI!"
	@echo "View at: https://pypi.org/project/lineagentic-kg/"
	@echo "Install with: pip install lineagentic-kg"

# =============================================================================
# TESTING COMMANDS ###########################################################
# =============================================================================



# Run registry tests only
test-registry:
	@echo "🧪 Running registry tests..."
	@echo "1️⃣ Syncing dependencies with uv..."
	@uv sync
	@echo "✅ Dependencies synced!"
	@echo ""
	@echo "2️⃣ Running registry test suite..."
	@python tests/run_registry_tests.py
	@echo "✅ Registry tests completed!"

# Run API generator tests only
test-api:
	@echo "🧪 Running API generator tests..."
	@echo "1️⃣ Syncing dependencies with uv..."
	@uv sync
	@echo "✅ Dependencies synced!"
	@echo ""
	@echo "2️⃣ Running API generator test suite..."
	@python tests/run_api_tests.py
	@echo "✅ API generator tests completed!"

# Run CLI generator tests only
test-cli:
	@echo "🧪 Running CLI generator tests..."
	@echo "1️⃣ Syncing dependencies with uv..."
	@uv sync
	@echo "✅ Dependencies synced!"
	@echo ""
	@echo "2️⃣ Running CLI generator test suite..."
	@python tests/run_cli_tests.py
	@echo "✅ CLI generator tests completed!"


