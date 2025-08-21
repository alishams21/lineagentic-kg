# LineAgent Project Makefile
# Centralized build and development commands

.PHONY: help start-databases stop-databases stop-databases-and-clean-data clean-all-stack generate-mermaid-diagram

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
clean-all-stack:
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
	@rm -rf lineagentic-catalog.egg-info 2>/dev/null || echo "No lineagentic-catalog.egg-info folder found"
	@rm -rf .pytest_cache 2>/dev/null || echo "No .pytest_cache folder found"
	@rm -rf .mypy_cache 2>/dev/null || echo "No .mypy_cache folder found"
	@rm -rf generated_api 2>/dev/null || echo "No generated_api folder found"
	@rm -rf generated_cli 2>/dev/null || echo "No generated_cli folder found"
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
