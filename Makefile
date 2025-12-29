# Domain Status Graph - Repeatable Pipeline
# ==========================================
#
# Usage:
#   make help          - Show available targets
#   make check         - Run health check
#   make pipeline      - Run full pipeline (dry-run)
#   make pipeline-exec - Run full pipeline (execute)
#
# Individual steps:
#   make bootstrap     - Load Domain/Technology nodes
#   make download      - Download 10-K filings
#   make parse         - Parse 10-K filings
#   make companies     - Collect domains + load companies
#   make relationships - Extract business relationships (competitor, customer, supplier, partner)
#   make embeddings    - Create embeddings
#   make gds           - Compute GDS features

.PHONY: help check pipeline pipeline-exec bootstrap download parse companies relationships embeddings gds clean

PYTHON := python

help:
	@echo "Domain Status Graph - Pipeline Commands"
	@echo "========================================"
	@echo ""
	@echo "Quick commands:"
	@echo "  make check          - Run health check"
	@echo "  make pipeline       - Dry-run full pipeline"
	@echo "  make pipeline-exec  - Execute full pipeline"
	@echo ""
	@echo "Individual steps (add -exec suffix to execute):"
	@echo "  make bootstrap      - Step 1: Load Domain/Technology nodes"
	@echo "  make download       - Step 2: Download 10-K filings"
	@echo "  make parse          - Step 3: Parse 10-K filings"
	@echo "  make companies      - Step 4-5: Collect domains + load companies"
	@echo "  make relationships  - Step 6: Extract business relationships"
	@echo "  make embeddings     - Step 7: Create embeddings"
	@echo "  make gds            - Step 8: Compute GDS features"
	@echo ""
	@echo "Maintenance:"
	@echo "  make lint           - Run linters"
	@echo "  make test           - Run tests"

# Health check
check:
	$(PYTHON) scripts/health_check.py

# Full pipeline
pipeline:
	$(PYTHON) scripts/run_all_pipelines.py

pipeline-exec:
	$(PYTHON) scripts/run_all_pipelines.py --execute

# Step 1: Bootstrap graph (Domain + Technology nodes)
bootstrap:
	$(PYTHON) scripts/bootstrap_graph.py

bootstrap-exec:
	$(PYTHON) scripts/bootstrap_graph.py --execute

# Step 2: Download 10-K filings
download:
	$(PYTHON) scripts/download_10k_filings.py

download-exec:
	$(PYTHON) scripts/download_10k_filings.py --execute

# Step 3: Parse 10-K filings (uses multiprocessing)
parse:
	$(PYTHON) scripts/parse_10k_filings.py

parse-exec:
	$(PYTHON) scripts/parse_10k_filings.py --execute

# Step 4-5: Collect domains and load companies
companies:
	$(PYTHON) scripts/collect_domains.py
	$(PYTHON) scripts/load_company_data.py

companies-exec:
	$(PYTHON) scripts/collect_domains.py --execute
	$(PYTHON) scripts/load_company_data.py --execute

# Step 6: Extract business relationships from 10-K filings
relationships:
	$(PYTHON) scripts/extract_business_relationships.py

relationships-exec:
	$(PYTHON) scripts/extract_business_relationships.py --execute

# Step 7: Create embeddings (requires OPENAI_API_KEY)
embeddings:
	$(PYTHON) scripts/create_company_embeddings.py
	$(PYTHON) scripts/create_domain_embeddings.py

embeddings-exec:
	$(PYTHON) scripts/create_company_embeddings.py --execute
	$(PYTHON) scripts/create_domain_embeddings.py --execute

# Step 7: Compute GDS features
gds:
	$(PYTHON) scripts/compute_gds_features.py

gds-exec:
	$(PYTHON) scripts/compute_gds_features.py --execute

# Development
lint:
	ruff check --fix domain_status_graph/ scripts/
	ruff format domain_status_graph/ scripts/
	black domain_status_graph/ scripts/

test:
	pytest tests/ -v

# Clean (be careful!)
clean:
	@echo "This would delete cached data. Use scripts/cleanup_10k_data.py instead."
	@echo "To clear Neo4j: MATCH (n) DETACH DELETE n"
