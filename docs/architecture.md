# Architecture Documentation

This document describes the architecture and package structure of the `public_company_graph` project.

## Package Structure

```
public_company_graph/
├── __init__.py                    # Package initialization, version
├── config.py                      # Configuration management (Neo4j, OpenAI, paths)
├── cache.py                       # Unified cache (diskcache-based)
├── constants.py                   # Project constants
├── retry.py                       # Retry utilities
│
├── cli/                           # Command-line interface
│   ├── __init__.py               # CLI entry points
│   ├── args.py                   # Argument parsing
│   ├── commands.py               # CLI commands
│   ├── connection.py             # Neo4j connection handling
│   └── logging.py                # Logging setup
│
├── company/                       # Company-related operations
│   ├── __init__.py
│   ├── enrichment.py             # Company data enrichment
│   ├── queries.py                # Company queries
│   └── similarity.py             # Company similarity computation
│
├── consensus/                     # Multi-source consensus
│   ├── __init__.py
│   └── domain_consensus.py       # Domain consensus from multiple sources
│
├── domain/                        # Domain-related operations
│   ├── __init__.py
│   ├── models.py                 # Domain data models
│   └── validation.py             # Domain validation (tldextract)
│
├── embeddings/                    # OpenAI embedding creation
│   ├── __init__.py               # Public API
│   ├── create.py                 # Embedding creation for Neo4j nodes
│   ├── chunking.py               # Text chunking for large documents
│   └── openai_client.py          # OpenAI API client wrapper
│
├── gds/                           # Graph Data Science
│   ├── __init__.py               # GDS client helpers
│   ├── company_similarity.py     # Company description similarity
│   ├── company_tech.py           # Company-technology similarity
│   ├── tech_adoption.py          # Technology adoption prediction
│   ├── tech_affinity.py          # Technology co-occurrence
│   └── utils.py                  # GDS utility functions
│
├── ingest/                        # Data ingestion (SQLite → Neo4j)
│   ├── __init__.py               # Public API
│   ├── sqlite_readers.py         # SQLite data readers
│   └── loaders.py                # Neo4j batch loaders
│
├── neo4j/                         # Neo4j database interaction
│   ├── __init__.py               # Public API
│   ├── connection.py             # Driver and session management
│   ├── constraints.py            # Constraint and index creation
│   └── utils.py                  # Utility functions
│
├── parsing/                       # 10-K filing parsing
│   ├── __init__.py
│   ├── base.py                   # TenKParser interface
│   ├── business_description.py   # Item 1 extraction (datamule + custom)
│   ├── business_relationship_extraction.py  # Competitor/customer/supplier/partner
│   ├── competitor_extraction.py  # Competitor extraction utilities
│   ├── filing_metadata.py        # Filing date, accession number
│   ├── risk_factors.py           # Item 1A extraction
│   ├── text_extraction.py        # Text extraction utilities
│   └── website_extraction.py     # Company website extraction
│
├── similarity/                    # Similarity computation (non-GDS)
│   ├── __init__.py
│   └── cosine.py                 # NumPy-based cosine similarity
│
├── sources/                       # External data sources
│   ├── __init__.py
│   ├── datamule_index.py         # Datamule submission index
│   ├── finnhub.py                # Finnhub API
│   ├── finviz.py                 # Finviz scraping
│   ├── sec_companies.py          # SEC company list
│   ├── sec_edgar.py              # SEC EDGAR API
│   ├── sec_edgar_check.py        # SEC EDGAR pre-checks
│   └── yfinance.py               # Yahoo Finance
│
└── utils/                         # Shared utilities
    ├── __init__.py
    ├── datamule.py               # Datamule wrapper (parsing, caching)
    ├── file_discovery.py         # File discovery utilities
    ├── hashing.py                # Content hashing
    ├── parallel.py               # Parallel processing
    ├── rate_limiting.py          # Rate limiting
    ├── stats.py                  # Statistics helpers
    ├── tar_extraction.py         # Tar file extraction
    ├── tar_selection.py          # Tar file selection
    ├── tenk_workers.py           # 10-K worker processes
    ├── thread_safe_output.py     # Thread-safe console output
    └── tqdm_logging.py           # Progress bar with logging
```

## Core Modules

### `config.py`

**Purpose**: Centralizes configuration management for Neo4j, OpenAI, and data paths.

**Design**: Uses `pydantic-settings` for type-safe configuration with automatic `.env` file loading.

### `cache.py`

**Purpose**: Unified caching using `diskcache` (SQLite-backed).

**Namespaces**:
- `10k_extracted` - Parsed 10-K data
- `company_domains` - Domain consensus
- `embeddings` - OpenAI embeddings

### `parsing/`

**Purpose**: 10-K filing parsing using pluggable interface pattern.

**Key Components**:
- **`base.py`**: `TenKParser` abstract interface
- **`business_description.py`**: Hybrid datamule + custom parser (99.85% coverage)
- **`business_relationship_extraction.py`**: Entity resolution for competitors/customers/suppliers

### `gds/`

**Purpose**: Neo4j Graph Data Science algorithm execution.

**Key Components**:
- **`company_similarity.py`**: SIMILAR_DESCRIPTION relationships
- **`tech_adoption.py`**: LIKELY_TO_ADOPT via Personalized PageRank
- **`tech_affinity.py`**: CO_OCCURS_WITH via Node Similarity

## Design Principles

### 1. Separation of Concerns

- **Readers** (SQLite) are separate from **Loaders** (Neo4j)
- **Configuration** is centralized, not scattered
- **CLI utilities** are shared, not duplicated

### 2. Idempotency

- All operations are re-runnable
- MERGE operations prevent duplicates
- Constraints ensure data integrity

### 3. Fail Fast

- Pre-flight checks verify prerequisites
- Scripts abort early if critical conditions aren't met
- No silent fallbacks for critical errors

### 4. Dry-Run Pattern

- All scripts support `--execute` flag
- Dry-run mode shows plan without making changes
- Execute mode performs actual work

### 5. Hybrid Parsing

- Primary: datamule library (~88% success)
- Fallback: Custom BeautifulSoup parser (~12% of cases)
- Combined: 99.85% coverage

## Data Flow

### 10-K Parsing Flow

```
SEC EDGAR
  ↓ [datamule download]
Tar files (data/10k_portfolios/)
  ↓ [extraction]
HTML files (data/10k_filings/)
  ↓ [parse_10k_filings.py]
Cache (10k_extracted namespace)
  ↓ [load_company_data.py]
Neo4j (Company nodes)
```

### Similarity Computation Flow

```
Company nodes (with descriptions)
  ↓ [create_company_embeddings.py]
OpenAI API → Cache → Neo4j (description_embedding)
  ↓ [compute_company_similarity.py]
SIMILAR_DESCRIPTION relationships
```

### Business Relationship Flow

```
10-K text (business descriptions, risk factors)
  ↓ [extract_business_relationships.py]
Entity resolution against SEC CIK database
  ↓
HAS_COMPETITOR, HAS_CUSTOMER, HAS_SUPPLIER, HAS_PARTNER relationships
```

## Testing

- **Unit Tests**: `tests/unit/` - Test individual functions
- **Integration Tests**: `tests/integration/` - Test full workflows

```bash
# Run tests
pytest tests/ -v

# Run with coverage
pytest tests/ -v --cov=public_company_graph
```

## Dependencies

### Core
- `neo4j` - Neo4j Python driver
- `graphdatascience` - Neo4j GDS client
- `openai` - OpenAI API client
- `datamule` - SEC filing download/parsing
- `pydantic-settings` - Configuration management
- `numpy` - Vector operations
- `beautifulsoup4`, `lxml` - HTML parsing

### Development
- `pytest`, `pytest-cov` - Testing
- `ruff` - Linting/formatting
- `mypy` - Type checking
- `pre-commit` - Git hooks

---

*Last Updated: 2025-01-01*
