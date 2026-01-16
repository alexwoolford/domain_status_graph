# Public Company Graph

A reproducible knowledge graph of U.S. public companies built from SEC 10-K filings, combining structured data extraction with graph analytics. Inspired by academic research on company knowledge graphs and designed to showcase Neo4j Graph Data Science (GDS) capabilities for business intelligence.

## Motivation & Background

### Why This Project Exists

Traditional company databases treat businesses as isolated records. But companies exist in rich relationship networks: they compete, partner, supply, and acquire each other. Their technology choices cluster in patterns. Their risk profiles correlate across industries.

This project builds a **knowledge graph** that captures these relationships, enabling queries that would be impossible (or extremely complex) in relational databases:

- *"Find companies similar to Apple by business model AND technology stack AND competitive position"*
- *"Map NVIDIA's supply chain and find companies 2 hops away"*
- *"Which technologies commonly co-occur with Kubernetes adoption?"*
- *"Which companies would be impacted by a helium shortage?"* → Reveals medical device companies, industrial gas distributors, and electronics manufacturers through supply chain relationships
- *"If Oracle went out of business, who would be affected?"* → Traces impacts to government contractors, e-commerce platforms, and enterprise software competitors
- *"How would China rare earth export controls affect EV manufacturers?"* → Connects rare earth producers → magnet suppliers → EV companies through multi-hop supply chains

### Related Research

This project draws inspiration from:

- **[CompanyKG: A Large-Scale Heterogeneous Graph for Company Similarity Quantification](https://arxiv.org/abs/2306.10649)** (NeurIPS 2023) - Academic research on building company knowledge graphs from SEC filings
- **[SEC EDGAR](https://www.sec.gov/edgar)** - The source of truth for public company disclosures

---

## What This Project Does

### 1. Data Collection

| Source | What We Extract | Tool/Method |
|--------|-----------------|-------------|
| **SEC EDGAR** | 10-K filings, company metadata | [datamule](https://github.com/john-googletv/datamule-python) library |
| **Yahoo Finance** | Sector, industry, market cap, employees | `yfinance` library |
| **Company Websites** | Technology fingerprints (566+ technologies) | [domain_status](https://github.com/alexwoolford/domain_status) (Rust) |

### 2. Information Extraction

From each 10-K filing, we extract:
- **Business descriptions** (Item 1) - What the company does
- **Risk factors** (Item 1A) - Company-specific risks
- **Competitor mentions** - Who they compete with
- **Customer/supplier/partner mentions** - Business relationships

### 3. Knowledge Graph Construction

We build a graph with:
- **5,398 Company nodes** with 17+ properties each
- **4,337 Domain nodes** with technology detection
- **827 Technology nodes** categorized by type
- **2+ million relationships** capturing similarity and business connections

### 4. Graph Analytics

Using Neo4j Graph Data Science (GDS):
- **Company similarity** via embedding cosine similarity
- **Technology adoption prediction** via Personalized PageRank
- **Technology co-occurrence** via Jaccard similarity
- **Industry/size/risk clustering** via custom algorithms

---

## Graph Schema Overview

**Nodes**: 5,398 Companies • 4,337 Domains • 827 Technologies

**Relationships** (~2M total):

| Type | From → To | Count | Source |
|------|-----------|-------|--------|
| `SIMILAR_INDUSTRY` | Company → Company | 520,672 | Sector/industry match |
| `SIMILAR_DESCRIPTION` | Company → Company | 436,973 | Embedding cosine similarity |
| `SIMILAR_SIZE` | Company → Company | 414,096 | Revenue/market cap buckets |
| `SIMILAR_RISK` | Company → Company | 394,372 | Risk factor embeddings |
| `SIMILAR_TECHNOLOGY` | Company → Company | 124,584 | Jaccard on tech stacks |
| `USES` | Domain → Technology | 46,081 | HTTP fingerprinting |
| `LIKELY_TO_ADOPT` | Domain → Technology | 41,250 | PageRank prediction |
| `CO_OCCURS_WITH` | Technology → Technology | 41,220 | Co-occurrence analysis |
| `HAS_COMPETITOR` | Company → Company | 3,249 | Extracted from 10-K (embedding verified, threshold ≥0.35) |
| `HAS_DOMAIN` | Company → Domain | 3,745 | Company website |
| `HAS_PARTNER` | Company → Company | 588 | Extracted from 10-K (embedding verified) |
| `HAS_CUSTOMER` | Company → Company | 243 | Extracted from 10-K (LLM verified) |
| `HAS_SUPPLIER` | Company → Company | 130 | Extracted from 10-K (LLM verified) |
| `CANDIDATE_*` | Company → Company | 1,048 | Medium confidence (with evidence) |

For complete schema documentation, see [docs/graph_schema.md](docs/graph_schema.md).

---

## Data Coverage

| Data Type | Coverage | Notes |
|-----------|----------|-------|
| Companies | 100% (5,398) | All U.S. public companies with 10-K filings |
| Business Descriptions | 99.85% | From 10-K Item 1 |
| Competitor Relationships | ~60% | 3,249 relationships (self-declared in 10-Ks) |
| Supply Chain | ~2.4% | 130 relationships (SEC doesn't require disclosure) |
| Sector/Industry | ~18% | Yahoo Finance (actively traded stocks only) |
| Technology Stack | ~69% | 3,745 companies with detected web technologies |

**Note**: Supply chain data is sparse because SEC filings don't require supplier disclosure. The relationships that exist are high-quality (LLM-verified, ~95% precision).

---

## Known Limitations

- **Technology Detection**: Only web technologies (JavaScript, CMS, CDN). Backend infrastructure (Kubernetes, Docker) not detected (requires different approach).
- **Time Series**: Single snapshot - no historical relationship tracking (future enhancement).
- **Supply Chain**: Sparse coverage (~2.4%) - SEC doesn't require supplier disclosure.
- **Market Data**: Sector/industry data only for ~18% of companies (Yahoo Finance limitation).
- **Large Tech Companies**: Often use generic language, missing specific relationships.

---

## Prerequisites

### Required Software

| Component | Version | Purpose |
|-----------|---------|---------|
| **Python** | 3.11+ | Runtime environment |
| **Neo4j** | 5.x+ | Graph database |
| **Neo4j GDS** | 2.x+ | Graph Data Science library (plugin) |
| **Conda** | Latest | Environment management (recommended) |

### Required API Keys

| Service | Purpose | Get Key |
|---------|---------|---------|
| **OpenAI** | Text embeddings for similarity | [platform.openai.com](https://platform.openai.com/api-keys) |
| **Datamule** | SEC 10-K filing download/parsing | [datamule.xyz](https://datamule.xyz) |

### Optional Data Sources

| Component | Purpose | Notes |
|-----------|---------|-------|
| **[domain_status](https://github.com/alexwoolford/domain_status)** | Technology detection on company websites | Rust tool, generates `domain_status.db` |
| **Yahoo Finance** | Company metadata enrichment | Free, no API key needed |

---

## Installation

### 1. Clone and Setup Environment

```bash
git clone https://github.com/alexwoolford/public-company-graph.git
cd public-company-graph

# Create conda environment (recommended)
conda create -n public_company_graph python=3.13
conda activate public_company_graph

# Install package in editable mode
pip install -e .

# For development (linting, testing)
pip install -e ".[dev]"
```

### 2. Configure Environment

```bash
cp .env.sample .env
```

Edit `.env` with your credentials:

```bash
# Neo4j Connection (required)
NEO4J_URI=bolt://localhost:7687
NEO4J_USER=neo4j
NEO4J_PASSWORD=your_neo4j_password
NEO4J_DATABASE=domain

# OpenAI API (required for embeddings)
OPENAI_API_KEY=sk-proj-your_openai_key_here

# Datamule API (required for 10-K download/parsing)
DATAMULE_API_KEY=your_datamule_key_here

# Optional
FINNHUB_API_KEY=your_finnhub_key_here
```

### 3. Verify Setup

```bash
# Check Neo4j connection
python -c "from public_company_graph.neo4j.connection import get_neo4j_driver; driver = get_neo4j_driver(); driver.verify_connectivity(); driver.close()"

# Run health check
health-check
```

---

## Quick Start with Pre-built Graph

If you want to explore the graph immediately without running the full ingest pipeline, you can restore from the included database dump.

### Prerequisites

- Neo4j 5.x+ installed and **stopped** (`bin/neo4j stop`)
- Git LFS installed (`brew install git-lfs` on macOS)

### Restore the Dump

```bash
# Pull LFS files if not already done
git lfs pull

# Copy dump to match target database name (e.g., neo4j)
cp data/domain.dump data/neo4j.dump

# Restore the database (Neo4j must be stopped)
neo4j-admin database load neo4j --from-path=data/ --overwrite-destination=true

# Start Neo4j
neo4j start

# Clean up the copied file
rm data/neo4j.dump
```

The dump contains:
- **10,562 nodes** (5,398 Companies, 4,337 Domains, 827 Technologies)
- **2+ million relationships** (similarity, competitors, tech adoption, etc.)

After restore, connect to Neo4j and start exploring with the [example queries](#example-queries) below.

> **Note**: The dump is stored in Git LFS (~6MB compressed, ~34MB uncompressed). The full pipeline with all data sources requires running the steps in [Running the Pipeline](#running-the-pipeline).

### Ask Questions with GraphRAG Chat

The easiest way to explore the graph is through the interactive chat interface. After restoring the dump, you can ask natural language questions:

```bash
# Start the chat interface (requires OpenAI API key for answer synthesis)
python scripts/chat_graphrag.py
```

**Example questions you can ask:**
- "Which companies would be impacted by a shortage of helium?"
- "If Oracle went out of business, which companies would be affected?"
- "What companies depend on NVIDIA as a supplier?"
- "Which companies are similar to Tesla?"
- "How would China rare earth export controls affect EV manufacturers?"

The chatbot uses GraphRAG to:
1. Search 10-K filing text via vector search
2. Traverse graph relationships (competitors, suppliers, partners)
3. Synthesize comprehensive answers with citations

**Requirements**:
- Neo4j running with the graph loaded (from dump or pipeline)
- OpenAI API key in `.env` (for answer synthesis)

> **Tip**: You can also explore via Cypher queries in Neo4j Browser, but the chat interface is the fastest way to get insights without writing queries.

---

## Running the Pipeline

### Option A: Full Pipeline (Recommended for Fresh Start)

```bash
# Download 10-K filings, parse, load, compute all features
python scripts/run_all_pipelines.py --execute
```

This runs the complete pipeline:
1. Download 10-K filings via datamule
2. Parse business descriptions and extract relationships
3. Load companies, domains, technologies into Neo4j
4. Create embeddings via OpenAI
5. Compute similarity relationships
6. Compute GDS features (adoption prediction, co-occurrence)

### Option B: Step-by-Step

```bash
# 1. Download 10-K filings (uses datamule)
python scripts/download_10k_filings.py --execute

# 2. Parse filings and extract data
python scripts/parse_10k_filings.py --execute

# 3. Load company data into Neo4j
python scripts/load_company_data.py --execute

# 4. Bootstrap domain/technology graph (requires domain_status.db)
python scripts/bootstrap_graph.py --execute

# 5. Create embeddings
python scripts/create_company_embeddings.py --execute

# 6. Compute similarity relationships
python scripts/compute_company_similarity.py --execute

# 7. Extract business relationships (competitors, customers, etc.)
python scripts/extract_with_llm_verification.py --clean --execute

# 8. Compute GDS features
python scripts/compute_gds_features.py --execute
```

### CLI Commands

All scripts support `--help` and follow a dry-run pattern (omit `--execute` to see plan without changes):

| Command | Description |
|---------|-------------|
| `health-check` | Verify Neo4j connection and data |
| `bootstrap-graph` | Load domains/technologies from SQLite |
| `compute-gds-features` | Compute GDS analytics |
| `compute-company-similarity` | Compute all similarity relationships |
| `validate-famous-pairs` | Validate known competitor pairs |

---

## Example Queries

### Who Are the Market Leaders?

Find companies most frequently cited as competitors (the dominant players everyone considers a threat):

```cypher
MATCH (c:Company)<-[r:HAS_COMPETITOR]-(:Company)
WITH c, count(r) as cited_by
ORDER BY cited_by DESC LIMIT 10
RETURN c.ticker, c.name, cited_by
```

**Result**: Pfizer (84), Microsoft (82), Apple (58), Amgen (52), AbbVie (46), Oracle (44)

### Threat Ratio: Dominant Companies

Find companies cited as competitors far more than they cite others:

```cypher
MATCH (c:Company)<-[inbound:HAS_COMPETITOR]-(:Company)
WITH c, count(inbound) as cited_by
MATCH (c)-[outbound:HAS_COMPETITOR]->(:Company)
WITH c, cited_by, count(outbound) as cites
WHERE cited_by >= 10
RETURN c.ticker, c.name, cited_by, cites,
       round(toFloat(cited_by)/cites * 10) / 10 as threat_ratio
ORDER BY threat_ratio DESC LIMIT 5
```

**Result**: Walmart (21:1), Microsoft (82:4 = 20.5x), Biogen (18:1), Google (20:2 = 10x)

### True Rivalries: Mutual Competitors

Find companies that cite each other as competitors:

```cypher
MATCH (a:Company)-[:HAS_COMPETITOR]->(b:Company)-[:HAS_COMPETITOR]->(a)
WHERE a.ticker < b.ticker
RETURN a.ticker, b.ticker LIMIT 10
```

**Result**: Cigna↔CVS, Broadcom↔IBM, Monster↔PepsiCo, Cirrus↔Skyworks

### Explainable Similarity: KO vs PEP

```cypher
MATCH (c1:Company {ticker: 'KO'}), (c2:Company {ticker: 'PEP'})
OPTIONAL MATCH (c1)-[r1:SIMILAR_DESCRIPTION]->(c2)
OPTIONAL MATCH (c1)-[r2:SIMILAR_RISK]->(c2)
OPTIONAL MATCH (c1)-[r3:SIMILAR_INDUSTRY]->(c2)
RETURN round(r1.score * 100) / 100 as description,
       round(r2.score * 100) / 100 as risk,
       r3.score as industry
```

**Result**: 88% similar descriptions, 87% similar risks, same industry — but **neither cites the other as a competitor**!

### LLM-Verified Supply Chain

```cypher
MATCH (c:Company)-[r:HAS_SUPPLIER]->(s:Company)
WHERE r.llm_verified = true
RETURN c.ticker, s.ticker, left(r.context, 100) as evidence
LIMIT 5
```

**Result**: IREN→NVIDIA ("procured 5.5k NVIDIA B200 GPUs"), United Airlines→Boeing ("sources majority of aircraft from Boeing")

### Find Companies Similar to Tesla

```cypher
MATCH (target:Company {ticker: 'TSLA'})-[r:SIMILAR_DESCRIPTION]->(similar:Company)
WHERE r.score > 0.75
RETURN similar.ticker, similar.name, round(r.score * 100) / 100 as similarity
ORDER BY r.score DESC LIMIT 10
```

**Result**: FTC Solar (0.80), Sunrun (0.80), Rivian (0.80), GM (0.79), Enphase Energy (0.79)

For 50+ more queries with verified results, see [docs/money_queries.md](docs/money_queries.md).

---

## Real-World Impact Analysis: Connecting Current Events to Companies

The graph excels at connecting **real-world events** (wars, tariffs, supply chain disruptions, political changes) to **potential impacts on publicly traded companies** through their disclosed relationships.

### Example: AI Chip Export Restrictions

**Event**: U.S. restrictions on AI chip exports to China (2023-2024)

**Surprising Discovery**: Cryptocurrency mining companies are indirectly impacted through their NVIDIA GPU supply chains—a connection that's not obvious from company descriptions alone.

**Query**:
```cypher
// Find companies that depend on NVIDIA as a supplier
MATCH (c:Company)-[:HAS_SUPPLIER]->(nvda:Company {ticker: 'NVDA'})
RETURN c.ticker, c.name, c.sector
```

**Impact Chain**:
1. **Direct**: NVIDIA faces export restrictions
2. **First-Order**: Companies directly sourcing GPUs:
   - **IREN Ltd** - "procured approximately 5.5k NVIDIA B200 GPUs"
   - **Applied Digital (APLD)** - Data center operator
   - **Bit Digital (BTBT)** - Cryptocurrency mining
3. **Second-Order**: Companies similar to these (via `SIMILAR_DESCRIPTION` or `SIMILAR_TECHNOLOGY`)

**Why It's Surprising**: Cryptocurrency miners aren't typically associated with AI chip restrictions, but the graph reveals they share the same critical supplier as AI companies.

### Example: Red Sea Shipping Disruptions

**Event**: Houthi attacks on Red Sea shipping routes (2024)

**Surprising Discovery**: E-commerce companies using Shopify are exposed through global supply chain dependencies revealed by their technology stack.

**Query**:
```cypher
// Find companies using Shopify (e-commerce platform)
MATCH (c:Company)-[:HAS_DOMAIN]->(d:Domain)-[:USES]->(t:Technology {name: 'Shopify'})
RETURN c.ticker, c.name, c.sector
```

**Impact Chain**:
1. **Direct**: Shipping disruptions affect global logistics
2. **First-Order**: E-commerce companies using Shopify:
   - **Allbirds (BIRD)** - Direct-to-consumer footwear
   - **Arhaus (ARHS)** - Furniture retailer
   - **Constellation Brands (STZ)** - Consumer goods
3. **Second-Order**: Companies with similar business models (via `SIMILAR_DESCRIPTION`)

**Why It's Surprising**: Technology choices (Shopify) reveal supply chain dependencies that aren't obvious from company descriptions alone.

### Example: Boeing Production Delays

**Event**: Boeing 737 MAX production issues (2024)

**Surprising Discovery**: Defense contractors and commercial airlines share the same supplier (Boeing), creating unexpected exposure clusters.

**Query**:
```cypher
// Find companies that depend on Boeing as a supplier
MATCH (c:Company)-[:HAS_SUPPLIER]->(ba:Company {ticker: 'BA'})
RETURN c.ticker, c.name, c.sector
```

**Impact Chain**:
1. **Direct**: Boeing production delays
2. **First-Order**: Airlines directly dependent on Boeing:
   - **United Airlines (UAL)** - "sources majority of aircraft from Boeing"
   - **Southwest Airlines (LUV)** - Boeing 737 fleet
3. **Second-Order**: Aerospace suppliers:
   - **General Electric (GE)** - Aircraft engines
   - **Moog Inc. (MOG-A)** - Aerospace components
   - **Textron (TXT)** - Aerospace systems

**Why It's Surprising**: Defense contractors (GE, Moog) share the same supplier as commercial airlines, creating unexpected exposure to commercial aviation disruptions.

### More Examples

For additional examples connecting current events to company impacts, see [docs/current_events_examples.md](docs/current_events_examples.md), including:
- China rare earth export controls → EV manufacturers
- Oracle Cloud outage → Government contractors
- Helium shortage → Medical device companies
- Semiconductor export controls → Data center operators
- Defense budget changes → Commercial aerospace

**Try it yourself**:
```bash
# Use the GraphRAG chat interface (see Quick Start section above)
python scripts/chat_graphrag.py
```

For more details, see the [GraphRAG documentation](docs/graphrag.md).

---

## Project Structure

```
public-company-graph/
├── public_company_graph/        # Main Python package
│   ├── parsing/                 # 10-K parsing (datamule + custom fallback)
│   │   ├── business_description.py    # Item 1 extraction
│   │   ├── risk_factors.py            # Item 1A extraction
│   │   └── business_relationship_extraction.py  # Competitor/customer/supplier
│   ├── embeddings/              # OpenAI embedding creation
│   ├── gds/                     # Graph Data Science utilities
│   ├── neo4j/                   # Neo4j connection and utilities
│   ├── ingest/                  # Data loading (SQLite → Neo4j)
│   ├── similarity/              # Similarity computation
│   ├── sources/                 # Data source integrations
│   └── utils/                   # Shared utilities (datamule, caching)
├── scripts/                     # Pipeline scripts (see above)
├── tests/                       # Test suite (unit + integration)
├── docs/                        # Documentation
│   ├── graph_schema.md          # Complete schema reference
│   ├── money_queries.md         # High-value Cypher queries + explainable similarity
│   ├── architecture.md          # Package architecture
│   ├── research_enhancements.md # Research-backed feature roadmap
│   └── ...                      # See docs/README.md for full list
└── data/                        # Data files (git-ignored)
    ├── domain_status.db         # Technology detection results
    ├── 10k_filings/             # Downloaded 10-K HTML files
    ├── 10k_portfolios/          # Datamule portfolio files
    └── cache/                   # Embedding and parsing caches
```

---

## Key Dependencies

| Package | Purpose |
|---------|---------|
| [datamule](https://github.com/john-googletv/datamule-python) | SEC 10-K filing download and parsing |
| [neo4j](https://neo4j.com/docs/python-manual/current/) | Neo4j Python driver |
| [graphdatascience](https://neo4j.com/docs/graph-data-science-client/current/) | Neo4j GDS Python client |
| [openai](https://platform.openai.com/docs/libraries/python) | Text embeddings |
| [beautifulsoup4](https://www.crummy.com/software/BeautifulSoup/) | HTML parsing (fallback parser) |
| [yfinance](https://github.com/ranaroussi/yfinance) | Yahoo Finance data |

---

## Documentation

| Document | Description |
|----------|-------------|
| [docs/graph_schema.md](docs/graph_schema.md) | Complete graph schema with all nodes, relationships, and properties |
| [docs/money_queries.md](docs/money_queries.md) | High-value Cypher queries including **explainable similarity** |
| [docs/architecture.md](docs/architecture.md) | Package architecture and design principles |
| [docs/step_by_step_guide.md](docs/step_by_step_guide.md) | Complete pipeline walkthrough |
| [docs/10k_parsing.md](docs/10k_parsing.md) | 10-K parsing pipeline details |
| [docs/research_enhancements.md](docs/research_enhancements.md) | Research-backed feature roadmap |
| [SETUP_GUIDE.md](SETUP_GUIDE.md) | Detailed setup instructions |

---

## Development

```bash
# Run tests
pytest tests/ -v

# Run linter
ruff check public_company_graph/ scripts/

# Format code
ruff format public_company_graph/ scripts/

# Run pre-commit hooks
pre-commit run --all-files
```

---

## Acknowledgments

- **[datamule](https://datamule.xyz)** by John Friedman - SEC filing download and parsing
- **[CompanyKG paper](https://arxiv.org/abs/2306.10649)** - Inspiration for company knowledge graph design
- **[Neo4j](https://neo4j.com)** - Graph database and GDS library
- **[domain_status](https://github.com/alexwoolford/domain_status)** - Rust-based technology detection

---

## License

MIT

## Author

[Alex Woolford](https://github.com/alexwoolford)
