# Public Company Graph

A knowledge graph of U.S. public companies built from SEC 10-K filings, combining structured data extraction with graph analytics. Designed to showcase Neo4j Graph Data Science (GDS) capabilities for business intelligence and investment analysis.

## What This Project Does

**Extracts structured intelligence from SEC filings**:
- Company websites, business descriptions, and risk factors from 10-K filings
- Competitor mentions and business relationships
- Technology stack detection from company websites

**Builds a knowledge graph** connecting:
- ~8,000+ public companies with their properties
- Technologies they use
- Similarity relationships (companies similar to each other)
- Competitor relationships extracted from filings

**Computes graph analytics** using Neo4j GDS:
- Company similarity via embeddings (business description vectors)
- Technology adoption prediction via Personalized PageRank
- Technology co-occurrence patterns via Node Similarity

## Why Graph?

Traditional relational approaches struggle with questions like:
- "Which companies are similar to Apple based on their business model AND technology stack?"
- "Find companies 2-3 hops away that might be acquisition targets"
- "What technologies commonly appear together across public companies?"

Graph excels at these multi-dimensional, relationship-driven queries.

## Quick Start

### Prerequisites

- **Neo4j** (5.x+) with GDS library installed
- **Python 3.11+**
- **OpenAI API key** (for embeddings)

### Setup

```bash
git clone https://github.com/alexwoolford/public-company-graph.git
cd public-company-graph
pip install -e .
cp .env.sample .env
# Edit .env with your Neo4j and OpenAI credentials
```

### Run the Pipeline

```bash
# Bootstrap graph from existing data
python scripts/bootstrap_graph.py --execute

# Or run the full pipeline (download 10-Ks, parse, load, compute)
python scripts/run_all_pipelines.py --execute
```

### CLI Commands

| Command | Description |
|---------|-------------|
| `run-all-pipelines` | Full pipeline: download, parse, load, compute |
| `bootstrap-graph` | Load data from SQLite into Neo4j |
| `compute-gds-features` | Compute GDS analytics (similarity, adoption) |
| `health-check` | Verify Neo4j connection and data |

## Data Sources

| Source | What We Extract |
|--------|-----------------|
| **SEC EDGAR** | 10-K filings (business descriptions, risk factors, competitors) |
| **Yahoo Finance** | Company metadata (sector, industry, market cap) |
| **FinHub/FinViz** | Additional company data |
| **domain_status** | Technology fingerprints from company websites |

## Graph Schema

```
(:Company)-[:SIMILAR_TO {score}]->(:Company)
(:Company)-[:USES]->(:Technology)
(:Company)-[:COMPETES_WITH]->(:Company)
(:Technology)-[:CO_OCCURS_WITH {similarity}]->(:Technology)
(:Domain)-[:LIKELY_TO_ADOPT {score}]->(:Technology)
```

## Example Queries

### Find companies similar to Apple
```cypher
MATCH (apple:Company {ticker: 'AAPL'})-[r:SIMILAR_TO]->(similar:Company)
RETURN similar.name, similar.ticker, r.score
ORDER BY r.score DESC
LIMIT 10
```

### Find technology adoption candidates
```cypher
MATCH (t:Technology {name: 'Kubernetes'})<-[r:LIKELY_TO_ADOPT]-(d:Domain)
RETURN d.final_domain, r.score
ORDER BY r.score DESC
LIMIT 20
```

### Find competitor clusters
```cypher
MATCH (c:Company)-[:COMPETES_WITH]-(competitor:Company)
WHERE c.sector = 'Technology'
RETURN c.name, collect(competitor.name) AS competitors
LIMIT 10
```

## Project Structure

```
public-company-graph/
├── public_company_graph/     # Python package
│   ├── parsing/              # 10-K filing parsers
│   ├── sources/              # Data source integrations
│   ├── embeddings/           # Embedding creation
│   ├── gds/                  # Graph Data Science features
│   ├── neo4j/                # Neo4j utilities
│   └── ingest/               # Data loading
├── scripts/                  # Pipeline scripts
├── tests/                    # Test suite
├── docs/                     # Documentation
└── data/                     # Data files (git-ignored)
```

## Documentation

- **[SETUP_GUIDE.md](SETUP_GUIDE.md)** - Complete setup instructions
- **[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)** - Package architecture
- **[docs/10K_PARSING.md](docs/10K_PARSING.md)** - 10-K parsing pipeline
- **[docs/money_queries.md](docs/money_queries.md)** - High-value Cypher queries
- **[docs/graph_schema.md](docs/graph_schema.md)** - Complete schema reference

## Requirements

- Python 3.11+ (tested on 3.11, 3.12, 3.13)
- Neo4j 5.x+ with GDS library
- OpenAI API access (for embeddings)

## License

MIT

## Author

Built as a showcase of graph analytics for business intelligence.
