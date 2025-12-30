# Quick Start

## TL;DR

The Public Company Graph is a knowledge graph of **5,398 U.S. public companies** with:
- Business relationships extracted from SEC 10-K filings (competitors, customers, suppliers, partners)
- Company similarity across 5 dimensions (description, industry, size, risk, technology)
- Technology detection on company websites (827 technologies)
- Graph analytics via Neo4j GDS

## What Can You Do With It?

### 1. Find Similar Companies

```cypher
MATCH (apple:Company {ticker: 'AAPL'})-[r:SIMILAR_DESCRIPTION]->(similar:Company)
WHERE r.score > 0.85
RETURN similar.ticker, similar.name, similar.sector, r.score
ORDER BY r.score DESC LIMIT 10
```

### 2. Map Competitive Landscape

```cypher
MATCH (c:Company {ticker: 'NVDA'})-[r:HAS_COMPETITOR]->(comp:Company)
RETURN comp.ticker, comp.name, r.raw_mention, r.confidence
ORDER BY r.confidence DESC
```

### 3. Explore Supply Chains

```cypher
MATCH (c:Company {ticker: 'TSLA'})
OPTIONAL MATCH (c)-[:HAS_SUPPLIER]->(supp:Company)
OPTIONAL MATCH (c)-[:HAS_CUSTOMER]->(cust:Company)
RETURN c.name, collect(DISTINCT supp.name) as suppliers, collect(DISTINCT cust.name) as customers
```

### 4. Technology Adoption Prediction

```cypher
MATCH (c:Company {ticker:'MSFT'})-[:HAS_DOMAIN]->(d:Domain)
MATCH (d)-[r:LIKELY_TO_ADOPT]->(t:Technology)
WHERE NOT (d)-[:USES]->(t)
RETURN t.name, t.category, r.score ORDER BY r.score DESC LIMIT 10
```

## How to Run

### Option A: Full Pipeline

```bash
python scripts/run_all_pipelines.py --execute
```

### Option B: Step-by-Step

```bash
# 1. Bootstrap domain/technology graph
python scripts/bootstrap_graph.py --execute

# 2. Download and parse 10-K filings
python scripts/download_10k_filings.py --execute
python scripts/parse_10k_filings.py --execute

# 3. Load companies
python scripts/load_company_data.py --execute

# 4. Extract business relationships
python scripts/extract_business_relationships.py --execute

# 5. Create embeddings and compute similarity
python scripts/create_company_embeddings.py --execute
python scripts/compute_company_similarity.py --execute

# 6. Compute GDS features
python scripts/compute_gds_features.py --execute
```

## Prerequisites

1. **Neo4j** with GDS plugin installed
2. **API Keys**: OpenAI (embeddings), Datamule (10-K downloads)
3. **domain_status.db**: Run [domain_status](https://github.com/alexwoolford/domain_status) first for technology detection

## What's in the Graph?

| Component | Count |
|-----------|-------|
| Company nodes | 5,398 |
| Domain nodes | 4,337 |
| Technology nodes | 827 |
| Total relationships | ~2M |

## Next Steps

- **[README.md](README.md)** - Full project overview
- **[docs/money_queries.md](docs/money_queries.md)** - High-value query examples
- **[docs/graph_schema.md](docs/graph_schema.md)** - Complete schema reference
- **[SETUP_GUIDE.md](SETUP_GUIDE.md)** - Detailed setup instructions
