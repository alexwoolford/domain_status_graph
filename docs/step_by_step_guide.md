# Step-by-Step Pipeline Guide

This guide walks you through running the complete pipeline safely, step-by-step.

## Prerequisites

1. **Neo4j running** - Database must be accessible with GDS plugin installed
2. **Environment configured** - `.env` file with credentials (see `.env.sample`)
3. **Required**: `OPENAI_API_KEY` for company embeddings
4. **Required**: `DATAMULE_API_KEY` for 10-K downloads (or use free SEC direct)

## Pipeline Overview

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        COMPLETE PIPELINE                                │
├─────────────────────────────────────────────────────────────────────────┤
│  1. Bootstrap Graph (Domain + Technology from domain_status.db)         │
│  2. Download 10-K Filings (via datamule)                               │
│  3. Parse 10-K Filings (business descriptions, metadata)               │
│  4. Load Company Data (Company nodes + HAS_DOMAIN)                     │
│  5. Enrich Company Properties (Yahoo Finance data)                     │
│  6. Extract Business Relationships (competitors, customers, etc.)      │
│  7. Create Company Embeddings (OpenAI)                                 │
│  8. Compute Similarity Relationships (description, industry, size...)  │
│  9. Compute GDS Features (LIKELY_TO_ADOPT, CO_OCCURS_WITH)            │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Step 1: Bootstrap Graph

**What it does**: Loads Domain and Technology nodes from `domain_status.db` into Neo4j

**Prerequisite**: Run [domain_status](https://github.com/alexwoolford/domain_status) tool first to generate `data/domain_status.db`

```bash
python scripts/bootstrap_graph.py --execute
```

**Expected output**:
- ~4,337 Domain nodes
- ~827 Technology nodes
- ~46,081 USES relationships

---

## Step 2: Download 10-K Filings

**What it does**: Downloads 10-K filings for all public companies

```bash
# Dry-run first
python scripts/download_10k_filings.py

# Execute
python scripts/download_10k_filings.py --execute
```

**With datamule API key** (faster):
- ~30-60 minutes for all companies
- Cost: ~$8-10

**Without API key** (free, slower):
- ~2-3 hours (SEC rate limited)

---

## Step 3: Parse 10-K Filings

**What it does**: Extracts business descriptions, websites, metadata from HTML files

```bash
python scripts/parse_10k_filings.py --execute
```

**Expected output**: ~99.85% success rate for business descriptions

---

## Step 4: Load Company Data

**What it does**: Creates Company nodes and HAS_DOMAIN relationships

```bash
python scripts/load_company_data.py --execute
```

**Expected output**:
- ~5,398 Company nodes
- ~3,745 HAS_DOMAIN relationships

---

## Step 5: Enrich Company Properties

**What it does**: Adds sector, industry, market cap, revenue, employees from Yahoo Finance

```bash
python scripts/enrich_company_properties.py --execute
```

---

## Step 6: Extract Business Relationships

**What it does**: Extracts competitors, customers, suppliers, partners from 10-K text

```bash
python scripts/extract_with_llm_verification.py --clean --execute
```

**Expected output**:
- ~3,843 HAS_COMPETITOR relationships
- ~2,597 HAS_SUPPLIER relationships
- ~2,139 HAS_PARTNER relationships
- ~1,714 HAS_CUSTOMER relationships

---

## Step 7: Create Company Embeddings

**What it does**: Creates OpenAI embeddings for business descriptions

**Requires**: `OPENAI_API_KEY` in `.env`

```bash
python scripts/create_company_embeddings.py --execute
```

---

## Step 8: Compute Similarity Relationships

**What it does**: Computes all similarity relationships between companies

```bash
python scripts/compute_company_similarity.py --execute
```

**Expected output**:
- ~420,531 SIMILAR_DESCRIPTION relationships
- ~520,672 SIMILAR_INDUSTRY relationships
- ~414,096 SIMILAR_SIZE relationships
- ~394,372 SIMILAR_RISK relationships
- ~124,584 SIMILAR_TECHNOLOGY relationships

---

## Step 9: Compute GDS Features

**What it does**: Technology adoption prediction and co-occurrence analysis

```bash
python scripts/compute_gds_features.py --execute
```

**Expected output**:
- ~41,250 LIKELY_TO_ADOPT relationships
- ~41,220 CO_OCCURS_WITH relationships

---

## All-in-One Command

To run the complete pipeline:

```bash
python scripts/run_all_pipelines.py --execute
```

---

## Verification Queries

### Check node counts

```cypher
MATCH (c:Company) RETURN 'Company' as label, count(c) as count
UNION ALL
MATCH (d:Domain) RETURN 'Domain', count(d)
UNION ALL
MATCH (t:Technology) RETURN 'Technology', count(t)
```

### Check relationship counts

```cypher
// Check specific relationship counts
MATCH ()-[r:SIMILAR_DESCRIPTION]->() RETURN 'SIMILAR_DESCRIPTION' as type, count(r) as count
UNION ALL
MATCH ()-[r:SIMILAR_INDUSTRY]->() RETURN 'SIMILAR_INDUSTRY', count(r)
UNION ALL
MATCH ()-[r:HAS_COMPETITOR]->() RETURN 'HAS_COMPETITOR', count(r)
UNION ALL
MATCH ()-[r:USES]->() RETURN 'USES', count(r)
```

### Test famous competitor pairs

```cypher
// PepsiCo vs Coca-Cola
MATCH (pep:Company {ticker:'PEP'})-[r:HAS_COMPETITOR]->(ko:Company {ticker:'KO'})
RETURN pep.name, ko.name, r.confidence

// Home Depot vs Lowes
MATCH (hd:Company {ticker:'HD'})-[r:SIMILAR_DESCRIPTION]->(low:Company {ticker:'LOW'})
RETURN hd.name, low.name, r.score
```

---

## Troubleshooting

### "No 10-K found" for many companies

**Expected**: Many entities (ETFs, funds, foreign companies) don't file 10-Ks

### Embeddings not created

**Check**: Is `OPENAI_API_KEY` set in `.env`?

### Similarity relationships missing

**Check**:
1. Do Company nodes have `description_embedding` property?
2. Run `compute_company_similarity.py` after creating embeddings

---

## Related Documentation

- [10k_parsing.md](./10k_parsing.md) - Parsing details
- [cache_management.md](./cache_management.md) - Cache troubleshooting
- [money_queries.md](./money_queries.md) - Query examples
