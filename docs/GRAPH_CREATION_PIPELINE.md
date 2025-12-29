# Graph Creation Pipeline: Embeddings & Similarity

## Overview

This document outlines the **clean, repeatable process** for creating embeddings and similarity relationships for the knowledge graph.

**Goal**: Create a "machine" that finds the most similar companies given a company (e.g., PEP ~ KO, Home Depot ~ Lowes).

---

## Pipeline Steps

### Step 1: Load 10-K Business Descriptions into Company Nodes

**Script**: `scripts/load_company_data.py`

**What it does**:
- Reads 10-K extracted data from cache (`10k_extracted` namespace)
- Updates Company nodes with `business_description_10k` property
- Creates/updates Company nodes with CIK, ticker, name, domain
- Creates `HAS_DOMAIN` relationships

**Status**: Already handles 10-K descriptions (lines 98-109)

**Command**:
```bash
python scripts/load_company_data.py --execute
```

**Expected output**: ~4,929 companies with `business_description_10k` property

---

### Step 2: Create Embeddings for Company Descriptions

**Script**: `scripts/create_company_embeddings.py`

**What it does**:
1. Updates Company nodes with 10-K descriptions (if available)
2. Creates embeddings using OpenAI `text-embedding-3-small` (1536 dimensions)
3. Stores embeddings in `description_embedding` property
4. Uses unified cache to avoid re-computation

**Command**:
```bash
python scripts/create_company_embeddings.py --execute
```

**Expected output**: ~4,929 companies with `description_embedding` property

**Note**: Script already prioritizes 10-K descriptions over regular descriptions (lines 117-128)

---

### Step 3: Create SIMILAR_DESCRIPTION Relationships Between Companies

**Script**: `scripts/compute_company_similarity.py` OR `domain_status_graph/gds/company_similarity.py`

**What it does**:
1. Loads all Company nodes with `description_embedding`
2. Computes pairwise cosine similarity using NumPy
3. Creates `SIMILAR_DESCRIPTION` relationships for top-k most similar companies
4. Only creates relationships above similarity threshold (default: 0.7)
5. Top-k per company (default: 50)

**Command**:
```bash
python scripts/compute_company_similarity.py --execute
# OR use the GDS module:
python -c "from domain_status_graph.gds.company_similarity import compute_company_description_similarity; from domain_status_graph.cli import get_driver_and_database, setup_logging; logger = setup_logging('company_sim'); driver, db = get_driver_and_database(logger); compute_company_description_similarity(driver, execute=True, database=db, logger=logger)"
```

**Expected output**: ~12,000-15,000 `SIMILAR_DESCRIPTION` relationships (top 50 per company, above 0.7 threshold)

**Relationship**: `Company-[:SIMILAR_DESCRIPTION {score, metric, computed_at}]->Company`

---

### Step 4: Create Embeddings for Domain Descriptions

**Script**: `scripts/create_domain_embeddings.py`

**What it does**:
1. Loads Domain nodes with `description` property
2. Creates embeddings using OpenAI `text-embedding-3-small`
3. Stores embeddings in `description_embedding` property
4. Uses unified cache to avoid re-computation

**Command**:
```bash
python scripts/create_domain_embeddings.py --execute
```

**Expected output**: Domains with `description_embedding` property

---

### Step 5: Create SIMILAR_DESCRIPTION Relationships Between Domains

**Script**: `scripts/compute_domain_similarity.py`

**What it does**:
1. Loads all Domain nodes with `description_embedding`
2. Computes pairwise cosine similarity using NumPy
3. Creates `SIMILAR_DESCRIPTION` relationships for top-k most similar domains
4. Only creates relationships above similarity threshold (default: 0.7)
5. Top-k per domain (default: 50)

**Command**:
```bash
python scripts/compute_domain_similarity.py --execute
```

**Expected output**: `SIMILAR_DESCRIPTION` relationships between domains

**Relationship**: `Domain-[:SIMILAR_DESCRIPTION {score, metric, computed_at}]->Domain`

---

## Complete Pipeline Script

**Script**: `scripts/run_all_pipelines.py` (may need updates)

**Or create a new script**: `scripts/create_similarity_graph.py`

---

## Testing: Verify Similarity Queries

### Test Query 1: PEP ~ KO (PepsiCo ~ Coca-Cola)

```cypher
// Find companies similar to PepsiCo
MATCH (c1:Company {ticker: 'PEP'})-[r:SIMILAR_DESCRIPTION]->(c2:Company)
WHERE c2.ticker = 'KO'
RETURN c1.name AS company1, c2.name AS company2, r.score AS similarity
```

**Expected**: High similarity score (>0.8)

### Test Query 2: Home Depot ~ Lowes

```cypher
// Find companies similar to Home Depot
MATCH (c1:Company {ticker: 'HD'})-[r:SIMILAR_DESCRIPTION]->(c2:Company)
WHERE c2.ticker = 'LOW'
RETURN c1.name AS company1, c2.name AS company2, r.score AS similarity
```

**Expected**: High similarity score (>0.8)

### Test Query 3: Top Similar Companies

```cypher
// Find top 20 most similar companies to a given company
MATCH (c1:Company {ticker: 'PEP'})-[r:SIMILAR_DESCRIPTION]->(c2:Company)
RETURN c2.name AS similar_company, c2.ticker AS ticker, r.score AS similarity
ORDER BY r.score DESC
LIMIT 20
```

---

## Configuration

### Similarity Threshold

**Default**: 0.7 (cosine similarity)

**Location**: `domain_status_graph/constants.py` → `DEFAULT_SIMILARITY_THRESHOLD`

**Rationale**:
- 0.7 = moderate similarity (filters noise)
- 0.8 = high similarity (very similar companies)
- 0.6 = lower threshold (more relationships, more noise)

### Top-K Per Company

**Default**: 50

**Location**: `domain_status_graph/constants.py` → `DEFAULT_TOP_K`

**Rationale**:
- 50 = good balance (not too sparse, not too dense)
- 100 = more relationships (slower queries)
- 20 = fewer relationships (faster queries, might miss some)

### Embedding Model

**Default**: `text-embedding-3-small` (1536 dimensions)

**Location**: `domain_status_graph/embeddings/__init__.py` → `EMBEDDING_MODEL`

**Rationale**:
- Good balance of quality and cost
- 1536 dimensions = sufficient for semantic similarity
- Alternative: `text-embedding-3-large` (3072 dimensions, higher cost)

---

## Repeatability

### Idempotent Operations

All scripts are **idempotent** (safe to run multiple times):

1. **Load Company Data**: Uses `MERGE` - updates existing nodes
2. **Create Embeddings**: Uses cache - skips already-computed embeddings
3. **Create Similarity Relationships**:
   - Deletes existing relationships first (idempotent)
   - Creates new relationships

### Cache Management

- **Embeddings cache**: `embeddings` namespace in unified cache
- **TTL**: Long-lived (expensive to regenerate)
- **Key format**: `{cik}:{property_name}` (e.g., `0001325964:business_description_10k`)

---

## Troubleshooting

### Issue: No embeddings created

**Check**:
1. Do Company nodes have `business_description_10k` property?
2. Is OpenAI API key set? (`OPENAI_API_KEY` environment variable)
3. Check logs for errors

### Issue: No similarity relationships

**Check**:
1. Do Company nodes have `description_embedding` property?
2. Are there at least 2 companies with embeddings?
3. Check similarity threshold (might be too high)

### Issue: Wrong similar companies

**Check**:
1. Similarity threshold (might be too low → noise)
2. Top-k (might be too high → includes less similar companies)
3. Embedding quality (10-K descriptions are better than short descriptions)

---

## Next Steps

1. ✅ Load 10-K descriptions into Company nodes
2. ✅ Create embeddings for Company nodes
3. ✅ Create SIMILAR_DESCRIPTION relationships between Companies
4. ✅ Create embeddings for Domain nodes (if needed)
5. ✅ Create SIMILAR_DESCRIPTION relationships between Domains
6. ✅ Test similarity queries (PEP ~ KO, HD ~ LOW)

---

## References

- **Graph Schema**: `docs/graph_schema.md`
- **GDS Features**: `docs/gds_features.md`
- **Similarity Utilities**: `domain_status_graph/similarity/cosine.py`
- **Company Queries**: `domain_status_graph/company/queries.py`
