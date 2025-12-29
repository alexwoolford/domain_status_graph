# Step-by-Step Pipeline Guide

This guide walks you through running the complete pipeline safely, step-by-step.

## Prerequisites

1. **Neo4j running** - Database must be accessible
2. **Environment configured** - `.env` file with `NEO4J_URI`, `NEO4J_USER`, `NEO4J_PASSWORD`
3. **Optional**: `DATAMULE_API_KEY` for faster 10-K downloads
4. **Optional**: `OPENAI_API_KEY` for company embeddings

## Step 1: Bootstrap Graph (Required)

**What it does**: Loads Domain and Technology nodes from SQLite into Neo4j

**Command**:
```bash
python scripts/bootstrap_graph.py --execute
```

**Expected output**:
- Domain nodes created
- Technology nodes created
- Relationships established

**Time**: ~5-10 minutes depending on database size

**Check success**:
```bash
# Verify in Neo4j browser
MATCH (d:Domain) RETURN count(d) as domain_count
MATCH (t:Technology) RETURN count(t) as tech_count
```

---

## Step 2: Download 10-K Filings (Optional but Recommended)

**What it does**: Downloads latest 10-K filings for all companies

**IMPORTANT**:
- ✅ **Pre-check enabled**: Script now pre-checks SEC EDGAR (free) before calling datamule API
- ✅ **Saves money**: Only companies with 10-Ks will trigger paid API calls
- ✅ **Tar files protected**: Tar files are ALWAYS kept (no option to delete)
- ✅ **Idempotent**: Safe to re-run - skips already downloaded files

**Command**:
```bash
# Dry-run first to see what will happen
python scripts/download_10k_filings.py

# Then execute
python scripts/download_10k_filings.py --execute
```

**With API key** (faster, ~$8-10 for all companies):
- Pre-check filters out companies without 10-Ks
- Only makes paid API calls for companies that have 10-Ks
- Much faster (~30-60 minutes)

**Without API key** (free, slower):
- Uses SEC direct (free)
- Rate limited to 5 req/sec
- Takes 2-3 hours for all companies

**Output**:
- Tar files: `data/10k_portfolios/{cik}/`
- HTML files: `data/10k_filings/{cik}/`

**Check success**:
```bash
# Count downloaded files
find data/10k_filings -name "*.html" | wc -l

# Check tar files (should match HTML count)
find data/10k_portfolios -name "*.tar" | wc -l
```

**Expected results**:
- ~4,000-5,000 companies with 10-Ks (out of ~6,500 total)
- ~1,500-2,500 companies without 10-Ks (ETFs, funds, foreign, inactive)
- Pre-check prevents wasted API calls for companies without 10-Ks

---

## Step 3: Parse 10-K Filings (Optional)

**What it does**: Extracts websites and business descriptions from HTML files

**Command**:
```bash
# Dry-run first
python scripts/parse_10k_filings.py

# Then execute
python scripts/parse_10k_filings.py --execute
```

**Quality options**:
- **With tar files**: Uses datamule parser (86-93% success rate, full descriptions)
- **Without tar files**: Uses custom parser (64% success rate, 50k char limit)
- **Fast mode**: `--skip-datamule` (custom parser only, faster)

**Output**: Cached data in `10k_extracted` namespace

**Check success**:
```bash
# Check cache stats
python -m public_company_graph.cli cache stats
```

---

## Step 4: Collect Company Domains (Required)

**What it does**: Multi-source domain collection with weighted voting

**Command**:
```bash
python scripts/collect_domains.py
```

**Sources** (in priority order):
1. 10-K data (if available) - highest priority
2. yfinance
3. Finviz
4. SEC EDGAR
5. Finnhub

**Output**: Cached data in `company_domains` namespace

**Check success**:
```bash
# Check cache stats
python -m public_company_graph.cli cache stats
```

---

## Step 5: Load Company Data (Required)

**What it does**: Creates Company nodes and HAS_DOMAIN relationships in Neo4j

**Command**:
```bash
python scripts/load_company_data.py --execute
```

**Output**: Company nodes in Neo4j

**Check success**:
```bash
# Verify in Neo4j browser
MATCH (c:Company) RETURN count(c) as company_count
MATCH (c:Company)-[:HAS_DOMAIN]->(d:Domain) RETURN count(*) as relationships
```

---

## Step 6: Create Company Embeddings (Optional)

**What it does**: Creates OpenAI embeddings for company descriptions

**Requires**: `OPENAI_API_KEY` in `.env`

**Command**:
```bash
python scripts/create_company_embeddings.py --execute
```

**Output**: Embeddings cached in `embeddings` namespace

---

## Step 7: Compute GDS Features (Optional)

**What it does**: Graph Data Science algorithms for adoption prediction

**Command**:
```bash
python scripts/compute_gds_features.py --execute
```

**Output**: GDS features stored on nodes

---

## Data Protection

### Preventing Accidental Data Loss

1. **Tar files are always kept** - No option to delete via CLI
2. **Directories in .gitignore** - But `.gitkeep` files preserve structure
3. **Cache is persistent** - Survives script re-runs
4. **Idempotent scripts** - Safe to re-run (skips existing data)

### Manual Cleanup (If Needed)

If you really need to clean up:
```bash
# View what would be deleted (dry-run)
python scripts/cleanup_10k_data.py

# Actually delete (use with caution)
python scripts/cleanup_10k_data.py --execute
```

---

## Troubleshooting

### "No 10-K found" for many companies
- **Expected**: Many companies (ETFs, funds, foreign) don't file 10-Ks
- **Pre-check prevents wasted API calls**: Script now checks SEC EDGAR first
- **Check logs**: Look for "pre-checked via SEC EDGAR" messages

### API costs higher than expected
- **Check pre-check is working**: Look for "pre-checked via SEC EDGAR" in logs
- **Verify API key**: Make sure `DATAMULE_API_KEY` is set correctly
- **Check for errors**: Failed downloads might retry (though max_retries=1)

### Data missing after script runs
- **Check .gitignore**: Data directories are ignored (as intended)
- **Check disk space**: Tar files can be large
- **Check logs**: Look for errors in `logs/` directory
