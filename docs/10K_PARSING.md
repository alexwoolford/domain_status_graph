# 10-K Filing Parsing

## Overview

This project extracts structured data from SEC 10-K filings:
- **Company websites** (from cover page)
- **Business descriptions** (Item 1: Business)
- **Filing metadata** (date, CIK, ticker, etc.)

## Architecture

### Two-Stage Process

1. **Download** (`scripts/download_10k_filings.py`):
   - Downloads 10-K filings from SEC EDGAR
   - Extracts HTML files from tar archives
   - Stores in `data/10k_filings/{cik}/10k_2024.html`

2. **Parse** (`scripts/parse_10k_filings.py`):
   - Extracts websites, descriptions, metadata
   - Stores in cache (`10k_extracted` namespace)
   - Used by other scripts (collect_domains, load_company_data, etc.)

## Website Extraction

### Priority Order

1. **Official iXBRL element** (`dei:EntityWebSite`) - SEC-mandated, most reliable
2. **XML structured data** - For XML filings
3. **Heuristic extraction** - Only if official methods fail, with strict validation

### Validation

All extracted domains are validated using:
- `tldextract` with Public Suffix List (authoritative TLD validation)
- Rejects known taxonomy domains (xbrl.org, sec.gov, etc.)
- Rejects very long TLDs (>15 chars, likely extraction errors)

**Module**: `domain_status_graph/domain/validation.py`

## Business Description Extraction

### Hybrid Approach

**Primary**: Datamule parser (if tar files exist)
- Best quality (~86-93% success rate)
- Full descriptions (no 50k character cap)
- Requires tar files in `data/10k_portfolios/`

**Fallback**: Custom parser (if no tar files)
- Faster (~64% success rate)
- Works with HTML files only
- 50k character limit

### Extraction Strategy

1. **TOC anchor-based navigation** - Follows "Item 1" links in table of contents
2. **Text-node search** - Finds section headings, filters out TOC elements
3. **Raw text fallback** - For files with unusual structure

## Usage

### Download 10-Ks

```bash
# Free (SEC direct, slow)
python scripts/download_10k_filings.py --execute

# Fast (with API key, see DATAMULE_SETUP.md)
python scripts/download_10k_filings.py --execute --keep-tar-files
```

### Parse 10-Ks

```bash
# Standard parsing (uses datamule if tar files exist)
python scripts/parse_10k_filings.py --execute

# Fast mode (skip datamule, custom parser only)
python scripts/parse_10k_filings.py --execute --skip-datamule

# Force re-parse (overwrite cache)
python scripts/parse_10k_filings.py --execute --force

# Adjust number of parallel workers
python scripts/parse_10k_filings.py --execute --workers 4
```

### Export Domains

```bash
# Export parsed domains to text file (for domain_status tool)
python scripts/export_10k_domains.py --execute
```

## Data Quality

### Current Statistics

- **Website extraction**: ~98.4% success rate (5,037/5,118 companies)
- **Business description**: ~70.8% success rate (3,626/5,118 companies)
- **Total parsed**: 5,118 companies

### Inspection

```bash
# View sample data
python scripts/inspect_10k_parsed_data.py --samples 10

# View statistics
python scripts/inspect_10k_parsed_data.py --stats
```

## Cache Management

### Cache Location

- **Namespace**: `10k_extracted`
- **Storage**: `diskcache` (SQLite-based)
- **Location**: `data/.cache/`

### Cache Behavior

- **Idempotent**: Re-running skips already-parsed files
- **Force re-parse**: Use `--force` flag to overwrite cache
- **Fast**: Parallel processing for efficient parsing

### Clearing Cache

```python
from domain_status_graph.cache import get_cache

cache = get_cache()
cache.clear("10k_extracted")  # Clear all 10-K data
```

## Integration

### Used By

1. **`collect_domains.py`**: Checks 10-K cache first for company websites
2. **`load_company_data.py`**: Uses 10-K websites and descriptions for Company nodes
3. **`create_company_embeddings.py`**: Uses 10-K descriptions for embeddings

### Priority

10-K data takes priority over other sources (yfinance, Finviz, etc.) when available.

## Related Documentation

- **Datamule Setup**: See `docs/DATAMULE_SETUP.md`
- **Pipeline Architecture**: See `docs/PIPELINE_ARCHITECTURE.md`
- **Domain Validation**: See `domain_status_graph/domain/validation.py`
