# 10-K Filing Parsing

## Overview

This project extracts structured data from SEC 10-K filings:
- **Business descriptions** (Item 1: Business)
- **Risk factors** (Item 1A: Risk Factors)
- **Business relationships** (competitors, customers, suppliers, partners)
- **Company websites** (from cover page)
- **Filing metadata** (date, CIK, ticker, accession number)

## Architecture

### Hybrid Parsing Approach

We use a **datamule-first, custom-fallback** approach:

1. **Primary**: [datamule](https://datamule.xyz) library (~88% success rate)
2. **Fallback**: Custom BeautifulSoup parser (~12% of cases)

This achieves **99.85% coverage** across all companies.

### Two-Stage Process

1. **Download** (`scripts/download_10k_filings.py`):
   - Downloads 10-K filings from SEC EDGAR via datamule
   - Extracts HTML files from tar archives
   - Stores in `data/10k_filings/{cik}/10k_{year}.html`

2. **Parse** (`scripts/parse_10k_filings.py`):
   - Extracts websites, descriptions, risk factors, metadata
   - Stores in cache (`10k_extracted` namespace)
   - Used by other scripts (load_company_data, extract_business_relationships, etc.)

## Business Description Extraction

### Datamule (Primary)

- Uses `doc.get_section(title='item1', format='text')`
- Best quality extraction
- Works for ~88% of filings

### Custom Parser (Fallback)

When datamule fails, we use a multi-strategy custom parser:

1. **TOC anchor-based navigation** - Follows "Item 1" links in table of contents
2. **Direct ID pattern matching** - Finds elements with ID containing "item1" + "business"
3. **Text node search** - Finds section headings, filters out TOC elements
4. **Raw regex extraction** - For files with unusual structure

**Stop patterns**: Item 1A, Item 1B, Item 1C, Item 2, Item 10, Part II, Risk Factors

### Minimum Length

- **500 characters** minimum for a valid business description
- Filters out TOC entries and short snippets

## Business Relationship Extraction

After parsing, we extract business relationships from the text:

**Script**: `scripts/extract_business_relationships.py`

**What it extracts**:
- Competitors (HAS_COMPETITOR)
- Customers (HAS_CUSTOMER)
- Suppliers (HAS_SUPPLIER)
- Partners (HAS_PARTNER)

**Method**: Entity resolution matching company mentions against SEC CIK database

## Usage

### Download 10-Ks

```bash
# With API key (faster, ~30-60 minutes)
python scripts/download_10k_filings.py --execute

# Without API key (free, slower, ~2-3 hours)
python scripts/download_10k_filings.py --execute
```

### Parse 10-Ks

```bash
# Standard parsing (uses datamule + custom fallback)
python scripts/parse_10k_filings.py --execute

# Force re-parse (overwrite cache)
python scripts/parse_10k_filings.py --execute --force

# Adjust number of parallel workers
python scripts/parse_10k_filings.py --execute --workers 4
```

### Extract Business Relationships

```bash
python scripts/extract_business_relationships.py --execute
```

## Data Quality

### Current Statistics

- **Business description**: ~99.85% success rate (5,390/5,398 companies)
- **Website extraction**: ~98% success rate
- **Business relationships**: ~3,843 competitor, ~2,597 supplier, ~2,139 partner, ~1,714 customer

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
- **Location**: `data/cache/`

### Cache Behavior

- **Idempotent**: Re-running skips already-parsed files
- **Force re-parse**: Use `--force` flag to overwrite cache

### Clearing Cache

```bash
# Clear via CLI
cache clear --namespace 10k_extracted
```

## Related Documentation

- **Datamule Setup**: See `datamule_setup.md`
- **Cache Management**: See `cache_management.md`
- **Adding Parsers**: See `adding_new_parser.md`
