# Datamule Setup & Usage

## Overview

Datamule is a Python library for downloading and parsing SEC EDGAR filings. This project uses it for:
1. **Downloading 10-K filings** (with optional paid API for faster downloads)
2. **Parsing business descriptions** (Item 1: Business) from 10-K filings

## Installation

Datamule is already included in `pyproject.toml`:
```toml
datamule>=0.332.0
```

Install with:
```bash
pip install -e .
```

## Free Usage (SEC Direct)

**Default behavior** (no API key required):
- Downloads directly from SEC EDGAR
- Rate limited to **5 requests/second** (SEC's long-duration limit)
- Free, but slow for bulk downloads (~2-3 hours for all companies)

**Code location**: `scripts/download_10k_filings.py`

## Paid API Service (Optional)

### Benefits

- **No rate limits**: Parallel downloads at full speed
- **Faster**: ~30-60 minutes for all companies (vs 2-3 hours)
- **Cost**: ~$0.001 per file (~$8-10 for all companies)

### Setup

1. **Get API key**: Sign up at [datamule.com](https://datamule.com)
2. **Add to `.env`**:
   ```bash
   DATAMULE_API_KEY=your_api_key_here
   ```
3. **That's it!** The scripts automatically use the API key if present.

### How It Works

The scripts check for `DATAMULE_API_KEY` in your `.env` file:
- **With API key**: Uses `provider="datamule-sgml"` (fast, no rate limits)
- **Without API key**: Falls back to `provider="sec"` (free, rate limited)

**No code changes needed** - just add the API key to `.env`.

## Parsing (No API Key Needed)

**Important**: Parsing does NOT require an API key.

The parsing script (`scripts/parse_10k_filings.py`) uses the `datamule` library to parse existing tar files:
- If tar files exist: Uses datamule parser (best quality, ~86-93% success)
- If no tar files: Uses custom parser (faster, ~64% success)
- **No API calls during parsing** - just reads local files

## Tar File Management

### Download Behavior

- Downloads create tar files in `data/10k_portfolios/10k_{cik}/`
- HTML files are extracted to `data/10k_filings/{cik}/10k_2024.html`
- Tar files are kept by default (use `--keep-tar-files` flag)

### Why Keep Tar Files?

- Datamule parser requires tar files (cannot parse standalone HTML)
- Better parsing quality (~86-93% vs ~64% for custom parser)
- Storage cost: ~225 GB for all tar files

### Idempotent Downloads

- Re-running the download script skips already-downloaded files
- Safe to re-run multiple times
- Use `--force` to re-download everything

## Troubleshooting

### "Successfully loaded 0 submissions"

**Cause**: API key not set correctly, or provider name incorrect.

**Solution**:
- Verify `DATAMULE_API_KEY` in `.env`
- Check that `portfolio.set_api_key(api_key)` is called before `download_submissions()`
- Ensure `provider="datamule-sgml"` is used (not `"datamule"`)

### Slow Downloads

**Without API key**: Expected - SEC rate limits to 5 req/sec.

**With API key**: Should be much faster. Check:
- API key is set in `.env`
- `provider="datamule-sgml"` is used
- No rate limiting parameters are passed

### Parsing Errors

**"No tar files found"**:
- Run `download_10k_filings.py` first
- Use `--keep-tar-files` flag to retain tar files

**"Portfolio initialization slow"**:
- Normal - datamule loads submission index on first use
- Subsequent uses are faster (cached)

## Related Documentation

- **Architecture**: See `docs/PIPELINE_ARCHITECTURE.md`
- **10-K Parsing**: See `docs/10K_PARSING.md` (consolidated)
- **Pipeline Process**: See `docs/COMPLETE_PIPELINE_PROCESS.md`
