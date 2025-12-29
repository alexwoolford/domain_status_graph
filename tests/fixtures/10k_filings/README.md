# Test Fixtures: 10-K Filings

This directory contains sample 10-K HTML files for integration tests.

## Purpose

These files allow tests to run immediately after cloning the repository, without requiring:
- Running `download_10k_filings.py`
- Having access to the full `data/10k_filings/` directory
- External API calls or downloads

## File Selection Criteria

Test fixtures should:
- ✅ Be from recent years (2020-2024)
- ✅ Have successfully parsed risk factors
- ✅ Have successfully parsed business descriptions
- ✅ Represent different industries (tech, consumer, finance)
- ✅ Be from well-known companies (easier to verify)
- ✅ Be reasonably sized (< 10 MB each)

## Current Fixtures

*To be added: Select 2-3 good test files from recent years*

## Adding New Test Files

1. **Select a good file** from `data/10k_filings/{CIK}/10k_{YEAR}.html`
2. **Verify it parses correctly**:
   ```bash
   python -c "from public_company_graph.parsing.risk_factors import extract_risk_factors; \
              from pathlib import Path; \
              result = extract_risk_factors(Path('data/10k_filings/{CIK}/10k_{YEAR}.html')); \
              print(f'Risk factors length: {len(result) if result else 0}')"
   ```
3. **Copy to fixtures**:
   ```bash
   mkdir -p tests/fixtures/10k_filings/{CIK}
   cp data/10k_filings/{CIK}/10k_{YEAR}.html tests/fixtures/10k_filings/{CIK}/
   ```
4. **Update tests** if needed (tests should auto-discover files)
5. **Commit to Git** (these files are tracked in the repo)

## File Structure

```
tests/fixtures/10k_filings/
  {CIK}/              # Company CIK (10-digit, zero-padded)
    10k_{YEAR}.html   # 10-K filing HTML file
```

## Usage in Tests

Tests should use the fixtures directory with fallback to data directory:

```python
@pytest.fixture
def filings_dir(self):
    """Get the 10-K filings directory (fixtures first, then data)."""
    fixtures_dir = Path(__file__).parent.parent / "fixtures" / "10k_filings"
    data_dir = get_data_dir() / "10k_filings"

    # Prefer fixtures, fallback to data directory
    if fixtures_dir.exists() and any(fixtures_dir.glob("**/*.html")):
        return fixtures_dir
    return data_dir
```

## Copyright

10-K filings are public SEC documents with no copyright restrictions. Safe to include in repository.
