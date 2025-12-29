"""
SEC EDGAR pre-check utilities.

Provides functions to check if a company has 10-K filings available
before making expensive API calls. Uses free SEC EDGAR API.

IMPORTANT: This should be run SEQUENTIALLY (not in parallel) to respect
SEC's rate limit of 10 requests per second.

Results are cached to avoid repeating the 14+ minute SEC check on subsequent runs.
"""

import logging
import time
from collections.abc import Iterator

import requests
from tqdm import tqdm

from public_company_graph.cache import get_cache

logger = logging.getLogger(__name__)

# SEC EDGAR rate limit: 10 requests per second
SEC_RATE_LIMIT_DELAY = 0.11  # Slightly over 0.1s to be safe

# Cache settings
CACHE_NAMESPACE = "sec_10k_check"
CACHE_TTL_DAYS = 30  # 10-K filings don't change often


def check_company_has_10k(
    cik: str,
    session: requests.Session | None = None,
    filing_date_start: str = "2020-01-01",
    filing_date_end: str = "2025-01-01",
) -> bool:
    """
    Check if a company has 10-K filings available using free SEC EDGAR API.

    This is a pre-check to avoid making expensive datamule API calls
    for companies that don't have 10-Ks (ETFs, funds, foreign companies, etc.).

    Args:
        cik: Company CIK (10-digit, zero-padded)
        session: Optional requests session (for connection pooling)
        filing_date_start: Start date for filing search (YYYY-MM-DD)
        filing_date_end: End date for filing search (YYYY-MM-DD)

    Returns:
        True if company has 10-K filings in date range, False otherwise
    """
    if session is None:
        session = requests.Session()

    # Ensure CIK is 10-digit zero-padded
    cik_padded = cik.zfill(10)

    # SEC EDGAR Submissions API (free, no authentication required)
    url = f"https://data.sec.gov/submissions/CIK{cik_padded}.json"

    headers = {
        "User-Agent": "public_company_graph script (contact: alexwoolford@example.com)",
        "Accept": "application/json",
    }

    try:
        response = session.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        data = response.json()

        # Check filings array
        filings = data.get("filings", {})
        if not filings:
            return False

        recent = filings.get("recent", {})
        if not recent:
            return False

        forms = recent.get("form", [])
        if not forms:
            return False

        # Check if any 10-K filings exist in date range
        filing_dates = recent.get("filingDate", [])

        for i, form_type in enumerate(forms):
            if form_type == "10-K":
                if i < len(filing_dates):
                    filing_date = filing_dates[i]
                    if filing_date_start <= filing_date <= filing_date_end:
                        return True

        return False

    except requests.exceptions.RequestException as e:
        logger.debug(f"SEC EDGAR check failed for CIK {cik_padded}: {e}")
        # On error, return True to allow datamule to try (fail-safe)
        return True
    except (KeyError, IndexError, ValueError) as e:
        logger.debug(f"SEC EDGAR parse error for CIK {cik_padded}: {e}")
        return True


def filter_companies_with_10k(
    companies: list[dict],
    filing_date_start: str = "2020-01-01",
    filing_date_end: str = "2026-01-01",
    show_progress: bool = True,
    force_refresh: bool = False,
) -> Iterator[dict]:
    """
    Filter companies to only those with 10-K filings.

    Results are CACHED to avoid repeating the 14+ minute SEC check.
    Use force_refresh=True to re-check SEC (e.g., after new filings).

    On first run: checks SEC EDGAR sequentially (~14 min for 8,000 companies)
    On subsequent runs: uses cache (instant)

    Args:
        companies: List of company dicts with 'cik' key
        filing_date_start: Start date for filing search
        filing_date_end: End date for filing search
        show_progress: Whether to show tqdm progress bar
        force_refresh: If True, ignore cache and re-check SEC

    Yields:
        Companies that have 10-K filings
    """
    cache = get_cache()
    cache_key = f"{filing_date_start}_{filing_date_end}"

    # Check if we have cached results
    if not force_refresh:
        cached_ciks = cache.get(CACHE_NAMESPACE, cache_key)
        if cached_ciks is not None:
            cached_set = set(cached_ciks)
            logger.info(
                f"Using cached pre-filter results ({len(cached_set):,} companies with 10-Ks)"
            )

            # Yield companies that are in the cached set
            for company in companies:
                cik = company.get("cik", "")
                if cik in cached_set:
                    yield company
            return

    # No cache or force refresh - check SEC EDGAR
    logger.info("Checking SEC EDGAR for 10-K filings (this will be cached for future runs)")

    session = requests.Session()
    session.headers.update(
        {"User-Agent": "public_company_graph script (contact: alexwoolford@example.com)"}
    )

    has_10k_count = 0
    no_10k_count = 0
    companies_with_10k = []  # Track CIKs for caching

    iterator = tqdm(
        companies,
        desc="Pre-checking for 10-Ks",
        unit="company",
        disable=not show_progress,
    )

    for company in iterator:
        cik = company.get("cik", "")
        ticker = company.get("ticker", "unknown")

        # Rate limit: 10 requests per second
        time.sleep(SEC_RATE_LIMIT_DELAY)

        has_10k = check_company_has_10k(
            cik,
            session=session,
            filing_date_start=filing_date_start,
            filing_date_end=filing_date_end,
        )

        if has_10k:
            has_10k_count += 1
            companies_with_10k.append(cik)
            yield company
        else:
            no_10k_count += 1
            logger.debug(f"Skipping {ticker} (CIK {cik}): No 10-K filings found")

        # Update progress bar with counts
        if show_progress:
            iterator.set_postfix(
                has_10k=has_10k_count,
                no_10k=no_10k_count,
            )

    # Cache the results
    cache.set(CACHE_NAMESPACE, cache_key, companies_with_10k, ttl_days=CACHE_TTL_DAYS)
    logger.info(f"Pre-check complete: {has_10k_count} with 10-Ks, {no_10k_count} without")
    logger.info(f"Results cached for {CACHE_TTL_DAYS} days (use --refresh-filter to re-check)")
