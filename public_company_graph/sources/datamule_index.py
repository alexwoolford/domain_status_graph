"""
Datamule Index utilities for bulk SEC filing queries.

Uses Datamule's Index.search_submissions() to efficiently identify all companies
with specific filing types (e.g., 10-K) in a single bulk query.

This is MUCH faster than the SEC EDGAR API pre-check approach:
- Old approach: ~14 minutes to check 8,000 companies one-by-one (SEC rate-limited)
- New approach: ~60-90 seconds to get all ~10,000 companies with 10-Ks in bulk

The search uses Datamule's indexed database, not the SEC API directly,
so it does NOT consume Datamule API credits and is not rate-limited.

NOTE: The Datamule Index is over-inclusive - it may return companies that have
10-K documents indexed but no downloadable files. We maintain a separate cache
of "known bad" CIKs to avoid wasting credits on repeated failed downloads.
"""

import logging
import os
from collections.abc import Iterator

from public_company_graph.cache import get_cache
from public_company_graph.config import get_datamule_api_key

logger = logging.getLogger(__name__)

# Cache settings
CACHE_NAMESPACE = "datamule_10k_index"
CACHE_NAMESPACE_NO_10K = "datamule_no_10k"  # CIKs with no downloadable 10-K
CACHE_TTL_DAYS = 7  # Refresh weekly to catch new filers
CACHE_TTL_NO_10K_DAYS = 90  # Keep "no 10-K" cache longer (unlikely to change)

# Try to import datamule
try:
    from datamule import Index

    DATAMULE_INDEX_AVAILABLE = True
except ImportError:
    DATAMULE_INDEX_AVAILABLE = False
    Index = None  # type: ignore


def get_all_ciks_with_10k(
    filing_date_start: str = "2020-01-01",
    filing_date_end: str = "2026-01-01",
    requests_per_second: float = 10.0,
    force_refresh: bool = False,
) -> set[str]:
    """
    Get all CIKs that have filed 10-K forms using Datamule's bulk index search.

    This is much faster than checking each company individually via SEC EDGAR API.
    Results are cached to avoid repeating the search unnecessarily.

    Args:
        filing_date_start: Start date for filing search (YYYY-MM-DD)
        filing_date_end: End date for filing search (YYYY-MM-DD)
        requests_per_second: Rate limit for the search (default: 10.0)
        force_refresh: If True, ignore cache and re-query Datamule

    Returns:
        Set of CIK strings (10-digit, zero-padded) that have 10-K filings
    """
    if not DATAMULE_INDEX_AVAILABLE:
        logger.warning("datamule not installed, cannot use index search")
        return set()

    cache = get_cache()
    cache_key = f"10k_ciks_{filing_date_start}_{filing_date_end}"

    # Check cache first
    if not force_refresh:
        cached_ciks = cache.get(CACHE_NAMESPACE, cache_key)
        if cached_ciks is not None:
            logger.info(
                f"Using cached Datamule index results ({len(cached_ciks):,} companies with 10-Ks)"
            )
            return set(cached_ciks)

    # Set up API key if available (may help with search, though search itself is free)
    key = get_datamule_api_key()
    if key:
        os.environ["DATAMULE_API_KEY"] = key

    logger.info(
        f"Querying Datamule index for 10-K filers ({filing_date_start} to {filing_date_end})..."
    )
    logger.info("This may take 60-90 seconds on first run (results will be cached)")

    try:
        idx = Index()
        results = idx.search_submissions(
            submission_type="10-K",
            filing_date=(filing_date_start, filing_date_end),
            requests_per_second=requests_per_second,
            quiet=True,
        )

        # Extract unique CIKs from results
        ciks: set[str] = set()
        for hit in results:
            source = hit.get("_source", {})
            hit_ciks = source.get("ciks", [])
            for cik in hit_ciks:
                # Normalize to 10-digit zero-padded format
                ciks.add(str(cik).zfill(10))

        logger.info(f"Found {len(ciks):,} unique companies with 10-K filings")

        # Cache the results
        cache.set(CACHE_NAMESPACE, cache_key, list(ciks), ttl_days=CACHE_TTL_DAYS)
        logger.info(f"Results cached for {CACHE_TTL_DAYS} days")

        return ciks

    except Exception as e:
        logger.error(
            f"Datamule index search failed: {e}. "
            f"Date range: {filing_date_start} to {filing_date_end}. "
            f"Check network connectivity and Datamule availability. "
            f"Falling back to unfiltered company list (may waste credits)."
        )
        return set()


def get_ciks_without_10k() -> set[str]:
    """
    Get CIKs that are known to have no downloadable 10-K files.

    These are companies where the Datamule Index says they have 10-Ks,
    but actual download attempts failed. Caching these prevents repeated
    wasted API credits.

    Returns:
        Set of CIK strings (10-digit, zero-padded) with no downloadable 10-Ks
    """
    cache = get_cache()
    cached = cache.get(CACHE_NAMESPACE_NO_10K, "ciks")
    if cached is not None:
        return set(cached)
    return set()


def mark_cik_no_10k_available(cik: str) -> None:
    """
    Mark a CIK as having no downloadable 10-K files.

    Called when a download attempt fails with "No 10-K found".
    Subsequent runs will skip this CIK to avoid wasting credits.

    Args:
        cik: CIK string (will be normalized to 10-digit zero-padded)
    """
    cik_normalized = str(cik).zfill(10)
    cache = get_cache()

    # Get existing set
    existing = cache.get(CACHE_NAMESPACE_NO_10K, "ciks")
    if existing is None:
        existing = []

    # Add new CIK if not already present
    existing_set = set(existing)
    if cik_normalized not in existing_set:
        existing_set.add(cik_normalized)
        cache.set(
            CACHE_NAMESPACE_NO_10K, "ciks", list(existing_set), ttl_days=CACHE_TTL_NO_10K_DAYS
        )


def clear_no_10k_cache() -> int:
    """
    Clear the cache of CIKs with no downloadable 10-K.

    Use this if Datamule adds new files and you want to retry previously failed CIKs.

    Returns:
        Number of entries cleared
    """
    cache = get_cache()
    count = cache.count(CACHE_NAMESPACE_NO_10K)
    cache.clear_namespace(CACHE_NAMESPACE_NO_10K)
    logger.info(f"Cleared {count} entries from no-10K cache")
    return count


def filter_companies_with_10k_fast(
    companies: list[dict],
    filing_date_start: str = "2020-01-01",
    filing_date_end: str = "2026-01-01",
    force_refresh: bool = False,
) -> Iterator[dict]:
    """
    Filter companies to only those with 10-K filings using Datamule's bulk index.

    This is MUCH faster than the SEC EDGAR API approach:
    - SEC approach: ~14 minutes (8,000 sequential API calls at 10 req/sec)
    - Datamule index: ~60-90 seconds (single bulk query)

    Also excludes CIKs that are known to have no downloadable 10-K files
    (from previous failed download attempts).

    Args:
        companies: List of company dicts with 'cik' key
        filing_date_start: Start date for filing search
        filing_date_end: End date for filing search
        force_refresh: If True, ignore cache and re-query Datamule

    Yields:
        Companies that have 10-K filings
    """
    # Get all CIKs with 10-K filings (cached after first call)
    ciks_with_10k = get_all_ciks_with_10k(
        filing_date_start=filing_date_start,
        filing_date_end=filing_date_end,
        force_refresh=force_refresh,
    )

    # Get CIKs known to have no downloadable 10-K (from previous failed attempts)
    ciks_no_10k = get_ciks_without_10k()
    if ciks_no_10k:
        logger.info(
            f"Excluding {len(ciks_no_10k):,} CIKs with no downloadable 10-K (from previous runs)"
        )

    if not ciks_with_10k:
        logger.warning(
            f"No CIKs found with 10-K filings (or Datamule index unavailable). "
            f"Date range searched: {filing_date_start} to {filing_date_end}. "
            f"Input companies: {len(companies):,}. "
            f"Possible causes: (1) Datamule Index service down, (2) network issue, "
            f"(3) invalid date range, (4) cache corruption. "
            f"Will attempt to download ALL {len(companies):,} companies (may waste credits). "
            f"To retry index search, use --refresh-filter flag."
        )
        # Fail-safe: yield all companies if we can't filter
        yield from companies
        return

    # Filter companies to only those with 10-Ks AND not in the "known bad" list
    has_10k_count = 0
    no_10k_index_count = 0  # Not in Datamule index
    no_10k_download_count = 0  # In index but known to have no downloadable file

    for company in companies:
        cik = company.get("cik", "")
        # Normalize CIK for comparison
        cik_normalized = str(cik).zfill(10)

        if cik_normalized not in ciks_with_10k:
            no_10k_index_count += 1
        elif cik_normalized in ciks_no_10k:
            no_10k_download_count += 1
        else:
            has_10k_count += 1
            yield company

    total_filtered = no_10k_index_count + no_10k_download_count
    logger.info(
        f"Pre-filter complete: {has_10k_count:,} with 10-Ks, "
        f"{total_filtered:,} filtered out "
        f"({no_10k_index_count:,} not in index, {no_10k_download_count:,} known no-download)"
    )
