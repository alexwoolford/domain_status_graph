#!/usr/bin/env python3
"""
Production-quality parallel domain collection with multi-source consensus.

This script collects company domains from multiple sources concurrently,
uses weighted voting to determine the correct domain, and stops early when
confidence is high. Designed for speed, accuracy, and cost efficiency.

Architecture:
- Multiple data sources executed in parallel (yfinance, Finviz, SEC, Finnhub)
- Weighted voting system (higher weight for more reliable sources)
- Early stopping when confidence threshold is met (2+ sources agree)
- Rate limiting per source with proper concurrency control
- Caching to avoid redundant API calls

Sources (in priority order):
1. yfinance (weight: 3) - Fast, reliable, good coverage
2. Finviz (weight: 2) - Fast, good coverage
3. SEC EDGAR (weight: 2) - Authoritative but slower
4. Finnhub (weight: 1) - Incomplete but can augment
"""

import logging
from datetime import UTC, datetime
from threading import Lock

import requests

from public_company_graph.cli import setup_logging
from public_company_graph.utils.parallel import execute_parallel

# Optional dependencies are now handled in the extracted modules

# Global logger
_logger: logging.Logger | None = None

# Thread-safe cache for domain validation
_domain_cache: dict[str, bool] = {}
_cache_lock = Lock()

# Import constants and extracted modules
from public_company_graph.consensus.domain_consensus import (
    collect_domains as _collect_domains,
)
from public_company_graph.constants import (
    CACHE_TTL_COMPANY_DOMAINS,
    CACHE_TTL_NEGATIVE_RESULT,
)
from public_company_graph.domain.models import CompanyResult


def collect_domains(
    session: requests.Session,
    cik: str,
    ticker: str,
    company_name: str,
    early_stop_confidence: float = 0.75,
) -> CompanyResult:
    """
    Collect domains from all sources in parallel with early stopping.

    This function is now in public_company_graph/consensus/domain_consensus.py.
    This is kept here for backward compatibility but delegates to the extracted module.
    """
    return _collect_domains(session, cik, ticker, company_name, early_stop_confidence)


def fetch_company_tickers(session: requests.Session) -> dict[str, dict[str, str]]:
    """Fetch all company tickers from SEC EDGAR."""
    url = "https://www.sec.gov/files/company_tickers.json"
    headers = {
        "User-Agent": "public_company_graph script (contact: your-email@example.com)",
        "Accept": "application/json",
    }

    response = session.get(url, headers=headers, timeout=30)
    response.raise_for_status()
    data = response.json()

    companies = {}
    for entry in data.values():
        cik = str(entry.get("cik_str", "")).zfill(10)
        if cik:
            companies[cik] = {
                "cik": cik,
                "ticker": entry.get("ticker", "").upper(),
                "name": entry.get("title", "").strip(),
            }

    return companies


def process_company(
    session: requests.Session,
    cik: str,
    company_info: dict[str, str],
    cache,
) -> tuple[dict | None, bool]:
    """Process a single company.

    Check 10-K cache first, then company_domains cache, then API calls.

    Returns:
        Tuple of (result_dict, was_cached)
    """
    ticker = company_info["ticker"]
    name = company_info["name"]

    # Check 10-K cache first (most authoritative source)
    ten_k_data = cache.get("10k_extracted", cik)
    if ten_k_data and ten_k_data.get("website"):
        # Use 10-K website directly (most reliable source)
        output = {
            "cik": cik,
            "ticker": ticker,
            "name": name,
            "domain": ten_k_data["website"],
            "source": "10k",
            "confidence": 1.0,
            "votes": 1,
            "all_sources": {"10k": ten_k_data["website"]},
            "collected_at": datetime.now(UTC).isoformat(),
        }
        # Add 10-K business description if available
        if ten_k_data.get("business_description"):
            output["description"] = ten_k_data["business_description"]
            output["description_source"] = "10k"

        # Store in cache (TTL from constants.py)
        cache.set("company_domains", cik, output, ttl_days=CACHE_TTL_COMPANY_DOMAINS)
        return output, False  # Not cached in company_domains, but from 10-K

    # Check company_domains cache
    cached = cache.get("company_domains", cik)
    if cached:
        return cached, True

    # Not in cache - do the expensive API calls
    result = collect_domains(session, cik, ticker, name)

    if result.domain:
        output = {
            "cik": cik,
            "ticker": ticker,
            "name": name,
            "domain": result.domain,
            "source": "+".join(sorted(set(result.sources))),
            "confidence": result.confidence,
            "votes": result.votes,
            "all_sources": result.all_candidates,
            "collected_at": datetime.now(UTC).isoformat(),
        }
        # Add description if available
        if result.description:
            output["description"] = result.description
            output["description_source"] = result.description_source

        # Store in cache (TTL from constants.py)
        cache.set("company_domains", cik, output, ttl_days=CACHE_TTL_COMPANY_DOMAINS)
        return output, False
    else:
        # Cache negative result (no domain found) to avoid retrying
        # Uses shorter TTL so we retry sooner if data becomes available
        negative_result = {
            "cik": cik,
            "ticker": ticker,
            "name": name,
            "domain": None,
            "source": "none",
            "confidence": 0.0,
            "votes": 0,
            "all_sources": {},
            "collected_at": datetime.now(UTC).isoformat(),
            "no_domain_found": True,
        }
        cache.set("company_domains", cik, negative_result, ttl_days=CACHE_TTL_NEGATIVE_RESULT)
        return negative_result, False


def main():
    """Run the domain collection script."""
    import argparse

    global _logger

    parser = argparse.ArgumentParser(
        description="Parallel domain collection with multi-source consensus",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--max-companies",
        type=int,
        default=None,
        help="Limit number of companies to process (for testing)",
    )
    parser.add_argument(
        "--test",
        action="store_true",
        help="Quick test with a few major companies (AAPL, MSFT, GOOGL, etc.)",
    )
    parser.add_argument(
        "--skip-uncached",
        action="store_true",
        help="Skip companies not in cache (fast iteration mode)",
    )
    parser.add_argument(
        "--max-new",
        type=int,
        default=None,
        help="Maximum number of new API calls to make (default: unlimited)",
    )
    parser.add_argument(
        "--ten-k-only",
        action="store_true",
        help="Only process companies with 10-K data (10-K first approach)",
    )

    args = parser.parse_args()

    # Set up logging using centralized setup (tqdm-compatible, suppresses noisy loggers)
    _logger = setup_logging("collect_domains", execute=True)

    # Initialize cache
    from public_company_graph.cache import get_cache

    cache = get_cache()
    cached_count = cache.count("company_domains")

    _logger.info("=" * 80)
    _logger.info("Starting parallel domain collection")
    _logger.info(f"Cache: {cached_count} companies already cached")

    # Create session
    session = requests.Session()
    adapter = requests.adapters.HTTPAdapter(
        pool_connections=20,
        pool_maxsize=20,
        max_retries=requests.adapters.Retry(total=3, backoff_factor=0.3),
    )
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    # Fetch companies
    _logger.info("Fetching company list from SEC EDGAR...")
    companies = fetch_company_tickers(session)
    _logger.info(f"Found {len(companies)} total SEC companies")

    # 10-K only mode: Filter to companies with 10-K data (default for 10-K first pipeline)
    if args.ten_k_only:
        ten_k_ciks = set(cache.keys("10k_extracted", limit=20000) or [])
        companies = {cik: info for cik, info in companies.items() if cik in ten_k_ciks}
        _logger.info(f"Filtered to {len(companies)} companies with 10-K data (--ten-k-only)")

    # Test mode: just test a few major companies
    if args.test:
        test_tickers = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "META", "NVDA"]
        test_companies = {}
        for cik, info in companies.items():
            if info["ticker"] in test_tickers:
                test_companies[cik] = info
        companies = test_companies
        _logger.info(f"TEST MODE: Processing {len(companies)} test companies")

    _logger.info(f"Processing {len(companies)} companies...")

    # Pre-load all cached entries
    _logger.info("Pre-loading cached entries...")
    cached_results = {}
    companies_to_fetch = {}
    for cik, info in companies.items():
        cached = cache.get("company_domains", cik)
        if cached:
            cached_results[cik] = cached
        else:
            companies_to_fetch[cik] = info

    cached_count = len(cached_results)
    uncached_count = len(companies_to_fetch)

    if args.skip_uncached:
        _logger.info(f"Found {cached_count} cached, skipping {uncached_count} uncached companies")
        _logger.info("Use without --skip-uncached to fetch uncached companies")
        results = list(cached_results.values())
        new_count = 0
        skipped_count = uncached_count
    else:
        _logger.info(f"Found {cached_count} cached, {uncached_count} need API calls")

        # Limit new API calls if requested
        if args.max_new and uncached_count > args.max_new:
            _logger.info(
                f"Limiting to {args.max_new} new API calls (out of {uncached_count} uncached)"
            )
            companies_to_fetch = dict(list(companies_to_fetch.items())[: args.max_new])
            skipped_count = uncached_count - args.max_new
        else:
            skipped_count = 0

        # Process only companies that need API calls in parallel
        results = list(cached_results.values())
        new_count = 0

        if companies_to_fetch:
            # Create worker function that takes (cik, info) tuple
            def worker_func(item: tuple[str, dict]) -> tuple[dict | None, bool]:
                """Worker function for parallel execution."""
                cik, info = item
                return process_company(session, cik, info, cache)

            # Result handler to collect results and track new_count
            def result_handler(item: tuple[str, dict], result: tuple[dict | None, bool]):
                """Handle result from worker."""
                nonlocal new_count
                result_dict, was_cached = result
                if result_dict:
                    results.append(result_dict)
                    if not was_cached:
                        new_count += 1

            # Error handler
            def error_handler(item: tuple[str, dict], error: Exception):
                """Handle errors from worker."""
                cik, info = item
                if isinstance(error, TimeoutError):
                    _logger.warning(f"Timeout processing CIK {cik}")
                else:
                    _logger.warning(f"Error processing CIK {cik}: {error}")

            # Convert companies_to_fetch dict to list of tuples for parallel execution
            items_to_process = list(companies_to_fetch.items())

            # Execute in parallel (results handled via callbacks)
            _ = execute_parallel(
                items_to_process,
                worker_func,
                max_workers=30,
                desc="Processing companies",
                unit="company",
                result_handler=result_handler,
                error_handler=error_handler,
                timeout=60,
            )
        else:
            _logger.info("All companies were cached - no API calls needed")

    # Summary
    domains = {r["domain"] for r in results if r.get("domain")}
    _logger.info("")
    _logger.info(f"✓ Complete: {len(results)} companies, {len(domains)} unique domains")
    summary_parts = [f"From cache: {cached_count}"]
    if new_count > 0:
        summary_parts.append(f"New API calls: {new_count}")
    if skipped_count > 0:
        summary_parts.append(f"Skipped: {skipped_count}")
    _logger.info(f"  {', '.join(summary_parts)}")
    _logger.info("=" * 80)


if __name__ == "__main__":
    main()
