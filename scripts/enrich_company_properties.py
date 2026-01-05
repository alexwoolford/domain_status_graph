#!/usr/bin/env python3
"""
Enrich Company nodes with additional properties from public data sources.

This script:
1. Fetches company properties from SEC EDGAR, Yahoo Finance, and Wikidata
2. Stores enriched data in unified cache (namespace: company_properties)
3. Updates Company nodes in Neo4j with properties

Company Properties:
- Industry classification: sic_code, naics_code, sector, industry
- Financial metrics: market_cap, revenue, employees
- Geographic data: headquarters_city, headquarters_state, headquarters_country
- Metadata: data_source, data_updated_at

Data Sources:
- SEC EDGAR API: SIC/NAICS codes (public domain)
- Yahoo Finance: Sector, industry, market cap, revenue, employees, HQ location
- Wikidata: Supplemental data (optional, lower priority)

Performance:
- Uses parallel processing with ThreadPoolExecutor
- SEC and Yahoo APIs have independent rate limits (10 req/sec each)
- First run: ~15-20 minutes for 5000 companies
- Subsequent runs: seconds (all cached for 30 days)

Usage:
    python scripts/enrich_company_properties.py          # Dry-run (plan only)
    python scripts/enrich_company_properties.py --execute  # Actually enrich data
    python scripts/enrich_company_properties.py --execute --workers 20  # More parallelism
"""

import argparse
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from tqdm import tqdm

from public_company_graph.cache import get_cache
from public_company_graph.cli import (
    add_execute_argument,
    get_driver_and_database,
    setup_logging,
    verify_neo4j_connection,
)
from public_company_graph.company.enrichment import (
    fetch_sec_company_info,
    fetch_wikidata_info,
    fetch_yahoo_finance_info,
    merge_company_data,
)
from public_company_graph.constants import BATCH_SIZE_SMALL, CACHE_TTL_COMPANY_PROPERTIES
from public_company_graph.neo4j import clean_properties_batch, create_company_constraints

# Rate limiting for Yahoo Finance (be conservative)
# 10 requests per second max
from public_company_graph.utils.rate_limiting import get_rate_limiter

_yahoo_rate_limiter = get_rate_limiter("yahoo_finance", requests_per_second=10.0)

# Default number of parallel workers
# With rate limits of 10 req/sec per API and ~200ms latency per call,
# 20 workers keeps both APIs saturated at their rate limits
DEFAULT_WORKERS = 20


def rate_limit_yahoo():
    """Simple rate limiting for Yahoo Finance."""
    _yahoo_rate_limiter()


def enrich_company(
    cik: str, ticker: str, name: str, session: requests.Session, cache
) -> tuple[dict | None, bool]:
    """
    Enrich a single company with data from all sources.

    Follows the same caching pattern as collect_domains.py:
    - Check cache first (by CIK)
    - If not cached, fetch from all sources
    - Cache final merged result

    Args:
        cik: Company CIK
        ticker: Stock ticker
        name: Company name
        session: HTTP session for SEC API
        cache: Unified cache instance

    Returns:
        Tuple of (enriched company data dictionary, was_cached).
        Returns (None, False) if enrichment failed.
    """
    # Check cache first (consistent with collect_domains.py pattern)
    cache_key = cik
    cached = cache.get("company_properties", cache_key)
    if cached:
        return cached, True

    # Not in cache - fetch from sources
    sec_data = fetch_sec_company_info(cik, session=session)
    rate_limit_yahoo()
    yahoo_data = fetch_yahoo_finance_info(ticker) if ticker else None
    wikidata_data = fetch_wikidata_info(ticker, name)  # Optional, may return None

    # Merge data from all sources
    enriched = merge_company_data(sec_data, yahoo_data, wikidata_data)

    # Store final merged result in cache (TTL: 30 days)
    # Cache even if partial - avoids re-fetching if one source fails
    if enriched:
        cache.set("company_properties", cache_key, enriched, ttl_days=CACHE_TTL_COMPANY_PROPERTIES)
        return enriched, False

    # If no data at all, return None (don't cache negative results)
    # This allows retry on next run
    return None, False


def enrich_all_companies(
    driver,
    cache,
    batch_size: int = BATCH_SIZE_SMALL,
    database: str = None,
    execute: bool = False,
    logger=None,
    max_workers: int = DEFAULT_WORKERS,
) -> int:
    """
    Enrich all Company nodes with properties from public data sources.

    Uses parallel processing to maximize throughput while respecting rate limits.
    SEC and Yahoo APIs have independent rate limits, so calling them in parallel
    roughly doubles throughput compared to sequential processing.

    Args:
        driver: Neo4j driver
        cache: Unified cache instance
        batch_size: Batch size for Neo4j updates
        database: Neo4j database name
        execute: If False, only print plan
        logger: Logger instance
        max_workers: Number of parallel worker threads (default: 20)

    Returns:
        Number of companies enriched
    """
    if logger is None:
        import logging

        logger = logging.getLogger(__name__)

    # Get all companies from Neo4j
    with driver.session(database=database) as neo4j_session:
        result = neo4j_session.run(
            """
            MATCH (c:Company)
            RETURN c.cik AS cik, c.ticker AS ticker, c.name AS name
            ORDER BY c.ticker
            """
        )
        companies = [dict(row) for row in result]

    if not companies:
        logger.warning("No companies found in Neo4j. Run load_company_data.py first.")
        return 0

    logger.info(f"Found {len(companies)} companies to enrich")

    if not execute:
        logger.info("=" * 80)
        logger.info("DRY RUN MODE")
        logger.info("=" * 80)
        logger.info(f"Would enrich {len(companies)} companies")
        logger.info(f"Workers: {max_workers} (parallel threads)")
        logger.info("Sources: SEC EDGAR, Yahoo Finance, Wikidata")
        logger.info("=" * 80)
        return 0

    # Thread-local storage for HTTP sessions (one per thread)
    thread_local = threading.local()

    def get_session():
        """Get or create thread-local HTTP session."""
        if not hasattr(thread_local, "session"):
            session = requests.Session()
            adapter = requests.adapters.HTTPAdapter(
                pool_connections=10,
                pool_maxsize=10,
                max_retries=requests.adapters.Retry(total=3, backoff_factor=0.3),
            )
            session.mount("http://", adapter)
            session.mount("https://", adapter)
            thread_local.session = session
        return thread_local.session

    # Thread-safe counters
    counters_lock = threading.Lock()
    counters = {"enriched": 0, "failed": 0, "cached": 0, "processed": 0}

    # Thread-safe results collection
    results_lock = threading.Lock()
    results = []

    def process_company(company: dict) -> tuple[str, dict | None, bool]:
        """Process a single company (thread worker function)."""
        cik = company.get("cik")
        ticker = company.get("ticker", "")
        name = company.get("name", "")

        if not cik:
            return cik, None, False

        # Get thread-local HTTP session
        http_session = get_session()

        # Enrich company (rate limiters are thread-safe)
        enriched_data, was_cached = enrich_company(cik, ticker, name, http_session, cache)

        return cik, enriched_data, was_cached

    logger.info("=" * 80)
    logger.info("Enriching Company Properties (Parallel)")
    logger.info("=" * 80)
    logger.info(f"  Workers: {max_workers} parallel threads")
    logger.info("  Rate limits: SEC 10/sec, Yahoo 10/sec (independent)")
    logger.info(
        f"  Estimated time: ~{len(companies) // 10} seconds ({len(companies) // 600} minutes)"
    )
    logger.info("")

    start_time = time.time()

    # Process companies in parallel with tqdm progress bar
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        future_to_company = {
            executor.submit(process_company, company): company for company in companies
        }

        # Process results as they complete with tqdm progress bar
        with tqdm(
            total=len(companies),
            desc="Enriching",
            unit="company",
            bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}{postfix}]",
        ) as pbar:
            for future in as_completed(future_to_company):
                company = future_to_company[future]
                try:
                    cik, enriched_data, was_cached = future.result()

                    with counters_lock:
                        counters["processed"] += 1

                        if enriched_data is None:
                            counters["failed"] += 1
                        else:
                            counters["enriched"] += 1
                            if was_cached:
                                counters["cached"] += 1

                        # Update tqdm postfix with cache stats
                        cache_pct = (
                            (counters["cached"] / counters["processed"] * 100)
                            if counters["processed"] > 0
                            else 0
                        )
                        pbar.set_postfix(
                            cached=f"{counters['cached']}",
                            cache_pct=f"{cache_pct:.0f}%",
                            failed=counters["failed"],
                        )

                    # Collect result for Neo4j batch update
                    if enriched_data:
                        batch_to_update = None
                        with results_lock:
                            results.append({"cik": cik, **enriched_data})

                            # Batch update to Neo4j when we have enough
                            if len(results) >= batch_size:
                                batch_to_update = results.copy()
                                results.clear()

                        # Update Neo4j outside the lock to avoid blocking other threads
                        if batch_to_update:
                            _update_companies_batch(
                                driver, batch_to_update, database=database, logger=logger
                            )
                            # Use tqdm.write to avoid interfering with progress bar
                            tqdm.write(
                                f"  Updated {len(batch_to_update)} companies in Neo4j... "
                                f"({counters['processed']}/{len(companies)})"
                            )

                    pbar.update(1)

                except Exception as e:
                    logger.warning(f"Error processing {company.get('ticker', 'unknown')}: {e}")
                    with counters_lock:
                        counters["processed"] += 1
                        counters["failed"] += 1
                    pbar.update(1)

    # Final batch update
    with results_lock:
        if results:
            _update_companies_batch(driver, results, database=database, logger=logger)
            logger.info(f"  Updated final {len(results)} companies in Neo4j")

    elapsed = time.time() - start_time
    logger.info("=" * 80)
    logger.info("Enrichment Complete")
    logger.info("=" * 80)
    logger.info(f"  Total companies: {len(companies)}")
    logger.info(f"  Enriched: {counters['enriched']}")
    logger.info(f"  From cache: {counters['cached']}")
    logger.info(f"  Failed: {counters['failed']}")
    logger.info(f"  Time: {elapsed:.1f}s ({elapsed / 60:.1f} min)")
    logger.info(f"  Rate: {len(companies) / elapsed:.1f} companies/sec")

    return counters["enriched"]


def _update_companies_batch(driver, batch: list[dict], database: str = None, logger=None) -> None:
    """Update a batch of Company nodes in Neo4j."""
    if logger is None:
        import logging

        logger = logging.getLogger(__name__)

    # Clean empty strings and None values - Neo4j doesn't store nulls
    # This prevents storing "" values that should be absent
    cleaned_batch = clean_properties_batch(batch)

    # Use SET c += to merge only non-empty properties from the cleaned batch
    # The cik is used for matching, other properties are merged
    query = """
    UNWIND $batch AS company
    MATCH (c:Company {cik: company.cik})
    SET c += company
    """

    try:
        with driver.session(database=database) as session:
            session.run(query, batch=cleaned_batch)
    except Exception as e:
        logger.error(f"Error updating companies batch: {e}")
        raise


def main():
    """Run the company property enrichment script."""
    parser = argparse.ArgumentParser(
        description="Enrich Company nodes with properties from public data sources"
    )
    add_execute_argument(parser)
    parser.add_argument(
        "--batch-size",
        type=int,
        default=BATCH_SIZE_SMALL,
        help=f"Batch size for Neo4j updates (default: {BATCH_SIZE_SMALL})",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=DEFAULT_WORKERS,
        help=f"Number of parallel worker threads (default: {DEFAULT_WORKERS})",
    )

    args = parser.parse_args()

    logger = setup_logging("enrich_company_properties", execute=args.execute)
    cache = get_cache()

    # Log cache status upfront
    cache_stats = cache.stats()
    logger.info("Cache status:")
    logger.info(f"  Total entries: {cache_stats['total']:,}")
    logger.info(f"  Size: {cache_stats['size_mb']} MB")
    for ns, ns_count in sorted(cache_stats["by_namespace"].items(), key=lambda x: -x[1]):
        logger.info(f"    {ns}: {ns_count:,}")

    if not args.execute:
        # Dry-run: show plan
        driver, database = get_driver_and_database(logger)
        try:
            enrich_all_companies(
                driver,
                cache,
                batch_size=args.batch_size,
                database=database,
                execute=False,
                logger=logger,
                max_workers=args.workers,
            )
        finally:
            driver.close()
        return

    logger.info("=" * 80)
    logger.info("Company Property Enrichment")
    logger.info("=" * 80)

    driver, database = get_driver_and_database(logger)

    try:
        # Verify connection
        if not verify_neo4j_connection(driver, database, logger):
            sys.exit(1)

        # Ensure constraints exist
        logger.info("")
        logger.info("1. Creating/verifying constraints...")
        create_company_constraints(driver, database=database, logger=logger)

        # Enrich companies
        logger.info("")
        logger.info("2. Enriching company properties...")
        enriched = enrich_all_companies(
            driver,
            cache,
            batch_size=args.batch_size,
            database=database,
            execute=True,
            logger=logger,
            max_workers=args.workers,
        )

        logger.info("")
        logger.info("=" * 80)
        logger.info("✓ Complete!")
        logger.info("=" * 80)
        logger.info(f"Enriched {enriched} companies")

    finally:
        driver.close()


if __name__ == "__main__":
    main()
