#!/usr/bin/env python3
"""
Download the most recent 10-K filing for all companies from SEC EDGAR.

This script:
1. Gets all companies from SEC EDGAR (or optionally from Neo4j)
2. Downloads the most recent 10-K per company using datamule
3. Stores files in data/10k_filings/{cik}/
4. Caches results (skips if file already exists)
5. Extracts structured data (company website, business description, etc.)

This becomes the START of the pipeline - everything cascades from 10-Ks:
- 10-Ks → Company websites → Domain collection
- 10-Ks → Business descriptions → Company embeddings
- 10-Ks → Competitor mentions → Direct competitor relationships

Usage:
    python scripts/download_10k_filings.py          # Dry-run (plan only)
    python scripts/download_10k_filings.py --execute  # Actually download
    python scripts/download_10k_filings.py --execute --from-neo4j  # Use Neo4j instead of SEC
"""

import argparse
import logging
import os
import sys
import time
from pathlib import Path

import requests

from public_company_graph.cli import (
    add_execute_argument,
    get_driver_and_database,
    setup_logging,
    verify_neo4j_connection,
)
from public_company_graph.config import get_data_dir, get_datamule_api_key

# CRITICAL: Export DATAMULE_API_KEY to environment BEFORE importing datamule
# The datamule library reads from os.environ directly, not from set_api_key()
_datamule_key = get_datamule_api_key()
if _datamule_key:
    os.environ["DATAMULE_API_KEY"] = _datamule_key

# CRITICAL: Disable tqdm BEFORE importing datamule to prevent progress bar spam
# tqdm checks TQDM_DISABLE at import time, not at runtime
os.environ["TQDM_DISABLE"] = "1"

from public_company_graph.constants import (
    DEFAULT_WORKERS,
    DEFAULT_WORKERS_WITH_API,
    SEC_EDGAR_LONG_DURATION_LIMIT,
    SEC_EDGAR_RATE_LIMIT,
)
from public_company_graph.sources.datamule_index import (
    filter_companies_with_10k_fast,
    mark_cik_no_10k_available,
)
from public_company_graph.sources.sec_companies import (
    get_all_companies_from_neo4j,
    get_all_companies_from_sec,
)
from public_company_graph.utils.datamule import suppress_datamule_output
from public_company_graph.utils.parallel import execute_parallel
from public_company_graph.utils.stats import ExecutionStats
from public_company_graph.utils.tar_extraction import (
    extract_from_tar,
    get_filing_date_from_tar_name,
)
from public_company_graph.utils.tar_selection import find_tar_with_latest_10k

# Try to import datamule
try:
    from datamule import Config, Portfolio

    DATAMULE_AVAILABLE = True

    # Suppress datamule's verbose logging (API URLs, query results, costs)
    # These loggers print directly and ignore quiet=True in download_submissions()
    for _logger_name in [
        "datamule",
        "datamule.book",
        "datamule.datamule",
        "datamule.datamule.downloader",
        "datamule.datamule.tar_downloader",
    ]:
        logging.getLogger(_logger_name).setLevel(logging.CRITICAL + 1)
except ImportError:
    DATAMULE_AVAILABLE = False
    Portfolio = None  # type: ignore
    Config = None  # type: ignore

logger = logging.getLogger(__name__)

# Output directories for 10-K filings
FILINGS_DIR = get_data_dir() / "10k_filings"  # Organized extracted HTML files
PORTFOLIOS_DIR = get_data_dir() / "10k_portfolios"  # Datamule portfolio directories (tar files)


def download_10k_for_company(
    cik: str,
    ticker: str,
    name: str,
    output_dir: Path,
    max_retries: int = 1,  # Reduced to 1 to prevent multiple API charges on failures
    keep_tar_files: bool = True,  # Keep tar files for datamule parsing (better quality)
    api_key: str | None = None,  # Datamule API key for fast downloads
    filing_date_start: str = "2020-01-01",  # Start date for filing search (focus on recent filings)
    filing_date_end: str = None,  # End date for filing search (None = current date + 1 year for future filings)
    force: bool = False,  # If True, delete existing files and re-download
) -> tuple[bool, Path | None, str | None]:
    """
    Download the most recent 10-K filing for a company.

    Args:
        cik: Company CIK (10-digit, zero-padded)
        ticker: Stock ticker (for logging)
        name: Company name (for logging)
        output_dir: Base directory for storing filings
        max_retries: Maximum number of retry attempts (default: 1 to prevent multiple API charges on failures)

    Returns:
        Tuple of (success, file_path, error_message)
    """
    if not DATAMULE_AVAILABLE:
        return False, None, "datamule not installed"

    # Ensure CIK is 10-digit zero-padded
    cik_padded = cik.zfill(10)

    # Set default end date to current year + 1 (to include future filings)
    if filing_date_end is None:
        from datetime import datetime, timedelta

        filing_date_end = (datetime.now() + timedelta(days=365)).strftime("%Y-%m-%d")

    # Create company-specific directory
    company_dir = output_dir / cik_padded
    company_dir.mkdir(parents=True, exist_ok=True)

    # Portfolio directory (datamule creates this)
    # Use a subdirectory in data/ to keep project root clean
    portfolio_name = f"10k_{cik_padded}"
    portfolio_path = PORTFOLIOS_DIR / portfolio_name
    portfolio_path.parent.mkdir(parents=True, exist_ok=True)  # Ensure parent directory exists

    # Check if HTML/XML file already exists (extracted)
    existing_files = list(company_dir.glob("**/*.html")) + list(company_dir.glob("**/*.xml"))
    if existing_files and not force:
        # Get most recent file
        most_recent = max(existing_files, key=lambda p: p.stat().st_mtime)
        logger.debug(f"  ✓ {ticker}: Found existing file {most_recent.name}")
        return True, most_recent, None
    elif existing_files and force:
        # Force re-download: delete existing files (shouldn't happen if bulk delete worked, but handle gracefully)
        logger.debug(
            f"  🔄 {ticker}: Found existing files (should have been deleted upfront, cleaning up)"
        )
        for existing_file in existing_files:
            try:
                existing_file.unlink()
                logger.debug(f"  🗑️  Deleted: {existing_file.name}")
            except Exception as e:
                logger.warning(f"  ⚠ Failed to delete {existing_file.name}: {e}")

    # Check if tar files exist in portfolio directory (from previous download)
    # If so, extract from them instead of re-downloading (unless force=True)
    # Sort by actual filing date (from filename), not modification time
    tar_files = sorted(
        portfolio_path.glob("*.tar") if portfolio_path.exists() else [],
        key=get_filing_date_from_tar_name,
        reverse=True,  # Most recent first
    )
    if tar_files and not force:
        logger.debug(f"  Found {len(tar_files)} existing tar file(s) for {ticker}")

        # REPEATABLE PROCESS: Identify tar file with latest 10-K BEFORE extraction
        tar_file_with_latest = find_tar_with_latest_10k(tar_files, ticker=ticker, cik=cik_padded)

        if not tar_file_with_latest:
            logger.warning(
                f"  ⚠ {ticker}: Could not identify tar file with latest 10-K (all may be empty)"
            )
            # Fallback: try all non-empty tar files (skip empty ones)
            from public_company_graph.utils.tar_selection import is_tar_file_empty

            last_error = None
            for tar_file in tar_files:
                if is_tar_file_empty(tar_file):
                    logger.debug(f"  Skipping empty tar: {tar_file.name}")
                    continue
                result = extract_from_tar(tar_file, company_dir, ticker, cik_padded)
                if result[0]:  # Success
                    tar_file_with_latest = tar_file
                    break
                last_error = result[2]

            if not tar_file_with_latest:
                if not keep_tar_files:
                    for tar_file in tar_files:
                        tar_file.unlink()
                return (
                    False,
                    None,
                    f"All {len(tar_files)} tar files failed. Last error: {last_error}",
                )

        # Extract from the identified tar file (contains latest 10-K)
        logger.debug(f"  Extracting from {tar_file_with_latest.name} (contains latest 10-K)")
        result = extract_from_tar(tar_file_with_latest, company_dir, ticker, cik_padded)

        if result[0]:  # Success
            # Delete all other tar files (we only need the one with latest 10-K)
            if keep_tar_files:
                for tar_file_to_delete in tar_files:
                    if tar_file_to_delete != tar_file_with_latest:
                        try:
                            tar_file_to_delete.unlink()
                            logger.debug(f"  🗑️  Deleted extra tar: {tar_file_to_delete.name}")
                        except Exception as e:
                            logger.debug(f"  ⚠ Failed to delete {tar_file_to_delete.name}: {e}")
            else:
                # Delete all tar files after extraction
                for tar_file_to_delete in tar_files:
                    tar_file_to_delete.unlink()
                # Remove empty portfolio directory
                if portfolio_path.exists() and not any(portfolio_path.iterdir()):
                    portfolio_path.rmdir()
            return result
        else:
            # Extraction failed from the identified tar file
            # This shouldn't happen often, but log it
            logger.warning(
                f"  ⚠ {ticker}: Extraction failed from {tar_file_with_latest.name}: {result[2]}"
            )
            if not keep_tar_files:
                for tar_file in tar_files:
                    tar_file.unlink()
            return False, None, f"Extraction failed from {tar_file_with_latest.name}: {result[2]}"

    # Set default end date to current year + 1 (to include future filings)
    if filing_date_end is None:
        from datetime import datetime, timedelta

        filing_date_end = (datetime.now() + timedelta(days=365)).strftime("%Y-%m-%d")

    # If force=True, also delete existing tar files to force re-download
    # (Shouldn't happen if bulk delete worked, but handle gracefully for any missed files)
    if force and portfolio_path.exists():
        tar_files_to_delete = list(portfolio_path.glob("*.tar"))
        if tar_files_to_delete:
            logger.debug(
                f"  🔄 {ticker}: Found existing tar files (should have been deleted upfront, cleaning up)"
            )
            for tar_file in tar_files_to_delete:
                try:
                    tar_file.unlink()
                    logger.debug(f"  🗑️  Deleted tar: {tar_file.name}")
                except Exception as e:
                    logger.warning(f"  ⚠ Failed to delete {tar_file.name}: {e}")

    # Download most recent 10-K
    # IMPORTANT: Only retry on network/timeout errors, not on "no files found"
    # This prevents multiple API charges for companies that legitimately have no 10-Ks
    for attempt in range(max_retries):
        try:
            # Log date range being used (helps verify correct range is applied)
            if attempt == 0:  # Only log on first attempt to avoid spam
                logger.debug(
                    f"  📅 {ticker}: Downloading 10-K with date range: {filing_date_start} to {filing_date_end}"
                )

            # Suppress datamule's verbose output
            with suppress_datamule_output():
                # Create portfolio in data/10k_portfolios/ directory
                portfolio = Portfolio(str(portfolio_path))

                # Set API key on portfolio if available (enables fast datamule-sgml provider)
                if api_key:
                    portfolio.set_api_key(api_key)
                    # Use datamule-sgml provider (fast, no rate limits)
                    # NOTE: Pre-check above should prevent most wasted API calls
                    portfolio.download_submissions(
                        submission_type="10-K",
                        cik=cik_padded,
                        filing_date=(filing_date_start, filing_date_end),
                        provider="datamule-sgml",
                        quiet=True,  # Suppress datamule's console output
                    )
                else:
                    # Fallback to SEC direct (free, but rate limited)
                    portfolio.download_submissions(
                        submission_type="10-K",
                        cik=cik_padded,
                        filing_date=(filing_date_start, filing_date_end),
                        provider="sec",
                        requests_per_second=SEC_EDGAR_LONG_DURATION_LIMIT,
                        quiet=True,  # Suppress datamule's console output
                    )

            # Datamule downloads tar files to the portfolio directory
            # Check if tar files were downloaded
            # Sort by actual filing date (from filename), not modification time
            tar_files = sorted(
                portfolio_path.glob("*.tar"),
                key=get_filing_date_from_tar_name,
                reverse=True,  # Most recent first
            )
            if not tar_files:
                # No files downloaded - likely means company has no 10-Ks in date range
                # Don't retry - this is not a transient error, and retrying would charge API again
                # Clean up empty portfolio directory
                if portfolio_path.exists() and not any(portfolio_path.iterdir()):
                    portfolio_path.rmdir()
                # Cache this CIK as "no 10-K available" to prevent future wasted API calls
                mark_cik_no_10k_available(cik_padded)
                return (
                    False,
                    None,
                    f"No 10-K found for {ticker} (CIK: {cik_padded}) in date range {filing_date_start} to {filing_date_end}",
                )

            # Log the filing dates of downloaded files (helps verify we got recent files)
            if tar_files:
                latest_tar = tar_files[0]
                latest_date = get_filing_date_from_tar_name(latest_tar)
                if latest_date:
                    logger.debug(
                        f"  ✓ {ticker}: Downloaded tar file with filing date: {latest_date.strftime('%Y-%m-%d')} ({latest_date.year})"
                    )
                else:
                    logger.debug(
                        f"  ✓ {ticker}: Downloaded {len(tar_files)} tar file(s) (could not extract date from filename)"
                    )

            # REPEATABLE PROCESS: Identify tar file with latest 10-K BEFORE extraction
            logger.debug(f"  Found {len(tar_files)} tar file(s) for {ticker}")
            tar_file_with_latest = find_tar_with_latest_10k(
                tar_files, ticker=ticker, cik=cik_padded
            )

            if not tar_file_with_latest:
                logger.warning(
                    f"  ⚠ {ticker}: Could not identify tar file with latest 10-K (all may be empty)"
                )
                # Fallback: try all non-empty tar files (skip empty ones)
                from public_company_graph.utils.tar_selection import is_tar_file_empty

                last_error = None
                for tar_file in tar_files:
                    if is_tar_file_empty(tar_file):
                        logger.debug(f"  Skipping empty tar: {tar_file.name}")
                        continue
                    logger.debug(f"  Trying {tar_file.name}...")
                    result = extract_from_tar(tar_file, company_dir, ticker, cik_padded)
                    if result[0]:  # Success
                        tar_file_with_latest = tar_file
                        break
                    last_error = result[2]

                if not tar_file_with_latest:
                    if not keep_tar_files:
                        for tar_file in tar_files:
                            tar_file.unlink()
                    # If all extractions failed, continue to retry (if retries available)
                    if attempt < max_retries - 1:
                        time.sleep(2**attempt)
                        continue
                    return (
                        False,
                        None,
                        f"All {len(tar_files)} tar files failed. Last error: {last_error}",
                    )

            # Extract from the identified tar file (contains latest 10-K)
            logger.debug(f"  Extracting from {tar_file_with_latest.name} (contains latest 10-K)")
            result = extract_from_tar(tar_file_with_latest, company_dir, ticker, cik_padded)

            if result[0]:  # Success
                # Delete all other tar files (we only need the one with latest 10-K)
                if keep_tar_files:
                    for tar_file_to_delete in tar_files:
                        if tar_file_to_delete != tar_file_with_latest:
                            try:
                                tar_file_to_delete.unlink()
                                logger.debug(f"  🗑️  Deleted extra tar: {tar_file_to_delete.name}")
                            except Exception as e:
                                logger.debug(f"  ⚠ Failed to delete {tar_file_to_delete.name}: {e}")
                else:
                    # Delete all tar files after extraction
                    for tar_file_to_delete in tar_files:
                        tar_file_to_delete.unlink()
                    # Remove empty portfolio directory
                    if portfolio_path.exists() and not any(portfolio_path.iterdir()):
                        portfolio_path.rmdir()
                return result
            else:
                # Extraction failed from the identified tar file
                logger.warning(
                    f"  ⚠ {ticker}: Extraction failed from {tar_file_with_latest.name}: {result[2]}"
                )

                # Even though extraction failed, clean up extra tar files if keeping tar files
                # This ensures we don't accumulate multiple tar files for failed extractions
                if keep_tar_files:
                    for tar_file_to_delete in tar_files:
                        if tar_file_to_delete != tar_file_with_latest:
                            try:
                                tar_file_to_delete.unlink()
                                logger.debug(f"  🗑️  Deleted extra tar: {tar_file_to_delete.name}")
                            except Exception as e:
                                logger.debug(f"  ⚠ Failed to delete {tar_file_to_delete.name}: {e}")

                # If all extractions failed, continue to retry (if retries available)
                if attempt < max_retries - 1:
                    time.sleep(2**attempt)
                    continue
                if not keep_tar_files:
                    for tar_file in tar_files:
                        tar_file.unlink()
                return (
                    False,
                    None,
                    f"Extraction failed from {tar_file_with_latest.name}: {result[2]}",
                )
            return False, None, f"All {len(tar_files)} tar files failed. Last error: {last_error}"

        except Exception as e:
            error_msg = str(e)
            # Only retry on network/timeout errors (transient failures)
            # Don't retry on other errors (likely permanent, retrying would charge API again)
            is_retryable = any(
                keyword in str(e).lower()
                for keyword in ["timeout", "connection", "network", "temporary", "retry"]
            )

            if is_retryable and attempt < max_retries - 1:
                wait_time = 2**attempt
                logger.debug(
                    f"  ⚠ {ticker}: Attempt {attempt + 1} failed (retryable): {error_msg}, retrying in {wait_time}s..."
                )
                time.sleep(wait_time)
                continue
            else:
                # Non-retryable error or max retries reached
                if not is_retryable:
                    logger.debug(f"  ✗ {ticker}: Non-retryable error: {error_msg}")
                return False, None, f"Failed after {attempt + 1} attempt(s): {error_msg}"

    return False, None, "Max retries exceeded"


def download_all_10ks(
    driver: object | None = None,
    database: str | None = None,
    execute: bool = False,
    limit: int | None = None,
    from_neo4j: bool = False,
    keep_tar_files: bool = True,  # Always True - tar files are required for quality parsing
    workers: int = DEFAULT_WORKERS,  # Number of parallel workers
    filing_date_start: str = "2020-01-01",  # Start date for filing search (focus on recent filings)
    filing_date_end: str = None,  # End date for filing search (None = current date + 1 year for future filings)
    force: bool = False,  # If True, delete existing files and re-download
    pre_filter: bool = True,  # Pre-filter companies using Datamule index (default: True, saves credits)
    refresh_filter: bool = False,  # If True, force refresh the pre-filter cache
) -> dict[str, int]:
    """
    Download 10-K filings for all companies.

    Args:
        driver: Neo4j driver (required if from_neo4j=True)
        database: Database name (required if from_neo4j=True)
        execute: If False, only print plan
        limit: Optional limit on number of companies to process
        from_neo4j: If True, get companies from Neo4j; if False, get from SEC EDGAR
        keep_tar_files: If True, keep tar files after extraction (default: True, needed for datamule parsing)
        workers: Number of parallel workers (default: {DEFAULT_WORKERS}, auto-increased to {DEFAULT_WORKERS_WITH_API} with API key)

    Returns:
        Dict with counts: total, downloaded, cached, no_10k_available, errors, failed
        - total: Total companies processed
        - downloaded: Newly downloaded 10-Ks
        - cached: Already existing 10-Ks
        - no_10k_available: Companies without 10-Ks (expected: ETFs, foreign, inactive)
        - errors: Actual errors (download/extraction failures)
        - failed: Total of no_10k_available + errors (for backward compatibility)
    """
    # Get companies from SEC or Neo4j
    if from_neo4j:
        if driver is None:
            raise ValueError("driver is required when from_neo4j=True")
        companies = get_all_companies_from_neo4j(driver, database=database)
        source = "Neo4j"
    else:
        # Create session for SEC API
        session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=10,
            pool_maxsize=10,
            max_retries=requests.adapters.Retry(total=3, backoff_factor=0.3),
        )
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        companies = get_all_companies_from_sec(session)
        source = "SEC EDGAR"

    if limit:
        companies = companies[:limit]
        logger.info(f"Processing first {limit} companies (--limit specified)")

    # Set default end date to current year + 1 (to include future filings)
    # MUST be set before pre-filter so the index query uses the correct date range
    if filing_date_end is None:
        from datetime import datetime, timedelta

        filing_date_end = (datetime.now() + timedelta(days=365)).strftime("%Y-%m-%d")

    # Pre-filter companies to only those with 10-Ks (saves datamule credits)
    # Uses Datamule's bulk index search (~60-90 seconds) instead of SEC API (~14 min)
    # Results are cached for 7 days
    if pre_filter and execute:
        logger.info("")
        logger.info("=" * 80)
        logger.info("Pre-filtering companies (skipping those without 10-Ks)")
        logger.info("=" * 80)
        logger.info("Using Datamule bulk index search (much faster than SEC API)")

        original_count = len(companies)

        # Filter to only companies with 10-Ks using Datamule's bulk index
        # This is ~10x faster than the old SEC API approach
        companies_with_10k = list(
            filter_companies_with_10k_fast(
                companies,
                filing_date_start=filing_date_start,
                filing_date_end=filing_date_end,
                force_refresh=refresh_filter,
            )
        )

        filtered_out = original_count - len(companies_with_10k)
        logger.info("")
        logger.info(
            f"Pre-filter: {len(companies_with_10k):,} companies with 10-Ks "
            f"({filtered_out:,} without, saving ~${filtered_out * 0.001:.2f} in credits)"
        )
        logger.info("")

        companies = companies_with_10k

    total = len(companies)

    # Track different outcomes (initialized here, used in execute block)
    downloaded = 0
    cached = 0
    no_10k_available = 0  # Companies that legitimately don't have 10-Ks (ETFs, foreign, etc.)
    errors = 0  # Actual errors (download failures, extraction failures)
    failed_companies = []  # List of (ticker, cik, error_type, error_message)

    if not execute:
        logger.info("=" * 80)
        logger.info("DRY RUN MODE")
        logger.info("=" * 80)
        logger.info(f"Source: {source}")
        logger.info(f"Would download 10-Ks for {total:,} companies")
        logger.info(f"Portfolio directory (tar files): {PORTFOLIOS_DIR}")
        logger.info(f"Output directory (extracted HTML): {FILINGS_DIR}")
        logger.info(f"Parallel workers: {workers}")
        logger.info(f"Filing date range: {filing_date_start} to {filing_date_end}")
        logger.info("  📅 This range will be used for ALL downloads (ensures recent filings)")
        logger.info("✓ Tar files will be KEPT after extraction (~100-300 GB estimated total)")
        logger.info(
            "  (Required for high-quality datamule parsing: 86-93% success vs 64% custom parser)"
        )
        logger.info(
            "  (Once deleted, you must pay again to re-download - keeping them is recommended)"
        )
        if pre_filter:
            logger.info(
                "✓ Pre-filter enabled: Will skip companies without 10-Ks (saves datamule credits)"
            )
            logger.info("  First run: ~60-90 seconds. Subsequent runs: instant (cached 7 days)")
            if refresh_filter:
                logger.info("  --refresh-filter: Will re-query Datamule index (ignoring cache)")
        else:
            logger.info("⚠️  Pre-filter DISABLED: Will attempt all companies (may waste credits)")
        logger.info("=" * 80)
        logger.info("To execute, run: python scripts/download_10k_filings.py --execute")
        return {
            "total": total,
            "downloaded": 0,
            "cached": 0,
            "no_10k_available": 0,
            "errors": 0,
            "failed": 0,
        }

    # Get all companies
    logger.info("=" * 80)
    logger.info("Downloading 10-K Filings")
    logger.info("=" * 80)
    logger.info(f"Source: {source}")
    logger.info(f"Portfolio directory (tar files): {PORTFOLIOS_DIR}")
    logger.info(f"Output directory (extracted HTML): {FILINGS_DIR}")
    logger.info(f"Filing date range: {filing_date_start} to {filing_date_end}")
    logger.info("✓ Tar files will be KEPT after extraction (~100-300 GB estimated total)")
    logger.info(
        "  (Required for high-quality datamule parsing: 86-93% success vs 64% custom parser)"
    )
    logger.info("  (Once deleted, you must pay again to re-download - keeping them is recommended)")

    logger.info(f"Found {total:,} companies with CIKs")
    logger.info("")

    if total == 0:
        logger.warning(f"⚠ No companies found from {source}.")
        return {
            "total": 0,
            "downloaded": 0,
            "cached": 0,
            "no_10k_available": 0,
            "errors": 0,
            "failed": 0,
        }

    # Check if API key is available for fast downloads
    api_key = get_datamule_api_key()
    if api_key:
        logger.info("✓ Using Datamule API (fast, no rate limits)")
        logger.info("  Provider: datamule-sgml (unrestricted speed)")
        # With API, can use more workers (no rate limits)
        if workers == DEFAULT_WORKERS:
            workers = DEFAULT_WORKERS_WITH_API  # Default to 16 workers with API (can be overridden)
    else:
        logger.info(
            f"⚠ Using SEC direct (free, but rate limited to {SEC_EDGAR_RATE_LIMIT} req/sec)"
        )
        # With SEC direct, limit workers to respect rate limits
        if workers > DEFAULT_WORKERS:
            workers = DEFAULT_WORKERS
            logger.info(f"  Limited to {workers} workers to respect SEC rate limits")

    logger.info(f"Using {workers} parallel workers")
    logger.info("")

    # Ensure output directory exists
    FILINGS_DIR.mkdir(parents=True, exist_ok=True)

    # If force=True, delete ALL existing files upfront (clean slate approach)
    # This ensures you're not "down" with partial data - either all old or all new
    if force:
        logger.info("=" * 80)
        logger.info("FORCE MODE: Deleting all existing files upfront")
        logger.info("=" * 80)

        # Delete all HTML/XML files
        html_files = list(FILINGS_DIR.glob("**/*.html")) + list(FILINGS_DIR.glob("**/*.xml"))
        if html_files:
            logger.info(f"Deleting {len(html_files):,} existing HTML/XML files...")
            deleted_html = 0
            for html_file in html_files:
                try:
                    html_file.unlink()
                    deleted_html += 1
                except Exception as e:
                    logger.warning(f"  ⚠ Failed to delete {html_file}: {e}")
            logger.info(f"✓ Deleted {deleted_html:,} HTML/XML files")
        else:
            logger.info("No existing HTML/XML files to delete")

        # Delete all tar files
        tar_files = list(PORTFOLIOS_DIR.glob("**/*.tar"))
        if tar_files:
            logger.info(f"Deleting {len(tar_files):,} existing tar files...")
            deleted_tar = 0
            for tar_file in tar_files:
                try:
                    tar_file.unlink()
                    deleted_tar += 1
                except Exception as e:
                    logger.warning(f"  ⚠ Failed to delete {tar_file}: {e}")
            logger.info(f"✓ Deleted {deleted_tar:,} tar files")
        else:
            logger.info("No existing tar files to delete")

        logger.info("=" * 80)
        logger.info("All existing files deleted. Starting fresh downloads...")
        logger.info("=" * 80)
        logger.info("")

    # Thread-safe stats
    stats = ExecutionStats(downloaded=0, cached=0, no_10k_available=0, errors=0)

    # Time-based progress logging (logs to file every 30 seconds)
    import threading

    progress_lock = threading.Lock()
    progress_state = {"start_time": time.time(), "last_log_time": time.time(), "processed": 0}

    logger.info("Downloading 10-K filings...")
    logger.info("")

    # Worker function for parallel processing
    def process_company(company: dict) -> tuple[str, bool, Path | None, str | None]:
        """Process a single company (worker function for parallel execution)."""
        cik = company["cik"]
        ticker = company.get("ticker", "N/A")
        name = company.get("name", "Unknown")

        success, file_path, error = download_10k_for_company(
            cik,
            ticker,
            name,
            FILINGS_DIR,
            keep_tar_files=keep_tar_files,
            api_key=api_key,
            filing_date_start=filing_date_start,
            filing_date_end=filing_date_end,
            force=force,
        )

        return ticker, success, file_path, error

    # Result handler to update stats and collect failed companies
    def result_handler(company: dict, result: tuple[str, bool, Path | None, str | None]):
        """Handle result from worker function."""
        ticker_result, success, file_path, error = result
        ticker = company.get("ticker", "N/A")
        cik = company["cik"]

        if success:
            if file_path and file_path.exists():
                # Check if it was just downloaded (new file) or already existed
                file_age_hours = (time.time() - file_path.stat().st_mtime) / 3600
                if file_age_hours < 1:  # File modified in last hour = newly downloaded
                    stats.increment("downloaded")
                    logger.debug(f"✓ Downloaded: {ticker} ({cik})")
                else:
                    stats.increment("cached")
                    logger.debug(f"✓ Cached: {ticker} ({cik})")
            else:
                stats.increment("cached")
                logger.debug(f"✓ Cached: {ticker} ({cik})")
        else:
            # Distinguish between "no 10-K available" (expected) and actual errors
            if error and "No 10-K found" in error:
                # This is expected for many companies (ETFs, foreign companies, etc.)
                stats.increment("no_10k_available")
                failed_companies.append((ticker, cik, "no_10k", error))
                logger.debug(
                    f"⊘ No 10-K: {ticker} ({cik}) - expected for ETFs/foreign/inactive companies"
                )
            else:
                # Actual error (download failure, extraction failure, etc.)
                stats.increment("errors")
                failed_companies.append((ticker, cik, "error", error or "Unknown error"))
                logger.debug(f"✗ Error: {ticker} ({cik}): {error}")

        # Time-based progress logging (every 30 seconds to log file)
        with progress_lock:
            progress_state["processed"] += 1
            current_time = time.time()
            if current_time - progress_state["last_log_time"] >= 30:
                elapsed = current_time - progress_state["start_time"]
                processed = progress_state["processed"]
                rate = processed / elapsed if elapsed > 0 else 0
                remaining = (total - processed) / rate if rate > 0 else 0
                pct = (processed / total * 100) if total > 0 else 0
                logger.info(
                    f"  Progress: {processed:,}/{total:,} ({pct:.1f}%) | "
                    f"Rate: {rate:.1f}/sec | ETA: {remaining / 60:.1f}min | "
                    f"Downloaded: {stats.get('downloaded'):,} | Cached: {stats.get('cached'):,} | "
                    f"No 10-K: {stats.get('no_10k_available'):,} | Errors: {stats.get('errors'):,}"
                )
                progress_state["last_log_time"] = current_time

    # Error handler
    def error_handler(company: dict, error: Exception):
        """Handle errors from worker function."""
        ticker = company.get("ticker", "N/A")
        cik = company["cik"]
        logger.debug(f"Unexpected error processing {ticker} ({cik}): {error}")
        stats.increment("errors")

    # Process companies in parallel using utility
    # Redirect stdout at FD level to suppress datamule's print() spam
    # (tqdm uses stderr, so progress bar still shows)
    stdout_fd = sys.stdout.fileno()
    stdout_backup = os.dup(stdout_fd)
    devnull_fd = os.open(os.devnull, os.O_WRONLY)
    os.dup2(devnull_fd, stdout_fd)
    os.close(devnull_fd)
    try:
        execute_parallel(
            companies,
            process_company,
            max_workers=workers,
            desc="Downloading 10-Ks",
            unit="company",
            result_handler=result_handler,
            error_handler=error_handler,
            progress_postfix=lambda: {
                "downloaded": stats.get("downloaded"),
                "cached": stats.get("cached"),
                "no_10k": stats.get("no_10k_available"),
                "errors": stats.get("errors"),
            },
        )
    finally:
        # Restore stdout
        sys.stdout.flush()
        os.dup2(stdout_backup, stdout_fd)
        os.close(stdout_backup)

    # Get final stats
    downloaded = stats.get("downloaded")
    cached = stats.get("cached")
    no_10k_available = stats.get("no_10k_available")
    errors = stats.get("errors")

    # Final summary
    logger.info("")  # New line after progress bar
    logger.info("=" * 80)
    logger.info("Download Complete")
    logger.info("=" * 80)
    logger.info(f"Total companies processed: {total:,}")
    logger.info(f"✅ Successfully downloaded: {downloaded:,}")
    logger.info(f"✅ Already cached: {cached:,}")
    logger.info(
        f"⊘ No 10-K available: {no_10k_available:,} (expected for ETFs, foreign companies, inactive companies)"
    )
    logger.info(f"✗ Errors: {errors:,} (actual failures)")
    logger.info("")
    logger.info(f"Success rate: {100 * (downloaded + cached) / total:.1f}% of companies have 10-Ks")

    if errors > 0:
        logger.info("")
        logger.info("Companies with errors (first 10):")
        error_companies = [f for f in failed_companies if f[2] == "error"]
        for ticker, cik, _error_type, error in error_companies[:10]:
            logger.info(f"  {ticker} ({cik}): {error}")
        if len(error_companies) > 10:
            logger.info(f"  ... and {len(error_companies) - 10} more (see log file for details)")

    if no_10k_available > 0 and no_10k_available <= 20:
        # Only show sample if there aren't too many
        logger.info("")
        logger.info("Sample companies with no 10-K available (first 10):")
        no_10k_companies = [f for f in failed_companies if f[2] == "no_10k"]
        for ticker, cik, _error_type, _error in no_10k_companies[:10]:
            logger.info(f"  {ticker} ({cik})")

    logger.info("")

    return {
        "total": total,
        "downloaded": downloaded,
        "cached": cached,
        "no_10k_available": no_10k_available,
        "errors": errors,
        "failed": no_10k_available + errors,  # Total for backward compatibility
    }


def main():
    """Run the 10-K download script."""
    parser = argparse.ArgumentParser(
        description="Download most recent 10-K filing for all companies from SEC EDGAR (or Neo4j)"
    )
    add_execute_argument(parser)
    parser.add_argument(
        "--limit",
        type=int,
        help="Limit number of companies to process (for testing)",
    )
    parser.add_argument(
        "--from-neo4j",
        action="store_true",
        help="Get companies from Neo4j instead of SEC EDGAR (default: SEC EDGAR)",
    )
    # Tar files are ALWAYS kept - they're expensive to re-download and required for quality parsing
    # If you really need to delete them to save space, do it manually after understanding the consequences:
    # - You'll lose 86-93% parsing success rate (drops to 64% with custom parser)
    # - You'll need to pay again to re-download if you want quality parsing later
    # - Storage cost: ~272 GB (one-time) vs repeated API costs if you re-download
    parser.add_argument(
        "--workers",
        type=int,
        default=DEFAULT_WORKERS,
        help=f"Number of parallel workers (default: {DEFAULT_WORKERS}, auto-increased to {DEFAULT_WORKERS_WITH_API} with API key)",
    )
    parser.add_argument(
        "--filing-date-start",
        type=str,
        default="2020-01-01",
        help="Start date for filing search (YYYY-MM-DD, default: 2020-01-01)",
    )
    parser.add_argument(
        "--filing-date-end",
        type=str,
        default=None,
        help="End date for filing search (YYYY-MM-DD, default: current date + 1 year to include future filings)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force re-download: delete existing HTML files and tar files, then re-download (ensures latest filings)",
    )
    parser.add_argument(
        "--no-pre-filter",
        action="store_false",
        dest="pre_filter",
        help="Skip pre-filtering (not recommended - wastes credits on companies without 10-Ks)",
    )
    parser.add_argument(
        "--refresh-filter",
        action="store_true",
        help="Force refresh the pre-filter cache (re-query Datamule index). Use when new 10-Ks may have been filed.",
    )
    args = parser.parse_args()

    # Set up logging first so all messages are properly logged
    logger = setup_logging("download_10k_filings", execute=args.execute)

    # Check datamule availability
    if not DATAMULE_AVAILABLE:
        logger.error("datamule not installed")
        logger.error("Install with: pip install datamule")
        sys.exit(1)

    # Get Neo4j connection (only if needed)
    driver = None
    database = None
    if args.from_neo4j:
        driver, database = get_driver_and_database()
        try:
            if not verify_neo4j_connection(driver, database, logger):
                sys.exit(1)
        except Exception as e:
            logger.error(f"Failed to connect to Neo4j: {e}")
            sys.exit(1)

    try:
        # Download 10-Ks
        download_all_10ks(
            driver=driver,
            database=database,
            execute=args.execute,
            limit=args.limit,
            from_neo4j=args.from_neo4j,
            workers=args.workers,
            filing_date_start=args.filing_date_start,
            filing_date_end=args.filing_date_end,
            force=args.force,
            pre_filter=args.pre_filter,
            refresh_filter=args.refresh_filter,
        )

        if args.execute:
            logger.info("Next steps:")
            logger.info("  1. Run: python scripts/parse_10k_filings.py --execute")
            logger.info("     (Extracts company websites, business descriptions, competitors)")
            logger.info("  2. Update pipeline to use 10-K data instead of collect_domains.py")

    finally:
        if driver:
            driver.close()


if __name__ == "__main__":
    main()
