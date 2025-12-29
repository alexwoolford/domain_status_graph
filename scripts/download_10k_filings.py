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
import sys
import time
from pathlib import Path

import requests

from domain_status_graph.cli import (
    add_execute_argument,
    get_driver_and_database,
    setup_logging,
    verify_neo4j_connection,
)
from domain_status_graph.config import get_data_dir, get_datamule_api_key
from domain_status_graph.constants import (
    DEFAULT_WORKERS,
    DEFAULT_WORKERS_WITH_API,
    SEC_EDGAR_LONG_DURATION_LIMIT,
    SEC_EDGAR_RATE_LIMIT,
)
from domain_status_graph.sources.sec_companies import (
    get_all_companies_from_neo4j,
    get_all_companies_from_sec,
)
from domain_status_graph.sources.sec_edgar_check import check_company_has_10k
from domain_status_graph.utils.datamule import suppress_datamule_output
from domain_status_graph.utils.parallel import execute_parallel
from domain_status_graph.utils.stats import ExecutionStats
from domain_status_graph.utils.tar_extraction import (
    extract_from_tar,
    get_filing_date_from_tar_name,
)
from domain_status_graph.utils.tar_selection import find_tar_with_latest_10k

# Try to import datamule
# Note: We set TQDM_DISABLE in suppress_datamule_output() context manager
# to disable datamule's internal progress bars
try:
    from datamule import Config, Portfolio

    DATAMULE_AVAILABLE = True
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
    skip_pre_check: bool = False,  # If True, skip pre-check and try all companies
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
        tar_file_with_latest = find_tar_with_latest_10k(tar_files)

        if not tar_file_with_latest:
            logger.warning(
                f"  ⚠ {ticker}: Could not identify tar file with latest 10-K (all may be empty)"
            )
            # Fallback: try all non-empty tar files (skip empty ones)
            from domain_status_graph.utils.tar_selection import is_tar_file_empty

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

    # Pre-check: Use free SEC EDGAR API to verify company has 10-Ks before calling datamule
    # This prevents expensive API calls for companies without 10-Ks (ETFs, funds, etc.)
    if api_key and not skip_pre_check:
        # Only pre-check if using paid API (to save money) and not disabled
        # For free SEC direct, we can skip pre-check (no cost if no files found)
        session = requests.Session()
        session.headers.update(
            {
                "User-Agent": "domain_status_graph script (contact: alexwoolford@example.com)",
            }
        )
        has_10k = check_company_has_10k(
            cik_padded,
            session=session,
            filing_date_start=filing_date_start,
            filing_date_end=filing_date_end,
        )
        if not has_10k:
            # Company doesn't have 10-Ks - skip expensive datamule API call
            logger.debug(
                f"  ⊘ {ticker}: No 10-K filings found (pre-check) - skipping datamule API call"
            )
            return (
                False,
                None,
                f"No 10-K found for {ticker} (CIK: {cik_padded}) - pre-checked via SEC EDGAR",
            )

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

            # Create portfolio in data/10k_portfolios/ directory
            portfolio = Portfolio(str(portfolio_path))

            # Suppress datamule's verbose output (redirect to log file)
            with suppress_datamule_output():
                # Set API key on portfolio if available (enables fast datamule-sgml provider)
                if api_key:
                    portfolio.set_api_key(api_key)
                    # Use datamule-sgml provider (fast, no rate limits)
                    # NOTE: Pre-check above should prevent most wasted API calls
                    portfolio.download_submissions(
                        submission_type="10-K",
                        cik=cik_padded,
                        filing_date=(filing_date_start, filing_date_end),  # Configurable date range
                        provider="datamule-sgml",  # Fast provider (requires API key)
                        # No requests_per_second needed - datamule-sgml has no rate limits
                    )
                else:
                    # Fallback to SEC direct (free, but rate limited)
                    portfolio.download_submissions(
                        submission_type="10-K",
                        cik=cik_padded,
                        filing_date=(filing_date_start, filing_date_end),  # Configurable date range
                        provider="sec",  # Download directly from SEC
                        requests_per_second=SEC_EDGAR_LONG_DURATION_LIMIT,  # SEC long-duration limit
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
            tar_file_with_latest = find_tar_with_latest_10k(tar_files)

            if not tar_file_with_latest:
                logger.warning(
                    f"  ⚠ {ticker}: Could not identify tar file with latest 10-K (all may be empty)"
                )
                # Fallback: try all non-empty tar files (skip empty ones)
                from domain_status_graph.utils.tar_selection import is_tar_file_empty

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
    skip_pre_check: bool = False,  # If True, skip pre-check and try all companies (for investigation)
    force: bool = False,  # If True, delete existing files and re-download
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

    total = len(companies)

    # Track different outcomes (initialized here, used in execute block)
    downloaded = 0
    cached = 0
    no_10k_available = 0  # Companies that legitimately don't have 10-Ks (ETFs, foreign, etc.)
    errors = 0  # Actual errors (download failures, extraction failures)
    failed_companies = []  # List of (ticker, cik, error_type, error_message)

    # Set default end date to current year + 1 (to include future filings)
    if filing_date_end is None:
        from datetime import datetime, timedelta

        filing_date_end = (datetime.now() + timedelta(days=365)).strftime("%Y-%m-%d")

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
        if skip_pre_check:
            logger.info("⚠️  PRE-CHECK DISABLED: Will try all companies (may waste API calls)")
        else:
            logger.info("✓ Pre-check enabled: Will filter companies without 10-Ks before API calls")
        logger.info("✓ Tar files will be KEPT after extraction (~100-300 GB estimated total)")
        logger.info(
            "  (Required for high-quality datamule parsing: 86-93% success vs 64% custom parser)"
        )
        logger.info(
            "  (Once deleted, you must pay again to re-download - keeping them is recommended)"
        )
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
    if skip_pre_check:
        logger.info("⚠️  PRE-CHECK DISABLED: Will try all companies (may waste API calls)")
    else:
        logger.info("✓ Pre-check enabled: Will filter companies without 10-Ks before API calls")
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

    logger.info("Downloading 10-K filings...")
    logger.info("Progress bar shows overall progress; detailed logs in log file.")
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
            skip_pre_check=skip_pre_check,
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
            if error and ("No 10-K found" in error or "pre-checked via SEC EDGAR" in error):
                # This is expected for many companies (ETFs, foreign companies, etc.)
                # Includes both pre-checked (via SEC EDGAR) and post-download (via datamule) results
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

    # Error handler
    def error_handler(company: dict, error: Exception):
        """Handle errors from worker function."""
        ticker = company.get("ticker", "N/A")
        cik = company["cik"]
        logger.debug(f"Unexpected error processing {ticker} ({cik}): {error}")
        stats.increment("errors")

    # Process companies in parallel using utility
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
    logger.info(f"Success rate: {100*(downloaded + cached)/total:.1f}% of companies have 10-Ks")

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
        "--no-pre-check",
        action="store_true",
        help="Skip pre-check and try all companies (for investigation - may waste API calls)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force re-download: delete existing HTML files and tar files, then re-download (ensures latest filings)",
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
            skip_pre_check=args.no_pre_check,
            force=args.force,  # Pass force flag to actually delete files
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
