#!/usr/bin/env python3
"""
Parse downloaded 10-K filings to extract structured data.

This script extracts:
1. Company website (from cover page structured data)
2. Business description (Item 1: Business)
3. Risk factors (Item 1A: Risk Factors)
4. Filing metadata (filing date, accession number, fiscal year end)

The extracted data is stored in cache and used to populate the knowledge graph.

Usage:
    python scripts/parse_10k_filings.py                              # Dry-run (plan only)
    python scripts/parse_10k_filings.py --execute                    # Parse with 8 workers
    python scripts/parse_10k_filings.py --execute --force            # Re-parse and overwrite cache
    python scripts/parse_10k_filings.py --execute --incremental      # Add new fields to existing entries
    python scripts/parse_10k_filings.py --execute --skip-datamule    # Fast mode (custom parser only)
    python scripts/parse_10k_filings.py --execute --workers 4        # Use 4 parallel workers
"""

import argparse
import logging
import sys
from pathlib import Path

from public_company_graph.cache import get_cache
from public_company_graph.cli import (
    add_execute_argument,
    setup_logging,
)
from public_company_graph.config import get_data_dir
from public_company_graph.constants import DEFAULT_WORKERS

# Note: Parsing now uses pluggable interface (public_company_graph.parsing.base)
# Individual extractors are imported within parse_10k_file() for clarity
from public_company_graph.utils.file_discovery import find_10k_files
from public_company_graph.utils.stats import ExecutionStats

# Logger will be set up in main()
logger: logging.Logger | None = None

# Directories
FILINGS_DIR = get_data_dir() / "10k_filings"
CACHE_NAMESPACE = "10k_extracted"


def parse_10k_file(file_path: Path, skip_datamule: bool = False) -> dict:
    """
    Parse a single 10-K file and extract structured data.

    Uses the pluggable parser interface for extensibility.
    To add new extractors, implement TenKParser and add to get_default_parsers()
    in public_company_graph/parsing/base.py.

    Args:
        file_path: Path to 10-K HTML/XML file (must be within FILINGS_DIR)
        skip_datamule: If True, skip datamule and use custom parser only (faster)

    Returns:
        Dictionary with extracted data from all registered parsers
    """
    from public_company_graph.parsing.base import get_default_parsers, parse_10k_with_parsers

    # OPTIMIZATION: Read file once and reuse for all parsers
    # This avoids reading the same file multiple times (significant I/O savings)
    file_content = None
    if file_path.suffix == ".html":
        try:
            with open(file_path, encoding="utf-8", errors="ignore") as f:
                file_content = f.read()
        except Exception as e:
            if logger:
                logger.debug(f"Error reading file {file_path}: {e}")

    # Extract CIK from file path for parsers that need it
    cik = file_path.parent.name if file_path.parent.name.isdigit() else None

    # Find corresponding tar file for filing date extraction (fallback)
    tar_file = None
    if cik:
        from public_company_graph.config import get_data_dir

        portfolios_dir = get_data_dir() / "10k_portfolios"
        portfolio_dir = portfolios_dir / f"10k_{cik}"
        if portfolio_dir.exists():
            tar_files = list(portfolio_dir.glob("*.tar"))
            if tar_files:
                tar_file = tar_files[0]  # Use first tar file found

    # Get parsers from single source of truth
    parsers = get_default_parsers()

    # Parse using pluggable interface
    result = parse_10k_with_parsers(
        file_path,
        parsers,
        file_content=file_content,
        cik=cik,
        skip_datamule=skip_datamule,
        filings_dir=FILINGS_DIR,
        tar_file=tar_file,  # Pass tar file for filing date extraction
    )

    # Extract filing_metadata into top-level fields for backward compatibility
    if "filing_metadata" in result and result["filing_metadata"]:
        metadata = result["filing_metadata"]
        result["filing_date"] = metadata.get("filing_date")
        result["accession_number"] = metadata.get("accession_number")
        result["fiscal_year_end"] = metadata.get("fiscal_year_end")
        result["filing_year"] = metadata.get("filing_year")

    # Ensure backward compatibility
    result.setdefault("filing_date", None)
    result.setdefault("competitors", [])  # Ensure competitors is always a list

    return result


def parse_all_10ks(
    execute: bool = False,
    limit: int | None = None,
    force: bool = False,
    workers: int = DEFAULT_WORKERS,
    skip_datamule: bool = False,
    incremental: bool = False,
) -> dict[str, int]:
    """
    Parse all downloaded 10-K filings.

    Args:
        execute: If False, only print plan
        limit: Optional limit on number of files to process
        force: If True, re-parse files even if already cached (overwrites cache)
        workers: Number of parallel workers (default: 8)
        skip_datamule: If True, skip datamule and use custom parser only (faster)
        incremental: If True, merge new fields into existing cache entries

    Returns:
        Dict with counts: total, parsed, cached, failed
    """
    if not execute:
        logger.info("=" * 80)
        logger.info("DRY RUN MODE")
        logger.info("=" * 80)

        # Count files
        files = find_10k_files(FILINGS_DIR)
        count = len(files)

        logger.info(f"Would parse {count} 10-K files")
        logger.info(f"Output: Cache namespace '{CACHE_NAMESPACE}'")
        logger.info(f"Parallel workers: {workers}")
        if force:
            logger.info("⚠️  FORCE MODE: Will re-parse and overwrite existing cache entries")
        if incremental:
            logger.info("🔄 INCREMENTAL MODE: Will merge new fields into existing cache entries")
        if skip_datamule:
            logger.info(
                "⚡ FAST MODE: Skipping datamule (custom parser only, faster but lower quality)"
            )
        logger.info("=" * 80)
        logger.info("To execute, run: python scripts/parse_10k_filings.py --execute")
        if force:
            logger.info("To force re-parse: python scripts/parse_10k_filings.py --execute --force")
        if incremental:
            logger.info(
                "To add missing fields: python scripts/parse_10k_filings.py --execute --incremental"
            )
        return {"total": count, "parsed": 0, "cached": 0, "failed": 0, "updated": 0}

    # Get cache (will be closed automatically on script exit)
    cache = get_cache()

    # Log cache status upfront
    cache_stats = cache.stats()
    logger.info("Cache status:")
    logger.info(f"  Total entries: {cache_stats['total']:,}")
    logger.info(f"  Size: {cache_stats['size_mb']} MB")
    for ns, ns_count in sorted(cache_stats["by_namespace"].items(), key=lambda x: -x[1]):
        logger.info(f"    {ns}: {ns_count:,}")

    # Register cleanup handler to close cache on exit (handles Ctrl+C, Ctrl+Z, etc.)
    import atexit

    def cleanup_cache():
        try:
            cache.close()
        except Exception:
            pass  # Ignore errors during cleanup

    atexit.register(cleanup_cache)

    # Find all 10-K files
    files = find_10k_files(FILINGS_DIR, limit=limit)

    total = len(files)
    logger.info("=" * 80)
    logger.info("Parsing 10-K Filings")
    logger.info("=" * 80)
    if force:
        logger.info("⚠️  FORCE MODE: Re-parsing all files (overwriting cache)")
    if incremental:
        logger.info("🔄 INCREMENTAL MODE: Merging new fields into existing cache entries")
        logger.info("   Existing data (website, business_description) will be preserved")
        logger.info("   New fields (risk_factors) will be added")
    if skip_datamule:
        logger.info("⚡ FAST MODE: Skipping datamule (custom parser only)")
    logger.info(f"Found {total} 10-K files")
    logger.info(f"Using {workers} parallel workers")

    # Show what parsers are active (use the same list as parse_10k_file)
    from public_company_graph.parsing.base import get_default_parsers

    active_parsers = get_default_parsers()
    logger.info(f"Active parsers: {', '.join(p.field_name for p in active_parsers)}")
    logger.info("")

    if total == 0:
        logger.warning("⚠ No 10-K files found. Run download_10k_filings.py first.")
        return {"total": 0, "parsed": 0, "cached": 0, "failed": 0}

    # Thread-safe stats
    stats = ExecutionStats(parsed=0, cached=0, failed=0, updated=0)

    logger.info("Parsing 10-K files...")
    if incremental:
        logger.info("🔄 INCREMENTAL MODE: Adding new fields to existing cache entries")
    logger.info("")

    import time
    from concurrent.futures import ProcessPoolExecutor, as_completed

    from tqdm import tqdm

    from public_company_graph.utils.tenk_workers import parse_10k_worker

    # Cap workers at available CPU cores
    mp_workers = min(workers, 8)
    logger.info(f"Using {mp_workers} process workers")

    # Prepare args (all must be picklable - use strings for paths)
    filings_dir_str = str(FILINGS_DIR)
    args_list = [
        (str(f), f.parent.name, filings_dir_str, force, skip_datamule, incremental) for f in files
    ]

    # Execute with ProcessPoolExecutor
    results = []
    parsed = cached = failed = updated = 0

    # Time-based progress logging (logs to file every 30 seconds)
    start_time = time.time()
    last_log_time = start_time

    with ProcessPoolExecutor(max_workers=mp_workers) as executor:
        futures = {executor.submit(parse_10k_worker, args): args for args in args_list}

        with tqdm(total=total, desc="Parsing 10-Ks", unit="file") as pbar:
            for future in as_completed(futures):
                try:
                    cik, status, error = future.result()
                    if status == "cached":
                        cached += 1
                    elif status == "updated":
                        updated += 1
                    elif status == "parsed":
                        parsed += 1
                    else:
                        failed += 1
                    results.append((cik, status, error))
                except Exception as e:
                    failed += 1
                    results.append((None, "failed", str(e)))

                pbar.update(1)
                pbar.set_postfix(parsed=parsed, updated=updated, cached=cached, failed=failed)

                # Time-based progress logging (every 30 seconds to log file)
                current_time = time.time()
                if current_time - last_log_time >= 30:
                    processed = len(results)
                    elapsed = current_time - start_time
                    rate = processed / elapsed if elapsed > 0 else 0
                    remaining = (total - processed) / rate if rate > 0 else 0
                    pct = (processed / total * 100) if total > 0 else 0
                    logger.info(
                        f"  Progress: {processed:,}/{total:,} ({pct:.1f}%) | "
                        f"Rate: {rate:.1f} files/sec | ETA: {remaining / 60:.1f}min | "
                        f"Parsed: {parsed:,} | Updated: {updated:,} | "
                        f"Cached: {cached:,} | Failed: {failed:,}"
                    )
                    last_log_time = current_time

    # Update stats for reporting
    stats = ExecutionStats(parsed=parsed, cached=cached, failed=failed, updated=updated)

    parsed = stats.get("parsed")
    updated = stats.get("updated", 0)  # Incremental updates
    cached = stats.get("cached")
    failed = stats.get("failed")

    logger.info("")
    logger.info("=" * 80)
    logger.info("Parsing Complete")
    logger.info("=" * 80)
    logger.info(f"Total files: {total}")
    logger.info(f"Parsed: {parsed}")
    if incremental:
        logger.info(f"Updated: {updated} (incremental)")
    logger.info(f"Cached: {cached}")
    logger.info(f"Failed: {failed}")
    logger.info("")

    # Close cache connection
    try:
        cache.close()
    except Exception:
        pass  # Ignore errors during cleanup

    return {
        "total": total,
        "parsed": parsed,
        "updated": updated,
        "cached": cached,
        "failed": failed,
    }


def main():
    """Run the 10-K parsing script."""
    parser = argparse.ArgumentParser(description="Parse downloaded 10-K filings")
    add_execute_argument(parser)
    parser.add_argument(
        "--limit",
        type=int,
        help="Limit number of files to process (for testing)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-parse files even if already cached (overwrites existing cache entries)",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=DEFAULT_WORKERS,
        help=f"Number of parallel workers (default: {DEFAULT_WORKERS})",
    )
    parser.add_argument(
        "--skip-datamule",
        action="store_true",
        help="Skip datamule parsing (use custom parser only - faster but lower quality)",
    )
    parser.add_argument(
        "--incremental",
        action="store_true",
        help="Incremental mode: Merge new fields into existing cache entries (don't overwrite)",
    )
    args = parser.parse_args()

    # Set up logging (logger is used globally in this module)
    global logger
    logger = setup_logging("parse_10k_filings", execute=args.execute)

    # Check for BeautifulSoup
    try:
        from bs4 import BeautifulSoup  # noqa: F401

        del BeautifulSoup  # Used only for availability check
    except ImportError:
        logger.error("ERROR: beautifulsoup4 not installed")
        logger.error("Install with: pip install beautifulsoup4")
        sys.exit(1)

    # Parse 10-Ks
    parse_all_10ks(
        execute=args.execute,
        limit=args.limit,
        force=args.force,
        workers=args.workers,
        skip_datamule=args.skip_datamule,
        incremental=args.incremental,
    )

    if args.execute:
        logger.info("Next steps:")
        logger.info("  1. Review extracted data in cache")
        logger.info("  2. Update collect_domains.py to use 10-K websites")
        logger.info("  3. Update create_company_embeddings.py to use 10-K descriptions")


if __name__ == "__main__":
    main()
