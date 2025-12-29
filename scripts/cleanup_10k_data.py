#!/usr/bin/env python3
"""
Clean up 10-K download artifacts for a fresh start.

This script removes:
- All extracted HTML files from data/10k_filings/
- All tar files from data/10k_portfolios/
- Empty directories (including datamule fallback directories)

Usage:
    python scripts/cleanup_10k_data.py          # Dry-run (show what would be deleted)
    python scripts/cleanup_10k_data.py --execute  # Actually delete files
"""

import argparse

from domain_status_graph.cli import add_execute_argument, setup_logging
from domain_status_graph.config import get_data_dir

FILINGS_DIR = get_data_dir() / "10k_filings"
PORTFOLIOS_DIR = get_data_dir() / "10k_portfolios"
DATA_DIR = get_data_dir()  # For finding project root


def cleanup_10k_data(execute: bool = False):
    """
    Clean up 10-K download artifacts.

    Args:
        execute: If False, only show what would be deleted
    """
    logger = setup_logging("cleanup_10k_data", execute=execute)

    if not execute:
        logger.info("=" * 80)
        logger.info("DRY RUN MODE - No files will be deleted")
        logger.info("=" * 80)

    # Count files to delete
    html_files = list(FILINGS_DIR.glob("**/*.html")) + list(FILINGS_DIR.glob("**/*.xml"))
    tar_files = list(PORTFOLIOS_DIR.glob("**/*.tar"))

    html_count = len(html_files)
    tar_count = len(tar_files)

    # Calculate sizes
    html_size = sum(f.stat().st_size for f in html_files if f.exists())
    tar_size = sum(f.stat().st_size for f in tar_files if f.exists())

    html_size_mb = html_size / (1024 * 1024)
    tar_size_mb = tar_size / (1024 * 1024)

    logger.info(f"Found {html_count} HTML/XML files ({html_size_mb:.1f} MB)")
    logger.info(f"Found {tar_count} tar files ({tar_size_mb:.1f} MB)")
    logger.info("")

    if html_count == 0 and tar_count == 0:
        logger.info("✓ No files to clean up - already clean!")
        return

    if not execute:
        logger.info("Would delete:")
        if html_count > 0:
            logger.info(f"  - {html_count} HTML/XML files from {FILINGS_DIR}")
        if tar_count > 0:
            logger.info(f"  - {tar_count} tar files from {PORTFOLIOS_DIR}")
        logger.info("")
        logger.info("To actually delete, run: python scripts/cleanup_10k_data.py --execute")
        return

    # Actually delete files
    logger.info("=" * 80)
    logger.info("Cleaning up 10-K data")
    logger.info("=" * 80)

    deleted_html = 0
    deleted_tar = 0

    # Delete HTML files
    if html_count > 0:
        logger.info(f"Deleting {html_count} HTML/XML files...")
        for file_path in html_files:
            try:
                file_path.unlink()
                deleted_html += 1
            except Exception as e:
                logger.warning(f"  Failed to delete {file_path}: {e}")

    # Delete tar files
    if tar_count > 0:
        logger.info(f"Deleting {tar_count} tar files...")
        for file_path in tar_files:
            try:
                file_path.unlink()
                deleted_tar += 1
            except Exception as e:
                logger.warning(f"  Failed to delete {file_path}: {e}")

    # Remove empty directories
    logger.info("Removing empty directories...")

    # Remove empty company directories in filings
    if FILINGS_DIR.exists():
        for company_dir in FILINGS_DIR.iterdir():
            if company_dir.is_dir():
                try:
                    # Check if directory is empty or only contains empty subdirectories
                    if not any(company_dir.iterdir()):
                        company_dir.rmdir()
                        logger.debug(f"  Removed empty directory: {company_dir}")
                    else:
                        # Try to remove empty subdirectories
                        for subdir in company_dir.iterdir():
                            if subdir.is_dir() and not any(subdir.iterdir()):
                                subdir.rmdir()
                                logger.debug(f"  Removed empty subdirectory: {subdir}")
                        # If company dir is now empty, remove it
                        if not any(company_dir.iterdir()):
                            company_dir.rmdir()
                            logger.debug(f"  Removed empty directory: {company_dir}")
                except Exception as e:
                    logger.debug(f"  Could not remove {company_dir}: {e}")

    # Remove empty portfolio directories
    if PORTFOLIOS_DIR.exists():
        for portfolio_dir in PORTFOLIOS_DIR.iterdir():
            if portfolio_dir.is_dir():
                try:
                    if not any(portfolio_dir.iterdir()):
                        portfolio_dir.rmdir()
                        logger.debug(f"  Removed empty portfolio directory: {portfolio_dir}")
                except Exception as e:
                    logger.debug(f"  Could not remove {portfolio_dir}: {e}")

    # Remove datamule fallback directories (created in project root on failed downloads)
    # These are empty directories created by datamule when downloads fail
    project_root = DATA_DIR.parent
    fallback_dirs = list(project_root.glob("fallback_*"))
    if fallback_dirs:
        logger.info(f"Found {len(fallback_dirs)} fallback directories in project root")
        removed_count = 0
        for fallback_dir in fallback_dirs:
            if fallback_dir.is_dir():
                try:
                    # Only remove if empty (datamule creates these but doesn't always populate them)
                    if not any(fallback_dir.iterdir()):
                        if execute:
                            fallback_dir.rmdir()
                        removed_count += 1
                        logger.debug(
                            f"  {'Removed' if execute else 'Would remove'} empty fallback directory: {fallback_dir.name}"
                        )
                    else:
                        logger.warning(
                            f"  Fallback directory {fallback_dir.name} is not empty, skipping"
                        )
                except Exception as e:
                    logger.debug(f"  Could not remove {fallback_dir}: {e}")
        if execute:
            logger.info(f"  Removed {removed_count} empty fallback directories")
        else:
            logger.info(f"  Would remove {removed_count} empty fallback directories")

    # Remove test directories (created by dev scripts for datamule testing)
    test_dirs = list(project_root.glob("test_*"))
    if test_dirs:
        logger.info(f"Found {len(test_dirs)} test directories in project root")
        removed_count = 0
        for test_dir in test_dirs:
            if test_dir.is_dir():
                try:
                    # Only remove if empty (test scripts create these but don't always populate them)
                    if not any(test_dir.iterdir()):
                        if execute:
                            test_dir.rmdir()
                        removed_count += 1
                        logger.debug(
                            f"  {'Removed' if execute else 'Would remove'} empty test directory: {test_dir.name}"
                        )
                    else:
                        logger.warning(f"  Test directory {test_dir.name} is not empty, skipping")
                except Exception as e:
                    logger.debug(f"  Could not remove {test_dir}: {e}")
        if execute:
            logger.info(f"  Removed {removed_count} empty test directories")
        else:
            logger.info(f"  Would remove {removed_count} empty test directories")

    # Remove quality test directories (created by dev scripts for quality testing)
    quality_test_dirs = list(project_root.glob("quality_test_*"))
    if quality_test_dirs:
        logger.info(f"Found {len(quality_test_dirs)} quality test directories in project root")
        removed_count = 0
        for quality_test_dir in quality_test_dirs:
            if quality_test_dir.is_dir():
                try:
                    # Only remove if empty (quality test scripts create these but don't always populate them)
                    if not any(quality_test_dir.iterdir()):
                        if execute:
                            quality_test_dir.rmdir()
                        removed_count += 1
                        logger.debug(
                            f"  {'Removed' if execute else 'Would remove'} empty quality test directory: {quality_test_dir.name}"
                        )
                    else:
                        logger.warning(
                            f"  Quality test directory {quality_test_dir.name} is not empty, skipping"
                        )
                except Exception as e:
                    logger.debug(f"  Could not remove {quality_test_dir}: {e}")
        if execute:
            logger.info(f"  Removed {removed_count} empty quality test directories")
        else:
            logger.info(f"  Would remove {removed_count} empty quality test directories")

    # Remove parent directories if empty
    try:
        if FILINGS_DIR.exists() and not any(FILINGS_DIR.iterdir()):
            FILINGS_DIR.rmdir()
            logger.debug(f"  Removed empty filings directory: {FILINGS_DIR}")
    except Exception:
        pass

    try:
        if PORTFOLIOS_DIR.exists() and not any(PORTFOLIOS_DIR.iterdir()):
            PORTFOLIOS_DIR.rmdir()
            logger.debug(f"  Removed empty portfolios directory: {PORTFOLIOS_DIR}")
    except Exception:
        pass

    logger.info("")
    logger.info("=" * 80)
    logger.info("Cleanup Complete")
    logger.info("=" * 80)
    logger.info(f"Deleted {deleted_html} HTML/XML files")
    logger.info(f"Deleted {deleted_tar} tar files")
    logger.info("")
    logger.info("Ready for a fresh download run!")


def main():
    """Run the cleanup script."""
    parser = argparse.ArgumentParser(
        description="Clean up 10-K download artifacts for a fresh start"
    )
    add_execute_argument(parser)
    args = parser.parse_args()

    cleanup_10k_data(execute=args.execute)


if __name__ == "__main__":
    main()
