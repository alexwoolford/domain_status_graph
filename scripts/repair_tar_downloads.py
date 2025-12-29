#!/usr/bin/env python3
"""
Repair problematic tar file downloads.

This script:
1. Identifies all tar files with issues (corrupt, truncated, empty, incomplete)
2. Backs up the problematic files
3. Re-downloads them from datamule
4. Verifies the re-downloaded files
5. Reports on the results

Usage:
    python scripts/repair_tar_downloads.py          # Dry-run (analyze only)
    python scripts/repair_tar_downloads.py --repair  # Actually repair
"""

import argparse
import logging
import shutil
import sys
import tarfile
from datetime import datetime
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from domain_status_graph.cli import setup_logging
from domain_status_graph.utils.datamule import suppress_datamule_output
from domain_status_graph.utils.tar_selection import (
    get_latest_10k_filing_date_from_tar,
)


def analyze_tar_file(tar_path: Path) -> dict:
    """
    Analyze a tar file and return its status.

    Returns dict with:
        - status: 'ok', 'empty', 'corrupt', 'truncated', 'incomplete', 'txt_only'
        - size: file size in bytes
        - member_count: number of members (if readable)
        - html_count: number of HTML files (if readable)
        - error: error message (if any)
        - filing_date: extracted filing date (if available)
    """
    result = {
        "path": tar_path,
        "status": "unknown",
        "size": tar_path.stat().st_size if tar_path.exists() else 0,
        "member_count": 0,
        "html_count": 0,
        "txt_count": 0,
        "error": None,
        "filing_date": None,
        "sample_files": [],
    }

    # Check for obvious issues first
    if result["size"] == 0:
        result["status"] = "zero_bytes"
        result["error"] = "File is 0 bytes"
        return result

    if result["size"] < 100:
        result["status"] = "truncated"
        result["error"] = f"File is only {result['size']} bytes (truncated)"
        return result

    # Try to open and analyze
    try:
        with tarfile.open(tar_path, "r") as tar:
            members = tar.getmembers()
            result["member_count"] = len(members)

            if len(members) == 0:
                result["status"] = "empty_tar"
                result["error"] = "Tar file contains no members"
                return result

            # Count file types
            html_members = [m for m in members if m.name.endswith((".htm", ".html"))]
            txt_members = [m for m in members if m.name.endswith(".txt")]
            result["html_count"] = len(html_members)
            result["txt_count"] = len(txt_members)
            result["sample_files"] = [m.name for m in members[:5]]

            if result["html_count"] == 0:
                if result["txt_count"] > 0:
                    result["status"] = "txt_only"
                    result["error"] = f"Contains {result['txt_count']} TXT files but no HTML"
                else:
                    result["status"] = "no_html"
                    result["error"] = "Contains files but no HTML or TXT"
                return result

            # Try to extract filing date
            result["filing_date"] = get_latest_10k_filing_date_from_tar(tar_path)
            result["status"] = "ok"
            return result

    except tarfile.TarError as e:
        error_str = str(e).lower()
        if "truncated" in error_str or "eof" in error_str:
            result["status"] = "incomplete"
            result["error"] = f"Tar file is incomplete: {e}"
        elif "not a gzip" in error_str or "invalid" in error_str:
            result["status"] = "corrupt"
            result["error"] = f"Tar file is corrupt: {e}"
        else:
            result["status"] = "corrupt"
            result["error"] = f"Tar file error: {e}"
        return result
    except Exception as e:
        result["status"] = "error"
        result["error"] = str(e)
        return result


def get_cik_from_portfolio_dir(dir_name: str) -> str:
    """Extract CIK from portfolio directory name like '10k_0000320193'."""
    if dir_name.startswith("10k_"):
        return dir_name[4:]
    return dir_name


def redownload_tar_files(
    cik: str,
    portfolio_path: Path,
    api_key: str | None,
    filing_date_start: str = "2020-01-01",
    filing_date_end: str | None = None,
    logger: logging.Logger | None = None,
) -> tuple[bool, str]:
    """
    Re-download tar files for a CIK.

    Returns (success, message).
    """
    if logger is None:
        logger = logging.getLogger(__name__)

    if filing_date_end is None:
        filing_date_end = datetime.now().strftime("%Y-%m-%d")

    try:
        from datamule import Portfolio
    except ImportError:
        return False, "datamule not installed"

    # Delete existing tar files in the portfolio
    existing_tars = list(portfolio_path.glob("*.tar"))
    for tar_file in existing_tars:
        try:
            tar_file.unlink()
            logger.debug(f"  Deleted old tar: {tar_file.name}")
        except Exception as e:
            logger.warning(f"  Failed to delete {tar_file.name}: {e}")

    # Re-download
    try:
        portfolio = Portfolio(str(portfolio_path))

        with suppress_datamule_output():
            if api_key:
                portfolio.set_api_key(api_key)
                portfolio.download_submissions(
                    submission_type="10-K",
                    cik=cik,
                    filing_date=(filing_date_start, filing_date_end),
                    provider="datamule-sgml",
                )
            else:
                portfolio.download_submissions(
                    submission_type="10-K",
                    cik=cik,
                    filing_date=(filing_date_start, filing_date_end),
                    provider="sec",
                    requests_per_second=5,
                )

        # Check what we got
        new_tars = list(portfolio_path.glob("*.tar"))
        if not new_tars:
            return False, "No tar files downloaded (company may have no 10-K filings)"

        return True, f"Downloaded {len(new_tars)} tar file(s)"

    except Exception as e:
        return False, f"Download error: {e}"


def main():
    parser = argparse.ArgumentParser(description="Repair problematic tar downloads")
    parser.add_argument("--repair", action="store_true", help="Actually repair (default: dry-run)")
    parser.add_argument("--cik", help="Only repair specific CIK")
    parser.add_argument("--backup-dir", default="data/tar_backup", help="Backup directory")
    args = parser.parse_args()

    # Setup logging
    logger = setup_logging("repair_tar_downloads", execute=args.repair)

    # Get API key
    import os

    from dotenv import load_dotenv

    load_dotenv()
    api_key = os.getenv("DATAMULE_API_KEY")

    logger.info("=" * 80)
    logger.info("TAR FILE REPAIR DIAGNOSTIC")
    logger.info("=" * 80)

    # Find all tar files
    portfolios_dir = Path("data/10k_portfolios")
    if not portfolios_dir.exists():
        logger.error(f"Portfolio directory not found: {portfolios_dir}")
        return 1

    # Analyze all tar files
    logger.info("\n1. ANALYZING ALL TAR FILES...")
    logger.info("-" * 80)

    all_results = []
    problematic = []
    company_dirs = sorted(portfolios_dir.glob("10k_*"))

    if args.cik:
        company_dirs = [d for d in company_dirs if args.cik in d.name]
        logger.info(f"Filtering to CIK: {args.cik}")

    for company_dir in company_dirs:
        cik = get_cik_from_portfolio_dir(company_dir.name)
        tar_files = list(company_dir.glob("*.tar"))

        for tar_file in tar_files:
            result = analyze_tar_file(tar_file)
            result["cik"] = cik
            all_results.append(result)

            if result["status"] != "ok":
                problematic.append(result)

    # Summary by status
    from collections import Counter

    status_counts = Counter(r["status"] for r in all_results)

    logger.info(f"\nTotal tar files analyzed: {len(all_results)}")
    logger.info("\nStatus breakdown:")
    for status, count in sorted(status_counts.items(), key=lambda x: -x[1]):
        emoji = "✓" if status == "ok" else "⚠" if status in ("txt_only", "empty_tar") else "✗"
        logger.info(f"  {emoji} {status}: {count}")

    if not problematic:
        logger.info("\n✓ All tar files are OK!")
        return 0

    # Show problematic files grouped by issue type
    logger.info(f"\n2. PROBLEMATIC FILES ({len(problematic)} total)")
    logger.info("-" * 80)

    issues_by_type = {}
    for result in problematic:
        status = result["status"]
        if status not in issues_by_type:
            issues_by_type[status] = []
        issues_by_type[status].append(result)

    # Categorize what can be repaired vs what's expected
    repairable_statuses = {"zero_bytes", "truncated", "incomplete", "corrupt", "error"}
    expected_statuses = {"empty_tar", "txt_only", "no_html"}

    repairable = [r for r in problematic if r["status"] in repairable_statuses]
    # expected_statuses used for filtering display categories, results checked inline
    _ = [r for r in problematic if r["status"] in expected_statuses]

    for status, results in sorted(issues_by_type.items()):
        is_repairable = status in repairable_statuses
        emoji = "🔧" if is_repairable else "📋"
        action = "CAN REPAIR" if is_repairable else "EXPECTED (datamule limitation)"

        logger.info(f"\n{emoji} {status.upper()} ({len(results)} files) - {action}")
        for r in results[:5]:  # Show first 5
            parent = r["path"].parent.name
            logger.info(f"    {parent}/{r['path'].name}")
            logger.info(f"      Size: {r['size']:,} bytes | Error: {r['error']}")
            if r["sample_files"]:
                logger.info(f"      Sample files: {r['sample_files'][:3]}")
        if len(results) > 5:
            logger.info(f"    ... and {len(results) - 5} more")

    # Repair section
    if repairable:
        logger.info("\n3. REPAIR PLAN")
        logger.info("-" * 80)

        # Group by CIK for efficient re-download
        ciks_to_repair = {}
        for r in repairable:
            cik = r["cik"]
            if cik not in ciks_to_repair:
                ciks_to_repair[cik] = []
            ciks_to_repair[cik].append(r)

        logger.info(f"Companies to re-download: {len(ciks_to_repair)}")
        for cik, files in ciks_to_repair.items():
            logger.info(f"  CIK {cik}: {len(files)} problematic file(s)")

        if not args.repair:
            logger.info("\n" + "=" * 80)
            logger.info("DRY RUN - No changes made")
            logger.info("To repair, run: python scripts/repair_tar_downloads.py --repair")
            logger.info("=" * 80)
            return 0

        # Actually repair
        logger.info(f"\n4. REPAIRING ({len(ciks_to_repair)} companies)...")
        logger.info("-" * 80)

        # Create backup directory
        backup_dir = Path(args.backup_dir)
        backup_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        repair_results = {"success": 0, "failed": 0, "unchanged": 0}

        for cik, files in ciks_to_repair.items():
            portfolio_path = portfolios_dir / f"10k_{cik}"
            logger.info(f"\n  CIK {cik}:")

            # Backup problematic files
            cik_backup_dir = backup_dir / f"{cik}_{timestamp}"
            cik_backup_dir.mkdir(parents=True, exist_ok=True)
            for r in files:
                if r["path"].exists():
                    shutil.copy2(r["path"], cik_backup_dir / r["path"].name)
                    logger.info(f"    Backed up: {r['path'].name}")

            # Re-download
            success, message = redownload_tar_files(
                cik=cik,
                portfolio_path=portfolio_path,
                api_key=api_key,
                logger=logger,
            )

            if success:
                # Verify the new files
                new_tars = list(portfolio_path.glob("*.tar"))
                all_ok = True
                for tar_file in new_tars:
                    new_result = analyze_tar_file(tar_file)
                    if new_result["status"] == "ok":
                        logger.info(
                            f"    ✓ {tar_file.name}: OK (date: {new_result['filing_date']})"
                        )
                    else:
                        logger.warning(
                            f"    ⚠ {tar_file.name}: {new_result['status']} - {new_result['error']}"
                        )
                        all_ok = False

                if all_ok:
                    repair_results["success"] += 1
                else:
                    repair_results["unchanged"] += 1
            else:
                logger.warning(f"    ✗ Failed: {message}")
                repair_results["failed"] += 1

        # Final summary
        logger.info("\n" + "=" * 80)
        logger.info("REPAIR SUMMARY")
        logger.info("=" * 80)
        logger.info(f"  ✓ Successfully repaired: {repair_results['success']} companies")
        logger.info(f"  ⚠ Partially repaired: {repair_results['unchanged']} companies")
        logger.info(f"  ✗ Failed to repair: {repair_results['failed']} companies")
        logger.info(f"\n  Backups saved to: {backup_dir}")

    else:
        logger.info("\n" + "=" * 80)
        logger.info("No repairable issues found (all issues are expected datamule limitations)")
        logger.info("=" * 80)

    return 0


if __name__ == "__main__":
    sys.exit(main())
