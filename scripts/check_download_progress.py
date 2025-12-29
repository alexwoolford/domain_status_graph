#!/usr/bin/env python3
"""
Quick diagnostic script to check download progress and verify date ranges.

Usage:
    python scripts/check_download_progress.py
"""

import sys
from collections import Counter
from datetime import datetime

from domain_status_graph.config import get_data_dir
from domain_status_graph.utils.tar_selection import get_latest_10k_filing_date_from_tar

FILINGS_DIR = get_data_dir() / "10k_filings"
PORTFOLIOS_DIR = get_data_dir() / "10k_portfolios"


def check_progress():
    """Check download progress and file dates."""
    print("=" * 80)
    print("Download Progress Check")
    print("=" * 80)
    print()

    # Count HTML files
    html_files = list(FILINGS_DIR.glob("**/*.html"))
    xml_files = list(FILINGS_DIR.glob("**/*.xml"))
    total_extracted = len(html_files) + len(xml_files)

    print(f"📄 Extracted HTML/XML files: {total_extracted:,}")
    print(f"   - HTML: {len(html_files):,}")
    print(f"   - XML: {len(xml_files):,}")
    print()

    # Count tar files
    tar_files = list(PORTFOLIOS_DIR.glob("**/*.tar"))
    print(f"📦 Tar files: {len(tar_files):,}")
    print()

    # Analyze tar file dates
    if tar_files:
        print("📅 Analyzing filing dates of tar files...")
        print()

        years = []
        dates = []

        # Sample up to 100 files (to avoid being too slow)
        sample_size = min(100, len(tar_files))
        sample_files = tar_files[:sample_size]

        print(
            f"   Analyzing {sample_size} files (sampling first {sample_size} of {len(tar_files)})..."
        )

        for tar_file in sample_files:
            date = get_latest_10k_filing_date_from_tar(tar_file)
            if date:
                years.append(date.year)
                dates.append(date)

        if years:
            year_counts = Counter(years)
            print()
            print("   Year distribution (sample):")
            for year in sorted(year_counts.keys(), reverse=True):
                count = year_counts[year]
                pct = (count / len(years)) * 100
                print(f"   - {year}: {count} files ({pct:.1f}%)")

            print()
            if dates:
                latest = max(dates)
                oldest = min(dates)
                print(f"   Latest filing date: {latest.strftime('%Y-%m-%d')} ({latest.year})")
                print(f"   Oldest filing date: {oldest.strftime('%Y-%m-%d')} ({oldest.year})")
                print()

                # Check if we're getting recent files
                current_year = datetime.now().year
                recent_count = sum(1 for y in years if y >= current_year - 1)
                recent_pct = (recent_count / len(years)) * 100

                print(
                    f"   Recent files (≥{current_year - 1}): {recent_count}/{len(years)} ({recent_pct:.1f}%)"
                )

                if recent_pct < 50:
                    print()
                    print("   ⚠️  WARNING: Less than 50% of files are from recent years!")
                    print("   This might indicate the date range isn't working correctly.")
                else:
                    print()
                    print("   ✅ Good: Majority of files are from recent years")
        else:
            print("   ⚠️  Could not extract dates from tar files")
    else:
        print("   No tar files found yet (downloads may still be in progress)")

    print()
    print("=" * 80)
    print()
    print("💡 Tips:")
    print("   - Check the log file for detailed progress: logs/download_10k_filings_*.log")
    print("   - Look for '📅' entries showing date ranges being used")
    print("   - Look for '✓' entries showing successful downloads with dates")
    print("   - Run this script periodically to watch progress")
    print()


if __name__ == "__main__":
    try:
        check_progress()
    except KeyboardInterrupt:
        print("\n\nInterrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n\nError: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)
