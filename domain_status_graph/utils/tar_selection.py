"""
Utilities for identifying which tar file contains the latest 10-K filing.

This module provides a robust, repeatable way to select the correct tar file
by inspecting tar contents to find the most recent 10-K filing date.
"""

import logging
import re
import tarfile
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)


def extract_filing_date_from_html_path(html_path: str) -> datetime | None:
    """
    Extract filing date from HTML file path within a tar archive.

    SEC HTML files are typically named like:
    - a-20241231.htm (main 10-K)
    - etr-20221231.htm (main 10-K)
    - {CIK}{date}/etr-{date}.htm

    Args:
        html_path: Path to HTML file within tar archive

    Returns:
        datetime object if date can be extracted, None otherwise
    """
    # Pattern 1: a-YYYYMMDD.htm or etr-YYYYMMDD.htm
    match = re.search(r"[a-z]+-(\d{8})\.(htm|html)", html_path, re.IGNORECASE)
    if match:
        date_str = match.group(1)
        try:
            return datetime.strptime(date_str, "%Y%m%d")
        except ValueError:
            pass

    # Pattern 2a: SEC accession number with full YYYYMMDD date
    # Format: {10-digit-CIK}{8-digit-YYYYMMDD}/filename
    # Example: 000010908720231231/10k.htm = CIK 0000109087, date 2023-12-31
    # NOTE: Must try this BEFORE Pattern 2b since both match 18 digits
    match = re.search(r"\d{10}(\d{8})", html_path)
    if match:
        date_part = match.group(1)
        try:
            date_obj = datetime.strptime(date_part, "%Y%m%d")
            # Validate it's a reasonable date
            if 1990 <= date_obj.year <= datetime.now().year + 1:
                return date_obj
        except ValueError:
            pass  # Not a valid date, try Pattern 2b

    # Pattern 2b: SEC accession number with YY year only
    # Format: {10-digit-CIK}{2-digit-YY}{6-digit-sequence}/filename
    # Example: 000114036114016669/form10k.htm = CIK 0001140361, year 14 (2014), sequence 016669
    # Note: This format only contains YEAR, not full date (no month/day)
    match = re.search(r"\d{10}(\d{2})\d{6}", html_path)
    if match:
        year_part = match.group(1)  # 2 digits (YY)
        try:
            year = int(f"20{year_part}")
            # Validate it's a reasonable year (2000-2099)
            if 2000 <= year <= datetime.now().year + 1:
                # Return Jan 1 of that year (best approximation without full date)
                return datetime(year, 1, 1)
        except ValueError:
            pass

    # Pattern 3: Look for YYYYMMDD pattern anywhere in the path (but not at start)
    # Avoid matching CIK digits at the start
    match = re.search(r"[^\d](\d{4})(\d{2})(\d{2})", html_path)
    if match:
        date_str = f"{match.group(1)}{match.group(2)}{match.group(3)}"
        try:
            # Validate it's a reasonable date (not part of CIK)
            date_obj = datetime.strptime(date_str, "%Y%m%d")
            # Check it's not too old (before 1990) or in future
            if 1990 <= date_obj.year <= datetime.now().year + 1:
                return date_obj
        except ValueError:
            pass

    # Pattern 3: YYYY-MM-DD in path
    match = re.search(r"(\d{4})-(\d{2})-(\d{2})", html_path)
    if match:
        date_str = f"{match.group(1)}{match.group(2)}{match.group(3)}"
        try:
            return datetime.strptime(date_str, "%Y%m%d")
        except ValueError:
            pass

    return None


def get_latest_10k_filing_date_from_tar(tar_file: Path) -> datetime | None:
    """
    Inspect tar file contents to find the latest 10-K filing date.

    This function:
    1. Opens the tar file
    2. Finds all HTML files (potential 10-Ks)
    3. Extracts filing dates from filenames
    4. Returns the most recent date

    Args:
        tar_file: Path to tar file

    Returns:
        datetime of latest 10-K filing, or None if no valid 10-K found
    """
    try:
        with tarfile.open(tar_file, "r") as tar:
            html_members = [m for m in tar.getmembers() if m.name.endswith((".html", ".htm"))]

            if not html_members:
                return None

            # Extract dates from all HTML files
            filing_dates = []
            for member in html_members:
                # Skip exhibits, TOC, etc.
                name_lower = member.name.lower()
                if any(
                    skip in name_lower
                    for skip in ["xexx", "exhibit", "toc", "cover", "graphic", "img"]
                ):
                    continue

                date = extract_filing_date_from_html_path(member.name)
                if date:
                    filing_dates.append(date)

            if not filing_dates:
                return None

            # Return the most recent date
            return max(filing_dates)

    except Exception as e:
        logger.debug(f"Error inspecting tar file {tar_file.name}: {e}")
        return None


def is_tar_file_empty(tar_file: Path) -> bool:
    """
    Check if a tar file is empty (no HTML files).

    Empty tar files are artifacts from datamule's batch download process
    and should be filtered out before selection.

    Args:
        tar_file: Path to tar file

    Returns:
        True if tar file is empty (no HTML files), False otherwise
    """
    try:
        with tarfile.open(tar_file, "r") as tar:
            html_members = [m for m in tar.getmembers() if m.name.endswith((".html", ".htm"))]
            return len(html_members) == 0
    except Exception:
        # If we can't open it, consider it empty/useless
        return True


def find_tar_with_latest_10k(tar_files: list[Path]) -> Path | None:
    """
    Find which tar file contains the latest 10-K filing.

    This is the core function for the repeatable process:
    1. Filters out empty tar files (no HTML files) - these are useless artifacts
    2. Inspects each non-empty tar file to find the latest 10-K filing date
    3. Returns the tar file containing the most recent 10-K
    4. If multiple tar files have the same latest date, returns the first one

    Args:
        tar_files: List of tar file paths to check

    Returns:
        Path to tar file with latest 10-K, or None if no valid tar files found
    """
    if not tar_files:
        return None

    # Filter out empty tar files first (they're useless)
    non_empty_tars = []
    for tar_file in tar_files:
        if not is_tar_file_empty(tar_file):
            non_empty_tars.append(tar_file)
        else:
            logger.debug(f"Skipping empty tar file: {tar_file.name}")

    if not non_empty_tars:
        # All tar files are empty - return None (will trigger fallback)
        logger.warning(f"All {len(tar_files)} tar files are empty")
        return None

    # If only one non-empty tar file, return it (no need to inspect dates)
    if len(non_empty_tars) == 1:
        return non_empty_tars[0]

    # Inspect each non-empty tar file to find latest filing date
    tar_with_dates = []
    for tar_file in non_empty_tars:
        latest_date = get_latest_10k_filing_date_from_tar(tar_file)
        if latest_date:
            tar_with_dates.append((tar_file, latest_date))
        else:
            # Tar file has HTML but we couldn't extract a date
            # This is less ideal, but still better than empty tar files
            # We'll include it but it will be sorted last
            logger.debug(f"Could not extract date from {tar_file.name}, will use as fallback")
            tar_with_dates.append((tar_file, datetime(1900, 1, 1)))

    if not tar_with_dates:
        # All tar files have HTML but no extractable dates - return first non-empty one
        logger.warning(
            f"Could not extract dates from any tar file, using first non-empty: {non_empty_tars[0].name}"
        )
        return non_empty_tars[0]

    # Sort by date (most recent first), then return the first one
    tar_with_dates.sort(key=lambda x: x[1], reverse=True)
    selected_tar = tar_with_dates[0][0]
    selected_date = tar_with_dates[0][1]

    if selected_date.year > 1900:
        logger.debug(
            f"Selected tar file: {selected_tar.name} (latest 10-K: {selected_date.strftime('%Y-%m-%d')})"
        )
    else:
        logger.debug(
            f"Selected tar file: {selected_tar.name} (date extraction failed, using as fallback)"
        )

    return selected_tar
