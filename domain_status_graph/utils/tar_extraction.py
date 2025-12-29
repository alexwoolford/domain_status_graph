"""
Utilities for extracting files from tar archives with security validation.

This module provides secure tar extraction functions that prevent Tar Slip attacks
by validating all extracted paths before extraction.
"""

import logging
import shutil

# Python 3.14 requires filter parameter for tar.extract()
# Use 'data' filter for security (default in Python 3.14+)
# This prevents extracting files outside the target directory
# Note: We already have manual Tar Slip protection, but using 'data' filter
# provides an additional layer of security and eliminates deprecation warnings
import sys
import tarfile
from pathlib import Path

# Import function to extract actual filing dates from tar contents
from domain_status_graph.utils.tar_selection import get_latest_10k_filing_date_from_tar

# Check if filter parameter is available (Python 3.12+)
if sys.version_info >= (3, 12):
    TAR_FILTER = "data"  # Safe filter that blocks path traversal
else:
    TAR_FILTER = None  # Not available in older Python versions

logger = logging.getLogger(__name__)


def get_filing_date_from_tar_name(tar_file: Path) -> tuple[int, int]:
    """
    Extract filing year and filing number from tar filename for sorting.

    Supports two naming formats:
    1. SEC direct: {CIK}{YY}{filing_number}.tar
       Example: 000109087224000049.tar = CIK 0001090872, year 24 (2024), filing 000049
    2. Datamule API: batch_XXX_YYY.tar (use modification time for sorting)

    Args:
        tar_file: Path to tar file

    Returns:
        Tuple of (year, filing_number) for sorting (higher = more recent)
    """
    name = tar_file.stem  # Without .tar extension

    # Handle datamule-sgml batch naming (batch_000_001.tar)
    if name.startswith("batch_"):
        # Use modification time for batch files (newer = more recent)
        mtime = tar_file.stat().st_mtime
        # Use a high year number so batch files sort after CIK files
        return (99, int(mtime))  # Use mtime as secondary sort

    # Handle SEC direct naming: {CIK}{YY}{filing_number}.tar
    if len(name) >= 12:
        try:
            year_str = name[10:12]  # Last 2 digits of year (24 = 2024)
            filing_str = name[12:]  # Filing number
            year = int(year_str)
            filing_num = int(filing_str)
            return (year, filing_num)
        except (ValueError, IndexError):
            pass

    # Fallback: use modification time as proxy
    return (0, int(tar_file.stat().st_mtime))


def extract_from_tar(
    tar_file: Path, company_dir: Path, ticker: str, cik_padded: str
) -> tuple[bool, Path | None, str | None]:
    """
    Extract the main 10-K HTML file from a tar archive with Tar Slip protection.

    This function implements security measures to prevent directory traversal attacks:
    - Validates all member paths before extraction
    - Ensures extracted files stay within the extract directory
    - Uses safe filename extraction (no path components)
    - Validates target paths before copying

    Args:
        tar_file: Path to the tar file
        company_dir: Directory to extract HTML file to
        ticker: Stock ticker (for logging)
        cik_padded: CIK (for logging)

    Returns:
        Tuple of (success, file_path, error_message)
        - success: True if extraction succeeded
        - file_path: Path to extracted HTML file (if successful)
        - error_message: Error message (if failed)
    """
    # Extract tar file to a temporary directory
    # Note: Deprecation warnings from tarfile are suppressed at module level
    extract_dir = company_dir / "extracted"
    extract_dir.mkdir(parents=True, exist_ok=True)

    main_10k = None
    try:
        # Open tar file
        # Note: filter parameter not available in Python 3.13
        # Deprecation warnings are suppressed at module level
        tar = tarfile.open(tar_file, "r")

        try:
            # Get all HTML/HTM files from the tar
            html_members = [m for m in tar.getmembers() if m.name.endswith((".htm", ".html"))]

            if not html_members:
                raise ValueError("No HTML files found in tar archive")

            # Find the main 10-K document (not exhibits)
            # SEC naming: a-{date}.htm = main document, a-{date}xexx{number}.htm = exhibits
            html_members.sort(key=lambda m: m.size, reverse=True)  # Largest first

            for member in html_members:
                name_lower = member.name.lower()
                # Skip exhibits (contain "xexx"), tables of contents, graphics, etc.
                if any(
                    skip in name_lower
                    for skip in ["xexx", "exhibit", "toc", "cover", "graphic", "img"]
                ):
                    continue
                # Main 10-K files typically match pattern: a-{date}.htm
                if member.name.endswith((".htm", ".html")):
                    # Prevent Tar Slip: Validate that extracted path stays within extract_dir
                    # Resolve paths to prevent directory traversal attacks
                    member_path = Path(member.name)
                    # Remove any leading slashes or parent directory references
                    safe_name = member_path.name  # Get just the filename, no path
                    if ".." in safe_name or safe_name.startswith("/"):
                        logger.warning(f"  ⚠️  Skipping suspicious tar member: {member.name}")
                        continue

                    # Extract to a safe path within extract_dir
                    safe_extract_path = extract_dir / safe_name
                    # Double-check: ensure resolved path is within extract_dir
                    try:
                        safe_extract_path.resolve().relative_to(extract_dir.resolve())
                    except ValueError:
                        logger.warning(f"  ⚠️  Path traversal attempt detected: {member.name}")
                        continue

                    # Extract using the safe path - modify member name to use safe_name
                    # This prevents tar.extract from using the original (potentially unsafe) path
                    original_name = member.name
                    member.name = safe_name  # Override with safe filename
                    try:
                        # Use 'data' filter for Python 3.12+ compatibility and security
                        if TAR_FILTER:
                            tar.extract(member, extract_dir, filter=TAR_FILTER)
                        else:
                            tar.extract(member, extract_dir)
                    finally:
                        member.name = original_name  # Restore original name

                    # Verify the extracted file is where we expect
                    extracted_file = extract_dir / safe_name
                    if extracted_file.exists():
                        main_10k = extracted_file
                        break

            # If no specific match, extract the largest HTML file
            if not main_10k and html_members:
                member = html_members[0]
                # Apply same Tar Slip protection
                member_path = Path(member.name)
                safe_name = member_path.name
                if ".." not in safe_name and not safe_name.startswith("/"):
                    safe_extract_path = extract_dir / safe_name
                    try:
                        safe_extract_path.resolve().relative_to(extract_dir.resolve())
                        # Modify member name to use safe_name before extraction
                        original_name = member.name
                        member.name = safe_name
                        try:
                            # Use 'data' filter for Python 3.12+ compatibility and security
                            if TAR_FILTER:
                                tar.extract(member, extract_dir, filter=TAR_FILTER)
                            else:
                                tar.extract(member, extract_dir)
                        finally:
                            member.name = original_name
                        extracted_file = extract_dir / safe_name
                        if extracted_file.exists():
                            main_10k = extracted_file
                    except ValueError:
                        logger.warning(f"  ⚠️  Path traversal attempt detected: {member.name}")
        finally:
            tar.close()

        if main_10k and main_10k.exists():
            # Copy to our company directory with a cleaner filename
            # Extract year from actual filing date in tar file (most accurate)
            year = None
            filing_date = get_latest_10k_filing_date_from_tar(tar_file)

            if filing_date:
                # Use the actual filing date from the HTML file path
                year = str(filing_date.year)
                logger.debug(
                    f"  Extracted year {year} from filing date: {filing_date.strftime('%Y-%m-%d')}"
                )
            else:
                # Fallback: Try to extract year from tar filename
                tar_name = tar_file.stem  # Without .tar extension
                if len(tar_name) >= 12 and not tar_name.startswith("batch_"):
                    # Format: 000109087224000049 -> year is positions 10-12
                    try:
                        year_str = tar_name[10:12]  # Last 2 digits of year (24 = 2024)
                        year = f"20{year_str}"  # Convert to full year
                        logger.debug(f"  Extracted year {year} from tar filename")
                    except Exception:
                        pass

            if year:
                target_file = company_dir / f"10k_{year}.html"
            else:
                # Last resort: Use original filename, but sanitize to prevent path traversal
                safe_filename = Path(main_10k.name).name  # Get just filename, no path
                target_file = company_dir / safe_filename
                logger.warning(
                    f"  ⚠️  Could not extract year from tar file {tar_file.name}, using original filename"
                )

            # Validate target_file is within company_dir (prevent path traversal)
            try:
                target_file.resolve().relative_to(company_dir.resolve())
            except ValueError:
                # Path traversal attempt - use safe fallback
                target_file = company_dir / f"10k_{year or 'unknown'}.html"

            shutil.copy2(main_10k, target_file)
            logger.debug(f"  ✓ {ticker}: Extracted {main_10k.name} -> {target_file.name}")

            # Clean up extracted directory (keep tar file for reference)
            shutil.rmtree(extract_dir, ignore_errors=True)

            return True, target_file, None
        else:
            raise ValueError("Failed to extract main 10-K HTML file")

    except Exception as e:
        # Clean up on error
        shutil.rmtree(extract_dir, ignore_errors=True)
        error_msg = f"Failed to extract tar file: {str(e)}"
        return False, None, error_msg
