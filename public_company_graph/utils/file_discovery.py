"""
File discovery utilities for public_company_graph.

Provides functions to find and filter files in directories.
"""

import logging
from pathlib import Path

logger = logging.getLogger(__name__)


def find_10k_files(
    filings_dir: Path,
    limit: int | None = None,
    extensions: list[str] | None = None,
) -> list[Path]:
    """
    Find all 10-K HTML/XML files in the filings directory.

    Args:
        filings_dir: Base directory containing 10-K files
        limit: Optional limit on number of files to return
        extensions: Optional list of extensions to search (default: ['.html', '.xml'])

    Returns:
        List of file paths
    """
    if extensions is None:
        extensions = [".html", ".xml"]

    if not filings_dir.exists():
        logger.warning(f"⚠ No 10-K filings directory found: {filings_dir}")
        return []

    files: list[Path] = []
    for ext in extensions:
        files.extend(filings_dir.glob(f"**/*{ext}"))

    if limit:
        files = files[:limit]

    return sorted(files)
