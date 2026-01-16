"""
Extract full text from 10-K HTML files for GraphRAG.

This module extracts the full text content from 10-K HTML files,
not just the parsed sections (Item 1, Item 1A) that are stored in Company nodes.
"""

import logging
from pathlib import Path

from bs4 import BeautifulSoup

from public_company_graph.utils.security import validate_path_within_base

logger = logging.getLogger(__name__)


def extract_full_text_from_html(file_path: Path, base_dir: Path | None = None) -> str | None:
    """
    Extract full text content from a 10-K HTML file.

    Removes HTML tags and extracts all text content, suitable for chunking.

    Args:
        file_path: Path to 10-K HTML file
        base_dir: Optional base directory to validate path is within (prevents path traversal)

    Returns:
        Full text content, or None if extraction fails
    """
    # Validate path to prevent path traversal attacks
    if base_dir:
        if not validate_path_within_base(file_path, base_dir, logger):
            return None

    try:
        with open(file_path, encoding="utf-8", errors="ignore") as f:
            content = f.read()

        # Parse HTML (suppress XML warnings)
        import warnings

        from bs4 import XMLParsedAsHTMLWarning

        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)
            try:
                soup = BeautifulSoup(content, "lxml")
            except Exception:
                soup = BeautifulSoup(content, "html.parser")

        # Remove script and style elements
        for script in soup(["script", "style"]):
            script.decompose()

        # Get text
        text = soup.get_text(separator="\n", strip=True)

        # Clean up excessive whitespace
        lines = [line.strip() for line in text.split("\n") if line.strip()]
        text = "\n".join(lines)

        if len(text) < 100:  # Too short, probably failed
            return None

        return text

    except Exception as e:
        logger.debug(f"Error extracting text from {file_path}: {e}")
        return None


def extract_full_text_with_datamule(
    file_path: Path, cik: str | None = None, base_dir: Path | None = None
) -> str | None:
    """
    Extract full text from 10-K HTML file.

    Currently uses HTML extraction only (fast, reliable).
    Datamule can be added later if needed for better structured extraction.

    Args:
        file_path: Path to 10-K HTML file
        cik: Company CIK (unused for now, kept for future datamule support)
        base_dir: Optional base directory to validate path is within (prevents path traversal)

    Returns:
        Full text content, or None if extraction fails
    """
    # Use HTML extraction (fast, reliable, always works)
    # Note: datamule support can be added if needed for enhanced parsing
    return extract_full_text_from_html(file_path, base_dir=base_dir)


def find_10k_file_for_company(cik: str, filings_dir: Path) -> Path | None:
    """
    Find the 10-K HTML file for a company CIK.

    Args:
        cik: Company CIK
        filings_dir: Base directory containing 10-K files (data/10k_filings)

    Returns:
        Path to HTML file, or None if not found
    """
    company_dir = filings_dir / cik
    if not company_dir.exists():
        return None

    # Look for HTML files (typically named 10k_{year}.html)
    html_files = list(company_dir.glob("*.html"))
    if html_files:
        # Return most recent if multiple
        return max(html_files, key=lambda p: p.stat().st_mtime)

    return None
