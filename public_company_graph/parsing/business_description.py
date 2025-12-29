"""
Business description extraction from 10-K filings.

This module provides functions to extract Item 1: Business descriptions from 10-K HTML files
using multiple strategies with datamule fallback.
"""

import logging
import re
from pathlib import Path
from typing import Any

from bs4 import BeautifulSoup

from public_company_graph.config import get_data_dir
from public_company_graph.utils.datamule import suppress_datamule_output

logger = logging.getLogger(__name__)


# Import shared text extraction utility
from public_company_graph.parsing.text_extraction import extract_between_anchors


def extract_section_text(start_element, soup: BeautifulSoup) -> str:
    """
    Extract text from a section starting at a heading until the next major section.

    Args:
        start_element: BeautifulSoup element where section starts
        soup: BeautifulSoup object

    Returns:
        Extracted text
    """
    text_parts = []
    current = start_element

    # Look for next major section (Item 1A, Item 2, etc.)
    stop_patterns = [
        r"Item\s+1A",
        r"Item\s+2",
        r"ITEM\s+1A",
        r"ITEM\s+2",
    ]

    while current:
        # Check if we hit a stop pattern
        if current.name in ["h1", "h2", "h3"]:
            text = current.get_text()
            if any(re.search(pattern, text, re.IGNORECASE) for pattern in stop_patterns):
                break

        # Collect text
        if current.name in ["p", "div", "span"]:
            text = current.get_text(strip=True)
            if text:
                text_parts.append(text)

        # Move to next sibling
        current = current.next_sibling
        if not current:
            break

    return " ".join(text_parts)


def extract_business_description(
    file_path: Path,
    file_content: str | None = None,
    filings_dir: Path | None = None,
    soup: BeautifulSoup | None = None,
) -> str | None:
    """
    Extract business description from 10-K Item 1 using TOC anchor-based approach.

    This method:
    1. Finds TOC link with href="#...item1...business..."
    2. Jumps to element with that id
    3. Extracts text until Item 1A or Item 2 anchor

    Args:
        file_path: Path to 10-K HTML file (must be within filings_dir if provided)
        file_content: Optional pre-read file content (avoids re-reading file)
        filings_dir: Optional base directory for path validation (if None, skips validation)
        soup: Optional pre-parsed BeautifulSoup object (for performance)

    Returns:
        Business description text or None if not found
    """
    # Validate file_path is within expected directory (prevent path traversal)
    if filings_dir is not None:
        try:
            file_path.resolve().relative_to(filings_dir.resolve())
        except ValueError:
            logger.warning(f"⚠️  Path traversal attempt detected: {file_path}")
            return None

    try:
        # Use provided content if available, otherwise read file
        if file_content is not None:
            content = file_content
        else:
            with open(file_path, encoding="utf-8", errors="ignore") as f:
                content = f.read()

        # PERFORMANCE: Reuse soup if provided, otherwise parse
        # Use lxml parser which is ~25% faster than html.parser
        if soup is None:
            try:
                soup = BeautifulSoup(content, "lxml")
            except Exception:
                soup = BeautifulSoup(content, "html.parser")

        # 1) Prefer TOC hrefs that contain both item1 and business
        def href_matcher(x: Any) -> bool:
            return bool(x and re.search(r"#.*item1.*business", str(x), re.I))

        toc_link = soup.find("a", href=href_matcher)
        start_id = (
            str(toc_link["href"]).lstrip("#") if toc_link and toc_link.has_attr("href") else None
        )

        start_el = soup.find(id=start_id) if start_id else None
        if not start_el:
            # 2) Fallback: direct id pattern (use callable for regex matching)
            def id_matcher(x: Any) -> bool:
                return bool(x and re.search(r"item1.*business", str(x), re.I))

            start_el = soup.find(id=id_matcher)
        if not start_el:
            # 3) Fallback: Find text node with "ITEM 1 BUSINESS" and walk from there
            # IMPORTANT: Skip TOC matches (they're in tables)
            item1_text_nodes = soup.find_all(string=re.compile(r"ITEM\s+1[\.:]?\s*BUSINESS", re.I))
            if item1_text_nodes:
                # Filter out TOC matches (in tables)
                for text_node in item1_text_nodes:
                    parent = text_node.parent
                    if parent is None:
                        continue
                    # Check if it's in a table (likely TOC)
                    table = parent.find_parent("table")
                    if table:
                        continue  # Skip TOC matches

                    # This is likely the actual section
                    start_el = parent
                    # Walk up to find a meaningful container (div, p, td, body)
                    for _ in range(5):
                        if start_el and start_el.name in ["div", "p", "td", "body"]:
                            break
                        start_el = start_el.parent if start_el else None

                    if start_el:
                        break  # Found a good candidate
        if not start_el:
            # 4) Last resort: raw text search (for files with unusual structure)
            item1_match = re.search(r"ITEM\s+1[\.:]?\s*BUSINESS", content, re.I)
            if item1_match:
                # Extract from raw text position
                # Skip past the heading itself to get actual content
                start_pos = item1_match.end()
                remaining = content[start_pos:]

                # Look for Item 1A, but also check for Item 2 if Item 1A is too close
                item1a_match = re.search(r"ITEM\s+1A", remaining, re.I)
                item2_match = re.search(r"ITEM\s+2[\.:]?", remaining, re.I)

                # Choose the closer stop point, but ensure we have enough content
                stop_pos = len(remaining)
                if item1a_match and item2_match:
                    # Use the one that gives us more content (further away)
                    stop_pos = max(item1a_match.start(), item2_match.start())
                elif item1a_match:
                    # If Item 1A is very close (< 1000 chars), continue to Item 2
                    if item1a_match.start() < 1000:
                        if item2_match:
                            stop_pos = item2_match.start()
                        else:
                            # No Item 2, use Item 1A anyway
                            stop_pos = item1a_match.start()
                    else:
                        stop_pos = item1a_match.start()
                elif item2_match:
                    stop_pos = item2_match.start()

                if stop_pos < len(remaining):
                    item1_section = remaining[:stop_pos]
                    # Parse the section to get clean text
                    section_soup = BeautifulSoup(item1_section, "html.parser")
                    item1_text = section_soup.get_text(separator=" ", strip=True)
                    if item1_text and len(item1_text) > 500:
                        if len(item1_text) > 50000:
                            logger.warning(
                                f"Business description truncated from {len(item1_text):,} to 50,000 chars "
                                f"in {file_path.name}"
                            )
                        return item1_text[:50000]
            return None

        # End at Item 1A (preferred), else Item 2
        # Use callable for regex matching with find_next
        def item1a_matcher(x: Any) -> bool:
            return bool(x and re.search(r"item1a", str(x), re.I))

        end_el = start_el.find_next(id=item1a_matcher)
        if not end_el:

            def item2_matcher(x: Any) -> bool:
                return bool(x and re.search(r"item2", str(x), re.I))

            end_el = start_el.find_next(id=item2_matcher)
        if not end_el:
            # Fallback: use old stop pattern approach
            fallback_text = extract_section_text(start_el, soup)
            if len(fallback_text) > 50000:
                logger.warning(
                    f"Business description truncated from {len(fallback_text):,} to 50,000 chars "
                    f"in {file_path.name}"
                )
            return fallback_text[:50000]

        item1_text = extract_between_anchors(start_el, end_el)
        if item1_text:
            # Clean up text
            item1_text = re.sub(r"\s+", " ", item1_text)  # Normalize whitespace
            item1_text = item1_text.strip()
            # Require minimum length to filter out noise/empty sections
            # Lower threshold (20 chars) allows meaningful test cases while filtering empty results
            if len(item1_text) > 20:
                if len(item1_text) > 50000:
                    logger.warning(
                        f"Business description truncated from {len(item1_text):,} to 50,000 chars "
                        f"in {file_path.name}"
                    )
                return item1_text[:50000]  # Limit to 50KB
            else:
                logger.debug(
                    f"Extracted text too short ({len(item1_text)} chars): {item1_text[:100]}"
                )

    except Exception as e:
        logger.debug(f"Error extracting business description from {file_path.name}: {e}")

    return None


def extract_business_description_with_datamule_fallback(
    file_path: Path,
    cik: str | None = None,
    file_content: str | None = None,
    skip_datamule: bool = False,
    filings_dir: Path | None = None,
    soup: BeautifulSoup | None = None,
) -> str | None:
    """
    Extract Item 1 Business description using datamule.

    Strategy: Use datamule's get_section() which works for ~94% of filings.
    For the ~6% where it fails, we log the CIK and return None (accept the gap).

    This keeps the code simple and relies on datamule's quality parsing rather than
    maintaining a parallel custom parser with arbitrary limits.

    Args:
        file_path: Path to 10-K HTML file
        cik: Company CIK (required for datamule)
        file_content: Optional pre-read file content (unused, kept for API compatibility)
        skip_datamule: If True, return None immediately (no extraction)
        filings_dir: Optional base directory (unused, kept for API compatibility)
        soup: Optional BeautifulSoup object (unused, kept for API compatibility)

    Returns:
        Business description text or None if extraction fails
    """
    # If skip_datamule flag is set, skip extraction entirely
    if skip_datamule:
        logger.debug(f"Skipping business description extraction for {file_path.name}")
        return None

    if not cik:
        # Try to extract CIK from file path
        cik = file_path.parent.name
        if not cik.isdigit():
            logger.warning(f"⚠️  Could not determine CIK for business description: {file_path}")
            return None

    # Use datamule for extraction (best quality, ~94% success rate)
    try:
        from public_company_graph.utils.datamule import get_cached_parsed_doc

        # Use portfolio directory (contains tar files from datamule download)
        portfolios_dir = get_data_dir() / "10k_portfolios"
        portfolio_path = portfolios_dir / f"10k_{cik}"

        # Get cached parsed document (or create, parse, and cache it)
        doc = get_cached_parsed_doc(cik, portfolio_path)

        if doc is None:
            logger.warning(
                f"⚠️  No datamule document available for CIK {cik} - skipping business description"
            )
            return None

        # Extract Item 1 section using datamule
        with suppress_datamule_output():
            if hasattr(doc, "get_section"):
                item1 = doc.get_section(title="item1", format="text")
                if item1:
                    item1_text = item1[0] if isinstance(item1, list) else str(item1)
                    if len(item1_text) > 1000:
                        # Return full description - no arbitrary limits
                        return item1_text

        # Datamule couldn't extract Item 1 section - log for investigation
        # Include accession number and filing date for reproducibility
        accession = getattr(doc, "accession", "unknown")
        filing_date = getattr(doc, "filing_date", "unknown")
        logger.warning(
            f"⚠️  Datamule could not extract Item 1 for CIK {cik} "
            f"(accession={accession}, date={filing_date})"
        )
        logger.debug(
            f"Item 1 extraction failed - CIK: {cik}, accession: {accession}, "
            f"filing_date: {filing_date}, path: {getattr(doc, 'path', 'unknown')}"
        )
        return None

    except ImportError:
        logger.error("❌ datamule library not available - cannot extract business descriptions")
        return None
    except Exception as e:
        logger.warning(f"⚠️  Datamule error for CIK {cik}: {e}")
        return None
