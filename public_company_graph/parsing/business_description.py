"""
Business description extraction from 10-K filings.

This module provides functions to extract Item 1: Business descriptions from 10-K HTML files
using multiple extraction strategies with configurable fallbacks.

Extraction Strategy Priority (highest to lowest):
1. Datamule library (highest quality when available)
2. TOC anchor-based navigation (follows href links to section IDs)
3. Direct ID pattern matching (elements with id containing "item1" + "business")
4. Text node search (finds "Item 1...Business" text, skips TOC tables)
5. Raw regex extraction (extracts text between section markers)

Patterns Handled:
- Split tags: "Item 1." and "Business" in separate HTML elements
- BUSINESS header: Section header without "Item 1" prefix
- TOC-first: Item 1 appears in TOC table, actual content is later
- Anchor-based: TOC links point to section IDs with content
- Standard format: "Item 1. Business" followed by content until "Item 1A"
"""

import logging
import re
from pathlib import Path
from typing import Any

from bs4 import BeautifulSoup

from public_company_graph.config import get_data_dir
from public_company_graph.utils.datamule import suppress_datamule_output
from public_company_graph.utils.security import validate_path_within_base

logger = logging.getLogger(__name__)


# Import shared text extraction utility
from public_company_graph.parsing.text_extraction import extract_between_anchors

# Minimum content length to consider extraction successful
MIN_BUSINESS_DESCRIPTION_LENGTH = 500

# Stop patterns that indicate end of Item 1 section
STOP_PATTERNS = [
    r"Item\s*1A[\.:\s]",  # Item 1A with various separators
    r"ITEM\s*1A[\.:\s]",
    r"Item\s*1B[\.:\s]",  # Item 1B (Unresolved Staff Comments)
    r"ITEM\s*1B[\.:\s]",
    r"Item\s*1C[\.:\s]",  # Item 1C (Cybersecurity)
    r"ITEM\s*1C[\.:\s]",
    r"Item\s*2[\.:\s]",  # Item 2 (Properties)
    r"ITEM\s*2[\.:\s]",
    r"Item\s*10[\.:\s]",  # Item 10 (some smaller filers skip to Part III)
    r"ITEM\s*10[\.:\s]",
    r"Risk\s*Factors",  # Risk Factors header
    r"RISK\s*FACTORS",
    r"PART\s*II",  # Part II header
    r"Part\s*II",
]


def _is_stop_pattern(text: str) -> bool:
    """Check if text matches any stop pattern indicating end of Item 1 section."""
    return any(re.search(pattern, text, re.IGNORECASE) for pattern in STOP_PATTERNS)


def _clean_extracted_text(text: str) -> str:
    """Clean and normalize extracted text."""
    if not text:
        return ""
    # Normalize whitespace
    text = re.sub(r"\s+", " ", text)
    # Remove excessive punctuation
    text = re.sub(r"\.{3,}", "...", text)
    return text.strip()


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

    while current:
        # Get text content from current element (including nested elements)
        if hasattr(current, "get_text"):
            text = current.get_text(strip=True)

            # Check for stop patterns in ANY element's text
            if text and _is_stop_pattern(text):
                break

            # Collect text from content elements
            if current.name in ["p", "div", "span", "td", "li"]:
                if text:
                    text_parts.append(text)

        # Move to next sibling
        current = current.next_sibling
        if not current:
            break

    return " ".join(text_parts)


def _extract_via_raw_regex(content: str, file_path: Path) -> str | None:
    """
    Extract business description using raw regex patterns on HTML content.

    This is a fallback strategy that works when structural parsing fails.
    It finds text between section markers (BUSINESS...ITEM 1A/RISK FACTORS).

    Args:
        content: Raw HTML content
        file_path: Path for logging purposes

    Returns:
        Extracted text or None
    """
    # Strategy 1: Find BUSINESS section until next major section
    # This handles files where "Item 1" and "Business" may be split or absent
    patterns = [
        # Pattern A: Item 1...Business...content...Item 1A (most specific)
        (
            r"(?:Item|ITEM)\s*1\.?\s*(?:<[^>]*>)*\s*(?:<[^>]*>)*\s*"
            r"(?:Business|BUSINESS)(.*?)(?:Item\s*1A|ITEM\s*1A|Risk\s*Factors|RISK\s*FACTORS)",
            "item1_business",
        ),
        # Pattern B: Combined Items 1 and 2 (Business and Properties)
        (
            r"(?:Items?\s*1\.?\s*(?:and|&)\s*2\.?\s*)"
            r"(?:<[^>]*>)*\s*(?:Business\s*and\s*Properties|BUSINESS\s*AND\s*PROPERTIES)"
            r"(.*?)(?:Item\s*1A|ITEM\s*1A|Item\s*3|ITEM\s*3|Risk\s*Factors)",
            "combined_items_1_2",
        ),
        # Pattern C: Just BUSINESS header until stop pattern (less specific)
        (
            r"(?:>|\s)BUSINESS(?:</[^>]+>)?\s*(.*?)(?:ITEM\s*1A|Item\s*1A|RISK\s*FACTORS|Risk\s*Factors)",
            "business_header",
        ),
        # Pattern D: BUSINESS with bold tags
        (
            r"<b[^>]*>BUSINESS</b>(.*?)(?:<b[^>]*>(?:ITEM\s*1A|RISK\s*FACTORS)</b>)",
            "business_bold",
        ),
    ]

    for pattern, pattern_name in patterns:
        # Find ALL matches and take the longest one that meets minimum length
        # This handles cases where TOC entries match before actual content
        matches = list(re.finditer(pattern, content, re.I | re.DOTALL))

        best_text = None
        best_length = 0

        for match in matches:
            raw_html = match.group(1)
            # Parse HTML to extract clean text
            section_soup = BeautifulSoup(raw_html, "html.parser")
            text = section_soup.get_text(separator=" ", strip=True)
            text = _clean_extracted_text(text)

            # Keep the longest extraction that meets minimum length
            if len(text) >= MIN_BUSINESS_DESCRIPTION_LENGTH and len(text) > best_length:
                best_text = text
                best_length = len(text)

        if best_text:
            logger.debug(
                f"Raw regex extraction succeeded ({pattern_name}): "
                f"{len(best_text):,} chars from {file_path.name}"
            )
            return best_text

    return None


def _extract_via_text_node_search(soup: BeautifulSoup, file_path: Path) -> str | None:
    """
    Extract business description by finding Item 1 Business text nodes.

    Skips TOC matches (text inside tables) and finds actual content sections.

    Args:
        soup: Parsed BeautifulSoup object
        file_path: Path for logging purposes

    Returns:
        Extracted text or None
    """
    # Find all "Item 1" text mentions with various formats
    search_patterns = [
        r"Item\s*1[\.:]?\s*Business",
        r"ITEM\s*1[\.:]?\s*BUSINESS",
        r"Item\s+1[\.:]?\s*$",  # Just "Item 1." (Business may be in sibling)
    ]

    for pattern in search_patterns:
        text_nodes = soup.find_all(string=re.compile(pattern, re.I))

        for text_node in text_nodes:
            parent = text_node.parent
            if parent is None:
                continue

            # Skip if in table (likely TOC)
            table = parent.find_parent("table")
            if table:
                continue

            # Found non-TOC Item 1 - extract content from here
            start_el = parent

            # Walk up to find meaningful container
            for _ in range(5):
                if start_el and start_el.name in ["div", "p", "section", "body"]:
                    break
                start_el = start_el.parent if start_el else None

            if start_el:
                # Extract text using stop pattern detection
                text = extract_section_text(start_el, soup)
                text = _clean_extracted_text(text)

                if len(text) >= MIN_BUSINESS_DESCRIPTION_LENGTH:
                    logger.debug(
                        f"Text node search succeeded: {len(text):,} chars from {file_path.name}"
                    )
                    return text

    return None


def _extract_via_anchor_navigation(
    soup: BeautifulSoup, content: str, file_path: Path
) -> str | None:
    """
    Extract business description by following TOC anchor links to section IDs.

    Args:
        soup: Parsed BeautifulSoup object
        content: Raw HTML content (for fallback)
        file_path: Path for logging purposes

    Returns:
        Extracted text or None
    """

    # Strategy 1: Find TOC hrefs containing item1 and business
    def href_matcher(x: Any) -> bool:
        return bool(x and re.search(r"#.*item.*1.*business", str(x), re.I))

    toc_link = soup.find("a", href=href_matcher)

    # Strategy 2: Find combined "Items 1. and 2. Business and Properties" links
    if not toc_link:

        def combined_href_matcher(x: Any) -> bool:
            return bool(x and re.search(r"#.*item.*1.*2.*business", str(x), re.I))

        toc_link = soup.find("a", href=combined_href_matcher)

    # Strategy 3: Find any TOC link to item1 section
    if not toc_link:

        def item1_href_matcher(x: Any) -> bool:
            return bool(x and re.search(r"#.*item.*1\b", str(x), re.I))

        # Find links that point to item1 and contain "business" in text
        for link in soup.find_all("a", href=item1_href_matcher):
            link_text = link.get_text(strip=True).lower()
            if "business" in link_text or "item 1" in link_text:
                toc_link = link
                break

    # Strategy 4: Find simple ID-based TOC links like #I1 (common in smaller filings)
    if not toc_link:

        def simple_i1_matcher(x: Any) -> bool:
            # Match #I1, #i1, #Item1 etc (short ID patterns)
            return bool(x and re.search(r"^#I1$|^#item1$", str(x), re.I))

        toc_link = soup.find("a", href=simple_i1_matcher)

    # Strategy 5: Find TOC links by link TEXT containing "Item 1" (for Workiva iXBRL)
    # These filings use GUID-based hrefs like #i1cb1ba018cb1455aa66bd3f9ab0c5b1a_1499
    if not toc_link:
        for link in soup.find_all("a", href=True):
            link_text = link.get_text(strip=True)
            # Match "Item 1." or "Item 1 " but NOT "Item 1A" or "Item 10"
            if re.match(r"^Item\s*1\.?\s*$", link_text, re.I):
                toc_link = link
                logger.debug(f"Found Item 1 link by text: href={link.get('href')}")
                break

    if not toc_link or not toc_link.has_attr("href"):
        return None

    start_id = str(toc_link["href"]).lstrip("#")
    start_el = soup.find(id=start_id) if start_id else None

    if not start_el:
        # Try finding element with ID containing the anchor reference
        def id_contains_matcher(x: Any) -> bool:
            return bool(x and start_id and start_id in str(x))

        start_el = soup.find(id=id_contains_matcher)

    if not start_el:
        return None

    # Find end element (Item 1A, Item 1B, Item 1C, Item 2, or Item 10)
    def end_section_matcher(x: Any) -> bool:
        if not x:
            return False
        x_str = str(x)
        # Match patterns like: item1a, item_1a, I1A, item2, I2, item10, I10
        return bool(
            re.search(r"item.*1[abc]", x_str, re.I)
            or re.search(r"^I1[ABC]$", x_str, re.I)
            or re.search(r"item.*2\b", x_str, re.I)
            or re.search(r"^I2$", x_str, re.I)
            or re.search(r"item.*10\b", x_str, re.I)
            or re.search(r"^I10$", x_str, re.I)
        )

    end_el = start_el.find_next(id=end_section_matcher)

    # For Workiva iXBRL files with GUID-based IDs, find end by TOC link text
    if not end_el:
        # Look for TOC link to Item 1A (or Item 2 if no Item 1A)
        for end_link in soup.find_all("a", href=True):
            link_text = end_link.get_text(strip=True)
            if re.match(r"^Item\s*1A\.?\s*$", link_text, re.I):
                end_href = end_link.get("href", "").lstrip("#")
                if end_href:
                    end_el = soup.find(id=end_href)
                    if end_el:
                        logger.debug(f"Found Item 1A end by text: id={end_href}")
                        break

    if end_el:
        # Use anchor-based extraction
        text = extract_between_anchors(start_el, end_el)
    else:
        # No end anchor found - use stop pattern approach
        text = extract_section_text(start_el, soup)

    text = _clean_extracted_text(text)

    if len(text) >= MIN_BUSINESS_DESCRIPTION_LENGTH:
        logger.debug(f"Anchor navigation succeeded: {len(text):,} chars from {file_path.name}")
        return text

    return None


def _extract_via_direct_id(soup: BeautifulSoup, file_path: Path) -> str | None:
    """
    Extract business description from elements with ID containing item1 + business.

    Args:
        soup: Parsed BeautifulSoup object
        file_path: Path for logging purposes

    Returns:
        Extracted text or None
    """

    # Find element with ID containing item1 and business
    def id_matcher(x: Any) -> bool:
        return bool(x and re.search(r"item.*1.*business", str(x), re.I))

    start_el = soup.find(id=id_matcher)

    if not start_el:
        # Try broader item1 ID pattern
        def item1_id_matcher(x: Any) -> bool:
            return bool(x and re.search(r"item.*1\b", str(x), re.I))

        start_el = soup.find(id=item1_id_matcher)

    if not start_el:
        return None

    # Find end element
    def item1a_matcher(x: Any) -> bool:
        return bool(x and re.search(r"item.*1a", str(x), re.I))

    end_el = start_el.find_next(id=item1a_matcher)

    if end_el:
        text = extract_between_anchors(start_el, end_el)
    else:
        text = extract_section_text(start_el, soup)

    text = _clean_extracted_text(text)

    if len(text) >= MIN_BUSINESS_DESCRIPTION_LENGTH:
        logger.debug(f"Direct ID extraction succeeded: {len(text):,} chars from {file_path.name}")
        return text

    return None


def extract_business_description(
    file_path: Path,
    file_content: str | None = None,
    filings_dir: Path | None = None,
    soup: BeautifulSoup | None = None,
) -> str | None:
    """
    Extract business description from 10-K Item 1 using multiple strategies.

    Tries extraction strategies in order of specificity:
    1. TOC anchor-based navigation (follows href links to section IDs)
    2. Direct ID pattern matching (elements with id containing "item1" + "business")
    3. Text node search (finds "Item 1...Business" text, skips TOC tables)
    4. Raw regex extraction (extracts text between section markers)

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
        if not validate_path_within_base(file_path, filings_dir, logger):
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

        # Try extraction strategies in order of reliability
        result = None

        # Strategy 1: TOC anchor navigation (most reliable for structured filings)
        result = _extract_via_anchor_navigation(soup, content, file_path)
        if result:
            return result

        # Strategy 2: Direct ID pattern matching
        result = _extract_via_direct_id(soup, file_path)
        if result:
            return result

        # Strategy 3: Text node search (skip TOC, find actual section)
        result = _extract_via_text_node_search(soup, file_path)
        if result:
            return result

        # Strategy 4: Raw regex extraction (fallback for unusual structures)
        result = _extract_via_raw_regex(content, file_path)
        if result:
            return result

        logger.debug(f"All extraction strategies failed for {file_path.name}")
        return None

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
    Extract Item 1 Business description using datamule with custom parser fallback.

    Strategy:
    1. Try datamule's get_section() which works for ~94% of filings
    2. Fall back to custom multi-strategy parser for the ~6% where datamule fails

    This provides high-quality extraction from datamule while ensuring we capture
    descriptions from filings with unusual HTML structures.

    Args:
        file_path: Path to 10-K HTML file
        cik: Company CIK (required for datamule)
        file_content: Optional pre-read file content (passed to custom parser)
        skip_datamule: If True, skip extraction entirely
        filings_dir: Optional base directory for custom parser path validation
        soup: Optional BeautifulSoup object (passed to custom parser for performance)

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

    datamule_failed = False
    datamule_error_msg = ""

    # Strategy 1: Use datamule for extraction (best quality, ~94% success rate)
    try:
        from public_company_graph.utils.datamule import get_cached_parsed_doc

        # Use portfolio directory (contains tar files from datamule download)
        portfolios_dir = get_data_dir() / "10k_portfolios"
        portfolio_path = portfolios_dir / f"10k_{cik}"

        # Get cached parsed document (or create, parse, and cache it)
        doc = get_cached_parsed_doc(cik, portfolio_path)

        if doc is not None:
            # Extract Item 1 section using datamule
            with suppress_datamule_output():
                if hasattr(doc, "get_section"):
                    item1 = doc.get_section(title="item1", format="text")
                    if item1:
                        item1_text = item1[0] if isinstance(item1, list) else str(item1)
                        if len(item1_text) >= MIN_BUSINESS_DESCRIPTION_LENGTH:
                            # Datamule succeeded - return the result
                            return item1_text

            # Datamule couldn't extract Item 1 section
            accession = getattr(doc, "accession", "unknown")
            filing_date = getattr(doc, "filing_date", "unknown")
            datamule_error_msg = f"accession={accession}, date={filing_date}"
            datamule_failed = True
        else:
            datamule_error_msg = "no datamule document available"
            datamule_failed = True

    except ImportError:
        datamule_error_msg = "datamule library not available"
        datamule_failed = True
    except Exception as e:
        datamule_error_msg = str(e)
        datamule_failed = True

    # Strategy 2: Fall back to custom multi-strategy parser
    if datamule_failed:
        logger.debug(
            f"Datamule extraction failed for CIK {cik} ({datamule_error_msg}), "
            f"trying custom parser..."
        )

        # Use custom parser as fallback
        result = extract_business_description(
            file_path=file_path,
            file_content=file_content,
            filings_dir=filings_dir,
            soup=soup,
        )

        if result:
            logger.info(
                f"✅ Custom parser succeeded for CIK {cik} where datamule failed: "
                f"{len(result):,} chars extracted"
            )
            return result

        # Both datamule and custom parser failed
        logger.warning(
            f"⚠️  All extraction methods failed for CIK {cik} (datamule: {datamule_error_msg})"
        )

    return None
