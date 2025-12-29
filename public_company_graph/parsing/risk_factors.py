"""
Risk factors extraction from 10-K filings.

This module provides functions to extract Item 1A: Risk Factors from 10-K HTML files.
The Risk Factors section is a standard, required section in all 10-K filings.
"""

import logging
import re
import warnings
from pathlib import Path
from typing import Any

from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning

from public_company_graph.config import get_data_dir
from public_company_graph.utils.datamule import suppress_datamule_output

logger = logging.getLogger(__name__)


def extract_risk_factors(
    file_path: Path,
    file_content: str | None = None,
    filings_dir: Path | None = None,
    soup: BeautifulSoup | None = None,
) -> str | None:
    """
    Extract Item 1A: Risk Factors section from a 10-K HTML file.

    Strategy (similar to business description extraction):
    1. Try TOC link (href="#item1a")
    2. Try direct ID (id="item1a" or similar)
    3. Try text node search (ITEM 1A or Item 1A: Risk Factors)
    4. Extract until Item 1B or Item 2

    Args:
        file_path: Path to 10-K HTML file
        file_content: Optional pre-read file content (avoids re-reading file)
        filings_dir: Optional base directory for path validation
        soup: Optional pre-parsed BeautifulSoup object (for performance)

    Returns:
        Risk factors text or None if not found
    """
    # Validate file path is within filings_dir (prevent path traversal)
    if filings_dir:
        try:
            file_path.resolve().relative_to(filings_dir.resolve())
        except ValueError:
            logger.debug(f"File path outside filings_dir: {file_path}")
            return None

    if file_path.suffix != ".html":
        return None

    try:
        # Read file if content not provided
        if file_content is None:
            with open(file_path, encoding="utf-8", errors="ignore") as f:
                content = f.read()
        else:
            content = file_content

        # PERFORMANCE: Reuse soup if provided, otherwise parse
        # Use lxml parser which is ~25% faster than html.parser
        if soup is None:
            # Suppress XML warning - we intentionally use HTML parser for mixed content
            with warnings.catch_warnings():
                warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)
                try:
                    soup = BeautifulSoup(content, "lxml")
                except Exception:
                    soup = BeautifulSoup(content, "html.parser")

        # Strategy 1: Try TOC link (href="#item1a" or similar)
        toc_link = soup.find("a", href=re.compile(r"#item1a", re.I))
        if toc_link:
            # Find the target element
            href_val = toc_link.get("href", "")
            target_id = str(href_val).lstrip("#") if href_val else ""

            def id_matcher(x: Any) -> bool:
                return bool(x and re.search(r"item1a", str(x), re.I))

            start_el = soup.find(id=target_id) or soup.find(id=id_matcher)
            if start_el:
                # Find end element (Item 1B or Item 2)
                def end_matcher(x: Any) -> bool:
                    return bool(x and re.search(r"item1b|item2", str(x), re.I))

                end_el = start_el.find_next(id=end_matcher)
                if end_el:
                    # Extract text between anchors
                    risk_text = _extract_between_elements(start_el, end_el)
                    if risk_text and len(risk_text) > 100:
                        cleaned = _clean_risk_text(risk_text)
                        if "risk" in cleaned.lower() or "factors" in cleaned.lower():
                            return cleaned

        # Strategy 2: Try direct ID pattern (id="item1a" or similar)
        # Look for span or other elements with id="item1a"
        def item1a_matcher(x: Any) -> bool:
            return bool(x and re.search(r"item1a", str(x), re.I))

        id_el = soup.find(id=item1a_matcher)
        if id_el:
            # If id_el is an empty span, find the next meaningful content element
            if id_el.name == "span" and not id_el.get_text(strip=True):
                # Empty span - find the table row containing it, then get next content
                table_row = id_el.find_parent("tr")
                if table_row:
                    # Start from the element after the table row
                    start_el = table_row.find_next(["p", "div"])
                else:
                    # No table row, find next content element after the span
                    start_el = id_el.find_next(["p", "div", "table"])
            else:
                # Not an empty span, use it directly
                start_el = id_el

            if start_el:
                # Find end element
                def end_id_matcher(x: Any) -> bool:
                    return bool(x and re.search(r"item1b|item2", str(x), re.I))

                end_el = start_el.find_next(id=end_id_matcher)
                if end_el:
                    risk_text = _extract_between_elements(start_el, end_el)
                    if (
                        risk_text and len(risk_text) > 100
                    ):  # Reasonable threshold for real 10-K risk sections (typically 10K+ chars)
                        cleaned = _clean_risk_text(risk_text)
                        # Business outcome: If we found Item 1A section, content is risk-related
                        # Real 10-K risk sections are typically 10,000+ characters, but we accept 100+ for edge cases
                        if len(cleaned) > 100:
                            return cleaned
                else:
                    # No end element found, extract until end of document
                    risk_text = _extract_between_elements(start_el, None, max_chars=200000)
                    if (
                        risk_text and len(risk_text) > 100
                    ):  # Reasonable threshold for real 10-K risk sections
                        cleaned = _clean_risk_text(risk_text)
                        if len(cleaned) > 100:
                            return cleaned

        # Strategy 3: Text node search (for files with unusual structure)
        # Search in parsed text (handles HTML separation better)
        parsed_text = soup.get_text(separator=" ", strip=True)

        # Try multiple patterns
        item1a_patterns = [
            r"ITEM\s+1A[\.:]?\s*RISK\s+FACTORS?",  # "ITEM 1A. Risk Factors"
            r"Item\s+1A[\.:]?\s*Risk\s+Factors?",  # "Item 1A. Risk Factors"
            r"ITEM\s+1A[\.:]",  # "ITEM 1A." or "ITEM 1A:"
            r"Item\s+1A[\.:]",  # "Item 1A." or "Item 1A:"
        ]

        for pattern in item1a_patterns:
            item1a_match = re.search(pattern, parsed_text, re.I)
            if item1a_match:
                # Found in parsed text - now find the corresponding HTML section
                # Get text after the match
                text_after_match = parsed_text[item1a_match.end() :]

                # Find Item 1B or Item 2 in parsed text
                item1b_match = re.search(r"ITEM\s+1B", text_after_match, re.I)
                item2_match = re.search(r"ITEM\s+2[\.:]?", text_after_match, re.I)

                stop_text_pos = len(text_after_match)
                if item1b_match and item2_match:
                    stop_text_pos = min(item1b_match.start(), item2_match.start())
                elif item1b_match:
                    stop_text_pos = item1b_match.start()
                elif item2_match:
                    stop_text_pos = item2_match.start()

                if stop_text_pos > 0:
                    # Extract the risk section from parsed text
                    if stop_text_pos < len(text_after_match):
                        risk_text = text_after_match[:stop_text_pos].strip()
                    else:
                        # No stop found, use all remaining text
                        risk_text = text_after_match.strip()

                    # Clean up leading punctuation/whitespace from match boundary
                    risk_text = risk_text.lstrip(". ").strip()

                    if (
                        risk_text and len(risk_text) > 100
                    ):  # Reasonable threshold for real 10-K risk sections (typically 10K+ chars)
                        cleaned = _clean_risk_text(risk_text)
                        # Business outcome: If we found Item 1A section, content is risk-related
                        # Real 10-K risk sections are typically 10,000+ characters, but we accept 100+ for edge cases
                        if len(cleaned) > 100:
                            return cleaned
                break  # Found a match, don't try other patterns

    except Exception as e:
        logger.debug(f"Error extracting risk factors from {file_path.name}: {e}")

    return None


from public_company_graph.parsing.text_extraction import extract_text_between_elements


def _extract_between_elements(start_el, end_el, max_chars: int = 200000) -> str:
    """
    Extract text between two HTML elements.

    Wrapper around the shared extract_text_between_elements utility
    configured for risk factors extraction (includes start element,
    includes tables, requires minimum text length of 5).

    Args:
        start_el: Starting element
        end_el: Ending element (None = extract until end)
        max_chars: Maximum characters to extract

    Returns:
        Extracted text
    """
    return extract_text_between_elements(
        start_el=start_el,
        end_el=end_el,
        max_chars=max_chars,
        include_start=True,
        include_tables=True,
        min_text_length=5,
    )


def _clean_risk_text(text: str) -> str:
    """
    Clean extracted risk factors text.

    Args:
        text: Raw extracted text

    Returns:
        Cleaned text
    """
    # Normalize whitespace
    text = re.sub(r"\s+", " ", text)
    text = text.strip()

    # Remove common header/footer artifacts (but keep the content)
    # Remove "Item 1A. Risk Factors." heading if it appears at the start
    text = re.sub(r"^(Item\s+1A[\.:]?\s*Risk\s+Factors?[\.:]?)\s*", "", text, flags=re.I)
    text = text.strip()

    return text


def extract_risk_factors_with_datamule_fallback(
    file_path: Path,
    cik: str | None = None,
    file_content: str | None = None,
    skip_datamule: bool = False,
    filings_dir: Path | None = None,
    soup: BeautifulSoup | None = None,
) -> str | None:
    """
    Extract Item 1A: Risk Factors using datamule as primary, custom parser as fallback.

    Strategy: Datamule first (best quality), custom parser as fallback.

    Args:
        file_path: Path to 10-K HTML file (for fallback)
        cik: Company CIK (required for datamule)
        file_content: Optional pre-read file content
        skip_datamule: If True, skip datamule and use custom parser only (faster)
        filings_dir: Optional base directory for path validation
        soup: Optional pre-parsed BeautifulSoup object (for performance)

    Returns:
        Risk factors text or None
    """
    # If skip_datamule flag is set, use custom parser directly
    if skip_datamule:
        return extract_risk_factors(
            file_path, file_content=file_content, filings_dir=filings_dir, soup=soup
        )

    if not cik:
        # Try to extract CIK from file path
        cik = file_path.parent.name
        if not cik.isdigit():
            logger.warning(f"Could not determine CIK: {file_path}")
            return extract_risk_factors(
                file_path, file_content=file_content, filings_dir=filings_dir, soup=soup
            )

    # Try datamule first (best quality)
    # OPTIMIZATION: Use cached parsed document to avoid expensive re-initialization AND re-parsing
    try:
        from public_company_graph.utils.datamule import get_cached_parsed_doc

        portfolios_dir = get_data_dir() / "10k_portfolios"
        portfolio_path = portfolios_dir / f"10k_{cik}"

        # Get cached parsed document (or create, parse, and cache it if not exists)
        # This avoids Portfolio init AND doc.parse() for subsequent parsers on same CIK
        doc = get_cached_parsed_doc(cik, portfolio_path)

        if doc is None:
            # No parsed document available - use custom parser
            return extract_risk_factors(
                file_path, file_content=file_content, filings_dir=filings_dir, soup=soup
            )

        # Extract section from cached parsed document (fast, ~0.1s)
        with suppress_datamule_output():
            if hasattr(doc, "get_section"):
                item1a = doc.get_section(title="item1a", format="text")
                if item1a:
                    item1a_text = item1a[0] if isinstance(item1a, list) else str(item1a)
                    if len(item1a_text) > 1000:
                        return item1a_text

        # Datamule failed - fallback to custom parser
        return extract_risk_factors(
            file_path, file_content=file_content, filings_dir=filings_dir, soup=soup
        )

    except ImportError:
        # datamule not available - fall back to custom parser
        return extract_risk_factors(
            file_path, file_content=file_content, filings_dir=filings_dir, soup=soup
        )
    except Exception as e:
        # Datamule error - fall back to custom parser
        logger.debug(f"Datamule error for CIK {cik}: {e}, using custom parser")
        return extract_risk_factors(
            file_path, file_content=file_content, filings_dir=filings_dir, soup=soup
        )
