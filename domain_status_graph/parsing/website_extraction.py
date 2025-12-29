"""
Website extraction from 10-K filings.

This module provides functions to extract company websites from 10-K HTML/XML files
using multiple strategies with proper priority ordering.
"""

import logging
import re
from pathlib import Path
from typing import Any, cast
from urllib.parse import urlparse

from bs4 import BeautifulSoup

try:
    from ixbrlparse import IXBRL

    HAS_IXBRLPARSE = True
except ImportError:
    HAS_IXBRLPARSE = False
    IXBRL = None  # type: ignore

try:
    # Use defusedxml to prevent XXE attacks
    from defusedxml.ElementTree import fromstring as safe_fromstring

    HAS_DEFUSEDXML = True
except ImportError:
    # Fallback: Use standard library (Python 3.13+ has better XXE protection)
    import xml.etree.ElementTree as ET

    HAS_DEFUSEDXML = False

from domain_status_graph.domain.validation import (
    is_valid_domain,
    normalize_domain,
    root_domain,
)

logger = logging.getLogger(__name__)

# Improved domain regex that captures multi-label domains
DOMAIN_RE = re.compile(
    r"\b((?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+(?:[a-z]{2,63}))\b",
    re.I,
)


def normalize_website_url(url: str) -> str | None:
    """
    Normalize website URL to just the domain using centralized validation.

    Examples:
        "http://www.apple.com" -> "apple.com"
        "https://www.microsoft.com/" -> "microsoft.com"
        "www.google.com" -> "google.com"
        "investor.apple.com" -> "apple.com" (extract root domain)
        "example.co.uk" -> "example.co.uk" (handles complex TLDs)

    Args:
        url: Raw URL string

    Returns:
        Normalized domain (validated) or None if invalid
    """
    return normalize_domain(url)


def extract_website_from_ixbrl_element(html: str, soup: BeautifulSoup | None = None) -> str | None:
    """
    Extract website from official dei:EntityWebSite iXBRL element.

    This is the SEC-mandated way to tag company websites in iXBRL filings.
    The element can appear in various formats:
    - <span name="dei:EntityWebSite">...</span>
    - <div id="dei-EntityWebSite">...</div>
    - <ix:nonNumeric data-ixbrl="dei:EntityWebSite">...</ix:nonNumeric>

    Args:
        html: HTML content of 10-K filing
        soup: Optional pre-parsed BeautifulSoup object (for performance)

    Returns:
        Normalized domain or None if not found or invalid
    """
    # Try ixbrlparse library first (most reliable)
    if HAS_IXBRLPARSE and IXBRL is not None:
        try:
            ixbrl = IXBRL(cast(Any, html))
            # Look for EntityWebSite element
            for fact in ixbrl.facts:
                if "EntityWebSite" in str(fact.name).lower():
                    value = str(fact.value).strip()
                    if value:
                        normalized = normalize_website_url(value)
                        if normalized and is_valid_domain(normalized):
                            return normalized
        except Exception:
            pass  # Fall back to BeautifulSoup parsing

    # Fallback: Parse with BeautifulSoup (reuse if provided)
    # Use lxml parser which is ~25% faster than html.parser
    if soup is None:
        try:
            soup = BeautifulSoup(html, "lxml")
        except Exception:
            soup = BeautifulSoup(html, "html.parser")

    # Look for EntityWebSite in various attribute formats
    patterns = [
        {"name": re.compile(r"EntityWebSite", re.I)},
        {"id": re.compile(r"EntityWebSite", re.I)},
        {"data-ixbrl": re.compile(r"EntityWebSite", re.I)},
        {"class": re.compile(r"EntityWebSite", re.I)},
    ]

    for pattern in patterns:
        elements = soup.find_all(attrs=cast(Any, pattern))
        for elem in elements:
            text = elem.text.strip()
            if text:
                normalized = normalize_website_url(text)
                if normalized and is_valid_domain(normalized):
                    return normalized

    return None


def extract_domains_from_ixbrl_namespaces(html: str) -> list[str]:
    """
    Extract candidate domains from xmlns:* declarations in the <html ...> tag.

    Many iXBRL filings include custom extension namespaces that use company domains,
    e.g., xmlns:air="http://www.aarcorp.com/20240531" -> aarcorp.com

    Args:
        html: HTML content (first 20KB is sufficient)

    Returns:
        List of candidate root domains (deduplicated, preserving order, validated)
    """
    head = html[:20000]
    xmlns_urls = re.findall(r'\sxmlns:[a-zA-Z0-9_-]+="([^"]+)"', head)
    candidates = []
    for u in xmlns_urls:
        netloc = urlparse(u).netloc if "://" in u else u.split("/")[0]
        if not netloc:
            continue
        rd = root_domain(netloc)
        # Only include if valid domain (tldextract validation)
        if rd and is_valid_domain(rd):
            candidates.append(rd)

    # Dedupe preserving order
    out, seen = [], set()
    for c in candidates:
        if c not in seen:
            out.append(c)
            seen.add(c)
    return out


def extract_domains_from_visible_text(
    html: str, max_chars: int = 200000, soup: BeautifulSoup | None = None
) -> list[str]:
    """
    Extract domain candidates from visible text (excluding script/style tags).

    Args:
        html: HTML content
        max_chars: Maximum characters to process
        soup: Optional pre-parsed BeautifulSoup object (for performance)

    Returns:
        List of candidate root domains (validated)
    """
    # Reuse soup if provided, otherwise parse (but don't modify the original)
    # Use lxml parser which is ~25% faster than html.parser
    if soup is None:
        try:
            soup = BeautifulSoup(html, "lxml")
        except Exception:
            soup = BeautifulSoup(html, "html.parser")
        # Safe to modify since we created it
        for tag in soup(["script", "style", "noscript"]):
            tag.decompose()
    else:
        # Don't modify shared soup - just extract text
        pass

    text = soup.get_text(" ", strip=True)[:max_chars]

    candidates = []
    for m in DOMAIN_RE.finditer(text):
        rd = root_domain(m.group(1))
        # Only include if valid domain
        if rd and is_valid_domain(rd):
            candidates.append(rd)

    return candidates


def choose_best_website_domain(html: str, soup: BeautifulSoup | None = None) -> str | None:
    """
    Choose the best website domain from multiple candidates using scoring.

    Priority:
    1. Domains near "internet address"/"website" keywords
    2. iXBRL namespace domains (often good signal)
    3. Generic .com domains

    Args:
        html: HTML content
        soup: Optional pre-parsed BeautifulSoup object (for performance)

    Returns:
        Best candidate domain or None if no good match
    """
    # Candidates from namespaces (fast regex, no soup needed)
    ns_candidates = extract_domains_from_ixbrl_namespaces(html)

    # Candidates from visible text (reuses soup)
    text_candidates = extract_domains_from_visible_text(html, soup=soup)

    # Parse soup if not provided (for scoring)
    # Use lxml parser which is ~25% faster than html.parser
    if soup is None:
        try:
            soup = BeautifulSoup(html, "lxml")
        except Exception:
            soup = BeautifulSoup(html, "html.parser")

    # Get text for scoring (soup might have scripts already removed or not)
    text = soup.get_text(" ", strip=True).lower()

    def score(domain: str) -> int:
        """Score a domain candidate (higher = better)."""
        # All candidates are already validated, so we can score them
        s = text.count(domain)
        # Strong keyword proximity bonus
        for m in re.finditer(re.escape(domain), text):
            window = text[max(0, m.start() - 80) : min(len(text), m.end() + 80)]
            if any(k in window for k in ["internet address", "our website", "website", "web site"]):
                s += 10
                break
        if domain.endswith(".com"):
            s += 2
        return s

    # Union candidates, score, pick best
    all_candidates = []
    for c in ns_candidates + text_candidates:
        if c not in all_candidates:
            all_candidates.append(c)

    if not all_candidates:
        return None

    best = max(all_candidates, key=score)
    return best if score(best) > 0 else None


def extract_website_from_cover_page(
    file_path: Path,
    file_content: str | None = None,
    filings_dir: Path | None = None,
    soup: BeautifulSoup | None = None,
) -> str | None:
    """
    Extract company website from 10-K cover page using proper priority order.

    Priority (in order):
    1. Official dei:EntityWebSite iXBRL element (SEC-mandated, most reliable)
    2. XML filing: companyWebsite tag
    3. Heuristic extraction from namespaces/text (with strict validation)

    Args:
        file_path: Path to 10-K HTML/XML file (must be within filings_dir if provided)
        file_content: Optional pre-read file content (avoids re-reading file)
        filings_dir: Optional base directory for path validation (if None, skips validation)
        soup: Optional pre-parsed BeautifulSoup object (for performance - avoids 3x parsing)

    Returns:
        Company website domain (normalized and validated) or None if not found
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

        # Parse HTML once and reuse for all operations (PERFORMANCE: avoids 3x parsing)
        # Use lxml parser which is ~25% faster than html.parser
        if soup is None and file_path.suffix == ".html":
            try:
                soup = BeautifulSoup(content, "lxml")
            except Exception:
                soup = BeautifulSoup(content, "html.parser")

        # Priority 1: Try official iXBRL element (SEC-mandated)
        if file_path.suffix == ".html":
            website = extract_website_from_ixbrl_element(content, soup=soup)
            if website:
                return website

        # Priority 2: Try XML structured data
        if file_path.suffix == ".xml":
            try:
                # Use defusedxml to prevent XXE attacks
                if HAS_DEFUSEDXML:
                    root = safe_fromstring(content)
                else:
                    # Fallback: Use standard library (Python 3.13+ has better XXE protection)
                    parser = ET.XMLParser()
                    root = ET.fromstring(content, parser=parser)
                # Look for companyWebsite tag
                for elem in root.iter():
                    if "companyWebsite" in elem.tag.lower() or "website" in elem.tag.lower():
                        url = elem.text
                        if url and url.strip():
                            normalized = normalize_website_url(url.strip())
                            if normalized and is_valid_domain(normalized):
                                return normalized
            except ET.ParseError:
                pass

        # Priority 3: Heuristic extraction (with strict validation, reuses soup)
        domain = choose_best_website_domain(content, soup=soup)
        if domain and is_valid_domain(domain):
            return domain

    except Exception as e:
        logger.debug(f"Error extracting website from {file_path.name}: {e}")

    return None
