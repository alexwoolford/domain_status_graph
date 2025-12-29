"""
Business relationship extraction from 10-K filings.

This module extracts business relationships (competitors, customers, suppliers, partners)
from Item 1 (Business) and Item 1A (Risk Factors) sections of 10-K filings.

Relationship Types (matching CompanyKG schema):
- competitor: Direct competitors mentioned in competitive landscape sections
- customer: Significant customers (SEC requires disclosure if >10% of revenue)
- supplier: Key suppliers and vendors
- partner: Business partners, strategic alliances

The approach:
1. Find sentences containing relationship-specific keywords
2. Extract potential company names from those sentences
3. Resolve against known companies using entity resolution lookup
4. Return resolved relationships for graph creation

Based on CompanyKG paper: https://arxiv.org/abs/2306.10649
"""

import logging
import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

logger = logging.getLogger(__name__)


class RelationshipType(Enum):
    """Business relationship types (matching CompanyKG schema)."""

    COMPETITOR = "competitor"
    CUSTOMER = "customer"
    SUPPLIER = "supplier"
    PARTNER = "partner"


# Relationship type to Neo4j relationship type mapping
RELATIONSHIP_TYPE_TO_NEO4J = {
    RelationshipType.COMPETITOR: "HAS_COMPETITOR",
    RelationshipType.CUSTOMER: "HAS_CUSTOMER",
    RelationshipType.SUPPLIER: "HAS_SUPPLIER",
    RelationshipType.PARTNER: "HAS_PARTNER",
}


@dataclass
class CompanyLookup:
    """Lookup table for entity resolution."""

    # Maps various name forms → (cik, ticker, official_name)
    name_to_company: dict[str, tuple[str, str, str]] = field(default_factory=dict)
    ticker_to_company: dict[str, tuple[str, str, str]] = field(default_factory=dict)
    # Set of all company names (for quick membership check)
    all_names: set[str] = field(default_factory=set)
    # Set of all tickers
    all_tickers: set[str] = field(default_factory=set)


@dataclass
class RelationshipMention:
    """A business relationship mention extracted from 10-K."""

    relationship_type: RelationshipType
    raw_text: str  # The raw text that was extracted
    context: str  # Surrounding context
    resolved_cik: str | None = None
    resolved_ticker: str | None = None
    resolved_name: str | None = None
    confidence: float = 0.0


# =============================================================================
# KEYWORD DEFINITIONS FOR EACH RELATIONSHIP TYPE
# =============================================================================

# Keywords that indicate competitor context
COMPETITOR_KEYWORDS = {
    "competitor",
    "competitors",
    "compete",
    "competes",
    "competing",
    "competition",
    "competitive",
    "rival",
    "rivals",
}

# Keywords that indicate customer context
# SEC requires disclosure of customers >10% of revenue
CUSTOMER_KEYWORDS = {
    "customer",
    "customers",
    "client",
    "clients",
    "significant customer",
    "major customer",
    "largest customer",
    "key customer",
    "principal customer",
    "revenue concentration",
    "customer concentration",
    "accounts for",
    "accounted for",
    "represents",
    "represented",
    "% of revenue",
    "percent of revenue",
    "% of sales",
    "percent of sales",
    "% of net revenue",
    "% of total revenue",
}

# Keywords that indicate supplier context
SUPPLIER_KEYWORDS = {
    "supplier",
    "suppliers",
    "vendor",
    "vendors",
    "supply chain",
    "supply agreement",
    "purchase agreement",
    "source",
    "sources",
    "sourcing",
    "procure",
    "procurement",
    "key supplier",
    "principal supplier",
    "sole supplier",
    "single source",
    "sole source",
    "depend on",
    "reliance on",
    "raw material",
    "raw materials",
    "component",
    "components",
    "manufacturer",
    "manufacturers",
    "contract manufacturer",
}

# Keywords that indicate partner context
PARTNER_KEYWORDS = {
    "partner",
    "partners",
    "partnership",
    "partnerships",
    "alliance",
    "alliances",
    "strategic alliance",
    "joint venture",
    "joint ventures",
    "collaboration",
    "collaborate",
    "collaborates",
    "collaborating",
    "agreement with",
    "arrangement with",
    "relationship with",
    "licensing agreement",
    "distribution agreement",
    "strategic relationship",
    "business relationship",
}

# Map relationship type to keywords
RELATIONSHIP_KEYWORDS = {
    RelationshipType.COMPETITOR: COMPETITOR_KEYWORDS,
    RelationshipType.CUSTOMER: CUSTOMER_KEYWORDS,
    RelationshipType.SUPPLIER: SUPPLIER_KEYWORDS,
    RelationshipType.PARTNER: PARTNER_KEYWORDS,
}


# =============================================================================
# BLOCKLISTS (shared across all relationship types)
# =============================================================================

# Blocklist: Common words/abbreviations that match ticker symbols
TICKER_BLOCKLIST = {
    # 2-letter words
    "AN",
    "BY",
    "ON",
    "OR",
    "SO",
    "BE",
    "DO",
    "GO",
    "IN",
    "AT",
    "IS",
    "UP",
    "WE",
    "AB",
    "SA",
    "AS",
    "EU",
    "UK",
    # 3-4 letter common words
    "FOR",
    "ALL",
    "ANY",
    "ARE",
    "CAN",
    "HAS",
    "NOW",
    "ONE",
    "OUT",
    "SEE",
    "TWO",
    "BIG",
    "NEW",
    "OLD",
    "OUR",
    "THE",
    "AND",
    "ACT",
    "YOU",
    "CAR",
    "NET",
    "BDC",
    "HHS",
    "ACA",
    "ESG",
    "DMA",
    # Longer common words
    "WHEN",
    "MOST",
    "ALSO",
    "ONLY",
    "VERY",
    "WELL",
    "EVEN",
    "JUST",
    "SOME",
    "SUCH",
    "MANY",
    "BOTH",
    "EACH",
    "MORE",
    "MUST",
    "WILL",
    "BEEN",
    "REAL",
    "DRUG",
    "FORM",
    # Technical acronyms
    "IT",
    "PC",
    "AI",
    "IP",
    "HR",
    "PR",
    "AG",
    "EC",
    "ASIC",
    "DSP",
    "GPU",
    "CPU",
    "SOC",
    # Geographic
    "US",
    "USA",
    "EEA",
    # Single letters
    "A",
    "B",
    "C",
    "D",
    "E",
    "F",
    "G",
    "H",
    "I",
    "J",
    "K",
    "L",
    "M",
    "N",
    "O",
    "P",
    "Q",
    "R",
    "S",
    "T",
    "U",
    "V",
    "W",
    "X",
    "Y",
    "Z",
}

# Company names that are also common English words
NAME_BLOCKLIST = {
    "reliance",
    "alliance",
    "target",
    "focus",
    "insight",
    "advantage",
    "premier",
    "progress",
    "catalyst",
    "service",
    "services",
    "system",
    "systems",
    "technology",
    "technologies",
    "solution",
    "solutions",
    "platform",
    "platforms",
    "group",
    "holdings",
    "partners",
    "associates",
    "ventures",
    "industries",
    "enterprises",
    "management",
    "investment",
    "investments",
    "financial",
    "securities",
    "resources",
    "capital",
}


# =============================================================================
# LOOKUP TABLE BUILDING
# =============================================================================


def build_company_lookup(driver, database: str | None = None) -> CompanyLookup:
    """
    Build a lookup table from Neo4j Company nodes for entity resolution.

    Creates multiple name variations for each company to improve matching.

    Args:
        driver: Neo4j driver
        database: Neo4j database name

    Returns:
        CompanyLookup with name → company mappings
    """
    lookup = CompanyLookup()

    query = """
    MATCH (c:Company)
    WHERE c.cik IS NOT NULL AND c.name IS NOT NULL
    RETURN c.cik as cik, c.ticker as ticker, c.name as name
    """

    with driver.session(database=database) as session:
        result = session.run(query)
        for record in result:
            cik = record["cik"]
            ticker = record["ticker"] or ""
            name = record["name"] or ""

            company_tuple = (cik, ticker, name)

            # Add full name (lowercased)
            name_lower = name.lower().strip()
            if name_lower:
                lookup.name_to_company[name_lower] = company_tuple
                lookup.all_names.add(name_lower)

            # Add name without common suffixes
            clean_name = _normalize_company_name(name)
            if clean_name and clean_name != name_lower:
                lookup.name_to_company[clean_name] = company_tuple
                lookup.all_names.add(clean_name)

            # Add ticker (uppercase)
            if ticker:
                ticker_upper = ticker.upper().strip()
                lookup.ticker_to_company[ticker_upper] = company_tuple
                lookup.all_tickers.add(ticker_upper)

    logger.info(
        f"Built company lookup: {len(lookup.name_to_company)} name variants, "
        f"{len(lookup.ticker_to_company)} tickers"
    )
    return lookup


def _normalize_company_name(name: str) -> str:
    """Normalize a company name by removing common suffixes."""
    name = name.lower().strip()

    suffixes = [
        " corporation",
        " incorporated",
        " holdings ltd",
        " holding ltd",
        " holdings",
        " holding",
        " technologies",
        " technology",
        " solutions",
        " platforms",
        " services",
        " systems",
        " group",
        " corp.",
        " corp",
        " inc.",
        " inc",
        " ltd.",
        " ltd",
        " llc",
        " plc",
        " co.",
        " co",
        "/de/",
        "/md/",
        "/nv/",
    ]

    for suffix in suffixes:
        if name.endswith(suffix):
            name = name[: -len(suffix)]

    name = re.sub(r"^[\s,.\-]+|[\s,.\-]+$", "", name)
    return name.strip()


# =============================================================================
# EXTRACTION FUNCTIONS
# =============================================================================


def extract_relationship_sentences(
    text: str,
    relationship_type: RelationshipType,
) -> list[tuple[str, int]]:
    """
    Find sentences that mention a specific relationship type.

    Args:
        text: Full text to search
        relationship_type: Type of relationship to find

    Returns:
        List of (sentence, start_position) tuples
    """
    if not text:
        return []

    keywords = RELATIONSHIP_KEYWORDS[relationship_type]
    sentences = []
    current_pos = 0

    for sentence in re.split(r"(?<=[.!?])\s+", text):
        sentence = sentence.strip()
        sentence_lower = sentence.lower()

        # Check if any keyword appears in sentence
        if sentence and any(kw in sentence_lower for kw in keywords):
            sentences.append((sentence, current_pos))
        current_pos += len(sentence) + 1

    return sentences


def extract_and_resolve_relationships(
    business_description: str | None,
    risk_factors: str | None,
    lookup: CompanyLookup,
    relationship_type: RelationshipType,
    self_cik: str | None = None,
) -> list[dict[str, Any]]:
    """
    Extract and resolve business relationships from 10-K text.

    Args:
        business_description: Item 1 Business description text
        risk_factors: Item 1A Risk Factors text
        lookup: CompanyLookup table
        relationship_type: Type of relationship to extract
        self_cik: CIK of the company filing (to exclude self-references)

    Returns:
        List of dicts with: cik, ticker, name, confidence, raw_mention, context
    """
    results = []
    seen_ciks: set[str] = set()

    # Combine texts
    texts = []
    if business_description:
        texts.append(business_description)
    if risk_factors:
        texts.append(risk_factors)

    for text in texts:
        # Find sentences with relationship keywords
        sentences = extract_relationship_sentences(text, relationship_type)

        for sentence, _ in sentences:
            # Extract potential company names from this sentence
            # Pattern 1: Capitalized multi-word sequences (1-4 words)
            candidates = re.findall(
                r"\b([A-Z][a-zA-Z&\.\-]*(?:\s+[A-Z][a-zA-Z&\.\-]*){0,3})\b",
                sentence,
            )

            # Pattern 2: All-caps sequences (2-5 chars) - likely tickers
            candidates += re.findall(r"\b([A-Z]{2,5})\b", sentence)

            for candidate in candidates:
                candidate = candidate.strip()
                if len(candidate) < 2:
                    continue

                # Try to resolve against lookup
                resolved = _resolve_candidate(candidate, lookup, self_cik)

                if resolved and resolved["cik"] not in seen_ciks:
                    seen_ciks.add(resolved["cik"])
                    results.append(
                        {
                            "target_cik": resolved["cik"],
                            "target_ticker": resolved["ticker"],
                            "target_name": resolved["name"],
                            "confidence": resolved["confidence"],
                            "raw_mention": candidate,
                            "context": sentence[:200],
                            "relationship_type": relationship_type.value,
                        }
                    )

    return results


def _resolve_candidate(
    candidate: str,
    lookup: CompanyLookup,
    self_cik: str | None,
) -> dict[str, Any] | None:
    """Try to resolve a candidate string to a known company."""
    candidate_lower = candidate.lower().strip()
    candidate_upper = candidate.upper().strip()

    # Skip blocklisted terms
    if candidate_upper in TICKER_BLOCKLIST:
        return None
    if candidate_lower in NAME_BLOCKLIST:
        return None

    # Try exact ticker match (highest confidence)
    if candidate_upper in lookup.ticker_to_company:
        cik, ticker, name = lookup.ticker_to_company[candidate_upper]
        if cik != self_cik:
            return {"cik": cik, "ticker": ticker, "name": name, "confidence": 1.0}

    # Try exact name match
    if candidate_lower in lookup.name_to_company:
        cik, ticker, name = lookup.name_to_company[candidate_lower]
        if cik != self_cik:
            return {"cik": cik, "ticker": ticker, "name": name, "confidence": 1.0}

    # Try normalized name match
    normalized = _normalize_company_name(candidate)
    if normalized and normalized in lookup.name_to_company:
        cik, ticker, name = lookup.name_to_company[normalized]
        if cik != self_cik:
            return {"cik": cik, "ticker": ticker, "name": name, "confidence": 0.95}

    return None


def extract_all_relationships(
    business_description: str | None,
    risk_factors: str | None,
    lookup: CompanyLookup,
    self_cik: str | None = None,
    relationship_types: list[RelationshipType] | None = None,
) -> dict[RelationshipType, list[dict[str, Any]]]:
    """
    Extract all business relationships from 10-K text.

    Convenience function to extract all relationship types in one pass.

    Args:
        business_description: Item 1 Business description text
        risk_factors: Item 1A Risk Factors text
        lookup: CompanyLookup table
        self_cik: CIK of the company filing
        relationship_types: Types to extract (default: all)

    Returns:
        Dict mapping relationship type → list of extracted relationships
    """
    if relationship_types is None:
        relationship_types = list(RelationshipType)

    results = {}
    for rel_type in relationship_types:
        results[rel_type] = extract_and_resolve_relationships(
            business_description=business_description,
            risk_factors=risk_factors,
            lookup=lookup,
            relationship_type=rel_type,
            self_cik=self_cik,
        )

    return results
