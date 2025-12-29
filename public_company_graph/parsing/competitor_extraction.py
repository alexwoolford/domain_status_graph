"""
Competitor extraction from 10-K filings.

This module extracts competitor mentions from Item 1 (Business) and Item 1A (Risk Factors)
sections of 10-K filings, then resolves them to companies in our graph using entity resolution.

The approach:
1. Extract text mentioning competitors from business description and risk factors
2. Use pattern matching to identify company names in competitive contexts
3. Resolve extracted names to Company nodes using fuzzy matching on names/tickers
4. Return list of resolved competitor CIKs (for creating HAS_COMPETITOR relationships)

Entity Resolution Strategy:
- Build a lookup table of company names → CIK from Neo4j
- Include variations: full name, normalized name, ticker, common aliases
- Use fuzzy matching with a high threshold to avoid false positives
- Only return high-confidence matches (exact or near-exact)
"""

import logging
import re
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class CompetitorMention:
    """A competitor mentioned in a 10-K filing."""

    raw_text: str  # The raw text that was extracted (e.g., "Intel Corporation")
    context: str  # The surrounding context where it was found
    resolved_cik: str | None = None  # CIK if resolved, None otherwise
    resolved_ticker: str | None = None  # Ticker if resolved
    resolved_name: str | None = None  # Official name if resolved
    confidence: float = 0.0  # Confidence score (0-1)


@dataclass
class CompetitorLookup:
    """Lookup table for entity resolution."""

    # Maps various name forms → (cik, ticker, official_name)
    name_to_company: dict[str, tuple[str, str, str]] = field(default_factory=dict)
    ticker_to_company: dict[str, tuple[str, str, str]] = field(default_factory=dict)
    # Set of all company names (for quick membership check)
    all_names: set[str] = field(default_factory=set)
    # Set of all tickers
    all_tickers: set[str] = field(default_factory=set)


def build_competitor_lookup(driver, database: str | None = None) -> CompetitorLookup:
    """
    Build a lookup table from Neo4j Company nodes for entity resolution.

    Creates multiple name variations for each company to improve matching:
    - Full official name (e.g., "INTEL CORP")
    - Normalized name (e.g., "intel corp")
    - Name without suffixes (e.g., "intel")
    - Ticker symbol (e.g., "INTC")

    Args:
        driver: Neo4j driver
        database: Neo4j database name

    Returns:
        CompetitorLookup with name → company mappings
    """
    lookup = CompetitorLookup()

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
        f"Built competitor lookup: {len(lookup.name_to_company)} name variants, "
        f"{len(lookup.ticker_to_company)} tickers"
    )
    return lookup


def _normalize_company_name(name: str) -> str:
    """
    Normalize a company name by removing common suffixes and cleaning.

    Examples:
        "INTEL CORP" → "intel"
        "Apple Inc." → "apple"
        "NVIDIA Corporation" → "nvidia"
        "Meta Platforms, Inc." → "meta platforms"
    """
    name = name.lower().strip()

    # Remove common suffixes (order matters - longer first)
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

    # Remove leading/trailing punctuation and whitespace
    name = re.sub(r"^[\s,.\-]+|[\s,.\-]+$", "", name)

    return name.strip()


# Patterns that indicate competitor context - more precise patterns first
# These patterns MUST have competitor/competition/compete context to avoid false positives
COMPETITOR_CONTEXT_PATTERNS = [
    # "Our current competitors include:" followed by bullet points
    # Capture large block after this phrase (stops at Patents/IP sections)
    r"[Oo]ur\s+(?:current\s+|primary\s+|principal\s+)?competitors?\s+include:?\s*(.{100,3000}?)(?:Patents|We rely|Intellectual|Our\s+(?:intellectual|business)|[A-Z][a-z]+\s+and\s+Proprietary|$)",
    # "Competitors to X include ... such as Y" (Microsoft style)
    r"[Cc]ompetitors?\s+(?:to\s+[\w\s]+\s+)?include\s+[^.]+such\s+as\s+([A-Z][^.]+)",
    # "principal/primary competitor is X" or "competitor in X is Y"
    r"(?:principal\s+|primary\s+)?competitor\s+(?:in\s+[\w\s]+\s+)?(?:is|are)\s+([A-Z][^.]{5,150})",
    # "compete with products from X, Y, Z" (Microsoft style)
    r"compete\s+with\s+(?:products?\s+from\s+)?([A-Z][^.]{10,200})",
    # "compete against X" - must have company suffix nearby
    r"compete\s+(?:directly\s+)?against\s+([A-Z][^.]{10,200}(?:Inc\.|Corp\.|Corporation|Ltd\.))",
    # "competition from X" with company suffix
    r"competition\s+from\s+(?:our\s+)?(?:primary\s+)?(?:\w+\s+)?(?:competitors?\s+such\s+as\s+)?([A-Z][^.]{10,200}(?:Inc\.|Corp\.|Corporation|Ltd\.))",
    # "competitors such as X" - company names with suffixes
    r"competitors?\s+such\s+as\s+([A-Z][^.]{10,200})",
    # "also compete with X" (common pattern)
    r"also\s+compete\s+with\s+([A-Z][^.]{10,150})",
]

# Secondary patterns to extract "such as X, Y, Z" from within competitor blocks
# These are applied to text already identified as competitor context
SUCH_AS_PATTERN = re.compile(r"such\s+as\s+([^;•\n]+)", re.I)

# Patterns to extract company names from competitive context
# Multiple patterns ordered by specificity
COMPANY_NAME_PATTERNS = [
    # Company with explicit suffix - HIGHEST QUALITY
    # e.g., "Intel Corporation", "NVIDIA Corporation", "Lattice Semiconductor Corporation"
    r"\b([A-Z][a-zA-Z0-9&\.\-]+(?:\s+[A-Z][a-zA-Z0-9&\.\-]+)*)\s+(?:Corporation|Corp\.?|Inc\.?|Ltd\.?|LLC|Company|Co\.)\b",
    # Multi-word capitalized names (2-4 words)
    # e.g., "Advanced Micro Devices", "International Business Machines"
    r"\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,4})\b",
    # Single capitalized proper name (will be validated against lookup)
    # Must be at least 4 chars to avoid noise like "ARM"
    r"\b([A-Z][a-z]{4,15})\b",
    # All-caps company names (3-8 chars) - tickers or acronyms
    # e.g., "NVIDIA", "INTC", "AMD" - validated against lookup
    r"\b([A-Z]{3,8})\b",
]


def extract_competitor_mentions(
    business_description: str | None,
    risk_factors: str | None,
    self_cik: str | None = None,
) -> list[CompetitorMention]:
    """
    Extract competitor mentions from 10-K text sections.

    Searches for patterns indicating competitive relationships and extracts
    company names from those contexts.

    Args:
        business_description: Item 1 Business description text
        risk_factors: Item 1A Risk Factors text
        self_cik: CIK of the company filing (to exclude self-references)

    Returns:
        List of CompetitorMention objects (not yet resolved)
    """
    mentions: list[CompetitorMention] = []
    seen_names: set[str] = set()  # Deduplicate

    # Combine texts
    texts = []
    if business_description:
        texts.append(("business_description", business_description))
    if risk_factors:
        texts.append(("risk_factors", risk_factors))

    for _source, text in texts:
        # Find competitor contexts
        for pattern in COMPETITOR_CONTEXT_PATTERNS:
            for match in re.finditer(pattern, text, re.IGNORECASE | re.DOTALL):
                context = match.group(0)
                captured = match.group(1) if match.lastindex else context

                # For large blocks (like bullet-point lists), also extract from "such as" clauses
                if len(captured) > 200:
                    for such_as_match in SUCH_AS_PATTERN.finditer(captured):
                        such_as_text = such_as_match.group(1)
                        _extract_names_from_text(such_as_text, context[:200], mentions, seen_names)

                # Extract company names from the captured text
                _extract_names_from_text(captured, context[:200], mentions, seen_names)

    return mentions


def _extract_names_from_text(
    text: str,
    context: str,
    mentions: list[CompetitorMention],
    seen_names: set[str],
) -> None:
    """Extract company names from text and add to mentions list."""
    for name_pattern in COMPANY_NAME_PATTERNS:
        for name_match in re.finditer(name_pattern, text):
            raw_name = name_match.group(1).strip()

            # Skip if too short or too long
            if len(raw_name) < 2 or len(raw_name) > 50:
                continue

            # Skip common non-company words
            if _is_common_word(raw_name):
                continue

            # Normalize for deduplication
            norm_name = raw_name.lower()
            if norm_name in seen_names:
                continue
            seen_names.add(norm_name)

            mentions.append(
                CompetitorMention(
                    raw_text=raw_name,
                    context=context,
                )
            )


# Common words to skip (not company names)
COMMON_WORDS = {
    # Generic business terms
    "company",
    "companies",
    "corporation",
    "corporations",
    "business",
    "businesses",
    "industry",
    "industries",
    "market",
    "markets",
    "product",
    "products",
    "service",
    "services",
    "customer",
    "customers",
    "competitor",
    "competitors",
    "competition",
    "competitive",
    # Common adjectives/articles
    "the",
    "and",
    "other",
    "certain",
    "various",
    "many",
    "some",
    "all",
    "our",
    "their",
    "these",
    "those",
    "such",
    "including",
    "particularly",
    "especially",
    "primarily",
    "mainly",
    # Generic tech terms that aren't companies
    "software",
    "hardware",
    "platform",
    "platforms",
    "technology",
    "technologies",
    "solution",
    "solutions",
    "system",
    "systems",
    "application",
    "applications",
    # Section headers
    "item",
    "risk",
    "factors",
    "overview",
    "table",
    "contents",
    # Common words that match ticker symbols but aren't companies in context
    "global",
    "rock",
    "live",
    "usa",
    "new",
    "big",
    "sun",
    "sky",
    "sea",
    "pro",
    "one",
    "two",
    "now",
    "core",
    "next",
    "fast",
    "best",
    "well",
    "high",
    "true",
    "real",
    "open",
    "free",
    "safe",
    "good",
    "hope",
    "care",
    "play",
    "life",
    "love",
    "star",
    "gold",
    "blue",
    "peak",
    "plus",
    "key",
    "way",
    "act",
    "fit",
    "hub",
    "win",
    "max",
    "air",
    "net",
    "icon",
    "west",
    "east",
    "north",
    "south",
    "central",
    "national",
    "international",
    "foreign",
    "domestic",
    "local",
    "regional",
    "federal",
    "state",
    "city",
    "united",
    "american",
    "first",
    "second",
    "third",
    "primary",
    "large",
    "small",
    "mid",
    "medium",
    "capital",
    "resources",
    "science",
    "synergy",
    "energy",
    "power",
    "dynamic",
    "strategic",
    "advanced",
    # Technical terms that look like company names
    "adaptive",
    "fpga",
    "cpu",
    "gpu",
    "dpu",
    "soc",
    "asic",
    "arm",  # ARM the architecture vs ARM Holdings - context dependent
    "semiconductor",
    "semiconductors",
    "microprocessor",
    "microprocessors",
    "embedded",
    "discrete",
    "integrated",
    # Words that often appear in company names but aren't companies alone
    "group",
    "holdings",
    "partners",
    "associates",
    "ventures",
    "enterprises",
    "management",
    "investment",
    "investments",
    "financial",
    "securities",
    # Geographic terms that match ticker symbols
    "china",
    "taiwan",
    "europe",
    "asia",
    "latin",
    "america",
    "southeast",
    "pacific",
    "atlantic",
    # Product/service names that aren't companies
    "cloud",
    "dgx",
    "omniverse",
    "foundations",
    # More technical terms
    "gdpr",
    "manufacturing",
    "limited",
    "micro",
    "devices",
    "networks",
    # Geographic/directional terms that match company names
    "australian",
    "canadian",
    "british",
    "european",
    "asian",
    "african",
    "northern",
    "southern",
    "eastern",
    "western",
    "continental",
    "coastal",
    "mobile",
    "emerald",
    "diamond",
    "platinum",
    "silver",
    "bronze",
    # Business operation terms
    "independent",
    "commercial",
    "industrial",
    "residential",
    "municipal",
    "retail",
    "wholesale",
    # Country/region names that match company names
    "states",
    "united states",
    "california",
    "texas",
    "canada",
    "goose",  # "Canada Goose" is clothing company, not competitor
    "health",  # too generic
    "medical",  # too generic
    "scientific",
    "information",
    "enterprise",
}


def _is_common_word(word: str) -> bool:
    """Check if a word is a common non-company word."""
    return word.lower() in COMMON_WORDS


def resolve_competitors(
    mentions: list[CompetitorMention],
    lookup: CompetitorLookup,
    self_cik: str | None = None,
    min_confidence: float = 0.8,
) -> list[CompetitorMention]:
    """
    Resolve competitor mentions to Company nodes using entity resolution.

    Uses the lookup table to match extracted names to known companies.
    Only returns high-confidence matches to avoid false positives.

    Args:
        mentions: List of unresolved CompetitorMention objects
        lookup: CompetitorLookup table built from Neo4j
        self_cik: CIK of the company filing (to exclude self-references)
        min_confidence: Minimum confidence threshold for matches

    Returns:
        List of CompetitorMention objects with resolved_cik populated for matches
    """
    resolved = []

    for mention in mentions:
        raw = mention.raw_text
        raw_lower = raw.lower().strip()
        raw_upper = raw.upper().strip()

        # Try exact ticker match first (highest confidence)
        if raw_upper in lookup.ticker_to_company:
            cik, ticker, name = lookup.ticker_to_company[raw_upper]
            if cik != self_cik:  # Exclude self
                mention.resolved_cik = cik
                mention.resolved_ticker = ticker
                mention.resolved_name = name
                mention.confidence = 1.0
                resolved.append(mention)
                continue

        # Try exact name match
        if raw_lower in lookup.name_to_company:
            cik, ticker, name = lookup.name_to_company[raw_lower]
            if cik != self_cik:
                mention.resolved_cik = cik
                mention.resolved_ticker = ticker
                mention.resolved_name = name
                mention.confidence = 1.0
                resolved.append(mention)
                continue

        # Try normalized name match
        normalized = _normalize_company_name(raw)
        if normalized in lookup.name_to_company:
            cik, ticker, name = lookup.name_to_company[normalized]
            if cik != self_cik:
                mention.resolved_cik = cik
                mention.resolved_ticker = ticker
                mention.resolved_name = name
                mention.confidence = 0.9
                resolved.append(mention)
                continue

        # Try partial match (name starts with or contains)
        # Lower confidence, but can catch "Intel" matching "INTEL CORP"
        best_match = _find_best_partial_match(normalized, lookup, min_confidence)
        if best_match:
            cik, ticker, name, conf = best_match
            if cik != self_cik:
                mention.resolved_cik = cik
                mention.resolved_ticker = ticker
                mention.resolved_name = name
                mention.confidence = conf
                resolved.append(mention)

    return resolved


def _find_best_partial_match(
    query: str, lookup: CompetitorLookup, min_confidence: float
) -> tuple[str, str, str, float] | None:
    """
    Find the best partial match for a company name.

    Uses substring matching with confidence based on match quality.
    """
    if len(query) < 3:
        return None

    best: tuple[str, str, str, float] | None = None
    best_conf = min_confidence

    for name in lookup.all_names:
        # Skip if the query is longer than the name
        if len(query) > len(name):
            continue

        # Exact start match (e.g., "intel" matches "intel corp")
        if name.startswith(query + " ") or name == query:
            conf = 0.95 if name.startswith(query + " ") else 1.0
            if conf > best_conf:
                cik, ticker, official = lookup.name_to_company[name]
                best = (cik, ticker, official, conf)
                best_conf = conf
            continue

        # Query is a significant prefix (> 60% of name)
        if name.startswith(query) and len(query) / len(name) > 0.6:
            conf = 0.85 * (len(query) / len(name))
            if conf > best_conf:
                cik, ticker, official = lookup.name_to_company[name]
                best = (cik, ticker, official, conf)
                best_conf = conf

    return best


def extract_and_resolve_competitors(
    business_description: str | None,
    risk_factors: str | None,
    lookup: CompetitorLookup,
    self_cik: str | None = None,
    min_confidence: float = 0.8,
) -> list[dict[str, Any]]:
    """
    Main function to extract and resolve competitors from 10-K text.

    This is the primary API for competitor extraction (regex-based method).

    Args:
        business_description: Item 1 Business description text
        risk_factors: Item 1A Risk Factors text
        lookup: CompetitorLookup table (build once, reuse)
        self_cik: CIK of the company filing
        min_confidence: Minimum confidence for entity resolution

    Returns:
        List of dicts with: cik, ticker, name, confidence, raw_mention, context
    """
    # Extract mentions
    mentions = extract_competitor_mentions(
        business_description=business_description,
        risk_factors=risk_factors,
        self_cik=self_cik,
    )

    # Resolve to known companies
    resolved = resolve_competitors(
        mentions=mentions,
        lookup=lookup,
        self_cik=self_cik,
        min_confidence=min_confidence,
    )

    # Deduplicate by CIK and return as dicts
    seen_ciks: set[str] = set()
    results = []

    for mention in resolved:
        if mention.resolved_cik and mention.resolved_cik not in seen_ciks:
            seen_ciks.add(mention.resolved_cik)
            results.append(
                {
                    "competitor_cik": mention.resolved_cik,
                    "competitor_ticker": mention.resolved_ticker,
                    "competitor_name": mention.resolved_name,
                    "confidence": mention.confidence,
                    "raw_mention": mention.raw_text,
                    "context": mention.context,
                }
            )

    return results


# =============================================================================
# SIMPLIFIED METHOD: Keyword + Lookup approach
# =============================================================================

# Keywords that indicate competitor context (much simpler than regex patterns)
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

# Blocklist: Common words/abbreviations that match ticker symbols
# These produce systematic false positives when extracting competitors
TICKER_BLOCKLIST = {
    # 2-letter English words that are tickers
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
    "SA",  # Common abbreviations
    # Common 3-4 letter English words that are tickers
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
    "BDC",  # Business Development Company (financial term)
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
    "DRUG",  # Common pharma term
    # Technical acronyms
    "IT",
    "PC",
    "AI",
    "IP",
    "HR",
    "PR",
    "AG",  # German company suffix
    "HHS",  # Health & Human Services
    "ACA",  # Affordable Care Act
    "EC",  # European Commission
    "ESG",  # Environmental Social Governance
    # Technical terms
    "ASIC",
    "DSP",
    "GPU",
    "CPU",
    "SOC",
    # Ambiguous tickers
    "NXP",
    "UTI",
    "FORM",
    "RS",
    "US",
    "USA",
    # Geographic abbreviations
    "AS",
    "EU",
    "UK",
    "EEA",
    "DMA",
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
# These produce false positives when the word appears in competitor context
# but refers to the common word meaning, not the company
NAME_BLOCKLIST = {
    "reliance",  # Reliance Inc - but "reliance on X" is common
    "alliance",  # Alliance Data - but "alliance with X" is common
    "target",  # Target Corp - but "target market" is common
    "focus",  # Focus Financial - but "focus on X" is common
    "insight",  # Insight Enterprises - but "insight into X" is common
    "advantage",  # Advantage Solutions - but "competitive advantage" is common
    "premier",  # Premier Inc - but "premier provider" is common
    "progress",  # Progress Software - but "progress in X" is common
    "catalyst",  # Catalyst Pharmaceuticals - but "catalyst for X" is common
}


def extract_competitor_sentences(text: str) -> list[tuple[str, int]]:
    """
    Find sentences that mention competitors.

    Args:
        text: Full text to search

    Returns:
        List of (sentence, start_position) tuples
    """
    if not text:
        return []

    # Split into sentences (simple approach - split on period followed by space/newline)
    # Keep track of position for context
    sentences = []
    current_pos = 0

    for sentence in re.split(r"(?<=[.!?])\s+", text):
        sentence = sentence.strip()
        if sentence and any(kw in sentence.lower() for kw in COMPETITOR_KEYWORDS):
            sentences.append((sentence, current_pos))
        current_pos += len(sentence) + 1

    return sentences


def extract_and_resolve_competitors_simple(
    business_description: str | None,
    risk_factors: str | None,
    lookup: CompetitorLookup,
    self_cik: str | None = None,
) -> list[dict[str, Any]]:
    """
    Simplified competitor extraction using keyword + lookup approach.

    This method is simpler and more robust:
    1. Find sentences containing competitor keywords
    2. Extract ALL capitalized proper nouns from those sentences
    3. Only return matches that exist in our company lookup

    The key insight: The lookup table IS the validation.
    No need for complex blocklists - if it's not a known company, we don't return it.

    Args:
        business_description: Item 1 Business description text
        risk_factors: Item 1A Risk Factors text
        lookup: CompetitorLookup table (build once, reuse)
        self_cik: CIK of the company filing

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
        # Find sentences with competitor keywords
        competitor_sentences = extract_competitor_sentences(text)

        for sentence, _ in competitor_sentences:
            # Extract potential company names from this sentence
            # Pattern 1: Capitalized multi-word sequences (1-4 words)
            # e.g., "Intel Corporation", "Advanced Micro Devices"
            candidates = re.findall(
                r"\b([A-Z][a-zA-Z&\.\-]*(?:\s+[A-Z][a-zA-Z&\.\-]*){0,3})\b",
                sentence,
            )

            # Pattern 2: All-caps sequences (2-5 chars) - likely tickers
            # e.g., "NVDA", "AMD", "IBM"
            candidates += re.findall(r"\b([A-Z]{2,5})\b", sentence)

            for candidate in candidates:
                candidate = candidate.strip()
                if len(candidate) < 2:
                    continue

                # Try to resolve against lookup
                resolved = _resolve_candidate_simple(candidate, lookup, self_cik)

                if resolved and resolved["cik"] not in seen_ciks:
                    seen_ciks.add(resolved["cik"])
                    results.append(
                        {
                            "competitor_cik": resolved["cik"],
                            "competitor_ticker": resolved["ticker"],
                            "competitor_name": resolved["name"],
                            "confidence": resolved["confidence"],
                            "raw_mention": candidate,
                            "context": sentence[:200],
                        }
                    )

    return results


def _resolve_candidate_simple(
    candidate: str,
    lookup: CompetitorLookup,
    self_cik: str | None,
) -> dict[str, Any] | None:
    """
    Try to resolve a candidate string to a known company.

    The lookup table is the source of truth.
    Uses blocklists for common words that match tickers or company names.

    Args:
        candidate: The candidate company name or ticker
        lookup: CompetitorLookup table
        self_cik: CIK to exclude (self-reference)

    Returns:
        Dict with cik, ticker, name, confidence if matched, else None
    """
    candidate_lower = candidate.lower().strip()
    candidate_upper = candidate.upper().strip()

    # Skip common words that match ticker symbols
    if candidate_upper in TICKER_BLOCKLIST:
        return None

    # Skip common English words that are also company names
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

    # No partial matching - if it doesn't match exactly, it's not a match
    # This is the key simplification: no fuzzy matching
    return None
