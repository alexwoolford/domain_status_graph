"""
Directional Relationship Extraction.

Uses precise patterns that indicate the DIRECTION of relationships:
- SUPPLIER: "[COMPANY] supplies/provides to us" or "we purchase/source from [COMPANY]"
- CUSTOMER: "we sell/provide to [COMPANY]" or "[COMPANY] purchases/buys from us"
- COMPETITOR: "we compete with [COMPANY]" or "competitors include [COMPANY]"
- PARTNER: "we partner with [COMPANY]"

This approach dramatically improves precision by only extracting when
the company is in the correct syntactic position.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import Enum


class RelationType(Enum):
    """Types of business relationships."""

    SUPPLIER = "HAS_SUPPLIER"
    CUSTOMER = "HAS_CUSTOMER"
    COMPETITOR = "HAS_COMPETITOR"
    PARTNER = "HAS_PARTNER"


@dataclass
class DirectionalMatch:
    """A relationship match with directional confidence."""

    company_text: str  # The matched company name
    relationship_type: RelationType
    confidence: float  # 0-1 confidence
    pattern_name: str  # Which pattern matched
    full_match: str  # The full matched text
    context: str  # Surrounding sentence


# Patterns that indicate WE → THEM (outbound)
# Company appears AFTER the relationship indicator
OUTBOUND_PATTERNS = {
    RelationType.CUSTOMER: [
        # High confidence: explicit selling patterns
        (
            r"we\s+sell\s+(?:our\s+)?(?:[\w\s]+?\s+)?(?:to|through)\s+([A-Z][A-Za-z\s&,\.]+?)(?:\.|,|;|and\s)",
            0.95,
            "sell_to",
        ),
        (
            r"we\s+provide\s+(?:[\w\s]+?\s+)?to\s+([A-Z][A-Za-z\s&,\.]+?)(?:\.|,|;|and\s)",
            0.90,
            "provide_to",
        ),
        (
            r"our\s+(?:largest|major|significant|key)\s+customers?\s+(?:include|are|is)[:\s]+([A-Z][A-Za-z\s&,\.]+?)(?:\.|,|;|and\s)",
            0.95,
            "major_customer",
        ),
        (
            r"customers?\s+(?:such\s+as|like)\s+([A-Z][A-Za-z\s&,\.]+?)(?:\.|,|;|and\s)",
            0.85,
            "customer_such_as",
        ),
        (
            r"customers?\s+(?:include|including)[:\s]+([A-Z][A-Za-z\s&,\.]+?)(?:\.|,|;|and\s)",
            0.90,
            "customer_include",
        ),
        # "for customers including X"
        (
            r"for\s+customers?\s+(?:such\s+as|including|like)\s+([A-Z][A-Za-z\s&,\.]+?)(?:\.|,|;|and\s)",
            0.90,
            "for_customer",
        ),
        # "contracted with us"
        (
            r"([A-Z][A-Za-z\s&,\.]+?)\s+(?:has\s+)?contracted\s+with\s+us",
            0.90,
            "contracted_with_us",
        ),
    ],
    RelationType.PARTNER: [
        (
            r"we\s+partner(?:ed)?\s+with\s+([A-Z][A-Za-z\s&,\.]+?)(?:\.|,|;|and\s|\s+to\s)",
            0.95,
            "partner_with",
        ),
        (
            r"(?:strategic\s+)?(?:alliance|partnership)\s+with\s+([A-Z][A-Za-z\s&,\.]+?)(?:\.|,|;|and\s)",
            0.90,
            "alliance_with",
        ),
        (
            r"joint\s+venture\s+with\s+([A-Z][A-Za-z\s&,\.]+?)(?:\.|,|;|and\s)",
            0.95,
            "joint_venture",
        ),
        (
            r"collaborat(?:e|ion|ing)\s+(?:with|agreement\s+with)\s+([A-Z][A-Za-z\s&,\.]+?)(?:\.|,|;|and\s)",
            0.85,
            "collaborate_with",
        ),
        (
            r"(?:alliance|channel)\s+partners?\s+(?:include|such\s+as|like)[:\s]+([A-Z][A-Za-z\s&,\.]+?)(?:\.|,|;|and\s)",
            0.90,
            "alliance_partner",
        ),
        # "partnership with X", "the X partnership/collaboration"
        (
            r"the\s+([A-Z][A-Za-z\s&,\.]+?)\s+(?:partnership|collaboration|alliance)",
            0.85,
            "the_partnership",
        ),
        # "cobrand/co-brand arrangements with X"
        (
            r"(?:co-?brand|licensing)\s+(?:arrangement|agreement)s?\s+with\s+([A-Z][A-Za-z\s&,\.]+?)(?:\.|,|;|and\s)",
            0.85,
            "cobrand",
        ),
        # "relationship with X"
        (
            r"(?:strategic|ecosystem)\s+relationship(?:s)?\s+with\s+([A-Z][A-Za-z\s&,\.]+?)(?:\.|,|;|and\s)",
            0.80,
            "strategic_relationship",
        ),
    ],
    RelationType.COMPETITOR: [
        (
            r"we\s+compete\s+(?:with|against)\s+([A-Z][A-Za-z\s&,\.]+?)(?:\.|,|;|and\s)",
            0.95,
            "compete_with",
        ),
        (
            r"(?:principal|main|primary|key|major)\s+competitors?\s+(?:include|are|is)[:\s]+([A-Z][A-Za-z\s&,\.]+?)(?:\.|,|;|and\s)",
            0.95,
            "principal_competitor",
        ),
        (
            r"competitors?\s+(?:such\s+as|like)\s+([A-Z][A-Za-z\s&,\.]+?)(?:\.|,|;|and\s)",
            0.90,
            "competitor_such_as",
        ),
        (
            r"competitors?\s+(?:include|including)[:\s]+([A-Z][A-Za-z\s&,\.]+?)(?:\.|,|;|and\s)",
            0.90,
            "competitor_include",
        ),
        (
            r"competition\s+(?:from|includes?|with)\s+(?:companies\s+(?:such\s+as|like)\s+)?([A-Z][A-Za-z\s&,\.]+?)(?:\.|,|;|and\s)",
            0.85,
            "competition_from",
        ),
        (
            r"rival(?:s|ry)?\s+(?:such\s+as|include|from|like)\s+([A-Z][A-Za-z\s&,\.]+?)(?:\.|,|;|and\s)",
            0.85,
            "rival",
        ),
        # "Competition" section header followed by company
        (
            r"Competition[\s\n]+(?:In\s+\d+,\s+)?(?:the\s+)?(?:FDA\s+)?(?:\w+\s+)?(?:granted\s+)?(?:approval\s+)?(?:for\s+)?([A-Z][A-Za-z\s&,\.]+?)(?:'s|')",
            0.80,
            "competition_section",
        ),
        # "compete in these markets from companies such as X"
        (
            r"competition\s+in\s+(?:these|the)\s+markets?\s+from\s+(?:companies\s+(?:such\s+as|like)\s+)?([A-Z][A-Za-z\s&,\.]+?)(?:\.|,|;|and\s)",
            0.85,
            "market_competition",
        ),
    ],
}

# Patterns that indicate THEM → US (inbound)
# Company appears BEFORE the relationship indicator
INBOUND_PATTERNS = {
    RelationType.SUPPLIER: [
        # High confidence: explicit sourcing patterns
        (
            r"we\s+(?:purchase|source|buy|procure|obtain)\s+(?:[\w\s]+?\s+)?from\s+([A-Z][A-Za-z\s&,\.]+?)(?:\.|,|;|and\s)",
            0.95,
            "purchase_from",
        ),
        (
            r"(?:supplied|provided|manufactured)\s+by\s+([A-Z][A-Za-z\s&,\.]+?)(?:\.|,|;|and\s)",
            0.90,
            "supplied_by",
        ),
        (
            r"(?:key|major|principal|primary)\s+suppliers?\s+(?:include|are|is)[:\s]+([A-Z][A-Za-z\s&,\.]+?)(?:\.|,|;|and\s)",
            0.95,
            "key_supplier",
        ),
        (
            r"suppliers?\s+(?:such\s+as|like)\s+([A-Z][A-Za-z\s&,\.]+?)(?:\.|,|;|and\s)",
            0.90,
            "supplier_such_as",
        ),
        (
            r"suppliers?\s+(?:include|including)[:\s]+([A-Z][A-Za-z\s&,\.]+?)(?:\.|,|;|and\s)",
            0.90,
            "supplier_include",
        ),
        (r"we\s+(?:rely|depend)\s+on\s+([A-Z][A-Za-z\s&,\.]+?)\s+(?:for|to\s+)", 0.85, "rely_on"),
        (r"components?\s+from\s+([A-Z][A-Za-z\s&,\.]+?)(?:\.|,|;|and\s)", 0.85, "components_from"),
        # "depend on ... those operated by Google, Apple"
        (
            r"depend\s+on\s+(?:[\w\s,]+?\s+)?(?:operated\s+by|provided\s+by|from)\s+([A-Z][A-Za-z\s&,\.]+?)(?:\.|,|;|and\s)",
            0.80,
            "depend_on_operated",
        ),
        # "vendor partners such as X"
        (
            r"vendor\s+partners?\s+(?:such\s+as|like|including)[:\s]+([A-Z][A-Za-z\s&,\.]+?)(?:\.|,|;|and\s)",
            0.85,
            "vendor_partner",
        ),
        # "distributors such as X"
        (
            r"(?:wholesale\s+)?distributors?\s+(?:such\s+as|like|including)[:\s]+([A-Z][A-Za-z\s&,\.]+?)(?:\.|,|;|and\s)",
            0.85,
            "distributor",
        ),
        # "products from more than 1,000 vendor partners, including X"
        (
            r"from\s+(?:more\s+than\s+)?[\d,]+\s+(?:vendor\s+)?partners?,?\s+(?:including|such\s+as)\s+(?:[\w\s]+?\s+)?([A-Z][A-Za-z\s&,\.]+?)(?:\.|,|;|and\s)",
            0.85,
            "vendor_partner_list",
        ),
    ],
    RelationType.CUSTOMER: [
        # These are less common but exist: "X buys from us"
        (
            r"([A-Z][A-Za-z\s&,\.]+?)\s+(?:purchases?|buys?)\s+(?:[\w\s]+?\s+)?from\s+us",
            0.90,
            "customer_buys",
        ),
        (
            r"([A-Z][A-Za-z\s&,\.]+?)\s+is\s+(?:a|our)\s+(?:major|key|significant)?\s*customer",
            0.90,
            "is_customer",
        ),
    ],
}


class DirectionalExtractor:
    """
    Extracts relationships using directional patterns.

    Much higher precision than keyword-based extraction because
    patterns ensure the company is in the correct syntactic position.
    """

    def __init__(self):
        """Initialize compiled patterns."""
        self.outbound_patterns: dict[RelationType, list[tuple]] = {}
        self.inbound_patterns: dict[RelationType, list[tuple]] = {}

        for rel_type, patterns in OUTBOUND_PATTERNS.items():
            self.outbound_patterns[rel_type] = [
                (re.compile(p, re.IGNORECASE), conf, name) for p, conf, name in patterns
            ]

        for rel_type, patterns in INBOUND_PATTERNS.items():
            self.inbound_patterns[rel_type] = [
                (re.compile(p, re.IGNORECASE), conf, name) for p, conf, name in patterns
            ]

    def extract_from_text(
        self,
        text: str,
        relationship_types: list[RelationType] | None = None,
    ) -> list[DirectionalMatch]:
        """
        Extract relationships from text using directional patterns.

        Args:
            text: The text to extract from
            relationship_types: Types to extract (default: all)

        Returns:
            List of DirectionalMatch objects
        """
        if relationship_types is None:
            relationship_types = list(RelationType)

        matches = []

        # Split into sentences for context
        sentences = re.split(r"(?<=[.!?])\s+", text)

        for sentence in sentences:
            # Check outbound patterns
            for rel_type in relationship_types:
                if rel_type in self.outbound_patterns:
                    for pattern, conf, name in self.outbound_patterns[rel_type]:
                        for match in pattern.finditer(sentence):
                            company = match.group(1).strip()
                            if company and len(company) > 2:
                                matches.append(
                                    DirectionalMatch(
                                        company_text=self._clean_company_name(company),
                                        relationship_type=rel_type,
                                        confidence=conf,
                                        pattern_name=name,
                                        full_match=match.group(0),
                                        context=sentence[:300],
                                    )
                                )

                # Check inbound patterns
                if rel_type in self.inbound_patterns:
                    for pattern, conf, name in self.inbound_patterns[rel_type]:
                        for match in pattern.finditer(sentence):
                            company = match.group(1).strip()
                            if company and len(company) > 2:
                                matches.append(
                                    DirectionalMatch(
                                        company_text=self._clean_company_name(company),
                                        relationship_type=rel_type,
                                        confidence=conf,
                                        pattern_name=name,
                                        full_match=match.group(0),
                                        context=sentence[:300],
                                    )
                                )

        return matches

    def _clean_company_name(self, name: str) -> str:
        """Clean extracted company name."""
        # Remove trailing punctuation and common suffixes
        name = re.sub(r"[,;\.]+$", "", name)
        name = re.sub(r"\s+(Inc|Corp|LLC|Ltd|Co)\.*$", "", name, flags=re.IGNORECASE)
        return name.strip()


def extract_directional_relationships(
    text: str,
    relationship_type: RelationType | str | None = None,
) -> list[DirectionalMatch]:
    """
    Convenience function to extract relationships.

    Args:
        text: Text to extract from
        relationship_type: Type to extract, or None for all

    Returns:
        List of matches
    """
    extractor = DirectionalExtractor()

    if relationship_type is None:
        types = None
    elif isinstance(relationship_type, str):
        type_map = {
            "HAS_SUPPLIER": RelationType.SUPPLIER,
            "HAS_CUSTOMER": RelationType.CUSTOMER,
            "HAS_COMPETITOR": RelationType.COMPETITOR,
            "HAS_PARTNER": RelationType.PARTNER,
            "supplier": RelationType.SUPPLIER,
            "customer": RelationType.CUSTOMER,
            "competitor": RelationType.COMPETITOR,
            "partner": RelationType.PARTNER,
        }
        types = [type_map.get(relationship_type, RelationType.COMPETITOR)]
    else:
        types = [relationship_type]

    return extractor.extract_from_text(text, types)
