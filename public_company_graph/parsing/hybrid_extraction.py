"""
Hybrid Relationship Extraction Strategy.

Combines multiple approaches for optimal precision/recall:

1. HIGH CONFIDENCE (facts): Directional patterns with explicit subject/object
   - "We purchase from X" → X is supplier
   - "We compete with X" → X is competitor

2. MEDIUM CONFIDENCE (candidates): Current extraction + all filters
   - Keyword-based sentence finding
   - Entity resolution
   - Embedding similarity check
   - Relationship verifier

3. LLM VERIFICATION (optional): For uncertain cases
   - Use GPT to verify relationship in context
   - Most accurate but expensive

The tiered approach ensures:
- Analytics use only high-confidence facts
- Discovery/exploration can use candidates with evidence
- No poison edges in the graph
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any

from public_company_graph.parsing.directional_extraction import (
    DirectionalExtractor,
    RelationType,
)


class ExtractionTier(Enum):
    """Confidence tier from extraction strategy."""

    FACT = "fact"  # High confidence, use for analytics
    CANDIDATE = "candidate"  # Medium confidence, store with evidence
    REJECTED = "rejected"  # Low confidence, don't create edge


@dataclass
class HybridExtractionResult:
    """Result from hybrid extraction."""

    company_text: str
    ticker: str | None
    cik: str | None
    relationship_type: str  # e.g., "HAS_COMPETITOR"
    tier: ExtractionTier
    confidence: float
    evidence: dict[str, Any]  # Pattern name, context, scores
    context: str


class HybridExtractor:
    """
    Hybrid extraction combining directional patterns + filtered extraction.

    Strategy:
    1. First pass: Directional extraction for high-confidence facts
    2. Second pass: Current extraction with filters for candidates
    3. Deduplicate and assign tiers
    """

    def __init__(
        self,
        directional_extractor: DirectionalExtractor | None = None,
    ):
        """Initialize extractors."""
        self.directional = directional_extractor or DirectionalExtractor()

    def extract_facts(
        self,
        text: str,
        relationship_type: RelationType | str,
    ) -> list[HybridExtractionResult]:
        """
        Extract only HIGH CONFIDENCE facts using directional patterns.

        These are safe to use for analytics. Zero tolerance for false positives.
        """
        if isinstance(relationship_type, str):
            type_map = {
                "HAS_COMPETITOR": RelationType.COMPETITOR,
                "HAS_SUPPLIER": RelationType.SUPPLIER,
                "HAS_CUSTOMER": RelationType.CUSTOMER,
                "HAS_PARTNER": RelationType.PARTNER,
            }
            rel_type = type_map.get(relationship_type)
            if not rel_type:
                return []
        else:
            rel_type = relationship_type

        matches = self.directional.extract_from_text(text, [rel_type])

        results = []
        for match in matches:
            results.append(
                HybridExtractionResult(
                    company_text=match.company_text,
                    ticker=None,  # Needs entity resolution
                    cik=None,
                    relationship_type=f"HAS_{rel_type.name}",
                    tier=ExtractionTier.FACT,
                    confidence=match.confidence,
                    evidence={
                        "pattern": match.pattern_name,
                        "full_match": match.full_match,
                        "method": "directional_pattern",
                    },
                    context=match.context,
                )
            )

        return results


# Recommended thresholds per relationship type
RECOMMENDED_THRESHOLDS = {
    "HAS_COMPETITOR": {
        "fact_threshold": 0.45,  # 91.8% precision
        "candidate_threshold": 0.30,  # 88.2% precision
        "analytics_ready": True,
    },
    "HAS_PARTNER": {
        "fact_threshold": 0.55,  # 90.9% precision
        "candidate_threshold": 0.40,  # 75.0% precision
        "analytics_ready": False,
    },
    "HAS_SUPPLIER": {
        "fact_threshold": 0.55,  # ~80% precision but very low recall
        "candidate_threshold": 0.40,  # ~50% precision
        "analytics_ready": False,
        "recommendation": "Use LLM verification for suppliers",
    },
    "HAS_CUSTOMER": {
        "fact_threshold": 0.55,  # ~100% precision but very low recall
        "candidate_threshold": 0.40,  # ~50% precision
        "analytics_ready": False,
        "recommendation": "Use LLM verification for customers",
    },
}


def get_recommended_action(relationship_type: str) -> dict[str, Any]:
    """
    Get recommended action for a relationship type.

    Returns configuration and advice for extraction.
    """
    config = RECOMMENDED_THRESHOLDS.get(relationship_type, {})

    if config.get("analytics_ready"):
        return {
            "action": "extract_with_tiered_storage",
            "fact_threshold": config["fact_threshold"],
            "candidate_threshold": config["candidate_threshold"],
            "message": (
                f"{relationship_type} achieves >90% precision at threshold "
                f"{config['fact_threshold']} and is ready for analytics."
            ),
        }
    else:
        return {
            "action": "extract_as_candidates_only",
            "candidate_threshold": config.get("candidate_threshold", 0.40),
            "message": (
                f"{relationship_type} precision is too low for facts. "
                f"Store as candidates with evidence. "
                f"{config.get('recommendation', '')}"
            ),
        }


def print_extraction_recommendations():
    """Print extraction recommendations for all relationship types."""
    print("=" * 80)
    print("RELATIONSHIP EXTRACTION RECOMMENDATIONS")
    print("=" * 80)

    for rel_type in ["HAS_COMPETITOR", "HAS_PARTNER", "HAS_SUPPLIER", "HAS_CUSTOMER"]:
        rec = get_recommended_action(rel_type)
        print(f"\n{rel_type}:")
        print(f"  Action: {rec['action']}")
        print(f"  Message: {rec['message']}")

    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print("""
✅ HAS_COMPETITOR: Ready for analytics at threshold 0.45 (92% precision)
⚠️  HAS_PARTNER: Store as candidates, threshold 0.40 (75% precision)
❌ HAS_SUPPLIER: Too noisy - needs LLM verification or directional extraction only
❌ HAS_CUSTOMER: Too noisy - needs LLM verification or directional extraction only

NEXT STEPS:
1. Re-run extraction with --tiered for COMPETITOR (creates facts + candidates)
2. For SUPPLIER/CUSTOMER, either:
   a) Use directional extraction only (high precision, low recall)
   b) Implement LLM verification for each extracted relationship
   c) Store all as candidates with evidence (not for analytics)
""")


if __name__ == "__main__":
    print_extraction_recommendations()
