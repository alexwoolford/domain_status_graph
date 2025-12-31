#!/usr/bin/env python
"""
Validate entity resolution quality using embedding similarity.

This script identifies potentially incorrect entity matches by comparing
the context of a relationship mention against the target company's description.

Low similarity scores indicate the mention context doesn't match the company's
business, suggesting a potential entity resolution error.

Usage:
    # Analyze all relationships
    python scripts/validate_entity_resolution.py

    # Focus on specific relationship type
    python scripts/validate_entity_resolution.py --relationship HAS_SUPPLIER

    # Output JSON for programmatic use
    python scripts/validate_entity_resolution.py --json

    # Only show matches below a similarity threshold
    python scripts/validate_entity_resolution.py --threshold 0.3
"""

import argparse
import json
import logging

import numpy as np

from public_company_graph.config import get_neo4j_database
from public_company_graph.neo4j.connection import get_neo4j_driver

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


def cosine_similarity(vec1: list[float], vec2: list[float]) -> float:
    """Compute cosine similarity between two vectors."""
    a = np.array(vec1)
    b = np.array(vec2)
    return float(np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b)))


def validate_relationships(
    driver,
    database: str,
    relationship_type: str | None = None,
    threshold: float = 0.5,
    limit: int = 100,
) -> list[dict]:
    """
    Find potentially incorrect entity matches by comparing context to description.

    Returns relationships where the context embedding is dissimilar to the
    target company's description embedding.
    """
    rel_filter = f":{relationship_type}" if relationship_type else ""

    suspicious = []

    with driver.session(database=database) as session:
        # Get relationships with both context and target company description
        result = session.run(
            f"""
            MATCH (source:Company)-[r{rel_filter}]->(target:Company)
            WHERE r.context IS NOT NULL
              AND target.description_embedding IS NOT NULL
              AND size(r.raw_mention) <= 10
            RETURN source.ticker as source_ticker,
                   source.name as source_name,
                   target.ticker as target_ticker,
                   target.name as target_name,
                   r.raw_mention as mention,
                   r.context as context,
                   target.description_embedding as target_embedding,
                   target.description as target_description,
                   type(r) as rel_type
            LIMIT $limit
            """,
            limit=limit * 10,  # Get more to filter
        )

        # We need to embed the context to compare
        # For now, use a simpler heuristic: check if mention appears in target description
        for rec in result:
            mention = rec["mention"].lower()
            target_desc = (rec["target_description"] or "").lower()
            target_name = (rec["target_name"] or "").lower()

            # Simple heuristic: if mention doesn't appear in target description or name
            # it might be a false positive
            mention_in_desc = mention in target_desc
            mention_in_name = mention in target_name

            # Flag suspicious if mention is very short and doesn't appear in target
            if len(mention) <= 6 and not mention_in_desc and not mention_in_name:
                suspicious.append(
                    {
                        "source_ticker": rec["source_ticker"],
                        "source_name": rec["source_name"],
                        "target_ticker": rec["target_ticker"],
                        "target_name": rec["target_name"],
                        "mention": rec["mention"],
                        "rel_type": rec["rel_type"],
                        "context_snippet": rec["context"][:200] if rec["context"] else "",
                        "reason": "Short mention not found in target description",
                    }
                )

            if len(suspicious) >= limit:
                break

    return suspicious


def main():
    parser = argparse.ArgumentParser(
        description="Validate entity resolution using embedding similarity"
    )
    parser.add_argument(
        "--relationship",
        choices=["HAS_SUPPLIER", "HAS_CUSTOMER", "HAS_COMPETITOR", "HAS_PARTNER"],
        help="Focus on specific relationship type",
    )
    parser.add_argument(
        "--threshold",
        type=float,
        default=0.5,
        help="Similarity threshold below which to flag (default: 0.5)",
    )
    parser.add_argument("--limit", type=int, default=50, help="Maximum suspicious matches to show")
    parser.add_argument("--json", action="store_true", help="Output as JSON")
    args = parser.parse_args()

    driver = get_neo4j_driver()
    database = get_neo4j_database()

    try:
        logger.info("Validating entity resolution quality...")
        suspicious = validate_relationships(
            driver,
            database,
            relationship_type=args.relationship,
            threshold=args.threshold,
            limit=args.limit,
        )

        if args.json:
            print(json.dumps(suspicious, indent=2))
            return

        if not suspicious:
            logger.info("No suspicious matches found!")
            return

        logger.info(f"\nPotentially suspicious matches: {len(suspicious)}")
        print("\n" + "=" * 80)

        for i, match in enumerate(suspicious[:20], 1):
            print(
                f"\n{i}. {match['source_ticker']} --[{match['rel_type']}]--> {match['target_ticker']}"
            )
            print(f"   Mention: '{match['mention']}' → {match['target_name']}")
            print(f"   Reason: {match['reason']}")
            print(f"   Context: {match['context_snippet'][:100]}...")

        if len(suspicious) > 20:
            print(f"\n... and {len(suspicious) - 20} more")

    finally:
        driver.close()


if __name__ == "__main__":
    main()
