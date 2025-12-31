#!/usr/bin/env python
"""
Remove known false positive relationships from the graph.

These are relationships created by entity resolution errors where
common words were incorrectly matched to company tickers/names:

- "Joint" → JYNT (Joint Corp) - from "joint venture" contexts
- "Cost" → COST (Costco) - from "cost of..." financial contexts
- "CRM" → CRM (Salesforce) - from "CRM" as software category
- "Regis" → RGS (Regis Corp) - from pharmaceutical supplier named "Regis"

Usage:
    # Dry run (shows what would be deleted)
    python scripts/cleanup_false_positives.py

    # Execute deletion
    python scripts/cleanup_false_positives.py --execute
"""

import argparse
import logging

from public_company_graph.config import get_neo4j_database
from public_company_graph.neo4j.connection import get_neo4j_driver

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

# False positive patterns: (raw_mention pattern, target ticker, reason)
# These are always false positives - delete unconditionally
FALSE_POSITIVE_PATTERNS = [
    ("Joint", "JYNT", "joint venture context"),
    ("JOINT", "JYNT", "joint venture context"),
    ("Cost", "COST", "cost of... financial context"),
    ("COST", "COST", "cost of... financial context"),
    ("CRM", "CRM", "CRM as software category"),
    ("Regis", "RGS", "pharmaceutical supplier, not hair salons"),
    ("REGIS", "RGS", "pharmaceutical supplier, not hair salons"),
]

# Context-sensitive patterns: only delete if context matches pattern
# (mention, ticker, context_contains, relationship_type, reason)
CONTEXT_SENSITIVE_PATTERNS = [
    # "Target" as "target business" (acquisition target), not Target Corp retail
    ("Target", "TGT", "target business", "HAS_SUPPLIER", "acquisition target context"),
    ("Target", "TGT", "target compan", "HAS_SUPPLIER", "acquisition target context"),
    # "Nasdaq" as exchange listing, not Nasdaq Inc company
    ("Nasdaq", "NDAQ", "listed on nasdaq", "HAS_SUPPLIER", "exchange listing context"),
    ("Nasdaq", "NDAQ", "nasdaq stock", "HAS_SUPPLIER", "exchange listing context"),
    ("Nasdaq", "NDAQ", "nasdaq global", "HAS_SUPPLIER", "exchange listing context"),
    ("Nasdaq", "NDAQ", "nasdaq listing", "HAS_SUPPLIER", "exchange listing context"),
    ("Nasdaq", "NDAQ", "nasdaq rules", "HAS_SUPPLIER", "exchange listing context"),
    ("NASDAQ", "NDAQ", "listed on nasdaq", "HAS_SUPPLIER", "exchange listing context"),
]


def analyze_false_positives(driver, database: str) -> dict[str, int]:
    """Count false positives by pattern."""
    counts = {}

    with driver.session(database=database) as session:
        # Unconditional patterns
        for mention, ticker, _reason in FALSE_POSITIVE_PATTERNS:
            result = session.run(
                """
                MATCH (c:Company)-[r:HAS_SUPPLIER|HAS_CUSTOMER|HAS_COMPETITOR|HAS_PARTNER]->(s:Company {ticker: $ticker})
                WHERE r.raw_mention = $mention
                RETURN count(r) as count
                """,
                ticker=ticker,
                mention=mention,
            )
            count = result.single()["count"]
            if count > 0:
                key = f"{mention} → {ticker}"
                counts[key] = count

        # Context-sensitive patterns
        for mention, ticker, context_pattern, rel_type, _reason in CONTEXT_SENSITIVE_PATTERNS:
            result = session.run(
                f"""
                MATCH (c:Company)-[r:{rel_type}]->(s:Company {{ticker: $ticker}})
                WHERE r.raw_mention = $mention
                  AND toLower(r.context) CONTAINS $context_pattern
                RETURN count(r) as count
                """,
                ticker=ticker,
                mention=mention,
                context_pattern=context_pattern.lower(),
            )
            count = result.single()["count"]
            if count > 0:
                key = f"{mention} → {ticker} (context: {context_pattern})"
                counts[key] = counts.get(key, 0) + count

    return counts


def delete_false_positives(driver, database: str) -> int:
    """Delete all false positive relationships."""
    total_deleted = 0

    with driver.session(database=database) as session:
        # Unconditional patterns
        for mention, ticker, _reason in FALSE_POSITIVE_PATTERNS:
            result = session.run(
                """
                MATCH (c:Company)-[r:HAS_SUPPLIER|HAS_CUSTOMER|HAS_COMPETITOR|HAS_PARTNER]->(s:Company {ticker: $ticker})
                WHERE r.raw_mention = $mention
                DELETE r
                RETURN count(r) as deleted
                """,
                ticker=ticker,
                mention=mention,
            )
            deleted = result.single()["deleted"]
            if deleted > 0:
                logger.info(f"  Deleted {deleted} '{mention}' → {ticker}")
                total_deleted += deleted

        # Context-sensitive patterns
        for mention, ticker, context_pattern, rel_type, _reason in CONTEXT_SENSITIVE_PATTERNS:
            result = session.run(
                f"""
                MATCH (c:Company)-[r:{rel_type}]->(s:Company {{ticker: $ticker}})
                WHERE r.raw_mention = $mention
                  AND toLower(r.context) CONTAINS $context_pattern
                DELETE r
                RETURN count(r) as deleted
                """,
                ticker=ticker,
                mention=mention,
                context_pattern=context_pattern.lower(),
            )
            deleted = result.single()["deleted"]
            if deleted > 0:
                logger.info(
                    f"  Deleted {deleted} '{mention}' → {ticker} (context: {context_pattern})"
                )
                total_deleted += deleted

    return total_deleted


def main():
    parser = argparse.ArgumentParser(description="Remove false positive relationships")
    parser.add_argument(
        "--execute", action="store_true", help="Execute deletion (default: dry run)"
    )
    args = parser.parse_args()

    driver = get_neo4j_driver()
    database = get_neo4j_database()

    try:
        # Analyze
        logger.info("Analyzing false positive relationships...")
        counts = analyze_false_positives(driver, database)

        if not counts:
            logger.info("No false positives found!")
            return

        total = sum(counts.values())
        logger.info(f"\nFalse positives found: {total}")
        for pattern, count in sorted(counts.items(), key=lambda x: -x[1]):
            logger.info(f"  {pattern}: {count}")

        if not args.execute:
            logger.info("\nDRY RUN - Pass --execute to delete these relationships")
            return

        # Delete
        logger.info("\nDeleting false positives...")
        deleted = delete_false_positives(driver, database)
        logger.info(f"\n✓ Deleted {deleted} false positive relationships")

    finally:
        driver.close()


if __name__ == "__main__":
    main()
