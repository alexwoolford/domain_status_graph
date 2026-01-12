#!/usr/bin/env python3
"""
Audit and clean low-quality edges in the graph.

This script:
1. Identifies edges that don't meet quality thresholds
2. Reports statistics by relationship type
3. Optionally converts low-quality edges to CANDIDATE_* or deletes them

Usage:
    # Dry run - just report statistics
    python scripts/audit_and_clean_edges.py

    # Convert low-quality edges to candidates
    python scripts/audit_and_clean_edges.py --convert-to-candidates

    # Delete low-quality edges (more aggressive)
    python scripts/audit_and_clean_edges.py --delete-low-quality

    # Focus on specific relationship types
    python scripts/audit_and_clean_edges.py --relationship-types HAS_SUPPLIER HAS_CUSTOMER
"""

import argparse
import logging
from collections import defaultdict

from public_company_graph.cli import get_driver_and_database
from public_company_graph.parsing.relationship_config import (
    RELATIONSHIP_CONFIGS,
    ConfidenceTier,
    get_confidence_tier,
)

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


def audit_edges(
    driver,
    database: str,
    relationship_types: list[str] | None = None,
) -> dict:
    """
    Audit edge quality and return statistics.

    Returns:
        Dict with statistics by relationship type
    """
    if relationship_types is None:
        relationship_types = ["HAS_COMPETITOR", "HAS_PARTNER", "HAS_SUPPLIER", "HAS_CUSTOMER"]

    stats = defaultdict(
        lambda: {
            "total": 0,
            "high_confidence": 0,
            "medium_confidence": 0,
            "low_confidence": 0,
            "no_embedding": 0,
            "below_threshold": 0,
        }
    )

    with driver.session(database=database) as session:
        for rel_type in relationship_types:
            config = RELATIONSHIP_CONFIGS.get(rel_type)
            if not config:
                logger.warning(f"No config for {rel_type}, skipping")
                continue

            # Query all edges of this type
            query = f"""
            MATCH (source:Company)-[r:{rel_type}]->(target:Company)
            RETURN
                source.ticker AS source_ticker,
                target.ticker AS target_ticker,
                r.embedding_similarity AS embedding_similarity,
                r.confidence AS confidence,
                elementId(r) AS edge_id
            """

            result = session.run(query)
            for record in result:
                stats[rel_type]["total"] += 1
                embedding_sim = record["embedding_similarity"]

                if embedding_sim is None:
                    stats[rel_type]["no_embedding"] += 1
                    # Without embedding, we can't determine tier accurately
                    # But we know from analysis that these are risky
                    tier = get_confidence_tier(rel_type, None)
                    if tier == ConfidenceTier.HIGH:
                        stats[rel_type]["high_confidence"] += 1
                    elif tier == ConfidenceTier.MEDIUM:
                        stats[rel_type]["medium_confidence"] += 1
                    else:
                        stats[rel_type]["low_confidence"] += 1
                else:
                    tier = get_confidence_tier(rel_type, embedding_sim)
                    if tier == ConfidenceTier.HIGH:
                        stats[rel_type]["high_confidence"] += 1
                    elif tier == ConfidenceTier.MEDIUM:
                        stats[rel_type]["medium_confidence"] += 1
                    else:
                        stats[rel_type]["low_confidence"] += 1

                    # Check if below high threshold (should be candidate, not fact)
                    if embedding_sim < config.high_threshold:
                        stats[rel_type]["below_threshold"] += 1

    return dict(stats)


def convert_to_candidates(
    driver,
    database: str,
    relationship_types: list[str] | None = None,
    dry_run: bool = True,
) -> dict:
    """
    Convert low-quality fact edges to candidate edges.

    Args:
        driver: Neo4j driver
        database: Database name
        relationship_types: Types to process (None = all)
        dry_run: If True, only report what would be done

    Returns:
        Dict with counts of conversions
    """
    if relationship_types is None:
        relationship_types = ["HAS_COMPETITOR", "HAS_PARTNER", "HAS_SUPPLIER", "HAS_CUSTOMER"]

    conversions = defaultdict(int)

    with driver.session(database=database) as session:
        for rel_type in relationship_types:
            config = RELATIONSHIP_CONFIGS.get(rel_type)
            if not config:
                continue

            # Find edges that should be candidates (below high threshold)
            query = f"""
            MATCH (source:Company)-[r:{rel_type}]->(target:Company)
            WHERE r.embedding_similarity IS NOT NULL
              AND r.embedding_similarity < $high_threshold
              AND r.embedding_similarity >= $medium_threshold
            RETURN
                source.ticker AS source_ticker,
                target.ticker AS target_ticker,
                r.embedding_similarity AS embedding_similarity,
                r.raw_mention AS raw_mention,
                r.context AS context,
                r.confidence AS confidence,
                elementId(r) AS edge_id
            """

            result = session.run(
                query,
                high_threshold=config.high_threshold,
                medium_threshold=config.medium_threshold,
            )

            edges_to_convert = list(result)

            if dry_run:
                logger.info(
                    f"DRY RUN: Would convert {len(edges_to_convert)} {rel_type} edges to {config.candidate_type}"
                )
                conversions[rel_type] = len(edges_to_convert)
            else:
                # Convert in batches
                batch_size = 100
                for i in range(0, len(edges_to_convert), batch_size):
                    batch = edges_to_convert[i : i + batch_size]

                    # Create candidate edges
                    create_query = f"""
                    UNWIND $batch AS edge
                    MATCH (source:Company {{ticker: edge.source_ticker}})
                    MATCH (target:Company {{ticker: edge.target_ticker}})
                    MERGE (source)-[r:{config.candidate_type}]->(target)
                    SET r.embedding_similarity = edge.embedding_similarity,
                        r.raw_mention = edge.raw_mention,
                        r.context = edge.context,
                        r.confidence = edge.confidence,
                        r.converted_from = '{rel_type}',
                        r.converted_at = datetime()
                    """

                    session.run(create_query, batch=[dict(rec) for rec in batch])

                    # Delete old fact edges
                    delete_query = f"""
                    UNWIND $batch AS edge
                    MATCH (source:Company {{ticker: edge.source_ticker}})
                      -[r:{rel_type}]->(target:Company {{ticker: edge.target_ticker}})
                    WHERE id(r) = edge.edge_id
                    DELETE r
                    """

                    session.run(delete_query, batch=[dict(rec) for rec in batch])

                conversions[rel_type] = len(edges_to_convert)
                logger.info(
                    f"✓ Converted {len(edges_to_convert)} {rel_type} edges to {config.candidate_type}"
                )

    return dict(conversions)


def delete_low_quality(
    driver,
    database: str,
    relationship_types: list[str] | None = None,
    dry_run: bool = True,
) -> dict:
    """
    Delete edges that are below medium threshold (low confidence).

    Args:
        driver: Neo4j driver
        database: Database name
        relationship_types: Types to process (None = all)
        dry_run: If True, only report what would be done

    Returns:
        Dict with counts of deletions
    """
    if relationship_types is None:
        relationship_types = ["HAS_COMPETITOR", "HAS_PARTNER", "HAS_SUPPLIER", "HAS_CUSTOMER"]

    deletions = defaultdict(int)

    with driver.session(database=database) as session:
        for rel_type in relationship_types:
            config = RELATIONSHIP_CONFIGS.get(rel_type)
            if not config:
                continue

            # Find edges below medium threshold
            query = f"""
            MATCH (source:Company)-[r:{rel_type}]->(target:Company)
            WHERE (r.embedding_similarity IS NOT NULL
                   AND r.embedding_similarity < $medium_threshold)
               OR (r.embedding_similarity IS NULL AND r.confidence IS NOT NULL
                   AND r.confidence < 0.5)
            RETURN
                source.ticker AS source_ticker,
                target.ticker AS target_ticker,
                r.embedding_similarity AS embedding_similarity,
                r.confidence AS confidence,
                elementId(r) AS edge_id
            """

            result = session.run(query, medium_threshold=config.medium_threshold)
            edges_to_delete = list(result)

            if dry_run:
                logger.info(
                    f"DRY RUN: Would delete {len(edges_to_delete)} low-quality {rel_type} edges"
                )
                deletions[rel_type] = len(edges_to_delete)
            else:
                # Delete in batches
                batch_size = 100
                for i in range(0, len(edges_to_delete), batch_size):
                    batch = edges_to_delete[i : i + batch_size]

                    delete_query = f"""
                    UNWIND $batch AS edge
                    MATCH ()-[r:{rel_type}]->()
                    WHERE id(r) = edge.edge_id
                    DELETE r
                    """

                    session.run(delete_query, batch=[dict(rec) for rec in batch])

                deletions[rel_type] = len(edges_to_delete)
                logger.info(f"✓ Deleted {len(edges_to_delete)} low-quality {rel_type} edges")

    return dict(deletions)


def print_statistics(stats: dict):
    """Print audit statistics in a readable format."""
    print("\n" + "=" * 80)
    print("EDGE QUALITY AUDIT RESULTS")
    print("=" * 80)

    for rel_type, data in sorted(stats.items()):
        config = RELATIONSHIP_CONFIGS.get(rel_type)
        print(f"\n{rel_type}:")
        print(f"  Total edges: {data['total']:,}")

        if data["total"] == 0:
            continue

        high_pct = (data["high_confidence"] / data["total"]) * 100
        medium_pct = (data["medium_confidence"] / data["total"]) * 100
        low_pct = (data["low_confidence"] / data["total"]) * 100
        no_embedding_pct = (data["no_embedding"] / data["total"]) * 100
        below_threshold_pct = (data["below_threshold"] / data["total"]) * 100

        print(f"  High confidence: {data['high_confidence']:,} ({high_pct:.1f}%)")
        print(f"  Medium confidence: {data['medium_confidence']:,} ({medium_pct:.1f}%)")
        print(f"  Low confidence: {data['low_confidence']:,} ({low_pct:.1f}%)")
        print(f"  No embedding data: {data['no_embedding']:,} ({no_embedding_pct:.1f}%)")
        print(f"  Below high threshold: {data['below_threshold']:,} ({below_threshold_pct:.1f}%)")

        if config:
            print(f"  Thresholds: high={config.high_threshold}, medium={config.medium_threshold}")
            print(f"  Analytics ready: {config.analytics_ready}")

        # Calculate estimated precision
        if rel_type == "HAS_COMPETITOR":
            estimated_precision = high_pct * 0.90 + medium_pct * 0.85
        elif rel_type == "HAS_PARTNER":
            estimated_precision = high_pct * 0.79 + medium_pct * 0.66
        elif rel_type in ["HAS_SUPPLIER", "HAS_CUSTOMER"]:
            estimated_precision = high_pct * 0.80 + medium_pct * 0.40
        else:
            estimated_precision = None

        if estimated_precision:
            print(f"  Estimated precision: {estimated_precision:.1f}%")

    print("\n" + "=" * 80)


def main():
    parser = argparse.ArgumentParser(description="Audit and clean low-quality edges in the graph")
    parser.add_argument(
        "--relationship-types",
        nargs="+",
        choices=["HAS_COMPETITOR", "HAS_PARTNER", "HAS_SUPPLIER", "HAS_CUSTOMER"],
        help="Specific relationship types to audit (default: all)",
    )
    parser.add_argument(
        "--convert-to-candidates",
        action="store_true",
        help="Convert low-quality fact edges to candidate edges",
    )
    parser.add_argument(
        "--delete-low-quality",
        action="store_true",
        help="Delete edges below medium threshold (aggressive)",
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Actually perform conversions/deletions (default is dry-run)",
    )
    args = parser.parse_args()

    driver, database = get_driver_and_database()

    try:
        # Always run audit first
        logger.info("Auditing edge quality...")
        stats = audit_edges(driver, database, args.relationship_types)
        print_statistics(stats)

        # Convert to candidates if requested
        if args.convert_to_candidates:
            logger.info("\nConverting low-quality edges to candidates...")
            conversions = convert_to_candidates(
                driver, database, args.relationship_types, dry_run=not args.execute
            )
            if conversions:
                print("\nConversions:")
                for rel_type, count in conversions.items():
                    print(f"  {rel_type}: {count:,}")

        # Delete low quality if requested
        if args.delete_low_quality:
            logger.info("\nDeleting low-quality edges...")
            deletions = delete_low_quality(
                driver, database, args.relationship_types, dry_run=not args.execute
            )
            if deletions:
                print("\nDeletions:")
                for rel_type, count in deletions.items():
                    print(f"  {rel_type}: {count:,}")

        if not args.execute and (args.convert_to_candidates or args.delete_low_quality):
            print("\n⚠️  DRY RUN MODE - No changes made. Use --execute to apply changes.")

    finally:
        driver.close()


if __name__ == "__main__":
    main()
