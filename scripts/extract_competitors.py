#!/usr/bin/env python3
"""
Extract competitor relationships from 10-K filings and load into Neo4j.

This script:
1. Builds a lookup table from Neo4j Company nodes for entity resolution
2. Iterates through cached 10-K extracted data (business descriptions + risk factors)
3. Extracts competitor mentions using pattern matching
4. Resolves mentions to Company nodes using fuzzy matching
5. Creates HAS_COMPETITOR relationships in Neo4j

Relationship Schema:
    (Company)-[:HAS_COMPETITOR {
        confidence: float,      # Entity resolution confidence (0-1)
        raw_mention: string,    # How competitor was mentioned in 10-K
        source: string,         # "10k_filing"
        extracted_at: datetime  # When extraction was performed
    }]->(Company)

Note: HAS_COMPETITOR is directional - it means "Company X cited Company Y as a competitor
in their 10-K filing". The reverse relationship may not exist (Y may not cite X).

Usage:
    python scripts/extract_competitors.py                    # Dry-run (plan only)
    python scripts/extract_competitors.py --execute          # Actually extract and load
    python scripts/extract_competitors.py --execute --limit 100  # Test with 100 companies
"""

import argparse
import logging
import sys
from collections import defaultdict
from typing import Any

from public_company_graph.cache import get_cache
from public_company_graph.cli import (
    add_execute_argument,
    get_driver_and_database,
    setup_logging,
    verify_neo4j_connection,
)
from public_company_graph.constants import BATCH_SIZE_LARGE
from public_company_graph.neo4j import clean_properties_batch
from public_company_graph.parsing.competitor_extraction import (
    CompetitorLookup,
    build_competitor_lookup,
    extract_and_resolve_competitors_simple,
)

logger = logging.getLogger(__name__)

# Cache namespace for 10-K extracted data
CACHE_NAMESPACE = "10k_extracted"


def create_competitor_constraints(
    driver, database: str | None = None, logger_instance: logging.Logger | None = None
) -> None:
    """
    Create constraints and indexes for HAS_COMPETITOR relationships.

    Note: Neo4j doesn't support constraints on relationships, but we ensure
    the Company nodes have constraints (done by create_company_constraints).
    """
    log = logger_instance or logger

    # The constraint on Company.cik should already exist
    # This is just for safety/documentation
    constraints = [
        # Index on relationship properties for faster queries
        "CREATE INDEX has_competitor_confidence IF NOT EXISTS FOR ()-[r:HAS_COMPETITOR]->() ON (r.confidence)",
    ]

    with driver.session(database=database) as session:
        for constraint in constraints:
            try:
                session.run(constraint)
                log.info(f"  ✓ Created: {constraint[:60]}...")
            except Exception as e:
                if "already exists" in str(e).lower() or "equivalent" in str(e).lower():
                    log.debug(f"  ✓ Already exists: {constraint[:60]}...")
                else:
                    log.warning(f"  ⚠ Failed: {constraint[:60]}... - {e}")


def extract_competitors_from_cache(
    cache,
    lookup: CompetitorLookup,
    limit: int | None = None,
) -> dict[str, list[dict[str, Any]]]:
    """
    Extract competitor relationships from cached 10-K data.

    Uses the simplified keyword+lookup method which:
    1. Finds sentences containing competitor keywords
    2. Extracts potential company names
    3. Resolves against known companies in the lookup table

    Args:
        cache: AppCache instance
        lookup: CompetitorLookup for entity resolution
        limit: Optional limit on companies to process

    Returns:
        Dict mapping source_cik -> list of competitor dicts
    """
    relationships: dict[str, list[dict[str, Any]]] = defaultdict(list)
    processed = 0
    with_competitors = 0

    # Get all keys from 10k_extracted namespace
    keys = cache.keys(namespace=CACHE_NAMESPACE, limit=limit or 100000)

    logger.info(f"Processing {len(keys)} companies from cache...")

    for cik in keys:
        data = cache.get(CACHE_NAMESPACE, cik)
        if not data:
            continue

        business_desc = data.get("business_description")
        risk_factors = data.get("risk_factors")

        if not business_desc and not risk_factors:
            continue

        # Extract and resolve competitors using simplified keyword+lookup method
        competitors = extract_and_resolve_competitors_simple(
            business_description=business_desc,
            risk_factors=risk_factors,
            lookup=lookup,
            self_cik=cik,
        )

        if competitors:
            relationships[cik] = competitors
            with_competitors += 1

        processed += 1
        if processed % 500 == 0:
            logger.info(
                f"  Processed {processed} companies, {with_competitors} with competitors..."
            )

        if limit and processed >= limit:
            break

    logger.info(
        f"Extraction complete: {processed} companies, {with_competitors} with competitor mentions"
    )

    return dict(relationships)


def load_competitor_relationships(
    driver,
    relationships: dict[str, list[dict[str, Any]]],
    database: str | None = None,
    batch_size: int = BATCH_SIZE_LARGE,
) -> int:
    """
    Load HAS_COMPETITOR relationships into Neo4j.

    Args:
        driver: Neo4j driver
        relationships: Dict mapping source_cik -> list of competitor dicts
        database: Neo4j database name
        batch_size: Batch size for UNWIND operations

    Returns:
        Number of relationships created
    """
    # Flatten relationships into a list of (source_cik, competitor_info) tuples
    flat_rels = []
    for source_cik, competitors in relationships.items():
        for comp in competitors:
            # Convert empty strings to None so they get cleaned out
            context_val = comp.get("context", "")[:500].strip()
            raw_mention_val = comp.get("raw_mention", "").strip()
            flat_rels.append(
                {
                    "source_cik": source_cik,
                    "target_cik": comp["competitor_cik"],
                    "confidence": comp["confidence"],
                    "raw_mention": raw_mention_val or None,
                    "context": context_val or None,
                }
            )

    if not flat_rels:
        logger.info("No competitor relationships to load")
        return 0

    # Clean empty strings and None values from relationship properties
    flat_rels = clean_properties_batch(flat_rels)

    logger.info(f"Loading {len(flat_rels)} HAS_COMPETITOR relationships...")

    total_created = 0

    # Load in batches
    with driver.session(database=database) as session:
        for i in range(0, len(flat_rels), batch_size):
            batch = flat_rels[i : i + batch_size]

            # Use SET r += rel to merge only non-empty properties
            query = """
            UNWIND $batch AS rel
            MATCH (source:Company {cik: rel.source_cik})
            MATCH (target:Company {cik: rel.target_cik})
            MERGE (source)-[r:HAS_COMPETITOR]->(target)
            SET r += rel,
                r.source = 'ten_k_filing',
                r.extracted_at = datetime()
            """

            result = session.run(query, batch=batch)
            summary = result.consume()
            created = summary.counters.relationships_created
            total_created += created

            logger.info(
                f"  Batch {i // batch_size + 1}: "
                f"processed {len(batch)} relationships, {created} new"
            )

    return total_created


def calculate_confidence_tiers(driver, database: str | None = None) -> dict[str, int]:
    """
    Calculate confidence tiers for HAS_COMPETITOR relationships based on graph structure.

    Tiers:
    - high: Mutual relationships (both companies cite each other)
    - medium: Target company cited by 3+ different companies
    - low: Target company cited by 1-2 companies only

    Args:
        driver: Neo4j driver
        database: Neo4j database name

    Returns:
        Dict with counts by tier
    """
    query = """
    MATCH (a:Company)-[r:HAS_COMPETITOR]->(b:Company)
    WITH a, b, r,
         EXISTS { (b)-[:HAS_COMPETITOR]->(a) } as is_mutual
    WITH a, b, r, is_mutual
    MATCH (x:Company)-[:HAS_COMPETITOR]->(b)
    WITH a, b, r, is_mutual, count(DISTINCT x) as inbound_count
    SET r.confidence_tier = CASE
      WHEN is_mutual THEN 'high'
      WHEN inbound_count >= 3 THEN 'medium'
      ELSE 'low'
    END,
    r.inbound_citations = inbound_count,
    r.is_mutual = is_mutual
    RETURN r.confidence_tier as tier, count(*) as count
    ORDER BY tier
    """

    with driver.session(database=database) as session:
        result = session.run(query)
        counts = {record["tier"]: record["count"] for record in result}

    return counts


def analyze_relationships(relationships: dict[str, list[dict[str, Any]]]) -> None:
    """Log analysis of extracted relationships."""
    total_rels = sum(len(v) for v in relationships.values())
    companies_with_competitors = len(relationships)

    # Count how often each company is cited as a competitor
    competitor_counts: dict[str, int] = defaultdict(int)
    for competitors in relationships.values():
        for comp in competitors:
            competitor_counts[comp["competitor_cik"]] += 1

    # Top cited competitors
    top_cited = sorted(competitor_counts.items(), key=lambda x: x[1], reverse=True)[:20]

    logger.info("")
    logger.info("=" * 80)
    logger.info("Extraction Analysis")
    logger.info("=" * 80)
    logger.info(f"Companies that cited competitors: {companies_with_competitors}")
    logger.info(f"Total competitor relationships: {total_rels}")
    logger.info(f"Unique companies cited as competitors: {len(competitor_counts)}")
    logger.info("")
    logger.info("Top 20 most-cited competitors:")
    for cik, count in top_cited:
        # Get sample company that cited this one
        sample_competitors = [
            c for comps in relationships.values() for c in comps if c["competitor_cik"] == cik
        ]
        if sample_competitors:
            name = sample_competitors[0].get("competitor_name", "Unknown")
            ticker = sample_competitors[0].get("competitor_ticker", "?")
            logger.info(f"  {ticker:6s} {name[:40]:40s} cited {count} times")


def dry_run_analysis(cache, driver, database: str | None = None, limit: int | None = 10) -> None:
    """
    Run analysis without loading data.

    Shows what would be extracted from a sample of companies.
    """
    logger.info("=" * 80)
    logger.info("DRY RUN - Competitor Extraction Preview")
    logger.info("=" * 80)
    logger.info("")

    # Build lookup
    logger.info("Building company lookup table...")
    lookup = build_competitor_lookup(driver, database=database)
    logger.info(f"  {len(lookup.name_to_company)} name variants")
    logger.info(f"  {len(lookup.ticker_to_company)} tickers")
    logger.info("")

    # Sample extraction
    logger.info(f"Extracting from sample of {limit} companies...")
    relationships = extract_competitors_from_cache(cache, lookup, limit=limit)

    logger.info("")
    logger.info("Sample extractions:")
    for source_cik, competitors in list(relationships.items())[:5]:
        source_data = cache.get(CACHE_NAMESPACE, source_cik)
        source_name = (
            source_data.get("filing_metadata", {}).get("company_name", source_cik)
            if source_data
            else source_cik
        )

        logger.info(f"  {source_cik} ({source_name[:30]}...):")
        for comp in competitors[:3]:
            logger.info(
                f"    → {comp['competitor_ticker']:6s} {comp['competitor_name'][:30]:30s} "
                f"(conf={comp['confidence']:.2f}, mentioned as '{comp['raw_mention']}')"
            )
        if len(competitors) > 3:
            logger.info(f"    ... and {len(competitors) - 3} more")

    logger.info("")
    logger.info("=" * 80)
    logger.info("To execute, run: python scripts/extract_competitors.py --execute")
    logger.info("=" * 80)


def main():
    """Run competitor extraction."""
    parser = argparse.ArgumentParser(
        description="Extract competitor relationships from 10-K filings"
    )
    add_execute_argument(parser)
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit number of companies to process (for testing)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=BATCH_SIZE_LARGE,
        help=f"Batch size for Neo4j writes (default: {BATCH_SIZE_LARGE})",
    )

    args = parser.parse_args()

    log = setup_logging("extract_competitors", execute=args.execute)

    # Get cache and Neo4j connection
    cache = get_cache()
    driver, database = get_driver_and_database(log)

    try:
        # Test Neo4j connection
        if not verify_neo4j_connection(driver, database, log):
            sys.exit(1)

        if not args.execute:
            # Dry run
            dry_run_analysis(cache, driver, database, limit=args.limit or 10)
            return

        # Full execution
        log.info("=" * 80)
        log.info("Extracting Competitor Relationships")
        log.info("=" * 80)
        log.info("")

        # Build lookup table
        log.info("1. Building company lookup table...")
        lookup = build_competitor_lookup(driver, database=database)

        # Extract relationships
        log.info("")
        log.info("2. Extracting competitors from 10-K data...")
        relationships = extract_competitors_from_cache(cache, lookup, limit=args.limit)

        # Analyze
        analyze_relationships(relationships)

        # Create constraints
        log.info("")
        log.info("3. Creating constraints/indexes...")
        create_competitor_constraints(driver, database=database, logger_instance=log)

        # Load relationships
        log.info("")
        log.info("4. Loading relationships into Neo4j...")
        created = load_competitor_relationships(
            driver,
            relationships,
            database=database,
            batch_size=args.batch_size,
        )

        # Calculate confidence tiers based on graph structure
        log.info("")
        log.info("5. Calculating confidence tiers...")
        tier_counts = calculate_confidence_tiers(driver, database=database)
        log.info(f"  High confidence (mutual):     {tier_counts.get('high', 0):,}")
        log.info(f"  Medium confidence (3+ cites): {tier_counts.get('medium', 0):,}")
        log.info(f"  Low confidence (1-2 cites):   {tier_counts.get('low', 0):,}")

        log.info("")
        log.info("=" * 80)
        log.info("✓ Complete!")
        log.info("=" * 80)
        log.info(f"Total relationships loaded: {created}")
        log.info("")
        log.info("Example queries:")
        log.info("  # Find NVDA's HIGH confidence competitors")
        log.info(
            "  MATCH (c:Company {ticker:'NVDA'})-[r:HAS_COMPETITOR {confidence_tier:'high'}]->(comp)"
        )
        log.info("  RETURN comp.ticker, comp.name")
        log.info("")
        log.info("  # Find all competitors (any confidence)")
        log.info("  MATCH (c:Company {ticker:'NVDA'})-[r:HAS_COMPETITOR]->(comp)")
        log.info("  RETURN comp.ticker, comp.name, r.confidence_tier")
        log.info("")
        log.info("  # Count by confidence tier")
        log.info("  MATCH ()-[r:HAS_COMPETITOR]->()")
        log.info("  RETURN r.confidence_tier, count(*) ORDER BY count(*) DESC")

    finally:
        driver.close()


if __name__ == "__main__":
    main()
