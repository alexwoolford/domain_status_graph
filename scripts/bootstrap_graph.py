#!/usr/bin/env python3
"""
Bootstrap the Neo4j graph from SQLite domain status data.

This script loads only the essential data needed for the two useful GDS features:
- Technology Adoption Prediction (Personalized PageRank)
- Technology Affinity Bundling (Node Similarity)

Schema:
- Nodes: Domain, Technology
- Relationships: USES (from bootstrap), LIKELY_TO_ADOPT (from GDS), CO_OCCURS_WITH (from GDS)

Usage:
    python scripts/bootstrap_graph.py          # Dry-run (plan only)
    python scripts/bootstrap_graph.py --execute  # Actually load data
"""

import argparse
import logging
import sys
from pathlib import Path

from domain_status_graph.cli import (
    add_execute_argument,
    get_driver_and_database,
    print_execute_header,
    setup_logging,
    verify_neo4j_connection,
)
from domain_status_graph.config import get_domain_status_db
from domain_status_graph.ingest import (
    get_domain_count,
    get_domain_metadata_counts,
    get_technology_count,
    get_uses_relationship_count,
    load_domains,
    load_technologies,
    read_domains,
    read_technologies,
)
from domain_status_graph.neo4j import create_bootstrap_constraints


def dry_run_plan(db_path: Path, logger: logging.Logger = None):
    """Print the ETL plan without executing."""
    if logger is None:
        logger = logging.getLogger(__name__)

    logger.info("=" * 70)
    logger.info("ETL PLAN (Dry Run)")
    logger.info("=" * 70)
    logger.info("")
    logger.info("This script loads only Domain and Technology nodes + USES relationships.")
    logger.info("This is all that's needed for the two useful GDS features:")
    logger.info("  1. Technology Adoption Prediction (Personalized PageRank)")
    logger.info("  2. Technology Affinity Bundling (Node Similarity)")
    logger.info("")

    # Get counts
    domain_count = get_domain_count(db_path)
    tech_count = get_technology_count(db_path)
    uses_count = get_uses_relationship_count(db_path)
    metadata_counts = get_domain_metadata_counts(db_path)

    logger.info("Data to be loaded:")
    logger.info("-" * 70)
    logger.info(f"  Domains: {domain_count:,}")
    title_pct = metadata_counts["with_title"] / metadata_counts["total"] * 100
    logger.info(f"    - With title: {metadata_counts['with_title']:,} ({title_pct:.1f}%)")
    keywords_pct = metadata_counts["with_keywords"] / metadata_counts["total"] * 100
    logger.info(f"    - With keywords: {metadata_counts['with_keywords']:,} ({keywords_pct:.1f}%)")
    desc_pct = metadata_counts["with_description"] / metadata_counts["total"] * 100
    logger.info(
        f"    - With description: {metadata_counts['with_description']:,} ({desc_pct:.1f}%)"
    )
    logger.info(f"  Technologies: {tech_count:,}")
    logger.info(f"  USES relationships: {uses_count:,}")

    logger.info("")
    logger.info("=" * 70)
    logger.info("To execute this plan, run: python scripts/bootstrap_graph.py --execute")
    logger.info("=" * 70)


def main():
    """Run the main ETL pipeline."""
    parser = argparse.ArgumentParser(description="Bootstrap Neo4j graph from SQLite domain data")
    add_execute_argument(parser)
    args = parser.parse_args()

    logger = setup_logging("bootstrap_graph", execute=args.execute)

    db_path = get_domain_status_db()

    if not db_path.exists():
        logger.error(f"Database not found at {db_path}")
        sys.exit(1)

    # Dry-run mode (default)
    if not args.execute:
        dry_run_plan(db_path, logger=logger)
        return

    # Execute mode
    print_execute_header("Domain Status Graph ETL Pipeline", logger)
    logger.info("")
    logger.info("Loading Domain and Technology nodes + USES relationships.")
    logger.info("Domain nodes include metadata: title, keywords, description.")
    logger.info("This enables domain-level text similarity for company comparison.")
    logger.info("")

    driver, database = get_driver_and_database(logger)

    logger.info(f"Using database: {database}")
    logger.info("")

    try:
        # Test connection
        if not verify_neo4j_connection(driver, database, logger):
            sys.exit(1)
        logger.info("")

        # Create constraints
        logger.info("Creating constraints and indexes...")
        create_bootstrap_constraints(driver, database=database)
        logger.info("")

        # Load data
        logger.info("Loading data from SQLite to Neo4j...")
        logger.info("-" * 70)

        # Read domains from SQLite
        domains = read_domains(db_path)
        logger.info(f"Loading {len(domains)} Domain nodes...")
        load_domains(driver, domains, database=database)
        logger.info(f"✓ Loaded {len(domains)} Domain nodes")
        logger.info("")

        # Read technologies from SQLite
        tech_mappings = read_technologies(db_path)
        logger.info(f"Loading {len(tech_mappings)} technologies and USES relationships...")
        load_technologies(driver, tech_mappings, database=database)
        logger.info("✓ Loaded USES relationships")
        logger.info("")

        # Summary
        logger.info("=" * 70)
        logger.info("ETL Complete!")
        logger.info("=" * 70)

        with driver.session(database=database) as session:
            result = session.run(
                "MATCH (n) RETURN labels(n)[0] AS label, count(*) AS count ORDER BY count DESC"
            )
            logger.info("")
            logger.info("Node counts:")
            for record in result:
                logger.info(f"  {record['label']:20s}: {record['count']:,}")

            result = session.run(
                "MATCH ()-[r]->() RETURN type(r) AS type, count(*) AS count ORDER BY count DESC"
            )
            logger.info("")
            logger.info("Relationship counts:")
            for record in result:
                logger.info(f"  {record['type']:20s}: {record['count']:,}")

    finally:
        driver.close()


if __name__ == "__main__":
    main()
