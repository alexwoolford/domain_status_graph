#!/usr/bin/env python3
"""
Compute Graph Data Science (GDS) features using Python GDS Client.

This script implements the GDS features using the modular functions in
public_company_graph.gds:
- Technology Adoption Prediction (Personalized PageRank)
- Technology Affinity and Bundling (Node Similarity)
- Company Description Similarity (Cosine similarity on embeddings)
- Company Technology Similarity (Jaccard on technology sets)

Usage:
    python scripts/compute_gds_features.py          # Dry-run (plan only)
    python scripts/compute_gds_features.py --execute  # Compute all features
"""

import argparse
import logging
import sys

from public_company_graph.cli import (
    add_execute_argument,
    get_driver_and_database,
    setup_logging,
    verify_neo4j_connection,
)
from public_company_graph.gds import (
    cleanup_leftover_graphs,
    compute_company_description_similarity,
    compute_company_technology_similarity,
    compute_tech_adoption_prediction,
    compute_tech_affinity_bundling,
    get_gds_client,
)


def print_dry_run_plan(logger: logging.Logger = None):
    """Print the GDS features plan without executing."""
    if logger is None:
        logger = logging.getLogger(__name__)

    logger.info("=" * 70)
    logger.info("GDS FEATURES PLAN (Dry Run)")
    logger.info("=" * 70)
    logger.info("")
    logger.info("This script will compute the following features:")
    logger.info("")
    logger.info("1. Technology Adopter Prediction (Technology → Domain)")
    logger.info("   - For each technology, predicts top 50 domains likely to adopt it")
    logger.info("   - Creates: Domain-[LIKELY_TO_ADOPT {score}]->Technology")
    logger.info("   - Use case: Software companies finding customers for their product")
    logger.info("")
    logger.info("2. Technology Affinity and Bundling (Node Similarity)")
    logger.info("   - Finds technology pairs that commonly co-occur")
    logger.info("   - Creates: Technology-[CO_OCCURS_WITH {similarity}]->Technology")
    logger.info("")
    logger.info("3. Company Description Similarity (Cosine Similarity)")
    logger.info("   - Finds companies with similar business descriptions")
    logger.info("   - Creates: Company-[SIMILAR_DESCRIPTION {score}]->Company")
    logger.info("   - Note: Requires Company nodes with description_embedding property")
    logger.info("")
    logger.info("4. Company Technology Similarity (Jaccard Similarity)")
    logger.info("   - Finds companies with similar technology stacks")
    logger.info("   - Creates: Company-[SIMILAR_TECHNOLOGY {score}]->Company")
    logger.info("   - Algorithm: Jaccard similarity on aggregated technology sets")
    logger.info("")
    logger.info("=" * 70)
    logger.info("To execute, run: python scripts/compute_gds_features.py --execute")
    logger.info("=" * 70)


def main():
    """Run main GDS computation pipeline."""
    parser = argparse.ArgumentParser(description="Compute GDS features using Python GDS client")
    add_execute_argument(parser)
    args = parser.parse_args()

    logger = setup_logging("compute_gds_features", execute=args.execute)

    # Check dependencies
    try:
        from graphdatascience import GraphDataScience  # noqa: F401
    except ImportError as e:
        logger.error(str(e))
        logger.error("Install missing dependencies with: pip install graphdatascience")
        sys.exit(1)

    driver, database = get_driver_and_database(logger)
    gds = get_gds_client(driver, database=database)

    try:
        # Test connection
        if not verify_neo4j_connection(driver, database, logger):
            sys.exit(1)

        # Dry-run mode (default)
        if not args.execute:
            print_dry_run_plan(logger=logger)
            return

        # Execute mode
        logger.info("=" * 70)
        logger.info("Computing GDS Features")
        logger.info("=" * 70)
        logger.info(f"Using database: {database}")
        logger.info("")

        # Clean up leftover graph projections
        logger.info("Cleaning up leftover graph projections...")
        cleanup_leftover_graphs(gds, database=database, logger=logger)
        logger.info("✓ Cleanup complete")

        # Compute tech features (they don't depend on companies)
        compute_tech_adoption_prediction(gds, driver, database=database, logger=logger)
        compute_tech_affinity_bundling(gds, driver, database=database, logger=logger)

        # Compute company similarity if Company nodes exist
        with driver.session(database=database) as session:
            result = session.run(
                """
                MATCH (c:Company)
                WHERE c.description_embedding IS NOT NULL
                RETURN count(c) AS company_count
                """
            )
            company_count = result.single()["company_count"]

        if company_count > 0:
            logger.info(f"Found {company_count} companies with embeddings - computing similarity")
            compute_company_description_similarity(
                driver, database=database, execute=True, logger=logger
            )
        else:
            logger.info("⚠ No companies with embeddings found - skipping description similarity")

        # Compute technology similarity between companies
        with driver.session(database=database) as session:
            result = session.run(
                """
                MATCH (c:Company)-[:HAS_DOMAIN]->(:Domain)-[:USES]->(:Technology)
                RETURN count(DISTINCT c) AS company_count
                """
            )
            company_count = result.single()["company_count"]

        if company_count > 0:
            logger.info(f"Found {company_count} companies with technologies")
            compute_company_technology_similarity(
                gds, driver, database=database, execute=True, logger=logger
            )
        else:
            logger.info("⚠ No companies with technologies found - skipping tech similarity")

        # Summary
        logger.info("")
        logger.info("=" * 70)
        logger.info("GDS Features Complete!")
        logger.info("=" * 70)

        with driver.session(database=database) as session:
            result = session.run("MATCH ()-[r:LIKELY_TO_ADOPT]->() RETURN count(r) AS count")
            logger.info(f"Technology Adoption Predictions: {result.single()['count']}")

            result = session.run("MATCH ()-[r:CO_OCCURS_WITH]->() RETURN count(r) AS count")
            logger.info(f"Technology Affinity Relationships: {result.single()['count']}")

            result = session.run("MATCH ()-[r:SIMILAR_DESCRIPTION]->() RETURN count(r) AS count")
            logger.info(f"Company Description Similarities: {result.single()['count']}")

            result = session.run("MATCH ()-[r:SIMILAR_TECHNOLOGY]->() RETURN count(r) AS count")
            logger.info(f"Company Technology Similarities: {result.single()['count']}")

    finally:
        driver.close()
        gds.close()


if __name__ == "__main__":
    main()
