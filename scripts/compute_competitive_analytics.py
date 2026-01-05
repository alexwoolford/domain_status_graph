#!/usr/bin/env python3
"""
Compute competitive graph analytics using GDS.

Computes:
- PageRank: Most "central" companies in competitive network
- Louvain: Competitive communities/clusters
- Degree Centrality: Most threatened/threatening companies
- Betweenness Centrality: Bridge companies connecting industries

Usage:
    python scripts/compute_competitive_analytics.py          # Dry-run (plan only)
    python scripts/compute_competitive_analytics.py --execute  # Compute all analytics
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
from public_company_graph.gds import get_gds_client
from public_company_graph.gds.competitive_analytics import (
    compute_all_competitive_analytics,
)


def print_dry_run_plan(logger: logging.Logger = None):
    """Print the competitive analytics plan without executing."""
    if logger is None:
        logger = logging.getLogger(__name__)

    logger.info("=" * 70)
    logger.info("COMPETITIVE ANALYTICS PLAN (Dry Run)")
    logger.info("=" * 70)
    logger.info("")
    logger.info("This script will compute the following analytics:")
    logger.info("")
    logger.info("1. PageRank")
    logger.info("   - Measures centrality in competitive network")
    logger.info("   - Property: Company.competitive_pagerank")
    logger.info("   - Use case: Find most influential/threatened companies")
    logger.info("")
    logger.info("2. Louvain Community Detection")
    logger.info("   - Finds competitive clusters")
    logger.info("   - Property: Company.competitive_community")
    logger.info("   - Use case: Identify groups of companies that compete together")
    logger.info("")
    logger.info("3. Degree Centrality")
    logger.info("   - In-degree: How many companies cite you as competitor")
    logger.info("   - Out-degree: How many competitors you cite")
    logger.info("   - Properties: Company.competitive_in_degree, Company.competitive_out_degree")
    logger.info(
        "   - Use case: Find most threatened (high in-degree) vs threatening (high out-degree)"
    )
    logger.info("")
    logger.info("4. Betweenness Centrality")
    logger.info("   - Measures bridge companies connecting competitive clusters")
    logger.info("   - Property: Company.competitive_betweenness")
    logger.info("   - Use case: Find companies that connect different industries")
    logger.info("")
    logger.info("=" * 70)
    logger.info("To execute, run: python scripts/compute_competitive_analytics.py --execute")
    logger.info("=" * 70)


def main():
    """Run competitive analytics pipeline."""
    parser = argparse.ArgumentParser(description="Compute competitive graph analytics using GDS")
    add_execute_argument(parser)
    args = parser.parse_args()

    logger = setup_logging("compute_competitive_analytics", execute=args.execute)

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
        logger.info("Computing Competitive Graph Analytics")
        logger.info("=" * 70)
        logger.info(f"Using database: {database}")
        logger.info("")

        # Compute all analytics
        results = compute_all_competitive_analytics(gds, driver, database=database, logger=logger)

        # Summary
        logger.info("")
        logger.info("=" * 70)
        logger.info("Analytics Complete!")
        logger.info("=" * 70)
        logger.info(f"PageRank: {results['pagerank']} companies")
        logger.info(f"Communities: {results['communities']} clusters")
        logger.info(f"Degree Centrality: {results['degree']} companies")
        logger.info(f"Betweenness Centrality: {results['betweenness']} companies")

    finally:
        driver.close()
        gds.close()


if __name__ == "__main__":
    main()
