#!/usr/bin/env python3
"""
Load Company nodes and relationships from unified cache.

This script:
1. Creates Company nodes with company information (CIK, ticker, name, description)
2. Creates HAS_DOMAIN relationships linking Company to Domain nodes

Schema additions:
- Nodes: Company (key=cik)
- Relationships: (Company)-[:HAS_DOMAIN]->(Domain)
- Properties: Company.description

Dependencies:
- Requires: Cache populated by collect_domains.py (namespace: company_domains)

Usage:
    python scripts/load_company_data.py          # Dry-run (plan only)
    python scripts/load_company_data.py --execute  # Actually load data
"""

import argparse
import sys
from typing import Dict, List

from domain_status_graph.cache import get_cache
from domain_status_graph.cli import (
    get_driver_and_database,
    setup_logging,
    verify_neo4j_connection,
)
from domain_status_graph.neo4j import create_company_constraints


def load_companies(
    driver,
    cache,
    batch_size: int = 1000,
    database: str = None,
    execute: bool = False,
):
    """
    Load Company nodes from unified cache.

    Args:
        driver: Neo4j driver
        cache: AppCache instance
        batch_size: Batch size for loading
        database: Neo4j database name
        execute: If False, only print plan
    """
    # Get all company keys from cache
    company_keys = cache.keys(namespace="company_domains", limit=10000)

    if not company_keys:
        print("ERROR: No companies found in cache. Run collect_domains.py first.")
        return []

    print(f"Loading companies from cache: {len(company_keys)} companies")

    # Load companies from cache
    companies_to_load = []
    for cik in company_keys:
        company_data = cache.get("company_domains", cik)
        if not company_data:
            continue

        domain = company_data.get("domain")
        if domain:
            # Normalize domain to match Domain.final_domain format
            domain = domain.lower().replace("www.", "").strip()

        companies_to_load.append(
            {
                "cik": str(company_data.get("cik", cik)),
                "ticker": company_data.get("ticker", "").upper(),
                "name": company_data.get("name", "").strip(),
                "description": company_data.get("description", "").strip() or None,
                "domain": domain,
            }
        )

    print(f"Found {len(companies_to_load)} companies with data")

    if not execute:
        print(f"\nDRY RUN: Would load {len(companies_to_load)} Company nodes")
        companies_with_domains = sum(1 for c in companies_to_load if c["domain"])
        print(f"  {companies_with_domains} companies would have domains")
        return []

    # Load Company nodes in batches
    with driver.session(database=database) as session:
        total_loaded = 0
        for i in range(0, len(companies_to_load), batch_size):
            batch = companies_to_load[i : i + batch_size]

            query = """
            UNWIND $batch AS company
            MERGE (c:Company {cik: company.cik})
            SET c.ticker = company.ticker,
                c.name = company.name,
                c.description = company.description,
                c.loaded_at = datetime()
            """

            session.run(query, batch=batch)
            total_loaded += len(batch)
            print(f"  Loaded {total_loaded}/{len(companies_to_load)} Company nodes...")

        print(f"✓ Loaded {total_loaded} Company nodes")

    return companies_to_load


def create_has_domain_relationships(
    driver,
    companies_data: List[Dict],
    batch_size: int = 1000,
    database: str = None,
    execute: bool = False,
):
    """
    Create HAS_DOMAIN relationships between Company and Domain nodes.

    Args:
        driver: Neo4j driver
        companies_data: List of company dictionaries with cik and domain
        batch_size: Batch size for loading
        database: Neo4j database name
        execute: If False, only print plan
    """
    # Filter to companies with domains
    companies_with_domains = [c for c in companies_data if c.get("domain") and c["domain"]]

    print(f"Found {len(companies_with_domains)} companies with domains")

    if not execute:
        print(f"\nDRY RUN: Would create {len(companies_with_domains)} HAS_DOMAIN relationships")
        return

    # Create relationships in batches
    with driver.session(database=database) as session:
        total_created = 0
        for i in range(0, len(companies_with_domains), batch_size):
            batch = companies_with_domains[i : i + batch_size]

            query = """
            UNWIND $batch AS company
            MATCH (c:Company {cik: company.cik})
            MATCH (d:Domain {final_domain: company.domain})
            MERGE (c)-[r:HAS_DOMAIN]->(d)
            SET r.loaded_at = datetime()
            """

            result = session.run(query, batch=batch)
            # Consume result to execute query
            result.consume()
            total_created += len(batch)
            print(
                f"  Created {total_created}/{len(companies_with_domains)} "
                f"HAS_DOMAIN relationships..."
            )

        print(f"✓ Created {total_created} HAS_DOMAIN relationships")


def dry_run_plan(cache):
    """Print a dry-run plan."""
    print("=" * 80)
    print("DRY RUN: Company Data Loading Plan")
    print("=" * 80)

    company_keys = cache.keys(namespace="company_domains", limit=10000)

    if not company_keys:
        print("ERROR: No companies found in cache. Run collect_domains.py first.")
        return

    companies_with_domains = 0
    companies_with_descriptions = 0

    for cik in company_keys:
        company_data = cache.get("company_domains", cik)
        if company_data:
            if company_data.get("domain"):
                companies_with_domains += 1
            if company_data.get("description"):
                companies_with_descriptions += 1

    print("\nCache: company_domains namespace")
    print(f"  Total companies: {len(company_keys)}")
    print(f"  Companies with domains: {companies_with_domains}")
    print(f"  Companies with descriptions: {companies_with_descriptions}")

    print("\n" + "=" * 80)
    print("To execute, run: python scripts/load_company_data.py --execute")
    print("=" * 80)


def main():
    """Run the company data loading script."""
    parser = argparse.ArgumentParser(description="Load Company nodes and relationships into Neo4j")
    parser.add_argument(
        "--execute", action="store_true", help="Actually load data (default is dry-run)"
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=1000,
        help="Batch size for loading (default: 1000)",
    )

    args = parser.parse_args()

    logger = setup_logging("load_company_data", execute=args.execute)
    cache = get_cache()

    if not args.execute:
        dry_run_plan(cache)
        return

    logger.info("=" * 80)
    logger.info("Loading Company Data into Neo4j")
    logger.info("=" * 80)

    driver, database = get_driver_and_database(logger)

    try:
        # Test connection
        if not verify_neo4j_connection(driver, database, logger):
            sys.exit(1)

        # Create constraints
        logger.info("\n1. Creating constraints...")
        create_company_constraints(driver, database=database)

        # Load Company nodes
        logger.info("\n2. Loading Company nodes...")
        companies_data = load_companies(
            driver,
            cache,
            batch_size=args.batch_size,
            database=database,
            execute=True,
        )

        # Create HAS_DOMAIN relationships
        if companies_data:
            logger.info("\n3. Creating HAS_DOMAIN relationships...")
            create_has_domain_relationships(
                driver,
                companies_data,
                batch_size=args.batch_size,
                database=database,
                execute=True,
            )

        logger.info("\n" + "=" * 80)
        logger.info("✓ Complete!")
        logger.info("=" * 80)

    finally:
        driver.close()


if __name__ == "__main__":
    main()
