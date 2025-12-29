#!/usr/bin/env python3
"""
Load Company nodes and relationships from 10-K filings.

This script (10-K first approach):
1. Creates Company nodes with company information from 10-K filings
2. Creates HAS_DOMAIN relationships linking Company to Domain nodes

Schema additions:
- Nodes: Company (key=cik)
- Relationships: (Company)-[:HAS_DOMAIN]->(Domain)
- Properties: Company.description, Company.risk_factors

Dependencies:
- Requires: Cache populated by parse_10k_filings.py (namespace: 10k_extracted)

Usage:
    python scripts/load_company_data.py          # Dry-run (plan only)
    python scripts/load_company_data.py --execute  # Actually load data
"""

import argparse
import logging
import sys

from domain_status_graph.cache import get_cache
from domain_status_graph.cli import (
    get_driver_and_database,
    setup_logging,
    verify_neo4j_connection,
)
from domain_status_graph.neo4j import clean_properties_batch, create_company_constraints


def load_companies(
    driver,
    cache,
    batch_size: int = 1000,
    database: str = None,
    execute: bool = False,
    logger: logging.Logger = None,
):
    """
    Load Company nodes from unified cache.

    Args:
        driver: Neo4j driver
        cache: AppCache instance
        batch_size: Batch size for loading
        database: Neo4j database name
        execute: If False, only print plan
        logger: Logger instance
    """
    if logger is None:
        logger = logging.getLogger(__name__)

    # 10-K first approach: Only use 10k_extracted cache
    company_keys = cache.keys(namespace="10k_extracted", limit=20000) or []

    if not company_keys:
        logger.error("No companies found in cache. Run parse_10k_filings.py first.")
        return []

    logger.info(f"Loading {len(company_keys)} companies from 10-K filings")

    # Load companies from 10-K cache
    companies_to_load = []
    for cik in company_keys:
        ten_k_data = cache.get("10k_extracted", cik)
        if not ten_k_data:
            continue

        # Get website from 10-K
        domain = ten_k_data.get("website")
        if domain:
            domain = domain.lower().replace("www.", "").strip()

        # Extract filing metadata
        filing_date = ten_k_data.get("filing_date")
        accession_number = ten_k_data.get("accession_number")
        fiscal_year_end = ten_k_data.get("fiscal_year_end")
        filing_year = ten_k_data.get("filing_year")

        # Fallback: Get from filing_metadata dict if present
        if not filing_date and ten_k_data.get("filing_metadata"):
            metadata = ten_k_data["filing_metadata"]
            filing_date = metadata.get("filing_date")
            accession_number = metadata.get("accession_number") or accession_number
            fiscal_year_end = metadata.get("fiscal_year_end") or fiscal_year_end
            filing_year = metadata.get("filing_year") or filing_year

        # Build company dict, using None for empty values (will be cleaned later)
        ticker_val = ten_k_data.get("ticker", "").upper().strip()
        name_val = ten_k_data.get("name", "").strip()
        desc_val = ten_k_data.get("business_description", "").strip()
        risk_val = ten_k_data.get("risk_factors", "").strip()

        companies_to_load.append(
            {
                "cik": str(cik),
                "ticker": ticker_val or None,
                "name": name_val or None,
                "description": desc_val or None,
                "description_source": "10k" if desc_val else None,
                "risk_factors": risk_val or None,
                "domain": domain or None,
                "filing_date": filing_date or None,
                "accession_number": accession_number or None,
                "fiscal_year_end": fiscal_year_end or None,
                "filing_year": filing_year or None,
            }
        )

    logger.info(f"Found {len(companies_to_load)} companies with data")

    if not execute:
        logger.info(f"DRY RUN: Would load {len(companies_to_load)} Company nodes")
        companies_with_domains = sum(1 for c in companies_to_load if c["domain"])
        logger.info(f"  {companies_with_domains} companies would have domains")
        return []

    # Load Company nodes in batches
    with driver.session(database=database) as session:
        total_loaded = 0
        for i in range(0, len(companies_to_load), batch_size):
            batch = companies_to_load[i : i + batch_size]

            # Clean empty strings and None values - Neo4j doesn't store nulls
            cleaned_batch = clean_properties_batch(batch)

            # Use CASE WHEN for date conversions (date() function) and computed fields
            # Properties in the cleaned batch will have empty strings/nulls removed
            query = """
            UNWIND $batch AS company
            MERGE (c:Company {cik: company.cik})
            SET c.ticker = company.ticker,
                c.name = company.name,
                c.description = company.description,
                c.description_source = company.description_source,
                c.risk_factors = company.risk_factors,
                c.loaded_at = datetime(),
                // Set filing metadata if available (using date() function for DATE type)
                c.filing_date = CASE WHEN company.filing_date IS NOT NULL THEN date(company.filing_date) ELSE c.filing_date END,
                c.filing_year = CASE WHEN company.filing_year IS NOT NULL THEN company.filing_year ELSE c.filing_year END,
                c.accession_number = CASE WHEN company.accession_number IS NOT NULL THEN company.accession_number ELSE c.accession_number END,
                c.fiscal_year_end = CASE WHEN company.fiscal_year_end IS NOT NULL THEN date(company.fiscal_year_end) ELSE c.fiscal_year_end END,
                // Construct SEC EDGAR URL for auditability (strip leading zeros from CIK)
                c.sec_filing_url = CASE
                    WHEN company.accession_number IS NOT NULL
                    THEN 'https://www.sec.gov/Archives/edgar/data/' +
                         toString(toInteger(company.cik)) + '/' +
                         replace(company.accession_number, '-', '') + '/'
                    ELSE c.sec_filing_url
                END
            REMOVE c.business_description_10k
            """

            session.run(query, batch=cleaned_batch)
            total_loaded += len(batch)
            logger.info(f"  Loaded {total_loaded}/{len(companies_to_load)} Company nodes...")

        logger.info(f"✓ Loaded {total_loaded} Company nodes")

    return companies_to_load


def create_has_domain_relationships(
    driver,
    companies_data: list[dict],
    batch_size: int = 1000,
    database: str = None,
    execute: bool = False,
    logger: logging.Logger = None,
):
    """
    Create HAS_DOMAIN relationships between Company and Domain nodes.

    Args:
        driver: Neo4j driver
        companies_data: List of company dictionaries with cik and domain
        batch_size: Batch size for loading
        database: Neo4j database name
        execute: If False, only print plan
        logger: Logger instance
    """
    if logger is None:
        logger = logging.getLogger(__name__)

    # Filter to companies with domains
    companies_with_domains = [c for c in companies_data if c.get("domain") and c["domain"]]

    logger.info(f"Found {len(companies_with_domains)} companies with domains")

    if not execute:
        logger.info(f"DRY RUN: Would create {len(companies_with_domains)} HAS_DOMAIN relationships")
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
            logger.info(
                f"  Created {total_created}/{len(companies_with_domains)} "
                f"HAS_DOMAIN relationships..."
            )

        logger.info(f"✓ Created {total_created} HAS_DOMAIN relationships")


def dry_run_plan(cache, logger: logging.Logger = None):
    """Print a dry-run plan."""
    if logger is None:
        logger = logging.getLogger(__name__)

    logger.info("=" * 80)
    logger.info("DRY RUN: Company Data Loading Plan")
    logger.info("=" * 80)

    # Get company keys from BOTH caches
    ten_k_keys = set(cache.keys(namespace="10k_extracted", limit=20000) or [])
    company_domain_keys = set(cache.keys(namespace="company_domains", limit=20000) or [])
    company_keys = list(ten_k_keys | company_domain_keys)

    if not company_keys:
        logger.error(
            "No companies found in cache. Run collect_domains.py or parse_10k_filings.py first."
        )
        return

    companies_with_domains = 0
    companies_with_descriptions = 0

    for cik in company_keys:
        # Check 10-K first
        ten_k_data = cache.get("10k_extracted", cik)
        company_data = cache.get("company_domains", cik)

        has_domain = False
        has_description = False

        if ten_k_data:
            if ten_k_data.get("website"):
                has_domain = True
            if ten_k_data.get("business_description"):
                has_description = True

        if company_data:
            if company_data.get("domain"):
                has_domain = True
            if company_data.get("description"):
                has_description = True

        if has_domain:
            companies_with_domains += 1
        if has_description:
            companies_with_descriptions += 1

    logger.info("")
    logger.info("Cache sources:")
    logger.info(f"  - 10k_extracted: {len(ten_k_keys)} companies")
    logger.info(f"  - company_domains: {len(company_domain_keys)} companies")
    logger.info(f"  Total (unique): {len(company_keys)}")
    logger.info(f"  Companies with domains: {companies_with_domains}")
    logger.info(f"  Companies with descriptions: {companies_with_descriptions}")

    logger.info("")
    logger.info("=" * 80)
    logger.info("To execute, run: python scripts/load_company_data.py --execute")
    logger.info("=" * 80)


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
        dry_run_plan(cache, logger=logger)
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
        logger.info("")
        logger.info("1. Creating constraints...")
        create_company_constraints(driver, database=database)

        # Load Company nodes
        logger.info("")
        logger.info("2. Loading Company nodes...")
        companies_data = load_companies(
            driver,
            cache,
            batch_size=args.batch_size,
            database=database,
            execute=True,
            logger=logger,
        )

        # Create HAS_DOMAIN relationships
        if companies_data:
            logger.info("")
            logger.info("3. Creating HAS_DOMAIN relationships...")
            create_has_domain_relationships(
                driver,
                companies_data,
                batch_size=args.batch_size,
                database=database,
                execute=True,
                logger=logger,
            )

        logger.info("")
        logger.info("=" * 80)
        logger.info("✓ Complete!")
        logger.info("=" * 80)

    finally:
        driver.close()


if __name__ == "__main__":
    main()
