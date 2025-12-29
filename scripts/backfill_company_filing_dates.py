#!/usr/bin/env python3
"""
Backfill Company nodes with filing_date from 10-K cache.

This script updates existing Company nodes that don't have filing_date
but have it available in the 10-K cache. This is useful for companies
that were loaded before the filing_date feature was implemented.

Usage:
    python scripts/backfill_company_filing_dates.py          # Dry-run (plan only)
    python scripts/backfill_company_filing_dates.py --execute  # Actually update nodes
"""

import argparse
import sys

from public_company_graph.cache import get_cache
from public_company_graph.cli import (
    add_execute_argument,
    get_driver_and_database,
    setup_logging,
    verify_neo4j_connection,
)
from public_company_graph.neo4j import create_company_constraints


def backfill_filing_dates(
    driver,
    cache,
    batch_size: int = 1000,
    database: str = None,
    execute: bool = False,
) -> dict[str, int]:
    """
    Backfill Company nodes with filing_date from 10-K cache.

    Args:
        driver: Neo4j driver
        cache: AppCache instance
        database: Neo4j database name
        execute: If False, only print plan
        batch_size: Batch size for updates

    Returns:
        Dict with counts: total, updated, already_had_date, no_cache_data
    """
    # Get all Company nodes from Neo4j
    with driver.session(database=database) as session:
        result = session.run(
            """
            MATCH (c:Company)
            RETURN c.cik AS cik, c.filing_date AS filing_date
            """
        )
        companies = [(record["cik"], record["filing_date"]) for record in result]

    print(f"Found {len(companies)} Company nodes in Neo4j")

    # Find companies that need updating (no filing_date but have it in cache)
    companies_to_update = []
    already_had_date = 0
    no_cache_data = 0

    for cik, existing_filing_date in companies:
        # Skip if already has filing_date
        if existing_filing_date is not None:
            already_had_date += 1
            continue

        # Check 10-K cache for filing metadata
        ten_k_data = cache.get("10k_extracted", cik)
        if not ten_k_data:
            no_cache_data += 1
            continue

        # Extract filing metadata
        filing_date = ten_k_data.get("filing_date")
        filing_year = ten_k_data.get("filing_year")
        accession_number = ten_k_data.get("accession_number")
        fiscal_year_end = ten_k_data.get("fiscal_year_end")

        # Fallback: Get from filing_metadata dict if present
        if not filing_date and ten_k_data.get("filing_metadata"):
            metadata = ten_k_data["filing_metadata"]
            filing_date = metadata.get("filing_date")
            filing_year = metadata.get("filing_year") or filing_year
            accession_number = metadata.get("accession_number") or accession_number
            fiscal_year_end = metadata.get("fiscal_year_end") or fiscal_year_end

        # Only add if we have filing_date
        if filing_date:
            companies_to_update.append(
                {
                    "cik": str(cik),
                    "filing_date": filing_date,  # YYYY-MM-DD format string
                    "filing_year": filing_year,
                    "accession_number": accession_number,
                    "fiscal_year_end": fiscal_year_end,  # YYYY-MM-DD format string
                }
            )

    print(f"\nCompanies that already have filing_date: {already_had_date}")
    print(f"Companies with no 10-K cache data: {no_cache_data}")
    print(f"Companies that can be updated: {len(companies_to_update)}")

    if not execute:
        print(f"\nDRY RUN: Would update {len(companies_to_update)} Company nodes")
        if companies_to_update:
            print("\nSample companies to update:")
            for company in companies_to_update[:5]:
                print(
                    f"  CIK {company['cik']}: filing_date={company['filing_date']}, "
                    f"filing_year={company.get('filing_year')}"
                )
        return {
            "total": len(companies),
            "updated": 0,
            "already_had_date": already_had_date,
            "no_cache_data": no_cache_data,
        }

    if not companies_to_update:
        print("\n✓ No companies need updating")
        return {
            "total": len(companies),
            "updated": 0,
            "already_had_date": already_had_date,
            "no_cache_data": no_cache_data,
        }

    # Update Company nodes in batches
    with driver.session(database=database) as session:
        total_updated = 0
        for i in range(0, len(companies_to_update), batch_size):
            batch = companies_to_update[i : i + batch_size]

            query = """
            UNWIND $batch AS company
            MATCH (c:Company {cik: company.cik})
            SET c.filing_date = date(company.filing_date),
                c.filing_year = CASE WHEN company.filing_year IS NOT NULL
                    THEN company.filing_year ELSE c.filing_year END,
                c.accession_number = CASE WHEN company.accession_number IS NOT NULL
                    THEN company.accession_number ELSE c.accession_number END,
                c.fiscal_year_end = CASE WHEN company.fiscal_year_end IS NOT NULL
                    THEN date(company.fiscal_year_end) ELSE c.fiscal_year_end END
            """

            session.run(query, batch=batch)
            total_updated += len(batch)
            print(f"  Updated {total_updated}/{len(companies_to_update)} Company nodes...")

        print(f"✓ Updated {total_updated} Company nodes with filing_date")

    return {
        "total": len(companies),
        "updated": total_updated,
        "already_had_date": already_had_date,
        "no_cache_data": no_cache_data,
    }


def main():
    """Run the backfill script."""
    parser = argparse.ArgumentParser(
        description="Backfill Company nodes with filing_date from 10-K cache"
    )
    add_execute_argument(parser)
    parser.add_argument(
        "--batch-size",
        type=int,
        default=1000,
        help="Batch size for updates (default: 1000)",
    )

    args = parser.parse_args()

    logger = setup_logging("backfill_company_filing_dates", execute=args.execute)
    cache = get_cache()

    if not args.execute:
        print("=" * 80)
        print("DRY RUN: Backfill Company Filing Dates")
        print("=" * 80)
        print()
        print("This script will update Company nodes that don't have filing_date")
        print("but have it available in the 10-K cache.")
        print()

    driver, database = get_driver_and_database(logger)

    try:
        # Test connection
        if not verify_neo4j_connection(driver, database, logger):
            sys.exit(1)

        # Ensure constraints exist
        logger.info("Ensuring constraints exist...")
        create_company_constraints(driver, database=database)

        # Backfill filing dates
        logger.info("")
        logger.info("Backfilling filing dates...")
        stats = backfill_filing_dates(
            driver,
            cache,
            batch_size=args.batch_size,
            database=database,
            execute=args.execute,
        )

        logger.info("")
        logger.info("=" * 80)
        logger.info("Backfill Complete!")
        logger.info("=" * 80)
        logger.info(f"Total companies: {stats['total']}")
        logger.info(f"Already had filing_date: {stats['already_had_date']}")
        logger.info(f"No cache data: {stats['no_cache_data']}")
        if args.execute:
            logger.info(f"Updated: {stats['updated']}")

        logger.info("")
        logger.info("To query companies with filing dates:")
        logger.info("  MATCH (c:Company) WHERE c.filing_date IS NOT NULL")
        logger.info("  RETURN c.ticker, c.name, c.filing_date, c.filing_year")
        logger.info("  ORDER BY c.filing_date DESC")

    finally:
        driver.close()
        cache.close()


if __name__ == "__main__":
    main()
