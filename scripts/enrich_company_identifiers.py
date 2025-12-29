#!/usr/bin/env python3
"""
Enrich Company nodes with name and ticker from SEC EDGAR.

This script fetches the authoritative company list from SEC's company_tickers.json
and updates Company nodes that are missing name or ticker properties.

The SEC data provides:
- cik: Central Index Key (unique company identifier)
- ticker: Stock ticker symbol
- title: Official company name

Usage:
    python scripts/enrich_company_identifiers.py                # Dry-run
    python scripts/enrich_company_identifiers.py --execute      # Update nodes
"""

import argparse
import logging
import sys

from domain_status_graph.cli import (
    add_execute_argument,
    get_driver_and_database,
    setup_logging,
    verify_neo4j_connection,
)
from domain_status_graph.sources.sec_companies import get_all_companies_from_sec

logger = logging.getLogger(__name__)


def enrich_company_identifiers(
    driver,
    database: str = None,
    execute: bool = False,
    batch_size: int = 1000,
    log: logging.Logger = None,
) -> dict:
    """
    Enrich Company nodes with name and ticker from SEC EDGAR.

    Args:
        driver: Neo4j driver
        database: Database name
        execute: If False, only show plan
        batch_size: Batch size for updates
        log: Logger instance

    Returns:
        Dict with stats: total_sec, matched, updated
    """
    if log is None:
        log = logger

    # Fetch SEC company data
    log.info("Fetching company data from SEC EDGAR...")
    sec_companies = get_all_companies_from_sec()
    log.info(f"Found {len(sec_companies):,} companies from SEC")

    # Build lookup by CIK
    sec_by_cik = {c["cik"]: c for c in sec_companies}

    # Get Company nodes missing name or ticker
    with driver.session(database=database) as session:
        result = session.run(
            """
            MATCH (c:Company)
            WHERE c.cik IS NOT NULL
            RETURN c.cik AS cik, c.name AS name, c.ticker AS ticker
        """
        )

        companies_to_update = []
        matched = 0
        already_complete = 0

        for record in result:
            cik = record["cik"]
            current_name = record["name"]
            current_ticker = record["ticker"]

            # Ensure CIK is 10-digit zero-padded for lookup
            cik_padded = str(cik).zfill(10)

            if cik_padded in sec_by_cik:
                matched += 1
                sec_data = sec_by_cik[cik_padded]

                # Check if update is needed
                needs_name = not current_name and sec_data["name"]
                needs_ticker = not current_ticker and sec_data["ticker"]

                if needs_name or needs_ticker:
                    companies_to_update.append(
                        {
                            "cik": cik,  # Use original CIK from graph
                            "name": sec_data["name"] if needs_name else None,
                            "ticker": sec_data["ticker"] if needs_ticker else None,
                        }
                    )
                else:
                    already_complete += 1

    log.info(f"Matched {matched:,} companies with SEC data")
    log.info(f"  Already complete (have name+ticker): {already_complete:,}")
    log.info(f"  Need updates: {len(companies_to_update):,}")

    if not execute:
        log.info("")
        log.info("DRY RUN: Would update the following:")
        # Show sample
        for c in companies_to_update[:10]:
            updates = []
            if c["name"]:
                updates.append(f"name='{c['name'][:40]}'")
            if c["ticker"]:
                updates.append(f"ticker='{c['ticker']}'")
            log.info(f"  CIK {c['cik']}: {', '.join(updates)}")
        if len(companies_to_update) > 10:
            log.info(f"  ... and {len(companies_to_update) - 10} more")
        return {
            "total_sec": len(sec_companies),
            "matched": matched,
            "to_update": len(companies_to_update),
            "updated": 0,
        }

    # Update in batches
    total_updated = 0
    with driver.session(database=database) as session:
        for i in range(0, len(companies_to_update), batch_size):
            batch = companies_to_update[i : i + batch_size]

            # Update query - only set non-null values
            session.run(
                """
                UNWIND $batch AS update
                MATCH (c:Company {cik: update.cik})
                SET c.name = COALESCE(update.name, c.name),
                    c.ticker = COALESCE(update.ticker, c.ticker)
            """,
                batch=batch,
            )

            total_updated += len(batch)
            log.info(f"  Updated {total_updated:,}/{len(companies_to_update):,}...")

    log.info(f"✓ Updated {total_updated:,} Company nodes with SEC identifiers")

    return {
        "total_sec": len(sec_companies),
        "matched": matched,
        "to_update": len(companies_to_update),
        "updated": total_updated,
    }


def main():
    """Run the company identifier enrichment."""
    parser = argparse.ArgumentParser(
        description="Enrich Company nodes with name and ticker from SEC EDGAR"
    )
    add_execute_argument(parser)
    parser.add_argument(
        "--batch-size",
        type=int,
        default=1000,
        help="Batch size for updates (default: 1000)",
    )

    args = parser.parse_args()

    log = setup_logging("enrich_company_identifiers", execute=args.execute)

    driver, database = get_driver_and_database(log)

    try:
        if not verify_neo4j_connection(driver, database, log):
            sys.exit(1)

        log.info("=" * 80)
        log.info("Enriching Company Nodes with SEC Identifiers")
        log.info("=" * 80)
        log.info("")

        stats = enrich_company_identifiers(
            driver,
            database=database,
            execute=args.execute,
            batch_size=args.batch_size,
            log=log,
        )

        log.info("")
        log.info("=" * 80)
        if args.execute:
            log.info("✓ Complete!")
            log.info(f"  SEC companies: {stats['total_sec']:,}")
            log.info(f"  Matched: {stats['matched']:,}")
            log.info(f"  Updated: {stats['updated']:,}")
        else:
            log.info("DRY RUN complete")
            log.info("To execute: python scripts/enrich_company_identifiers.py --execute")
        log.info("=" * 80)

    finally:
        driver.close()


if __name__ == "__main__":
    main()
