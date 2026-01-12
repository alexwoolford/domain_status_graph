#!/usr/bin/env python3
"""
Systemic edge cleanup using tiered confidence system.

This script applies the same tiered confidence logic used in the extraction
pipeline to clean up existing edges. It's idempotent and repeatable.

The cleanup ensures:
1. Edges below high threshold are converted to candidates
2. Edges below medium threshold are deleted
3. All fact edges meet high confidence requirements

This should be run:
- After initial data load (one-time cleanup of historical data)
- As part of the pipeline to ensure quality (optional, but recommended)
- After any manual edge creation to ensure consistency

Usage:
    # Dry run - see what would be cleaned
    python scripts/cleanup_edges_systemic.py

    # Execute cleanup
    python scripts/cleanup_edges_systemic.py --execute

    # Clean specific relationship types
    python scripts/cleanup_edges_systemic.py --execute --types HAS_CUSTOMER HAS_SUPPLIER
"""

import argparse
import logging

from public_company_graph.cli import get_driver_and_database, setup_logging
from public_company_graph.parsing.edge_cleanup import cleanup_relationship_edges

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(
        description="Systemically clean up relationship edges using tiered confidence system"
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Actually perform cleanup (default is dry-run)",
    )
    parser.add_argument(
        "--types",
        nargs="+",
        choices=["HAS_COMPETITOR", "HAS_PARTNER", "HAS_SUPPLIER", "HAS_CUSTOMER"],
        help="Specific relationship types to clean (default: all)",
    )
    args = parser.parse_args()

    log = setup_logging("cleanup_edges_systemic", execute=args.execute)
    driver, database = get_driver_and_database(log)

    try:
        if not args.execute:
            log.info("=" * 80)
            log.info("DRY RUN - Edge Quality Cleanup")
            log.info("=" * 80)
            log.info("This will show what would be cleaned without making changes.")
            log.info("Use --execute to apply changes.")
            log.info("")

        stats = cleanup_relationship_edges(
            driver=driver,
            database=database,
            relationship_types=args.types,
            dry_run=not args.execute,
        )

        # Print summary
        log.info("")
        log.info("=" * 80)
        log.info("CLEANUP SUMMARY")
        log.info("=" * 80)

        total_kept = 0
        total_converted = 0
        total_deleted = 0

        for rel_type, counts in sorted(stats.items()):
            kept = counts["kept"]
            converted = counts["converted"]
            deleted = counts["deleted"]
            total = kept + converted + deleted

            total_kept += kept
            total_converted += converted
            total_deleted += deleted

            log.info(f"\n{rel_type}:")
            log.info(f"  Kept (high confidence): {kept:,}")
            log.info(f"  Converted to candidate: {converted:,}")
            log.info(f"  Deleted (low confidence): {deleted:,}")
            log.info(f"  Total processed: {total:,}")

        log.info("")
        log.info("=" * 80)
        log.info("TOTALS")
        log.info("=" * 80)
        log.info(f"  Kept: {total_kept:,}")
        log.info(f"  Converted: {total_converted:,}")
        log.info(f"  Deleted: {total_deleted:,}")
        log.info(f"  Total: {total_kept + total_converted + total_deleted:,}")

        if not args.execute:
            log.info("")
            log.info("⚠️  DRY RUN - No changes made. Use --execute to apply cleanup.")

    finally:
        driver.close()


if __name__ == "__main__":
    main()
