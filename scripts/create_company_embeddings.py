#!/usr/bin/env python3
"""
Create OpenAI embeddings for company descriptions and store in Neo4j.

This script:
1. Reads Company nodes from Neo4j (with descriptions)
2. Creates embeddings using OpenAI (with unified cache)
3. Updates Company nodes with embeddings

Usage:
    python scripts/create_company_embeddings.py          # Dry-run (plan only)
    python scripts/create_company_embeddings.py --execute  # Actually create embeddings
"""

import argparse
import sys
import time

from tqdm import tqdm

from public_company_graph.cache import get_cache
from public_company_graph.cli import (
    get_driver_and_database,
    setup_logging,
    verify_neo4j_connection,
)
from public_company_graph.constants import BATCH_SIZE_SMALL
from public_company_graph.embeddings import (
    create_embeddings_for_nodes,
    get_openai_client,
    suppress_http_logging,
)


def main():
    """Run the company embeddings creation script."""
    parser = argparse.ArgumentParser(
        description="Create OpenAI embeddings for company descriptions"
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Actually create embeddings (default is dry-run)",
    )

    args = parser.parse_args()

    logger = setup_logging("create_company_embeddings", execute=args.execute)
    suppress_http_logging()

    driver, database = get_driver_and_database(logger)

    if not verify_neo4j_connection(driver, database, logger):
        sys.exit(1)

    # Check how many companies have descriptions
    with driver.session(database=database) as session:
        result = session.run(
            """
            MATCH (c:Company)
            WHERE c.description IS NOT NULL AND c.description <> ''
            RETURN count(c) AS count
            """
        )
        count = result.single()["count"]
        logger.info(f"Found {count} Company nodes with descriptions")

    if not args.execute:
        logger.info("=" * 80)
        logger.info("DRY RUN MODE")
        logger.info("=" * 80)
        logger.info(f"Would create embeddings for {count} companies")
        logger.info("To execute, run: python scripts/create_company_embeddings.py --execute")
        logger.info("=" * 80)
        driver.close()
        return

    # Get OpenAI client
    try:
        client = get_openai_client()
    except (ImportError, ValueError) as e:
        logger.error(str(e))
        driver.close()
        sys.exit(1)

    cache = get_cache()

    # Log cache status upfront
    cache_stats = cache.stats()
    logger.info("Cache status:")
    logger.info(f"  Total entries: {cache_stats['total']:,}")
    logger.info(f"  Size: {cache_stats['size_mb']} MB")
    for ns, ns_count in sorted(cache_stats["by_namespace"].items(), key=lambda x: -x[1]):
        logger.info(f"    {ns}: {ns_count:,}")

    logger.info("=" * 80)
    logger.info("Creating Company Description Embeddings")
    logger.info("=" * 80)

    # Step 1: Update Company nodes with 10-K descriptions (if available)
    # This ensures companies have the best available description in the description property
    logger.info("Updating Company nodes with 10-K descriptions (if available)...")

    # Get all companies and check cache for 10-K descriptions
    with driver.session(database=database) as session:
        result = session.run("MATCH (c:Company) RETURN c.cik AS cik")
        companies = [record["cik"] for record in result]

    # Build batch of updates from cache
    updates_batch: list[dict] = []
    cache_hits = 0
    cache_misses = 0

    for cik in tqdm(companies, desc="Checking 10-K cache", unit="company"):
        ten_k_data = cache.get("10k_extracted", cik)
        if ten_k_data and ten_k_data.get("business_description"):
            updates_batch.append(
                {
                    "cik": cik,
                    "desc": ten_k_data["business_description"],
                }
            )
            cache_hits += 1
        else:
            cache_misses += 1

    logger.info(
        f"  10-K cache: {cache_hits:,} hits, {cache_misses:,} misses "
        f"({100 * cache_hits / len(companies):.1f}% hit rate)"
    )

    # Batch update Neo4j (much faster than individual updates)
    if updates_batch:
        start_time = time.time()
        total_updated = 0

        with driver.session(database=database) as session:
            for i in tqdm(
                range(0, len(updates_batch), BATCH_SIZE_SMALL),
                desc="Updating Neo4j",
                unit="batch",
            ):
                batch = updates_batch[i : i + BATCH_SIZE_SMALL]
                session.run(
                    """
                    UNWIND $batch AS row
                    MATCH (c:Company {cik: row.cik})
                    SET c.description = row.desc,
                        c.description_source = '10k'
                    """,
                    batch=batch,
                )
                total_updated += len(batch)

        elapsed = time.time() - start_time
        logger.info(f"  Updated {total_updated:,} companies in {elapsed:.1f}s")

    # Step 2: Create embeddings for all companies with descriptions
    logger.info("Creating embeddings for companies with descriptions...")
    processed, created, cached, failed = create_embeddings_for_nodes(
        driver=driver,
        cache=cache,
        node_label="Company",
        text_property="description",
        key_property="cik",
        embedding_property="description_embedding",
        openai_client=client,
        database=database,
        execute=True,
        log=logger,
    )

    logger.info("=" * 80)
    logger.info("Complete!")
    logger.info("=" * 80)
    logger.info(f"Processed: {processed}")
    logger.info(f"Created: {created}")
    logger.info(f"Cached: {cached}")
    logger.info(f"Failed: {failed}")

    driver.close()


if __name__ == "__main__":
    main()
