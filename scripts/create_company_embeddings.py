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

from domain_status_graph.cache import get_cache
from domain_status_graph.cli import (
    get_driver_and_database,
    setup_logging,
    verify_neo4j_connection,
)
from domain_status_graph.embeddings import (
    create_embedding,
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

    logger.info("=" * 80)
    logger.info("Creating Company Description Embeddings")
    logger.info("=" * 80)

    # Create embeddings using the standard function
    processed, created, cached, failed = create_embeddings_for_nodes(
        driver=driver,
        cache=cache,
        node_label="Company",
        text_property="description",
        key_property="cik",
        embedding_property="description_embedding",
        create_fn=lambda text, model: create_embedding(client, text, model),
        database=database,
        execute=True,
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
