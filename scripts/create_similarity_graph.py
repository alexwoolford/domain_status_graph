#!/usr/bin/env python3
"""
Create embeddings and similarity relationships for the knowledge graph.

This script orchestrates the complete pipeline:
1. Load Company nodes with 10-K business descriptions
2. Create embeddings for Company nodes
3. Create SIMILAR_DESCRIPTION relationships between Companies
4. Create embeddings for Domain nodes (if descriptions exist)
5. Create SIMILAR_DESCRIPTION relationships between Domains

This is a clean, repeatable process for building the similarity graph.

Usage:
    python scripts/create_similarity_graph.py                    # Dry-run (plan only)
    python scripts/create_similarity_graph.py --execute          # Actually create embeddings and relationships
"""

import argparse
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

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
from domain_status_graph.gds.company_similarity import compute_company_description_similarity
from domain_status_graph.similarity.cosine import (
    compute_similarity_for_node_type,
    write_similarity_relationships,
)
from scripts.load_company_data import load_companies


def main():
    """Run the complete similarity graph creation pipeline."""
    parser = argparse.ArgumentParser(
        description="Create embeddings and similarity relationships for the knowledge graph"
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Actually create embeddings and relationships (default is dry-run)",
    )
    parser.add_argument(
        "--skip-companies",
        action="store_true",
        help="Skip company-related steps (load, embeddings, similarity)",
    )
    parser.add_argument(
        "--skip-domains",
        action="store_true",
        help="Skip domain-related steps (embeddings, similarity)",
    )

    args = parser.parse_args()

    logger = setup_logging("create_similarity_graph", execute=args.execute)
    suppress_http_logging()

    driver, database = get_driver_and_database(logger)

    if not verify_neo4j_connection(driver, database, logger):
        sys.exit(1)

    cache = get_cache()

    logger.info("=" * 80)
    logger.info("SIMILARITY GRAPH CREATION PIPELINE")
    logger.info("=" * 80)
    logger.info("")

    if not args.execute:
        logger.info("DRY RUN MODE - No changes will be made")
        logger.info("")
        logger.info("Pipeline steps:")
        logger.info("  1. Load Company nodes with 10-K business descriptions")
        logger.info("  2. Create embeddings for Company nodes")
        logger.info("  3. Create SIMILAR_DESCRIPTION relationships between Companies")
        if not args.skip_domains:
            logger.info("  4. Create embeddings for Domain nodes")
            logger.info("  5. Create SIMILAR_DESCRIPTION relationships between Domains")
        logger.info("")
        logger.info("To execute, run: python scripts/create_similarity_graph.py --execute")
        logger.info("=" * 80)
        driver.close()
        return

    # Step 1: Load Company nodes with 10-K business descriptions
    if not args.skip_companies:
        logger.info("=" * 80)
        logger.info("STEP 1: Load Company Nodes with 10-K Business Descriptions")
        logger.info("=" * 80)
        logger.info("")

        load_companies(
            driver=driver,
            cache=cache,
            batch_size=1000,
            database=database,
            execute=True,
        )

        logger.info("")
        logger.info("✓ Step 1 complete: Company nodes loaded")
        logger.info("")

        # Step 2: Create embeddings for Company nodes
        logger.info("=" * 80)
        logger.info("STEP 2: Create Embeddings for Company Nodes")
        logger.info("=" * 80)
        logger.info("")

        try:
            client = get_openai_client()
        except (ImportError, ValueError) as e:
            logger.error(f"Failed to get OpenAI client: {e}")
            logger.error("Set OPENAI_API_KEY environment variable")
            driver.close()
            sys.exit(1)

        # Update Company nodes with 10-K descriptions first
        logger.info("Updating Company nodes with 10-K business descriptions...")
        with driver.session(database=database) as session:
            result = session.run("MATCH (c:Company) RETURN c.cik AS cik")
            companies = [record["cik"] for record in result]

            updated_count = 0
            for cik in companies:
                ten_k_data = cache.get("10k_extracted", cik)
                if ten_k_data and ten_k_data.get("business_description"):
                    session.run(
                        """
                        MATCH (c:Company {cik: $cik})
                        SET c.description = $desc,
                            c.description_source = '10k'
                        """,
                        cik=cik,
                        desc=ten_k_data["business_description"],
                    )
                    updated_count += 1

            logger.info(f"  Updated {updated_count} companies with 10-K descriptions")

        # Create embeddings for all companies with descriptions
        # (description_source indicates quality: '10k' preferred, others are fallback)
        logger.info("Creating embeddings for companies with descriptions...")
        processed, created, cached, failed = create_embeddings_for_nodes(
            driver=driver,
            cache=cache,
            node_label="Company",
            text_property="description",  # Single property - source indicated by description_source
            key_property="cik",
            embedding_property="description_embedding",
            create_fn=lambda text, model: create_embedding(client, text, model),
            database=database,
            execute=True,
            log=logger,  # Pass logger for proper output
        )

        logger.info(
            f"  Processed: {processed}, Created: {created}, Cached: {cached}, Failed: {failed}"
        )

        logger.info("")
        logger.info("✓ Step 2 complete: Company embeddings created")
        logger.info("")

        # Step 3: Create SIMILAR_DESCRIPTION relationships between Companies
        logger.info("=" * 80)
        logger.info("STEP 3: Create SIMILAR_DESCRIPTION Relationships Between Companies")
        logger.info("=" * 80)
        logger.info("")

        relationships_created = compute_company_description_similarity(
            driver=driver,
            similarity_threshold=0.6,  # Lowered from 0.7 to capture more known competitor pairs
            top_k=50,
            database=database,
            execute=True,
            logger=logger,
        )

        logger.info("")
        logger.info(
            f"✓ Step 3 complete: {relationships_created} SIMILAR_DESCRIPTION relationships created"
        )
        logger.info("")

    # Step 4: Create embeddings for Domain nodes
    if not args.skip_domains:
        logger.info("=" * 80)
        logger.info("STEP 4: Create Embeddings for Domain Nodes")
        logger.info("=" * 80)
        logger.info("")

        try:
            client = get_openai_client()
        except (ImportError, ValueError) as e:
            logger.error(f"Failed to get OpenAI client: {e}")
            logger.error("Set OPENAI_API_KEY environment variable")
            driver.close()
            sys.exit(1)

        processed_domains, created_domains, cached_domains, failed_domains = (
            create_embeddings_for_nodes(
                driver=driver,
                cache=cache,
                node_label="Domain",
                text_property="description",
                key_property="final_domain",
                embedding_property="description_embedding",
                create_fn=lambda text, model: create_embedding(client, text, model),
                database=database,
                execute=True,
                log=logger,  # Pass logger for proper output
            )
        )

        logger.info(
            f"  Processed: {processed_domains}, Created: {created_domains}, Cached: {cached_domains}, Failed: {failed_domains}"
        )
        logger.info("")
        logger.info("✓ Step 4 complete: Domain embeddings created")
        logger.info("")

        # Step 5: Create SIMILAR_DESCRIPTION relationships between Domains
        logger.info("=" * 80)
        logger.info("STEP 5: Create SIMILAR_DESCRIPTION Relationships Between Domains")
        logger.info("=" * 80)
        logger.info("")

        pairs = compute_similarity_for_node_type(
            driver=driver,
            node_label="Domain",
            key_property="final_domain",
            embedding_property="description_embedding",
            similarity_threshold=0.7,
            top_k=50,
            database=database,
            logger_instance=logger,
        )

        relationships_created = write_similarity_relationships(
            driver=driver,
            pairs=pairs,
            node_label="Domain",
            key_property="final_domain",
            relationship_type="SIMILAR_DESCRIPTION",
            database=database,
            batch_size=1000,
            logger_instance=logger,
        )

        logger.info("")
        logger.info(
            f"✓ Step 5 complete: {relationships_created} SIMILAR_DESCRIPTION relationships created"
        )
        logger.info("")

    # Summary
    logger.info("=" * 80)
    logger.info("PIPELINE COMPLETE")
    logger.info("=" * 80)
    logger.info("")

    # Show final statistics
    with driver.session(database=database) as session:
        # Company stats
        if not args.skip_companies:
            result = session.run(
                """
                MATCH (c:Company)
                RETURN
                    count(c) AS total,
                    sum(CASE WHEN c.description_embedding IS NOT NULL THEN 1 ELSE 0 END) AS with_embedding,
                    sum(CASE WHEN EXISTS((c)-[:SIMILAR_DESCRIPTION]->()) THEN 1 ELSE 0 END) AS with_similar_rel
                """
            )
            row = result.single()
            logger.info("Company Statistics:")
            logger.info(f"  Total: {row['total']:,}")
            logger.info(f"  With embeddings: {row['with_embedding']:,}")
            logger.info(f"  With SIMILAR_DESCRIPTION relationships: {row['with_similar_rel']:,}")

            result = session.run(
                """
                MATCH ()-[r:SIMILAR_DESCRIPTION]->()
                WHERE startNode(r):Company
                RETURN count(r) AS count
                """
            )
            rel_count = result.single()["count"]
            logger.info(f"  Total Company SIMILAR_DESCRIPTION relationships: {rel_count:,}")
            logger.info("")

        # Domain stats
        if not args.skip_domains:
            result = session.run(
                """
                MATCH (d:Domain)
                RETURN
                    count(d) AS total,
                    sum(CASE WHEN d.description_embedding IS NOT NULL THEN 1 ELSE 0 END) AS with_embedding,
                    sum(CASE WHEN EXISTS((d)-[:SIMILAR_DESCRIPTION]->()) THEN 1 ELSE 0 END) AS with_similar_rel
                """
            )
            row = result.single()
            logger.info("Domain Statistics:")
            logger.info(f"  Total: {row['total']:,}")
            logger.info(f"  With embeddings: {row['with_embedding']:,}")
            logger.info(f"  With SIMILAR_DESCRIPTION relationships: {row['with_similar_rel']:,}")

            result = session.run(
                """
                MATCH ()-[r:SIMILAR_DESCRIPTION]->()
                WHERE startNode(r):Domain
                RETURN count(r) AS count
                """
            )
            rel_count = result.single()["count"]
            logger.info(f"  Total Domain SIMILAR_DESCRIPTION relationships: {rel_count:,}")
            logger.info("")

    logger.info("=" * 80)
    logger.info("Next Steps:")
    logger.info("  1. Test similarity queries (see docs/GRAPH_CREATION_PIPELINE.md)")
    logger.info("  2. Verify PEP ~ KO, Home Depot ~ Lowes similarity")
    logger.info("=" * 80)

    driver.close()


if __name__ == "__main__":
    main()
