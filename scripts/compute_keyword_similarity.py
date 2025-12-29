#!/usr/bin/env python3
"""
Compute Domain-Domain similarity based on keyword embeddings.

This script demonstrates the standard pattern for embedding-based similarity:
1. Create embeddings for a text property (keywords) using the shared infrastructure
2. Compute pairwise cosine similarity using the shared similarity module
3. Write SIMILAR_KEYWORD relationships to Neo4j

The same pattern works for any text property on any node type.

Usage:
    python scripts/compute_keyword_similarity.py                    # Dry-run
    python scripts/compute_keyword_similarity.py --execute          # Execute
    python scripts/compute_keyword_similarity.py --validate         # Validate embeddings
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
    EMBEDDING_MODEL,
    create_embedding,
    create_embeddings_for_nodes,
    get_openai_client,
    suppress_http_logging,
)
from domain_status_graph.similarity import (
    compute_similarity_for_node_type,
    write_similarity_relationships,
)


def validate_keyword_embeddings(driver, database: str, logger) -> bool:
    """
    Validate that keyword embeddings capture semantic similarity.

    Picks a few domains with keywords and checks that similar keywords
    produce similar embeddings.
    """
    logger.info("")
    logger.info("=" * 70)
    logger.info("Validating Keyword Embeddings")
    logger.info("=" * 70)

    try:
        client = get_openai_client()
    except (ImportError, ValueError) as e:
        logger.error(f"Cannot validate: {e}")
        return False

    # Get sample keywords from the database
    with driver.session(database=database) as session:
        result = session.run(
            """
            MATCH (d:Domain)
            WHERE d.keywords IS NOT NULL AND d.keywords <> ''
            RETURN d.final_domain AS domain, d.keywords AS keywords
            LIMIT 10
            """
        )
        samples = [(r["domain"], r["keywords"]) for r in result]

    if len(samples) < 2:
        logger.warning("Not enough domains with keywords to validate")
        return False

    logger.info(f"Testing embeddings on {len(samples)} sample keyword sets...")

    # Create embeddings for samples
    embeddings = []
    for domain, keywords in samples:
        emb = create_embedding(client, keywords, EMBEDDING_MODEL)
        if emb:
            embeddings.append((domain, keywords[:50] + "...", emb))
            logger.info(f"  ✓ {domain}: {keywords[:50]}...")

    if len(embeddings) < 2:
        logger.error("Failed to create embeddings for samples")
        return False

    # Compute pairwise similarities
    import numpy as np

    logger.info("")
    logger.info("Pairwise similarities (should be higher for related domains):")
    logger.info("-" * 50)

    for i, (d1, _k1, e1) in enumerate(embeddings):
        for j, (d2, _k2, e2) in enumerate(embeddings):
            if i < j:
                # Cosine similarity
                sim = np.dot(e1, e2) / (np.linalg.norm(e1) * np.linalg.norm(e2))
                logger.info(f"  {d1} <-> {d2}: {sim:.3f}")

    logger.info("")
    logger.info("✓ Validation complete - review similarities above")
    logger.info("  High scores (>0.8) indicate semantic similarity")
    logger.info("  Low scores (<0.5) indicate different domains")
    return True


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Compute Domain keyword similarity using embeddings"
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Actually execute (default is dry-run)",
    )
    parser.add_argument(
        "--validate",
        action="store_true",
        help="Validate embeddings on sample data",
    )
    parser.add_argument(
        "--similarity-threshold",
        type=float,
        default=0.7,
        help="Minimum similarity score (default: 0.7)",
    )
    parser.add_argument(
        "--top-k",
        type=int,
        default=50,
        help="Max similar domains per domain (default: 50)",
    )
    args = parser.parse_args()

    logger = setup_logging("compute_keyword_similarity", execute=args.execute)
    suppress_http_logging()

    driver, database = get_driver_and_database(logger)

    if not verify_neo4j_connection(driver, database, logger):
        sys.exit(1)

    # Validation mode
    if args.validate:
        success = validate_keyword_embeddings(driver, database, logger)
        driver.close()
        sys.exit(0 if success else 1)

    # Check data
    with driver.session(database=database) as session:
        result = session.run(
            """
            MATCH (d:Domain)
            WHERE d.keywords IS NOT NULL AND d.keywords <> ''
            RETURN count(d) AS count
            """
        )
        keyword_count = result.single()["count"]

    logger.info("")
    logger.info("=" * 70)
    logger.info("Keyword Similarity Pipeline")
    logger.info("=" * 70)
    logger.info(f"Domains with keywords: {keyword_count}")
    logger.info(f"Similarity threshold: {args.similarity_threshold}")
    logger.info(f"Top-K per domain: {args.top_k}")

    if keyword_count == 0:
        logger.error("No domains with keywords found")
        driver.close()
        sys.exit(1)

    if not args.execute:
        logger.info("")
        logger.info("DRY RUN - Would perform:")
        logger.info(f"  1. Create keyword embeddings for {keyword_count} domains")
        logger.info("  2. Compute pairwise cosine similarity")
        logger.info("  3. Write SIMILAR_KEYWORD relationships")
        logger.info("")
        logger.info("Run with --execute to perform these operations")
        logger.info("Run with --validate to test embedding quality first")
        driver.close()
        return

    # Step 1: Create embeddings for keywords
    logger.info("")
    logger.info("Step 1: Creating keyword embeddings")
    logger.info("-" * 40)

    try:
        client = get_openai_client()
    except (ImportError, ValueError) as e:
        logger.error(str(e))
        driver.close()
        sys.exit(1)

    cache = get_cache()

    def create_fn(text, model):
        return create_embedding(client, text, model)

    processed, created, cached, failed = create_embeddings_for_nodes(
        driver=driver,
        cache=cache,
        node_label="Domain",
        text_property="keywords",
        key_property="final_domain",
        embedding_property="keyword_embedding",
        model_property="keyword_embedding_model",
        dimension_property="keyword_embedding_dimension",
        create_fn=create_fn,
        database=database,
        execute=True,
        log=logger,  # Pass logger for proper output
    )

    logger.info(f"Processed: {processed}, Created: {created}, Cached: {cached}, Failed: {failed}")

    # Step 2: Compute similarity
    logger.info("")
    logger.info("Step 2: Computing keyword similarity")
    logger.info("-" * 40)

    pairs = compute_similarity_for_node_type(
        driver=driver,
        node_label="Domain",
        key_property="final_domain",
        embedding_property="keyword_embedding",
        similarity_threshold=args.similarity_threshold,
        top_k=args.top_k,
        database=database,
        logger_instance=logger,
    )

    # Step 3: Write relationships
    logger.info("")
    logger.info("Step 3: Writing SIMILAR_KEYWORD relationships")
    logger.info("-" * 40)

    relationships = write_similarity_relationships(
        driver=driver,
        pairs=pairs,
        node_label="Domain",
        key_property="final_domain",
        relationship_type="SIMILAR_KEYWORD",
        database=database,
        logger_instance=logger,
    )

    # Summary
    logger.info("")
    logger.info("=" * 70)
    logger.info("Complete!")
    logger.info("=" * 70)
    logger.info(f"Embeddings created: {created}")
    logger.info(f"Embeddings from cache: {cached}")
    logger.info(f"SIMILAR_KEYWORD relationships: {relationships}")

    driver.close()


if __name__ == "__main__":
    main()
