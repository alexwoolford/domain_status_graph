#!/usr/bin/env python3
"""
Create embeddings and similarity relationships for company risk factors.

This script:
1. Loads risk factors from 10-K cache
2. Creates embeddings for risk factors
3. Stores embeddings on Company nodes
4. Creates SIMILAR_RISK relationships between companies with similar risk profiles

Usage:
    python scripts/create_risk_similarity_graph.py                    # Dry-run (plan only)
    python scripts/create_risk_similarity_graph.py --execute          # Actually create embeddings and relationships
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
    create_embeddings_for_nodes,
    get_openai_client,
    suppress_http_logging,
)


def main():
    """Run the risk factors similarity graph creation pipeline."""
    parser = argparse.ArgumentParser(
        description="Create embeddings and similarity relationships for company risk factors"
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Actually create embeddings and relationships (default is dry-run)",
    )

    args = parser.parse_args()

    logger = setup_logging("create_risk_similarity_graph", execute=args.execute)
    suppress_http_logging()

    driver, database = get_driver_and_database(logger)

    if not verify_neo4j_connection(driver, database, logger):
        sys.exit(1)

    cache = get_cache()

    # Step 1: Load risk factors from cache into Neo4j
    logger.info("=" * 80)
    logger.info("STEP 1: Load Risk Factors into Neo4j")
    logger.info("=" * 80)
    logger.info("")

    if not args.execute:
        logger.info("DRY RUN MODE")
        logger.info("Would load risk factors from cache into Company nodes")
        logger.info("")
        driver.close()
        return

    # Update Company nodes with risk factors from cache
    logger.info("Updating Company nodes with risk factors from cache...")
    with driver.session(database=database) as session:
        result = session.run("MATCH (c:Company) RETURN c.cik AS cik")
        companies = [record["cik"] for record in result]

        updated_count = 0
        for cik in companies:
            ten_k_data = cache.get("10k_extracted", cik)
            if ten_k_data and ten_k_data.get("risk_factors"):
                risk_text = ten_k_data["risk_factors"].strip()
                # Only update if risk factors are meaningful (not too short)
                if len(risk_text) >= 200:  # Use same minimum as descriptions
                    session.run(
                        """
                        MATCH (c:Company {cik: $cik})
                        SET c.risk_factors = $risk_text
                        """,
                        cik=cik,
                        risk_text=risk_text,
                    )
                    updated_count += 1

        logger.info(f"  Updated {updated_count} companies with risk factors")

    logger.info("")
    logger.info("✓ Step 1 complete: Risk factors loaded")
    logger.info("")

    # Step 2: Create embeddings for risk factors
    logger.info("=" * 80)
    logger.info("STEP 2: Create Embeddings for Risk Factors")
    logger.info("=" * 80)
    logger.info("")

    try:
        client = get_openai_client()
    except (ImportError, ValueError) as e:
        logger.error(f"Failed to get OpenAI client: {e}")
        logger.error("Set OPENAI_API_KEY environment variable")
        driver.close()
        sys.exit(1)

    # Create embeddings for companies with risk factors (using batch API for speed)
    logger.info("Creating embeddings for companies with risk factors...")
    processed, created, cached, failed = create_embeddings_for_nodes(
        driver=driver,
        cache=cache,
        node_label="Company",
        text_property="risk_factors",  # Use risk_factors property
        key_property="cik",
        embedding_property="risk_factors_embedding",  # Store as risk_factors_embedding
        openai_client=client,  # Use batch API for ~20x faster embedding
        database=database,
        execute=True,
        log=logger,  # Pass logger for proper output
    )

    logger.info(f"  Processed: {processed}, Created: {created}, Cached: {cached}, Failed: {failed}")
    logger.info("")
    logger.info("✓ Step 2 complete: Risk factor embeddings created")
    logger.info("")

    # Step 3: Create SIMILAR_RISK relationships between Companies
    logger.info("=" * 80)
    logger.info("STEP 3: Create SIMILAR_RISK Relationships Between Companies")
    logger.info("=" * 80)
    logger.info("")

    # Reuse the similarity computation function, but for risk factors
    # We'll need to create a similar function or adapt the existing one
    relationships_created = compute_company_risk_similarity(
        driver=driver,
        similarity_threshold=0.6,  # Same threshold as descriptions
        top_k=50,
        database=database,
        execute=True,
        logger=logger,
    )

    logger.info("")
    logger.info(f"✓ Step 3 complete: {relationships_created} SIMILAR_RISK relationships created")
    logger.info("")

    # Summary
    logger.info("=" * 80)
    logger.info("PIPELINE COMPLETE")
    logger.info("=" * 80)
    logger.info("")

    # Show final statistics
    with driver.session(database=database) as session:
        # Risk factors stats
        result = session.run(
            """
            MATCH (c:Company)
            RETURN
                count(c) AS total,
                sum(CASE WHEN c.risk_factors IS NOT NULL THEN 1 ELSE 0 END) AS with_risks,
                sum(CASE WHEN c.risk_factors_embedding IS NOT NULL THEN 1 ELSE 0 END) AS with_embedding,
                sum(CASE WHEN EXISTS((c)-[:SIMILAR_RISK]->()) THEN 1 ELSE 0 END) AS with_similar_rel
            """
        )
        row = result.single()
        logger.info("Risk Factors Statistics:")
        logger.info(f"  Total companies: {row['total']:,}")
        logger.info(f"  With risk factors: {row['with_risks']:,}")
        logger.info(f"  With embeddings: {row['with_embedding']:,}")
        logger.info(f"  With SIMILAR_RISK relationships: {row['with_similar_rel']:,}")

        result = session.run(
            """
            MATCH ()-[r:SIMILAR_RISK]->()
            WHERE startNode(r):Company
            RETURN count(r) AS count
            """
        )
        rel_count = result.single()["count"]
        logger.info(f"  Total Company SIMILAR_RISK relationships: {rel_count:,}")
        logger.info("")

    logger.info("=" * 80)
    logger.info("Next Steps:")
    logger.info("  1. Test similarity queries for risk factors")
    logger.info("  2. Compare risk similarity vs description similarity")
    logger.info("=" * 80)

    driver.close()


def compute_company_risk_similarity(
    driver,
    similarity_threshold: float = 0.6,
    top_k: int = 50,
    database: str = None,
    execute: bool = True,
    logger=None,
) -> int:
    """
    Compute similarity between companies based on risk factors.

    This is similar to compute_company_description_similarity but uses risk_factors_embedding.
    """
    import logging

    import numpy as np

    if logger is None:
        logger = logging.getLogger(__name__)

    if not execute:
        logger.info("   (DRY RUN - no changes will be made)")
        return 0

    from domain_status_graph.constants import MIN_DESCRIPTION_LENGTH_FOR_SIMILARITY

    relationships_written = 0

    try:
        # Delete existing relationships
        logger.info("   Deleting existing SIMILAR_RISK relationships (Company-Company)...")
        with driver.session(database=database) as session:
            result = session.run(
                """
                MATCH (c1:Company)-[r:SIMILAR_RISK]->(c2:Company)
                DELETE r
                RETURN count(r) AS deleted
                """
            )
            deleted = result.single()["deleted"]
            if deleted > 0:
                logger.info(f"   ✓ Deleted {deleted} existing relationships")
            else:
                logger.info("   ✓ No existing relationships to delete")

        with driver.session(database=database) as session:
            # Load companies with risk factor embeddings
            logger.info("   Loading Company nodes with risk factor embeddings...")
            result = session.run(
                """
                MATCH (c:Company)
                WHERE c.risk_factors_embedding IS NOT NULL
                  AND c.risk_factors IS NOT NULL
                  AND size(c.risk_factors) >= $min_length
                RETURN c.cik AS cik, c.risk_factors_embedding AS embedding
                """,
                min_length=MIN_DESCRIPTION_LENGTH_FOR_SIMILARITY,
            )

            companies = []
            for record in result:
                embedding = record["embedding"]
                if embedding and isinstance(embedding, list):
                    companies.append(
                        {
                            "cik": record["cik"],
                            "embedding": np.array(embedding, dtype=np.float32),
                        }
                    )

            logger.info(f"   Found {len(companies)} companies with risk factor embeddings")

            if len(companies) < 2:
                logger.warning("   ⚠ Not enough companies with risk factor embeddings")
                return 0

            # Compute pairwise cosine similarity
            logger.info("   Computing pairwise cosine similarity...")
            logger.info(f"   Threshold: {similarity_threshold}, Top-K: {top_k}")

            embeddings_matrix = np.array([c["embedding"] for c in companies])
            ciks = [c["cik"] for c in companies]

            # Normalize embeddings
            norms = np.linalg.norm(embeddings_matrix, axis=1, keepdims=True)
            norms[norms == 0] = 1
            embeddings_normalized = embeddings_matrix / norms

            # Compute similarity matrix
            similarity_matrix = np.dot(embeddings_normalized, embeddings_normalized.T)

            # Collect pairs above threshold
            logger.info("   Collecting similar pairs (top-k per company, above threshold)...")
            pairs = {}

            for i, _company in enumerate(companies):
                similarities = similarity_matrix[i].copy()
                similarities[i] = -1  # Exclude self

                top_indices = np.argsort(similarities)[::-1][:top_k]

                for j in top_indices:
                    similarity_score = float(similarities[j])
                    if similarity_score >= similarity_threshold:
                        cik1, cik2 = ciks[i], ciks[j]
                        if cik1 > cik2:
                            cik1, cik2 = cik2, cik1

                        pair_key = (cik1, cik2)
                        if pair_key not in pairs or similarity_score > pairs[pair_key]:
                            pairs[pair_key] = similarity_score

            logger.info(f"   Found {len(pairs)} unique similar pairs")

            # Write relationships
            logger.info("   Writing SIMILAR_RISK relationships...")
            batch = []

            for (cik1, cik2), score in pairs.items():
                batch.append({"cik1": cik1, "cik2": cik2, "score": score})

                if len(batch) >= 1000:
                    session.run(
                        """
                        UNWIND $batch AS rel
                        MATCH (c1:Company {cik: rel.cik1})
                        MATCH (c2:Company {cik: rel.cik2})
                        WHERE c1 <> c2
                        MERGE (c1)-[r:SIMILAR_RISK]->(c2)
                        SET r.score = rel.score,
                            r.metric = 'COSINE',
                            r.computed_at = datetime()
                        """,
                        batch=batch,
                    )
                    relationships_written += len(batch)
                    batch = []

            if batch:
                session.run(
                    """
                    UNWIND $batch AS rel
                    MATCH (c1:Company {cik: rel.cik1})
                    MATCH (c2:Company {cik: rel.cik2})
                    WHERE c1 <> c2
                    MERGE (c1)-[r:SIMILAR_RISK]->(c2)
                    SET r.score = rel.score,
                        r.metric = 'COSINE',
                        r.computed_at = datetime()
                    """,
                    batch=batch,
                )
                relationships_written += len(batch)

            logger.info(f"   ✓ Created {relationships_written} SIMILAR_RISK relationships")
            logger.info("   ✓ Complete")

    except Exception as e:
        logger.error(f"   ✗ Error: {e}")
        import traceback

        logger.error(traceback.format_exc())

    return relationships_written


if __name__ == "__main__":
    main()
