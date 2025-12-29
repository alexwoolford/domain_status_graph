"""
Company Description Similarity using cosine similarity on embeddings.

Find companies with similar descriptions using cosine similarity on embeddings.
Example: Companies in similar industries, with similar business models.
"""

import logging

from domain_status_graph.constants import (
    DEFAULT_SIMILARITY_THRESHOLD,
    DEFAULT_TOP_K,
    MIN_DESCRIPTION_LENGTH_FOR_SIMILARITY,
)
from domain_status_graph.similarity.cosine import find_top_k_similar_pairs

logger = logging.getLogger(__name__)


def compute_company_description_similarity(
    driver,
    similarity_threshold: float = DEFAULT_SIMILARITY_THRESHOLD,
    top_k: int = DEFAULT_TOP_K,
    database: str | None = None,
    execute: bool = True,
    logger: logging.Logger | None = None,
) -> int:
    """
    Company Description Similarity.

    Find companies with similar descriptions using cosine similarity on embeddings.

    Args:
        driver: Neo4j driver instance
        similarity_threshold: Minimum cosine similarity
        top_k: Max similar companies per company
        database: Neo4j database name
        execute: If False, only print plan
        logger: Optional logger instance

    Returns:
        Number of SIMILAR_DESCRIPTION relationships created
    """
    if logger is None:
        logger = logging.getLogger(__name__)

    if not execute:
        logger.info("")
        logger.info("=" * 70)
        logger.info("3. Company Description Similarity (Dry Run)")
        logger.info("=" * 70)
        logger.info("   Use case: Find companies with similar business descriptions")
        logger.info("   Relationship: Company-[SIMILAR_DESCRIPTION {score}]->Company")
        logger.info("   Algorithm: Cosine similarity on description embeddings")
        return 0

    logger.info("")
    logger.info("=" * 70)
    logger.info("3. Company Description Similarity")
    logger.info("=" * 70)
    logger.info("   Use case: Find companies with similar business descriptions")
    logger.info("   Relationship: Company-[SIMILAR_DESCRIPTION {score}]->Company")
    logger.info("   Algorithm: Cosine similarity on description embeddings")

    relationships_written = 0

    try:
        # Delete existing relationships
        logger.info("   Deleting existing SIMILAR_DESCRIPTION relationships (Company-Company)...")
        with driver.session(database=database) as session:
            result = session.run(
                """
                MATCH (c1:Company)-[r:SIMILAR_DESCRIPTION]->(c2:Company)
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
            # Load companies with embeddings and meaningful descriptions
            logger.info("   Loading Company nodes with embeddings...")
            logger.info(
                f"   Filtering out descriptions < {MIN_DESCRIPTION_LENGTH_FOR_SIMILARITY} characters"
            )
            result = session.run(
                """
                MATCH (c:Company)
                WHERE c.description_embedding IS NOT NULL
                  AND c.description IS NOT NULL
                  AND size(c.description) >= $min_length
                RETURN c.cik AS cik, c.description_embedding AS embedding
                """,
                min_length=MIN_DESCRIPTION_LENGTH_FOR_SIMILARITY,
            )

            ciks = []
            embeddings = []
            for record in result:
                embedding = record["embedding"]
                if embedding and isinstance(embedding, list):
                    ciks.append(record["cik"])
                    embeddings.append(embedding)

            logger.info(f"   Found {len(ciks)} companies with embeddings")

            if len(ciks) < 2:
                logger.warning("   ⚠ Not enough companies with embeddings")
                return 0

            # Compute pairwise cosine similarity using shared utility
            logger.info("   Computing pairwise cosine similarity...")
            logger.info(f"   Threshold: {similarity_threshold}, Top-K: {top_k}")

            pairs = find_top_k_similar_pairs(
                keys=ciks,
                embeddings=embeddings,
                similarity_threshold=similarity_threshold,
                top_k=top_k,
            )

            logger.info(f"   Found {len(pairs)} unique similar pairs")

            # Write relationships
            logger.info("   Writing SIMILAR_DESCRIPTION relationships...")
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
                        MERGE (c1)-[r:SIMILAR_DESCRIPTION]->(c2)
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
                    MERGE (c1)-[r:SIMILAR_DESCRIPTION]->(c2)
                    SET r.score = rel.score,
                        r.metric = 'COSINE',
                        r.computed_at = datetime()
                    """,
                    batch=batch,
                )
                relationships_written += len(batch)

            logger.info(f"   ✓ Created {relationships_written} SIMILAR_DESCRIPTION relationships")
            logger.info("   ✓ Complete")

    except Exception as e:
        logger.error(f"   ✗ Error: {e}")
        import traceback

        logger.error(traceback.format_exc())

    return relationships_written
