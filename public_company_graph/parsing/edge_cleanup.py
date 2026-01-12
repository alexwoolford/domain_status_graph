"""
Systemic edge cleanup using tiered confidence system.

This module provides functions to clean up existing edges by applying
the same tiered confidence logic used in the extraction pipeline.

The cleanup ensures:
1. Edges below high threshold are converted to candidates
2. Edges below medium threshold are deleted
3. All fact edges meet high confidence requirements
4. Process is idempotent and repeatable
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from public_company_graph.parsing.relationship_config import (
    RELATIONSHIP_CONFIGS,
    ConfidenceTier,
    get_confidence_tier,
)

if TYPE_CHECKING:
    from neo4j import Driver

logger = logging.getLogger(__name__)


def cleanup_relationship_edges(
    driver: Driver,
    database: str,
    relationship_types: list[str] | None = None,
    dry_run: bool = True,
) -> dict[str, dict[str, int]]:
    """
    Systematically clean up relationship edges using tiered confidence system.

    This applies the same logic as the extraction pipeline:
    - HIGH confidence (≥high_threshold) → Keep as fact
    - MEDIUM confidence (≥medium_threshold, <high_threshold) → Convert to candidate
    - LOW confidence (<medium_threshold) → Delete

    Args:
        driver: Neo4j driver
        database: Database name
        relationship_types: List of relationship types to clean (None = all configured types)
        dry_run: If True, only report what would be done

    Returns:
        Dict with statistics: {rel_type: {"converted": N, "deleted": N, "kept": N}}
    """
    if relationship_types is None:
        relationship_types = list(RELATIONSHIP_CONFIGS.keys())

    stats: dict[str, dict[str, int]] = {}

    with driver.session(database=database) as session:
        for rel_type in relationship_types:
            config = RELATIONSHIP_CONFIGS.get(rel_type)
            if not config or not config.enabled:
                logger.warning(f"Skipping {rel_type} - not configured or disabled")
                continue

            logger.info(f"Cleaning {rel_type} edges...")

            # Get all edges of this type
            query = f"""
            MATCH (source:Company)-[r:{rel_type}]->(target:Company)
            RETURN
                source.cik AS source_cik,
                target.cik AS target_cik,
                r.embedding_similarity AS embedding_similarity,
                r.confidence AS confidence,
                r.raw_mention AS raw_mention,
                r.context AS context,
                r.confidence_tier AS confidence_tier,
                elementId(r) AS edge_id
            """

            result = session.run(query)
            edges = list(result)

            if not edges:
                logger.info(f"  No {rel_type} edges found")
                stats[rel_type] = {"converted": 0, "deleted": 0, "kept": 0}
                continue

            # Categorize edges
            to_keep: list[dict] = []
            to_convert: list[dict] = []
            to_delete: list[dict] = []

            for edge in edges:
                embedding_sim = edge["embedding_similarity"]

                # Special handling: edges without embeddings should be deleted
                # (incomplete/old data, not candidates)
                if embedding_sim is None:
                    to_delete.append(edge)
                    continue

                tier = get_confidence_tier(rel_type, embedding_sim)

                if tier == ConfidenceTier.HIGH:
                    to_keep.append(edge)
                elif tier == ConfidenceTier.MEDIUM:
                    to_convert.append(edge)
                else:  # LOW
                    to_delete.append(edge)

            logger.info(
                f"  {rel_type}: {len(to_keep)} keep, {len(to_convert)} convert, {len(to_delete)} delete"
            )

            if dry_run:
                stats[rel_type] = {
                    "kept": len(to_keep),
                    "converted": len(to_convert),
                    "deleted": len(to_delete),
                }
                continue

            # Convert medium-confidence edges to candidates
            if to_convert:
                convert_batch = []
                for edge in to_convert:
                    convert_batch.append(
                        {
                            "source_cik": edge["source_cik"],
                            "target_cik": edge["target_cik"],
                            "embedding_similarity": edge["embedding_similarity"],
                            "confidence": edge["confidence"],
                            "raw_mention": edge["raw_mention"],
                            "context": edge["context"],
                            "edge_id": edge["edge_id"],
                        }
                    )

                # Create candidate edges
                create_candidate_query = f"""
                UNWIND $batch AS edge
                MATCH (source:Company {{cik: edge.source_cik}})
                MATCH (target:Company {{cik: edge.target_cik}})
                MERGE (source)-[r:{config.candidate_type}]->(target)
                SET r.embedding_similarity = edge.embedding_similarity,
                    r.confidence = edge.confidence,
                    r.raw_mention = edge.raw_mention,
                    r.context = edge.context,
                    r.confidence_tier = 'medium',
                    r.converted_from = '{rel_type}',
                    r.converted_at = datetime(),
                    r.source = 'ten_k_filing'
                """

                session.run(create_candidate_query, batch=convert_batch)

                # Delete old fact edges
                delete_fact_query = f"""
                UNWIND $batch AS edge
                MATCH (source:Company {{cik: edge.source_cik}})
                  -[r:{rel_type}]->(target:Company {{cik: edge.target_cik}})
                WHERE elementId(r) = edge.edge_id
                DELETE r
                """

                session.run(delete_fact_query, batch=convert_batch)
                logger.info(f"  ✓ Converted {len(to_convert)} edges to {config.candidate_type}")

            # Delete low-confidence edges
            if to_delete:
                delete_batch = [{"edge_id": edge["edge_id"]} for edge in to_delete]

                delete_query = f"""
                UNWIND $batch AS edge
                MATCH ()-[r:{rel_type}]->()
                WHERE elementId(r) = edge.edge_id
                DELETE r
                """

                session.run(delete_query, batch=delete_batch)
                logger.info(f"  ✓ Deleted {len(to_delete)} low-confidence edges")

            stats[rel_type] = {
                "kept": len(to_keep),
                "converted": len(to_convert),
                "deleted": len(to_delete),
            }

    return stats


def ensure_edge_quality(
    driver: Driver,
    database: str,
    relationship_types: list[str] | None = None,
    dry_run: bool = True,
) -> dict[str, dict[str, int]]:
    """
    Ensure all relationship edges meet quality thresholds.

    This is a wrapper around cleanup_relationship_edges that can be called
    as part of the pipeline to ensure data quality.

    Args:
        driver: Neo4j driver
        database: Database name
        relationship_types: List of relationship types to check (None = all)
        dry_run: If True, only report what would be done

    Returns:
        Dict with cleanup statistics
    """
    logger.info("Ensuring edge quality using tiered confidence system...")
    return cleanup_relationship_edges(
        driver=driver,
        database=database,
        relationship_types=relationship_types,
        dry_run=dry_run,
    )
