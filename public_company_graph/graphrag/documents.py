"""
Document and Chunk node creation and management for GraphRAG.

Creates Document nodes (representing filings) and Chunk nodes (representing text pieces)
with proper GraphRAG relationships:
- (Company)-[:HAS]->(Document)
- (Chunk)-[:PART_OF_DOCUMENT]->(Document)
- (Chunk)-[:NEXT_CHUNK]->(Chunk)
"""

import logging
from collections import defaultdict
from typing import Any

from neo4j import Driver

from public_company_graph.graphrag.chunking import DocumentChunk
from public_company_graph.neo4j.utils import safe_single

logger = logging.getLogger(__name__)


def create_documents_and_chunks(
    driver: Driver,
    chunks: list[DocumentChunk],
    database: str | None = None,
    batch_size: int = 1000,
    execute: bool = False,
) -> tuple[int, int]:
    """
    Create Document nodes (filings) and Chunk nodes from chunks in Neo4j.

    Groups chunks by filing (company_cik + section_type + filing_year) to create
    one Document per filing, then creates Chunk nodes for each text piece.

    Args:
        driver: Neo4j driver
        chunks: List of DocumentChunk objects
        database: Neo4j database name
        batch_size: Batch size for UNWIND operations
        execute: If False, only print plan

    Returns:
        Tuple of (documents_created, chunks_created)
    """
    if not chunks:
        return (0, 0)

    if not execute:
        # Group chunks to count documents
        grouped = defaultdict(list)
        for chunk in chunks:
            doc_key = (chunk.company_cik, chunk.section_type, chunk.filing_year)
            grouped[doc_key].append(chunk)
        logger.info(
            f"[DRY RUN] Would create {len(grouped)} Document nodes and {len(chunks)} Chunk nodes"
        )
        return (0, 0)

    logger.info(f"Creating Document and Chunk nodes from {len(chunks)} chunks...")

    # Group chunks by filing (company_cik + section_type + filing_year)
    # This determines which chunks belong to the same Document
    chunks_by_document: dict[tuple[str, str, int | None], list[DocumentChunk]] = defaultdict(list)
    for chunk in chunks:
        doc_key = (chunk.company_cik, chunk.section_type, chunk.filing_year)
        chunks_by_document[doc_key].append(chunk)

    # Sort chunks within each document by chunk_index to maintain order
    for doc_key in chunks_by_document:
        chunks_by_document[doc_key].sort(key=lambda c: c.chunk_index)

    logger.info(f"Grouped into {len(chunks_by_document)} documents (filings)")

    documents_created = 0
    chunks_created = 0

    with driver.session(database=database) as session:
        # Create Document nodes (one per filing)
        documents = []
        for (company_cik, section_type, filing_year), doc_chunks in chunks_by_document.items():
            # Deterministic document ID: company_cik + section_type + filing_year
            doc_id = f"{company_cik}_{section_type}_{filing_year or 'unknown'}"

            # Use first chunk's metadata for document-level info
            first_chunk = doc_chunks[0]

            documents.append(
                {
                    "doc_id": doc_id,
                    "company_cik": company_cik,
                    "company_ticker": first_chunk.company_ticker,
                    "company_name": first_chunk.company_name,
                    "section_type": section_type,
                    "filing_year": filing_year,
                    "chunk_count": len(doc_chunks),
                }
            )

        # Batch create Document nodes
        for i in range(0, len(documents), batch_size):
            batch = documents[i : i + batch_size]

            query = """
            UNWIND $documents AS doc
            MERGE (d:Document {doc_id: doc.doc_id})
            SET d.company_cik = doc.company_cik,
                d.company_ticker = doc.company_ticker,
                d.company_name = doc.company_name,
                d.section_type = doc.section_type,
                d.filing_year = doc.filing_year,
                d.chunk_count = doc.chunk_count,
                d.created_at = datetime()
            RETURN count(d) as created
            """

            result = session.run(query, documents=batch)
            batch_created = safe_single(result, default=0, key="created")
            if batch_created:
                documents_created += batch_created

        logger.info(f"✓ Created {documents_created} Document nodes")

        # Create Chunk nodes and relationships
        all_chunk_data = []
        all_relationships = []

        for (company_cik, section_type, filing_year), doc_chunks in chunks_by_document.items():
            doc_id = f"{company_cik}_{section_type}_{filing_year or 'unknown'}"

            for idx, chunk in enumerate(doc_chunks):
                # Deterministic chunk ID: doc_id + chunk_index
                chunk_id = f"{doc_id}_chunk_{chunk.chunk_index}"

                # Serialize metadata as JSON string
                import json

                metadata_str = json.dumps(chunk.metadata or {}) if chunk.metadata else "{}"

                all_chunk_data.append(
                    {
                        "chunk_id": chunk_id,
                        "doc_id": doc_id,
                        "text": chunk.text,
                        "chunk_index": chunk.chunk_index,
                        "metadata": metadata_str,
                    }
                )

                # Track relationships to create later
                all_relationships.append(
                    {
                        "chunk_id": chunk_id,
                        "doc_id": doc_id,
                        "next_chunk_id": f"{doc_id}_chunk_{doc_chunks[idx + 1].chunk_index}"
                        if idx + 1 < len(doc_chunks)
                        else None,
                    }
                )

        # Batch create Chunk nodes
        for i in range(0, len(all_chunk_data), batch_size):
            batch = all_chunk_data[i : i + batch_size]

            query = """
            UNWIND $chunks AS chunk
            MERGE (c:Chunk {chunk_id: chunk.chunk_id})
            SET c.text = chunk.text,
                c.chunk_index = chunk.chunk_index,
                c.metadata = chunk.metadata,
                c.created_at = datetime()
            RETURN count(c) as created
            """

            result = session.run(query, chunks=batch)
            batch_created = safe_single(result, default=0, key="created")
            if batch_created:
                chunks_created += batch_created

            if (i + batch_size) % (batch_size * 10) == 0:
                logger.info(f"  Created {chunks_created}/{len(all_chunk_data)} chunks...")

        logger.info(f"✓ Created {chunks_created} Chunk nodes")

        # Create relationships: (Chunk)-[:PART_OF_DOCUMENT]->(Document)
        logger.info("Creating Chunk-Document relationships...")
        for i in range(0, len(all_relationships), batch_size):
            batch = all_relationships[i : i + batch_size]

            query = """
            UNWIND $rels AS rel
            MATCH (c:Chunk {chunk_id: rel.chunk_id})
            MATCH (d:Document {doc_id: rel.doc_id})
            MERGE (c)-[:PART_OF_DOCUMENT]->(d)
            RETURN count(*) as created
            """

            result = session.run(query, rels=batch)
            # Don't count, just ensure they're created

        logger.info("✓ Created Chunk-Document relationships")

        # Create relationships: (Chunk)-[:NEXT_CHUNK]->(Chunk)
        logger.info("Creating Chunk-Chunk NEXT_CHUNK relationships...")
        next_chunk_rels = [r for r in all_relationships if r["next_chunk_id"] is not None]

        for i in range(0, len(next_chunk_rels), batch_size):
            batch = next_chunk_rels[i : i + batch_size]

            query = """
            UNWIND $rels AS rel
            MATCH (c1:Chunk {chunk_id: rel.chunk_id})
            MATCH (c2:Chunk {chunk_id: rel.next_chunk_id})
            MERGE (c1)-[:NEXT_CHUNK]->(c2)
            RETURN count(*) as created
            """

            result = session.run(query, rels=batch)

        logger.info(f"✓ Created {len(next_chunk_rels)} NEXT_CHUNK relationships")

    return (documents_created, chunks_created)


def link_documents_to_companies(
    driver: Driver,
    database: str | None = None,
    execute: bool = False,
) -> int:
    """
    Link Document nodes to Company nodes via HAS relationship.

    Args:
        driver: Neo4j driver
        database: Neo4j database name
        execute: If False, only print plan

    Returns:
        Number of relationships created
    """
    if not execute:
        logger.info("[DRY RUN] Would link Document nodes to Company nodes")
        return 0

    logger.info("Linking Document nodes to Company nodes...")

    query = """
    MATCH (d:Document)
    MATCH (c:Company {cik: d.company_cik})
    MERGE (c)-[:HAS]->(d)
    RETURN count(*) as linked
    """

    with driver.session(database=database) as session:
        result = session.run(query)
        linked = safe_single(result, default=0, key="linked")

    logger.info(f"✓ Linked {linked} Company-Document relationships")
    return linked


def create_chunk_embeddings(
    driver: Driver,
    cache: Any,
    openai_client: Any,
    database: str | None = None,
    batch_size: int = 100,
    execute: bool = False,
    log: logging.Logger | None = None,
) -> tuple[int, int, int]:
    """
    Create embeddings for Chunk nodes that don't have them.

    Uses existing embedding infrastructure for consistency.

    Args:
        driver: Neo4j driver
        cache: AppCache instance
        openai_client: OpenAI client
        database: Neo4j database name
        batch_size: Batch size for processing
        execute: If False, only print plan
        log: Logger instance

    Returns:
        Tuple of (processed, created, cached) counts
    """
    _log = log if log is not None else logger

    if not execute:
        _log.info("[DRY RUN] Would create embeddings for Chunk nodes")
        return (0, 0, 0)

    from public_company_graph.embeddings.create import create_embeddings_for_nodes

    processed, created, cached, failed = create_embeddings_for_nodes(
        driver=driver,
        cache=cache,
        node_label="Chunk",  # Will need to add to ALLOWED_NODE_LABELS
        text_property="text",
        key_property="chunk_id",
        embedding_property="embedding",
        model_property="embedding_model",
        dimension_property="embedding_dimension",
        embedding_model="text-embedding-3-small",
        embedding_dimension=1536,
        openai_client=openai_client,
        database=database,
        execute=execute,
        log=_log,
    )

    return (processed, created, cached)


# Deprecated functions removed - use create_documents_and_chunks() and create_chunk_embeddings() instead
