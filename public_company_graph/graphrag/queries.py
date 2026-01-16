"""
GraphRAG query interface for semantic search and Q&A.

Combines vector search with graph traversal for comprehensive retrieval.
Searches Chunk nodes (which have text and embeddings) and traverses to
Document nodes (filings) and Company nodes for context.
"""

import logging
import time
from typing import Any

from neo4j import Driver

logger = logging.getLogger(__name__)


def _cosine_similarity(a: list[float], b: list[float]) -> float:
    """Compute cosine similarity between two vectors."""
    import numpy as np

    a_arr = np.array(a)
    b_arr = np.array(b)
    dot_product = np.dot(a_arr, b_arr)
    norm_a = np.linalg.norm(a_arr)
    norm_b = np.linalg.norm(b_arr)
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return float(dot_product / (norm_a * norm_b))


def _check_vector_index_online(
    driver: Driver, index_name: str, database: str | None = None, max_wait_seconds: int = 30
) -> bool:
    """
    Check if a vector index exists and is online.

    Args:
        driver: Neo4j driver
        index_name: Name of the vector index
        database: Neo4j database name
        max_wait_seconds: Maximum time to wait for index to come online

    Returns:
        True if index is online, False otherwise
    """
    start_time = time.time()
    while time.time() - start_time < max_wait_seconds:
        try:
            with driver.session(database=database) as session:
                result = session.run(
                    "SHOW VECTOR INDEXES YIELD name, state WHERE name = $name RETURN state",
                    name=index_name,
                )
                record = result.single()
                if record and record["state"] == "ONLINE":
                    return True
                elif record and record["state"] in ("POPULATING", "BUILDING"):
                    logger.debug(f"Index {index_name} is {record['state']}, waiting...")
                    time.sleep(2)
                    continue
                else:
                    # Index doesn't exist
                    return False
        except Exception as e:
            logger.debug(f"Error checking index status: {e}")
            return False

    return False


def search_documents(
    driver: Driver,
    query_text: str,
    query_embedding: list[float],
    limit: int = 10,
    database: str | None = None,
    min_similarity: float = 0.5,
) -> list[dict[str, Any]]:
    """
    Semantic search over Chunk nodes using Neo4j vector index (fast!).

    Uses db.index.vector.queryNodes() for approximate nearest neighbor search if available.
    Falls back to Python-based similarity if index is not online.

    Args:
        driver: Neo4j driver
        query_text: Query text (for logging)
        query_embedding: Query embedding vector
        limit: Maximum number of results
        database: Neo4j database name
        min_similarity: Minimum similarity threshold (default: 0.5)

    Returns:
        List of chunk results with similarity scores and document/company metadata
    """
    # Check if vector index is available
    index_name = "chunk_embedding_vector"
    use_vector_index = _check_vector_index_online(driver, index_name, database, max_wait_seconds=5)

    if use_vector_index:
        # Fast path: Use Neo4j vector index
        query_limit = limit * 3  # Get 3x to filter by min_similarity

        query = """
        CALL db.index.vector.queryNodes($index_name, $limit, $query_embedding)
        YIELD node AS chunk, score
        WHERE score >= $min_similarity
        OPTIONAL MATCH (chunk)-[:PART_OF_DOCUMENT]->(doc:Document)
        RETURN chunk.chunk_id AS chunk_id,
               chunk.text AS text,
               chunk.chunk_index AS chunk_index,
               chunk.metadata AS metadata,
               score AS similarity,
               doc.doc_id AS doc_id,
               doc.section_type AS section_type,
               doc.company_cik AS company_cik,
               doc.company_ticker AS company_ticker,
               doc.company_name AS company_name,
               doc.filing_year AS filing_year
        ORDER BY score DESC
        LIMIT $final_limit
        """

        results = []
        try:
            with driver.session(database=database) as session:
                result = session.run(
                    query,
                    index_name=index_name,
                    query_embedding=query_embedding,
                    limit=query_limit,
                    min_similarity=min_similarity,
                    final_limit=limit,
                )
                for record in result:
                    chunk_dict = dict(record)
                    results.append(chunk_dict)
            return results
        except Exception as e:
            logger.warning(f"Vector index query failed, falling back to Python similarity: {e}")
            # Fall through to Python-based search

    # Fallback: Python-based similarity (slower but works without index)
    logger.debug("Using Python-based similarity search (vector index not available)")
    query = """
    MATCH (chunk:Chunk)
    WHERE chunk.embedding IS NOT NULL
    OPTIONAL MATCH (chunk)-[:PART_OF_DOCUMENT]->(doc:Document)
    RETURN chunk.chunk_id AS chunk_id,
           chunk.text AS text,
           chunk.chunk_index AS chunk_index,
           chunk.metadata AS metadata,
           chunk.embedding AS embedding,
           doc.doc_id AS doc_id,
           doc.section_type AS section_type,
           doc.company_cik AS company_cik,
           doc.company_ticker AS company_ticker,
           doc.company_name AS company_name,
           doc.filing_year AS filing_year
    LIMIT 10000
    """

    results = []
    with driver.session(database=database) as session:
        result = session.run(query)
        for record in result:
            chunk_embedding = record["embedding"]
            if chunk_embedding:
                similarity = _cosine_similarity(query_embedding, chunk_embedding)
                if similarity >= min_similarity:
                    chunk_dict = dict(record)
                    chunk_dict["similarity"] = similarity
                    del chunk_dict["embedding"]  # Remove embedding from result
                    results.append(chunk_dict)

    # Sort by similarity and limit
    results.sort(key=lambda x: x["similarity"], reverse=True)
    return results[:limit]


def search_with_graph_context(
    driver: Driver,
    query_text: str,
    query_embedding: list[float],
    company_ticker: str | None = None,
    limit: int = 10,
    database: str | None = None,
    min_similarity: float = 0.5,
) -> list[dict[str, Any]]:
    """
    Semantic search with graph context (e.g., from a specific company or related companies).

    Combines vector search with graph traversal to find relevant chunks
    from the queried company or related companies (competitors, partners, etc.).

    Args:
        driver: Neo4j driver
        query_text: Query text
        query_embedding: Query embedding vector
        company_ticker: Optional company ticker to focus search
        limit: Maximum number of results
        database: Neo4j database name
        min_similarity: Minimum similarity threshold

    Returns:
        List of chunk results with similarity scores and graph context
    """
    if company_ticker:
        # Search within company's chunks and related companies' chunks
        # Note: Using vector index with company filter is more complex, so we use Python fallback
        """
        MATCH (c:Company {ticker: $company_ticker})
        OPTIONAL MATCH (c)-[:HAS]->(doc:Document)<-[:PART_OF_DOCUMENT]-(chunk:Chunk)
        OPTIONAL MATCH (c)-[:HAS_COMPETITOR|HAS_PARTNER|SIMILAR_DESCRIPTION]-(related:Company)
        OPTIONAL MATCH (related)-[:HAS]->(related_doc:Document)<-[:PART_OF_DOCUMENT]-(related_chunk:Chunk)
        WITH collect(DISTINCT chunk) + collect(DISTINCT related_chunk) AS all_chunks
        UNWIND all_chunks AS chunk
        WHERE chunk IS NOT NULL AND chunk.embedding IS NOT NULL
        OPTIONAL MATCH (chunk)-[:PART_OF_DOCUMENT]->(doc:Document)
        OPTIONAL MATCH (doc)<-[:HAS]-(company:Company)
        RETURN chunk.chunk_id AS chunk_id,
               chunk.text AS text,
               chunk.chunk_index AS chunk_index,
               chunk.metadata AS metadata,
               chunk.embedding AS embedding,
               doc.doc_id AS doc_id,
               doc.section_type AS section_type,
               company.cik AS company_cik,
               company.ticker AS company_ticker,
               company.name AS company_name,
               doc.filing_year AS filing_year
        """
    else:
        # Global search
        return search_documents(
            driver, query_text, query_embedding, limit, database, min_similarity
        )

    # For company-specific search, we still need to filter by company first
    # Then use vector search on the filtered chunks
    # Note: We fetch more chunks initially to account for filtering

    # First, get chunks for the company and related companies
    company_query = """
    MATCH (c:Company {ticker: $company_ticker})
    OPTIONAL MATCH (c)-[:HAS]->(doc:Document)<-[:PART_OF_DOCUMENT]-(chunk:Chunk)
    WHERE chunk.embedding IS NOT NULL
    OPTIONAL MATCH (c)-[:HAS_COMPETITOR|HAS_PARTNER|SIMILAR_DESCRIPTION]-(related:Company)
    OPTIONAL MATCH (related)-[:HAS]->(related_doc:Document)<-[:PART_OF_DOCUMENT]-(related_chunk:Chunk)
    WHERE related_chunk.embedding IS NOT NULL
    WITH collect(DISTINCT chunk) + collect(DISTINCT related_chunk) AS all_chunks
    UNWIND all_chunks AS chunk
    WHERE chunk IS NOT NULL
    RETURN DISTINCT chunk.chunk_id AS chunk_id, chunk.embedding AS embedding
    LIMIT 10000
    """

    # Get candidate chunks
    candidate_chunk_ids = []
    candidate_embeddings = []
    with driver.session(database=database) as session:
        result = session.run(company_query, company_ticker=company_ticker)
        for record in result:
            if record["chunk_id"] and record["embedding"]:
                candidate_chunk_ids.append(record["chunk_id"])
                candidate_embeddings.append(record["embedding"])

    if not candidate_chunk_ids:
        return []

    # Compute similarity for candidate chunks (much smaller set)
    results = []
    for chunk_id, chunk_embedding in zip(candidate_chunk_ids, candidate_embeddings, strict=True):
        similarity = _cosine_similarity(query_embedding, chunk_embedding)
        if similarity >= min_similarity:
            results.append(
                {
                    "chunk_id": chunk_id,
                    "similarity": similarity,
                }
            )

    # Sort and get top results, then fetch full chunk data
    results.sort(key=lambda x: x["similarity"], reverse=True)
    top_chunk_ids = [r["chunk_id"] for r in results[:limit]]

    if not top_chunk_ids:
        return []

    # Fetch full chunk data with document/company info
    fetch_query = """
    MATCH (chunk:Chunk)
    WHERE chunk.chunk_id IN $chunk_ids
    OPTIONAL MATCH (chunk)-[:PART_OF_DOCUMENT]->(doc:Document)
    RETURN chunk.chunk_id AS chunk_id,
           chunk.text AS text,
           chunk.chunk_index AS chunk_index,
           chunk.metadata AS metadata,
           doc.doc_id AS doc_id,
           doc.section_type AS section_type,
           doc.company_cik AS company_cik,
           doc.company_ticker AS company_ticker,
           doc.company_name AS company_name,
           doc.filing_year AS filing_year
    """

    final_results = []
    similarity_map = {r["chunk_id"]: r["similarity"] for r in results[:limit]}

    with driver.session(database=database) as session:
        result = session.run(fetch_query, chunk_ids=top_chunk_ids)
        for record in result:
            chunk_dict = dict(record)
            chunk_dict["similarity"] = similarity_map.get(record["chunk_id"], 0.0)
            final_results.append(chunk_dict)

    # Sort by similarity (maintain order from similarity_map)
    final_results.sort(key=lambda x: x["similarity"], reverse=True)
    return final_results


def answer_question(
    driver: Driver,
    question: str,
    question_embedding: list[float],
    company_ticker: str | None = None,
    max_documents: int = 5,
    database: str | None = None,
    use_graph_traversal: bool = True,
    max_hops: int = 2,
) -> dict[str, Any]:
    """
    Answer a question using GraphRAG with multi-hop graph traversal.

    This is TRUE GraphRAG - not just vector search:
    1. Vector search finds initial relevant chunks
    2. Extracts companies from those chunks
    3. Traverses graph relationships (HAS_COMPETITOR, HAS_PARTNER, SIMILAR_DESCRIPTION, etc.)
    4. Retrieves chunks from related companies
    5. Combines all context for comprehensive answer

    Args:
        driver: Neo4j driver
        question: Question text
        question_embedding: Question embedding vector
        company_ticker: Optional company ticker to focus search
        max_documents: Maximum chunks to retrieve
        database: Neo4j database name
        use_graph_traversal: If True, use multi-hop graph traversal (default: True)
        max_hops: Maximum graph traversal depth (default: 2)

    Returns:
        Dictionary with:
        - chunks: List of relevant chunks (from vector search + graph traversal)
        - context: Combined context from chunks
        - companies: List of companies mentioned in chunks
        - related_companies: Companies found via graph traversal
        - traversal_paths: Graph paths showing how companies are related
    """
    # Step 1: Vector search to find initial relevant chunks
    initial_chunks = search_documents(
        driver,
        question,
        question_embedding,
        limit=max_documents * 2,  # Get more for graph expansion
        database=database,
        min_similarity=0.5,
    )

    if not initial_chunks:
        return {
            "question": question,
            "chunks": [],
            "context": "",
            "companies": [],
            "related_companies": [],
            "traversal_paths": [],
            "num_chunks": 0,
        }

    # Step 2: Extract companies from initial chunks
    initial_companies = set()
    for chunk in initial_chunks:
        if chunk.get("company_ticker"):
            initial_companies.add(chunk["company_ticker"])

    logger.info(
        f"Found {len(initial_companies)} companies in initial chunks: {list(initial_companies)[:10]}"
    )

    all_chunks = list(initial_chunks)
    related_companies = set()
    traversal_paths = []

    if use_graph_traversal and initial_companies:
        # Step 3: Enhanced graph traversal to find related companies with relationship context
        # This finds:
        # 1. Direct relationships (competitors, partners, suppliers, customers)
        # 2. Indirect relationships (suppliers to companies with exposure, etc.)
        # 3. Similar companies (similar description, risk, industry)

        company_list = list(initial_companies)

        # Enhanced traversal query that captures relationship paths and context
        traversal_query = f"""
        MATCH path = (start:Company)-[rels*1..{max_hops}]-(related:Company)
        WHERE start.ticker IN $company_tickers
          AND related.ticker <> start.ticker
        WITH start, related, path, rels,
             [r IN rels WHERE type(r) IN ['HAS_COMPETITOR', 'HAS_PARTNER', 'HAS_SUPPLIER', 'HAS_CUSTOMER',
                                         'SIMILAR_DESCRIPTION', 'SIMILAR_RISK', 'SIMILAR_INDUSTRY'] | type(r)] AS rel_types
        WHERE size(rel_types) > 0
        WITH start, related, path, rels, rel_types,
             // Get the first relationship type and the connecting company (if multi-hop)
             rel_types[0] AS primary_rel_type,
             CASE WHEN length(path) > 1 THEN [n IN nodes(path)[1..-1] | n.ticker][0] ELSE null END AS via_company
        RETURN DISTINCT
               start.ticker AS source_ticker,
               start.name AS source_name,
               related.ticker AS ticker,
               related.name AS name,
               primary_rel_type AS relationship_type,
               length(path) AS hop_distance,
               via_company
        ORDER BY hop_distance,
                 // Prioritize supplier/customer relationships (more impactful for geo risk)
                 CASE primary_rel_type
                   WHEN 'HAS_SUPPLIER' THEN 1
                   WHEN 'HAS_CUSTOMER' THEN 2
                   WHEN 'HAS_PARTNER' THEN 3
                   WHEN 'HAS_COMPETITOR' THEN 4
                   WHEN 'SIMILAR_DESCRIPTION' THEN 5
                   WHEN 'SIMILAR_RISK' THEN 6
                   ELSE 7
                 END
        LIMIT 100
        """

        # Map to track which initial company each related company is connected to
        company_relationship_map: dict[str, list[dict]] = {}  # ticker -> list of relationship info

        with driver.session(database=database) as session:
            result = session.run(traversal_query, company_tickers=company_list)
            for record in result:
                ticker = record["ticker"]
                name = record["name"]
                rel_type = record["relationship_type"]
                hops = record["hop_distance"]
                source_ticker = record["source_ticker"]
                source_name = record["source_name"]
                via_company = record.get("via_company")

                related_companies.add(ticker)

                # Build relationship description showing the pair
                rel_type_name = rel_type.replace("_", " ").title()
                if hops == 1:
                    # Direct relationship: show both companies
                    if rel_type in ["HAS_SUPPLIER", "HAS_CUSTOMER"]:
                        # For supplier/customer, show direction clearly
                        if rel_type == "HAS_SUPPLIER":
                            rel_desc = (
                                f"{source_name} ({source_ticker}) ← Supplier: {name} ({ticker})"
                            )
                        else:  # HAS_CUSTOMER
                            rel_desc = (
                                f"{source_name} ({source_ticker}) → Customer: {name} ({ticker})"
                            )
                    else:
                        # For other relationships, show bidirectional context
                        rel_desc = (
                            f"{source_name} ({source_ticker}) - {rel_type_name} - {name} ({ticker})"
                        )
                else:
                    # Multi-hop: show the path
                    rel_desc = f"{source_name} ({source_ticker}) - {rel_type_name} - {name} ({ticker}) via {via_company or 'intermediate'}"

                traversal_paths.append(
                    {
                        "ticker": ticker,
                        "name": name,
                        "relationship": rel_type,
                        "hops": hops,
                        "source_company": source_name,
                        "source_ticker": source_ticker,
                        "description": rel_desc,
                    }
                )

                # Track relationships for each company
                if ticker not in company_relationship_map:
                    company_relationship_map[ticker] = []
                company_relationship_map[ticker].append(
                    {
                        "source_ticker": source_ticker,
                        "source_name": source_name,
                        "relationship": rel_type,
                        "hops": hops,
                        "description": rel_desc,
                    }
                )

        logger.info(f"Found {len(related_companies)} related companies via graph traversal")

        # Step 4: Get chunks from related companies with relationship context
        if related_companies:
            # Get chunks from related companies and compute similarity
            # Also include relationship context in the chunk metadata
            related_chunks_query = """
            MATCH (c:Company)
            WHERE c.ticker IN $related_tickers
            MATCH (c)-[:HAS]->(doc:Document)<-[:PART_OF_DOCUMENT]-(chunk:Chunk)
            WHERE chunk.embedding IS NOT NULL
            RETURN chunk.chunk_id AS chunk_id,
                   chunk.text AS text,
                   chunk.chunk_index AS chunk_index,
                   chunk.metadata AS metadata,
                   chunk.embedding AS embedding,
                   doc.doc_id AS doc_id,
                   doc.section_type AS section_type,
                   doc.company_cik AS company_cik,
                   doc.company_ticker AS company_ticker,
                   doc.company_name AS company_name,
                   doc.filing_year AS filing_year
            LIMIT 10000
            """

            candidate_chunks = []
            with driver.session(database=database) as session:
                result = session.run(related_chunks_query, related_tickers=list(related_companies))
                for record in result:
                    chunk_embedding = record.get("embedding")
                    if chunk_embedding:
                        similarity = _cosine_similarity(question_embedding, chunk_embedding)
                        if similarity >= 0.35:  # Lower threshold for related company chunks
                            chunk_dict = dict(record)
                            chunk_dict["similarity"] = similarity
                            chunk_dict["source"] = "graph_traversal"

                            # Add relationship context to chunk
                            ticker = record["company_ticker"]
                            if ticker and ticker in company_relationship_map:
                                relationships = company_relationship_map[ticker]
                                # Get the most direct relationship (lowest hops)
                                best_rel = min(relationships, key=lambda x: x["hops"])
                                chunk_dict["graph_relationship"] = best_rel["description"]
                                chunk_dict["related_to"] = best_rel["source_name"]

                            del chunk_dict["embedding"]
                            candidate_chunks.append(chunk_dict)

            # Sort by similarity (prioritize semantically relevant chunks from related companies)
            candidate_chunks.sort(key=lambda x: x["similarity"], reverse=True)

            # Add top related chunks (more than max_documents to allow for deduplication)
            all_chunks.extend(candidate_chunks[: max_documents * 2])
            logger.info(
                f"Added {len(candidate_chunks[: max_documents * 2])} chunks from related companies"
            )

    # Step 5: Deduplicate and sort all chunks by similarity
    seen_chunk_ids = set()
    unique_chunks = []
    for chunk in all_chunks:
        chunk_id = chunk.get("chunk_id")
        if chunk_id and chunk_id not in seen_chunk_ids:
            seen_chunk_ids.add(chunk_id)
            unique_chunks.append(chunk)

    unique_chunks.sort(key=lambda x: x.get("similarity", 0), reverse=True)
    final_chunks = unique_chunks[:max_documents]

    # Extract all companies (initial + related)
    all_company_tickers = set()
    for chunk in final_chunks:
        if chunk.get("company_ticker"):
            all_company_tickers.add(chunk["company_ticker"])

    companies = [
        (chunk.get("company_ticker"), chunk.get("company_name"))
        for chunk in final_chunks
        if chunk.get("company_ticker")
    ]
    companies = list({(t, n) for t, n in companies if t})  # Deduplicate

    # Combine chunk text as context with relationship information
    context_parts = []
    for chunk in final_chunks:
        source = chunk.get("source", "vector_search")
        company_name = chunk.get("company_name", "Unknown")
        section = chunk.get("section_type", "unknown")
        chunk_idx = chunk.get("chunk_index", "?")

        # Add relationship context if available
        if source == "graph_traversal" and chunk.get("graph_relationship"):
            rel_info = f" (Related: {chunk['graph_relationship']})"
        else:
            rel_info = ""

        context_parts.append(
            f"[{company_name} - {section} - Chunk {chunk_idx} - Source: {source}{rel_info}]:\n{chunk['text']}"
        )

    context = "\n\n".join(context_parts)

    return {
        "question": question,
        "chunks": final_chunks,
        "context": context,
        "companies": companies,
        "related_companies": list(related_companies)[:20],  # Limit for display
        "traversal_paths": traversal_paths[:20],  # Limit for display
        "num_chunks": len(final_chunks),
    }
