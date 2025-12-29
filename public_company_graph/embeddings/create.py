"""
General-purpose embedding creation for Neo4j nodes.

Simple module that:
1. Loads nodes with text from Neo4j
2. Creates/caches embeddings using the unified cache (with batch API for speed)
3. Updates Neo4j nodes with embeddings

Performance: Uses OpenAI's batch embedding API to process texts efficiently,
reducing embedding time from ~40 minutes to ~2 minutes for 6000+ nodes.

Long text handling: Texts exceeding token limits are processed with chunking
and weighted averaging (earlier chunks weighted higher). This preserves
accuracy for long 10-K business descriptions while maintaining speed for shorter texts.
"""

import logging
import re
import time
from typing import Any

from public_company_graph.cache import AppCache
from public_company_graph.constants import (
    BATCH_SIZE_SMALL,
    MIN_DESCRIPTION_LENGTH_FOR_SIMILARITY,
)
from public_company_graph.embeddings.openai_client import (
    EMBEDDING_TRUNCATE_TOKENS,
    count_tokens,
)

logger = logging.getLogger(__name__)

# Allowed node labels and property names for security (prevent injection)
ALLOWED_NODE_LABELS = {"Domain", "Company"}
ALLOWED_PROPERTY_PATTERN = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


def _validate_property_name(name: str, param_name: str) -> None:
    """Validate that a property name is safe to use in Cypher queries."""
    if not ALLOWED_PROPERTY_PATTERN.match(name):
        raise ValueError(
            f"Invalid {param_name}: '{name}'. "
            "Property names must start with a letter or underscore and contain "
            "only alphanumeric characters and underscores."
        )


def _validate_node_label(label: str) -> None:
    """Validate that a node label is allowed."""
    if label not in ALLOWED_NODE_LABELS:
        raise ValueError(f"Invalid node_label: '{label}'. Allowed labels: {ALLOWED_NODE_LABELS}")


def create_embeddings_for_nodes(
    driver,
    cache: AppCache,
    node_label: str,
    text_property: str,
    key_property: str,
    embedding_property: str = "description_embedding",
    model_property: str = "embedding_model",
    dimension_property: str = "embedding_dimension",
    embedding_model: str = "text-embedding-3-small",
    embedding_dimension: int = 1536,
    openai_client: Any = None,
    database: str | None = None,
    execute: bool = False,
    log: logging.Logger | None = None,
) -> tuple[int, int, int, int]:
    """
    Create/load embeddings for Neo4j nodes and update them.

    Uses OpenAI's batch embedding API for efficient processing:
    - Short texts: Batched together in single API calls
    - Long texts: Chunked with weighted averaging for accuracy

    Args:
        driver: Neo4j driver
        cache: AppCache instance (unified diskcache)
        node_label: Neo4j node label (e.g., "Domain", "Company")
        text_property: Property name containing text to embed
        key_property: Property name for unique key
        embedding_property: Property name to store embedding
        model_property: Property name to store model name
        dimension_property: Property name to store dimension
        embedding_model: Embedding model name
        embedding_dimension: Expected embedding dimension
        openai_client: OpenAI client instance (required)
        database: Neo4j database name
        execute: If False, only print plan
        log: Logger instance for output

    Returns:
        Tuple of (processed, created, cached, failed) counts

    Raises:
        ValueError: If openai_client is not provided
    """
    # Use passed logger if provided, otherwise use module logger
    _logger = log if log is not None else logger

    # Validate inputs for security (prevent injection)
    _validate_node_label(node_label)
    _validate_property_name(text_property, "text_property")
    _validate_property_name(key_property, "key_property")
    _validate_property_name(embedding_property, "embedding_property")
    _validate_property_name(model_property, "model_property")
    _validate_property_name(dimension_property, "dimension_property")

    # Load nodes with text
    _logger.info(f"Loading {node_label} nodes with {text_property}...")
    with driver.session(database=database) as session:
        query = f"""
        MATCH (n:{node_label})
        WHERE n.{text_property} IS NOT NULL
          AND n.{text_property} <> ''
          AND size(n.{text_property}) >= $min_length
        RETURN n.{key_property} AS key, n.{text_property} AS text
        ORDER BY n.{key_property}
        """
        result = session.run(query, min_length=MIN_DESCRIPTION_LENGTH_FOR_SIMILARITY)
        nodes = [
            (f"{record['key']}:{text_property}", record["text"])
            for record in result
            if record["text"] and record["text"].strip()
        ]

    _logger.info(f"Found {len(nodes)} {node_label} nodes with {text_property}")

    if not execute:
        _logger.info(f"DRY RUN: Would process embeddings for {len(nodes)} nodes")
        return (0, 0, 0, 0)

    # Step 1: Check cache for existing embeddings
    _logger.info("Checking cache for existing embeddings...")
    cached_embeddings: dict[str, list[float]] = {}
    uncached_items: list[tuple[str, str]] = []  # (cache_key, text)

    for cache_key, text in nodes:
        cached_data = cache.get("embeddings", cache_key)
        if cached_data and "embedding" in cached_data:
            if (
                cached_data.get("model") == embedding_model
                and len(cached_data["embedding"]) == embedding_dimension
            ):
                cached_embeddings[cache_key] = cached_data["embedding"]
                continue
        uncached_items.append((cache_key, text))

    _logger.info(f"  Cached: {len(cached_embeddings)}, Need to create: {len(uncached_items)}")

    # Step 2: Create embeddings for uncached items
    # Strategy: Short texts use fast batch API, long texts use chunking for accuracy
    new_embeddings: dict[str, list[float]] = {}
    failed = 0

    if uncached_items:
        if openai_client is None:
            raise ValueError("openai_client is required. Use get_openai_client() to create one.")

        from public_company_graph.embeddings.openai_client import (
            create_embeddings_batch,
        )

        # Separate short texts (batch-able) from long texts (need chunking)
        short_items: list[tuple[str, str]] = []
        long_items: list[tuple[str, str]] = []

        for cache_key, text in uncached_items:
            token_count = count_tokens(text, embedding_model)
            if token_count <= EMBEDDING_TRUNCATE_TOKENS:
                short_items.append((cache_key, text))
            else:
                long_items.append((cache_key, text))

        if long_items:
            _logger.info(
                f"  Text length distribution: {len(short_items)} short (batch), "
                f"{len(long_items)} long (chunked)"
            )

        # Process short texts with fast batch API
        # create_embeddings_batch handles token-based batching internally
        if short_items:
            _logger.info(f"Creating {len(short_items)} embeddings via batch API...")
            batch_keys = [k for k, _ in short_items]
            batch_texts = [t for _, t in short_items]

            # create_embeddings_batch uses token-based batching (max 150K tokens per API call)
            embeddings = create_embeddings_batch(openai_client, batch_texts, embedding_model)

            start_time = time.time()
            last_log_time = start_time
            total_short = len(short_items)

            for i, embedding in enumerate(embeddings):
                cache_key = batch_keys[i]
                if embedding and len(embedding) == embedding_dimension:
                    new_embeddings[cache_key] = embedding
                    cache.set(
                        "embeddings",
                        cache_key,
                        {
                            "embedding": embedding,
                            "text": batch_texts[i],
                            "model": embedding_model,
                            "dimension": embedding_dimension,
                        },
                    )
                else:
                    failed += 1

                # Time-based progress logging (every 30 seconds)
                current_time = time.time()
                if current_time - last_log_time >= 30:
                    processed_short = i + 1
                    elapsed = current_time - start_time
                    rate = processed_short / elapsed if elapsed > 0 else 0
                    remaining = (total_short - processed_short) / rate if rate > 0 else 0
                    pct = (processed_short / total_short * 100) if total_short > 0 else 0
                    _logger.info(
                        f"  Progress: {processed_short:,}/{total_short:,} ({pct:.1f}%) | "
                        f"Rate: {rate:.1f}/sec | ETA: {remaining / 60:.1f}min"
                    )
                    last_log_time = current_time

            _logger.info(f"  Completed {len(short_items)} short text embeddings")

        # Process long texts with batched chunking
        if long_items:
            from public_company_graph.embeddings.chunking import (
                create_embeddings_for_long_texts_batched,
            )

            _logger.info(f"Creating {len(long_items)} embeddings with chunking...")

            # Create text lookup for caching
            text_by_key = dict(long_items)

            try:
                batched_results = create_embeddings_for_long_texts_batched(
                    client=openai_client,
                    items=long_items,
                    model=embedding_model,
                    log=_logger,
                )

                # Process results and update cache
                for cache_key, embedding in batched_results.items():
                    if embedding and len(embedding) == embedding_dimension:
                        new_embeddings[cache_key] = embedding
                        cache.set(
                            "embeddings",
                            cache_key,
                            {
                                "embedding": embedding,
                                "text": text_by_key[cache_key],
                                "model": embedding_model,
                                "dimension": embedding_dimension,
                            },
                        )
                    else:
                        failed += 1

                # Count failures (items not in results)
                missing = set(text_by_key.keys()) - set(batched_results.keys())
                failed += len(missing)

            except Exception as e:
                _logger.error(f"Batched chunking failed: {e}")
                failed += len(long_items)

    # Step 3: Update Neo4j nodes with all embeddings (cached + new)
    all_embeddings = {**cached_embeddings, **new_embeddings}
    _logger.info(f"Updating {len(all_embeddings)} {node_label} nodes in Neo4j...")

    update_batch: list[dict[str, Any]] = []
    processed = 0

    for cache_key, embedding in all_embeddings.items():
        node_key = cache_key.split(":", 1)[0]
        update_batch.append(
            {
                "key": node_key,
                "embedding": embedding,
                "model": embedding_model,
                "dimension": embedding_dimension,
            }
        )

        if len(update_batch) >= BATCH_SIZE_SMALL:
            with driver.session(database=database) as session:
                query = f"""
                UNWIND $batch AS row
                MATCH (n:{node_label} {{{key_property}: row.key}})
                SET n.{embedding_property} = row.embedding,
                    n.{model_property} = row.model,
                    n.{dimension_property} = row.dimension
                """
                session.run(query, batch=update_batch)
            processed += len(update_batch)
            update_batch.clear()

    # Flush remaining
    if update_batch:
        with driver.session(database=database) as session:
            query = f"""
            UNWIND $batch AS row
            MATCH (n:{node_label} {{{key_property}: row.key}})
            SET n.{embedding_property} = row.embedding,
                n.{model_property} = row.model,
                n.{dimension_property} = row.dimension
            """
            session.run(query, batch=update_batch)
        processed += len(update_batch)

    return (processed, len(new_embeddings), len(cached_embeddings), failed)
