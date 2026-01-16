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
import traceback
from typing import Any

try:
    import os

    import psutil

    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False

from public_company_graph.cache import AppCache
from public_company_graph.constants import (
    EMBEDDING_NEO4J_BATCH_SIZE_LARGE,
    EMBEDDING_NEO4J_BATCH_SIZE_SMALL,
    EMBEDDING_PAGE_SIZE,
    MIN_DESCRIPTION_LENGTH_FOR_SIMILARITY,
)
from public_company_graph.embeddings.openai_client import (
    EMBEDDING_TRUNCATE_TOKENS,
    count_tokens,
)
from public_company_graph.neo4j.utils import safe_single

logger = logging.getLogger(__name__)


def get_memory_usage_mb() -> float:
    """Get current process memory usage in MB."""
    if PSUTIL_AVAILABLE:
        try:
            process = psutil.Process(os.getpid())
            return process.memory_info().rss / 1024 / 1024
        except Exception:
            pass
    return 0.0


def log_memory_state(logger: logging.Logger, context: str = "") -> None:
    """Log current memory state for debugging."""
    mem_mb = get_memory_usage_mb()
    logger.info(f"💾 Memory: {mem_mb:.1f} MB {context}")


# Allowed node labels and property names for security (prevent injection)
ALLOWED_NODE_LABELS = {"Domain", "Company", "Document", "Chunk"}
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

    # STREAMING: Process nodes directly from Neo4j result iterator
    # Never build full nodes list - process in chunks as we read
    _logger.info(
        f"Loading and processing {node_label} nodes with {text_property} in streaming fashion..."
    )

    # Get total count first (for progress logging)
    with driver.session(database=database) as session:
        count_query = f"""
        MATCH (n:{node_label})
        WHERE n.{text_property} IS NOT NULL
          AND n.{text_property} <> ''
          AND size(n.{text_property}) >= $min_length
        RETURN count(n) AS total
        """
        count_result = session.run(count_query, min_length=MIN_DESCRIPTION_LENGTH_FOR_SIMILARITY)
        count_record = safe_single(count_result)
        if not count_record:
            _logger.warning(f"No {node_label} nodes found matching criteria")
            return (0, 0, 0, 0)
        total_nodes = count_record["total"]

    _logger.info(f"Found {total_nodes:,} {node_label} nodes with {text_property}")

    if not execute:
        _logger.info(f"DRY RUN: Would process embeddings for {total_nodes:,} nodes")
        return (0, 0, 0, 0)

    # Step 1: Check cache and process in streaming chunks
    # STREAMING: Never build full lists - process nodes in chunks directly
    # This prevents holding all 2.16M texts in memory at once
    _logger.info("=" * 80)
    _logger.info("STREAMING MODE: Processing in 50K chunks (no full lists)")
    _logger.info("This is the NEW code path - if you see 'Separating texts', old code is running!")
    _logger.info("=" * 80)
    _logger.info("Checking cache and processing embeddings in streaming chunks...")
    # CRITICAL: Don't accumulate cached embeddings in memory - write directly to Neo4j
    # Only store newly created embeddings temporarily before writing
    new_embeddings: dict[str, list[float]] = {}
    failed = 0
    cached_count_for_neo4j = 0  # Track count for final stats

    if openai_client is None:
        raise ValueError("openai_client is required. Use get_openai_client() to create one.")

    import asyncio

    from public_company_graph.embeddings.openai_client_async import (
        create_embeddings_batch_async,
        get_async_openai_client,
    )

    async_client = get_async_openai_client()

    # STREAMING: Process nodes directly from Neo4j iterator in chunks
    # Never build full nodes list - process as we read
    # Increased to 20K now that memory is stable and sessions are fresh
    chunk_size = 20_000  # Process 20K nodes at a time (increased from 5K for speed)
    cached_count = 0
    uncached_count = 0
    long_items: list[tuple[str, str]] = []  # Only keep long items (usually very few)

    _logger.info(f"Processing {total_nodes:,} nodes in streaming chunks of {chunk_size:,}")

    # Setup Neo4j batch writing
    neo4j_batch: list[dict[str, Any]] = []
    neo4j_batch_size = EMBEDDING_NEO4J_BATCH_SIZE_LARGE

    # Create callback factory - returns a callback bound to specific cache_keys
    def make_cache_and_write_callback(cache_keys: list[str]):
        """Create a callback function bound to specific cache keys."""

        def cache_and_write_batch(
            indices: list[int], embeddings: list[list[float]], texts: list[str]
        ):
            """Cache and write embeddings immediately as each API batch completes."""
            for idx_in_batch, embedding in enumerate(embeddings):
                if not embedding or len(embedding) != embedding_dimension:
                    continue

                # indices[idx_in_batch] is the index within the batch
                text_idx = indices[idx_in_batch]
                if text_idx >= len(cache_keys):
                    continue

                cache_key = cache_keys[text_idx]
                text = texts[idx_in_batch] if idx_in_batch < len(texts) else ""

                new_embeddings[cache_key] = embedding

                # Cache immediately (diskcache auto-commits)
                # Wrap in try/except to handle disk full, permission errors gracefully
                try:
                    cache.set(
                        "embeddings",
                        cache_key,
                        {
                            "embedding": embedding,
                            "text": text,
                            "model": embedding_model,
                            "dimension": embedding_dimension,
                        },
                    )
                except Exception as e:
                    _logger.warning(f"Cache write failed for {cache_key}: {e}")
                    # Continue - cache failures shouldn't stop embedding creation

                # Add to Neo4j batch for incremental writes
                node_key = cache_key.split(":", 1)[0]
                neo4j_batch.append(
                    {
                        "key": node_key,
                        "embedding": embedding,
                        "model": embedding_model,
                        "dimension": embedding_dimension,
                    }
                )

            # Write to Neo4j in batches to avoid losing work
            if len(neo4j_batch) >= neo4j_batch_size:
                with driver.session(database=database) as session:
                    query = f"""
                    UNWIND $batch AS row
                    MATCH (n:{node_label} {{{key_property}: row.key}})
                    SET n.{embedding_property} = row.embedding,
                        n.{model_property} = row.model,
                        n.{dimension_property} = row.dimension
                    """
                    session.run(query, batch=neo4j_batch)
                neo4j_batch.clear()

        return cache_and_write_batch

    # STREAMING: Read from Neo4j and process in chunks (never build full nodes list)
    log_memory_state(_logger, "(before streaming)")

    try:
        # Cursor-based pagination: safer and more efficient than SKIP/LIMIT
        # Uses WHERE key > last_key to avoid issues with concurrent data changes
        processed_count = 0
        page_size = EMBEDDING_PAGE_SIZE
        last_key: str | None = None

        while True:
            # Build query with cursor (WHERE key > last_key if we have one)
            # Only process nodes that don't already have embeddings
            if last_key is None:
                key_query = f"""
                MATCH (n:{node_label})
                WHERE n.{text_property} IS NOT NULL
                  AND n.{text_property} <> ''
                  AND size(n.{text_property}) >= $min_length
                  AND n.{embedding_property} IS NULL
                RETURN n.{key_property} AS key
                ORDER BY n.{key_property}
                LIMIT $limit
                """
                query_params = {
                    "min_length": MIN_DESCRIPTION_LENGTH_FOR_SIMILARITY,
                    "limit": page_size,
                }
            else:
                key_query = f"""
                MATCH (n:{node_label})
                WHERE n.{text_property} IS NOT NULL
                  AND n.{text_property} <> ''
                  AND size(n.{text_property}) >= $min_length
                  AND n.{embedding_property} IS NULL
                  AND n.{key_property} > $last_key
                RETURN n.{key_property} AS key
                ORDER BY n.{key_property}
                LIMIT $limit
                """
                query_params = {
                    "min_length": MIN_DESCRIPTION_LENGTH_FOR_SIMILARITY,
                    "last_key": last_key,
                    "limit": page_size,
                }

            # Fetch a page of keys
            with driver.session(database=database) as session:
                key_result = session.run(key_query, **query_params)
                page_keys = [f"{record['key']}:{text_property}" for record in key_result]

            if not page_keys:
                break

            # Update cursor to last key in this page
            last_key = page_keys[-1].split(":", 1)[0]  # Extract node key from cache key

            # Check cache for all keys in this page
            cached_results = cache.get_many("embeddings", page_keys)

            # Separate cached vs uncached
            cached_batch: list[dict[str, Any]] = []
            uncached_keys: list[str] = []

            for cache_key in page_keys:
                cached_data = cached_results.get(cache_key)
                if cached_data and "embedding" in cached_data:
                    model_match = cached_data.get("model") == embedding_model
                    dim_match = len(cached_data["embedding"]) == embedding_dimension
                    if model_match and dim_match:
                        node_key = cache_key.split(":", 1)[0]
                        cached_batch.append(
                            {
                                "key": node_key,
                                "embedding": cached_data["embedding"],
                                "model": embedding_model,
                                "dimension": embedding_dimension,
                            }
                        )
                        cached_count += 1
                        cached_count_for_neo4j += 1
                        continue
                # Not cached or validation failed - add to uncached
                uncached_keys.append(cache_key)
                uncached_count += 1

            # Write cached embeddings to Neo4j immediately
            if cached_batch:
                neo4j_batch.extend(cached_batch)
                if len(neo4j_batch) >= neo4j_batch_size:
                    with driver.session(database=database) as write_session:
                        query = f"""
                        UNWIND $batch AS row
                        MATCH (n:{node_label} {{{key_property}: row.key}})
                        SET n.{embedding_property} = row.embedding,
                            n.{model_property} = row.model,
                            n.{dimension_property} = row.dimension
                        """
                        write_session.run(query, batch=neo4j_batch)
                    neo4j_batch.clear()

            # Process uncached items (fetch text, call API, write)
            if uncached_keys:
                uncached_node_keys = [ck.split(":", 1)[0] for ck in uncached_keys]
                text_query = f"""
                MATCH (n:{node_label})
                WHERE n.{key_property} IN $keys
                RETURN n.{key_property} AS key, n.{text_property} AS text
                """
                with driver.session(database=database) as text_session:
                    text_result = text_session.run(text_query, keys=uncached_node_keys)
                    text_map = {f"{r['key']}:{text_property}": r["text"] for r in text_result}

                # Separate short/long texts
                short_items: list[tuple[str, str]] = []
                for cache_key in uncached_keys:
                    text = text_map.get(cache_key)
                    if text and text.strip():
                        token_count = count_tokens(text, embedding_model)
                        if token_count <= EMBEDDING_TRUNCATE_TOKENS:
                            short_items.append((cache_key, text))
                        else:
                            long_items.append((cache_key, text))

                # Process short items with API
                if short_items:
                    short_keys = [k for k, _ in short_items]
                    short_texts = [t for _, t in short_items]

                    # Create callback bound to these cache keys
                    batch_callback = make_cache_and_write_callback(short_keys)

                    # Capture loop variables as default arguments to avoid closure bugs
                    async def process_short_items(texts=short_texts, callback=batch_callback):
                        return await create_embeddings_batch_async(
                            async_client,
                            texts.copy(),
                            embedding_model,
                            max_concurrent=10,
                            on_batch_complete=callback,
                        )

                    asyncio.run(process_short_items())

            processed_count += len(page_keys)

            # Log progress (more frequently for visibility)
            if processed_count % EMBEDDING_PAGE_SIZE == 0 or len(page_keys) < page_size:
                mem_mb = get_memory_usage_mb()
                _logger.info(
                    f"✓ Processed {processed_count:,}/{total_nodes:,} nodes ({processed_count / total_nodes * 100:.1f}%) | Cached: {cached_count:,} | Created: {uncached_count:,} | Memory: {mem_mb:.1f} MB"
                )

            # Done if we got fewer keys than requested (no more to fetch)
            if len(page_keys) < page_size:
                break

    except Exception as e:
        # Capture state before crash
        mem_mb = get_memory_usage_mb()
        _logger.error(
            f"❌ FATAL ERROR in streaming loop at {processed_count:,}/{total_nodes:,} nodes"
        )
        _logger.error(f"   Memory: {mem_mb:.1f} MB")
        _logger.error(f"   Error: {e}")
        _logger.error(f"   Traceback:\n{traceback.format_exc()}")
        raise

    # Flush any remaining Neo4j batch from streaming loop
    if neo4j_batch:
        with driver.session(database=database) as session:
            query = f"""
            UNWIND $batch AS row
            MATCH (n:{node_label} {{{key_property}: row.key}})
            SET n.{embedding_property} = row.embedding,
                n.{model_property} = row.model,
                n.{dimension_property} = row.dimension
            """
            session.run(query, batch=neo4j_batch)
        neo4j_batch.clear()

    # Log final stats
    _logger.info(
        f"  ✓ Cache check complete: {cached_count:,} cached, {uncached_count:,} need creation"
    )

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

            # Process results, update cache, and write to Neo4j incrementally
            neo4j_batch: list[dict[str, Any]] = []
            neo4j_batch_size = EMBEDDING_NEO4J_BATCH_SIZE_SMALL

            for cache_key, embedding in batched_results.items():
                if embedding and len(embedding) == embedding_dimension:
                    new_embeddings[cache_key] = embedding
                    # Cache immediately
                    # Wrap in try/except to handle disk full, permission errors gracefully
                    try:
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
                    except Exception as e:
                        _logger.warning(f"Cache write failed for {cache_key}: {e}")
                        # Continue - cache failures shouldn't stop embedding creation

                    # Add to Neo4j batch for incremental writes
                    node_key = cache_key.split(":", 1)[0]
                    neo4j_batch.append(
                        {
                            "key": node_key,
                            "embedding": embedding,
                            "model": embedding_model,
                            "dimension": embedding_dimension,
                        }
                    )

                    # Write to Neo4j in batches to avoid losing work
                    if len(neo4j_batch) >= neo4j_batch_size:
                        with driver.session(database=database) as session:
                            query = f"""
                            UNWIND $batch AS row
                            MATCH (n:{node_label} {{{key_property}: row.key}})
                            SET n.{embedding_property} = row.embedding,
                                n.{model_property} = row.model,
                                n.{dimension_property} = row.dimension
                            """
                            session.run(query, batch=neo4j_batch)
                        neo4j_batch.clear()
                else:
                    failed += 1

            # Flush remaining Neo4j batch
            if neo4j_batch:
                with driver.session(database=database) as session:
                    query = f"""
                    UNWIND $batch AS row
                    MATCH (n:{node_label} {{{key_property}: row.key}})
                    SET n.{embedding_property} = row.embedding,
                        n.{model_property} = row.model,
                        n.{dimension_property} = row.dimension
                    """
                    session.run(query, batch=neo4j_batch)

            # Count failures (items not in results)
            missing = set(text_by_key.keys()) - set(batched_results.keys())
            failed += len(missing)

        except Exception as e:
            _logger.error(f"Batched chunking failed: {e}")
            failed += len(long_items)

    # Step 3: Update Neo4j nodes with cached embeddings (new ones already written incrementally)
    # Only need to write cached embeddings that weren't just created
    # Count processed nodes: both cached (written here) and new (written incrementally via callback)
    # Cached embeddings were already written to Neo4j incrementally during processing
    # New embeddings were also written incrementally via callback
    processed = len(new_embeddings) + cached_count_for_neo4j

    if cached_count_for_neo4j > 0:
        _logger.info(
            f"✓ Already updated {cached_count_for_neo4j:,} cached {node_label} nodes in Neo4j (written incrementally)"
        )

    return (processed, len(new_embeddings), cached_count_for_neo4j, failed)
