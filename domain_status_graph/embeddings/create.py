"""
General-purpose embedding creation for Neo4j nodes.

Simple module that:
1. Loads nodes with text from Neo4j
2. Creates/caches embeddings using the unified cache
3. Updates Neo4j nodes with embeddings
"""

import logging
import re
import sys
import time
from typing import Callable, List, Optional, Tuple

from domain_status_graph.cache import AppCache
from domain_status_graph.constants import BATCH_SIZE_SMALL, EMBEDDING_REQUEST_INTERVAL

try:
    from tqdm import tqdm

    TQDM_AVAILABLE = True
except ImportError:
    TQDM_AVAILABLE = False

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
    create_fn: Callable[[str, str], Optional[List[float]]] = None,
    database: str = None,
    execute: bool = False,
) -> Tuple[int, int, int, int]:
    """
    Create/load embeddings for Neo4j nodes and update them.

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
        create_fn: Function to create embedding: (text, model) -> embedding
        database: Neo4j database name
        execute: If False, only print plan

    Returns:
        Tuple of (processed, created, cached, failed) counts
    """
    if not create_fn:
        raise ValueError("create_fn is required")

    # Validate inputs for security (prevent injection)
    _validate_node_label(node_label)
    _validate_property_name(text_property, "text_property")
    _validate_property_name(key_property, "key_property")
    _validate_property_name(embedding_property, "embedding_property")
    _validate_property_name(model_property, "model_property")
    _validate_property_name(dimension_property, "dimension_property")

    # Load nodes with text
    logger.info(f"Loading {node_label} nodes with {text_property}...")
    with driver.session(database=database) as session:
        # Use parameterized query with validated property names
        query = f"""
        MATCH (n:{node_label})
        WHERE n.{text_property} IS NOT NULL AND n.{text_property} <> ''
        RETURN n.{key_property} AS key, n.{text_property} AS text
        ORDER BY n.{key_property}
        """
        result = session.run(query)
        # Cache key includes property name to distinguish different embeddings on same node
        # Filter out empty text to avoid wasting API calls
        nodes = [
            (f"{record['key']}:{text_property}", record["text"])
            for record in result
            if record["text"] and record["text"].strip()
        ]

    logger.info(f"Found {len(nodes)} {node_label} nodes with {text_property}")

    if not execute:
        logger.info(f"DRY RUN: Would process embeddings for {len(nodes)} nodes")
        return (0, 0, 0, 0)

    processed = 0
    created = 0
    cached = 0
    failed = 0
    last_request_time = 0

    # Batch updates for performance (reuse single session)
    update_batch = []
    batch_size = BATCH_SIZE_SMALL

    progress_desc = f"Processing {node_label} {text_property} embeddings"
    if TQDM_AVAILABLE:
        iterator = tqdm(
            nodes,
            desc=progress_desc,
            unit="node",
            file=sys.stderr,
            miniters=10,
            mininterval=1.0,
        )
    else:
        iterator = nodes

    def flush_batch():
        """Flush accumulated batch updates to Neo4j."""
        nonlocal processed
        if not update_batch:
            return

        with driver.session(database=database) as session:
            # Use parameterized query with validated property names
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

    for cache_key, text in iterator:
        # Check cache first
        cached_data = cache.get("embeddings", cache_key)
        embedding = None
        was_cached = False

        if cached_data and "embedding" in cached_data:
            # Validate cached embedding
            if (
                cached_data.get("model") == embedding_model
                and len(cached_data["embedding"]) == embedding_dimension
            ):
                embedding = cached_data["embedding"]
                was_cached = True

        if not embedding:
            # Rate limit API calls (OpenAI embeddings allow higher rates)
            current_time = time.time()
            elapsed = current_time - last_request_time
            if elapsed < EMBEDDING_REQUEST_INTERVAL:
                time.sleep(EMBEDDING_REQUEST_INTERVAL - elapsed)

            # Create new embedding with error handling
            try:
                embedding = create_fn(text, embedding_model)
                last_request_time = time.time()
            except Exception as e:
                logger.warning(f"Failed to create embedding for {node_label} {cache_key}: {e}")
                embedding = None

            if embedding:
                # Validate embedding dimension
                if len(embedding) != embedding_dimension:
                    logger.warning(
                        f"Invalid embedding dimension for {cache_key}: "
                        f"got {len(embedding)}, expected {embedding_dimension}"
                    )
                    embedding = None
                else:
                    # Store in cache
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

        if embedding:
            # Extract node key from cache key (format: "node_key:property_name")
            node_key = cache_key.split(":", 1)[0]

            # Add to batch for efficient updates
            update_batch.append(
                {
                    "key": node_key,
                    "embedding": embedding,
                    "model": embedding_model,
                    "dimension": embedding_dimension,
                }
            )

            if was_cached:
                cached += 1
            else:
                created += 1

            # Flush batch when it reaches batch_size
            if len(update_batch) >= batch_size:
                flush_batch()
        else:
            failed += 1
            logger.warning(f"Failed to create embedding for {node_label} {cache_key}")

    # Flush remaining batch
    flush_batch()

    return (processed, created, cached, failed)
