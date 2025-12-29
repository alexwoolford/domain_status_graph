"""
Chunking utilities for long text embeddings.

When text exceeds token limits, split into chunks and aggregate embeddings.

Optimization: Batched chunk embedding
- Instead of 1 API call per chunk (slow), we batch all chunks together
- OpenAI allows up to 300K tokens per request
- With ~7K tokens per chunk, we can fit ~40 chunks per batch
- This reduces API calls by ~40x for long text processing
"""

import logging
from collections.abc import Callable
from typing import Any

import numpy as np

from domain_status_graph.embeddings.openai_client import (
    count_tokens,
)

logger = logging.getLogger(__name__)

# Chunking parameters
CHUNK_SIZE_TOKENS = 7000  # Leave room for overlap
CHUNK_OVERLAP_TOKENS = 200  # Overlap between chunks to preserve context

# Batching parameters for chunk embedding
# OpenAI has 300K token limit per request
# Actual chunks average ~8K tokens (not 7K) due to overlap and tokenization
# 300K / 8.5K = ~35 chunks max, use 30 for safety margin
MAX_TOKENS_PER_BATCH = 250000  # Leave margin below 300K limit
MAX_CHUNKS_PER_BATCH = 30  # Safe limit: 30 * 8.5K = 255K tokens


def chunk_text(
    text: str,
    chunk_size_tokens: int = CHUNK_SIZE_TOKENS,
    overlap_tokens: int = CHUNK_OVERLAP_TOKENS,
    model: str = "text-embedding-3-small",
) -> list[str]:
    """
    Split text into chunks that fit within token limits.

    Uses token-aware chunking to preserve semantic boundaries.
    Overlaps chunks to preserve context between chunks.

    Args:
        text: Text to chunk
        chunk_size_tokens: Target chunk size in tokens
        overlap_tokens: Overlap between chunks in tokens
        model: Model name (determines encoding)

    Returns:
        List of text chunks
    """
    if not text:
        return []

    # Count tokens
    total_tokens = count_tokens(text, model)

    # If text fits in one chunk, return as-is
    if total_tokens <= chunk_size_tokens:
        return [text]

    try:
        import tiktoken

        # Get encoding
        encoding = tiktoken.encoding_for_model(model)
        tokens = encoding.encode(text)

        chunks = []
        start_idx = 0

        while start_idx < len(tokens):
            # Calculate end index for this chunk
            end_idx = min(start_idx + chunk_size_tokens, len(tokens))

            # Extract chunk tokens
            chunk_tokens = tokens[start_idx:end_idx]

            # Decode back to text
            chunk_text = encoding.decode(chunk_tokens)
            chunks.append(chunk_text)

            # Move to next chunk with overlap
            if end_idx >= len(tokens):
                break

            # Start next chunk with overlap (go back by overlap amount)
            start_idx = end_idx - overlap_tokens

            # Ensure we make progress (handle edge case where overlap is too large)
            # Check if we've already processed this position
            if start_idx <= (end_idx - chunk_size_tokens) and len(chunks) > 1:
                start_idx = end_idx

        logger.debug(
            f"Chunked text: {total_tokens} tokens → {len(chunks)} chunks "
            f"(avg {total_tokens // len(chunks)} tokens/chunk)"
        )

        return chunks

    except ImportError:
        # Fallback: character-based chunking
        logger.warning("tiktoken not available, using character-based chunking")
        # Rough estimate: 1 token ≈ 4 chars
        chunk_size_chars = chunk_size_tokens * 4
        overlap_chars = overlap_tokens * 4

        chunks = []
        start = 0

        while start < len(text):
            end = min(start + chunk_size_chars, len(text))
            chunk = text[start:end]
            chunks.append(chunk)

            if end >= len(text):
                break

            # Move to next chunk with overlap
            start = end - overlap_chars
            if start <= 0:
                start = end

        return chunks


def aggregate_embeddings(
    embeddings: list[list[float]],
    method: str = "average",
    weights: list[float] | None = None,
) -> list[float]:
    """
    Aggregate multiple embeddings into a single embedding.

    Args:
        embeddings: List of embedding vectors (all same dimension)
        method: Aggregation method ("average", "weighted_average", "max")
        weights: Optional weights for weighted average (default: equal weights)

    Returns:
        Single aggregated embedding vector
    """
    if not embeddings:
        raise ValueError("Cannot aggregate empty list of embeddings")

    if len(embeddings) == 1:
        return embeddings[0]

    # Convert to numpy array
    embeddings_array = np.array(embeddings, dtype=np.float32)

    # Validate all embeddings have same dimension
    expected_dim = len(embeddings[0])
    if embeddings_array.shape[1] != expected_dim:
        raise ValueError(
            f"Embeddings have inconsistent dimensions: "
            f"expected {expected_dim}, got {embeddings_array.shape[1]}"
        )

    if method == "average":
        # Simple average
        aggregated = np.mean(embeddings_array, axis=0)

    elif method == "weighted_average":
        # Weighted average
        if weights is None:
            weights = [1.0 / len(embeddings)] * len(embeddings)

        if len(weights) != len(embeddings):
            raise ValueError(
                f"Weights length ({len(weights)}) must match embeddings length ({len(embeddings)})"
            )

        # Normalize weights
        weights_array = np.array(weights, dtype=np.float32)
        weights_array = weights_array / np.sum(weights_array)

        # Weighted average
        aggregated = np.average(embeddings_array, axis=0, weights=weights_array)

    elif method == "max":
        # Element-wise max (less common, but sometimes useful)
        aggregated = np.max(embeddings_array, axis=0)

    else:
        raise ValueError(f"Unknown aggregation method: {method}")

    return list(aggregated.tolist())


def create_embedding_with_chunking(
    client,
    text: str,
    model: str = "text-embedding-3-small",
    chunk_size_tokens: int = CHUNK_SIZE_TOKENS,
    overlap_tokens: int = CHUNK_OVERLAP_TOKENS,
    aggregation_method: str = "weighted_average",
    create_embedding_fn=None,
) -> list[float] | None:
    """
    Create embedding for text, using chunking if text exceeds token limit.

    If text fits in one chunk, creates single embedding.
    If text exceeds limit, chunks it and aggregates embeddings.

    Args:
        client: OpenAI client instance
        text: Text to embed
        model: Embedding model name
        chunk_size_tokens: Target chunk size in tokens
        overlap_tokens: Overlap between chunks in tokens
        aggregation_method: How to aggregate chunk embeddings ("average", "weighted_average", "max")
        create_embedding_fn: Function to create embedding: (client, text, model) -> embedding

    Returns:
        Single embedding vector (aggregated if chunked)
    """
    if not text or not text.strip():
        return None

    if create_embedding_fn is None:
        # Use the internal retry function to avoid recursion
        from domain_status_graph.embeddings.openai_client import _create_embedding_with_retry

        def create_embedding_fn(client, text, model):
            return _create_embedding_with_retry(client, text, model)

    # Check if chunking is needed
    total_tokens = count_tokens(text, model)

    if total_tokens <= chunk_size_tokens:
        # Fits in one chunk - create single embedding
        result = create_embedding_fn(client, text, model)
        return list(result) if result is not None else None

    # Need to chunk
    logger.info(
        f"Text exceeds token limit ({total_tokens:,} tokens > {chunk_size_tokens:,}), "
        f"chunking into multiple pieces..."
    )

    # Split into chunks
    chunks = chunk_text(text, chunk_size_tokens, overlap_tokens, model)

    if not chunks:
        logger.warning("No chunks created from text")
        return None

    logger.info(f"Created {len(chunks)} chunks, creating embeddings...")

    # Create embedding for each chunk
    chunk_embeddings = []
    for i, chunk in enumerate(chunks):
        chunk_tokens = count_tokens(chunk, model)
        logger.debug(f"  Chunk {i+1}/{len(chunks)}: {chunk_tokens:,} tokens")

        embedding = create_embedding_fn(client, chunk, model)
        if embedding:
            chunk_embeddings.append(embedding)
        else:
            logger.warning(f"  Failed to create embedding for chunk {i+1}")

    if not chunk_embeddings:
        logger.error("Failed to create any chunk embeddings")
        return None

    if len(chunk_embeddings) == 1:
        return list(chunk_embeddings[0])

    # Aggregate embeddings
    logger.info(f"Aggregating {len(chunk_embeddings)} embeddings using {aggregation_method}...")

    # For weighted average, weight earlier chunks higher (they contain most important info)
    weights = None
    if aggregation_method == "weighted_average":
        # Exponential decay: first chunk gets highest weight
        # Weight = exp(-decay * index), normalized
        decay = 0.2  # Adjustable: higher = more weight on early chunks
        weights = [np.exp(-decay * i) for i in range(len(chunk_embeddings))]
        # Normalize
        total_weight = sum(weights)
        weights = [w / total_weight for w in weights]

        logger.debug(f"  Weights: {[f'{w:.3f}' for w in weights]}")

    aggregated = aggregate_embeddings(chunk_embeddings, method=aggregation_method, weights=weights)

    logger.info(
        f"✓ Aggregated {len(chunk_embeddings)} embeddings into single embedding "
        f"({len(aggregated)} dimensions)"
    )

    return aggregated


def create_embeddings_for_long_texts_batched(
    client: Any,
    items: list[tuple[str, str]],  # List of (cache_key, text)
    model: str = "text-embedding-3-small",
    chunk_size_tokens: int = CHUNK_SIZE_TOKENS,
    overlap_tokens: int = CHUNK_OVERLAP_TOKENS,
    aggregation_method: str = "weighted_average",
    batch_embed_fn: Callable[[Any, list[str], str], list[list[float] | None]] | None = None,
    log: logging.Logger | None = None,
) -> dict[str, list[float] | None]:
    """
    Create embeddings for multiple long texts using batched chunk processing.

    Instead of making 1 API call per chunk (slow), this function:
    1. Chunks all texts upfront
    2. Batches chunks together (up to ~40 per API call)
    3. Maps results back and aggregates per text

    This reduces API calls by ~40x for long text processing.

    Example:
        - 100 long texts × 3 chunks each = 300 chunks
        - Old way: 300 API calls
        - New way: 300 ÷ 40 = 8 API calls (~40x faster)

    Args:
        client: OpenAI client instance
        items: List of (cache_key, text) tuples to process
        model: Embedding model name
        chunk_size_tokens: Target chunk size in tokens
        overlap_tokens: Overlap between chunks in tokens
        aggregation_method: How to aggregate chunk embeddings
        batch_embed_fn: Function to create batch embeddings: (client, texts, model) -> embeddings
        log: Logger instance

    Returns:
        Dict mapping cache_key to final aggregated embedding (or None if failed)
    """
    _logger = log if log is not None else logger

    if not items:
        return {}

    if batch_embed_fn is None:
        from domain_status_graph.embeddings.openai_client import create_embeddings_batch

        def batch_embed_fn(c, texts, m):
            return create_embeddings_batch(c, texts, m, batch_size=MAX_CHUNKS_PER_BATCH)

    # Step 1: Chunk all texts and collect metadata
    _logger.info(f"Chunking {len(items)} long texts...")

    all_chunks: list[str] = []
    chunk_metadata: list[tuple[str, int, int]] = []  # (cache_key, chunk_idx, total_chunks)
    texts_chunk_counts: dict[str, int] = {}

    for cache_key, text in items:
        if not text or not text.strip():
            continue

        chunks = chunk_text(text, chunk_size_tokens, overlap_tokens, model)
        if not chunks:
            continue

        texts_chunk_counts[cache_key] = len(chunks)
        for i, chunk in enumerate(chunks):
            all_chunks.append(chunk)
            chunk_metadata.append((cache_key, i, len(chunks)))

    total_chunks = len(all_chunks)
    if total_chunks == 0:
        _logger.warning("No chunks to process")
        return {}

    _logger.info(
        f"  Created {total_chunks} chunks from {len(texts_chunk_counts)} texts "
        f"(avg {total_chunks / len(texts_chunk_counts):.1f} chunks/text)"
    )

    # Step 2: Batch all chunks together for embedding
    # The batch function handles splitting into API-safe batches internally
    _logger.info(f"Embedding {total_chunks} chunks in batched API calls...")

    # Calculate expected API calls for logging
    expected_api_calls = (total_chunks + MAX_CHUNKS_PER_BATCH - 1) // MAX_CHUNKS_PER_BATCH
    _logger.info(
        f"  Estimated API calls: ~{expected_api_calls} (vs {total_chunks} without batching)"
    )

    all_embeddings = batch_embed_fn(client, all_chunks, model)

    if len(all_embeddings) != total_chunks:
        _logger.error(
            f"Embedding count mismatch: got {len(all_embeddings)}, expected {total_chunks}"
        )
        return {}

    # Step 3: Group embeddings by cache_key
    _logger.info("Aggregating chunk embeddings per text...")

    embeddings_by_key: dict[str, list[list[float]]] = {}
    failed_chunks_by_key: dict[str, int] = {}

    for i, (cache_key, chunk_idx, total_chunks_for_text) in enumerate(chunk_metadata):
        embedding = all_embeddings[i]

        if cache_key not in embeddings_by_key:
            embeddings_by_key[cache_key] = [None] * total_chunks_for_text  # type: ignore
            failed_chunks_by_key[cache_key] = 0

        if embedding is not None:
            embeddings_by_key[cache_key][chunk_idx] = embedding
        else:
            failed_chunks_by_key[cache_key] += 1

    # Step 4: Aggregate embeddings for each text
    results: dict[str, list[float] | None] = {}
    success_count = 0
    partial_count = 0
    fail_count = 0

    for cache_key, chunk_embeds in embeddings_by_key.items():
        # Filter out None values (failed chunks)
        valid_embeds = [e for e in chunk_embeds if e is not None]

        if not valid_embeds:
            results[cache_key] = None
            fail_count += 1
            continue

        if len(valid_embeds) < len(chunk_embeds):
            partial_count += 1
            _logger.warning(
                f"  {cache_key}: Only {len(valid_embeds)}/{len(chunk_embeds)} chunks succeeded"
            )

        # Aggregate
        if len(valid_embeds) == 1:
            results[cache_key] = list(valid_embeds[0])
        else:
            # Calculate weights for weighted average (earlier chunks weighted higher)
            weights = None
            if aggregation_method == "weighted_average":
                decay = 0.2
                weights = [np.exp(-decay * i) for i in range(len(valid_embeds))]
                total_weight = sum(weights)
                weights = [w / total_weight for w in weights]

            results[cache_key] = aggregate_embeddings(
                valid_embeds, method=aggregation_method, weights=weights
            )

        success_count += 1

    _logger.info(
        f"  Completed: {success_count} succeeded, {partial_count} partial, {fail_count} failed"
    )

    return results
