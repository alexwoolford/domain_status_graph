"""
Async OpenAI client for parallel embedding creation.

Provides async functions for creating embeddings with concurrent requests,
significantly faster than synchronous sequential processing.
"""

import asyncio
import logging
from collections.abc import Callable

from public_company_graph.config import get_openai_api_key
from public_company_graph.constants import EMBEDDING_MODEL
from public_company_graph.embeddings.openai_client import (
    EMBEDDING_TRUNCATE_TOKENS,
    count_tokens,
    truncate_to_token_limit,
)

try:
    from openai import AsyncOpenAI

    ASYNC_OPENAI_AVAILABLE = True
except ImportError:
    ASYNC_OPENAI_AVAILABLE = False

logger = logging.getLogger(__name__)


def get_async_openai_client() -> AsyncOpenAI:
    """Get async OpenAI client instance."""
    if not ASYNC_OPENAI_AVAILABLE:
        raise ImportError("openai not available. Install with: pip install openai")
    api_key = get_openai_api_key()
    return AsyncOpenAI(api_key=api_key)


async def create_embeddings_batch_async(
    client: AsyncOpenAI,
    texts: list[str],
    model: str = EMBEDDING_MODEL,
    max_tokens_per_batch: int = 40_000,
    max_concurrent: int = 5,  # Process 5 batches in parallel (reduced from 10 to prevent OOM)
    on_batch_complete: Callable[[list[int], list[list[float]], list[str]], None] | None = None,
) -> list[list[float] | None]:
    """
    Create embeddings for multiple texts using async parallel processing.

    Processes multiple batches concurrently for much faster throughput.

    Args:
        client: AsyncOpenAI client instance
        texts: List of texts to embed
        model: Embedding model name
        max_tokens_per_batch: Max tokens per API call (default: 40K)
        max_concurrent: Max concurrent API requests (default: 50)
        on_batch_complete: Optional callback for immediate caching/writing

    Returns:
        List of embedding vectors (same order as input texts)
    """
    if not texts:
        return []

    # SIMPLIFIED: Pre-process and batch all texts (they're already in memory from caller)
    # The caller handles chunking to avoid loading all 2.16M at once
    # We just process what we're given efficiently with async parallelism
    logger.info(f"Pre-processing {len(texts):,} texts and creating batches...")

    # Pre-process: truncate and count tokens
    processed_texts: list[tuple[int, str, int]] = []
    for i, text in enumerate(texts):
        if text and text.strip():
            truncated = truncate_to_token_limit(text.strip(), EMBEDDING_TRUNCATE_TOKENS, model)
            token_count = count_tokens(truncated, model)
            processed_texts.append((i, truncated, token_count))

    # Build batches
    batches: list[list[tuple[int, str, int]]] = []
    current_batch: list[tuple[int, str, int]] = []
    current_batch_tokens = 0

    for item in processed_texts:
        original_idx, text, token_count = item
        if current_batch and current_batch_tokens + token_count > max_tokens_per_batch:
            batches.append(current_batch)
            current_batch = []
            current_batch_tokens = 0
        current_batch.append(item)
        current_batch_tokens += token_count

    if current_batch:
        batches.append(current_batch)

    logger.info(f"Created {len(batches):,} batches from {len(processed_texts):,} texts")

    # Don't create results list if using callback (saves memory)
    use_results_list = on_batch_complete is None
    if use_results_list:
        results: list[list[float] | None] = [None] * len(texts)
    else:
        results: list[list[float] | None] = []  # Empty - callback handles everything

    # Async function to process one batch
    async def process_batch(
        batch: list[tuple[int, str, int]],
        semaphore: asyncio.Semaphore,
    ) -> tuple[list[int], list[list[float]], list[str]]:
        async with semaphore:
            valid_indices = [item[0] for item in batch]
            valid_texts = [item[1] for item in batch]

            try:
                response = await client.embeddings.create(model=model, input=valid_texts)
                batch_embeddings = [list(emb.embedding) for emb in response.data]
                return (valid_indices, batch_embeddings, valid_texts)
            except Exception as e:
                logger.warning(f"Batch failed: {e}")
                return (valid_indices, [None] * len(valid_texts), valid_texts)

    # Process batches incrementally to avoid creating all tasks at once
    # This prevents memory accumulation when processing large chunks
    semaphore = asyncio.Semaphore(max_concurrent)
    completed = 0
    total_batches = len(batches)

    # Process in smaller groups to avoid memory accumulation
    # Create tasks in groups of max_concurrent, process them, then create next group
    batch_groups = [batches[i : i + max_concurrent] for i in range(0, len(batches), max_concurrent)]

    for _group_idx, batch_group in enumerate(batch_groups):
        # Create tasks for this group only
        tasks = [process_batch(batch, semaphore) for batch in batch_group]

        # Process this group
        for coro in asyncio.as_completed(tasks):
            indices, embeddings, batch_texts = await coro

            # Store results and call callback
            batch_indices = []
            batch_embeddings = []
            batch_texts_list = []

            for i, embedding in enumerate(embeddings):
                if embedding:
                    original_idx = indices[i]
                    if use_results_list:
                        results[original_idx] = embedding
                    batch_indices.append(original_idx)
                    batch_embeddings.append(embedding)
                    batch_texts_list.append(batch_texts[i])

            # Call callback with all embeddings from this batch (caches/writes immediately)
            if on_batch_complete and batch_embeddings:
                on_batch_complete(batch_indices, batch_embeddings, batch_texts_list)

            completed += 1
            if completed % 100 == 0 or completed == total_batches:
                logger.info(
                    f"Completed {completed}/{total_batches} batches ({completed / total_batches * 100:.1f}%)"
                )

    return results
