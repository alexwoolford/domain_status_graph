"""
Shared OpenAI client setup and embedding creation functions.

This module provides common functionality for creating embeddings:
- OpenAI client initialization
- create_embedding function
- Token counting and truncation
- HTTP logging suppression

Used by embedding creation scripts to avoid code duplication.
"""

import logging
import sys
import time
from collections.abc import Callable

from tqdm import tqdm

from public_company_graph.config import get_openai_api_key
from public_company_graph.constants import EMBEDDING_MODEL

# Try to import OpenAI
try:
    from openai import OpenAI

    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False

# Try to import tiktoken for token counting
try:
    import tiktoken

    TIKTOKEN_AVAILABLE = True
except ImportError:
    TIKTOKEN_AVAILABLE = False

# Token limits for embedding models
# text-embedding-3-small: 8192 tokens
# text-embedding-3-large: 8192 tokens
EMBEDDING_MAX_TOKENS = 8192

# Safety margin: truncate to 8000 tokens to leave room for encoding variations
EMBEDDING_TRUNCATE_TOKENS = 8000


def get_openai_client() -> OpenAI:
    """Get OpenAI client instance."""
    if not OPENAI_AVAILABLE:
        raise ImportError("openai not available. Install with: pip install openai")
    api_key = get_openai_api_key()  # Raises ValueError if not set
    return OpenAI(api_key=api_key)


def count_tokens(text: str, model: str = EMBEDDING_MODEL) -> int:
    """
    Count tokens in text using tiktoken.

    Args:
        text: Text to count tokens for
        model: Model name (determines encoding)

    Returns:
        Number of tokens
    """
    if not TIKTOKEN_AVAILABLE:
        # Fallback: rough estimate (1 token ≈ 4 characters)
        return len(text) // 4

    try:
        # Get encoding for the model
        # text-embedding-3-small and text-embedding-3-large use cl100k_base
        encoding = tiktoken.encoding_for_model(model)
        return len(encoding.encode(text))
    except Exception as e:
        logging.warning(f"Error counting tokens with tiktoken: {e}, using fallback")
        # Fallback: rough estimate
        return len(text) // 4


def truncate_to_token_limit(
    text: str, max_tokens: int = EMBEDDING_TRUNCATE_TOKENS, model: str = EMBEDDING_MODEL
) -> str:
    """
    Truncate text to fit within token limit.

    Uses tiktoken to accurately count tokens and truncate at token boundaries.
    If tiktoken is not available, uses character-based truncation as fallback.

    Args:
        text: Text to truncate
        max_tokens: Maximum number of tokens (default: 8000 for safety margin)
        model: Model name (determines encoding)

    Returns:
        Truncated text (guaranteed to be within token limit)
    """
    if not text:
        return text

    if not TIKTOKEN_AVAILABLE:
        # Fallback: character-based truncation (rough estimate: 1 token ≈ 4 chars)
        # Use 3.5 chars per token to be conservative
        max_chars = int(max_tokens * 3.5)
        if len(text) <= max_chars:
            return text
        logging.warning(
            f"tiktoken not available, using character-based truncation. "
            f"Text length: {len(text)}, max_chars: {max_chars}"
        )
        return text[:max_chars]

    try:
        # Get encoding for the model
        encoding = tiktoken.encoding_for_model(model)
        tokens = encoding.encode(text)

        if len(tokens) <= max_tokens:
            return text

        # Truncate to max_tokens and decode back to text
        truncated_tokens = tokens[:max_tokens]
        truncated_text = encoding.decode(truncated_tokens)

        # Log as WARNING so we know when truncation happens - this means data loss!
        logging.warning(
            f"TRUNCATION: Text reduced from {len(tokens):,} tokens to {max_tokens:,} tokens "
            f"({len(text):,} chars → {len(truncated_text):,} chars). "
            f"Consider using chunking instead."
        )

        return truncated_text
    except Exception as e:
        logging.warning(f"Error truncating with tiktoken: {e}, using fallback")
        # Fallback: character-based truncation
        max_chars = int(max_tokens * 3.5)
        return text[:max_chars] if len(text) > max_chars else text


def create_embedding(
    client: OpenAI,
    text: str,
    model: str = EMBEDDING_MODEL,
) -> list[float] | None:
    """
    Create an embedding for a single text using OpenAI.

    For long texts, uses chunking to preserve all information.
    Chunks are aggregated using weighted average (earlier chunks weighted higher).

    Args:
        client: OpenAI client instance
        text: Text to embed
        model: Embedding model name

    Returns:
        Embedding vector or None if creation failed
    """
    if not text or not text.strip():
        return None

    text = text.strip()
    token_count = count_tokens(text, model)

    # If text fits in one request, create single embedding
    if token_count <= EMBEDDING_TRUNCATE_TOKENS:
        return _create_embedding_with_retry(client, text, model)

    # Long text - use chunking to preserve all information
    from public_company_graph.embeddings.chunking import create_embedding_with_chunking

    return create_embedding_with_chunking(
        client=client,
        text=text,
        model=model,
        create_embedding_fn=lambda c, t, m: _create_embedding_with_retry(c, t, m),
    )


def _create_embedding_with_retry(client: OpenAI, text: str, model: str) -> list[float] | None:
    """Internal function with retry logic for embedding creation."""
    from public_company_graph.retry import retry_openai

    @retry_openai
    def _call_api():
        response = client.embeddings.create(model=model, input=text)
        return response.data[0].embedding

    try:
        result = _call_api()
        return list(result) if result is not None else None
    except Exception as e:
        logging.warning(f"Error creating embedding after retries: {e}")
        return None


def create_embeddings_batch(
    client: OpenAI,
    texts: list[str],
    model: str = EMBEDDING_MODEL,
    max_tokens_per_batch: int = 40_000,  # Very conservative due to massive tiktoken discrepancies
    on_batch_complete: Callable[[list[int], list[list[float]], list[str]], None]
    | None = None,  # Callback: (indices, embeddings, texts) -> None
) -> list[list[float] | None]:
    """
    Create embeddings for multiple texts in batches.

    OpenAI's embedding API accepts an array of inputs, returning all embeddings
    in a single response.

    Note: OpenAI has a 300K token limit per request. We use 40K as our limit because
    tiktoken can undercount by up to 7x in some cases (observed 126K estimate vs 933K actual).
    With a 7x safety factor: 40K * 7 = 280K, still under 300K.

    Batches are formed based purely on token counts - texts are added to a batch
    until adding another would exceed max_tokens_per_batch.

    Args:
        client: OpenAI client instance
        texts: List of texts to embed
        model: Embedding model name
        max_tokens_per_batch: Max tokens per API call (default: 40K, very conservative)
        on_batch_complete: Optional callback function called after each API batch completes.
                          Signature: (indices: list[int], embeddings: list[list[float]], texts: list[str]) -> None
                          This allows caching/writing embeddings as they're created, not after all are done.

    Returns:
        List of embedding vectors (same order as input texts).
        None for texts that failed or were empty.
    """
    from public_company_graph.retry import retry_openai

    # Log immediately when function is called to diagnose hangs
    logging.info(f"[create_embeddings_batch] Function called with {len(texts):,} texts")

    if not texts:
        return []

    # If callback is provided, we don't need to accumulate all embeddings in memory
    # The callback handles caching/writing immediately, so we can skip the results list
    # This allows processing millions of embeddings without OOM
    use_results_list = on_batch_complete is None
    if use_results_list:
        logging.info(f"[create_embeddings_batch] Creating results list of size {len(texts):,}...")
        results: list[list[float] | None] = [None] * len(texts)
        logging.info("[create_embeddings_batch] Results list created, starting pre-processing...")
    else:
        results: list[list[float] | None] = []  # Empty list when using callback (no accumulation)
        logging.info(
            "[create_embeddings_batch] Using callback mode - no results accumulation, starting pre-processing..."
        )

    # Pre-process: truncate all texts and count their tokens upfront
    # Add progress logging for large batches
    total_texts = len(texts)
    show_preprocess_progress = total_texts > 10000

    if show_preprocess_progress:
        logging.info(f"Pre-processing {total_texts:,} texts (truncating and counting tokens)...")

    processed_texts: list[tuple[int, str, int]] = []  # (original_idx, text, token_count)
    preprocess_start = time.time()
    last_log_time = preprocess_start

    for i, text in enumerate(texts):
        if text and text.strip():
            truncated = truncate_to_token_limit(text.strip(), EMBEDDING_TRUNCATE_TOKENS, model)
            token_count = count_tokens(truncated, model)
            processed_texts.append((i, truncated, token_count))

        # Log progress every 100K texts or every 10 seconds
        if show_preprocess_progress and (i + 1) % 100000 == 0:
            elapsed = time.time() - preprocess_start
            rate = (i + 1) / elapsed if elapsed > 0 else 0
            remaining = (total_texts - (i + 1)) / rate if rate > 0 else 0
            logging.info(
                f"  Pre-processing: {i + 1:,}/{total_texts:,} texts "
                f"({(i + 1) / total_texts * 100:.1f}%) | "
                f"Rate: {rate:.0f} texts/sec | ETA: {remaining / 60:.1f}min"
            )
        elif show_preprocess_progress:
            current_time = time.time()
            if current_time - last_log_time >= 10:  # Log every 10 seconds
                elapsed = current_time - preprocess_start
                rate = (i + 1) / elapsed if elapsed > 0 else 0
                remaining = (total_texts - (i + 1)) / rate if rate > 0 else 0
                logging.info(
                    f"  Pre-processing: {i + 1:,}/{total_texts:,} texts "
                    f"({(i + 1) / total_texts * 100:.1f}%) | "
                    f"Rate: {rate:.0f} texts/sec | ETA: {remaining / 60:.1f}min"
                )
                last_log_time = current_time

    if show_preprocess_progress:
        elapsed = time.time() - preprocess_start
        logging.info(
            f"  ✓ Pre-processing complete: {len(processed_texts):,} texts in {elapsed:.1f}s"
        )

    if not processed_texts:
        # Return empty list if using callback (no accumulation), otherwise return results
        return results if use_results_list else []

    # Build batches based on token counts
    batches: list[list[tuple[int, str, int]]] = []
    current_batch: list[tuple[int, str, int]] = []
    current_batch_tokens = 0

    for item in processed_texts:
        original_idx, text, token_count = item

        # Start new batch if adding this text would exceed token limit
        if current_batch and current_batch_tokens + token_count > max_tokens_per_batch:
            batches.append(current_batch)
            current_batch = []
            current_batch_tokens = 0

        current_batch.append(item)
        current_batch_tokens += token_count

    # Don't forget the last batch
    if current_batch:
        batches.append(current_batch)

    logging.info(
        f"Token-based batching: {len(processed_texts):,} texts -> {len(batches):,} batches "
        f"(max {max_tokens_per_batch:,} tokens per batch)"
    )

    # Time-based progress logging
    start_time = time.time()
    last_log_time = start_time
    total_batches = len(batches)
    completed_texts = 0
    total_texts_count = len(processed_texts)

    # Process each batch with tqdm progress bar for console
    # Use sys.stderr to avoid conflicts with logging output

    # Only show progress bar if we're in a TTY (interactive terminal)
    # and have significant work to do
    is_tty = hasattr(sys.stderr, "isatty") and sys.stderr.isatty()
    show_progress = is_tty and total_batches >= 5

    # Rate limiting: OpenAI allows ~100 requests/sec for embeddings
    # Use 95 req/sec for better throughput (still safe headroom)
    min_request_interval = 1.0 / 95.0  # 95 requests per second max (increased from 80)
    last_request_time = 0.0

    # Log batch processing start (to file only - use debug to avoid console)
    logging.debug(
        f"Processing {total_batches:,} batches ({total_texts_count:,} texts total) "
        f"with rate limiting ({1.0 / min_request_interval:.0f} req/sec max)"
    )

    # Temporarily suppress console logging during tqdm to prevent interference
    # Find and disable console handlers that write to stdout/stderr
    root_logger = logging.getLogger()
    pkg_logger = logging.getLogger("public_company_graph")
    original_levels = []

    for logger_instance in [root_logger, pkg_logger]:
        for handler in logger_instance.handlers:
            # Check if this is a console handler (StreamHandler writing to stdout/stderr)
            if isinstance(handler, logging.StreamHandler) and not isinstance(
                handler, logging.FileHandler
            ):
                if hasattr(handler, "stream") and handler.stream in (sys.stdout, sys.stderr):
                    original_levels.append((handler, handler.level))
                    handler.setLevel(logging.ERROR)  # Suppress INFO/DEBUG during progress

    try:
        with tqdm(
            total=total_texts_count,
            desc="Embedding texts",
            unit="text",
            file=sys.stderr,  # Write to stderr
            disable=not show_progress,
            ncols=100,  # Fixed width
            mininterval=2.0,  # Update at most once per 2 seconds (reduce flicker)
            maxinterval=10.0,  # Force update every 10 seconds max
            dynamic_ncols=False,
            leave=True,
            position=0,  # Always use position 0
            smoothing=0.1,  # Smooth progress updates (reduce jitter)
        ) as pbar:
            for batch_idx, batch in enumerate(batches):
                valid_indices = [item[0] for item in batch]
                valid_texts = [item[1] for item in batch]
                token_counts = [item[2] for item in batch]
                batch_token_count = sum(token_counts)
                max_text_tokens = max(token_counts) if token_counts else 0

                logging.debug(
                    f"Batch {batch_idx + 1}/{len(batches)}: {len(valid_texts)} texts, "
                    f"{batch_token_count:,} tokens (avg: {batch_token_count // len(valid_texts):,}, "
                    f"max: {max_text_tokens:,})"
                )

                # CRITICAL: Bind loop variables as default arguments to avoid closure bugs
                # Python closures capture by reference, not value - without binding,
                # the closure could use values from a different loop iteration
                batch_texts = tuple(valid_texts)  # Immutable copy

                # Rate limiting: ensure minimum interval between API calls
                current_time = time.time()
                time_since_last = current_time - last_request_time
                if time_since_last < min_request_interval:
                    sleep_time = min_request_interval - time_since_last
                    time.sleep(sleep_time)
                last_request_time = time.time()

                @retry_openai
                def _call_batch_api(
                    _texts: tuple = batch_texts,  # Bind to current iteration's value
                    _expected_tokens: int = batch_token_count,
                ):
                    logging.debug(
                        f"API call sending {len(_texts)} texts, expect ~{_expected_tokens:,} tokens"
                    )
                    response = client.embeddings.create(model=model, input=list(_texts))
                    # Log actual token usage for debugging (safely handle mock objects)
                    try:
                        if (
                            hasattr(response, "usage")
                            and response.usage is not None
                            and hasattr(response.usage, "total_tokens")
                        ):
                            actual = response.usage.total_tokens
                            # Ensure actual is a number (handles mock objects)
                            if isinstance(actual, (int, float)) and _expected_tokens > 0:
                                ratio = actual / _expected_tokens
                                if ratio > 1.5:
                                    logging.warning(
                                        f"TOKEN DISCREPANCY: API used {actual:,} tokens, "
                                        f"expected {_expected_tokens:,} ({ratio:.1f}x)"
                                    )
                    except (TypeError, AttributeError):
                        pass  # Skip usage logging if response doesn't have expected structure
                    return response.data

                try:
                    response_data = _call_batch_api()
                    # Map embeddings back to original indices
                    batch_embeddings = []
                    batch_indices = []
                    for i, embedding_obj in enumerate(response_data):
                        original_idx = valid_indices[i]
                        embedding = list(embedding_obj.embedding)
                        batch_embeddings.append(embedding)
                        batch_indices.append(original_idx)

                        # Only store in results if not using callback (to avoid OOM)
                        if use_results_list:
                            results[original_idx] = embedding

                    # Call callback immediately after batch is complete (for caching/writing)
                    # With callback, we don't accumulate embeddings - they're cached/written immediately
                    if on_batch_complete:
                        batch_texts_for_callback = [valid_texts[i] for i in range(len(valid_texts))]
                        on_batch_complete(batch_indices, batch_embeddings, batch_texts_for_callback)

                    completed_texts += len(valid_texts)
                    pbar.update(len(valid_texts))
                except Exception as e:
                    # Include our token estimate vs what OpenAI reported for debugging
                    logging.warning(
                        f"Batch {batch_idx + 1}/{len(batches)} embedding failed: {e}. "
                        f"Our token estimate: {batch_token_count:,} ({len(valid_texts)} texts, "
                        f"avg: {batch_token_count // len(valid_texts):,}/text, "
                        f"max: {max_text_tokens:,}/text)"
                    )
                    # Fall back to individual calls for this batch
                    individual_embeddings = []
                    individual_indices = []
                    individual_texts = []
                    for i, text in enumerate(valid_texts):
                        original_idx = valid_indices[i]
                        try:
                            embedding = _create_embedding_with_retry(client, text, model)
                            if embedding:
                                embedding_list = list(embedding)
                                individual_embeddings.append(embedding_list)
                                individual_indices.append(original_idx)
                                individual_texts.append(text)

                                # Only store in results if not using callback (to avoid OOM)
                                if use_results_list:
                                    results[original_idx] = embedding_list

                            completed_texts += 1
                            pbar.update(1)
                        except Exception as e2:
                            logging.warning(
                                f"Individual embedding also failed for index {original_idx}: {e2}"
                            )
                            pbar.update(1)  # Still count as processed

                    # Call callback for individual embeddings too
                    if on_batch_complete and individual_embeddings:
                        on_batch_complete(
                            individual_indices, individual_embeddings, individual_texts
                        )

                # Time-based progress logging (every 30 seconds to log file only)
                # Use debug level to avoid interfering with tqdm progress bar
                current_time = time.time()
                if current_time - last_log_time >= 30:
                    elapsed = current_time - start_time
                    rate = completed_texts / elapsed if elapsed > 0 else 0
                    total_texts = len(processed_texts)
                    remaining = (total_texts - completed_texts) / rate if rate > 0 else 0
                    pct = (completed_texts / total_texts * 100) if total_texts > 0 else 0
                    # Use DEBUG level - progress visible in log file, tqdm handles console
                    logging.debug(
                        f"  Batch embedding progress: {completed_texts:,}/{total_texts:,} texts "
                        f"({pct:.1f}%) | Batch {batch_idx + 1}/{total_batches} | "
                        f"Rate: {rate:.1f} texts/sec | ETA: {remaining / 60:.1f}min"
                    )
                    last_log_time = current_time

    finally:
        # Restore original console handler levels
        for handler, original_level in original_levels:
            handler.setLevel(original_level)

    return results


def suppress_http_logging():
    """
    Suppress verbose HTTP logging from OpenAI, httpx, and httpcore.

    Note: If setup_logging() from public_company_graph.cli has been called,
    these loggers are already suppressed. This function is safe to call
    redundantly for scripts that may not use setup_logging().
    """
    logging.getLogger("httpx").setLevel(logging.ERROR)
    logging.getLogger("openai").setLevel(logging.ERROR)
    logging.getLogger("httpcore").setLevel(logging.ERROR)
