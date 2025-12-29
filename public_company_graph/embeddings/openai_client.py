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
    use_chunking: bool = True,  # Default: use chunking (preserves all info) instead of truncation
) -> list[float] | None:
    """
    Create an embedding for a single text using OpenAI.

    For long texts, uses chunking to preserve all information instead of truncating.
    Chunks are aggregated using weighted average (earlier chunks weighted higher).

    Args:
        client: OpenAI client instance
        text: Text to embed
        model: Embedding model name
        use_chunking: If True, use chunking for long texts. If False, truncate.

    Returns:
        Embedding vector or None if creation failed
    """
    if not text or not text.strip():
        return None

    text = text.strip()
    token_count = count_tokens(text, model)

    # If text fits in one chunk, create single embedding
    if token_count <= EMBEDDING_TRUNCATE_TOKENS:
        return _create_embedding_with_retry(client, text, model)

    # Text exceeds limit - use chunking if enabled
    if use_chunking:
        from public_company_graph.embeddings.chunking import create_embedding_with_chunking

        return create_embedding_with_chunking(
            client=client,
            text=text,
            model=model,
            create_embedding_fn=lambda c, t, m: _create_embedding_with_retry(c, t, m),
        )
    else:
        # Fallback to truncation (legacy behavior)
        truncated_text = truncate_to_token_limit(
            text, max_tokens=EMBEDDING_TRUNCATE_TOKENS, model=model
        )

        if len(truncated_text) < len(text):
            logging.info(
                f"Truncated text from {token_count:,} tokens to {EMBEDDING_TRUNCATE_TOKENS:,} tokens "
                f"({len(text):,} chars → {len(truncated_text):,} chars)"
            )

        return _create_embedding_with_retry(client, truncated_text, model)


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
    batch_size: int = 20,  # Conservative: ~20 texts * ~8K tokens = ~160K tokens (under 300K limit)
) -> list[list[float] | None]:
    """
    Create embeddings for multiple texts in batches (much faster than one-at-a-time).

    OpenAI's embedding API accepts an array of inputs, returning all embeddings
    in a single response. This amortizes HTTP overhead across many texts.

    Note: OpenAI has a 300K token limit per request. With company descriptions
    averaging ~5-8K tokens, batch_size=20 keeps us safely under the limit.

    Args:
        client: OpenAI client instance
        texts: List of texts to embed
        model: Embedding model name
        batch_size: Number of texts per API call (default: 20, safe for long texts)

    Returns:
        List of embedding vectors (same order as input texts).
        None for texts that failed or were empty.
    """
    from public_company_graph.retry import retry_openai

    if not texts:
        return []

    results: list[list[float] | None] = [None] * len(texts)

    # Process in batches
    for batch_start in range(0, len(texts), batch_size):
        batch_end = min(batch_start + batch_size, len(texts))
        batch_texts = texts[batch_start:batch_end]

        # Filter out empty texts, keeping track of original indices
        valid_indices = []
        valid_texts = []
        for i, text in enumerate(batch_texts):
            if text and text.strip():
                # Truncate long texts to fit token limit
                truncated = truncate_to_token_limit(text.strip(), EMBEDDING_TRUNCATE_TOKENS, model)
                valid_texts.append(truncated)
                valid_indices.append(batch_start + i)

        if not valid_texts:
            continue

        # Pre-flight check: validate total tokens before API call
        # This helps catch issues before they hit OpenAI's 300K limit
        batch_token_count = sum(count_tokens(t, model) for t in valid_texts)
        MAX_TOKENS_PER_REQUEST = 300_000

        if batch_token_count > MAX_TOKENS_PER_REQUEST:
            logging.warning(
                f"Batch {batch_start}-{batch_end} has {batch_token_count:,} tokens "
                f"(exceeds {MAX_TOKENS_PER_REQUEST:,} limit). "
                f"Texts: {len(valid_texts)}, avg: {batch_token_count // len(valid_texts):,}/text. "
                f"Falling back to individual calls."
            )
            # Fall back to individual calls for this oversized batch
            for i, text in enumerate(valid_texts):
                original_idx = valid_indices[i]
                try:
                    embedding = _create_embedding_with_retry(client, text, model)
                    results[original_idx] = embedding
                except Exception as e:
                    logging.warning(f"Individual embedding failed for index {original_idx}: {e}")
            continue

        @retry_openai
        def _call_batch_api(texts=valid_texts):
            response = client.embeddings.create(model=model, input=texts)
            return response.data

        try:
            response_data = _call_batch_api()
            # Map embeddings back to original indices
            for i, embedding_obj in enumerate(response_data):
                original_idx = valid_indices[i]
                results[original_idx] = list(embedding_obj.embedding)
        except Exception as e:
            logging.warning(f"Batch embedding failed for texts {batch_start}-{batch_end}: {e}")
            # Fall back to individual calls for this batch
            for i, text in enumerate(valid_texts):
                original_idx = valid_indices[i]
                try:
                    embedding = _create_embedding_with_retry(client, text, model)
                    results[original_idx] = embedding
                except Exception as e2:
                    logging.warning(
                        f"Individual embedding also failed for index {original_idx}: {e2}"
                    )

    return results


def suppress_http_logging():
    """Suppress verbose HTTP logging from OpenAI, httpx, and httpcore."""
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("openai").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
