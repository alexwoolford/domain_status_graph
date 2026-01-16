"""
Diagnostic tests for embedding cache behavior.

These tests help identify caching issues by analyzing:
1. Cache hit rates with real data
2. Long text vs short text distribution
3. Expected processing time estimates
"""

import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

from public_company_graph.cache import AppCache
from public_company_graph.embeddings.create import create_embeddings_for_nodes
from public_company_graph.embeddings.openai_client import (
    EMBEDDING_TRUNCATE_TOKENS,
    count_tokens,
)


def mock_async_embedding_function(mock_client):
    """Create a mock async embedding function that calls the sync version for testing."""
    from public_company_graph.embeddings.openai_client import create_embeddings_batch

    async def mock_async_embed(client, texts, model, max_concurrent=None, **kwargs):
        # Convert async to sync for testing - call the sync version
        kwargs.pop("max_concurrent", None)
        return create_embeddings_batch(mock_client, texts, model, **kwargs)

    return mock_async_embed


class MockNeo4jSession:
    """Mock Neo4j session for testing."""

    def __init__(self, nodes):
        self.nodes = nodes
        self.updates = []

    def run(self, query, **kwargs):
        if "RETURN" in query and "count" in query.lower() and "total" in query.lower():
            # It's a count query (e.g., "RETURN count(n) AS total")
            # Filter nodes based on query conditions (but skip embedding filter for count)
            filtered_nodes = self._filter_nodes_by_query(query, kwargs, skip_embedding_filter=True)
            count = len(filtered_nodes) if filtered_nodes else 0
            return MockResult([{"total": count}])
        elif "RETURN" in query and "key" in query and "text" not in query.lower():
            # It's a read query for keys only (e.g., "RETURN n.key AS key")
            # Filter nodes based on query conditions (e.g., embedding_property IS NULL)
            filtered_nodes = self._filter_nodes_by_query(query, kwargs)
            # Extract just the key property
            records = [
                {"key": node.get("key", node.get("chunk_id", ""))} for node in filtered_nodes
            ]
            return MockResult(records)
        elif "RETURN" in query and "text" in query.lower():
            # It's a read query for keys and/or text
            filtered_nodes = self._filter_nodes_by_query(query, kwargs)
            records = []
            for node in filtered_nodes:
                record = {}
                if "key" in query.lower() or "AS key" in query:
                    record["key"] = node.get("key", node.get("chunk_id", ""))
                if "text" in query.lower():
                    record["text"] = node.get("text", "")
                records.append(record)
            return MockResult(records)
        elif "UNWIND" in query:
            self.updates.extend(kwargs.get("batch", []))
            return MockResult([])
        return MockResult([])

    def _filter_nodes_by_query(
        self, query: str, kwargs: dict, skip_embedding_filter: bool = False
    ) -> list:
        """Filter nodes based on query conditions."""
        import re

        filtered = self.nodes.copy()

        # Find embedding_property name (e.g., "description_embedding" from "n.description_embedding IS NULL")
        if not skip_embedding_filter:
            is_null_matches = list(re.finditer(r"n\.(\w+)\s+IS\s+NULL", query))
            embedding_property = None
            if is_null_matches:
                embedding_property = is_null_matches[-1].group(1)

            # Filter by embedding_property IS NULL (nodes without embeddings)
            if embedding_property:
                filtered = [
                    n
                    for n in filtered
                    if embedding_property not in n or n.get(embedding_property) is None
                ]

        # Filter by text_property IS NOT NULL and not empty
        if "IS NOT NULL" in query:
            filtered = [
                n
                for n in filtered
                if "text" in n
                and n.get("text") is not None
                and str(n.get("text", "")).strip() != ""
            ]

        # Filter by text_property <> '' (not empty string)
        if "<>" in query and "''" in query:
            filtered = [n for n in filtered if "text" in n and str(n.get("text", "")).strip() != ""]

        # Filter by size (minimum length)
        if "size(" in query.lower() and ">=" in query:
            size_match = re.search(r"size\([^)]+\)\s*>=\s*\$(\w+)", query)
            if size_match:
                param_name = size_match.group(1)
                min_length = kwargs.get(param_name, 0)
                filtered = [
                    n
                    for n in filtered
                    if "text" in n and len(str(n.get("text", "")).strip()) >= min_length
                ]

        # Filter by key > last_key (cursor-based pagination)
        if ">" in query and "last_key" in query and "key" in query:
            last_key = kwargs.get("last_key")
            if last_key:
                filtered = [n for n in filtered if n.get("key", "") > last_key]

        # Filter by key IN $keys (for text fetching queries)
        if "IN $keys" in query or "IN $keys" in query.replace(" ", ""):
            keys = kwargs.get("keys", [])
            if keys:
                filtered = [n for n in filtered if n.get("key") in keys]

        # Apply LIMIT if present
        if "LIMIT" in query.upper():
            limit_match = re.search(r"LIMIT\s+\$(\w+)", query)
            if limit_match:
                param_name = limit_match.group(1)
                limit = kwargs.get(param_name, len(filtered))
                filtered = filtered[:limit]
            else:
                limit_match = re.search(r"LIMIT\s+(\d+)", query)
                if limit_match:
                    limit = int(limit_match.group(1))
                    filtered = filtered[:limit]

        return filtered

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass


class MockRecord:
    """Mock Neo4j record that avoids closure bugs."""

    def __init__(self, data):
        self._data = data

    def __getitem__(self, key):
        return self._data[key]


class MockResult:
    def __init__(self, records):
        self.records = [MockRecord(r) for r in records]

    def __iter__(self):
        return iter(self.records)

    def single(self):
        """Return the first record or None if empty (matches Neo4j API)."""
        return self.records[0] if self.records else None


class MockDriver:
    def __init__(self, nodes):
        self.nodes = nodes
        self._session = None

    def session(self, database=None):
        self._session = MockNeo4jSession(self.nodes)
        return self._session


def create_mock_client():
    """Create mock OpenAI client tracking all API calls."""
    client = MagicMock()
    call_log = {"calls": [], "total_texts": 0}

    def mock_create(**kwargs):
        texts = kwargs.get("input", [])
        if isinstance(texts, str):
            texts = [texts]

        call_log["calls"].append({"texts": texts, "count": len(texts)})
        call_log["total_texts"] += len(texts)

        response = MagicMock()
        response.data = []
        for text in texts:
            obj = MagicMock()
            base = hash(text) % 1000 / 1000.0
            obj.embedding = [base + j * 0.001 for j in range(1536)]
            response.data.append(obj)
        return response

    client.embeddings.create = mock_create
    client._call_log = call_log
    return client


class TestLongTextChunkingBehavior:
    """Tests verifying that long texts trigger chunking (more API calls)."""

    def test_short_text_uses_batch_api(self):
        """Short texts should use efficient batch API (fewer calls)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = AppCache(Path(tmpdir) / "cache")

            # Create 5 short texts (well under 8000 token limit)
            short_text = "This is a short company description. " * 100  # ~500 tokens
            nodes = [{"key": f"short{i}.com", "text": short_text} for i in range(5)]

            driver = MockDriver(nodes)
            client = create_mock_client()

            with (
                patch("public_company_graph.embeddings.create.BATCH_SIZE_SMALL", 100),
                patch(
                    "public_company_graph.embeddings.openai_client_async.create_embeddings_batch_async",
                    side_effect=mock_async_embedding_function(client),
                ),
            ):
                create_embeddings_for_nodes(
                    driver=driver,
                    cache=cache,
                    node_label="Domain",
                    text_property="description",
                    key_property="key",
                    openai_client=client,
                    execute=True,
                )

            # Should be efficient - 5 texts in ~1 batch call
            total_calls = len(client._call_log["calls"])
            assert total_calls == 1, (
                f"Short texts should batch: got {total_calls} calls for 5 texts"
            )
            cache.close()

    def test_long_text_uses_batched_chunking(self):
        """Long texts should use batched chunking (efficient API usage)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = AppCache(Path(tmpdir) / "cache")

            # Create text that significantly exceeds 8000 token limit
            # Need well over the limit to ensure chunking kicks in
            # ~4 chars per token, so 80K chars = ~20K tokens
            long_text = "This is a very detailed business description with lots of content. " * 2000

            # Verify this text actually exceeds the limit significantly
            token_count = count_tokens(long_text, "text-embedding-3-small")
            assert token_count > EMBEDDING_TRUNCATE_TOKENS * 1.5, (
                f"Test setup error: text has {token_count} tokens, "
                f"should significantly exceed {EMBEDDING_TRUNCATE_TOKENS}"
            )

            nodes = [{"key": "long.com", "text": long_text}]
            driver = MockDriver(nodes)
            client = create_mock_client()

            with (
                patch("public_company_graph.embeddings.create.BATCH_SIZE_SMALL", 100),
                patch(
                    "public_company_graph.embeddings.openai_client_async.create_embeddings_batch_async",
                    side_effect=mock_async_embedding_function(client),
                ),
            ):
                create_embeddings_for_nodes(
                    driver=driver,
                    cache=cache,
                    node_label="Domain",
                    text_property="description",
                    key_property="key",
                    openai_client=client,
                    execute=True,
                )

            # With batched chunking, all chunks go in a single API call
            total_calls = len(client._call_log["calls"])
            assert total_calls >= 1, "Should make at least one API call"

            # But we should have processed multiple texts (the chunks)
            # Check that all chunks were batched together
            total_texts = client._call_log["total_texts"]
            assert total_texts >= 2, (
                f"Long text should be chunked into multiple pieces: "
                f"got {total_texts} texts embedded for {token_count} token text"
            )
            cache.close()

    def test_cached_long_text_skips_chunking(self):
        """Cached long text should NOT trigger any API calls."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = AppCache(Path(tmpdir) / "cache")

            # Pre-cache a long text's embedding
            cache.set(
                "embeddings",
                "cached-long.com:description",
                {
                    "embedding": [0.1] * 1536,
                    "text": "This would be very long text..." * 1000,
                    "model": "text-embedding-3-small",
                    "dimension": 1536,
                },
            )

            nodes = [{"key": "cached-long.com", "text": "This would be very long text..." * 1000}]
            driver = MockDriver(nodes)
            client = create_mock_client()

            with patch("public_company_graph.embeddings.create.BATCH_SIZE_SMALL", 100):
                processed, created, cached_count, failed = create_embeddings_for_nodes(
                    driver=driver,
                    cache=cache,
                    node_label="Domain",
                    text_property="description",
                    key_property="key",
                    openai_client=client,
                    execute=True,
                )

            # Should use cache, NO API calls
            assert client._call_log["total_texts"] == 0, "Cached embedding should not trigger API"
            assert cached_count == 1, "Should report 1 cache hit"
            assert created == 0, "Should not create new embedding"
            cache.close()


class TestCacheHitRateCalculation:
    """Tests for calculating cache hit rates."""

    def test_cache_hit_rate_reporting(self):
        """Verify cache hit rate is correctly calculated."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = AppCache(Path(tmpdir) / "cache")

            # Create descriptions long enough to pass MIN_DESCRIPTION_LENGTH_FOR_SIMILARITY (200 chars)
            def make_description(i: int) -> str:
                base = f"Company {i} is a technology firm that provides innovative solutions. "
                return base * 5  # ~300+ characters

            # Pre-cache 3 of 5 embeddings
            for i in range(3):
                desc = make_description(i)
                cache.set(
                    "embeddings",
                    f"domain{i}.com:description",
                    {
                        "embedding": [0.1] * 1536,
                        "text": desc,
                        "model": "text-embedding-3-small",
                        "dimension": 1536,
                    },
                )

            # Create 5 nodes (3 cached, 2 new) - all with sufficient description length
            nodes = [{"key": f"domain{i}.com", "text": make_description(i)} for i in range(5)]
            driver = MockDriver(nodes)
            client = create_mock_client()

            with (
                patch("public_company_graph.embeddings.create.BATCH_SIZE_SMALL", 100),
                patch(
                    "public_company_graph.embeddings.openai_client_async.create_embeddings_batch_async",
                    side_effect=mock_async_embedding_function(client),
                ),
            ):
                processed, created, cached_count, failed = create_embeddings_for_nodes(
                    driver=driver,
                    cache=cache,
                    node_label="Domain",
                    text_property="description",
                    key_property="key",
                    openai_client=client,
                    execute=True,
                )

            # Verify hit rate calculation
            hit_rate = (
                cached_count / (cached_count + created) * 100 if (cached_count + created) > 0 else 0
            )
            assert cached_count == 3, "Should have 3 cache hits"
            assert created == 2, "Should create 2 new embeddings"
            assert hit_rate == 60.0, f"Cache hit rate should be 60%, got {hit_rate}%"
            cache.close()


class TestCacheKeyConsistency:
    """Tests verifying cache key consistency between runs."""

    def test_cache_lookup_consistency(self):
        """Verify cache lookup uses same key format as cache storage."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = AppCache(Path(tmpdir) / "cache")

            # Store with explicit key
            store_key = "COMPANY123:description"
            cache.set("embeddings", store_key, {"embedding": [0.5] * 1536})

            # Retrieve with same format
            lookup_key = "COMPANY123:description"
            result = cache.get("embeddings", lookup_key)

            assert result is not None, "Cache lookup should find stored embedding"
            assert result["embedding"][0] == 0.5
            cache.close()


class TestTokenBasedBatching:
    """Tests verifying token-based batching in create_embeddings_batch."""

    def test_batch_splits_on_token_limit(self):
        """Verify that batches are split based on token counts, not text counts."""
        from public_company_graph.embeddings.openai_client import create_embeddings_batch

        client = create_mock_client()

        # Create texts that when combined exceed a small token limit
        # Each text is ~200 tokens (50 words × ~4 tokens/word)
        text1 = "The company provides enterprise software solutions. " * 50  # ~200 tokens
        text2 = "Our business focuses on cloud computing services. " * 50  # ~200 tokens
        text3 = "We deliver innovative technology products globally. " * 50  # ~200 tokens

        # Use a small token limit that forces splitting
        # With max_tokens_per_batch=300, texts should be batched separately
        results = create_embeddings_batch(
            client,
            [text1, text2, text3],
            "text-embedding-3-small",
            max_tokens_per_batch=300,  # Very small - forces separate batches
        )

        # Should get 3 embeddings back
        assert len(results) == 3
        assert all(r is not None for r in results)

        # With such a small limit, should have made multiple API calls
        # (each text gets its own batch because ~200 tokens each)
        assert client._call_log["total_texts"] == 3

    def test_batch_combines_small_texts(self):
        """Small texts should be combined into single batch when under token limit."""
        from public_company_graph.embeddings.openai_client import create_embeddings_batch

        client = create_mock_client()

        # Create very short texts
        texts = [
            "Small company A.",
            "Small company B.",
            "Small company C.",
        ]

        # Use default (large) token limit
        results = create_embeddings_batch(client, texts, "text-embedding-3-small")

        # All 3 should be in one batch call
        assert len(results) == 3
        assert len(client._call_log["calls"]) == 1  # Single batch
        assert client._call_log["calls"][0]["count"] == 3

    def test_empty_texts_return_none(self):
        """Empty texts should return None without API calls."""
        from public_company_graph.embeddings.openai_client import create_embeddings_batch

        client = create_mock_client()

        results = create_embeddings_batch(
            client,
            ["Valid text here.", "", "   ", "Another valid."],
            "text-embedding-3-small",
        )

        assert len(results) == 4
        # Valid texts get embeddings
        assert results[0] is not None
        assert results[3] is not None
        # Empty/whitespace texts get None
        assert results[1] is None
        assert results[2] is None
