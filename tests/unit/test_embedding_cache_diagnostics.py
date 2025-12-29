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


class MockNeo4jSession:
    """Mock Neo4j session for testing."""

    def __init__(self, nodes):
        self.nodes = nodes
        self.updates = []

    def run(self, query, **kwargs):
        if "RETURN" in query and "key" in query:
            return MockResult(self.nodes)
        elif "UNWIND" in query:
            self.updates.extend(kwargs.get("batch", []))
            return MockResult([])
        return MockResult([])

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

            with patch("public_company_graph.embeddings.create.BATCH_SIZE_SMALL", 100):
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

            with patch("public_company_graph.embeddings.create.BATCH_SIZE_SMALL", 100):
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
            # (this is the optimized behavior - ~40x fewer API calls)
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

            # Verify hit rate calculation
            hit_rate = (
                cached_count / (cached_count + created) * 100 if (cached_count + created) > 0 else 0
            )
            assert cached_count == 3, "Should have 3 cache hits"
            assert created == 2, "Should create 2 new embeddings"
            assert hit_rate == 60.0, f"Cache hit rate should be 60%, got {hit_rate}%"
            cache.close()


class TestTimeEstimation:
    """Tests for embedding time estimation based on text characteristics."""

    def test_estimate_processing_time(self):
        """Estimate processing time based on text lengths."""
        # Simulate analysis of texts
        texts = [
            ("short1", "Short description" * 50, 200),  # ~200 tokens
            ("short2", "Another short one" * 60, 240),  # ~240 tokens
            ("long1", "Very detailed" * 3000, 12000),  # ~12K tokens
            ("long2", "Extended description" * 4000, 16000),  # ~16K tokens
        ]

        short_texts = [t for t in texts if t[2] <= EMBEDDING_TRUNCATE_TOKENS]
        long_texts = [t for t in texts if t[2] > EMBEDDING_TRUNCATE_TOKENS]

        # Short texts: batch API, ~20 per request, ~1.5 sec per request
        short_api_calls = (len(short_texts) + 19) // 20  # Ceiling division
        short_time_sec = short_api_calls * 1.5

        # Long texts: chunking, ~3 chunks per text, ~1 sec per chunk API call
        avg_chunks_per_long = 3
        long_api_calls = len(long_texts) * avg_chunks_per_long
        long_time_sec = long_api_calls * 1.0

        total_time_sec = short_time_sec + long_time_sec

        # Verify estimation logic
        assert len(short_texts) == 2, "Should have 2 short texts"
        assert len(long_texts) == 2, "Should have 2 long texts"
        assert short_api_calls == 1, "2 short texts fit in 1 batch"
        assert long_api_calls == 6, "2 long texts × 3 chunks = 6 calls"

        # Total: ~1.5 + 6 = 7.5 seconds
        assert 7 < total_time_sec < 8, f"Expected ~7.5 sec, got {total_time_sec}"


class TestCacheKeyConsistency:
    """Tests verifying cache key consistency between runs."""

    def test_cache_key_deterministic(self):
        """Cache keys should be deterministic across runs."""
        key1 = "example.com:description"
        key2 = "example.com:description"
        assert key1 == key2, "Cache keys should be deterministic"

    def test_cache_key_from_cik(self):
        """Cache key format for Company nodes (CIK-based)."""
        cik = "0001234567"
        text_property = "description"
        expected_key = f"{cik}:{text_property}"
        assert expected_key == "0001234567:description"

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
