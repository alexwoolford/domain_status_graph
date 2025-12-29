"""
Unit tests for embedding caching functionality.

Tests that:
1. Embeddings are correctly cached and retrieved
2. Cache hits avoid API calls
3. Cache key format is correct
4. Model/dimension validation works
5. Running embedding creation twice results in 100% cache hits
"""

import tempfile
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from public_company_graph.cache import AppCache
from public_company_graph.embeddings.create import (
    create_embeddings_for_nodes,
)


class MockNeo4jSession:
    """Mock Neo4j session for testing."""

    def __init__(self, nodes: list[dict[str, Any]]):
        self.nodes = nodes
        self.updates: list[dict[str, Any]] = []

    def run(self, query: str, **kwargs):
        """Mock run method."""
        if "RETURN" in query and "key" in query:
            # It's a read query
            return MockResult(self.nodes)
        elif "UNWIND" in query:
            # It's a batch update
            batch = kwargs.get("batch", [])
            self.updates.extend(batch)
            return MockResult([])
        return MockResult([])

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass


class MockResult:
    """Mock Neo4j result."""

    def __init__(self, records: list[dict[str, Any]]):
        self.records = [MockRecord(r) for r in records]
        self._index = 0

    def __iter__(self):
        return iter(self.records)


class MockRecord:
    """Mock Neo4j record."""

    def __init__(self, data: dict[str, Any]):
        self._data = data

    def __getitem__(self, key: str):
        return self._data[key]


class MockDriver:
    """Mock Neo4j driver for testing."""

    def __init__(self, nodes: list[dict[str, Any]]):
        self.nodes = nodes
        self._session = None

    def session(self, database: str | None = None):
        self._session = MockNeo4jSession(self.nodes)
        return self._session

    def get_updates(self) -> list[dict[str, Any]]:
        """Get all updates made through sessions."""
        return self._session.updates if self._session else []


def create_mock_openai_client(embeddings_to_return: dict[str, list[float]]):
    """Create a mock OpenAI client that tracks API calls."""
    client = MagicMock()
    call_count = {"count": 0, "texts": []}

    def mock_create(**kwargs):
        texts = kwargs.get("input", [])
        if isinstance(texts, str):
            texts = [texts]

        call_count["count"] += 1
        call_count["texts"].extend(texts)

        # Create mock response
        response = MagicMock()
        response.data = []
        for _i, text in enumerate(texts):
            embedding_obj = MagicMock()
            # Use provided embedding or generate a deterministic one
            if text in embeddings_to_return:
                embedding_obj.embedding = embeddings_to_return[text]
            else:
                # Deterministic embedding based on text hash
                base = hash(text) % 1000 / 1000.0
                embedding_obj.embedding = [base + j * 0.001 for j in range(1536)]
            response.data.append(embedding_obj)
        return response

    client.embeddings.create = mock_create
    client._call_count = call_count
    return client


class TestEmbeddingCacheKeyFormat:
    """Tests for cache key format and validation."""

    def test_cache_key_format_domain(self):
        """Cache key for Domain nodes uses correct format."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = AppCache(Path(tmpdir) / "cache")

            # Simulate what create_embeddings_for_nodes does
            # Key format: {node_key}:{text_property}
            cache_key = "example.com:description"
            cache.set(
                "embeddings",
                cache_key,
                {"embedding": [0.1] * 1536, "model": "text-embedding-3-small", "dimension": 1536},
            )

            # Verify retrieval
            cached = cache.get("embeddings", cache_key)
            assert cached is not None
            assert len(cached["embedding"]) == 1536
            cache.close()

    def test_cache_key_format_company(self):
        """Cache key for Company nodes uses correct format."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = AppCache(Path(tmpdir) / "cache")

            # Company nodes use CIK as key
            cache_key = "0001234567:description"
            cache.set(
                "embeddings",
                cache_key,
                {"embedding": [0.2] * 1536, "model": "text-embedding-3-small", "dimension": 1536},
            )

            cached = cache.get("embeddings", cache_key)
            assert cached is not None
            assert len(cached["embedding"]) == 1536
            cache.close()


class TestEmbeddingCacheHitMiss:
    """Tests for cache hit/miss behavior."""

    def test_cache_hit_skips_api_call(self):
        """Cached embeddings should not trigger API calls."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = AppCache(Path(tmpdir) / "cache")

            # Pre-populate cache with embeddings
            cache.set(
                "embeddings",
                "example.com:description",
                {
                    "embedding": [0.1] * 1536,
                    "text": "Example company description",
                    "model": "text-embedding-3-small",
                    "dimension": 1536,
                },
            )
            cache.set(
                "embeddings",
                "test.com:description",
                {
                    "embedding": [0.2] * 1536,
                    "text": "Test company description",
                    "model": "text-embedding-3-small",
                    "dimension": 1536,
                },
            )

            # Create mock driver with nodes matching cached keys
            nodes = [
                {"key": "example.com", "text": "Example company description"},
                {"key": "test.com", "text": "Test company description"},
            ]
            driver = MockDriver(nodes)

            # Create mock OpenAI client
            mock_client = create_mock_openai_client({})

            # Run embedding creation
            with patch("public_company_graph.embeddings.create.BATCH_SIZE_SMALL", 100):
                processed, created, cached_count, failed = create_embeddings_for_nodes(
                    driver=driver,
                    cache=cache,
                    node_label="Domain",
                    text_property="description",
                    key_property="key",
                    openai_client=mock_client,
                    execute=True,
                )

            # Verify NO API calls were made (all from cache)
            assert mock_client._call_count["count"] == 0, (
                "API should not be called for cached embeddings"
            )
            assert created == 0, "No new embeddings should be created"
            assert cached_count == 2, "Both embeddings should come from cache"
            assert processed == 2, "Both nodes should be processed"
            cache.close()

    def test_cache_miss_triggers_api_call(self):
        """Non-cached embeddings should trigger API calls."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = AppCache(Path(tmpdir) / "cache")

            # Cache is empty - no pre-populated embeddings

            # Create mock driver with nodes
            nodes = [
                {"key": "new.com", "text": "A new company that needs embedding"},
            ]
            driver = MockDriver(nodes)

            # Create mock OpenAI client
            mock_client = create_mock_openai_client({})

            # Run embedding creation
            with patch("public_company_graph.embeddings.create.BATCH_SIZE_SMALL", 100):
                processed, created, cached_count, failed = create_embeddings_for_nodes(
                    driver=driver,
                    cache=cache,
                    node_label="Domain",
                    text_property="description",
                    key_property="key",
                    openai_client=mock_client,
                    execute=True,
                )

            # Verify API was called
            assert mock_client._call_count["count"] > 0, (
                "API should be called for non-cached embeddings"
            )
            assert created == 1, "One new embedding should be created"
            assert cached_count == 0, "No embeddings from cache"
            cache.close()

    def test_mixed_cache_hit_and_miss(self):
        """Mix of cached and non-cached embeddings."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = AppCache(Path(tmpdir) / "cache")

            # Pre-populate cache with ONE embedding
            cache.set(
                "embeddings",
                "cached.com:description",
                {
                    "embedding": [0.1] * 1536,
                    "text": "Cached description",
                    "model": "text-embedding-3-small",
                    "dimension": 1536,
                },
            )

            # Create mock driver with mixed nodes
            nodes = [
                {"key": "cached.com", "text": "Cached description"},
                {"key": "new.com", "text": "A brand new description that needs embedding"},
            ]
            driver = MockDriver(nodes)

            # Create mock OpenAI client
            mock_client = create_mock_openai_client({})

            # Run embedding creation
            with patch("public_company_graph.embeddings.create.BATCH_SIZE_SMALL", 100):
                processed, created, cached_count, failed = create_embeddings_for_nodes(
                    driver=driver,
                    cache=cache,
                    node_label="Domain",
                    text_property="description",
                    key_property="key",
                    openai_client=mock_client,
                    execute=True,
                )

            # Verify correct behavior
            assert created == 1, "One new embedding should be created"
            assert cached_count == 1, "One embedding from cache"
            assert processed == 2, "Both nodes should be processed"

            # Verify only the new text was sent to API
            assert len(mock_client._call_count["texts"]) == 1
            assert "brand new description" in mock_client._call_count["texts"][0]
            cache.close()


class TestEmbeddingCacheValidation:
    """Tests for cache validation (model/dimension checks)."""

    def test_wrong_model_triggers_recalculation(self):
        """Cached embedding with wrong model should be recalculated."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = AppCache(Path(tmpdir) / "cache")

            # Pre-populate cache with WRONG model
            cache.set(
                "embeddings",
                "example.com:description",
                {
                    "embedding": [0.1] * 1536,
                    "text": "Example description",
                    "model": "text-embedding-ada-002",  # Wrong model!
                    "dimension": 1536,
                },
            )

            nodes = [{"key": "example.com", "text": "Example description"}]
            driver = MockDriver(nodes)
            mock_client = create_mock_openai_client({})

            with patch("public_company_graph.embeddings.create.BATCH_SIZE_SMALL", 100):
                processed, created, cached_count, failed = create_embeddings_for_nodes(
                    driver=driver,
                    cache=cache,
                    node_label="Domain",
                    text_property="description",
                    key_property="key",
                    openai_client=mock_client,
                    embedding_model="text-embedding-3-small",  # Requested model
                    execute=True,
                )

            # Should create new embedding due to model mismatch
            assert created == 1, "Should create new embedding due to model mismatch"
            assert cached_count == 0, "Should not use cached embedding with wrong model"
            cache.close()

    def test_wrong_dimension_triggers_recalculation(self):
        """Cached embedding with wrong dimension should be recalculated."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = AppCache(Path(tmpdir) / "cache")

            # Pre-populate cache with WRONG dimension
            cache.set(
                "embeddings",
                "example.com:description",
                {
                    "embedding": [0.1] * 768,  # Wrong dimension!
                    "text": "Example description",
                    "model": "text-embedding-3-small",
                    "dimension": 768,
                },
            )

            nodes = [{"key": "example.com", "text": "Example description"}]
            driver = MockDriver(nodes)
            mock_client = create_mock_openai_client({})

            with patch("public_company_graph.embeddings.create.BATCH_SIZE_SMALL", 100):
                processed, created, cached_count, failed = create_embeddings_for_nodes(
                    driver=driver,
                    cache=cache,
                    node_label="Domain",
                    text_property="description",
                    key_property="key",
                    openai_client=mock_client,
                    embedding_dimension=1536,  # Requested dimension
                    execute=True,
                )

            # Should create new embedding due to dimension mismatch
            assert created == 1, "Should create new embedding due to dimension mismatch"
            assert cached_count == 0, "Should not use cached embedding with wrong dimension"
            cache.close()

    def test_correct_model_and_dimension_uses_cache(self):
        """Cached embedding with correct model and dimension should be used."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = AppCache(Path(tmpdir) / "cache")

            # Pre-populate cache with correct model and dimension
            cache.set(
                "embeddings",
                "example.com:description",
                {
                    "embedding": [0.1] * 1536,
                    "text": "Example description",
                    "model": "text-embedding-3-small",
                    "dimension": 1536,
                },
            )

            nodes = [{"key": "example.com", "text": "Example description"}]
            driver = MockDriver(nodes)
            mock_client = create_mock_openai_client({})

            with patch("public_company_graph.embeddings.create.BATCH_SIZE_SMALL", 100):
                processed, created, cached_count, failed = create_embeddings_for_nodes(
                    driver=driver,
                    cache=cache,
                    node_label="Domain",
                    text_property="description",
                    key_property="key",
                    openai_client=mock_client,
                    embedding_model="text-embedding-3-small",
                    embedding_dimension=1536,
                    execute=True,
                )

            # Should use cache
            assert created == 0, "Should not create new embedding"
            assert cached_count == 1, "Should use cached embedding"
            assert mock_client._call_count["count"] == 0, "No API calls should be made"
            cache.close()


class TestEmbeddingCacheIdempotency:
    """Tests for idempotent behavior - running twice should use cache."""

    def test_second_run_uses_cache(self):
        """Second run should use 100% cache hits."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = AppCache(Path(tmpdir) / "cache")

            nodes = [
                {"key": "company1.com", "text": "Company one does amazing things"},
                {"key": "company2.com", "text": "Company two makes great products"},
                {"key": "company3.com", "text": "Company three provides services"},
            ]
            driver = MockDriver(nodes)
            mock_client = create_mock_openai_client({})

            # First run - should call API for all
            with patch("public_company_graph.embeddings.create.BATCH_SIZE_SMALL", 100):
                proc1, created1, cached1, failed1 = create_embeddings_for_nodes(
                    driver=driver,
                    cache=cache,
                    node_label="Domain",
                    text_property="description",
                    key_property="key",
                    openai_client=mock_client,
                    execute=True,
                )

            assert mock_client._call_count["count"] > 0, "First run should make API calls"
            assert created1 == 3, "First run should create all 3 embeddings"
            assert cached1 == 0, "First run should have no cache hits"

            # Reset API call tracking
            mock_client._call_count = {"count": 0, "texts": []}

            # Create fresh driver for second run
            driver2 = MockDriver(nodes)

            # Second run - should use cache for all
            with patch("public_company_graph.embeddings.create.BATCH_SIZE_SMALL", 100):
                proc2, created2, cached2, failed2 = create_embeddings_for_nodes(
                    driver=driver2,
                    cache=cache,
                    node_label="Domain",
                    text_property="description",
                    key_property="key",
                    openai_client=mock_client,
                    execute=True,
                )

            # Verify second run uses cache
            assert created2 == 0, "Second run should create 0 new embeddings"
            assert cached2 == 3, "Second run should have 3 cache hits"
            assert mock_client._call_count["count"] == 0, "Second run should make 0 API calls"
            cache.close()


class TestEmbeddingCachePersistence:
    """Tests for cache persistence across sessions."""

    def test_cache_persists_after_close(self):
        """Cache should persist after closing and reopening."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_dir = Path(tmpdir) / "cache"

            # Create and populate cache
            cache1 = AppCache(cache_dir)
            cache1.set(
                "embeddings",
                "persisted.com:description",
                {
                    "embedding": [0.123] * 1536,
                    "text": "Persisted description",
                    "model": "text-embedding-3-small",
                    "dimension": 1536,
                },
            )
            cache1.close()

            # Reopen cache and verify data persists
            cache2 = AppCache(cache_dir)
            cached = cache2.get("embeddings", "persisted.com:description")

            assert cached is not None, "Cache should persist after close/reopen"
            assert cached["embedding"][0] == 0.123
            assert len(cached["embedding"]) == 1536
            cache2.close()


class TestEmbeddingCacheStats:
    """Tests for cache statistics reporting."""

    def test_stats_track_embeddings_namespace(self):
        """Stats should correctly count embeddings namespace."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = AppCache(Path(tmpdir) / "cache")

            # Add some embeddings
            for i in range(5):
                cache.set(
                    "embeddings",
                    f"domain{i}.com:description",
                    {"embedding": [0.1] * 1536, "model": "test", "dimension": 1536},
                )

            # Add something in another namespace
            cache.set("10k_extracted", "12345", {"data": "test"})

            stats = cache.stats()

            assert stats["by_namespace"]["embeddings"] == 5
            assert stats["by_namespace"]["10k_extracted"] == 1
            assert stats["total"] == 6
            cache.close()


class TestEmbeddingCacheEdgeCases:
    """Tests for edge cases in embedding caching."""

    def test_empty_text_not_cached(self):
        """Empty text should not be cached."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = AppCache(Path(tmpdir) / "cache")

            nodes = [
                {"key": "empty.com", "text": ""},  # Empty text
                {"key": "whitespace.com", "text": "   "},  # Whitespace only
            ]
            driver = MockDriver(nodes)
            mock_client = create_mock_openai_client({})

            with patch("public_company_graph.embeddings.create.BATCH_SIZE_SMALL", 100):
                processed, created, cached_count, failed = create_embeddings_for_nodes(
                    driver=driver,
                    cache=cache,
                    node_label="Domain",
                    text_property="description",
                    key_property="key",
                    openai_client=mock_client,
                    execute=True,
                )

            # Empty/whitespace texts should be filtered out
            assert processed == 0, "Empty texts should not be processed"
            assert created == 0
            assert cache.count(namespace="embeddings") == 0
            cache.close()

    def test_dry_run_does_not_cache(self):
        """Dry run mode should not populate cache."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = AppCache(Path(tmpdir) / "cache")

            nodes = [{"key": "dryrun.com", "text": "This is a dry run test"}]
            driver = MockDriver(nodes)
            mock_client = create_mock_openai_client({})

            with patch("public_company_graph.embeddings.create.BATCH_SIZE_SMALL", 100):
                processed, created, cached_count, failed = create_embeddings_for_nodes(
                    driver=driver,
                    cache=cache,
                    node_label="Domain",
                    text_property="description",
                    key_property="key",
                    openai_client=mock_client,
                    execute=False,  # Dry run!
                )

            # Dry run should not cache anything
            assert processed == 0
            assert cache.count(namespace="embeddings") == 0
            assert mock_client._call_count["count"] == 0, "Dry run should not call API"
            cache.close()

    def test_invalid_node_label_rejected(self):
        """Invalid node labels should be rejected."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = AppCache(Path(tmpdir) / "cache")
            driver = MockDriver([])

            with pytest.raises(ValueError, match="Invalid node_label"):
                create_embeddings_for_nodes(
                    driver=driver,
                    cache=cache,
                    node_label="InvalidLabel",  # Not in ALLOWED_NODE_LABELS
                    text_property="description",
                    key_property="key",
                    execute=True,
                )
            cache.close()


class TestEmbeddingCacheKeyDerivation:
    """Tests to verify cache keys are derived correctly from node data."""

    def test_cache_key_derivation_from_node(self):
        """Verify cache key is correctly derived: {node_key}:{text_property}."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = AppCache(Path(tmpdir) / "cache")

            # The create_embeddings_for_nodes function creates keys like:
            # f"{record['key']}:{text_property}"
            expected_key = "mycompany.com:description"

            nodes = [{"key": "mycompany.com", "text": "My company does great things"}]
            driver = MockDriver(nodes)
            mock_client = create_mock_openai_client({})

            with patch("public_company_graph.embeddings.create.BATCH_SIZE_SMALL", 100):
                create_embeddings_for_nodes(
                    driver=driver,
                    cache=cache,
                    node_label="Domain",
                    text_property="description",
                    key_property="key",
                    openai_client=mock_client,
                    execute=True,
                )

            # Verify the cache key format
            cached = cache.get("embeddings", expected_key)
            assert cached is not None, f"Cache key should be '{expected_key}'"
            assert "embedding" in cached
            cache.close()

    def test_cache_key_with_special_characters(self):
        """Cache keys with special characters should work."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = AppCache(Path(tmpdir) / "cache")

            # Domain with hyphen and numbers
            nodes = [{"key": "my-company-123.com", "text": "Special chars company"}]
            driver = MockDriver(nodes)
            mock_client = create_mock_openai_client({})

            with patch("public_company_graph.embeddings.create.BATCH_SIZE_SMALL", 100):
                create_embeddings_for_nodes(
                    driver=driver,
                    cache=cache,
                    node_label="Domain",
                    text_property="description",
                    key_property="key",
                    openai_client=mock_client,
                    execute=True,
                )

            expected_key = "my-company-123.com:description"
            cached = cache.get("embeddings", expected_key)
            assert cached is not None
            cache.close()
