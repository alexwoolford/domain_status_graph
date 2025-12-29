"""
Unit tests for public_company_graph.cache (AppCache) module.

Tests the unified cache implementation used throughout the application.
"""

import tempfile
from pathlib import Path

from public_company_graph.cache import AppCache


class TestAppCacheBasics:
    """Basic AppCache functionality tests."""

    def test_initialization(self):
        """Test AppCache initialization creates cache directory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_dir = Path(tmpdir) / "test_cache"
            cache = AppCache(cache_dir)

            assert cache.cache_dir == cache_dir
            assert cache_dir.exists()
            cache.close()

    def test_set_and_get(self):
        """Test setting and getting values."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = AppCache(Path(tmpdir) / "cache")

            cache.set("namespace", "key1", {"value": "test"})
            result = cache.get("namespace", "key1")

            assert result == {"value": "test"}
            cache.close()

    def test_get_missing_key(self):
        """Missing key returns None."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = AppCache(Path(tmpdir) / "cache")

            result = cache.get("namespace", "nonexistent")
            assert result is None
            cache.close()

    def test_delete(self):
        """Test deleting values."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = AppCache(Path(tmpdir) / "cache")

            cache.set("namespace", "key1", "value1")
            assert cache.get("namespace", "key1") == "value1"

            deleted = cache.delete("namespace", "key1")
            assert deleted is True
            assert cache.get("namespace", "key1") is None
            cache.close()

    def test_delete_nonexistent(self):
        """Deleting nonexistent key returns False."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = AppCache(Path(tmpdir) / "cache")

            deleted = cache.delete("namespace", "nonexistent")
            assert deleted is False
            cache.close()


class TestAppCacheNamespaces:
    """Tests for namespace functionality."""

    def test_namespaces_are_isolated(self):
        """Same key in different namespaces are separate."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = AppCache(Path(tmpdir) / "cache")

            cache.set("ns1", "key", "value1")
            cache.set("ns2", "key", "value2")

            assert cache.get("ns1", "key") == "value1"
            assert cache.get("ns2", "key") == "value2"
            cache.close()

    def test_clear_namespace(self):
        """Clear only removes keys in specified namespace."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = AppCache(Path(tmpdir) / "cache")

            cache.set("ns1", "key1", "value1")
            cache.set("ns1", "key2", "value2")
            cache.set("ns2", "key1", "value3")

            deleted = cache.clear_namespace("ns1")
            assert deleted == 2

            assert cache.get("ns1", "key1") is None
            assert cache.get("ns1", "key2") is None
            assert cache.get("ns2", "key1") == "value3"
            cache.close()

    def test_count_by_namespace(self):
        """Count can filter by namespace."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = AppCache(Path(tmpdir) / "cache")

            cache.set("ns1", "key1", "value1")
            cache.set("ns1", "key2", "value2")
            cache.set("ns2", "key1", "value3")

            assert cache.count() == 3
            assert cache.count(namespace="ns1") == 2
            assert cache.count(namespace="ns2") == 1
            assert cache.count(namespace="ns3") == 0
            cache.close()


class TestAppCacheStats:
    """Tests for cache statistics."""

    def test_stats_structure(self):
        """Stats returns expected structure."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = AppCache(Path(tmpdir) / "cache")

            cache.set("embeddings", "key1", {"embedding": [0.1, 0.2]})
            cache.set("embeddings", "key2", {"embedding": [0.3, 0.4]})
            cache.set("10k_extracted", "key1", {"data": "test"})

            stats = cache.stats()

            assert "total" in stats
            assert "by_namespace" in stats
            assert "size_mb" in stats
            assert "cache_dir" in stats

            assert stats["total"] == 3
            assert stats["by_namespace"]["embeddings"] == 2
            assert stats["by_namespace"]["10k_extracted"] == 1
            cache.close()

    def test_keys_filtered_by_namespace(self):
        """Keys can be filtered by namespace."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = AppCache(Path(tmpdir) / "cache")

            cache.set("embeddings", "domain1.com:description", {"embedding": [0.1]})
            cache.set("embeddings", "domain2.com:description", {"embedding": [0.2]})
            cache.set("10k_extracted", "12345", {"data": "test"})

            keys = cache.keys(namespace="embeddings")
            assert len(keys) == 2
            assert "domain1.com:description" in keys
            assert "domain2.com:description" in keys
            cache.close()


class TestAppCacheEmbeddingsWorkflow:
    """Tests simulating the embeddings caching workflow."""

    def test_embedding_cache_workflow(self):
        """Test the typical embedding caching workflow."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = AppCache(Path(tmpdir) / "cache")
            namespace = "embeddings"

            # Simulate caching an embedding
            embedding_data = {
                "embedding": [0.1, 0.2, 0.3],
                "text": "Apple Inc. is a technology company.",
                "model": "text-embedding-3-small",
                "dimension": 3,
            }
            cache.set(namespace, "apple.com:description", embedding_data)

            # Retrieve and verify
            cached = cache.get(namespace, "apple.com:description")
            assert cached is not None
            assert cached["embedding"] == [0.1, 0.2, 0.3]
            assert cached["model"] == "text-embedding-3-small"

            # Cache stats
            stats = cache.stats()
            assert stats["by_namespace"].get("embeddings", 0) == 1
            cache.close()

    def test_embedding_cache_overwrite(self):
        """Test that embeddings can be overwritten."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = AppCache(Path(tmpdir) / "cache")
            namespace = "embeddings"

            # Initial embedding
            cache.set(namespace, "domain.com:description", {"embedding": [0.1]})
            assert cache.get(namespace, "domain.com:description")["embedding"] == [0.1]

            # Overwrite with new embedding
            cache.set(namespace, "domain.com:description", {"embedding": [0.9]})
            assert cache.get(namespace, "domain.com:description")["embedding"] == [0.9]

            # Should still be only 1 entry
            assert cache.count(namespace=namespace) == 1
            cache.close()
