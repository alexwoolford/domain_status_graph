"""Unit tests for SQLiteEmbeddingCache."""

import tempfile
from pathlib import Path

from domain_status_graph.embeddings.sqlite_cache import (
    SQLiteEmbeddingCache,
    compute_text_hash,
)


class TestComputeTextHash:
    """Tests for compute_text_hash function."""

    def test_empty_text(self):
        """Empty text returns empty hash."""
        assert compute_text_hash("") == ""

    def test_consistent_hash(self):
        """Same text produces same hash."""
        text = "hello world"
        hash1 = compute_text_hash(text)
        hash2 = compute_text_hash(text)
        assert hash1 == hash2

    def test_strips_whitespace(self):
        """Text is stripped before hashing."""
        hash1 = compute_text_hash("hello")
        hash2 = compute_text_hash("  hello  ")
        assert hash1 == hash2


class TestSQLiteEmbeddingCache:
    """Tests for SQLiteEmbeddingCache class."""

    def test_set_and_get(self):
        """Can store and retrieve embeddings."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            cache = SQLiteEmbeddingCache(db_path)

            embedding = [0.1, 0.2, 0.3]
            cache.set(
                key="test:description",
                text="hello world",
                model="test-model",
                dimension=3,
                embedding=embedding,
            )

            result = cache.get(
                key="test:description",
                text="hello world",
                model="test-model",
                expected_dimension=3,  # Must match stored dimension
            )

            assert result is not None
            assert len(result) == 3
            # Check values are approximately equal (float precision)
            assert abs(result[0] - 0.1) < 0.0001

    def test_get_missing_key(self):
        """Missing key returns None."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            cache = SQLiteEmbeddingCache(db_path)

            result = cache.get(
                key="nonexistent",
                text="hello",
                model="test-model",
            )
            assert result is None

    def test_invalidates_on_text_change(self):
        """Cache invalidates when text changes."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            cache = SQLiteEmbeddingCache(db_path)

            cache.set(
                key="test:description",
                text="original text",
                model="test-model",
                dimension=2,
                embedding=[0.1, 0.2],
            )

            # Same key, different text - should return None
            result = cache.get(
                key="test:description",
                text="changed text",
                model="test-model",
            )
            assert result is None

    def test_model_mismatch_returns_none(self):
        """Different model returns None."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            cache = SQLiteEmbeddingCache(db_path)

            cache.set(
                key="test:description",
                text="hello",
                model="model-a",
                dimension=2,
                embedding=[0.1, 0.2],
            )

            result = cache.get(
                key="test:description",
                text="hello",
                model="model-b",  # Different model
            )
            assert result is None

    def test_count(self):
        """Count returns correct number of entries."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            cache = SQLiteEmbeddingCache(db_path)

            assert cache.count() == 0

            cache.set("key1:desc", "text1", "model", 2, [0.1, 0.2])
            cache.set("key2:desc", "text2", "model", 2, [0.3, 0.4])

            assert cache.count() == 2

    def test_stats(self):
        """Stats returns correct structure."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            cache = SQLiteEmbeddingCache(db_path)

            cache.set("domain.com:description", "text1", "model", 2, [0.1, 0.2])
            cache.set("domain.com:keywords", "text2", "model", 2, [0.3, 0.4])

            stats = cache.stats()
            assert stats["total"] == 2
            assert "by_type" in stats
            assert stats["by_type"]["description"] == 1
            assert stats["by_type"]["keywords"] == 1

    def test_get_or_create_cached(self):
        """get_or_create returns cached value without calling create_fn."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            cache = SQLiteEmbeddingCache(db_path)

            cache.set("key:desc", "text", "model", 2, [0.1, 0.2])

            # create_fn should NOT be called since value is cached
            def create_fn(text, model):
                raise AssertionError("Should not be called")

            result = cache.get_or_create(
                key="key:desc",
                text="text",
                model="model",
                dimension=2,
                create_fn=create_fn,
            )

            assert result is not None

    def test_get_or_create_creates_new(self):
        """get_or_create calls create_fn when not cached."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            cache = SQLiteEmbeddingCache(db_path)

            called = []

            def create_fn(text, model):
                called.append(True)
                return [0.5, 0.6]

            result = cache.get_or_create(
                key="new:desc",
                text="new text",
                model="model",
                dimension=2,
                create_fn=create_fn,
            )

            assert len(called) == 1
            assert result == [0.5, 0.6]
            # Should now be cached
            assert cache.count() == 1

    def test_delete(self):
        """Delete removes entry."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            cache = SQLiteEmbeddingCache(db_path)

            cache.set("key:desc", "text", "model", 2, [0.1, 0.2])
            assert cache.count() == 1

            deleted = cache.delete("key:desc")
            assert deleted is True
            assert cache.count() == 0

    def test_delete_nonexistent(self):
        """Delete returns False for nonexistent key."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            cache = SQLiteEmbeddingCache(db_path)

            deleted = cache.delete("nonexistent")
            assert deleted is False
