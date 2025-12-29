"""
Unit tests for domain_status_graph.utils.stats module.
"""

import threading

from domain_status_graph.utils.stats import ExecutionStats


class TestExecutionStats:
    """Test ExecutionStats class."""

    def test_initialization(self):
        """Test stats initialization with initial values."""
        stats = ExecutionStats(success=0, failed=0, cached=5)
        assert stats.get("success") == 0
        assert stats.get("failed") == 0
        assert stats.get("cached") == 5

    def test_increment(self):
        """Test thread-safe increment."""
        stats = ExecutionStats(success=0, failed=0)
        stats.increment("success")
        assert stats.get("success") == 1
        stats.increment("success", amount=2)
        assert stats.get("success") == 3
        stats.increment("failed")
        assert stats.get("failed") == 1

    def test_thread_safety(self):
        """Test that increments are thread-safe."""
        stats = ExecutionStats(counter=0)

        def increment_many():
            for _ in range(1000):
                stats.increment("counter")

        threads = [threading.Thread(target=increment_many) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # Should be exactly 10000 (10 threads * 1000 increments)
        assert stats.get("counter") == 10000

    def test_dict_like_access(self):
        """Test dict-like access patterns."""
        stats = ExecutionStats(success=0, failed=0)
        assert stats["success"] == 0
        stats["success"] = 5
        assert stats["success"] == 5
        assert stats.get("success") == 5

    def test_to_dict(self):
        """Test conversion to dictionary."""
        stats = ExecutionStats(success=10, failed=5, cached=3)
        stats_dict = stats.to_dict()
        assert stats_dict == {"success": 10, "failed": 5, "cached": 3}
        # Should be a copy
        stats_dict["new"] = 1
        assert "new" not in stats.to_dict()

    def test_get_with_default(self):
        """Test get with default value."""
        stats = ExecutionStats(success=5)
        assert stats.get("success") == 5
        assert stats.get("nonexistent") == 0
        assert stats.get("nonexistent", default=99) == 99

    def test_lock_property(self):
        """Test that lock property returns a Lock."""
        stats = ExecutionStats()
        # Check it's a lock by verifying it has acquire/release methods
        assert hasattr(stats.lock, "acquire")
        assert hasattr(stats.lock, "release")

        # Test manual locking
        with stats.lock:
            stats.set("test", 42)
        assert stats.get("test") == 42

    def test_repr(self):
        """Test string representation."""
        stats = ExecutionStats(success=5, failed=2)
        repr_str = repr(stats)
        assert "success=5" in repr_str
        assert "failed=2" in repr_str
