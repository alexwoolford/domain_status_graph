"""
Unit tests for rate limiting utility.

Tests the RateLimiter class and get_rate_limiter function.
"""

import threading
import time

import pytest

from public_company_graph.utils.rate_limiting import RateLimiter, get_rate_limiter


class TestRateLimiter:
    """Test RateLimiter class."""

    def test_init_valid(self):
        """Test RateLimiter initialization with valid parameters."""
        limiter = RateLimiter(requests_per_second=10.0, source_name="test")
        assert limiter.requests_per_second == 10.0
        assert limiter.source_name == "test"
        assert limiter.min_interval == 0.1  # 1.0 / 10.0

    def test_init_invalid_zero(self):
        """Test RateLimiter initialization with zero requests per second."""
        with pytest.raises(ValueError, match="requests_per_second must be > 0"):
            RateLimiter(requests_per_second=0.0)

    def test_init_invalid_negative(self):
        """Test RateLimiter initialization with negative requests per second."""
        with pytest.raises(ValueError, match="requests_per_second must be > 0"):
            RateLimiter(requests_per_second=-1.0)

    def test_call_no_wait_first_call(self):
        """Test that first call doesn't wait."""
        limiter = RateLimiter(requests_per_second=10.0)
        start = time.time()
        limiter()
        elapsed = time.time() - start
        # First call should be immediate (very fast, < 0.01 seconds)
        assert elapsed < 0.01

    def test_call_waits_for_min_interval(self):
        """Test that second call waits to maintain rate limit."""
        limiter = RateLimiter(requests_per_second=10.0)  # 0.1 second minimum interval

        # First call
        limiter()

        # Second call should wait ~0.1 seconds
        start = time.time()
        limiter()
        elapsed = time.time() - start

        # Should wait approximately 0.1 seconds (allow small tolerance)
        assert 0.09 <= elapsed <= 0.15, f"Expected ~0.1s wait, got {elapsed}s"

    def test_call_thread_safe(self):
        """Test that rate limiter is thread-safe."""
        limiter = RateLimiter(requests_per_second=100.0)  # 0.01 second interval
        results = []
        errors = []

        def worker(worker_id: int):
            try:
                start = time.time()
                limiter()
                elapsed = time.time() - start
                results.append((worker_id, elapsed))
            except Exception as e:
                errors.append((worker_id, e))

        # Create 10 threads that all call the limiter
        threads = [threading.Thread(target=worker, args=(i,)) for i in range(10)]

        # Start all threads at approximately the same time
        for thread in threads:
            thread.start()

        # Wait for all threads to complete
        for thread in threads:
            thread.join(timeout=5.0)

        # Check that no errors occurred
        assert len(errors) == 0, f"Errors occurred: {errors}"

        # Check that all threads completed
        assert len(results) == 10, f"Expected 10 results, got {len(results)}"

        # Verify that calls were serialized (no two calls happened simultaneously)
        # The total time should be at least 10 * 0.01 = 0.1 seconds
        total_time = max(r[1] for r in results) - min(r[1] for r in results)
        # Allow some tolerance for thread scheduling
        assert total_time >= 0.05, f"Expected serialized calls, total time was {total_time}s"

    def test_context_manager(self):
        """Test using RateLimiter as a context manager."""
        limiter = RateLimiter(requests_per_second=10.0)

        # First call
        limiter()

        # Use as context manager
        start = time.time()
        with limiter:
            pass
        elapsed = time.time() - start

        # Should wait approximately 0.1 seconds
        assert 0.09 <= elapsed <= 0.15, f"Expected ~0.1s wait, got {elapsed}s"

    def test_reset(self):
        """Test resetting the rate limiter."""
        limiter = RateLimiter(requests_per_second=10.0)

        # First call
        limiter()

        # Reset
        limiter.reset()

        # Next call should be immediate (no wait)
        start = time.time()
        limiter()
        elapsed = time.time() - start

        assert elapsed < 0.01, "Reset call should be immediate"

    def test_different_rates(self):
        """Test rate limiters with different rates."""
        # Fast rate (100 req/sec = 0.01s interval)
        fast_limiter = RateLimiter(requests_per_second=100.0)
        fast_limiter()
        start = time.time()
        fast_limiter()
        fast_elapsed = time.time() - start

        # Slow rate (1 req/sec = 1.0s interval)
        slow_limiter = RateLimiter(requests_per_second=1.0)
        slow_limiter()
        start = time.time()
        slow_limiter()
        slow_elapsed = time.time() - start

        # Slow limiter should wait much longer
        assert slow_elapsed > fast_elapsed
        assert slow_elapsed >= 0.9  # At least 0.9 seconds
        assert fast_elapsed <= 0.05  # Less than 0.05 seconds


class TestGetRateLimiter:
    """Test get_rate_limiter function."""

    def test_get_existing_limiter(self):
        """Test getting an existing rate limiter."""
        # Create a limiter
        limiter1 = get_rate_limiter("test_source", requests_per_second=10.0)

        # Get it again - should return the same instance
        limiter2 = get_rate_limiter("test_source", requests_per_second=10.0)

        assert limiter1 is limiter2, "Should return the same limiter instance"

    def test_get_new_limiter(self):
        """Test creating a new rate limiter."""
        limiter = get_rate_limiter("new_source", requests_per_second=5.0)

        assert limiter is not None
        assert limiter.requests_per_second == 5.0
        assert limiter.source_name == "new_source"

    def test_get_missing_no_create(self):
        """Test getting a non-existent limiter without creating."""
        limiter = get_rate_limiter("nonexistent", requests_per_second=10.0, create_if_missing=False)

        assert limiter is None

    def test_different_sources_different_limiters(self):
        """Test that different sources get different limiter instances."""
        limiter1 = get_rate_limiter("source1", requests_per_second=10.0)
        limiter2 = get_rate_limiter("source2", requests_per_second=10.0)

        assert limiter1 is not limiter2, "Different sources should get different limiters"

    def test_same_source_same_limiter(self):
        """Test that same source gets same limiter even with different rates."""
        # Note: This tests current behavior - first call creates limiter with given rate
        # Subsequent calls return the same limiter (rate is not updated)
        limiter1 = get_rate_limiter("shared_source", requests_per_second=10.0)
        limiter2 = get_rate_limiter("shared_source", requests_per_second=20.0)

        assert limiter1 is limiter2, "Same source should return same limiter"
        # Note: The rate is set on first call, subsequent calls don't change it
        assert limiter2.requests_per_second == 10.0, "Rate should be from first call"
