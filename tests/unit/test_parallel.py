"""
Unit tests for public_company_graph.utils.parallel module.
"""

import time

from public_company_graph.utils.parallel import (
    execute_parallel,
    execute_parallel_with_stats,
)
from public_company_graph.utils.stats import ExecutionStats


class TestExecuteParallel:
    """Test execute_parallel function."""

    def test_basic_execution(self):
        """Test basic parallel execution."""
        items = [1, 2, 3, 4, 5]

        def square(x: int) -> int:
            return x * x

        results = execute_parallel(items, square, max_workers=2, show_progress=False)

        assert len(results) == 5
        for item, result, error in results:
            assert error is None
            assert result == item * item

    def test_error_handling(self):
        """Test error handling in parallel execution."""
        items = [1, 2, 3]
        errors_caught = []

        def fail_on_2(x: int) -> int:
            if x == 2:
                raise ValueError(f"Failed on {x}")
            return x * x

        def error_handler(item: int, error: Exception):
            errors_caught.append((item, error))

        results = execute_parallel(
            items,
            fail_on_2,
            max_workers=2,
            show_progress=False,
            error_handler=error_handler,
        )

        assert len(results) == 3
        # Check that error was caught
        assert len(errors_caught) == 1
        assert errors_caught[0][0] == 2

        # Check results
        for item, result, error in results:
            if item == 2:
                assert error is not None
                assert isinstance(error, ValueError)
            else:
                assert error is None
                assert result == item * item

    def test_result_handler(self):
        """Test result handler callback."""
        items = [1, 2, 3]
        results_collected = []

        def square(x: int) -> int:
            return x * x

        def result_handler(item: int, result: int):
            results_collected.append((item, result))

        execute_parallel(
            items,
            square,
            max_workers=2,
            show_progress=False,
            result_handler=result_handler,
        )

        assert len(results_collected) == 3
        assert (1, 1) in results_collected
        assert (2, 4) in results_collected
        assert (3, 9) in results_collected

    def test_stats_tracking(self):
        """Test stats tracking integration."""
        items = [1, 2, 3, 4, 5]
        stats = ExecutionStats(success=0, failed=0)

        def square(x: int) -> int:
            return x * x

        execute_parallel(
            items,
            square,
            max_workers=2,
            show_progress=False,
            stats=stats,
            stats_key="success",
        )

        assert stats.get("success") == 5
        assert stats.get("failed") == 0

    def test_empty_items(self):
        """Test with empty item list."""
        results = execute_parallel([], lambda x: x, show_progress=False)
        assert results == []

    def test_timeout(self):
        """Test timeout handling."""
        items = [1, 2]

        def slow_func(x: int) -> int:
            time.sleep(0.1)  # Short sleep
            return x * x

        # Timeout is applied to as_completed, not individual tasks
        # For this test, we just verify it doesn't crash
        results = execute_parallel(
            items, slow_func, max_workers=2, show_progress=False, timeout=10.0
        )

        # Should complete successfully
        assert len(results) == 2
        for item, result, error in results:
            assert error is None
            assert result == item * item


class TestExecuteParallelWithStats:
    """Test execute_parallel_with_stats function."""

    def test_basic_execution_with_stats(self):
        """Test parallel execution with stats passed to worker."""
        items = [1, 2, 3, 4, 5]

        def square_with_stats(x: int, stats: ExecutionStats) -> int:
            result = x * x
            stats.increment("success")
            return result

        results, stats = execute_parallel_with_stats(
            items,
            square_with_stats,
            max_workers=2,
            show_progress=False,
        )

        assert len(results) == 5
        assert stats.get("success") == 5

        for item, result, error in results:
            assert error is None
            assert result == item * item

    def test_stats_updates_in_worker(self):
        """Test that workers can update stats directly."""
        items = [1, 2, 3]

        def process_with_stats(x: int, stats: ExecutionStats) -> int:
            if x % 2 == 0:
                stats.increment("even")
            else:
                stats.increment("odd")
            return x * x

        results, stats = execute_parallel_with_stats(
            items,
            process_with_stats,
            max_workers=2,
            show_progress=False,
        )

        assert stats.get("even") == 1  # Only 2 is even
        assert stats.get("odd") == 2  # 1 and 3 are odd

    def test_progress_postfix(self):
        """Test progress bar postfix generation."""
        items = [1, 2, 3]
        postfix_calls = []

        def square_with_stats(x: int, stats: ExecutionStats) -> int:
            stats.increment("success")
            return x * x

        def postfix_func(stats: ExecutionStats) -> dict:
            postfix_calls.append(stats.to_dict())
            return {"success": stats.get("success")}

        results, stats = execute_parallel_with_stats(
            items,
            square_with_stats,
            max_workers=2,
            show_progress=True,  # Must be True for postfix to be called
            progress_postfix=postfix_func,
        )

        # Postfix should be called multiple times (once per completion)
        # Note: May be called fewer times if progress bar batches updates
        assert len(postfix_calls) >= 1  # At least once

    def test_log_interval(self, caplog):
        """Test periodic logging."""
        import logging

        items = list(range(1, 251))  # 250 items

        def square_with_stats(x: int, stats: ExecutionStats) -> int:
            stats.increment("success")
            return x * x

        # Create a logger for this test
        test_logger = logging.getLogger("test_parallel")
        test_logger.setLevel(logging.INFO)

        results, stats = execute_parallel_with_stats(
            items,
            square_with_stats,
            max_workers=4,
            show_progress=False,
            log_interval=100,
            logger_instance=test_logger,
        )

        # Should have logged at 100, 200, and possibly 250
        log_messages = [r.message for r in caplog.records if "Progress:" in r.message]
        # May log fewer times if execution is very fast
        assert len(log_messages) >= 1  # At least 1 log message

    def test_error_handling_with_stats(self):
        """Test error handling when stats are used."""
        items = [1, 2, 3]

        def fail_on_2(x: int, stats: ExecutionStats) -> int:
            if x == 2:
                stats.increment("failed")
                raise ValueError(f"Failed on {x}")
            stats.increment("success")
            return x * x

        results, stats = execute_parallel_with_stats(
            items,
            fail_on_2,
            max_workers=2,
            show_progress=False,
        )

        assert stats.get("success") == 2
        assert stats.get("failed") == 1

        # Check results
        for item, result, error in results:
            if item == 2:
                assert error is not None
            else:
                assert error is None
                assert result == item * item
