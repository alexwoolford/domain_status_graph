"""
Unit tests for GDS utility functions.
"""

from unittest.mock import MagicMock

import pandas as pd

from public_company_graph.gds.utils import (
    cleanup_leftover_graphs,
    safe_drop_graph,
)


class TestSafeDropGraph:
    """Tests for safe_drop_graph function."""

    def test_drops_existing_graph(self):
        """Test that existing graph is dropped successfully."""
        mock_gds = MagicMock()
        mock_gds.graph.drop.return_value = None

        result = safe_drop_graph(mock_gds, "test_graph")

        assert result is True
        mock_gds.graph.drop.assert_called_once_with("test_graph")

    def test_returns_false_when_graph_doesnt_exist(self):
        """Test that False is returned when graph doesn't exist."""
        mock_gds = MagicMock()
        mock_gds.graph.drop.side_effect = Exception("Graph not found")

        result = safe_drop_graph(mock_gds, "nonexistent_graph")

        assert result is False

    def test_returns_false_on_any_exception(self):
        """Test that any exception results in False."""
        mock_gds = MagicMock()
        mock_gds.graph.drop.side_effect = RuntimeError("Unexpected error")

        result = safe_drop_graph(mock_gds, "some_graph")

        assert result is False


class TestCleanupLeftoverGraphs:
    """Tests for cleanup_leftover_graphs function."""

    def test_cleanup_with_dataframe_graph_list(self):
        """Test cleanup when graph.list returns a DataFrame."""
        mock_gds = MagicMock()
        mock_gds.graph.list.return_value = pd.DataFrame(
            {"graphName": ["graph_test_db", "other_graph", "another_test_db"]}
        )
        mock_logger = MagicMock()

        cleanup_leftover_graphs(mock_gds, database="test_db", logger=mock_logger)

        # Should have dropped graphs ending with _test_db
        assert mock_gds.graph.drop.call_count == 2

    def test_cleanup_with_no_matching_graphs(self):
        """Test cleanup when no graphs match the database suffix."""
        mock_gds = MagicMock()
        mock_gds.graph.list.return_value = pd.DataFrame(
            {"graphName": ["graph_other", "some_graph"]}
        )
        mock_logger = MagicMock()

        cleanup_leftover_graphs(mock_gds, database="test_db", logger=mock_logger)

        mock_gds.graph.drop.assert_not_called()

    def test_cleanup_handles_exception_gracefully(self):
        """Test that exceptions during cleanup are logged, not raised."""
        mock_gds = MagicMock()
        mock_gds.graph.list.side_effect = Exception("Connection error")
        mock_logger = MagicMock()

        # Should not raise
        cleanup_leftover_graphs(mock_gds, database="test_db", logger=mock_logger)

        mock_logger.warning.assert_called_once()

    def test_cleanup_uses_default_logger_when_none_provided(self):
        """Test that default logger is used when none provided."""
        mock_gds = MagicMock()
        mock_gds.graph.list.side_effect = Exception("Error")

        # Should not raise, uses default logger
        cleanup_leftover_graphs(mock_gds, database="test_db", logger=None)

    def test_cleanup_with_empty_graph_list(self):
        """Test cleanup with empty graph list."""
        mock_gds = MagicMock()
        mock_gds.graph.list.return_value = pd.DataFrame({"graphName": []})
        mock_logger = MagicMock()

        cleanup_leftover_graphs(mock_gds, database="test_db", logger=mock_logger)

        mock_gds.graph.drop.assert_not_called()

    def test_cleanup_without_database_filter(self):
        """Test cleanup when database is None (no filtering)."""
        mock_gds = MagicMock()
        mock_gds.graph.list.return_value = pd.DataFrame({"graphName": ["graph1", "graph2"]})
        mock_logger = MagicMock()

        cleanup_leftover_graphs(mock_gds, database=None, logger=mock_logger)

        # With no database filter, no graphs match the suffix pattern
        mock_gds.graph.drop.assert_not_called()


class TestGetGdsClient:
    """Tests for get_gds_client function."""

    # Note: Tests for get_gds_client are skipped because:
    # - The function has dynamic imports that are hard to mock
    # - Behavior is covered by integration tests
    # - These tests would require complex patching with minimal value
