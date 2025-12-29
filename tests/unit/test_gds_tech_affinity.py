"""
Unit tests for GDS technology affinity functions.
"""

from unittest.mock import MagicMock, patch

import pandas as pd

from domain_status_graph.gds.tech_affinity import (
    _identify_columns,
    compute_tech_affinity_bundling,
)


class TestIdentifyColumns:
    """Tests for _identify_columns helper function."""

    def test_identifies_standard_column_names(self):
        """Test identification of standard GDS column names."""
        df = pd.DataFrame(
            {
                "nodeId1": [1, 2, 3],
                "nodeId2": [4, 5, 6],
                "similarity": [0.9, 0.8, 0.7],
            }
        )
        mock_logger = MagicMock()

        col1, col2, sim_col = _identify_columns(df, mock_logger)

        assert col1 == "nodeId1"
        assert col2 == "nodeId2"
        assert sim_col == "similarity"

    def test_identifies_alternative_column_names(self):
        """Test identification of alternative column names (source/target)."""
        df = pd.DataFrame(
            {
                "source": [1, 2, 3],
                "target": [4, 5, 6],
                "score": [0.9, 0.8, 0.7],
            }
        )
        mock_logger = MagicMock()

        col1, col2, sim_col = _identify_columns(df, mock_logger)

        assert col1 == "source"
        assert col2 == "target"
        assert sim_col == "score"

    def test_identifies_node1_node2_columns(self):
        """Test identification of node1/node2 column names."""
        df = pd.DataFrame(
            {
                "node1": [1, 2],
                "node2": [4, 5],
                "weight": [0.9, 0.8],
            }
        )
        mock_logger = MagicMock()

        col1, col2, sim_col = _identify_columns(df, mock_logger)

        assert col1 == "node1"
        assert col2 == "node2"
        assert sim_col == "weight"

    def test_returns_none_for_unrecognized_columns(self):
        """Test that None is returned for unrecognized column names."""
        df = pd.DataFrame(
            {
                "foo": [1, 2],
                "bar": [4, 5],
                "baz": [0.9, 0.8],
            }
        )
        mock_logger = MagicMock()

        col1, col2, sim_col = _identify_columns(df, mock_logger)

        assert col1 is None
        assert col2 is None
        assert sim_col is None
        mock_logger.error.assert_called()

    def test_handles_mixed_case_column_names(self):
        """Test that column identification is case-insensitive."""
        df = pd.DataFrame(
            {
                "NODEID1": [1, 2],
                "NodeId2": [4, 5],
                "SIMILARITY": [0.9, 0.8],
            }
        )
        mock_logger = MagicMock()

        col1, col2, sim_col = _identify_columns(df, mock_logger)

        # Should find them (case-insensitive comparison)
        assert col1 == "NODEID1"
        assert col2 == "NodeId2"
        assert sim_col == "SIMILARITY"


class TestComputeTechAffinityBundling:
    """Tests for compute_tech_affinity_bundling function."""

    def test_creates_graph_and_runs_similarity(self):
        """Test full flow: graph creation and similarity computation."""
        mock_gds = MagicMock()
        mock_driver = MagicMock()
        mock_session = MagicMock()
        mock_driver.session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_driver.session.return_value.__exit__ = MagicMock(return_value=False)

        # Mock graph projection
        mock_graph = MagicMock()
        mock_projection_result = {"nodeCount": 100, "relationshipCount": 500}
        mock_gds.graph.project.cypher.return_value = (mock_graph, mock_projection_result)

        # Mock similarity result as DataFrame
        mock_similarity_df = pd.DataFrame(
            {
                "nodeId1": [1, 2],
                "nodeId2": [3, 4],
                "similarity": [0.9, 0.8],
            }
        )
        mock_gds.nodeSimilarity.stream.return_value = mock_similarity_df

        # Mock batch write result
        mock_write_result = MagicMock()
        mock_write_result.single.return_value = {"created": 2}
        mock_session.run.return_value = mock_write_result

        result = compute_tech_affinity_bundling(
            gds=mock_gds,
            driver=mock_driver,
            database="testdb",
            logger=MagicMock(),
        )

        # Verify graph was projected
        mock_gds.graph.project.cypher.assert_called_once()

        # Verify node similarity was computed
        mock_gds.nodeSimilarity.stream.assert_called_once()
        call_kwargs = mock_gds.nodeSimilarity.stream.call_args[1]
        assert call_kwargs["similarityMetric"] == "JACCARD"

        # Verify graph was dropped
        mock_graph.drop.assert_called_once()

        assert result == 2

    def test_handles_dict_similarity_results(self):
        """Test handling of dict-based similarity results."""
        mock_gds = MagicMock()
        mock_driver = MagicMock()
        mock_session = MagicMock()
        mock_driver.session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_driver.session.return_value.__exit__ = MagicMock(return_value=False)

        mock_graph = MagicMock()
        mock_gds.graph.project.cypher.return_value = (
            mock_graph,
            {"nodeCount": 10, "relationshipCount": 20},
        )

        # Return dict-based results (iterator)
        mock_gds.nodeSimilarity.stream.return_value = iter(
            [
                {"nodeId1": 1, "nodeId2": 2, "similarity": 0.9},
                {"nodeId1": 3, "nodeId2": 4, "similarity": 0.8},
            ]
        )

        mock_write_result = MagicMock()
        mock_write_result.single.return_value = {"created": 2}
        mock_session.run.return_value = mock_write_result

        result = compute_tech_affinity_bundling(
            gds=mock_gds,
            driver=mock_driver,
            logger=MagicMock(),
        )

        assert result == 2

    def test_handles_tuple_similarity_results(self):
        """Test handling of tuple-based similarity results."""
        mock_gds = MagicMock()
        mock_driver = MagicMock()
        mock_session = MagicMock()
        mock_driver.session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_driver.session.return_value.__exit__ = MagicMock(return_value=False)

        mock_graph = MagicMock()
        mock_gds.graph.project.cypher.return_value = (
            mock_graph,
            {"nodeCount": 10, "relationshipCount": 20},
        )

        # Return tuple-based results (iterator of tuples)
        mock_gds.nodeSimilarity.stream.return_value = iter(
            [
                (1, 2, 0.9),
                (3, 4, 0.8),
            ]
        )

        mock_write_result = MagicMock()
        mock_write_result.single.return_value = {"created": 2}
        mock_session.run.return_value = mock_write_result

        result = compute_tech_affinity_bundling(
            gds=mock_gds,
            driver=mock_driver,
            logger=MagicMock(),
        )

        assert result == 2

    def test_handles_exception_gracefully(self):
        """Test that exceptions are caught and logged."""
        mock_gds = MagicMock()
        mock_gds.graph.project.cypher.side_effect = Exception("GDS error")
        mock_driver = MagicMock()
        mock_logger = MagicMock()

        result = compute_tech_affinity_bundling(
            gds=mock_gds,
            driver=mock_driver,
            logger=mock_logger,
        )

        assert result == 0
        mock_logger.error.assert_called()

    def test_uses_default_logger_when_none_provided(self):
        """Test that default logger is used when none provided."""
        mock_gds = MagicMock()
        mock_gds.graph.project.cypher.side_effect = Exception("Error")

        result = compute_tech_affinity_bundling(
            gds=mock_gds,
            driver=MagicMock(),
            logger=None,
        )

        assert result == 0

    def test_respects_similarity_cutoff_parameter(self):
        """Test that similarity_cutoff is passed to GDS."""
        mock_gds = MagicMock()
        mock_driver = MagicMock()
        mock_session = MagicMock()
        mock_driver.session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_driver.session.return_value.__exit__ = MagicMock(return_value=False)

        mock_graph = MagicMock()
        mock_gds.graph.project.cypher.return_value = (
            mock_graph,
            {"nodeCount": 10, "relationshipCount": 20},
        )
        mock_gds.nodeSimilarity.stream.return_value = pd.DataFrame(
            {"nodeId1": [], "nodeId2": [], "similarity": []}
        )

        mock_session.run.return_value = MagicMock()

        compute_tech_affinity_bundling(
            gds=mock_gds,
            driver=mock_driver,
            similarity_cutoff=0.75,
            logger=MagicMock(),
        )

        call_kwargs = mock_gds.nodeSimilarity.stream.call_args[1]
        assert call_kwargs["similarityCutoff"] == 0.75

    def test_respects_top_k_parameter(self):
        """Test that top_k is passed to GDS."""
        mock_gds = MagicMock()
        mock_driver = MagicMock()
        mock_session = MagicMock()
        mock_driver.session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_driver.session.return_value.__exit__ = MagicMock(return_value=False)

        mock_graph = MagicMock()
        mock_gds.graph.project.cypher.return_value = (
            mock_graph,
            {"nodeCount": 10, "relationshipCount": 20},
        )
        mock_gds.nodeSimilarity.stream.return_value = pd.DataFrame(
            {"nodeId1": [], "nodeId2": [], "similarity": []}
        )

        mock_session.run.return_value = MagicMock()

        compute_tech_affinity_bundling(
            gds=mock_gds,
            driver=mock_driver,
            top_k=15,
            logger=MagicMock(),
        )

        call_kwargs = mock_gds.nodeSimilarity.stream.call_args[1]
        assert call_kwargs["topK"] == 15

    @patch("domain_status_graph.gds.tech_affinity.safe_drop_graph")
    def test_drops_existing_graph_before_projection(self, mock_safe_drop):
        """Test that existing graph is dropped before creating new one."""
        mock_gds = MagicMock()
        mock_driver = MagicMock()
        mock_session = MagicMock()
        mock_driver.session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_driver.session.return_value.__exit__ = MagicMock(return_value=False)

        mock_graph = MagicMock()
        mock_gds.graph.project.cypher.return_value = (
            mock_graph,
            {"nodeCount": 10, "relationshipCount": 20},
        )
        mock_gds.nodeSimilarity.stream.return_value = pd.DataFrame(
            {"nodeId1": [], "nodeId2": [], "similarity": []}
        )

        mock_session.run.return_value = MagicMock()

        compute_tech_affinity_bundling(
            gds=mock_gds,
            driver=mock_driver,
            database="testdb",
            logger=MagicMock(),
        )

        mock_safe_drop.assert_called_once()
        call_args = mock_safe_drop.call_args[0]
        assert call_args[0] == mock_gds
        assert "testdb" in call_args[1]

    def test_writes_in_batches(self):
        """Test that relationships are written in batches."""
        mock_gds = MagicMock()
        mock_driver = MagicMock()
        mock_session = MagicMock()
        mock_driver.session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_driver.session.return_value.__exit__ = MagicMock(return_value=False)

        mock_graph = MagicMock()
        mock_gds.graph.project.cypher.return_value = (
            mock_graph,
            {"nodeCount": 100, "relationshipCount": 500},
        )

        # Create many similarity pairs
        data = {
            "nodeId1": list(range(100)),
            "nodeId2": list(range(100, 200)),
            "similarity": [0.9] * 100,
        }
        mock_gds.nodeSimilarity.stream.return_value = pd.DataFrame(data)

        mock_write_result = MagicMock()
        mock_write_result.single.return_value = {"created": 50}  # Each batch creates some
        mock_session.run.return_value = mock_write_result

        compute_tech_affinity_bundling(
            gds=mock_gds,
            driver=mock_driver,
            batch_size=50,  # Small batch size
            logger=MagicMock(),
        )

        # Should have multiple session.run calls for batches
        assert mock_session.run.call_count >= 2
