"""
Unit tests for GDS company technology similarity functions.
"""

from unittest.mock import MagicMock, patch

import pandas as pd

from public_company_graph.gds.company_tech import (
    _build_batch,
    _identify_node_columns,
    compute_company_technology_similarity,
)


class TestIdentifyNodeColumns:
    """Tests for _identify_node_columns helper function."""

    def test_identifies_nodeid_columns(self):
        """Test identification of nodeId1/nodeId2 columns."""
        df = pd.DataFrame(
            {
                "nodeId1": [1, 2],
                "nodeId2": [3, 4],
                "similarity": [0.9, 0.8],
            }
        )

        col1, col2 = _identify_node_columns(df)

        assert col1 == "nodeId1"
        assert col2 == "nodeId2"

    def test_identifies_source_target_columns(self):
        """Test identification of source/target columns."""
        df = pd.DataFrame(
            {
                "source": [1, 2],
                "target": [3, 4],
                "score": [0.9, 0.8],
            }
        )

        col1, col2 = _identify_node_columns(df)

        assert col1 == "source"
        assert col2 == "target"

    def test_identifies_node1_node2_columns(self):
        """Test identification of node1/node2 columns."""
        df = pd.DataFrame(
            {
                "node1": [1, 2],
                "node2": [3, 4],
                "weight": [0.9, 0.8],
            }
        )

        col1, col2 = _identify_node_columns(df)

        assert col1 == "node1"
        assert col2 == "node2"

    def test_returns_none_for_unrecognized_columns(self):
        """Test that None is returned for unrecognized columns."""
        df = pd.DataFrame(
            {
                "foo": [1, 2],
                "bar": [3, 4],
            }
        )

        col1, col2 = _identify_node_columns(df)

        assert col1 is None
        assert col2 is None


class TestBuildBatch:
    """Tests for _build_batch helper function."""

    def test_builds_batch_from_dataframe(self):
        """Test building batch from DataFrame."""
        df = pd.DataFrame(
            {
                "nodeId1": [1, 2],
                "nodeId2": [3, 4],
                "similarity": [0.9, 0.8],
            }
        )
        mock_logger = MagicMock()

        batch = _build_batch(df, mock_logger)

        assert len(batch) == 2
        assert batch[0] == {"node_id1": 1, "node_id2": 3, "similarity": 0.9}
        assert batch[1] == {"node_id1": 2, "node_id2": 4, "similarity": 0.8}

    def test_builds_batch_from_dict_iterator(self):
        """Test building batch from dict-based iterator."""
        results = iter(
            [
                {"nodeId1": 1, "nodeId2": 2, "similarity": 0.9},
                {"node1": 3, "node2": 4, "score": 0.8},
            ]
        )
        mock_logger = MagicMock()

        batch = _build_batch(results, mock_logger)

        assert len(batch) == 2
        assert batch[0]["node_id1"] == 1
        assert batch[0]["node_id2"] == 2
        assert batch[0]["similarity"] == 0.9

    def test_builds_batch_from_tuple_iterator(self):
        """Test building batch from tuple-based iterator."""
        results = iter(
            [
                (1, 2, 0.9),
                (3, 4, 0.8),
            ]
        )
        mock_logger = MagicMock()

        batch = _build_batch(results, mock_logger)

        assert len(batch) == 2
        assert batch[0] == {"node_id1": 1, "node_id2": 2, "similarity": 0.9}
        assert batch[1] == {"node_id1": 3, "node_id2": 4, "similarity": 0.8}

    def test_handles_empty_dataframe(self):
        """Test building batch from empty DataFrame."""
        df = pd.DataFrame(
            {
                "nodeId1": [],
                "nodeId2": [],
                "similarity": [],
            }
        )
        mock_logger = MagicMock()

        batch = _build_batch(df, mock_logger)

        assert len(batch) == 0


class TestComputeCompanyTechnologySimilarity:
    """Tests for compute_company_technology_similarity function."""

    def test_dry_run_returns_zero(self):
        """Test that dry run mode returns 0 and doesn't modify data."""
        mock_gds = MagicMock()
        mock_driver = MagicMock()
        mock_logger = MagicMock()

        result = compute_company_technology_similarity(
            gds=mock_gds,
            driver=mock_driver,
            execute=False,
            logger=mock_logger,
        )

        assert result == 0
        mock_driver.session.assert_not_called()
        mock_gds.graph.project.cypher.assert_not_called()

    @patch("public_company_graph.gds.company_tech.safe_drop_graph")
    def test_creates_bipartite_graph(self, mock_safe_drop):
        """Test that bipartite Company-Technology graph is created."""
        mock_gds = MagicMock()
        mock_driver = MagicMock()
        mock_session = MagicMock()
        mock_driver.session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_driver.session.return_value.__exit__ = MagicMock(return_value=False)

        # Mock delete result
        mock_delete_result = MagicMock()
        mock_delete_result.single.return_value = {"deleted": 0}

        # Mock company IDs query
        mock_company_ids_result = MagicMock()
        mock_company_ids_result.single.return_value = {"company_ids": [1, 2, 3]}

        # Mock CIK mapping query
        mock_cik_map_result = MagicMock()
        mock_cik_map_result.__iter__ = MagicMock(
            return_value=iter([{"node_id": 1, "cik": "0001"}, {"node_id": 2, "cik": "0002"}])
        )

        mock_session.run.side_effect = [
            mock_delete_result,
            mock_company_ids_result,
            mock_cik_map_result,
        ]

        mock_graph = MagicMock()
        mock_gds.graph.project.cypher.return_value = (
            mock_graph,
            {"nodeCount": 100, "relationshipCount": 500},
        )

        # Empty similarity result
        mock_gds.nodeSimilarity.stream.return_value = pd.DataFrame(
            {"nodeId1": [], "nodeId2": [], "similarity": []}
        )

        compute_company_technology_similarity(
            gds=mock_gds,
            driver=mock_driver,
            execute=True,
            logger=MagicMock(),
        )

        # Verify bipartite graph was created
        mock_gds.graph.project.cypher.assert_called_once()
        # Check that the query includes both Company and Technology
        call_args = mock_gds.graph.project.cypher.call_args[0]
        assert "Company" in call_args[1]
        assert "Technology" in call_args[1]

    @patch("public_company_graph.gds.company_tech.safe_drop_graph")
    def test_deletes_existing_relationships_first(self, mock_safe_drop):
        """Test that existing SIMILAR_TECHNOLOGY relationships are deleted."""
        mock_gds = MagicMock()
        mock_driver = MagicMock()
        mock_session = MagicMock()
        mock_driver.session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_driver.session.return_value.__exit__ = MagicMock(return_value=False)

        mock_delete_result = MagicMock()
        mock_delete_result.single.return_value = {"deleted": 100}

        mock_company_ids_result = MagicMock()
        mock_company_ids_result.single.return_value = {"company_ids": []}

        mock_cik_map_result = MagicMock()
        mock_cik_map_result.__iter__ = MagicMock(return_value=iter([]))

        mock_session.run.side_effect = [
            mock_delete_result,
            mock_company_ids_result,
            mock_cik_map_result,
        ]

        mock_graph = MagicMock()
        mock_gds.graph.project.cypher.return_value = (
            mock_graph,
            {"nodeCount": 10, "relationshipCount": 20},
        )
        mock_gds.nodeSimilarity.stream.return_value = pd.DataFrame(
            {"nodeId1": [], "nodeId2": [], "similarity": []}
        )

        mock_logger = MagicMock()
        compute_company_technology_similarity(
            gds=mock_gds,
            driver=mock_driver,
            execute=True,
            logger=mock_logger,
        )

        # Verify delete query was called first
        first_query = mock_session.run.call_args_list[0][0][0]
        assert "DELETE" in first_query
        assert "SIMILAR_TECHNOLOGY" in first_query

    @patch("public_company_graph.gds.company_tech.safe_drop_graph")
    def test_filters_to_company_company_pairs_only(self, mock_safe_drop):
        """Test that results are filtered to Company-Company pairs only."""
        mock_gds = MagicMock()
        mock_driver = MagicMock()
        mock_session = MagicMock()
        mock_driver.session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_driver.session.return_value.__exit__ = MagicMock(return_value=False)

        mock_delete_result = MagicMock()
        mock_delete_result.single.return_value = {"deleted": 0}

        # Companies have IDs 1, 2, 3; Technologies have IDs 100, 101
        mock_company_ids_result = MagicMock()
        mock_company_ids_result.single.return_value = {"company_ids": [1, 2, 3]}

        mock_cik_map_result = MagicMock()
        mock_cik_map_result.__iter__ = MagicMock(
            return_value=iter(
                [
                    {"node_id": 1, "cik": "0001"},
                    {"node_id": 2, "cik": "0002"},
                    {"node_id": 3, "cik": "0003"},
                ]
            )
        )

        mock_write_result = MagicMock()
        mock_write_result.single.return_value = {"created": 1}

        mock_session.run.side_effect = [
            mock_delete_result,
            mock_company_ids_result,
            mock_cik_map_result,
            mock_write_result,
        ]

        mock_graph = MagicMock()
        mock_gds.graph.project.cypher.return_value = (
            mock_graph,
            {"nodeCount": 5, "relationshipCount": 10},
        )

        # Include Company-Company and Company-Technology pairs
        mock_gds.nodeSimilarity.stream.return_value = pd.DataFrame(
            {
                "nodeId1": [1, 1, 2],  # Companies 1 and 2 are similar; 1 similar to tech 100
                "nodeId2": [2, 100, 3],
                "similarity": [0.9, 0.5, 0.8],
            }
        )

        result = compute_company_technology_similarity(
            gds=mock_gds,
            driver=mock_driver,
            execute=True,
            logger=MagicMock(),
        )

        # Only Company-Company pairs should be written
        assert result == 1

    @patch("public_company_graph.gds.company_tech.safe_drop_graph")
    def test_handles_exception_gracefully(self, mock_safe_drop):
        """Test that exceptions are caught and logged."""
        mock_gds = MagicMock()
        mock_gds.graph.project.cypher.side_effect = Exception("GDS error")
        mock_driver = MagicMock()
        mock_session = MagicMock()
        mock_driver.session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_driver.session.return_value.__exit__ = MagicMock(return_value=False)

        mock_delete_result = MagicMock()
        mock_delete_result.single.return_value = {"deleted": 0}
        mock_session.run.return_value = mock_delete_result

        mock_logger = MagicMock()

        result = compute_company_technology_similarity(
            gds=mock_gds,
            driver=mock_driver,
            execute=True,
            logger=mock_logger,
        )

        assert result == 0
        mock_logger.error.assert_called()

    def test_uses_default_logger_when_none_provided(self):
        """Test that default logger is used when none provided."""
        mock_gds = MagicMock()
        mock_driver = MagicMock()

        # Should not raise - uses default logger
        result = compute_company_technology_similarity(
            gds=mock_gds,
            driver=mock_driver,
            execute=False,
            logger=None,
        )

        assert result == 0

    @patch("public_company_graph.gds.company_tech.safe_drop_graph")
    def test_respects_similarity_threshold(self, mock_safe_drop):
        """Test that similarity_threshold is passed to GDS."""
        mock_gds = MagicMock()
        mock_driver = MagicMock()
        mock_session = MagicMock()
        mock_driver.session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_driver.session.return_value.__exit__ = MagicMock(return_value=False)

        mock_delete_result = MagicMock()
        mock_delete_result.single.return_value = {"deleted": 0}

        mock_company_ids_result = MagicMock()
        mock_company_ids_result.single.return_value = {"company_ids": []}

        mock_cik_map_result = MagicMock()
        mock_cik_map_result.__iter__ = MagicMock(return_value=iter([]))

        mock_session.run.side_effect = [
            mock_delete_result,
            mock_company_ids_result,
            mock_cik_map_result,
        ]

        mock_graph = MagicMock()
        mock_gds.graph.project.cypher.return_value = (
            mock_graph,
            {"nodeCount": 10, "relationshipCount": 20},
        )
        mock_gds.nodeSimilarity.stream.return_value = pd.DataFrame(
            {"nodeId1": [], "nodeId2": [], "similarity": []}
        )

        compute_company_technology_similarity(
            gds=mock_gds,
            driver=mock_driver,
            similarity_threshold=0.7,
            execute=True,
            logger=MagicMock(),
        )

        call_kwargs = mock_gds.nodeSimilarity.stream.call_args[1]
        assert call_kwargs["similarityCutoff"] == 0.7

    @patch("public_company_graph.gds.company_tech.safe_drop_graph")
    def test_respects_top_k_parameter(self, mock_safe_drop):
        """Test that top_k is passed to GDS."""
        mock_gds = MagicMock()
        mock_driver = MagicMock()
        mock_session = MagicMock()
        mock_driver.session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_driver.session.return_value.__exit__ = MagicMock(return_value=False)

        mock_delete_result = MagicMock()
        mock_delete_result.single.return_value = {"deleted": 0}

        mock_company_ids_result = MagicMock()
        mock_company_ids_result.single.return_value = {"company_ids": []}

        mock_cik_map_result = MagicMock()
        mock_cik_map_result.__iter__ = MagicMock(return_value=iter([]))

        mock_session.run.side_effect = [
            mock_delete_result,
            mock_company_ids_result,
            mock_cik_map_result,
        ]

        mock_graph = MagicMock()
        mock_gds.graph.project.cypher.return_value = (
            mock_graph,
            {"nodeCount": 10, "relationshipCount": 20},
        )
        mock_gds.nodeSimilarity.stream.return_value = pd.DataFrame(
            {"nodeId1": [], "nodeId2": [], "similarity": []}
        )

        compute_company_technology_similarity(
            gds=mock_gds,
            driver=mock_driver,
            top_k=25,
            execute=True,
            logger=MagicMock(),
        )

        call_kwargs = mock_gds.nodeSimilarity.stream.call_args[1]
        assert call_kwargs["topK"] == 25

    @patch("public_company_graph.gds.company_tech.safe_drop_graph")
    def test_ensures_consistent_relationship_direction(self, mock_safe_drop):
        """Test that relationship direction is consistent (alphabetical CIK order)."""
        mock_gds = MagicMock()
        mock_driver = MagicMock()
        mock_session = MagicMock()
        mock_driver.session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_driver.session.return_value.__exit__ = MagicMock(return_value=False)

        mock_delete_result = MagicMock()
        mock_delete_result.single.return_value = {"deleted": 0}

        mock_company_ids_result = MagicMock()
        mock_company_ids_result.single.return_value = {"company_ids": [1, 2]}

        mock_cik_map_result = MagicMock()
        mock_cik_map_result.__iter__ = MagicMock(
            return_value=iter(
                [
                    {"node_id": 1, "cik": "000222"},  # Higher CIK
                    {"node_id": 2, "cik": "000111"},  # Lower CIK
                ]
            )
        )

        mock_write_result = MagicMock()
        mock_write_result.single.return_value = {"created": 1}

        mock_session.run.side_effect = [
            mock_delete_result,
            mock_company_ids_result,
            mock_cik_map_result,
            mock_write_result,
        ]

        mock_graph = MagicMock()
        mock_gds.graph.project.cypher.return_value = (
            mock_graph,
            {"nodeCount": 2, "relationshipCount": 5},
        )

        # Similarity result has "wrong" order (higher CIK first)
        mock_gds.nodeSimilarity.stream.return_value = pd.DataFrame(
            {
                "nodeId1": [1],  # CIK 000222 (higher)
                "nodeId2": [2],  # CIK 000111 (lower)
                "similarity": [0.9],
            }
        )

        compute_company_technology_similarity(
            gds=mock_gds,
            driver=mock_driver,
            execute=True,
            logger=MagicMock(),
        )

        # The batch should have been reordered so lower CIK comes first
        write_call = mock_session.run.call_args_list[-1]
        batch = write_call[1]["batch"]
        assert batch[0]["cik1"] == "000111"  # Lower CIK first
        assert batch[0]["cik2"] == "000222"

    @patch("public_company_graph.gds.company_tech.safe_drop_graph")
    def test_drops_graph_after_completion(self, mock_safe_drop):
        """Test that graph is dropped after processing."""
        mock_gds = MagicMock()
        mock_driver = MagicMock()
        mock_session = MagicMock()
        mock_driver.session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_driver.session.return_value.__exit__ = MagicMock(return_value=False)

        mock_delete_result = MagicMock()
        mock_delete_result.single.return_value = {"deleted": 0}

        mock_company_ids_result = MagicMock()
        mock_company_ids_result.single.return_value = {"company_ids": []}

        mock_cik_map_result = MagicMock()
        mock_cik_map_result.__iter__ = MagicMock(return_value=iter([]))

        mock_session.run.side_effect = [
            mock_delete_result,
            mock_company_ids_result,
            mock_cik_map_result,
        ]

        mock_graph = MagicMock()
        mock_gds.graph.project.cypher.return_value = (
            mock_graph,
            {"nodeCount": 10, "relationshipCount": 20},
        )
        mock_gds.nodeSimilarity.stream.return_value = pd.DataFrame(
            {"nodeId1": [], "nodeId2": [], "similarity": []}
        )

        compute_company_technology_similarity(
            gds=mock_gds,
            driver=mock_driver,
            execute=True,
            logger=MagicMock(),
        )

        mock_graph.drop.assert_called_once()
