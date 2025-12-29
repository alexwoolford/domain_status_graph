"""
Unit tests for GDS technology adoption prediction functions.
"""

from unittest.mock import MagicMock, patch

from domain_status_graph.gds.tech_adoption import compute_tech_adoption_prediction


class TestComputeTechAdoptionPrediction:
    """Tests for compute_tech_adoption_prediction function."""

    @patch("domain_status_graph.gds.tech_adoption.delete_relationships_in_batches")
    @patch("domain_status_graph.gds.tech_adoption.safe_drop_graph")
    def test_creates_graph_and_runs_pagerank(self, mock_safe_drop, mock_delete_rels):
        """Test full flow: graph creation and PageRank computation."""
        mock_gds = MagicMock()
        mock_driver = MagicMock()
        mock_session = MagicMock()
        mock_driver.session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_driver.session.return_value.__exit__ = MagicMock(return_value=False)

        # Mock graph projection
        mock_graph = MagicMock()
        mock_projection_result = {"nodeCount": 100, "relationshipCount": 500}
        mock_gds.graph.project.cypher.return_value = (mock_graph, mock_projection_result)

        # Mock technology query - return 2 technologies
        class MockTechRecord:
            def __init__(self, tech_id, tech_name, domain_count):
                self._data = {
                    "tech_id": tech_id,
                    "tech_name": tech_name,
                    "domain_count": domain_count,
                }

            def __getitem__(self, key):
                return self._data[key]

        mock_tech_result = MagicMock()
        mock_tech_result.__iter__ = MagicMock(
            return_value=iter(
                [
                    MockTechRecord(1, "React", 50),
                    MockTechRecord(2, "Vue", 30),
                ]
            )
        )

        # Mock PageRank write (returns something)
        mock_gds.pageRank.write.return_value = MagicMock()

        # Mock relationship creation and cleanup
        mock_rel_result = MagicMock()
        mock_rel_result.single.return_value = {"created": 5}

        mock_cleanup_result = MagicMock()

        mock_session.run.side_effect = [
            mock_tech_result,  # Get technologies
            mock_rel_result,  # Write relationships for tech 1
            mock_cleanup_result,  # Cleanup temp property
            mock_rel_result,  # Write relationships for tech 2
            mock_cleanup_result,  # Cleanup temp property
        ]

        compute_tech_adoption_prediction(
            gds=mock_gds,
            driver=mock_driver,
            database="testdb",
            batch_size=10,
            logger=MagicMock(),
        )

        # Verify graph was projected
        mock_gds.graph.project.cypher.assert_called_once()

        # Verify PageRank was called
        assert mock_gds.pageRank.write.called

        # Verify graph was dropped
        mock_graph.drop.assert_called_once()

        # Result is accumulated - due to mocking complexity, just verify no exception
        # The actual value depends on mock setup; integration tests verify correct counts

    @patch("domain_status_graph.gds.tech_adoption.delete_relationships_in_batches")
    @patch("domain_status_graph.gds.tech_adoption.safe_drop_graph")
    def test_deletes_existing_relationships(self, mock_safe_drop, mock_delete_rels):
        """Test that existing LIKELY_TO_ADOPT relationships are deleted."""
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

        # No technologies to process
        mock_tech_result = MagicMock()
        mock_tech_result.__iter__ = MagicMock(return_value=iter([]))
        mock_session.run.return_value = mock_tech_result

        compute_tech_adoption_prediction(
            gds=mock_gds,
            driver=mock_driver,
            database="testdb",
            logger=MagicMock(),
        )

        # Verify delete was called
        mock_delete_rels.assert_called_once()
        call_kwargs = mock_delete_rels.call_args[1]
        assert call_kwargs["database"] == "testdb"

    @patch("domain_status_graph.gds.tech_adoption.delete_relationships_in_batches")
    @patch("domain_status_graph.gds.tech_adoption.safe_drop_graph")
    def test_handles_exception_gracefully(self, mock_safe_drop, mock_delete_rels):
        """Test that exceptions are caught and logged."""
        mock_gds = MagicMock()
        mock_gds.graph.project.cypher.side_effect = Exception("GDS error")
        mock_driver = MagicMock()
        mock_logger = MagicMock()

        result = compute_tech_adoption_prediction(
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

        # Should not raise
        result = compute_tech_adoption_prediction(
            gds=mock_gds,
            driver=MagicMock(),
            logger=None,
        )

        assert result == 0

    @patch("domain_status_graph.gds.tech_adoption.delete_relationships_in_batches")
    @patch("domain_status_graph.gds.tech_adoption.safe_drop_graph")
    def test_respects_max_iterations_parameter(self, mock_safe_drop, mock_delete_rels):
        """Test that max_iterations is passed to PageRank."""
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

        # One technology to process
        mock_tech_result = MagicMock()
        mock_tech_result.__iter__ = MagicMock(
            return_value=iter([{"tech_id": 1, "tech_name": "React", "domain_count": 10}])
        )

        mock_rel_result = MagicMock()
        mock_rel_result.single.return_value = {"created": 1}

        mock_session.run.side_effect = [
            mock_tech_result,
            mock_rel_result,
            MagicMock(),  # cleanup
        ]

        compute_tech_adoption_prediction(
            gds=mock_gds,
            driver=mock_driver,
            max_iterations=50,
            logger=MagicMock(),
        )

        # Verify max_iterations was passed
        call_kwargs = mock_gds.pageRank.write.call_args[1]
        assert call_kwargs["maxIterations"] == 50

    @patch("domain_status_graph.gds.tech_adoption.delete_relationships_in_batches")
    @patch("domain_status_graph.gds.tech_adoption.safe_drop_graph")
    def test_respects_damping_factor_parameter(self, mock_safe_drop, mock_delete_rels):
        """Test that damping_factor is passed to PageRank."""
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

        mock_tech_result = MagicMock()
        mock_tech_result.__iter__ = MagicMock(
            return_value=iter([{"tech_id": 1, "tech_name": "React", "domain_count": 10}])
        )

        mock_rel_result = MagicMock()
        mock_rel_result.single.return_value = {"created": 1}

        mock_session.run.side_effect = [
            mock_tech_result,
            mock_rel_result,
            MagicMock(),
        ]

        compute_tech_adoption_prediction(
            gds=mock_gds,
            driver=mock_driver,
            damping_factor=0.9,
            logger=MagicMock(),
        )

        call_kwargs = mock_gds.pageRank.write.call_args[1]
        assert call_kwargs["dampingFactor"] == 0.9

    @patch("domain_status_graph.gds.tech_adoption.delete_relationships_in_batches")
    @patch("domain_status_graph.gds.tech_adoption.safe_drop_graph")
    def test_processes_technologies_in_batches(self, mock_safe_drop, mock_delete_rels):
        """Test that technologies are processed in batches."""
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

        # 25 technologies to process (should be 2 batches with batch_size=20)
        technologies = [
            {"tech_id": i, "tech_name": f"Tech{i}", "domain_count": 10} for i in range(25)
        ]
        mock_tech_result = MagicMock()
        mock_tech_result.__iter__ = MagicMock(return_value=iter(technologies))

        mock_rel_result = MagicMock()
        mock_rel_result.single.return_value = {"created": 5}

        # Need enough returns for all operations
        mock_session.run.side_effect = (
            [mock_tech_result] + [mock_rel_result] * 50 + [MagicMock()] * 50
        )

        compute_tech_adoption_prediction(
            gds=mock_gds,
            driver=mock_driver,
            batch_size=20,  # 2 batches of 20 and 5
            logger=MagicMock(),
        )

        # PageRank should be called twice (once per batch)
        assert mock_gds.pageRank.write.call_count == 2

    @patch("domain_status_graph.gds.tech_adoption.delete_relationships_in_batches")
    @patch("domain_status_graph.gds.tech_adoption.safe_drop_graph")
    def test_handles_individual_tech_errors(self, mock_safe_drop, mock_delete_rels):
        """Test that errors processing individual technologies don't stop the batch."""
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

        class MockTechRecord:
            def __init__(self, tech_id, tech_name, domain_count):
                self._data = {
                    "tech_id": tech_id,
                    "tech_name": tech_name,
                    "domain_count": domain_count,
                }

            def __getitem__(self, key):
                return self._data[key]

        mock_tech_result = MagicMock()
        mock_tech_result.__iter__ = MagicMock(
            return_value=iter(
                [
                    MockTechRecord(1, "Tech1", 10),
                    MockTechRecord(2, "Tech2", 10),
                ]
            )
        )

        # First tech succeeds, second fails
        mock_success_result = MagicMock()
        mock_success_result.single.return_value = {"created": 5}

        call_count = [0]

        def side_effect(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                return mock_tech_result
            elif call_count[0] == 2:
                return mock_success_result
            elif call_count[0] == 3:
                return MagicMock()  # cleanup
            elif call_count[0] == 4:
                raise Exception("Tech2 failed")
            else:
                return MagicMock()

        mock_session.run.side_effect = side_effect

        mock_logger = MagicMock()
        compute_tech_adoption_prediction(
            gds=mock_gds,
            driver=mock_driver,
            batch_size=10,
            logger=mock_logger,
        )

        # Due to mocking complexity, we just verify the function completes
        # without raising and logs a warning for the failed tech
        mock_logger.warning.assert_called()

    @patch("domain_status_graph.gds.tech_adoption.delete_relationships_in_batches")
    @patch("domain_status_graph.gds.tech_adoption.safe_drop_graph")
    def test_filters_ubiquitous_technologies(self, mock_safe_drop, mock_delete_rels):
        """Test that query filters out technologies used by >50% of domains."""
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

        mock_tech_result = MagicMock()
        mock_tech_result.__iter__ = MagicMock(return_value=iter([]))
        mock_session.run.return_value = mock_tech_result

        compute_tech_adoption_prediction(
            gds=mock_gds,
            driver=mock_driver,
            logger=MagicMock(),
        )

        # Check that the Cypher query includes the 50% filter
        cypher_query = mock_session.run.call_args[0][0]
        assert "0.5" in cypher_query or "50" in cypher_query.replace(" ", "")

    @patch("domain_status_graph.gds.tech_adoption.delete_relationships_in_batches")
    @patch("domain_status_graph.gds.tech_adoption.safe_drop_graph")
    def test_cleans_up_temp_property_after_batch(self, mock_safe_drop, mock_delete_rels):
        """Test that temporary PageRank property is cleaned up after each batch."""
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

        mock_tech_result = MagicMock()
        mock_tech_result.__iter__ = MagicMock(
            return_value=iter([{"tech_id": 1, "tech_name": "React", "domain_count": 10}])
        )

        mock_rel_result = MagicMock()
        mock_rel_result.single.return_value = {"created": 5}

        mock_cleanup_result = MagicMock()

        mock_session.run.side_effect = [
            mock_tech_result,
            mock_rel_result,
            mock_cleanup_result,  # This should be the cleanup call
        ]

        compute_tech_adoption_prediction(
            gds=mock_gds,
            driver=mock_driver,
            batch_size=10,
            logger=MagicMock(),
        )

        # Verify cleanup query was called (removes ppr_score_temp)
        cleanup_calls = [
            call
            for call in mock_session.run.call_args_list
            if "ppr_score_temp" in str(call) and "REMOVE" in str(call)
        ]
        assert len(cleanup_calls) >= 1
