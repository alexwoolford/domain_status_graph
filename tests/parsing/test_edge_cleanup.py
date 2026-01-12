"""Tests for edge cleanup functionality."""

import pytest

from public_company_graph.parsing.edge_cleanup import cleanup_relationship_edges
from public_company_graph.parsing.relationship_config import (
    RELATIONSHIP_CONFIGS,
    get_confidence_tier,
)


class TestEdgeCleanup:
    """Tests for edge cleanup."""

    @pytest.fixture
    def mock_driver(self, mocker):
        """Create a mock Neo4j driver."""
        driver = mocker.Mock()
        session = mocker.Mock()
        # Make session() return a context manager
        session_context = mocker.MagicMock()
        session_context.__enter__ = mocker.Mock(return_value=session)
        session_context.__exit__ = mocker.Mock(return_value=None)
        driver.session = mocker.Mock(return_value=session_context)
        return driver, session

    def test_cleanup_keeps_high_confidence_edges(self, mock_driver):
        """High confidence edges should be kept."""
        driver, session = mock_driver

        # Mock edge with high confidence
        edge = {
            "source_cik": "0001234567",
            "target_cik": "0007654321",
            "embedding_similarity": 0.50,  # Above high threshold (0.35)
            "confidence": 0.8,
            "raw_mention": "Microsoft",
            "context": "Our competitors include Microsoft",
            "confidence_tier": "high",
            "edge_id": "edge1",
        }

        session.run.return_value = [edge]
        session.write_transaction = lambda fn, *args: fn(session, *args)

        stats = cleanup_relationship_edges(
            driver=driver,
            database="test",
            relationship_types=["HAS_COMPETITOR"],
            dry_run=True,
        )

        assert stats["HAS_COMPETITOR"]["kept"] == 1
        assert stats["HAS_COMPETITOR"]["converted"] == 0
        assert stats["HAS_COMPETITOR"]["deleted"] == 0

    def test_cleanup_converts_medium_confidence_edges(self, mock_driver):
        """Medium confidence edges should be converted to candidates."""
        driver, session = mock_driver

        # Mock edge with medium confidence
        edge = {
            "source_cik": "0001234567",
            "target_cik": "0007654321",
            "embedding_similarity": 0.30,  # Between medium (0.25) and high (0.35)
            "confidence": 0.6,
            "raw_mention": "Microsoft",
            "context": "We work with Microsoft",
            "confidence_tier": "medium",
            "edge_id": "edge1",
        }

        session.run.return_value = [edge]
        session.write_transaction = lambda fn, *args: fn(session, *args)

        stats = cleanup_relationship_edges(
            driver=driver,
            database="test",
            relationship_types=["HAS_COMPETITOR"],
            dry_run=True,
        )

        assert stats["HAS_COMPETITOR"]["kept"] == 0
        assert stats["HAS_COMPETITOR"]["converted"] == 1
        assert stats["HAS_COMPETITOR"]["deleted"] == 0

    def test_cleanup_deletes_low_confidence_edges(self, mock_driver):
        """Low confidence edges should be deleted."""
        driver, session = mock_driver

        # Mock edge with low confidence
        edge = {
            "source_cik": "0001234567",
            "target_cik": "0007654321",
            "embedding_similarity": 0.20,  # Below medium threshold (0.25)
            "confidence": 0.4,
            "raw_mention": "Microsoft",
            "context": "We have no relationship with Microsoft",
            "confidence_tier": "low",
            "edge_id": "edge1",
        }

        session.run.return_value = [edge]
        session.write_transaction = lambda fn, *args: fn(session, *args)

        stats = cleanup_relationship_edges(
            driver=driver,
            database="test",
            relationship_types=["HAS_COMPETITOR"],
            dry_run=True,
        )

        assert stats["HAS_COMPETITOR"]["kept"] == 0
        assert stats["HAS_COMPETITOR"]["converted"] == 0
        assert stats["HAS_COMPETITOR"]["deleted"] == 1

    def test_cleanup_handles_missing_embedding_similarity(self, mock_driver):
        """Edges without embedding_similarity should be deleted."""
        driver, session = mock_driver

        # Mock edge without embedding similarity
        edge = {
            "source_cik": "0001234567",
            "target_cik": "0007654321",
            "embedding_similarity": None,  # Missing
            "confidence": 0.5,
            "raw_mention": "Microsoft",
            "context": "We work with Microsoft",
            "confidence_tier": None,
            "edge_id": "edge1",
        }

        session.run.return_value = [edge]
        session.write_transaction = lambda fn, *args: fn(session, *args)

        stats = cleanup_relationship_edges(
            driver=driver,
            database="test",
            relationship_types=["HAS_COMPETITOR"],
            dry_run=True,
        )

        # Missing embedding should be treated as low confidence
        assert stats["HAS_COMPETITOR"]["deleted"] == 1

    def test_cleanup_handles_empty_result(self, mock_driver):
        """Should handle empty edge results gracefully."""
        driver, session = mock_driver

        session.run.return_value = []

        stats = cleanup_relationship_edges(
            driver=driver,
            database="test",
            relationship_types=["HAS_COMPETITOR"],
            dry_run=True,
        )

        assert stats["HAS_COMPETITOR"]["kept"] == 0
        assert stats["HAS_COMPETITOR"]["converted"] == 0
        assert stats["HAS_COMPETITOR"]["deleted"] == 0

    def test_cleanup_respects_relationship_config(self, mock_driver):
        """Should use correct thresholds from relationship config."""
        driver, session = mock_driver

        config = RELATIONSHIP_CONFIGS["HAS_COMPETITOR"]

        # Test with edge at exact high threshold
        edge = {
            "source_cik": "0001234567",
            "target_cik": "0007654321",
            "embedding_similarity": config.high_threshold,  # Exactly at threshold
            "confidence": 0.7,
            "raw_mention": "Microsoft",
            "context": "Our competitors include Microsoft",
            "confidence_tier": "high",
            "edge_id": "edge1",
        }

        session.run.return_value = [edge]
        session.write_transaction = lambda fn, *args: fn(session, *args)

        stats = cleanup_relationship_edges(
            driver=driver,
            database="test",
            relationship_types=["HAS_COMPETITOR"],
            dry_run=True,
        )

        # Should be kept (at threshold is high)
        assert stats["HAS_COMPETITOR"]["kept"] == 1

        # Verify tier calculation
        tier = get_confidence_tier("HAS_COMPETITOR", config.high_threshold)
        assert tier.value == "high"

    def test_cleanup_skips_disabled_relationship_types(self, mock_driver):
        """Should skip relationship types that are disabled."""
        driver, session = mock_driver

        stats = cleanup_relationship_edges(
            driver=driver,
            database="test",
            relationship_types=["DISABLED_REL_TYPE"],  # Not in config
            dry_run=True,
        )

        # Should not crash, just skip
        assert (
            "DISABLED_REL_TYPE" not in stats
            or stats.get("DISABLED_REL_TYPE", {}).get("kept", 0) == 0
        )
