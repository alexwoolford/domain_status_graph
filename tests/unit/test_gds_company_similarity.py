"""
Unit tests for GDS company similarity functions.
"""

from unittest.mock import MagicMock, patch

from public_company_graph.gds.company_similarity import (
    compute_company_description_similarity,
)


class TestComputeCompanyDescriptionSimilarity:
    """Tests for compute_company_description_similarity function."""

    def test_dry_run_returns_zero(self):
        """Test that dry run mode returns 0 and doesn't modify data."""
        mock_driver = MagicMock()
        mock_logger = MagicMock()

        result = compute_company_description_similarity(
            driver=mock_driver,
            execute=False,
            logger=mock_logger,
        )

        assert result == 0
        mock_driver.session.assert_not_called()
        mock_logger.info.assert_called()  # Should log the dry run info

    def test_returns_zero_with_no_companies(self):
        """Test that function returns 0 when no companies have embeddings."""
        mock_driver = MagicMock()
        mock_session = MagicMock()
        mock_driver.session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_driver.session.return_value.__exit__ = MagicMock(return_value=False)

        # First call: delete existing relationships
        mock_delete_result = MagicMock()
        mock_delete_result.single.return_value = {"deleted": 0}

        # Second call: query for companies with embeddings
        mock_query_result = MagicMock()
        mock_query_result.__iter__ = MagicMock(return_value=iter([]))

        mock_session.run.side_effect = [mock_delete_result, mock_query_result]

        result = compute_company_description_similarity(
            driver=mock_driver,
            execute=True,
            logger=MagicMock(),
        )

        assert result == 0

    def test_returns_zero_with_only_one_company(self):
        """Test that function returns 0 when only one company has embeddings."""
        mock_driver = MagicMock()
        mock_session = MagicMock()
        mock_driver.session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_driver.session.return_value.__exit__ = MagicMock(return_value=False)

        # First call: delete existing relationships
        mock_delete_result = MagicMock()
        mock_delete_result.single.return_value = {"deleted": 0}

        # Second call: query for companies with embeddings - only one company
        class MockRecord:
            def __init__(self, cik, embedding):
                self._data = {"cik": cik, "embedding": embedding}

            def __getitem__(self, key):
                return self._data[key]

        mock_query_result = MagicMock()
        mock_query_result.__iter__ = MagicMock(
            return_value=iter([MockRecord("0001234567", [0.1, 0.2, 0.3])])
        )

        mock_session.run.side_effect = [mock_delete_result, mock_query_result]

        mock_logger = MagicMock()
        result = compute_company_description_similarity(
            driver=mock_driver,
            execute=True,
            logger=mock_logger,
        )

        assert result == 0
        mock_logger.warning.assert_called()

    @patch("public_company_graph.gds.company_similarity.find_top_k_similar_pairs")
    def test_computes_similarity_and_writes_relationships(self, mock_find_similar):
        """Test that similarity is computed and relationships are written."""
        mock_driver = MagicMock()
        mock_session = MagicMock()
        mock_driver.session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_driver.session.return_value.__exit__ = MagicMock(return_value=False)

        # First call: delete existing relationships
        mock_delete_result = MagicMock()
        mock_delete_result.single.return_value = {"deleted": 5}

        # Second call: query for companies with embeddings
        class MockRecord:
            def __init__(self, cik, embedding):
                self._data = {"cik": cik, "embedding": embedding}

            def __getitem__(self, key):
                return self._data[key]

        companies = [
            MockRecord("0001234567", [0.1, 0.2, 0.3]),
            MockRecord("0007654321", [0.15, 0.25, 0.35]),
            MockRecord("0009999999", [0.2, 0.3, 0.4]),
        ]
        mock_query_result = MagicMock()
        mock_query_result.__iter__ = MagicMock(return_value=iter(companies))

        # Third call: write relationships
        mock_write_result = MagicMock()
        mock_write_result.single.return_value = {"created": 3}

        mock_session.run.side_effect = [
            mock_delete_result,
            mock_query_result,
            mock_write_result,
        ]

        # Mock the similarity function
        mock_find_similar.return_value = {
            ("0001234567", "0007654321"): 0.95,
            ("0001234567", "0009999999"): 0.85,
            ("0007654321", "0009999999"): 0.75,
        }

        result = compute_company_description_similarity(
            driver=mock_driver,
            execute=True,
            logger=MagicMock(),
        )

        # Bidirectional relationships: 3 pairs * 2 directions = 6 relationships
        assert result == 6
        mock_find_similar.assert_called_once()

    def test_handles_exception_gracefully(self):
        """Test that exceptions are handled and logged."""
        mock_driver = MagicMock()
        mock_driver.session.side_effect = Exception("Connection failed")
        mock_logger = MagicMock()

        result = compute_company_description_similarity(
            driver=mock_driver,
            execute=True,
            logger=mock_logger,
        )

        assert result == 0
        mock_logger.error.assert_called()

    def test_uses_default_logger_when_none_provided(self):
        """Test that default logger is used when none provided."""
        mock_driver = MagicMock()
        mock_session = MagicMock()
        mock_driver.session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_driver.session.return_value.__exit__ = MagicMock(return_value=False)

        mock_delete_result = MagicMock()
        mock_delete_result.single.return_value = {"deleted": 0}

        mock_query_result = MagicMock()
        mock_query_result.__iter__ = MagicMock(return_value=iter([]))

        mock_session.run.side_effect = [mock_delete_result, mock_query_result]

        # Should not raise - uses default logger
        result = compute_company_description_similarity(
            driver=mock_driver,
            execute=True,
            logger=None,
        )

        assert result == 0

    def test_respects_similarity_threshold_and_top_k(self):
        """Test that similarity_threshold and top_k parameters are passed correctly."""
        mock_driver = MagicMock()
        mock_session = MagicMock()
        mock_driver.session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_driver.session.return_value.__exit__ = MagicMock(return_value=False)

        mock_delete_result = MagicMock()
        mock_delete_result.single.return_value = {"deleted": 0}

        class MockRecord:
            def __init__(self, cik, embedding):
                self._data = {"cik": cik, "embedding": embedding}

            def __getitem__(self, key):
                return self._data[key]

        mock_query_result = MagicMock()
        mock_query_result.__iter__ = MagicMock(
            return_value=iter(
                [
                    MockRecord("0001234567", [0.1] * 100),
                    MockRecord("0007654321", [0.2] * 100),
                ]
            )
        )

        mock_write_result = MagicMock()
        mock_write_result.single.return_value = {"created": 1}

        mock_session.run.side_effect = [
            mock_delete_result,
            mock_query_result,
            mock_write_result,
        ]

        with patch(
            "public_company_graph.gds.company_similarity.find_top_k_similar_pairs"
        ) as mock_find:
            mock_find.return_value = {("0001234567", "0007654321"): 0.9}

            compute_company_description_similarity(
                driver=mock_driver,
                similarity_threshold=0.8,
                top_k=5,
                execute=True,
            )

            mock_find.assert_called_once()
            call_kwargs = mock_find.call_args[1]
            assert call_kwargs["similarity_threshold"] == 0.8
            assert call_kwargs["top_k"] == 5

    @patch("public_company_graph.gds.company_similarity.find_top_k_similar_pairs")
    def test_batch_writing_large_result_sets(self, mock_find_similar):
        """Test that large result sets are written in batches."""
        mock_driver = MagicMock()
        mock_session = MagicMock()
        mock_driver.session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_driver.session.return_value.__exit__ = MagicMock(return_value=False)

        mock_delete_result = MagicMock()
        mock_delete_result.single.return_value = {"deleted": 0}

        class MockRecord:
            def __init__(self, cik, embedding):
                self._data = {"cik": cik, "embedding": embedding}

            def __getitem__(self, key):
                return self._data[key]

        # Create many companies
        companies = [MockRecord(f"000{i:07d}", [0.1] * 100) for i in range(10)]
        mock_query_result = MagicMock()
        mock_query_result.__iter__ = MagicMock(return_value=iter(companies))

        # Generate many similar pairs (more than batch size)
        pairs = {}
        for i in range(45):  # 10 choose 2 = 45 pairs
            cik1 = f"000{i // 9:07d}"
            cik2 = f"000{(i % 9) + 1:07d}"
            if cik1 != cik2:
                pairs[(cik1, cik2)] = 0.9

        mock_find_similar.return_value = pairs

        # Each batch write returns count
        mock_write_result = MagicMock()
        mock_write_result.single.return_value = {"created": len(pairs)}

        mock_session.run.side_effect = [
            mock_delete_result,
            mock_query_result,
            mock_write_result,
        ]

        result = compute_company_description_similarity(
            driver=mock_driver,
            execute=True,
        )

        # Should have written all pairs bidirectionally (pairs * 2)
        assert result == len(pairs) * 2
