"""
Unit tests for datamule_index module.

Tests the bulk index search and "no 10-K" caching functionality.
"""

from unittest.mock import MagicMock, patch

from public_company_graph.sources.datamule_index import (
    CACHE_NAMESPACE_NO_10K,
    clear_no_10k_cache,
    filter_companies_with_10k_fast,
    get_all_ciks_with_10k,
    get_ciks_without_10k,
    mark_cik_no_10k_available,
)


class TestNo10KCache:
    """Tests for the 'no 10-K available' caching functions."""

    def test_mark_and_get_cik_no_10k(self, tmp_path):
        """Test marking a CIK as having no 10-K and retrieving it."""
        with patch("public_company_graph.sources.datamule_index.get_cache") as mock_get_cache:
            mock_cache = MagicMock()
            mock_cache.get.return_value = None  # Empty cache initially
            mock_get_cache.return_value = mock_cache

            # Mark a CIK
            mark_cik_no_10k_available("1234567890")

            # Verify cache.set was called with the CIK
            mock_cache.set.assert_called_once()
            call_args = mock_cache.set.call_args
            assert call_args[0][0] == CACHE_NAMESPACE_NO_10K
            assert call_args[0][1] == "ciks"
            assert "1234567890" in call_args[0][2]

    def test_mark_cik_normalizes_to_10_digits(self, tmp_path):
        """Test that CIKs are normalized to 10-digit zero-padded format."""
        with patch("public_company_graph.sources.datamule_index.get_cache") as mock_get_cache:
            mock_cache = MagicMock()
            mock_cache.get.return_value = None
            mock_get_cache.return_value = mock_cache

            # Mark with non-padded CIK
            mark_cik_no_10k_available("12345")

            call_args = mock_cache.set.call_args
            # Should be zero-padded to 10 digits
            assert "0000012345" in call_args[0][2]

    def test_mark_cik_does_not_duplicate(self):
        """Test that marking the same CIK twice doesn't create duplicates."""
        with patch("public_company_graph.sources.datamule_index.get_cache") as mock_get_cache:
            mock_cache = MagicMock()
            # First call: empty cache
            # Second call: cache has the CIK
            mock_cache.get.side_effect = [None, ["0000012345"]]
            mock_get_cache.return_value = mock_cache

            mark_cik_no_10k_available("12345")
            mark_cik_no_10k_available("12345")

            # Both calls should result in a set with just one CIK
            # (the second set call should still only have one element)
            last_call_args = mock_cache.set.call_args
            assert len(last_call_args[0][2]) == 1

    def test_get_ciks_without_10k_empty(self):
        """Test getting CIKs when cache is empty."""
        with patch("public_company_graph.sources.datamule_index.get_cache") as mock_get_cache:
            mock_cache = MagicMock()
            mock_cache.get.return_value = None
            mock_get_cache.return_value = mock_cache

            result = get_ciks_without_10k()
            assert result == set()

    def test_get_ciks_without_10k_returns_set(self):
        """Test getting CIKs returns a set."""
        with patch("public_company_graph.sources.datamule_index.get_cache") as mock_get_cache:
            mock_cache = MagicMock()
            mock_cache.get.return_value = ["0000012345", "0000067890"]
            mock_get_cache.return_value = mock_cache

            result = get_ciks_without_10k()
            assert isinstance(result, set)
            assert "0000012345" in result
            assert "0000067890" in result

    def test_clear_no_10k_cache(self):
        """Test clearing the no-10K cache."""
        with patch("public_company_graph.sources.datamule_index.get_cache") as mock_get_cache:
            mock_cache = MagicMock()
            mock_cache.count.return_value = 5
            mock_get_cache.return_value = mock_cache

            result = clear_no_10k_cache()

            assert result == 5
            mock_cache.clear_namespace.assert_called_once_with(CACHE_NAMESPACE_NO_10K)


class TestFilterCompaniesWithNo10KCache:
    """Tests for filter_companies_with_10k_fast with no-10K cache integration."""

    def test_filter_excludes_known_no_10k_ciks(self):
        """Test that companies in the no-10K cache are filtered out."""
        companies = [
            {"cik": "0000012345", "ticker": "GOOD", "name": "Good Company"},
            {"cik": "0000067890", "ticker": "BAD", "name": "Bad Company"},
            {"cik": "0000011111", "ticker": "ALSO_GOOD", "name": "Also Good"},
        ]

        with patch(
            "public_company_graph.sources.datamule_index.get_all_ciks_with_10k"
        ) as mock_get_10k:
            with patch(
                "public_company_graph.sources.datamule_index.get_ciks_without_10k"
            ) as mock_get_no_10k:
                # All three are in the 10-K index
                mock_get_10k.return_value = {"0000012345", "0000067890", "0000011111"}
                # But one is known to have no downloadable 10-K
                mock_get_no_10k.return_value = {"0000067890"}

                result = list(filter_companies_with_10k_fast(companies))

                # Should exclude the "BAD" company
                assert len(result) == 2
                tickers = [c["ticker"] for c in result]
                assert "GOOD" in tickers
                assert "ALSO_GOOD" in tickers
                assert "BAD" not in tickers

    def test_filter_excludes_companies_not_in_index(self):
        """Test that companies not in the Datamule index are filtered out."""
        companies = [
            {"cik": "0000012345", "ticker": "IN_INDEX", "name": "In Index"},
            {"cik": "0000099999", "ticker": "NOT_IN_INDEX", "name": "Not In Index"},
        ]

        with patch(
            "public_company_graph.sources.datamule_index.get_all_ciks_with_10k"
        ) as mock_get_10k:
            with patch(
                "public_company_graph.sources.datamule_index.get_ciks_without_10k"
            ) as mock_get_no_10k:
                mock_get_10k.return_value = {"0000012345"}  # Only one in index
                mock_get_no_10k.return_value = set()  # No known bad CIKs

                result = list(filter_companies_with_10k_fast(companies))

                assert len(result) == 1
                assert result[0]["ticker"] == "IN_INDEX"

    def test_filter_yields_all_when_index_unavailable(self):
        """Test fail-safe: yields all companies when index search fails."""
        companies = [
            {"cik": "0000012345", "ticker": "A", "name": "Company A"},
            {"cik": "0000067890", "ticker": "B", "name": "Company B"},
        ]

        with patch(
            "public_company_graph.sources.datamule_index.get_all_ciks_with_10k"
        ) as mock_get_10k:
            with patch(
                "public_company_graph.sources.datamule_index.get_ciks_without_10k"
            ) as mock_get_no_10k:
                mock_get_10k.return_value = set()  # Empty - index unavailable
                mock_get_no_10k.return_value = set()

                result = list(filter_companies_with_10k_fast(companies))

                # Should yield all companies as fail-safe
                assert len(result) == 2

    def test_filter_normalizes_ciks_for_comparison(self):
        """Test that CIKs are normalized for comparison."""
        companies = [
            {"cik": "12345", "ticker": "SHORT_CIK", "name": "Short CIK Company"},
        ]

        with patch(
            "public_company_graph.sources.datamule_index.get_all_ciks_with_10k"
        ) as mock_get_10k:
            with patch(
                "public_company_graph.sources.datamule_index.get_ciks_without_10k"
            ) as mock_get_no_10k:
                # Index has zero-padded CIK
                mock_get_10k.return_value = {"0000012345"}
                mock_get_no_10k.return_value = set()

                result = list(filter_companies_with_10k_fast(companies))

                # Should match despite different formatting
                assert len(result) == 1


class TestGetAllCiksWith10K:
    """Tests for get_all_ciks_with_10k function."""

    def test_returns_cached_results(self):
        """Test that cached results are returned without API call."""
        with patch("public_company_graph.sources.datamule_index.get_cache") as mock_get_cache:
            mock_cache = MagicMock()
            mock_cache.get.return_value = ["0000012345", "0000067890"]
            mock_get_cache.return_value = mock_cache

            result = get_all_ciks_with_10k()

            assert isinstance(result, set)
            assert "0000012345" in result
            assert "0000067890" in result

    def test_returns_empty_set_when_datamule_unavailable(self):
        """Test that empty set is returned when datamule is not installed."""
        with patch("public_company_graph.sources.datamule_index.DATAMULE_INDEX_AVAILABLE", False):
            result = get_all_ciks_with_10k()
            assert result == set()

    def test_force_refresh_ignores_cache(self):
        """Test that force_refresh=True ignores cache."""
        with patch("public_company_graph.sources.datamule_index.get_cache") as mock_get_cache:
            with patch(
                "public_company_graph.sources.datamule_index.DATAMULE_INDEX_AVAILABLE", True
            ):
                with patch("public_company_graph.sources.datamule_index.Index") as mock_index_class:
                    mock_cache = MagicMock()
                    mock_cache.get.return_value = ["cached_cik"]
                    mock_get_cache.return_value = mock_cache

                    mock_index = MagicMock()
                    mock_index.search_submissions.return_value = [
                        {"_source": {"ciks": ["0000099999"]}}
                    ]
                    mock_index_class.return_value = mock_index

                    result = get_all_ciks_with_10k(force_refresh=True)

                    # Should have called the index, not used cache
                    mock_index.search_submissions.assert_called_once()
                    assert "0000099999" in result

    def test_extracts_ciks_from_search_results(self):
        """Test that CIKs are correctly extracted from search results."""
        with patch("public_company_graph.sources.datamule_index.get_cache") as mock_get_cache:
            with patch(
                "public_company_graph.sources.datamule_index.DATAMULE_INDEX_AVAILABLE", True
            ):
                with patch("public_company_graph.sources.datamule_index.Index") as mock_index_class:
                    mock_cache = MagicMock()
                    mock_cache.get.return_value = None  # No cache
                    mock_get_cache.return_value = mock_cache

                    mock_index = MagicMock()
                    mock_index.search_submissions.return_value = [
                        {"_source": {"ciks": ["1234567890"]}},
                        {"_source": {"ciks": ["0000012345", "9999999999"]}},
                        {"_source": {}},  # No ciks field
                    ]
                    mock_index_class.return_value = mock_index

                    result = get_all_ciks_with_10k(force_refresh=True)

                    # Should have all unique CIKs, zero-padded
                    assert "1234567890" in result
                    assert "0000012345" in result
                    assert "9999999999" in result
                    assert len(result) == 3
