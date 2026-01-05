"""
Unit tests for SEC EDGAR company data sources.

These tests verify that SEC company data fetching works correctly
and handles errors gracefully.
"""

from unittest.mock import Mock, patch

import pytest
import requests

from public_company_graph.sources.sec_companies import (
    get_all_companies_from_neo4j,
    get_all_companies_from_sec,
)


class TestGetAllCompaniesFromSec:
    """Tests for fetching companies from SEC EDGAR."""

    @patch("public_company_graph.sources.sec_companies.requests.Session")
    def test_fetches_companies_successfully(self, mock_session_class):
        """Test successful fetch from SEC EDGAR."""
        # Mock response
        mock_response = Mock()
        mock_response.json.return_value = {
            "0": {"cik_str": 320193, "ticker": "AAPL", "title": "Apple Inc."},
            "1": {"cik_str": 789019, "ticker": "MSFT", "title": "Microsoft Corporation"},
            "2": {"cik_str": 1018724, "ticker": "AMZN", "title": "Amazon.com Inc."},
        }
        mock_response.raise_for_status = Mock()

        mock_session = Mock()
        mock_session.get.return_value = mock_response
        mock_session_class.return_value = mock_session

        companies = get_all_companies_from_sec()

        # Verify request was made correctly
        mock_session.get.assert_called_once()
        call_args = mock_session.get.call_args
        assert call_args[0][0] == "https://www.sec.gov/files/company_tickers.json"
        assert "User-Agent" in call_args[1]["headers"]
        assert call_args[1]["timeout"] == 30

        # Verify results
        assert len(companies) == 3
        assert companies[0]["cik"] == "0000320193"  # Zero-padded
        assert companies[0]["ticker"] == "AAPL"
        assert companies[0]["name"] == "Apple Inc."

        assert companies[1]["cik"] == "0000789019"
        assert companies[1]["ticker"] == "MSFT"
        assert companies[1]["name"] == "Microsoft Corporation"

    @patch("public_company_graph.sources.sec_companies.requests.Session")
    def test_handles_http_error(self, mock_session_class):
        """Test that HTTP errors are raised (not swallowed)."""
        mock_session = Mock()
        mock_response = Mock()
        mock_response.raise_for_status.side_effect = requests.HTTPError("404 Not Found")
        mock_session.get.return_value = mock_response
        mock_session_class.return_value = mock_session

        with pytest.raises(requests.HTTPError):
            get_all_companies_from_sec()

    @patch("public_company_graph.sources.sec_companies.requests.Session")
    def test_handles_duplicate_ciks(self, mock_session_class):
        """Test that duplicate CIKs are handled correctly (keep first)."""
        mock_response = Mock()
        # Same CIK with different tickers (should keep first)
        mock_response.json.return_value = {
            "0": {"cik_str": 320193, "ticker": "AAPL", "title": "Apple Inc."},
            "1": {"cik_str": 320193, "ticker": "AAPL2", "title": "Apple Inc. (duplicate)"},
        }
        mock_response.raise_for_status = Mock()

        mock_session = Mock()
        mock_session.get.return_value = mock_response
        mock_session_class.return_value = mock_session

        companies = get_all_companies_from_sec()

        # Should only have one company (duplicate CIK removed)
        assert len(companies) == 1
        assert companies[0]["cik"] == "0000320193"
        assert companies[0]["ticker"] == "AAPL"  # First one kept

    @patch("public_company_graph.sources.sec_companies.requests.Session")
    def test_zero_pads_cik(self, mock_session_class):
        """Test that CIKs are zero-padded to 10 digits."""
        mock_response = Mock()
        mock_response.json.return_value = {
            "0": {"cik_str": 1, "ticker": "TEST", "title": "Test Company"},
            "1": {"cik_str": 12345, "ticker": "TEST2", "title": "Test Company 2"},
        }
        mock_response.raise_for_status = Mock()

        mock_session = Mock()
        mock_session.get.return_value = mock_response
        mock_session_class.return_value = mock_session

        companies = get_all_companies_from_sec()

        assert companies[0]["cik"] == "0000000001"  # Zero-padded to 10
        assert companies[1]["cik"] == "0000012345"  # Zero-padded to 10

    @patch("public_company_graph.sources.sec_companies.requests.Session")
    def test_handles_missing_fields(self, mock_session_class):
        """Test that missing fields are handled gracefully."""
        mock_response = Mock()
        mock_response.json.return_value = {
            "0": {"cik_str": 320193, "ticker": "AAPL", "title": "Apple Inc."},
            "1": {"cik_str": 789019},  # Missing ticker and title
            "2": {"cik_str": None, "ticker": "INVALID"},  # Missing CIK
        }
        mock_response.raise_for_status = Mock()

        mock_session = Mock()
        mock_session.get.return_value = mock_response
        mock_session_class.return_value = mock_session

        companies = get_all_companies_from_sec()

        # All entries are included (code doesn't filter None CIKs, just converts to string)
        assert len(companies) == 3
        assert companies[0]["cik"] == "0000320193"
        assert companies[0]["ticker"] == "AAPL"
        assert companies[1]["cik"] == "0000789019"
        assert companies[1]["ticker"] == ""  # Missing ticker becomes empty string
        # Note: None CIK gets converted to string "000000None" - this is current behavior


class TestGetAllCompaniesFromNeo4j:
    """Tests for fetching companies from Neo4j."""

    def test_fetches_companies_from_neo4j(self):
        """Test fetching companies from Neo4j."""
        from contextlib import contextmanager

        mock_driver = Mock()
        mock_session = Mock()

        # Properly mock context manager
        @contextmanager
        def mock_session_context(*args, **kwargs):
            yield mock_session

        mock_driver.session = Mock(return_value=mock_session_context())

        # Mock Neo4j result - use dict-like objects
        class MockRecord:
            def __init__(self, data):
                self._data = data

            def __getitem__(self, key):
                return self._data[key]

        mock_record1 = MockRecord({"cik": "0000320193", "ticker": "AAPL", "name": "Apple Inc."})
        mock_record2 = MockRecord(
            {"cik": "0000789019", "ticker": "MSFT", "name": "Microsoft Corporation"}
        )

        # Make result directly iterable
        mock_result = [mock_record1, mock_record2]
        mock_session.run.return_value = mock_result

        companies = get_all_companies_from_neo4j(mock_driver, database="test")

        # Verify query was executed
        mock_session.run.assert_called_once()
        query = mock_session.run.call_args[0][0]
        assert "MATCH (c:Company)" in query
        assert "c.cik IS NOT NULL" in query

        # Verify results
        assert len(companies) == 2
        assert companies[0]["cik"] == "0000320193"
        assert companies[0]["ticker"] == "AAPL"
        assert companies[0]["name"] == "Apple Inc."

    def test_handles_missing_ticker(self):
        """Test that missing ticker is handled (becomes empty string)."""
        from contextlib import contextmanager

        mock_driver = Mock()
        mock_session = Mock()

        @contextmanager
        def mock_session_context(*args, **kwargs):
            yield mock_session

        mock_driver.session = Mock(return_value=mock_session_context())

        class MockRecord:
            def __init__(self, data):
                self._data = data

            def __getitem__(self, key):
                return self._data[key]

        mock_record = MockRecord({"cik": "0000320193", "ticker": None, "name": "Apple Inc."})

        # Make result directly iterable
        mock_result = [mock_record]
        mock_session.run.return_value = mock_result

        companies = get_all_companies_from_neo4j(mock_driver)

        assert len(companies) == 1
        assert companies[0]["ticker"] == ""  # None becomes empty string

    def test_zero_pads_cik_from_neo4j(self):
        """Test that CIKs from Neo4j are zero-padded."""
        from contextlib import contextmanager

        mock_driver = Mock()
        mock_session = Mock()

        @contextmanager
        def mock_session_context(*args, **kwargs):
            yield mock_session

        mock_driver.session = Mock(return_value=mock_session_context())

        class MockRecord:
            def __init__(self, data):
                self._data = data

            def __getitem__(self, key):
                return self._data[key]

        mock_record = MockRecord({"cik": "320193", "ticker": "AAPL", "name": "Apple Inc."})

        # Make result directly iterable
        mock_result = [mock_record]
        mock_session.run.return_value = mock_result

        companies = get_all_companies_from_neo4j(mock_driver)

        assert companies[0]["cik"] == "0000320193"  # Zero-padded to 10
