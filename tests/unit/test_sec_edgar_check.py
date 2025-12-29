"""
Unit tests for domain_status_graph.sources.sec_edgar_check module.
"""

from unittest.mock import MagicMock, patch

import requests

from domain_status_graph.sources.sec_edgar_check import check_company_has_10k


class TestCheckCompanyHas10k:
    """Test check_company_has_10k function."""

    @patch("domain_status_graph.sources.sec_edgar_check.requests.Session")
    def test_company_with_10k(self, mock_session_class):
        """Test that company with 10-K returns True."""
        mock_session = MagicMock()
        mock_session_class.return_value = mock_session

        # Mock SEC EDGAR API response with 10-K filing
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "filings": {
                "recent": {
                    "form": ["10-K", "10-Q", "8-K"],
                    "filingDate": ["2024-03-15", "2024-05-10", "2024-06-01"],
                }
            }
        }
        mock_response.raise_for_status = MagicMock()
        mock_session.get.return_value = mock_response

        result = check_company_has_10k("0000320193")  # Apple CIK

        assert result is True
        mock_session.get.assert_called_once()

    @patch("domain_status_graph.sources.sec_edgar_check.requests.Session")
    def test_company_without_10k(self, mock_session_class):
        """Test that company without 10-K returns False."""
        mock_session = MagicMock()
        mock_session_class.return_value = mock_session

        # Mock SEC EDGAR API response without 10-K filing
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "filings": {
                "recent": {
                    "form": ["10-Q", "8-K", "N-CSR"],  # No 10-K
                    "filingDate": ["2024-05-10", "2024-06-01", "2024-07-01"],
                }
            }
        }
        mock_response.raise_for_status = MagicMock()
        mock_session.get.return_value = mock_response

        result = check_company_has_10k("0001234567")

        assert result is False

    @patch("domain_status_graph.sources.sec_edgar_check.requests.Session")
    def test_10k_outside_date_range(self, mock_session_class):
        """Test that 10-K outside date range returns False."""
        mock_session = MagicMock()
        mock_session_class.return_value = mock_session

        # Mock response with 10-K but old date
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "filings": {
                "recent": {
                    "form": ["10-K"],
                    "filingDate": ["2019-03-15"],  # Before 2020-01-01
                }
            }
        }
        mock_response.raise_for_status = MagicMock()
        mock_session.get.return_value = mock_response

        result = check_company_has_10k("0000320193")

        assert result is False

    @patch("domain_status_graph.sources.sec_edgar_check.requests.Session")
    def test_api_error_returns_true(self, mock_session_class):
        """Test that API errors return True (fail-safe - allow datamule to try)."""
        mock_session = MagicMock()
        mock_session_class.return_value = mock_session

        # Mock API error
        mock_session.get.side_effect = requests.exceptions.RequestException("API error")

        result = check_company_has_10k("0000320193")

        # Should return True as fail-safe (allow datamule to try)
        assert result is True

    @patch("domain_status_graph.sources.sec_edgar_check.requests.Session")
    def test_malformed_response_returns_true(self, mock_session_class):
        """Test that malformed response returns True (fail-safe)."""
        mock_session = MagicMock()
        mock_session_class.return_value = mock_session

        # Mock malformed response
        mock_response = MagicMock()
        mock_response.json.return_value = {}  # Missing expected fields
        mock_response.raise_for_status = MagicMock()
        mock_session.get.return_value = mock_response

        result = check_company_has_10k("0000320193")

        # Should return True as fail-safe
        assert result is True
