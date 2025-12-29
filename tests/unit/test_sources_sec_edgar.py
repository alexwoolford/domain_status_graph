"""
Unit tests for SEC EDGAR domain source.
"""

from unittest.mock import MagicMock, patch

from public_company_graph.sources.sec_edgar import get_domain_from_sec


class TestGetDomainFromSec:
    """Tests for get_domain_from_sec function."""

    @patch("public_company_graph.sources.sec_edgar._rate_limiter")
    def test_successful_extraction_from_website_field(self, mock_rate_limiter):
        """Test successful domain extraction from website field."""
        mock_session = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"website": "https://www.apple.com"}
        mock_session.get.return_value = mock_response

        result = get_domain_from_sec(mock_session, "0000320193", "AAPL", "Apple Inc.")

        assert result.domain == "apple.com"
        assert result.source == "sec_edgar"
        assert result.confidence == 0.85
        # Metadata should contain the field information
        assert "field" in result.metadata, (
            f"Metadata missing 'field' key. Metadata: {result.metadata}"
        )
        assert result.metadata["field"] == "website"
        mock_rate_limiter.assert_called_once()

    @patch("public_company_graph.sources.sec_edgar._rate_limiter")
    def test_successful_extraction_from_investor_website(self, mock_rate_limiter):
        """Test successful domain extraction from investorWebsite field."""
        mock_session = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"investorWebsite": "https://investor.apple.com"}
        mock_session.get.return_value = mock_response

        result = get_domain_from_sec(mock_session, "0000320193", "AAPL", "Apple Inc.")

        assert result.domain == "apple.com"  # investor. prefix removed
        assert result.source == "sec_edgar"
        assert result.confidence == 0.75
        assert result.metadata["field"] == "investorWebsite"

    @patch("public_company_graph.sources.sec_edgar._rate_limiter")
    def test_investor_website_without_prefix(self, mock_rate_limiter):
        """Test investor website that doesn't have investor. prefix."""
        mock_session = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"investorWebsite": "https://www.apple.com/investor"}
        mock_session.get.return_value = mock_response

        result = get_domain_from_sec(mock_session, "0000320193", "AAPL", "Apple Inc.")

        assert result.domain == "apple.com"
        assert result.confidence == 0.75

    @patch("public_company_graph.sources.sec_edgar._rate_limiter")
    def test_website_field_preferred_over_investor(self, mock_rate_limiter):
        """Test that website field is preferred over investorWebsite."""
        mock_session = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "website": "https://www.apple.com",
            "investorWebsite": "https://investor.apple.com",
        }
        mock_session.get.return_value = mock_response

        result = get_domain_from_sec(mock_session, "0000320193", "AAPL", "Apple Inc.")

        assert result.domain == "apple.com"
        assert result.confidence == 0.85  # Higher confidence for website field
        assert result.metadata["field"] == "website"

    @patch("public_company_graph.sources.sec_edgar._rate_limiter")
    def test_no_website_fields(self, mock_rate_limiter):
        """Test when SEC submission has no website fields."""
        mock_session = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {}
        mock_session.get.return_value = mock_response

        result = get_domain_from_sec(mock_session, "0000320193", "AAPL", "Apple Inc.")

        assert result.domain is None
        assert result.confidence == 0.0

    @patch("public_company_graph.sources.sec_edgar._rate_limiter")
    def test_infrastructure_domain_filtered(self, mock_rate_limiter):
        """Test that infrastructure domains are filtered out."""
        mock_session = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"website": "https://www.sec.gov"}
        mock_session.get.return_value = mock_response

        result = get_domain_from_sec(mock_session, "0000320193", "AAPL", "Apple Inc.")

        assert result.domain is None
        assert result.confidence == 0.0

    @patch("public_company_graph.sources.sec_edgar._rate_limiter")
    def test_http_error(self, mock_rate_limiter):
        """Test handling of HTTP errors."""
        mock_session = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_session.get.return_value = mock_response

        result = get_domain_from_sec(mock_session, "0000320193", "AAPL", "Apple Inc.")

        assert result.domain is None
        assert result.confidence == 0.0

    @patch("public_company_graph.sources.sec_edgar._rate_limiter")
    def test_exception_handling(self, mock_rate_limiter):
        """Test that exceptions are handled gracefully."""
        mock_session = MagicMock()
        mock_session.get.side_effect = Exception("Network error")

        result = get_domain_from_sec(mock_session, "0000320193", "AAPL", "Apple Inc.")

        assert result.domain is None
        assert result.confidence == 0.0

    @patch("public_company_graph.sources.sec_edgar._rate_limiter")
    def test_cik_zero_padding(self, mock_rate_limiter):
        """Test that CIK is zero-padded to 10 digits."""
        mock_session = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"website": "https://www.apple.com"}
        mock_session.get.return_value = mock_response

        get_domain_from_sec(mock_session, "320193", "AAPL", "Apple Inc.")

        # Verify the URL was called with zero-padded CIK
        call_args = mock_session.get.call_args
        assert "CIK0000320193" in call_args[0][0] or "CIK0000320193" in str(call_args)
