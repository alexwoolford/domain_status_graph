"""
Unit tests for Finviz domain source.
"""

from unittest.mock import MagicMock, patch

from public_company_graph.sources.finviz import get_domain_from_finviz


class TestGetDomainFromFinviz:
    """Tests for get_domain_from_finviz function."""

    @patch("public_company_graph.sources.finviz._rate_limiter")
    def test_successful_extraction(self, mock_rate_limiter):
        """Test successful domain extraction from Finviz."""
        mock_session = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = '<td>Website</td><td><a href="https://www.apple.com">Website</a></td>'
        mock_session.get.return_value = mock_response

        result = get_domain_from_finviz(mock_session, "AAPL")

        assert result.domain == "apple.com"
        assert result.source == "finviz"
        assert result.confidence == 0.7
        mock_rate_limiter.assert_called_once()

    @patch("public_company_graph.sources.finviz._rate_limiter")
    def test_no_website_found(self, mock_rate_limiter):
        """Test when Finviz page doesn't contain website."""
        mock_session = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = "<html><body>No website here</body></html>"
        mock_session.get.return_value = mock_response

        result = get_domain_from_finviz(mock_session, "AAPL")

        assert result.domain is None
        assert result.confidence == 0.0

    @patch("public_company_graph.sources.finviz._rate_limiter")
    def test_infrastructure_domain_filtered(self, mock_rate_limiter):
        """Test that infrastructure domains are filtered out."""
        mock_session = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = '<td>Website</td><td><a href="https://www.sec.gov">Website</a></td>'
        mock_session.get.return_value = mock_response

        result = get_domain_from_finviz(mock_session, "AAPL")

        assert result.domain is None
        assert result.confidence == 0.0

    @patch("public_company_graph.sources.finviz._rate_limiter")
    def test_finviz_domain_filtered(self, mock_rate_limiter):
        """Test that finviz.com domains are filtered out."""
        mock_session = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = (
            '<td>Website</td><td><a href="https://finviz.com/quote">Website</a></td>'
        )
        mock_session.get.return_value = mock_response

        result = get_domain_from_finviz(mock_session, "AAPL")

        assert result.domain is None

    @patch("public_company_graph.sources.finviz._rate_limiter")
    def test_http_error(self, mock_rate_limiter):
        """Test handling of HTTP errors."""
        mock_session = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_session.get.return_value = mock_response

        result = get_domain_from_finviz(mock_session, "AAPL")

        assert result.domain is None
        assert result.confidence == 0.0

    @patch("public_company_graph.sources.finviz._rate_limiter")
    def test_exception_handling(self, mock_rate_limiter):
        """Test that exceptions are handled gracefully."""
        mock_session = MagicMock()
        mock_session.get.side_effect = Exception("Network error")

        result = get_domain_from_finviz(mock_session, "AAPL")

        assert result.domain is None
        assert result.confidence == 0.0
