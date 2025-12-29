"""
Unit tests for Finnhub domain source.
"""

from unittest.mock import MagicMock, patch

from public_company_graph.sources.finnhub import get_domain_from_finnhub


class TestGetDomainFromFinnhub:
    """Tests for get_domain_from_finnhub function."""

    @patch("public_company_graph.sources.finnhub.get_finnhub_api_key", return_value="test_key")
    @patch("public_company_graph.sources.finnhub._rate_limiter")
    @patch("public_company_graph.sources.finnhub.requests.get")
    def test_successful_extraction(self, mock_get, mock_rate_limiter, mock_api_key):
        """Test successful domain extraction from Finnhub."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "weburl": "https://www.apple.com",
            "description": "Apple Inc. designs consumer electronics.",
        }
        mock_get.return_value = mock_response

        result = get_domain_from_finnhub("AAPL")

        assert result.domain == "apple.com"
        assert result.source == "finnhub"
        assert result.confidence == 0.6
        assert result.description == "Apple Inc. designs consumer electronics."
        mock_rate_limiter.assert_called_once()
        mock_get.assert_called_once()

    @patch("public_company_graph.sources.finnhub.get_finnhub_api_key", return_value=None)
    @patch("public_company_graph.sources.finnhub._rate_limiter")
    @patch("public_company_graph.sources.finnhub.requests.get")
    def test_no_api_key(self, mock_get, mock_rate_limiter, mock_api_key):
        """Test when API key is not set."""
        result = get_domain_from_finnhub("AAPL")

        assert result.domain is None
        assert result.confidence == 0.0
        mock_get.assert_not_called()

    @patch("public_company_graph.sources.finnhub.get_finnhub_api_key", return_value="test_key")
    @patch("public_company_graph.sources.finnhub._rate_limiter")
    @patch("public_company_graph.sources.finnhub.requests.get")
    def test_no_weburl(self, mock_get, mock_rate_limiter, mock_api_key):
        """Test when Finnhub returns no weburl."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"description": "Some description"}
        mock_get.return_value = mock_response

        result = get_domain_from_finnhub("AAPL")

        assert result.domain is None
        assert result.confidence == 0.0

    @patch("public_company_graph.sources.finnhub.get_finnhub_api_key", return_value="test_key")
    @patch("public_company_graph.sources.finnhub._rate_limiter")
    @patch("public_company_graph.sources.finnhub.requests.get")
    def test_description_fallback_to_industry(self, mock_get, mock_rate_limiter, mock_api_key):
        """Test that description falls back to finnhubIndustry."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "weburl": "https://www.apple.com",
            "finnhubIndustry": "Technology",
        }
        mock_get.return_value = mock_response

        result = get_domain_from_finnhub("AAPL")

        assert result.domain == "apple.com"
        assert result.description == "Technology"

    @patch("public_company_graph.sources.finnhub.get_finnhub_api_key", return_value="test_key")
    @patch("public_company_graph.sources.finnhub._rate_limiter")
    @patch("public_company_graph.sources.finnhub.requests.get")
    def test_long_description_preserved(self, mock_get, mock_rate_limiter, mock_api_key):
        """Test that long descriptions are preserved in full (no truncation).

        Truncation was removed because:
        1. It silently loses data
        2. Downstream code (embeddings) handles long text via chunking
        3. Neo4j can store large text properties
        """
        long_description = "A" * 3000
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "weburl": "https://www.apple.com",
            "description": long_description,
        }
        mock_get.return_value = mock_response

        result = get_domain_from_finnhub("AAPL")

        assert result.domain == "apple.com"
        # Full description should be preserved - no truncation!
        assert len(result.description) == 3000
        assert not result.description.endswith("...")

    @patch("public_company_graph.sources.finnhub.get_finnhub_api_key", return_value="test_key")
    @patch("public_company_graph.sources.finnhub._rate_limiter")
    @patch("public_company_graph.sources.finnhub.requests.get")
    def test_infrastructure_domain_filtered(self, mock_get, mock_rate_limiter, mock_api_key):
        """Test that infrastructure domains are filtered out."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"weburl": "https://www.sec.gov"}
        mock_get.return_value = mock_response

        result = get_domain_from_finnhub("AAPL")

        assert result.domain is None
        assert result.confidence == 0.0

    @patch("public_company_graph.sources.finnhub.get_finnhub_api_key", return_value="test_key")
    @patch("public_company_graph.sources.finnhub._rate_limiter")
    @patch("public_company_graph.sources.finnhub.requests.get")
    def test_http_error(self, mock_get, mock_rate_limiter, mock_api_key):
        """Test handling of HTTP errors."""
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_get.return_value = mock_response

        result = get_domain_from_finnhub("AAPL")

        assert result.domain is None
        assert result.confidence == 0.0

    @patch("public_company_graph.sources.finnhub.get_finnhub_api_key", return_value="test_key")
    @patch("public_company_graph.sources.finnhub._rate_limiter")
    @patch("public_company_graph.sources.finnhub.requests.get")
    def test_exception_handling(self, mock_get, mock_rate_limiter, mock_api_key):
        """Test that exceptions are handled gracefully."""
        mock_get.side_effect = Exception("Network error")

        result = get_domain_from_finnhub("AAPL")

        assert result.domain is None
        assert result.confidence == 0.0
