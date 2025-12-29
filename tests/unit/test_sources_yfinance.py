"""
Unit tests for yfinance domain source.
"""

from unittest.mock import MagicMock, patch

from domain_status_graph.sources.yfinance import get_domain_from_yfinance


class TestGetDomainFromYfinance:
    """Tests for get_domain_from_yfinance function."""

    @patch("domain_status_graph.sources.yfinance.yf")
    @patch("domain_status_graph.sources.yfinance._rate_limiter")
    def test_successful_extraction(self, mock_rate_limiter, mock_yf):
        """Test successful domain extraction from yfinance."""
        # Mock yfinance response
        mock_ticker = MagicMock()
        mock_ticker.info = {
            "website": "https://www.apple.com",
            "longBusinessSummary": "Apple Inc. designs and manufactures consumer electronics.",
        }
        mock_yf.Ticker.return_value = mock_ticker

        result = get_domain_from_yfinance("AAPL", "Apple Inc.")

        assert result.domain == "apple.com"
        assert result.source == "yfinance"
        assert result.confidence == 0.9
        assert result.description == "Apple Inc. designs and manufactures consumer electronics."
        assert "raw_website" in result.metadata
        mock_rate_limiter.assert_called_once()

    @patch("domain_status_graph.sources.yfinance.yf")
    @patch("domain_status_graph.sources.yfinance._rate_limiter")
    def test_no_website(self, mock_rate_limiter, mock_yf):
        """Test when yfinance returns no website."""
        mock_ticker = MagicMock()
        mock_ticker.info = {"longBusinessSummary": "Some description"}
        mock_yf.Ticker.return_value = mock_ticker

        result = get_domain_from_yfinance("AAPL", "Apple Inc.")

        assert result.domain is None
        assert result.source == "yfinance"
        assert result.confidence == 0.0

    @patch("domain_status_graph.sources.yfinance.yf")
    @patch("domain_status_graph.sources.yfinance._rate_limiter")
    def test_infrastructure_domain_filtered(self, mock_rate_limiter, mock_yf):
        """Test that infrastructure domains are filtered out."""
        mock_ticker = MagicMock()
        mock_ticker.info = {"website": "https://www.sec.gov"}
        mock_yf.Ticker.return_value = mock_ticker

        result = get_domain_from_yfinance("AAPL", "Apple Inc.")

        assert result.domain is None
        assert result.confidence == 0.0

    @patch("domain_status_graph.sources.yfinance.yf")
    @patch("domain_status_graph.sources.yfinance._rate_limiter")
    def test_description_fallback(self, mock_rate_limiter, mock_yf):
        """Test that description falls back to 'description' if 'longBusinessSummary' not available."""
        mock_ticker = MagicMock()
        mock_ticker.info = {
            "website": "https://www.apple.com",
            "description": "Fallback description",
        }
        mock_yf.Ticker.return_value = mock_ticker

        result = get_domain_from_yfinance("AAPL", "Apple Inc.")

        assert result.domain == "apple.com"
        assert result.description == "Fallback description"

    @patch("domain_status_graph.sources.yfinance.yf")
    @patch("domain_status_graph.sources.yfinance._rate_limiter")
    def test_long_description_preserved(self, mock_rate_limiter, mock_yf):
        """Test that long descriptions are preserved in full (no truncation).

        Truncation was removed because:
        1. It silently loses data
        2. Downstream code (embeddings) handles long text via chunking
        3. Neo4j can store large text properties
        """
        long_description = "A" * 3000
        mock_ticker = MagicMock()
        mock_ticker.info = {
            "website": "https://www.apple.com",
            "longBusinessSummary": long_description,
        }
        mock_yf.Ticker.return_value = mock_ticker

        result = get_domain_from_yfinance("AAPL", "Apple Inc.")

        assert result.domain == "apple.com"
        # Full description should be preserved - no truncation!
        assert len(result.description) == 3000
        assert not result.description.endswith("...")

    @patch("domain_status_graph.sources.yfinance.yf")
    @patch("domain_status_graph.sources.yfinance._rate_limiter")
    def test_exception_handling(self, mock_rate_limiter, mock_yf):
        """Test that exceptions are handled gracefully."""
        mock_yf.Ticker.side_effect = Exception("API error")

        result = get_domain_from_yfinance("AAPL", "Apple Inc.")

        assert result.domain is None
        assert result.confidence == 0.0

    @patch("domain_status_graph.sources.yfinance.YFINANCE_AVAILABLE", False)
    def test_yfinance_not_available(self):
        """Test when yfinance is not installed."""
        result = get_domain_from_yfinance("AAPL", "Apple Inc.")

        assert result.domain is None
        assert result.confidence == 0.0
