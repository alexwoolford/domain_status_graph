"""
Finnhub domain source.

Incomplete coverage but can augment other sources. Weight: 1.0
"""

import logging

import requests

from domain_status_graph.config import get_finnhub_api_key
from domain_status_graph.constants import FINNHUB_RATE_LIMIT
from domain_status_graph.domain.models import DomainResult
from domain_status_graph.domain.validation import (
    is_infrastructure_domain,
    normalize_domain,
)
from domain_status_graph.utils.rate_limiting import get_rate_limiter

logger = logging.getLogger(__name__)

# Rate limiter for Finnhub
_rate_limiter = get_rate_limiter("finnhub", FINNHUB_RATE_LIMIT)


def get_domain_from_finnhub(ticker: str) -> DomainResult:
    """
    Get domain and description from Finnhub (low confidence, incomplete coverage).

    Args:
        ticker: Stock ticker symbol

    Returns:
        DomainResult with domain, source, confidence, and description
    """
    api_key = get_finnhub_api_key()
    if not api_key:
        return DomainResult(None, "finnhub", 0.0)

    if _rate_limiter is not None:
        _rate_limiter()

    try:
        url = "https://finnhub.io/api/v1/stock/profile2"
        params = {"symbol": ticker, "token": api_key}
        response = requests.get(url, params=params, timeout=5)

        if response.status_code == 200:
            data = response.json()
            weburl = data.get("weburl")

            # Extract description if available (Finnhub may have finnhubIndustry or description)
            description = data.get("description") or data.get("finnhubIndustry")
            if description:
                # Clean up description: remove extra whitespace only
                description = " ".join(str(description).split())
                # NOTE: We intentionally keep the FULL description. Downstream code
                # (embeddings, Neo4j) handles long text via chunking. Don't truncate!

            if weburl:
                domain = normalize_domain(weburl)
                if domain and not is_infrastructure_domain(domain):
                    return DomainResult(domain, "finnhub", 0.6, description=description)
    except Exception as e:
        logger.debug(f"Finnhub error for {ticker}: {e}")

    return DomainResult(None, "finnhub", 0.0)
