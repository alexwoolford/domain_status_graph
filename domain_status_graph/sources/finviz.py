"""
Finviz domain source.

Fast source with good coverage. Weight: 2.0
"""

import logging
import re

import requests

from domain_status_graph.constants import FINVIZ_RATE_LIMIT
from domain_status_graph.domain.models import DomainResult
from domain_status_graph.domain.validation import (
    is_infrastructure_domain,
    normalize_domain,
)
from domain_status_graph.utils.rate_limiting import get_rate_limiter

logger = logging.getLogger(__name__)

# Rate limiter for finviz
_rate_limiter = get_rate_limiter("finviz", FINVIZ_RATE_LIMIT)


def get_domain_from_finviz(session: requests.Session, ticker: str) -> DomainResult:
    """
    Get domain from Finviz (medium confidence source).

    Args:
        session: HTTP session for requests
        ticker: Stock ticker symbol

    Returns:
        DomainResult with domain, source, and confidence
    """
    if _rate_limiter is not None:
        _rate_limiter()

    try:
        url = f"https://finviz.com/quote.ashx?t={ticker}"
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            )
        }
        response = session.get(url, headers=headers, timeout=5)

        if response.status_code == 200:
            # Finviz has website in a table:
            # <td>Website</td><td><a href="https://www.company.com">Website</a></td>
            # More specific pattern to avoid catching Yahoo Finance links
            # Look for the Website label followed by a link that's NOT yahoo.com
            website_pattern = (
                r'Website["\']?\s*</td>\s*<td[^>]*>\s*<a[^>]*href=["\']'
                r"(https?://(?:www\.)?([a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?"
                r"(\.[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?)+))"
            )
            match = re.search(website_pattern, response.text, re.IGNORECASE)
            if match:
                domain = normalize_domain(match.group(1))
                # Filter out infrastructure and known bad domains
                if (
                    domain
                    and not is_infrastructure_domain(domain)
                    and "finviz.com" not in domain
                    and "yahoo.com" not in domain
                    and "google.com" not in domain
                ):
                    return DomainResult(domain, "finviz", 0.7)
    except Exception as e:
        logger.debug(f"Finviz error for {ticker}: {e}")

    return DomainResult(None, "finviz", 0.0)
