"""
SEC EDGAR domain source.

Authoritative but slower source. Weight: 2.5
"""

import logging

import requests

from public_company_graph.constants import SEC_EDGAR_RATE_LIMIT
from public_company_graph.domain.models import DomainResult
from public_company_graph.domain.validation import (
    is_infrastructure_domain,
    normalize_domain,
)
from public_company_graph.utils.rate_limiting import get_rate_limiter

logger = logging.getLogger(__name__)

# Rate limiter for SEC EDGAR
_rate_limiter = get_rate_limiter("sec_edgar", SEC_EDGAR_RATE_LIMIT)


def get_domain_from_sec(
    session: requests.Session, cik: str, ticker: str, company_name: str
) -> DomainResult:
    """
    Get domain from SEC EDGAR (authoritative but slower).

    Args:
        session: HTTP session for requests
        cik: Company CIK (10-digit, zero-padded)
        ticker: Stock ticker symbol (for logging)
        company_name: Company name (for logging)

    Returns:
        DomainResult with domain, source, and confidence
    """
    if _rate_limiter is not None:
        _rate_limiter()

    try:
        # Fetch SEC submission
        url = f"https://data.sec.gov/submissions/CIK{cik.zfill(10)}.json"
        headers = {
            "User-Agent": "public_company_graph script (contact: your-email@example.com)",
            "Accept": "application/json",
        }
        response = session.get(url, headers=headers, timeout=10)

        if response.status_code == 200:
            submission = response.json()

            # Check website field first (fastest, most reliable)
            website = submission.get("website")
            if website:
                domain = normalize_domain(website)
                if domain and not is_infrastructure_domain(domain):
                    return DomainResult(domain, "sec_edgar", 0.85, metadata={"field": "website"})

            # Fallback: check investor website (sometimes populated when website isn't)
            investor_website = submission.get("investorWebsite")
            if investor_website:
                domain = normalize_domain(investor_website)
                if domain and not is_infrastructure_domain(domain):
                    # Prefer main domain over investor relations subdomain
                    # e.g., "investor.apple.com" -> "apple.com"
                    if domain.startswith("investor."):
                        domain = domain.replace("investor.", "")
                    return DomainResult(
                        domain, "sec_edgar", 0.75, metadata={"field": "investorWebsite"}
                    )
    except Exception as e:
        logger.debug(f"SEC error for {ticker} (CIK {cik}): {e}")

    return DomainResult(None, "sec_edgar", 0.0)
