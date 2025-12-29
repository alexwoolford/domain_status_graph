"""
SEC EDGAR pre-check utilities.

Provides functions to check if a company has 10-K filings available
before making expensive API calls. Uses free SEC EDGAR API.
"""

import logging
import time

import requests

logger = logging.getLogger(__name__)


def check_company_has_10k(
    cik: str,
    session: requests.Session | None = None,
    filing_date_start: str = "2020-01-01",
    filing_date_end: str = "2025-01-01",
) -> bool:
    """
    Check if a company has 10-K filings available using free SEC EDGAR API.

    This is a pre-check to avoid making expensive datamule API calls
    for companies that don't have 10-Ks (ETFs, funds, foreign companies, etc.).

    Args:
        cik: Company CIK (10-digit, zero-padded)
        session: Optional requests session (for connection pooling)
        filing_date_start: Start date for filing search (YYYY-MM-DD)
        filing_date_end: End date for filing search (YYYY-MM-DD)

    Returns:
        True if company has 10-K filings in date range, False otherwise

    Example:
        has_10k = check_company_has_10k("0000320193")  # Apple
        if has_10k:
            # Safe to call datamule API
            portfolio.download_submissions(...)
    """
    if session is None:
        session = requests.Session()

    # Ensure CIK is 10-digit zero-padded
    cik_padded = cik.zfill(10)

    # SEC EDGAR Submissions API (free, no authentication required)
    # URL format: https://data.sec.gov/submissions/CIK##########.json
    url = f"https://data.sec.gov/submissions/CIK{cik_padded}.json"

    headers = {
        "User-Agent": "public_company_graph script (contact: alexwoolford@example.com)",
        "Accept": "application/json",
    }

    try:
        # SEC rate limits: 10 requests per second
        # Add small delay to be respectful
        time.sleep(0.1)

        response = session.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        data = response.json()

        # Check filings array
        filings = data.get("filings", {})
        if not filings:
            # No filings data - fail-safe: return True to allow datamule to try
            return True

        recent = filings.get("recent", {})
        if not recent:
            # No recent filings data - fail-safe: return True
            return True

        forms = recent.get("form", [])
        if not forms:
            # No forms - company likely has no filings
            return False

        # Check if any 10-K filings exist in date range
        filing_dates = recent.get("filingDate", [])

        for i, form_type in enumerate(forms):
            if form_type == "10-K":
                # Check if filing date is in range
                if i < len(filing_dates):
                    filing_date = filing_dates[i]
                    if filing_date_start <= filing_date <= filing_date_end:
                        return True

        return False

    except requests.exceptions.RequestException as e:
        # If API call fails, log but don't fail - allow datamule to try
        logger.debug(f"SEC EDGAR pre-check failed for CIK {cik_padded}: {e}")
        # Return True to allow datamule to try (fail-safe)
        return True
    except (KeyError, IndexError, ValueError) as e:
        # Malformed response - log and allow datamule to try
        logger.debug(f"SEC EDGAR pre-check parse error for CIK {cik_padded}: {e}")
        return True
