"""
Company property enrichment from public data sources.

This module provides functions to fetch and enrich Company node properties
from SEC EDGAR, Yahoo Finance, and Wikidata.

Data Sources:
- SEC EDGAR API: https://www.sec.gov/edgar/sec-api-documentation
  License: Public domain (17 CFR 240.12g-1)
- Yahoo Finance (via yfinance): https://github.com/ranaroussi/yfinance
  License: Free, no explicit license restrictions
- Wikidata: https://www.wikidata.org/
  License: CC0 (public domain)

Reference: CompanyKG paper - Company node attributes (employees, sector, etc.)
"""

import contextlib
import io
import logging
import sys
import time

import requests

from public_company_graph.utils.rate_limiting import get_rate_limiter

logger = logging.getLogger(__name__)

# Rate limiting for SEC EDGAR API (10 requests per second)
_sec_rate_limiter = get_rate_limiter("sec_edgar_enrichment", requests_per_second=10.0)


def _rate_limit_sec():
    """Enforce SEC EDGAR API rate limiting (10 req/sec)."""
    _sec_rate_limiter()


def fetch_sec_company_info(cik: str, session: requests.Session | None = None) -> dict | None:
    """
    Fetch company information from SEC EDGAR API.

    Args:
        cik: SEC Central Index Key (10-digit string, zero-padded)
        session: Optional requests.Session for connection pooling

    Returns:
        Dictionary with company info (SIC, NAICS, etc.) or None if not found

    Data Source: SEC EDGAR API (public domain)
    Reference: https://www.sec.gov/edgar/sec-api-documentation
    """
    _rate_limit_sec()

    try:
        if session is None:
            session = requests.Session()

        # SEC requires User-Agent header
        url = f"https://data.sec.gov/submissions/CIK{cik.zfill(10)}.json"
        headers = {
            "User-Agent": "public_company_graph enrichment script (contact: alex@woolford.io)",
            "Accept": "application/json",
        }

        response = session.get(url, headers=headers, timeout=10)
        response.raise_for_status()

        data = response.json()
        company_info = data.get("name", "")

        # Extract SIC and NAICS from the submissions data
        sic_code = None
        naics_code = None

        # SIC code: SEC API returns as string (e.g., "3571") or array
        sic_value = data.get("sic")
        if sic_value:
            if isinstance(sic_value, str):
                # Format: "3571" (just the code)
                sic_code = sic_value.strip()
            elif isinstance(sic_value, list) and len(sic_value) > 0:
                # Format: ["3571", "Description"] or ["3571 - Description"]
                sic_entry = str(sic_value[0])
                sic_code = sic_entry.split("-")[0].split()[0].strip()

        # NAICS code: SEC API may return as string or array
        naics_value = data.get("naics")
        if naics_value:
            if isinstance(naics_value, str):
                # Format: "511210" (just the code)
                naics_code = naics_value.strip()
            elif isinstance(naics_value, list) and len(naics_value) > 0:
                # Format: ["511210", "Description"] or ["511210 - Description"]
                naics_entry = str(naics_value[0])
                naics_code = naics_entry.split("-")[0].split()[0].strip()

        result = {
            "sic_code": sic_code,
            "naics_code": naics_code,
            "company_name": company_info,
        }

        # Normalize codes
        normalized = normalize_industry_codes(sic_code, naics_code)
        result.update(normalized)

        return result

    except requests.exceptions.RequestException as e:
        logger.debug(f"SEC EDGAR API error for CIK {cik}: {e}")
        return None
    except Exception as e:
        logger.warning(f"Unexpected error fetching SEC data for CIK {cik}: {e}")
        return None


@contextlib.contextmanager
def _suppress_yfinance_errors():
    """
    Context manager to suppress yfinance's verbose HTTP error logging.

    yfinance logs raw HTTP errors at ERROR level (e.g., 404 JSON responses)
    which clutters our logs. This temporarily raises the yfinance logger's
    level to suppress these expected errors.

    Note: We also capture stderr for any direct printing yfinance might do.
    """
    # Suppress yfinance's ERROR level logging
    yfinance_logger = logging.getLogger("yfinance")
    old_level = yfinance_logger.level
    yfinance_logger.setLevel(logging.CRITICAL)  # Only show CRITICAL (none expected)

    # Also capture stderr in case of direct printing
    old_stderr = sys.stderr
    try:
        sys.stderr = io.StringIO()
        yield sys.stderr
    finally:
        # Restore original stderr and logger level
        sys.stderr = old_stderr
        yfinance_logger.setLevel(old_level)


def fetch_yahoo_finance_info(ticker: str) -> dict | None:
    """
    Fetch company information from Yahoo Finance.

    Args:
        ticker: Stock ticker symbol (e.g., 'AAPL')

    Returns:
        Dictionary with company info (sector, industry, market_cap, revenue, employees) or None

    Data Source: Yahoo Finance via yfinance library
    """
    try:
        import yfinance as yf

        # Suppress yfinance's internal HTTP error printing to stderr
        with _suppress_yfinance_errors() as captured_stderr:
            ticker_obj = yf.Ticker(ticker)
            info = ticker_obj.info

        # Check for captured errors (e.g., 404 responses)
        captured_output = captured_stderr.getvalue()
        if captured_output:
            # Check if it's a "not found" error
            if "Not Found" in captured_output or "404" in captured_output:
                logger.debug(f"Yahoo Finance: Symbol not found: {ticker}")
                return None
            # Log other captured errors at debug level
            logger.debug(f"Yahoo Finance stderr for {ticker}: {captured_output.strip()}")

        # Check if we got valid data (yfinance returns empty dict for invalid symbols)
        if not info or info.get("regularMarketPrice") is None:
            # Symbol exists but no price data - might be delisted or invalid
            logger.debug(f"Yahoo Finance: No data available for {ticker}")
            return None

        # Extract relevant fields
        result = {
            "sector": info.get("sector"),
            "industry": info.get("industry"),
            "market_cap": info.get("marketCap"),
            "revenue": info.get("totalRevenue"),
            "employees": info.get("fullTimeEmployees"),
            "headquarters_city": info.get("city"),
            "headquarters_state": info.get("state"),
            "headquarters_country": info.get("country", "US"),
            "founded_year": info.get("founded"),
        }

        # Filter out None values for cleaner output
        return {k: v for k, v in result.items() if v is not None}

    except ImportError:
        logger.warning("yfinance not available. Install with: pip install yfinance")
        return None
    except Exception as e:
        # Log with source context for easier debugging
        error_str = str(e)
        if "Not Found" in error_str or "404" in error_str:
            logger.debug(f"Yahoo Finance: Symbol not found: {ticker}")
        else:
            logger.warning(f"Yahoo Finance error for {ticker}: {e}")
        return None


def fetch_wikidata_info(ticker: str, company_name: str) -> dict | None:
    """
    Fetch company information from Wikidata using SPARQL.

    Args:
        ticker: Stock ticker symbol
        company_name: Company name for disambiguation

    Returns:
        Dictionary with company info (employees, HQ location, etc.) or None

    Data Source: Wikidata SPARQL endpoint (CC0/public domain)
    Reference: https://www.wikidata.org/wiki/Wikidata:Main_Page
    """
    # TODO: Implement Wikidata SPARQL queries
    # This is lower priority - Yahoo Finance and SEC provide most of what we need
    # Wikidata can supplement with employees and HQ location if missing
    logger.debug("Wikidata queries not yet implemented (lower priority)")
    return None


def normalize_industry_codes(sic: str | None, naics: str | None) -> dict:
    """
    Normalize and validate industry classification codes.

    Args:
        sic: Standard Industrial Classification code
        naics: North American Industry Classification System code

    Returns:
        Dictionary with normalized codes
    """
    result = {}
    if sic:
        # SIC codes are typically 4 digits, extract numeric part
        sic_clean = "".join(filter(str.isdigit, str(sic)))
        if sic_clean and len(sic_clean) >= 2:
            result["sic_code"] = sic_clean[:4].zfill(4)
    if naics:
        # NAICS codes are typically 6 digits, extract numeric part
        naics_clean = "".join(filter(str.isdigit, str(naics)))
        if naics_clean and len(naics_clean) >= 2:
            result["naics_code"] = naics_clean[:6].zfill(6)
    return result


def merge_company_data(
    sec_data: dict | None, yahoo_data: dict | None, wikidata_data: dict | None
) -> dict:
    """
    Merge data from multiple sources, with priority order.

    Priority: SEC > Yahoo Finance > Wikidata (for overlapping fields)

    Args:
        sec_data: Data from SEC EDGAR
        yahoo_data: Data from Yahoo Finance
        wikidata_data: Data from Wikidata

    Returns:
        Merged dictionary with all available data
    """
    result = {}

    # Start with Yahoo Finance (most complete for financials)
    if yahoo_data:
        result.update(yahoo_data)

    # Override with SEC data (more authoritative for SIC/NAICS)
    if sec_data:
        # SEC provides SIC/NAICS codes
        if sec_data.get("sic_code"):
            result["sic_code"] = sec_data["sic_code"]
        if sec_data.get("naics_code"):
            result["naics_code"] = sec_data["naics_code"]

    # Add Wikidata data (supplemental)
    if wikidata_data:
        # Only add fields not already present
        for key, value in wikidata_data.items():
            if key not in result or result[key] is None:
                result[key] = value

    # Add metadata
    sources = []
    if sec_data:
        sources.append("SEC_EDGAR")
    if yahoo_data:
        sources.append("YAHOO_FINANCE")
    if wikidata_data:
        sources.append("WIKIDATA")

    result["data_source"] = ",".join(sources) if sources else None
    result["data_updated_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    return result
