"""
SEC EDGAR company data sources.

Provides functions to fetch company lists from SEC EDGAR or Neo4j.
"""

import logging

import requests

logger = logging.getLogger(__name__)


def get_all_companies_from_sec(
    session: requests.Session | None = None,
) -> list[dict[str, str]]:
    """
    Get all companies directly from SEC EDGAR company_tickers.json.

    This is the authoritative source - includes all public companies that file with SEC.

    Args:
        session: Optional requests session (for connection pooling)

    Returns:
        List of dicts with keys: cik, ticker, name
    """
    if session is None:
        session = requests.Session()

    url = "https://www.sec.gov/files/company_tickers.json"
    headers = {
        "User-Agent": "public_company_graph script (contact: alexwoolford@example.com)",
        "Accept": "application/json",
    }

    logger.info("Fetching company list from SEC EDGAR...")
    response = session.get(url, headers=headers, timeout=30)
    response.raise_for_status()
    data = response.json()

    # Extract unique companies (some CIKs may appear multiple times with different tickers)
    companies_dict = {}
    for entry in data.values():
        cik = str(entry.get("cik_str", "")).zfill(10)
        if cik:
            # Use CIK as key to ensure uniqueness
            # If same CIK appears multiple times, keep the first one (or could merge)
            if cik not in companies_dict:
                companies_dict[cik] = {
                    "cik": cik,
                    "ticker": entry.get("ticker", "").upper(),
                    "name": entry.get("title", "").strip(),
                }

    companies = list(companies_dict.values())
    logger.info(f"Found {len(companies):,} unique companies from SEC EDGAR")
    return companies


def get_all_companies_from_neo4j(
    driver, database: str | None = None, exchange: str | None = None
) -> list[dict[str, str]]:
    """
    Get all companies from Neo4j with CIK, ticker, and name.

    Args:
        driver: Neo4j driver
        database: Database name
        exchange: Optional exchange filter ('NASDAQ', 'NYSE', or None for all)

    Returns:
        List of dicts with keys: cik, ticker, name
    """
    # Build query with optional exchange filter
    # Note: Exchange info might be in ticker format or a separate property
    # For now, we'll filter by ticker patterns (NASDAQ typically 1-4 chars, NYSE typically 1-3 chars)
    # A better approach would be to add an 'exchange' property to Company nodes
    if exchange:
        # Simple heuristic: NASDAQ tickers are typically longer (1-5 chars), NYSE shorter (1-3 chars)
        # But this is imperfect - better to add exchange property to Company nodes
        # For now, we'll just return all and let user filter manually if needed
        logger.warning(
            "Exchange filtering not yet implemented. "
            "All companies will be processed. "
            "To filter by exchange, add 'exchange' property to Company nodes first."
        )

    with driver.session(database=database) as session:
        result = session.run(
            """
            MATCH (c:Company)
            WHERE c.cik IS NOT NULL AND c.cik <> ''
            RETURN DISTINCT
                c.cik AS cik,
                COALESCE(c.ticker, '') AS ticker,
                COALESCE(c.name, '') AS name
            ORDER BY c.cik
            """
        )
        companies = []
        for record in result:
            cik = str(record["cik"]).zfill(10)  # Ensure 10-digit zero-padded
            companies.append(
                {
                    "cik": cik,
                    "ticker": record["ticker"].upper() if record["ticker"] else "",
                    "name": record["name"] if record["name"] else "",
                }
            )

    logger.info(f"Found {len(companies):,} companies from Neo4j")
    return companies
