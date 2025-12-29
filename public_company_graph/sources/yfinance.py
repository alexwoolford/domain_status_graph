"""
yfinance domain source.

Fast, reliable source with good coverage. Weight: 3.0
"""

import contextlib
import io
import logging
import sys

from public_company_graph.constants import YFINANCE_RATE_LIMIT
from public_company_graph.domain.models import DomainResult
from public_company_graph.domain.validation import (
    is_infrastructure_domain,
    normalize_domain,
)
from public_company_graph.utils.rate_limiting import get_rate_limiter

logger = logging.getLogger(__name__)

# Try to import yfinance
try:
    import yfinance as yf

    YFINANCE_AVAILABLE = True
except ImportError:
    YFINANCE_AVAILABLE = False
    yf = None  # type: ignore

# Rate limiter for yfinance
_rate_limiter = get_rate_limiter(
    "yfinance", YFINANCE_RATE_LIMIT if YFINANCE_RATE_LIMIT > 0 else 1.0
)


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
        sys.stderr.getvalue()  # Drain buffer before restoring
        sys.stderr = old_stderr
        yfinance_logger.setLevel(old_level)


def get_domain_from_yfinance(ticker: str, company_name: str = "") -> DomainResult:
    """
    Get domain and description from yfinance (high confidence source).

    Args:
        ticker: Stock ticker symbol
        company_name: Company name (optional, for logging)

    Returns:
        DomainResult with domain, source, confidence, and description
    """
    if not YFINANCE_AVAILABLE:
        return DomainResult(None, "yfinance", 0.0)

    if _rate_limiter is not None:
        _rate_limiter()

    try:
        # Suppress yfinance's internal HTTP error printing to stderr
        with _suppress_yfinance_errors() as captured_stderr:
            stock = yf.Ticker(ticker)
            info = stock.info

        # Check for captured errors (e.g., 404 responses)
        captured_output = captured_stderr.getvalue()
        if captured_output:
            if "Not Found" in captured_output or "404" in captured_output:
                logger.debug(f"Yahoo Finance: Symbol not found: {ticker}")
                return DomainResult(None, "yfinance", 0.0)
            logger.debug(f"Yahoo Finance stderr for {ticker}: {captured_output.strip()}")

        website = info.get("website")

        # Extract description (prefer longBusinessSummary, fallback to description)
        description = info.get("longBusinessSummary") or info.get("description")
        if description:
            # Clean up description: remove extra whitespace only
            description = " ".join(description.split())
            # NOTE: We intentionally keep the FULL description. Downstream code
            # (embeddings, Neo4j) handles long text via chunking. Don't truncate!

        if website:
            domain = normalize_domain(website)
            if domain and not is_infrastructure_domain(domain):
                return DomainResult(
                    domain,
                    "yfinance",
                    0.9,
                    description=description,
                    metadata={"raw_website": website},
                )
    except Exception as e:
        error_str = str(e)
        if "Not Found" in error_str or "404" in error_str:
            logger.debug(f"Yahoo Finance: Symbol not found: {ticker}")
        else:
            logger.debug(f"Yahoo Finance error for {ticker}: {e}")

    return DomainResult(None, "yfinance", 0.0)
