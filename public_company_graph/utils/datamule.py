"""
Utilities for working with datamule library.

This module provides:
1. Thread-safe Portfolio cache for expensive re-initialization
2. Parsed document cache for avoiding repeated doc.parse() calls
3. Helper to suppress datamule's verbose output (Portfolio init messages)

IMPORTANT: For multi-threaded scripts using datamule:
1. Set os.environ["TQDM_DISABLE"] = "1" BEFORE importing datamule
2. Use quiet=True in download_submissions() calls
3. Redirect stdout at FD level around parallel execution blocks
   (contextlib.redirect_stdout doesn't work across threads)
"""

import contextlib
import logging
import threading
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

# Thread-safe Portfolio cache (keyed by CIK)
_portfolio_cache: dict[str, Any] = {}
_portfolio_cache_lock = threading.RLock()

# Thread-safe parsed document cache (keyed by CIK)
# Caches the parsed 10-K document to avoid calling doc.parse() multiple times
_parsed_doc_cache: dict[str, Any] = {}
_parsed_doc_cache_lock = threading.RLock()


def get_cached_portfolio(cik: str, portfolio_path: Path) -> Any | None:
    """
    Get a cached Portfolio object for a CIK, or create and cache it if not exists.

    This avoids expensive Portfolio initialization when multiple parsers need
    the same Portfolio object (e.g., business_description and risk_factors
    parsers both need the same Portfolio for the same CIK).

    Thread-safe: Uses RLock to allow concurrent reads but exclusive writes.

    Args:
        cik: Company CIK identifier
        portfolio_path: Path to portfolio directory

    Returns:
        Portfolio object if available, None if datamule not available or error
    """
    import time

    try:
        from datamule import Portfolio
    except ImportError:
        return None

    # Fast path: Check cache first (read lock)
    with _portfolio_cache_lock:
        if cik in _portfolio_cache:
            return _portfolio_cache[cik]

    # Slow path: Create Portfolio (write lock)
    # Check if tar files exist first (cheap check)
    tar_files = list(portfolio_path.glob("*.tar")) if portfolio_path.exists() else []
    if not tar_files:
        return None  # No tar file, can't use datamule

    # Create Portfolio with output suppression
    start_time = time.perf_counter()
    with suppress_datamule_output():
        try:
            portfolio = Portfolio(str(portfolio_path))
            elapsed = time.perf_counter() - start_time

            # Log slow Portfolio creation (> 1 second)
            if elapsed > 1.0:
                logger.debug(f"Portfolio init for CIK {cik}: {elapsed:.2f}s")

            # Cache it (write lock)
            with _portfolio_cache_lock:
                _portfolio_cache[cik] = portfolio

            return portfolio
        except Exception as e:
            logger.debug(f"Failed to create Portfolio for CIK {cik}: {e}")
            return None


def get_cached_parsed_doc(cik: str, portfolio_path: Path) -> Any | None:
    """
    Get a cached PARSED 10-K document for a CIK.

    This avoids the expensive doc.parse() call when multiple parsers need
    to extract different sections from the same 10-K document.

    The parsing flow is:
    1. Portfolio init: ~2-3 seconds (cached per CIK)
    2. doc.parse(): ~1-2 seconds (cached per CIK)
    3. get_section(): ~0.1 seconds (fast, not cached)

    By caching the parsed document, we save ~1-2 seconds per parser after the first.

    Args:
        cik: Company CIK identifier
        portfolio_path: Path to portfolio directory

    Returns:
        Parsed 10-K document object, or None if not available
    """
    import time

    # Fast path: Check cache first
    with _parsed_doc_cache_lock:
        if cik in _parsed_doc_cache:
            return _parsed_doc_cache[cik]

    # Get Portfolio (may create and cache it)
    portfolio = get_cached_portfolio(cik, portfolio_path)
    if portfolio is None:
        return None

    # Find and parse the 10-K document
    start_time = time.perf_counter()
    with suppress_datamule_output():
        try:
            ten_ks = list(portfolio.document_type("10-K"))
            if not ten_ks:
                return None

            doc = ten_ks[0]
            doc.parse()  # This is expensive (~1-2 seconds)
            elapsed = time.perf_counter() - start_time

            # Log slow document parsing (> 1 second)
            if elapsed > 1.0:
                logger.debug(f"Document parse for CIK {cik}: {elapsed:.2f}s")

            # Cache the parsed document
            with _parsed_doc_cache_lock:
                _parsed_doc_cache[cik] = doc

            return doc
        except Exception as e:
            logger.debug(f"Failed to parse document for CIK {cik}: {e}")
            return None


def clear_portfolio_cache():
    """
    Clear the Portfolio and parsed document caches.

    Useful for testing or when you want to force re-initialization.
    """
    with _portfolio_cache_lock:
        _portfolio_cache.clear()
    with _parsed_doc_cache_lock:
        _parsed_doc_cache.clear()


def _is_tqdm_progress_bar(text: str) -> bool:
    """
    Check if a line of text is a tqdm progress bar.

    This is a safety filter in case TQDM_DISABLE doesn't catch everything.
    tqdm progress bars have characteristic patterns:
    - Contain percentage and bar: "100%|████|"
    - Contain rate/speed info: "[00:01<00:00, 192.97it/s]"
    - May contain ANSI escape sequences
    - Contain progress bar characters like █, ▉, ▊, etc.

    Args:
        text: Line of text to check

    Returns:
        True if the text appears to be a tqdm progress bar
    """
    if not text.strip():
        return False

    # Progress bar characters (Unicode block elements)
    progress_chars = ["█", "▉", "▊", "▋", "▌", "▍", "▎", "▏", "▐", "░", "▒", "▓"]
    if any(char in text for char in progress_chars):
        return True

    # Common tqdm patterns
    tqdm_patterns = [
        "%|" in text,  # Percentage and bar
        "it/s]" in text or "s/it]" in text,  # Rate information
        (text.count("|") > 0 and "/" in text and any(c.isdigit() for c in text)),  # "5/5" with bar
        "\x1b[" in text,  # ANSI escape sequences
    ]

    return any(tqdm_patterns)


@contextlib.contextmanager
def suppress_datamule_output():
    """
    Context manager to suppress datamule's verbose output (Portfolio init messages).

    IMPORTANT: For API output, use quiet=True in download_submissions().
    For tqdm progress bars, set TQDM_DISABLE=1 BEFORE importing datamule.

    Example:
        import os
        os.environ["TQDM_DISABLE"] = "1"  # BEFORE datamule import!
        from datamule import Portfolio

        with suppress_datamule_output():
            portfolio = Portfolio(...)
        portfolio.download_submissions(..., quiet=True)

    This context manager:
    1. Suppresses Python warnings from datamule
    2. Redirects stdout/stderr to /dev/null using contextlib (Portfolio init messages)
    """
    import io
    import warnings

    # Suppress Python warnings (datamule uses warnings.warn for some messages)
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", message=".*extract year.*")
        warnings.filterwarnings("ignore", message=".*original filename.*")

        # Use contextlib.redirect_stdout/stderr for cleaner redirection
        devnull = io.StringIO()
        with contextlib.redirect_stdout(devnull), contextlib.redirect_stderr(devnull):
            yield
