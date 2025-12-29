"""
Utilities for working with datamule library.

This module provides helpers to suppress datamule's verbose output and redirect
it to log files, making scripts cleaner and more maintainable.

Uses thread-safe output capture to handle datamule printing from worker threads.

Also provides a thread-safe Portfolio cache to avoid expensive re-initialization
when multiple parsers need the same Portfolio object.
"""

import contextlib
import datetime
import logging
import os
import threading
from pathlib import Path
from typing import Any

from domain_status_graph.utils.thread_safe_output import (
    ThreadSafeOutputCapture,
)

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
    Context manager to suppress datamule's verbose output and redirect it to the log file.

    Uses thread-safe output capture to handle datamule printing from worker threads.
    This is more reliable than previous approaches because it works across thread boundaries.

    IMPORTANT: This must wrap Portfolio() creation to catch "Loading submissions" messages.

    Example:
        from domain_status_graph.utils.datamule import suppress_datamule_output

        with suppress_datamule_output():
            portfolio = Portfolio(...)
            portfolio.download_submissions(...)

    The context manager will:
    1. Disable tqdm globally using TQDM_DISABLE environment variable
    2. Install thread-safe stdout/stderr capture
    3. Filter out progress bars and noise
    4. Suppress Python warnings from datamule
    5. Write meaningful content to the log file (if available)
    """
    import warnings

    # Find the log file handler
    log_file_handler: logging.FileHandler | None = None
    for handler in logger.handlers:
        if isinstance(handler, logging.FileHandler):
            log_file_handler = handler
            break

    # Save original environment state
    old_tqdm_disable = os.environ.get("TQDM_DISABLE")

    # Use thread-safe output capture (works across thread boundaries)
    output_capture = ThreadSafeOutputCapture(capture_stdout=True, capture_stderr=True)

    try:
        # Disable tqdm globally using the official mechanism
        os.environ["TQDM_DISABLE"] = "1"

        # Suppress Python warnings (datamule uses warnings.warn for some messages)
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", message=".*extract year.*")
            warnings.filterwarnings("ignore", message=".*original filename.*")

            # Install thread-safe output capture
            # This works even if datamule prints from worker threads
            output_capture.install()

            try:
                yield
            finally:
                # Uninstall capture (restores stdout/stderr)
                output_capture.uninstall()
    finally:
        # Restore original environment
        if old_tqdm_disable is None:
            os.environ.pop("TQDM_DISABLE", None)
        else:
            os.environ["TQDM_DISABLE"] = old_tqdm_disable

        # Write captured output to log file (if available) - but only if there's meaningful content
        if log_file_handler:
            log_file = log_file_handler.stream

            # Get captured output
            stdout_content, stderr_content = output_capture.get_captured_output()

            # Filter out common datamule noise (progress bars, "Loading submissions", etc.)
            filtered_lines = []
            for line in stdout_content.splitlines() + stderr_content.splitlines():
                line = line.strip()
                if not line:
                    continue
                # Skip tqdm progress bars
                if _is_tqdm_progress_bar(line):
                    continue
                # Skip common datamule noise
                if any(
                    noise in line.lower()
                    for noise in [
                        "loading submissions",
                        "loading regular submissions",
                        "successfully loaded",
                        "could not extract year",  # Harmless filename parsing warning
                        "using original filename",  # Related to above
                        "%|",
                        "it/s",
                    ]
                ):
                    continue
                # Only log meaningful content
                if len(line) > 10:  # Skip very short lines (likely progress bar fragments)
                    filtered_lines.append(line)

            # Only write if there's meaningful content (avoid log spam)
            if filtered_lines:
                timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                for line in filtered_lines:
                    log_file.write(f"[{timestamp}] [datamule] {line}\n")
                log_file.flush()
