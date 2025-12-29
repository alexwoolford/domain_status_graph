"""
Worker functions for parsing SEC 10-K filings in parallel.

This module provides worker functions that can be used with parallel
execution frameworks to parse 10-K filings efficiently.

Functions must be defined at module level to be serializable across processes.
"""

import logging
import warnings
from collections.abc import Callable
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

from public_company_graph.constants import CACHE_TTL_10K_EXTRACTED

logger = logging.getLogger(__name__)

# Cache constants
CACHE_NAMESPACE = "10k_extracted"

# Flag to ensure logging is only configured once per worker process
_worker_logging_configured = False


def _configure_worker_logging():
    """
    Configure logging for worker processes to suppress console output.

    In multiprocessing, child processes don't inherit the parent's logging
    configuration. This function ensures that warnings from parsing modules
    go to a NullHandler (silently ignored) instead of stderr.

    This is called once per worker process to prevent log spam during parallel parsing.
    """
    global _worker_logging_configured
    if _worker_logging_configured:
        return
    _worker_logging_configured = True

    # Suppress all public_company_graph logs from going to stderr
    # (they would clutter the tqdm progress bar)
    pkg_logger = logging.getLogger("public_company_graph")
    pkg_logger.handlers = []
    pkg_logger.addHandler(logging.NullHandler())
    pkg_logger.propagate = False

    # Also suppress datamule logs
    logging.getLogger("datamule").setLevel(logging.CRITICAL + 1)


def parse_10k_worker(args: tuple[str, str, str, bool, bool, bool]) -> tuple[str, str, str | None]:
    """
    Parse a single 10-K file with caching support.

    Args:
        args: Tuple of (file_path_str, cik, filings_dir_str, force, skip_datamule, incremental)

    Returns:
        Tuple of (cik, status, error_message)
        status is one of: 'parsed', 'updated', 'cached', 'failed'
    """
    warnings.filterwarnings("ignore")

    # Configure logging for this worker process to suppress console output
    # (multiprocessing workers don't inherit parent's logging config)
    _configure_worker_logging()

    file_path_str, cik, filings_dir_str, force_flag, skip_dm, incremental = args
    file_path = Path(file_path_str)
    filings_dir = Path(filings_dir_str) if filings_dir_str else None

    try:
        from public_company_graph.cache import get_cache
        from public_company_graph.parsing.base import get_default_parsers, parse_10k_with_parsers

        parsers = get_default_parsers()
        cache = get_cache()

        # Check cache
        existing_data = cache.get(CACHE_NAMESPACE, cik)
        if existing_data and not force_flag and not incremental:
            return (cik, "cached", None)

        # Parse file
        content = file_path.read_text(encoding="utf-8", errors="ignore")
        tar_file = _find_tar_file_for_cik(filings_dir, cik)

        data = parse_10k_with_parsers(
            file_path,
            parsers,
            file_content=content,
            cik=cik,
            filings_dir=filings_dir,
            skip_datamule=skip_dm,
            tar_file=tar_file,
        )
        data["cik"] = cik

        # Incremental merge
        if incremental and existing_data:
            merged = existing_data.copy()
            merged.update(data)
            data = merged
            status = "updated"
        else:
            status = "parsed"

        cache.set(CACHE_NAMESPACE, cik, data, ttl_days=CACHE_TTL_10K_EXTRACTED)
        return (cik, status, None)

    except Exception as e:
        return (cik, "failed", str(e))


def _find_tar_file_for_cik(filings_dir: Path | None, cik: str) -> Path | None:
    """
    Find the tar file containing 10-K metadata for a given CIK.

    The tar file contains metadata.json with authoritative filing dates
    from the SEC.

    Args:
        filings_dir: Base filings directory (e.g., data/10k_filings)
        cik: Company CIK (e.g., "0001687187")

    Returns:
        Path to tar file if found, None otherwise
    """
    if not filings_dir:
        return None

    portfolios_dir = filings_dir.parent / "10k_portfolios" / f"10k_{cik}"
    if not portfolios_dir.exists():
        return None

    # Filter out corrupt tar files (< 1KB)
    MIN_TAR_SIZE = 1024
    tar_files = [f for f in portfolios_dir.glob("*.tar") if f.stat().st_size >= MIN_TAR_SIZE]
    if not tar_files:
        return None

    if len(tar_files) == 1:
        return tar_files[0]

    # Prefer most recent by modification time
    return max(tar_files, key=lambda f: f.stat().st_mtime)


def _parse_single_file(args: tuple[str, str, str]) -> tuple[str, dict | None, str | None]:
    """
    Parse a single 10-K file without caching.

    Args:
        args: Tuple of (file_path_str, cik, filings_dir_str)

    Returns:
        Tuple of (cik, result_dict, error_message)
    """
    warnings.filterwarnings("ignore")

    file_path_str, cik, filings_dir_str = args
    file_path = Path(file_path_str)
    filings_dir = Path(filings_dir_str) if filings_dir_str else None

    try:
        from public_company_graph.parsing.base import get_default_parsers, parse_10k_with_parsers

        parsers = get_default_parsers()
        content = file_path.read_text(encoding="utf-8", errors="ignore")
        tar_file = _find_tar_file_for_cik(filings_dir, cik)

        result = parse_10k_with_parsers(
            file_path,
            parsers,
            file_content=content,
            cik=cik,
            filings_dir=filings_dir,
            tar_file=tar_file,
        )

        return (cik, result, None)

    except Exception as e:
        return (cik, None, str(e))


def parse_files_parallel(
    files: list[Path],
    filings_dir: Path | None = None,
    max_workers: int = 4,
    progress_callback: Callable[[int, int], None] | None = None,
) -> list[tuple[str, dict | None, str | None]]:
    """
    Parse multiple 10-K files in parallel.

    Args:
        files: List of file paths to parse
        filings_dir: Base directory for locating metadata
        max_workers: Number of parallel workers (default 4)
        progress_callback: Optional callback(completed, total) for progress updates

    Returns:
        List of (cik, result_dict, error_message) tuples
    """
    total = len(files)
    if total == 0:
        return []

    filings_dir_str = str(filings_dir) if filings_dir else ""
    args_list = [(str(f), f.parent.name, filings_dir_str) for f in files]

    results = []

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        future_to_args = {executor.submit(_parse_single_file, args): args for args in args_list}

        for future in as_completed(future_to_args):
            args = future_to_args[future]
            try:
                result = future.result()
                results.append(result)
            except Exception as e:
                cik = args[1]
                results.append((cik, None, f"Process error: {e}"))

            if progress_callback:
                progress_callback(len(results), total)

    return results
