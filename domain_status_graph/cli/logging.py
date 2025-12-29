"""
Logging utilities for domain_status_graph CLI.

Provides logging setup and header printing functions.
"""

import logging
import sys
import time
from pathlib import Path


def setup_logging(
    script_name: str,
    execute: bool = False,
    log_dir: Path = Path("logs"),
) -> logging.Logger:
    """
    Set up logging for a script.

    Args:
        script_name: Name of the script (for log file naming)
        execute: If True, log to file + console. If False, only console.
        log_dir: Directory for log files

    Returns:
        Configured logger instance
    """
    if execute:
        log_dir.mkdir(parents=True, exist_ok=True)
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        log_file = log_dir / f"{script_name}_{timestamp}.log"

        # Create logger with script name (not __name__) for better identification
        logger = logging.getLogger(script_name)
        logger.setLevel(logging.DEBUG)  # Capture all levels
        logger.handlers = []  # Clear any existing handlers

        # Create a custom handler class that flushes after each emit
        # This ensures log messages are written to disk immediately
        class FlushingFileHandler(logging.FileHandler):
            def emit(self, record):
                super().emit(record)
                self.flush()

        # File handler: DEBUG and above (detailed logs)
        # Use flushing handler to ensure logs are written immediately
        file_handler = FlushingFileHandler(log_file, mode="a", encoding="utf-8")
        file_handler.setLevel(logging.DEBUG)
        file_formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        file_handler.setFormatter(file_formatter)

        # Console handler: INFO and above (summary only)
        # Use stderr to avoid conflicts with tqdm (which uses stdout)
        console_handler = logging.StreamHandler(sys.stderr)
        console_handler.setLevel(logging.INFO)
        console_formatter = logging.Formatter("%(message)s")
        console_handler.setFormatter(console_formatter)

        # Add handlers
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

        # Prevent propagation to root logger (avoid duplicate messages)
        logger.propagate = False

        # Suppress noisy external library loggers
        # These print warnings to console that clutter output
        for noisy_logger in [
            "datamule",  # 10-K downloader library
            "httpx",  # HTTP client
            "openai",  # OpenAI API
            "urllib3",  # HTTP library
            "httpcore",  # HTTP core
        ]:
            logging.getLogger(noisy_logger).setLevel(logging.ERROR)

        logger.info(f"Log file: {log_file}")
        return logger
    else:
        logging.basicConfig(
            level=logging.INFO,
            format="%(message)s",
            stream=sys.stdout,
        )
        return logging.getLogger(__name__)


def print_dry_run_header(title: str, logger: logging.Logger | None = None):
    """
    Print a standard dry-run header.

    Args:
        title: Title for the dry-run section
        logger: Optional logger instance (if None, uses print)
    """
    if logger is None:
        logger = logging.getLogger(__name__)

    logger.info("=" * 70)
    logger.info(f"{title} (Dry Run)")
    logger.info("=" * 70)


def print_execute_header(title: str, logger: logging.Logger | None = None):
    """
    Print a standard execute mode header.

    Args:
        title: Title for the execute section
        logger: Optional logger instance (if None, uses print)
    """
    if logger is None:
        logger = logging.getLogger(__name__)

    logger.info("=" * 70)
    logger.info(title)
    logger.info("=" * 70)
