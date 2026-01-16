"""
Security utilities for path validation and sanitization.

This module provides functions to prevent path traversal attacks and validate
file paths are within expected directories.
"""

import logging
from pathlib import Path

logger = logging.getLogger(__name__)


def validate_path_within_base(
    file_path: Path, base_dir: Path, logger_instance: logging.Logger | None = None
) -> bool:
    """
    Validate that file_path is within base_dir to prevent path traversal attacks.

    This function resolves both paths and ensures the file_path is a subdirectory
    or file within base_dir. This prevents attacks using `../` sequences.

    Args:
        file_path: Path to validate
        base_dir: Base directory that file_path must be within
        logger_instance: Optional logger instance for warnings (defaults to module logger)

    Returns:
        True if path is safe (within base_dir), False otherwise

    Example:
        >>> validate_path_within_base(Path("/data/file.txt"), Path("/data"))
        True
        >>> validate_path_within_base(Path("/etc/passwd"), Path("/data"))
        False
    """
    _log = logger_instance if logger_instance is not None else logger

    try:
        resolved_path = file_path.resolve()
        resolved_base = base_dir.resolve()
        resolved_path.relative_to(resolved_base)
        return True
    except (ValueError, OSError):
        _log.warning(f"Path traversal attempt detected: {file_path} (base: {base_dir})")
        return False
