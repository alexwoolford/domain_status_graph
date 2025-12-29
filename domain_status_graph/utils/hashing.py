"""
Hashing utilities for domain_status_graph.

Provides common hashing functions used across the codebase.
"""

import hashlib


def compute_text_hash(text: str) -> str:
    """
    Compute SHA256 hash of text for change detection.

    Args:
        text: Text to hash

    Returns:
        SHA256 hex digest of normalized text, or empty string if text is empty
    """
    if not text:
        return ""
    normalized = text.strip()
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()
