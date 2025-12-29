"""
Domain validation and normalization utilities.

Provides centralized domain validation, normalization, and root domain extraction
using tldextract and the Public Suffix List.
"""

from domain_status_graph.domain.validation import (
    is_valid_domain,
    normalize_domain,
    root_domain,
)

__all__ = [
    "is_valid_domain",
    "normalize_domain",
    "root_domain",
]
