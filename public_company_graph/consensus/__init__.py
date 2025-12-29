"""
Domain consensus logic.

This package contains modules for determining the correct domain from multiple sources
using weighted voting and early stopping.
"""

from public_company_graph.consensus.domain_consensus import collect_domains

__all__ = ["collect_domains"]
