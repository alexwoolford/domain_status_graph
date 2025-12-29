"""
Domain collection sources.

This package contains modules for collecting company domains from various sources:
- yfinance: Fast, reliable, good coverage
- finviz: Fast, good coverage
- sec_edgar: Authoritative but slower
- finnhub: Incomplete but can augment
"""

from domain_status_graph.sources.finnhub import get_domain_from_finnhub
from domain_status_graph.sources.finviz import get_domain_from_finviz
from domain_status_graph.sources.sec_edgar import get_domain_from_sec
from domain_status_graph.sources.yfinance import get_domain_from_yfinance

__all__ = [
    "get_domain_from_yfinance",
    "get_domain_from_finviz",
    "get_domain_from_sec",
    "get_domain_from_finnhub",
]
