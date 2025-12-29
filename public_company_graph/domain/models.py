"""
Data models for domain collection results.

These dataclasses represent results from domain collection sources
and the final consensus result for a company.
"""

from dataclasses import dataclass, field


@dataclass
class DomainResult:
    """Result from a single data source."""

    domain: str | None
    source: str
    confidence: float  # 0.0 to 1.0
    description: str | None = None  # Company description from this source
    metadata: dict = field(default_factory=dict)


@dataclass
class CompanyResult:
    """Final result for a company."""

    cik: str
    ticker: str
    name: str
    domain: str | None
    sources: list[str]
    confidence: float
    votes: int
    all_candidates: dict[str, list[str]]  # domain -> list of sources
    description: str | None = None  # Company description (best available)
    description_source: str | None = None  # Source of the description
    metadata: dict = field(default_factory=dict)
