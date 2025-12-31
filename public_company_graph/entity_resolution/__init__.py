"""
Entity Resolution Module.

Provides testable, modular entity resolution for company mentions in 10-K filings.

This module separates concerns into distinct, testable components:
- Candidate extraction (finding potential company mentions)
- Candidate filtering (blocklists, length checks)
- Candidate matching (lookup table resolution)
- Confidence scoring (multi-factor scoring)

Each component can be tested independently and swapped out for improved versions.
"""

from public_company_graph.entity_resolution.candidates import (
    CandidateExtractor,
    extract_candidates,
)
from public_company_graph.entity_resolution.filters import (
    CandidateFilter,
    FilterResult,
    filter_candidate,
)
from public_company_graph.entity_resolution.matchers import (
    CandidateMatcher,
    MatchResult,
    match_candidate,
)
from public_company_graph.entity_resolution.resolver import (
    EntityResolver,
    ResolutionResult,
)
from public_company_graph.entity_resolution.scoring import (
    ConfidenceScorer,
    compute_confidence,
)

__all__ = [
    # Candidates
    "CandidateExtractor",
    "extract_candidates",
    # Filters
    "CandidateFilter",
    "FilterResult",
    "filter_candidate",
    # Matchers
    "CandidateMatcher",
    "MatchResult",
    "match_candidate",
    # Scoring
    "ConfidenceScorer",
    "compute_confidence",
    # Main resolver
    "EntityResolver",
    "ResolutionResult",
]
