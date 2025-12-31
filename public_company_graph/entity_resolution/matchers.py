"""
Candidate Matching Module.

Matches candidates against the company lookup table.
Each matching strategy is isolated and testable.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum

from public_company_graph.entity_resolution.candidates import Candidate


class MatchType(Enum):
    """Types of matches."""

    NO_MATCH = "no_match"
    EXACT_TICKER = "exact_ticker"
    EXACT_NAME = "exact_name"
    NORMALIZED_NAME = "normalized_name"
    FUZZY_NAME = "fuzzy_name"
    ALIAS = "alias"


@dataclass(frozen=True)
class MatchResult:
    """Result of attempting to match a candidate."""

    candidate: Candidate
    matched: bool
    match_type: MatchType
    cik: str | None = None
    ticker: str | None = None
    name: str | None = None
    base_confidence: float = 0.0  # Before context adjustments
    matcher_name: str = ""


class CandidateMatcher(ABC):
    """Abstract base class for candidate matchers."""

    @abstractmethod
    def match(
        self,
        candidate: Candidate,
        lookup: CompanyLookup,
        context: dict | None = None,
    ) -> MatchResult:
        """
        Attempt to match a candidate.

        Args:
            candidate: The candidate to match
            lookup: Company lookup table
            context: Optional context dict

        Returns:
            MatchResult with match details
        """
        ...

    @property
    @abstractmethod
    def name(self) -> str:
        """Name of this matcher for debugging."""
        ...

    @property
    @abstractmethod
    def priority(self) -> int:
        """Priority (lower = tried first)."""
        ...


# Import CompanyLookup for type hints
from public_company_graph.parsing.business_relationship_extraction import (
    CompanyLookup,
    _normalize_company_name,
)


class ExactTickerMatcher(CandidateMatcher):
    """
    Matches candidates against exact ticker symbols.

    Most precise match type.
    """

    @property
    def name(self) -> str:
        return "exact_ticker"

    @property
    def priority(self) -> int:
        return 1

    def match(
        self,
        candidate: Candidate,
        lookup: CompanyLookup,
        context: dict | None = None,
    ) -> MatchResult:
        """Match against ticker lookup."""
        upper = candidate.text.upper().strip()

        if upper in lookup.ticker_to_company:
            cik, ticker, name = lookup.ticker_to_company[upper]
            return MatchResult(
                candidate=candidate,
                matched=True,
                match_type=MatchType.EXACT_TICKER,
                cik=cik,
                ticker=ticker,
                name=name,
                base_confidence=1.0,
                matcher_name=self.name,
            )

        return MatchResult(
            candidate=candidate,
            matched=False,
            match_type=MatchType.NO_MATCH,
            matcher_name=self.name,
        )


class ExactNameMatcher(CandidateMatcher):
    """
    Matches candidates against exact company names (lowercase).
    """

    @property
    def name(self) -> str:
        return "exact_name"

    @property
    def priority(self) -> int:
        return 2

    def match(
        self,
        candidate: Candidate,
        lookup: CompanyLookup,
        context: dict | None = None,
    ) -> MatchResult:
        """Match against name lookup."""
        lower = candidate.text.lower().strip()

        if lower in lookup.name_to_company:
            cik, ticker, name = lookup.name_to_company[lower]
            return MatchResult(
                candidate=candidate,
                matched=True,
                match_type=MatchType.EXACT_NAME,
                cik=cik,
                ticker=ticker,
                name=name,
                base_confidence=1.0,
                matcher_name=self.name,
            )

        return MatchResult(
            candidate=candidate,
            matched=False,
            match_type=MatchType.NO_MATCH,
            matcher_name=self.name,
        )


class NormalizedNameMatcher(CandidateMatcher):
    """
    Matches candidates after normalizing (removing Corp, Inc, etc.).
    """

    @property
    def name(self) -> str:
        return "normalized_name"

    @property
    def priority(self) -> int:
        return 3

    def match(
        self,
        candidate: Candidate,
        lookup: CompanyLookup,
        context: dict | None = None,
    ) -> MatchResult:
        """Match after normalization."""
        normalized = _normalize_company_name(candidate.text)

        if normalized and normalized in lookup.name_to_company:
            cik, ticker, name = lookup.name_to_company[normalized]
            return MatchResult(
                candidate=candidate,
                matched=True,
                match_type=MatchType.NORMALIZED_NAME,
                cik=cik,
                ticker=ticker,
                name=name,
                base_confidence=0.95,  # Slightly lower confidence
                matcher_name=self.name,
            )

        return MatchResult(
            candidate=candidate,
            matched=False,
            match_type=MatchType.NO_MATCH,
            matcher_name=self.name,
        )


class FuzzyNameMatcher(CandidateMatcher):
    """
    Matches candidates using fuzzy string matching.

    Uses edit distance or token-based similarity.
    """

    def __init__(self, min_similarity: float = 0.85):
        """
        Initialize fuzzy matcher.

        Args:
            min_similarity: Minimum similarity score (0-1) to accept
        """
        self.min_similarity = min_similarity

    @property
    def name(self) -> str:
        return "fuzzy_name"

    @property
    def priority(self) -> int:
        return 4

    def match(
        self,
        candidate: Candidate,
        lookup: CompanyLookup,
        context: dict | None = None,
    ) -> MatchResult:
        """Match using fuzzy string similarity."""
        candidate_lower = candidate.text.lower().strip()
        candidate_normalized = _normalize_company_name(candidate.text)

        best_match = None
        best_similarity = 0.0

        # Check against all names
        for name_key in lookup.name_to_company:
            # Use simple containment check for now
            # Could be upgraded to proper fuzzy matching
            sim = self._compute_similarity(candidate_normalized or candidate_lower, name_key)
            if sim > best_similarity and sim >= self.min_similarity:
                best_similarity = sim
                best_match = lookup.name_to_company[name_key]

        if best_match:
            cik, ticker, name = best_match
            return MatchResult(
                candidate=candidate,
                matched=True,
                match_type=MatchType.FUZZY_NAME,
                cik=cik,
                ticker=ticker,
                name=name,
                base_confidence=best_similarity * 0.9,  # Scaled confidence
                matcher_name=self.name,
            )

        return MatchResult(
            candidate=candidate,
            matched=False,
            match_type=MatchType.NO_MATCH,
            matcher_name=self.name,
        )

    def _compute_similarity(self, s1: str, s2: str) -> float:
        """
        Compute string similarity (0-1).

        Simple implementation - could be upgraded to Levenshtein, Jaro-Winkler, etc.
        """
        if not s1 or not s2:
            return 0.0

        # Token overlap (Jaccard-like)
        tokens1 = set(s1.lower().split())
        tokens2 = set(s2.lower().split())

        if not tokens1 or not tokens2:
            return 0.0

        intersection = len(tokens1 & tokens2)
        union = len(tokens1 | tokens2)

        return intersection / union if union > 0 else 0.0


def match_candidate(
    candidate: Candidate,
    lookup: CompanyLookup,
    matchers: list[CandidateMatcher] | None = None,
    context: dict | None = None,
) -> MatchResult:
    """
    Try all matchers in priority order, return first match.

    Args:
        candidate: Candidate to match
        lookup: Company lookup table
        matchers: List of matchers (default: standard matchers)
        context: Optional context dict

    Returns:
        MatchResult from first successful matcher, or NO_MATCH
    """
    if matchers is None:
        matchers = [
            ExactTickerMatcher(),
            ExactNameMatcher(),
            NormalizedNameMatcher(),
        ]

    # Sort by priority
    sorted_matchers = sorted(matchers, key=lambda m: m.priority)

    for matcher in sorted_matchers:
        result = matcher.match(candidate, lookup, context)
        if result.matched:
            return result

    return MatchResult(
        candidate=candidate,
        matched=False,
        match_type=MatchType.NO_MATCH,
        matcher_name="all_matchers",
    )


def match_candidates_with_stats(
    candidates: list[Candidate],
    lookup: CompanyLookup,
    matchers: list[CandidateMatcher] | None = None,
    context: dict | None = None,
) -> tuple[list[MatchResult], dict[str, int]]:
    """
    Match all candidates and return statistics.

    Returns:
        Tuple of (match_results, stats_dict)
        stats_dict has matcher_name → success_count mapping
    """
    if matchers is None:
        matchers = [
            ExactTickerMatcher(),
            ExactNameMatcher(),
            NormalizedNameMatcher(),
        ]

    results: list[MatchResult] = []
    stats: dict[str, int] = {"no_match": 0}
    for m in matchers:
        stats[m.name] = 0

    for candidate in candidates:
        result = match_candidate(candidate, lookup, matchers, context)
        results.append(result)

        if result.matched:
            stats[result.matcher_name] = stats.get(result.matcher_name, 0) + 1
        else:
            stats["no_match"] += 1

    return results, stats
