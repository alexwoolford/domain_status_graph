"""
Entity Resolver Module.

Orchestrates the full entity resolution pipeline:
1. Extract candidates
2. Filter candidates
3. Match against lookup
4. Score confidence

Each step is testable independently.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from public_company_graph.entity_resolution.candidates import (
    Candidate,
    CandidateExtractor,
    CapitalizedWordExtractor,
    TickerExtractor,
    extract_candidates,
)
from public_company_graph.entity_resolution.filters import (
    CandidateFilter,
    FilterResult,
    LengthFilter,
    NameBlocklistFilter,
    TickerBlocklistFilter,
    filter_candidate,
)
from public_company_graph.entity_resolution.matchers import (
    CandidateMatcher,
    ExactNameMatcher,
    ExactTickerMatcher,
    MatchResult,
    NormalizedNameMatcher,
    match_candidate,
)
from public_company_graph.entity_resolution.scoring import (
    ConfidenceResult,
    ConfidenceScorer,
    RuleBasedScorer,
    compute_confidence,
)
from public_company_graph.parsing.business_relationship_extraction import (
    CompanyLookup,
)


@dataclass
class ResolutionResult:
    """Complete result of entity resolution."""

    # Original input
    raw_text: str
    sentence: str

    # Resolution details
    matched: bool
    cik: str | None = None
    ticker: str | None = None
    name: str | None = None
    confidence: float = 0.0

    # Pipeline details (for debugging)
    candidate: Candidate | None = None
    filter_result: FilterResult | None = None
    match_result: MatchResult | None = None
    confidence_result: ConfidenceResult | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "raw_text": self.raw_text,
            "sentence": self.sentence[:200],  # Truncate
            "matched": self.matched,
            "cik": self.cik,
            "ticker": self.ticker,
            "name": self.name,
            "confidence": self.confidence,
        }


class EntityResolver:
    """
    Main entity resolution orchestrator.

    Configurable pipeline with pluggable components.
    """

    def __init__(
        self,
        extractors: list[CandidateExtractor] | None = None,
        filters: list[CandidateFilter] | None = None,
        matchers: list[CandidateMatcher] | None = None,
        scorer: ConfidenceScorer | None = None,
        min_confidence: float = 0.5,
    ):
        """
        Initialize resolver with configurable components.

        Args:
            extractors: Candidate extractors (default: standard extractors)
            filters: Candidate filters (default: standard filters)
            matchers: Candidate matchers (default: standard matchers)
            scorer: Confidence scorer (default: RuleBasedScorer)
            min_confidence: Minimum confidence to accept (default: 0.5)
        """
        self.extractors = extractors or [
            CapitalizedWordExtractor(),
            TickerExtractor(),
        ]

        self.filters = filters or [
            TickerBlocklistFilter(),
            NameBlocklistFilter(),
            LengthFilter(),
        ]

        self.matchers = matchers or [
            ExactTickerMatcher(),
            ExactNameMatcher(),
            NormalizedNameMatcher(),
        ]

        self.scorer = scorer or RuleBasedScorer()
        self.min_confidence = min_confidence

    def resolve(
        self,
        text: str,
        lookup: CompanyLookup,
        context: dict | None = None,
    ) -> list[ResolutionResult]:
        """
        Resolve all company mentions in text.

        Args:
            text: Source text
            lookup: Company lookup table
            context: Optional context (self_cik, self_name, etc.)

        Returns:
            List of ResolutionResult for each resolved entity
        """
        results: list[ResolutionResult] = []
        seen_ciks: set[str] = set()

        # 1. Extract candidates
        candidates = extract_candidates(text, self.extractors)

        for candidate in candidates:
            # 2. Filter
            filter_result = filter_candidate(candidate, self.filters, context)
            if not filter_result.passed:
                continue

            # 3. Match
            match_result = match_candidate(candidate, lookup, self.matchers, context)
            if not match_result.matched:
                continue

            # Skip self-references
            self_cik = context.get("self_cik") if context else None
            if self_cik and match_result.cik == self_cik:
                continue

            # Skip duplicates
            if match_result.cik in seen_ciks:
                continue

            # 4. Score
            confidence_result = compute_confidence(match_result, self.scorer, context)

            # Check minimum confidence
            if confidence_result.final_confidence < self.min_confidence:
                continue

            seen_ciks.add(match_result.cik)  # type: ignore

            results.append(
                ResolutionResult(
                    raw_text=candidate.text,
                    sentence=candidate.sentence,
                    matched=True,
                    cik=match_result.cik,
                    ticker=match_result.ticker,
                    name=match_result.name,
                    confidence=confidence_result.final_confidence,
                    candidate=candidate,
                    filter_result=filter_result,
                    match_result=match_result,
                    confidence_result=confidence_result,
                )
            )

        return results

    def resolve_with_stats(
        self,
        text: str,
        lookup: CompanyLookup,
        context: dict | None = None,
    ) -> tuple[list[ResolutionResult], dict[str, Any]]:
        """
        Resolve entities and return detailed statistics.

        Returns:
            Tuple of (results, stats_dict)
        """
        stats: dict[str, Any] = {
            "candidates_extracted": 0,
            "candidates_filtered": 0,
            "candidates_matched": 0,
            "candidates_below_confidence": 0,
            "final_results": 0,
            "filter_reasons": {},
            "match_types": {},
        }

        results: list[ResolutionResult] = []
        seen_ciks: set[str] = set()

        # 1. Extract
        candidates = extract_candidates(text, self.extractors)
        stats["candidates_extracted"] = len(candidates)

        for candidate in candidates:
            # 2. Filter
            filter_result = filter_candidate(candidate, self.filters, context)
            if not filter_result.passed:
                stats["candidates_filtered"] += 1
                reason = filter_result.reason.value
                stats["filter_reasons"][reason] = stats["filter_reasons"].get(reason, 0) + 1
                continue

            # 3. Match
            match_result = match_candidate(candidate, lookup, self.matchers, context)
            if not match_result.matched:
                continue

            stats["candidates_matched"] += 1
            match_type = match_result.match_type.value
            stats["match_types"][match_type] = stats["match_types"].get(match_type, 0) + 1

            # Skip self-references
            self_cik = context.get("self_cik") if context else None
            if self_cik and match_result.cik == self_cik:
                continue

            # Skip duplicates
            if match_result.cik in seen_ciks:
                continue

            # 4. Score
            confidence_result = compute_confidence(match_result, self.scorer, context)

            if confidence_result.final_confidence < self.min_confidence:
                stats["candidates_below_confidence"] += 1
                continue

            seen_ciks.add(match_result.cik)  # type: ignore

            results.append(
                ResolutionResult(
                    raw_text=candidate.text,
                    sentence=candidate.sentence,
                    matched=True,
                    cik=match_result.cik,
                    ticker=match_result.ticker,
                    name=match_result.name,
                    confidence=confidence_result.final_confidence,
                    candidate=candidate,
                    filter_result=filter_result,
                    match_result=match_result,
                    confidence_result=confidence_result,
                )
            )

        stats["final_results"] = len(results)
        return results, stats


# =============================================================================
# Convenience Functions
# =============================================================================


def resolve_company_mentions(
    text: str,
    lookup: CompanyLookup,
    self_cik: str | None = None,
    min_confidence: float = 0.5,
) -> list[dict[str, Any]]:
    """
    Simple interface for entity resolution.

    Args:
        text: Text to analyze
        lookup: Company lookup table
        self_cik: CIK of the filing company (to exclude)
        min_confidence: Minimum confidence threshold

    Returns:
        List of dicts with: cik, ticker, name, confidence, raw_text, sentence
    """
    resolver = EntityResolver(min_confidence=min_confidence)
    context = {"self_cik": self_cik} if self_cik else None

    results = resolver.resolve(text, lookup, context)

    return [r.to_dict() for r in results]


def analyze_resolution_quality(
    text: str,
    lookup: CompanyLookup,
    self_cik: str | None = None,
) -> dict[str, Any]:
    """
    Analyze entity resolution quality for debugging.

    Returns detailed statistics about the resolution process.
    """
    resolver = EntityResolver(min_confidence=0.0)  # Get all matches for analysis
    context = {"self_cik": self_cik} if self_cik else None

    _, stats = resolver.resolve_with_stats(text, lookup, context)

    return {
        "summary": {
            "extracted": stats["candidates_extracted"],
            "filtered": stats["candidates_filtered"],
            "matched": stats["candidates_matched"],
            "below_confidence": stats["candidates_below_confidence"],
            "final": stats["final_results"],
        },
        "filter_breakdown": stats["filter_reasons"],
        "match_breakdown": stats["match_types"],
        "efficiency": {
            "filter_rate": (
                stats["candidates_filtered"] / stats["candidates_extracted"]
                if stats["candidates_extracted"] > 0
                else 0
            ),
            "match_rate": (
                stats["candidates_matched"]
                / (stats["candidates_extracted"] - stats["candidates_filtered"])
                if (stats["candidates_extracted"] - stats["candidates_filtered"]) > 0
                else 0
            ),
        },
    }
