"""
Confidence Scoring Module.

Computes confidence scores based on multiple factors.
Each scoring component is isolated and testable.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass

from public_company_graph.entity_resolution.candidates import Candidate
from public_company_graph.entity_resolution.matchers import MatchResult, MatchType


@dataclass(frozen=True)
class ScoringFactors:
    """Individual factors that contribute to confidence score."""

    match_quality: float  # Base score from match type (0-1)
    length_penalty: float  # Penalty for short candidates (0-1)
    context_quality: float  # Quality of surrounding context (0-1)
    high_value_bonus: float  # Bonus for high-value companies (0-1)
    semantic_similarity: float  # Semantic match quality (0-1)


@dataclass(frozen=True)
class ConfidenceResult:
    """Result of confidence scoring."""

    match_result: MatchResult
    final_confidence: float  # Combined confidence (0-1)
    factors: ScoringFactors
    scorer_name: str


class ConfidenceScorer(ABC):
    """Abstract base class for confidence scorers."""

    @abstractmethod
    def score(
        self,
        match_result: MatchResult,
        context: dict | None = None,
    ) -> ConfidenceResult:
        """
        Compute confidence score for a match.

        Args:
            match_result: The match to score
            context: Optional context dict

        Returns:
            ConfidenceResult with detailed scoring
        """
        ...

    @property
    @abstractmethod
    def name(self) -> str:
        """Name of this scorer for debugging."""
        ...


class RuleBasedScorer(ConfidenceScorer):
    """
    Rule-based confidence scorer.

    Combines multiple factors using configurable weights.
    """

    # Default weights for each factor
    DEFAULT_WEIGHTS = {
        "match_quality": 0.40,
        "length_penalty": 0.20,
        "context_quality": 0.20,
        "high_value_bonus": 0.10,
        "semantic_similarity": 0.10,
    }

    def __init__(
        self,
        weights: dict[str, float] | None = None,
        high_value_names: set[str] | None = None,
    ):
        """
        Initialize scorer with optional custom weights.

        Args:
            weights: Custom factor weights (should sum to 1.0)
            high_value_names: Set of high-value company names
        """
        self.weights = weights or self.DEFAULT_WEIGHTS

        if high_value_names is not None:
            self.high_value_names = high_value_names
        else:
            from public_company_graph.parsing.business_relationship_extraction import (
                HIGH_VALUE_COMPANY_NAMES,
            )

            self.high_value_names = HIGH_VALUE_COMPANY_NAMES

    @property
    def name(self) -> str:
        return "rule_based"

    def score(
        self,
        match_result: MatchResult,
        context: dict | None = None,
    ) -> ConfidenceResult:
        """Compute confidence using rule-based scoring."""
        if not match_result.matched:
            return ConfidenceResult(
                match_result=match_result,
                final_confidence=0.0,
                factors=ScoringFactors(0.0, 0.0, 0.0, 0.0, 0.0),
                scorer_name=self.name,
            )

        # Compute individual factors
        match_quality = self._score_match_quality(match_result)
        length_penalty = self._score_length(match_result.candidate)
        context_quality = self._score_context(match_result.candidate, context)
        high_value_bonus = self._score_high_value(match_result)
        semantic_sim = self._score_semantic(match_result.candidate, match_result.name)

        factors = ScoringFactors(
            match_quality=match_quality,
            length_penalty=length_penalty,
            context_quality=context_quality,
            high_value_bonus=high_value_bonus,
            semantic_similarity=semantic_sim,
        )

        # Weighted combination
        final_confidence = (
            self.weights["match_quality"] * match_quality
            + self.weights["length_penalty"] * length_penalty
            + self.weights["context_quality"] * context_quality
            + self.weights["high_value_bonus"] * high_value_bonus
            + self.weights["semantic_similarity"] * semantic_sim
        )

        # Clamp to [0, 1]
        final_confidence = max(0.0, min(1.0, final_confidence))

        return ConfidenceResult(
            match_result=match_result,
            final_confidence=final_confidence,
            factors=factors,
            scorer_name=self.name,
        )

    def _score_match_quality(self, match_result: MatchResult) -> float:
        """Score based on match type."""
        scores = {
            MatchType.EXACT_TICKER: 1.0,
            MatchType.EXACT_NAME: 1.0,
            MatchType.NORMALIZED_NAME: 0.95,
            MatchType.FUZZY_NAME: 0.80,
            MatchType.ALIAS: 0.85,
            MatchType.NO_MATCH: 0.0,
        }
        return scores.get(match_result.match_type, 0.5)

    def _score_length(self, candidate: Candidate) -> float:
        """
        Score based on candidate length.

        Short candidates (≤4 chars) are penalized due to high false positive rates.
        """
        length = len(candidate.text.strip())

        if length <= 2:
            return 0.3  # Very short - high risk
        elif length <= 4:
            return 0.6  # Short - moderate risk
        elif length <= 6:
            return 0.8  # Medium
        else:
            return 1.0  # Long - low risk

    def _score_context(self, candidate: Candidate, context: dict | None) -> float:
        """
        Score based on context quality.

        Higher score if context contains relationship indicators.
        """
        sentence = candidate.sentence.lower()

        # Relationship keywords that increase confidence
        positive_indicators = [
            "competitor",
            "customer",
            "supplier",
            "partner",
            "vendor",
            "client",
            "collaboration",
            "agreement",
            "contract",
            "relationship",
        ]

        # Count positive indicators
        indicator_count = sum(1 for ind in positive_indicators if ind in sentence)

        if indicator_count >= 3:
            return 1.0
        elif indicator_count >= 2:
            return 0.9
        elif indicator_count >= 1:
            return 0.8
        else:
            return 0.6  # No strong indicators

    def _score_high_value(self, match_result: MatchResult) -> float:
        """Score bonus for high-value companies."""
        if not match_result.name:
            return 0.5  # Neutral

        name_lower = match_result.name.lower()

        # Check if any high-value name is in the matched name
        for hv_name in self.high_value_names:
            if hv_name in name_lower:
                return 1.0  # High-value company

        return 0.5  # Not high-value (neutral)

    def _score_semantic(self, candidate: Candidate, matched_name: str | None) -> float:
        """
        Score semantic similarity between candidate and matched name.

        Simple implementation - could be enhanced with embeddings.
        """
        if not matched_name:
            return 0.5

        candidate_text = candidate.text.lower()
        matched_lower = matched_name.lower()

        # Exact match
        if candidate_text == matched_lower:
            return 1.0

        # Containment
        if candidate_text in matched_lower or matched_lower in candidate_text:
            return 0.9

        # Token overlap
        candidate_tokens = set(candidate_text.split())
        matched_tokens = set(matched_lower.split())

        if candidate_tokens and matched_tokens:
            overlap = len(candidate_tokens & matched_tokens)
            total = len(candidate_tokens | matched_tokens)
            if total > 0:
                return 0.5 + 0.5 * (overlap / total)

        return 0.5  # Default


def compute_confidence(
    match_result: MatchResult,
    scorer: ConfidenceScorer | None = None,
    context: dict | None = None,
) -> ConfidenceResult:
    """
    Compute confidence for a match.

    Args:
        match_result: The match to score
        scorer: Scorer to use (default: RuleBasedScorer)
        context: Optional context dict

    Returns:
        ConfidenceResult with detailed scoring
    """
    if scorer is None:
        scorer = RuleBasedScorer()

    return scorer.score(match_result, context)


def compute_confidences_with_stats(
    match_results: list[MatchResult],
    scorer: ConfidenceScorer | None = None,
    context: dict | None = None,
) -> tuple[list[ConfidenceResult], dict[str, float]]:
    """
    Compute confidence for all matches and return statistics.

    Returns:
        Tuple of (confidence_results, stats_dict)
        stats_dict has aggregated stats like avg_confidence, min, max
    """
    if scorer is None:
        scorer = RuleBasedScorer()

    results: list[ConfidenceResult] = []
    confidences: list[float] = []

    for match_result in match_results:
        result = scorer.score(match_result, context)
        results.append(result)
        if result.final_confidence > 0:
            confidences.append(result.final_confidence)

    stats: dict[str, float] = {
        "count": len(results),
        "matched_count": len(confidences),
        "avg_confidence": sum(confidences) / len(confidences) if confidences else 0.0,
        "min_confidence": min(confidences) if confidences else 0.0,
        "max_confidence": max(confidences) if confidences else 0.0,
    }

    return results, stats
