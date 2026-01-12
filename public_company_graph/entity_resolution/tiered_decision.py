"""
Tiered Decision System for Entity Resolution.

Implements a cost-aware decision system that applies rules in order of cost:
1. Tier 1 (Free): Simple rules (blocklists, heuristics)
2. Tier 2 (Cheap): Pattern matching (regex, filters)
3. Tier 3 (Moderate): Embedding similarity
4. Tier 4 (Expensive): LLM verification

This ensures expensive operations are only used when necessary.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from public_company_graph.entity_resolution.candidates import Candidate

logger = logging.getLogger(__name__)


class Decision(Enum):
    """Decision outcome."""

    ACCEPT = "accept"  # Create fact edge
    CANDIDATE = "candidate"  # Create candidate edge
    REJECT = "reject"  # Don't create edge


class DecisionTier(Enum):
    """Which tier made the decision."""

    TIER1_RULES = "tier1_rules"  # Free: Simple rules
    TIER2_PATTERNS = "tier2_patterns"  # Cheap: Pattern matching
    TIER3_EMBEDDINGS = "tier3_embeddings"  # Moderate: Embedding similarity
    TIER4_LLM = "tier4_llm"  # Expensive: LLM verification


@dataclass
class TieredDecision:
    """Result of tiered decision system."""

    decision: Decision
    tier: DecisionTier
    confidence: float
    reason: str
    cost: float = 0.0  # Cost in dollars for this decision


@dataclass
class TieredMetrics:
    """Metrics for tiered decision system."""

    tier1_decisions: int = 0
    tier2_decisions: int = 0
    tier3_decisions: int = 0
    tier4_decisions: int = 0

    tier1_cost: float = 0.0  # Free
    tier2_cost: float = 0.0  # Free
    tier3_cost: float = 0.001  # Embedding API cost per call
    tier4_cost: float = 0.01  # LLM API cost per call

    def total_cost(self) -> float:
        """Calculate total cost."""
        return (
            self.tier1_decisions * self.tier1_cost
            + self.tier2_decisions * self.tier2_cost
            + self.tier3_decisions * self.tier3_cost
            + self.tier4_decisions * self.tier4_cost
        )

    def cost_per_decision(self) -> float:
        """Calculate average cost per decision."""
        total = (
            self.tier1_decisions
            + self.tier2_decisions
            + self.tier3_decisions
            + self.tier4_decisions
        )
        if total == 0:
            return 0.0
        return self.total_cost() / total


class TieredDecisionSystem:
    """
    Tiered decision system for entity resolution.

    Applies rules in order of cost, only using expensive methods when needed.
    """

    def __init__(
        self,
        use_tier1: bool = True,
        use_tier2: bool = True,
        use_tier3: bool = True,
        use_tier4: bool = True,
    ):
        """
        Initialize tiered decision system.

        Args:
            use_tier1: Enable Tier 1 (free rules)
            use_tier2: Enable Tier 2 (pattern matching)
            use_tier3: Enable Tier 3 (embedding similarity)
            use_tier4: Enable Tier 4 (LLM verification)
        """
        self.use_tier1 = use_tier1
        self.use_tier2 = use_tier2
        self.use_tier3 = use_tier3
        self.use_tier4 = use_tier4

        self.metrics = TieredMetrics()

        # Initialize filters (Tier 2)
        if self.use_tier2:
            from public_company_graph.entity_resolution.filters import (
                BiographicalContextFilter,
                CorporateStructureFilter,
                ExchangeReferenceFilter,
                PlatformDependencyFilter,
            )

            self.bio_filter = BiographicalContextFilter()
            self.exchange_filter = ExchangeReferenceFilter()
            self.corporate_filter = CorporateStructureFilter()
            self.platform_filter = PlatformDependencyFilter()

    def decide(
        self,
        candidate: Candidate,
        context: str,
        relationship_type: str,
        company_name: str | None = None,
        embedding_similarity: float | None = None,
        llm_verifier=None,
    ) -> TieredDecision:
        """
        Make a decision using tiered system.

        Args:
            candidate: The candidate to evaluate
            context: Sentence context
            relationship_type: Type of relationship
            company_name: Target company name
            embedding_similarity: Pre-computed embedding similarity (optional)
            llm_verifier: LLM verifier instance (optional, for Tier 4)

        Returns:
            TieredDecision with outcome and metadata
        """
        # Tier 1: Simple rules (free)
        if self.use_tier1:
            tier1_decision = self._tier1_decide(candidate, context, company_name=company_name)
            if tier1_decision:
                self.metrics.tier1_decisions += 1
                return tier1_decision

        # Tier 2: Pattern matching (cheap)
        if self.use_tier2:
            tier2_decision = self._tier2_decide(candidate, context, relationship_type)
            if tier2_decision:
                self.metrics.tier2_decisions += 1
                return tier2_decision

        # Tier 3: Embedding similarity (moderate cost)
        if self.use_tier3 and embedding_similarity is not None:
            tier3_decision = self._tier3_decide(relationship_type, embedding_similarity)
            if tier3_decision:
                self.metrics.tier3_decisions += 1
                return tier3_decision

        # Tier 4: LLM verification (expensive, only for edge cases)
        if self.use_tier4 and llm_verifier:
            tier4_decision = self._tier4_decide(
                candidate, context, relationship_type, company_name, llm_verifier
            )
            if tier4_decision:
                self.metrics.tier4_decisions += 1
                return tier4_decision

        # Default: Reject if no tier made a decision
        return TieredDecision(
            decision=Decision.REJECT,
            tier=DecisionTier.TIER3_EMBEDDINGS,  # Default tier
            confidence=0.0,
            reason="No tier made a decision",
            cost=0.0,
        )

    def _is_in_company_list(self, context: str, mention: str) -> bool:
        """Check if mention appears in a company list (context-aware)."""
        import re

        # Patterns that indicate mention is in a company list
        patterns = [
            rf"{re.escape(mention)},",  # "Target, Walmart"
            rf"{re.escape(mention)}\s+and",  # "Target and Walmart"
            rf"such\s+as\s+{re.escape(mention)}",  # "customers such as Target"
            rf"including\s+{re.escape(mention)}",  # "including Target"
            rf"{re.escape(mention)}\s+and\s+other",  # "Target and other"
        ]
        return any(re.search(p, context, re.IGNORECASE) for p in patterns)

    def _tier1_decide(
        self, candidate: Candidate, context: str, company_name: str | None = None
    ) -> TieredDecision | None:
        """Tier 1: Simple rules (free)."""
        mention = candidate.text.lower().strip()
        context_lower = context.lower()

        # Rule: Block generic words (but only if NOT in company list AND not resolved to a company)
        generic_words = {"target", "master", "apple", "amazon", "google", "microsoft"}
        if mention in generic_words:
            # If company_name is provided, it means entity resolution succeeded
            # This is a real company, not generic use - skip blocklist
            if company_name:
                return None  # Continue to next tier - it's a resolved company

            # Context-aware: If in company list, it's correct
            if self._is_in_company_list(context_lower, mention):
                # It's a real company, not generic use
                return None  # Continue to next tier
            else:
                # Generic word use, not company name
                return TieredDecision(
                    decision=Decision.REJECT,
                    tier=DecisionTier.TIER1_RULES,
                    confidence=0.95,
                    reason=f"Generic word blocklist: {mention} (not in company list)",
                    cost=0.0,
                )

        # Rule: Block very short mentions (except known companies or in lists)
        if len(mention) <= 2:
            # If in company list, it's OK
            if self._is_in_company_list(context_lower, mention):
                return None  # Continue to next tier

            # High-value tickers are OK
            high_value_tickers = {"ibm", "hp", "ge", "at", "ma"}
            if mention in high_value_tickers:
                return None  # Continue to next tier

            return TieredDecision(
                decision=Decision.REJECT,
                tier=DecisionTier.TIER1_RULES,
                confidence=0.9,
                reason=f"Too short: {mention} (not in company list)",
                cost=0.0,
            )

        return None  # No rule matched, continue to next tier

    def _tier2_decide(
        self, candidate: Candidate, context: str, relationship_type: str
    ) -> TieredDecision | None:
        """Tier 2: Pattern matching (cheap)."""
        # Biographical filter
        bio_result = self.bio_filter.filter(candidate)
        if not bio_result.passed:
            return TieredDecision(
                decision=Decision.REJECT,
                tier=DecisionTier.TIER2_PATTERNS,
                confidence=0.9,
                reason=f"Biographical context: {bio_result.reason.value}",
                cost=0.0,
            )

        # Exchange filter
        exchange_result = self.exchange_filter.filter(candidate)
        if not exchange_result.passed:
            return TieredDecision(
                decision=Decision.REJECT,
                tier=DecisionTier.TIER2_PATTERNS,
                confidence=0.9,
                reason=f"Exchange reference: {exchange_result.reason.value}",
                cost=0.0,
            )

        # Corporate structure filter
        corporate_result = self.corporate_filter.filter(candidate)
        if not corporate_result.passed:
            return TieredDecision(
                decision=Decision.REJECT,
                tier=DecisionTier.TIER2_PATTERNS,
                confidence=0.85,
                reason=f"Corporate structure: {corporate_result.reason.value}",
                cost=0.0,
            )

        return None  # No pattern matched, continue to next tier

    def _tier3_decide(
        self, relationship_type: str, embedding_similarity: float
    ) -> TieredDecision | None:
        """Tier 3: Embedding similarity (moderate cost)."""
        from public_company_graph.parsing.relationship_config import (
            ConfidenceTier,
            get_confidence_tier,
        )

        tier = get_confidence_tier(relationship_type, embedding_similarity)

        if tier == ConfidenceTier.HIGH:
            return TieredDecision(
                decision=Decision.ACCEPT,
                tier=DecisionTier.TIER3_EMBEDDINGS,
                confidence=embedding_similarity,
                reason=f"High embedding similarity: {embedding_similarity:.3f}",
                cost=self.metrics.tier3_cost,
            )
        elif tier == ConfidenceTier.MEDIUM:
            return TieredDecision(
                decision=Decision.CANDIDATE,
                tier=DecisionTier.TIER3_EMBEDDINGS,
                confidence=embedding_similarity,
                reason=f"Medium embedding similarity: {embedding_similarity:.3f}",
                cost=self.metrics.tier3_cost,
            )
        else:
            return TieredDecision(
                decision=Decision.REJECT,
                tier=DecisionTier.TIER3_EMBEDDINGS,
                confidence=embedding_similarity,
                reason=f"Low embedding similarity: {embedding_similarity:.3f}",
                cost=self.metrics.tier3_cost,
            )

    def _tier4_decide(
        self,
        candidate: Candidate,
        context: str,
        relationship_type: str,
        company_name: str | None,
        llm_verifier,
    ) -> TieredDecision | None:
        """Tier 4: LLM verification (expensive, only for edge cases)."""
        # Only use LLM for certain relationship types or uncertain cases
        if relationship_type not in ["HAS_SUPPLIER", "HAS_CUSTOMER"]:
            return None  # Don't use LLM for competitors/partners

        try:
            result = llm_verifier.verify(
                context=context,
                mention=candidate.text,
                relationship_type=relationship_type,
                company_name=company_name or "",
            )

            if result.verified:
                return TieredDecision(
                    decision=Decision.ACCEPT,
                    tier=DecisionTier.TIER4_LLM,
                    confidence=result.confidence,
                    reason=f"LLM verified: {result.reasoning[:50]}...",
                    cost=self.metrics.tier4_cost,
                )
            else:
                return TieredDecision(
                    decision=Decision.REJECT,
                    tier=DecisionTier.TIER4_LLM,
                    confidence=result.confidence,
                    reason=f"LLM rejected: {result.reasoning[:50]}...",
                    cost=self.metrics.tier4_cost,
                )
        except Exception as e:
            logger.warning(f"LLM verification failed: {e}")
            return None  # Fall back to previous tier's decision
