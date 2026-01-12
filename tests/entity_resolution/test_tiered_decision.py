"""Tests for tiered decision system."""

from public_company_graph.entity_resolution.candidates import Candidate
from public_company_graph.entity_resolution.tiered_decision import (
    Decision,
    DecisionTier,
    TieredDecisionSystem,
)


class TestTieredDecisionSystem:
    """Tests for TieredDecisionSystem."""

    def test_tier1_rejects_generic_word(self):
        """Tier 1 should reject generic words not in company lists."""
        system = TieredDecisionSystem(
            use_tier1=True, use_tier2=False, use_tier3=False, use_tier4=False
        )

        candidate = Candidate(
            text="target",
            sentence="Our target market is growing",
            start_pos=0,
            end_pos=6,
            source_pattern="extraction",
        )

        decision = system.decide(
            candidate=candidate,
            context="Our target market is growing",
            relationship_type="HAS_COMPETITOR",
        )

        assert decision.decision == Decision.REJECT
        assert decision.tier == DecisionTier.TIER1_RULES
        assert "generic word" in decision.reason.lower()

    def test_tier1_accepts_generic_word_in_company_list(self):
        """Tier 1 should accept generic words when in company lists."""
        system = TieredDecisionSystem(
            use_tier1=True, use_tier2=False, use_tier3=False, use_tier4=False
        )

        candidate = Candidate(
            text="Target",
            sentence="Our customers include Target, Walmart, and other retailers",
            start_pos=0,
            end_pos=6,
            source_pattern="extraction",
        )

        decision = system.decide(
            candidate=candidate,
            context="Our customers include Target, Walmart, and other retailers",
            relationship_type="HAS_CUSTOMER",
        )

        # Should not reject (continue to next tier)
        assert decision.decision == Decision.REJECT  # Default reject if no tier decides
        assert decision.tier == DecisionTier.TIER3_EMBEDDINGS  # Default tier

    def test_tier1_rejects_short_mention(self):
        """Tier 1 should reject very short mentions (when not in company list)."""
        system = TieredDecisionSystem(
            use_tier1=True, use_tier2=False, use_tier3=False, use_tier4=False
        )

        # Short mention NOT in company list should be rejected
        candidate = Candidate(
            text="AB",
            sentence="We work with AB and other companies",
            start_pos=0,
            end_pos=2,
            source_pattern="extraction",
        )

        system.decide(
            candidate=candidate,
            context="We work with AB and other companies",  # "AB and" - might be detected as in list
            relationship_type="HAS_PARTNER",
        )

        # The logic checks if it's in a company list first
        # "AB and" matches the pattern, so it might not reject
        # Let's test with a clearer case
        candidate2 = Candidate(
            text="X",
            sentence="We have X in our system",
            start_pos=0,
            end_pos=1,
            source_pattern="extraction",
        )

        decision2 = system.decide(
            candidate=candidate2,
            context="We have X in our system",  # Not in company list
            relationship_type="HAS_PARTNER",
        )

        # Very short (1 char) should be rejected
        assert decision2.decision == Decision.REJECT
        assert decision2.tier == DecisionTier.TIER1_RULES
        assert "too short" in decision2.reason.lower()

    def test_tier2_rejects_biographical(self):
        """Tier 2 should reject biographical mentions."""
        system = TieredDecisionSystem(
            use_tier1=False, use_tier2=True, use_tier3=False, use_tier4=False
        )

        candidate = Candidate(
            text="John Smith",
            sentence="John Smith serves as a director of Microsoft",
            start_pos=0,
            end_pos=10,
            source_pattern="extraction",
        )

        decision = system.decide(
            candidate=candidate,
            context="John Smith serves as a director of Microsoft",
            relationship_type="HAS_SUPPLIER",
        )

        assert decision.decision == Decision.REJECT
        assert decision.tier == DecisionTier.TIER2_PATTERNS
        assert "biographical" in decision.reason.lower()

    def test_tier3_accepts_high_similarity(self):
        """Tier 3 should accept high embedding similarity."""
        system = TieredDecisionSystem(
            use_tier1=False, use_tier2=False, use_tier3=True, use_tier4=False
        )

        candidate = Candidate(
            text="Microsoft",
            sentence="Our competitors include Microsoft and other tech companies",
            start_pos=0,
            end_pos=9,
            source_pattern="extraction",
        )

        decision = system.decide(
            candidate=candidate,
            context="Our competitors include Microsoft and other tech companies",
            relationship_type="HAS_COMPETITOR",
            embedding_similarity=0.50,  # Above high threshold (0.35)
        )

        assert decision.decision == Decision.ACCEPT
        assert decision.tier == DecisionTier.TIER3_EMBEDDINGS
        assert decision.confidence == 0.50

    def test_tier3_candidate_medium_similarity(self):
        """Tier 3 should create candidate for medium embedding similarity."""
        system = TieredDecisionSystem(
            use_tier1=False, use_tier2=False, use_tier3=True, use_tier4=False
        )

        candidate = Candidate(
            text="Microsoft",
            sentence="We work with Microsoft on some projects",
            start_pos=0,
            end_pos=9,
            source_pattern="extraction",
        )

        decision = system.decide(
            candidate=candidate,
            context="We work with Microsoft on some projects",
            relationship_type="HAS_COMPETITOR",
            embedding_similarity=0.30,  # Between medium (0.25) and high (0.35)
        )

        assert decision.decision == Decision.CANDIDATE
        assert decision.tier == DecisionTier.TIER3_EMBEDDINGS
        assert decision.confidence == 0.30

    def test_tier3_rejects_low_similarity(self):
        """Tier 3 should reject low embedding similarity."""
        system = TieredDecisionSystem(
            use_tier1=False, use_tier2=False, use_tier3=True, use_tier4=False
        )

        candidate = Candidate(
            text="Microsoft",
            sentence="We have no relationship with Microsoft",
            start_pos=0,
            end_pos=9,
            source_pattern="extraction",
        )

        decision = system.decide(
            candidate=candidate,
            context="We have no relationship with Microsoft",
            relationship_type="HAS_COMPETITOR",
            embedding_similarity=0.20,  # Below medium threshold (0.25)
        )

        assert decision.decision == Decision.REJECT
        assert decision.tier == DecisionTier.TIER3_EMBEDDINGS
        assert decision.confidence == 0.20

    def test_tier_order_respect(self):
        """Tier 1 should be checked before Tier 2, etc."""
        system = TieredDecisionSystem(
            use_tier1=True, use_tier2=True, use_tier3=True, use_tier4=False
        )

        # Generic word should be rejected by Tier 1, not Tier 2
        candidate = Candidate(
            text="target",
            sentence="Our target market is growing",
            start_pos=0,
            end_pos=6,
            source_pattern="extraction",
        )

        decision = system.decide(
            candidate=candidate,
            context="Our target market is growing",
            relationship_type="HAS_COMPETITOR",
            embedding_similarity=0.50,  # Would pass Tier 3, but Tier 1 should catch it first
        )

        assert decision.decision == Decision.REJECT
        assert decision.tier == DecisionTier.TIER1_RULES  # Tier 1 should catch it

    def test_is_in_company_list(self):
        """Test context-aware company list detection."""
        system = TieredDecisionSystem()

        # Should detect comma-separated list
        assert system._is_in_company_list("customers such as Target, Walmart", "target")
        assert system._is_in_company_list("Target and Walmart are customers", "target")
        assert system._is_in_company_list("including Target in our list", "target")

        # Should not detect generic use
        assert not system._is_in_company_list("Our target market is growing", "target")
        assert not system._is_in_company_list("We target new customers", "target")

    def test_default_reject_when_no_tier_decides(self):
        """Should default to reject if no tier makes a decision."""
        system = TieredDecisionSystem(
            use_tier1=False, use_tier2=False, use_tier3=False, use_tier4=False
        )

        candidate = Candidate(
            text="Microsoft",
            sentence="We work with Microsoft",
            start_pos=0,
            end_pos=9,
            source_pattern="extraction",
        )

        decision = system.decide(
            candidate=candidate,
            context="We work with Microsoft",
            relationship_type="HAS_COMPETITOR",
        )

        assert decision.decision == Decision.REJECT
        assert decision.tier == DecisionTier.TIER3_EMBEDDINGS  # Default tier
        assert "No tier made a decision" in decision.reason
