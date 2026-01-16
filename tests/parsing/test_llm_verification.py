"""Tests for LLM relationship verification."""

from unittest.mock import Mock

import pytest

from public_company_graph.parsing.llm_verification import (
    RELATIONSHIP_DESCRIPTIONS,
    LLMRelationshipVerifier,
    LLMVerificationResult,
    VerificationResult,
    estimate_verification_cost,
)


class TestVerificationResult:
    """Tests for VerificationResult enum."""

    def test_result_values(self):
        """Verify all expected result values exist."""
        assert VerificationResult.CONFIRMED.value == "confirmed"
        assert VerificationResult.REJECTED.value == "rejected"
        assert VerificationResult.UNCERTAIN.value == "uncertain"


class TestLLMVerificationResult:
    """Tests for LLMVerificationResult dataclass."""

    def test_result_creation(self):
        """Test creating a verification result."""
        result = LLMVerificationResult(
            result=VerificationResult.CONFIRMED,
            confidence=0.95,
            explanation="Clear supplier relationship mentioned.",
            suggested_relationship=None,
            cost_tokens=150,
        )

        assert result.result == VerificationResult.CONFIRMED
        assert result.confidence == 0.95
        assert "supplier" in result.explanation.lower()
        assert result.cost_tokens == 150


class TestRelationshipDescriptions:
    """Tests for relationship type descriptions."""

    def test_all_types_have_descriptions(self):
        """All relationship types should have descriptions."""
        expected_types = ["HAS_SUPPLIER", "HAS_CUSTOMER", "HAS_COMPETITOR", "HAS_PARTNER"]
        for rel_type in expected_types:
            assert rel_type in RELATIONSHIP_DESCRIPTIONS
            assert len(RELATIONSHIP_DESCRIPTIONS[rel_type]) > 0


class TestLLMRelationshipVerifier:
    """Tests for LLMRelationshipVerifier."""

    @pytest.fixture
    def mock_client(self):
        """Create a mock OpenAI client."""
        client = Mock()

        def mock_chat_create(model, messages, temperature, max_tokens):
            mock_response = Mock()
            mock_choice = Mock()
            mock_message = Mock()
            mock_usage = Mock()

            # Return a valid JSON response
            mock_message.content = (
                '{"verified": true, "confidence": 0.9, '
                '"explanation": "Clear supplier relationship.", '
                '"actual_relationship": "HAS_SUPPLIER"}'
            )
            mock_choice.message = mock_message
            mock_response.choices = [mock_choice]
            mock_usage.total_tokens = 200
            mock_response.usage = mock_usage
            return mock_response

        client.chat.completions.create = mock_chat_create
        return client

    def test_verify_returns_result(self, mock_client):
        """Verify should return an LLMVerificationResult."""
        verifier = LLMRelationshipVerifier(client=mock_client)

        result = verifier.verify(
            context="We purchase components from Intel Corporation.",
            source_company="ACME Corp",
            target_company="Intel",
            claimed_relationship="HAS_SUPPLIER",
        )

        assert isinstance(result, LLMVerificationResult)
        assert result.result == VerificationResult.CONFIRMED
        assert result.confidence >= 0.7

    def test_cache_key_generation(self, mock_client):
        """Cache keys should be consistent for same inputs."""
        verifier = LLMRelationshipVerifier(client=mock_client)

        key1 = verifier._get_cache_key(
            context="Test context",
            source_company="Company A",
            target_company="Company B",
            claimed_relationship="HAS_SUPPLIER",
        )

        key2 = verifier._get_cache_key(
            context="Test context",
            source_company="Company A",
            target_company="Company B",
            claimed_relationship="HAS_SUPPLIER",
        )

        assert key1 == key2

    def test_different_inputs_different_keys(self, mock_client):
        """Different inputs should produce different cache keys."""
        verifier = LLMRelationshipVerifier(client=mock_client)

        key1 = verifier._get_cache_key(
            context="Context A",
            source_company="Company A",
            target_company="Company B",
            claimed_relationship="HAS_SUPPLIER",
        )

        key2 = verifier._get_cache_key(
            context="Context B",  # Different
            source_company="Company A",
            target_company="Company B",
            claimed_relationship="HAS_SUPPLIER",
        )

        assert key1 != key2

    def test_cache_stats(self, mock_client):
        """Cache stats should return namespace info."""
        from unittest.mock import patch

        from public_company_graph.cache import get_cache

        verifier = LLMRelationshipVerifier(client=mock_client)

        # Mock cache.count() to avoid iterating through all keys (expensive)
        with patch.object(get_cache(), "count", return_value=42) as mock_count:
            stats = verifier.cache_stats()

            assert "namespace" in stats
            assert stats["namespace"] == "llm_verification"
            assert "count" in stats
            assert stats["count"] == 42
            # Verify cache.count was called with the correct namespace
            mock_count.assert_called_once_with("llm_verification")


class TestCostEstimation:
    """Tests for cost estimation."""

    def test_estimate_returns_dict(self):
        """Estimate should return a dict with cost info."""
        estimate = estimate_verification_cost(1000)

        assert "model" in estimate
        assert "num_relationships" in estimate
        assert "estimated_cost_usd" in estimate
        assert "per_relationship_cost" in estimate

    def test_estimate_scales_with_count(self):
        """Cost should scale with number of relationships."""
        estimate_100 = estimate_verification_cost(100)
        estimate_1000 = estimate_verification_cost(1000)

        # Cost should be roughly 10x for 10x relationships
        assert estimate_1000["estimated_cost_usd"] > estimate_100["estimated_cost_usd"] * 5

    def test_estimate_default_model(self):
        """Default model should be gpt-4.1-mini."""
        estimate = estimate_verification_cost(100)
        assert estimate["model"] == "gpt-4.1-mini"

    def test_per_relationship_cost_reasonable(self):
        """Per-relationship cost should be reasonable (< $0.01)."""
        estimate = estimate_verification_cost(100)
        assert estimate["per_relationship_cost"] < 0.01
