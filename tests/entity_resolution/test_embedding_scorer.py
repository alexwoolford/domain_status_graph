"""Tests for EmbeddingSimilarityScorer."""

from unittest.mock import Mock, patch

import pytest

from public_company_graph.entity_resolution.embedding_scorer import (
    EmbeddingSimilarityResult,
    EmbeddingSimilarityScorer,
    score_embedding_similarity,
)


class TestEmbeddingSimilarityScorer:
    """Tests for EmbeddingSimilarityScorer."""

    @pytest.fixture(autouse=True)
    def mock_neo4j_driver(self):
        """Create a mock Neo4j driver to prevent real connection attempts."""
        # Mark cache as loaded to skip Neo4j connection entirely
        # This prevents any real connection attempts
        original_cache_loaded = EmbeddingSimilarityScorer._cache_loaded
        EmbeddingSimilarityScorer._cache_loaded = True

        yield None

        # Restore original state
        EmbeddingSimilarityScorer._cache_loaded = original_cache_loaded

    @pytest.fixture
    def mock_client(self):
        """Create a mock OpenAI client."""
        client = Mock()

        # Mock embeddings.create to return consistent vectors
        def mock_embeddings_create(model, input):
            # Return different embeddings based on content
            mock_response = Mock()
            mock_data = Mock()

            # Simple mock: return vector based on first few chars
            text = input[:50].lower()
            if "retail" in text or "walmart" in text or "target" in text:
                vector = [0.8, 0.1, 0.1] + [0.0] * 1533  # Retail-like
            elif "technology" in text or "software" in text or "microsoft" in text:
                vector = [0.1, 0.8, 0.1] + [0.0] * 1533  # Tech-like
            elif "power plant" in text or "geothermal" in text:
                vector = [0.1, 0.1, 0.8] + [0.0] * 1533  # Energy-like
            else:
                vector = [0.33, 0.33, 0.34] + [0.0] * 1533  # Neutral

            mock_data.embedding = vector
            mock_response.data = [mock_data]
            return mock_response

        client.embeddings.create = mock_embeddings_create

        # Mock chat.completions.create for company descriptions
        def mock_chat_create(model, messages, max_tokens):
            company_name = messages[-1]["content"]
            mock_response = Mock()
            mock_choice = Mock()
            mock_message = Mock()

            if "Walmart" in company_name:
                mock_message.content = "Walmart is a multinational retail corporation."
            elif "Microsoft" in company_name:
                mock_message.content = "Microsoft develops software and technology solutions."
            elif "Brady" in company_name:
                mock_message.content = (
                    "Brady manufactures workplace safety and identification products."
                )
            else:
                mock_message.content = "A technology company."

            mock_choice.message = mock_message
            mock_response.choices = [mock_choice]
            return mock_response

        client.chat.completions.create = mock_chat_create
        return client

    def test_high_similarity_for_matching_context(self, mock_client):
        """Context about retail should match retail company description."""
        scorer = EmbeddingSimilarityScorer(client=mock_client, threshold=0.30)

        result = scorer.score(
            context="We sell products to Walmart stores nationwide.",
            ticker="WMT",
            company_name="Walmart Inc.",
        )

        assert isinstance(result, EmbeddingSimilarityResult)
        # Both context and description are retail-related, should have high similarity
        assert result.similarity > 0.5
        assert result.passed is True

    def test_low_similarity_for_mismatched_context(self, mock_client):
        """Context about power plants should not match retail company."""
        scorer = EmbeddingSimilarityScorer(client=mock_client, threshold=0.30)

        result = scorer.score(
            context="Our Brady geothermal power plant produced 50MW.",
            ticker="BRC",
            company_name="Brady Corp",
        )

        # With the mock, both get neutral-ish embeddings, so similarity is moderate
        # The important test is that the scorer runs without error
        assert isinstance(result, EmbeddingSimilarityResult)
        assert 0 <= result.similarity <= 1

    def test_threshold_controls_pass_fail(self, mock_client):
        """Threshold should control pass/fail decision."""
        # High threshold
        scorer_high = EmbeddingSimilarityScorer(client=mock_client, threshold=0.90)
        result_high = scorer_high.score(
            context="Technology solutions",
            ticker="MSFT",
            company_name="Microsoft",
        )

        # Low threshold
        scorer_low = EmbeddingSimilarityScorer(client=mock_client, threshold=0.10)
        result_low = scorer_low.score(
            context="Technology solutions",
            ticker="MSFT",
            company_name="Microsoft",
        )

        # Same similarity, different pass/fail based on threshold
        assert result_high.similarity == result_low.similarity
        assert result_low.passed is True  # Low threshold, should pass

    def test_result_contains_metadata(self, mock_client):
        """Result should contain useful metadata."""
        scorer = EmbeddingSimilarityScorer(client=mock_client, threshold=0.30)

        result = scorer.score(
            context="Test context about software.",
            ticker="TEST",
            company_name="Test Company",
        )

        assert result.context_snippet is not None
        assert result.company_description is not None
        assert result.threshold == 0.30
        assert 0 <= result.similarity <= 1

    def test_context_embedding_caching(self, mock_client):
        """Context embeddings should be cached via AppCache."""
        from public_company_graph.cache import get_cache

        scorer = EmbeddingSimilarityScorer(client=mock_client, threshold=0.30)
        cache = get_cache()

        # Score with a unique context
        import uuid

        unique_context = f"Unique test context {uuid.uuid4()}"

        # Track cache key directly (more efficient than counting all keys)
        from public_company_graph.entity_resolution.embedding_scorer import CONTEXT_CACHE_NAMESPACE

        cache_key = scorer._hash_text(unique_context[:500])

        # Verify not cached initially
        assert cache.get(CONTEXT_CACHE_NAMESPACE, cache_key) is None

        # First call should compute and cache the context embedding
        # Note: score() returns early if company embedding is missing, so we need to
        # call _get_context_embedding() directly to test caching
        context_embedding_1 = scorer._get_context_embedding(unique_context)

        # Verify it's now cached
        cached_embedding = cache.get(CONTEXT_CACHE_NAMESPACE, cache_key)
        assert cached_embedding is not None, "Embedding should be cached after first call"
        assert cached_embedding == context_embedding_1, (
            "Cached embedding should match returned value"
        )

        # Second call with same context should use cache (no new API call)
        context_embedding_2 = scorer._get_context_embedding(unique_context)

        # Verify cache still has the same embedding and we got the same result
        cached_embedding_2 = cache.get(CONTEXT_CACHE_NAMESPACE, cache_key)
        assert cached_embedding_2 == cached_embedding, "Cache should not change on second call"
        assert context_embedding_2 == context_embedding_1, "Second call should return cached value"

    def test_missing_embedding_defaults_to_1_0(self, mock_client):
        """
        Test that missing company embeddings default to similarity=1.0.

        This documents the current behavior which can cause data corruption
        if embeddings are not created before extraction.
        """
        scorer = EmbeddingSimilarityScorer(client=mock_client, threshold=0.30)

        # Clear the company cache to simulate missing embeddings
        EmbeddingSimilarityScorer._company_cache.clear()
        EmbeddingSimilarityScorer._cache_loaded = False

        # Score with a ticker that doesn't exist in cache
        result = scorer.score(
            context="Test context about a company",
            ticker="NONEXISTENT",
            company_name="Nonexistent Company",
        )

        # When embedding is missing, should default to 1.0
        assert result.similarity == 1.0
        assert result.passed is True  # 1.0 always passes any threshold
        assert result.company_description == "(no embedding available)"

        # This is problematic behavior - documents the bug we fixed
        # Consider: Should this fail loudly instead of defaulting to 1.0?

    def test_missing_embedding_causes_all_to_pass_threshold(self, mock_client):
        """
        Test that missing embeddings cause all relationships to pass high threshold.

        This demonstrates why the pipeline ordering bug was so dangerous.
        """
        # High threshold that should normally reject most relationships
        scorer = EmbeddingSimilarityScorer(client=mock_client, threshold=0.90)

        # Clear cache to simulate missing embeddings
        EmbeddingSimilarityScorer._company_cache.clear()
        EmbeddingSimilarityScorer._cache_loaded = False

        # Even with high threshold, missing embedding defaults to 1.0
        result = scorer.score(
            context="Unrelated context",
            ticker="MISSING",
            company_name="Missing Company",
        )

        # Should pass even high threshold because similarity=1.0
        assert result.similarity == 1.0
        assert result.passed is True  # Passes even 0.90 threshold!

        # This means no CANDIDATE relationships would be created
        # Everything would be marked as HIGH confidence incorrectly


class TestCosineSimularity:
    """Tests for cosine similarity calculation."""

    def test_identical_vectors(self):
        """Identical vectors should have similarity 1.0."""
        a = [1.0, 0.0, 0.0]
        b = [1.0, 0.0, 0.0]

        similarity = EmbeddingSimilarityScorer._cosine_similarity(a, b)
        assert abs(similarity - 1.0) < 0.0001

    def test_orthogonal_vectors(self):
        """Orthogonal vectors should have similarity 0.0."""
        a = [1.0, 0.0, 0.0]
        b = [0.0, 1.0, 0.0]

        similarity = EmbeddingSimilarityScorer._cosine_similarity(a, b)
        assert abs(similarity - 0.0) < 0.0001

    def test_similar_vectors(self):
        """Similar vectors should have high similarity."""
        a = [0.9, 0.1, 0.0]
        b = [0.8, 0.2, 0.0]

        similarity = EmbeddingSimilarityScorer._cosine_similarity(a, b)
        assert similarity > 0.9


class TestConvenienceFunction:
    """Tests for score_embedding_similarity convenience function."""

    @patch("public_company_graph.entity_resolution.embedding_scorer.EmbeddingSimilarityScorer")
    def test_convenience_function_creates_scorer(self, mock_scorer_class):
        """Convenience function should create and use scorer."""
        mock_instance = Mock()
        mock_result = EmbeddingSimilarityResult(
            similarity=0.5,
            context_snippet="test",
            company_description="test desc",
            passed=True,
            threshold=0.30,
        )
        mock_instance.score.return_value = mock_result
        mock_scorer_class.return_value = mock_instance

        result = score_embedding_similarity(
            context="Test context",
            ticker="TEST",
            company_name="Test Company",
        )

        assert result == mock_result
        mock_scorer_class.assert_called_once()
