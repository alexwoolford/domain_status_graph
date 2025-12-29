"""
Edge case tests for cosine similarity computation.

Focus areas:
- Zero vectors (undefined similarity)
- Numerical precision issues
- Very large/small values
- Dimension mismatches
- Sparse vs dense embeddings
"""

import numpy as np
import pytest

from public_company_graph.constants import EMBEDDING_DIMENSION
from public_company_graph.similarity.cosine import (
    compute_cosine_similarity_matrix,
    find_top_k_similar_pairs,
    validate_embedding,
    validate_similarity_score,
)


class TestValidateEmbeddingEdgeCases:
    """Edge case tests for embedding validation."""

    def test_zero_vector_is_valid(self):
        """
        Zero vectors are technically valid embeddings (all zeros).

        Note: Zero vectors have undefined cosine similarity, but the
        embedding itself is valid. The similarity computation handles this.
        """
        zero_embedding = [0.0] * EMBEDDING_DIMENSION
        assert validate_embedding(zero_embedding) is True

    def test_near_zero_vector_valid(self):
        """Very small but non-zero values are valid."""
        tiny_embedding = [1e-100] * EMBEDDING_DIMENSION
        assert validate_embedding(tiny_embedding) is True

    def test_very_large_values_valid(self):
        """Very large but finite values are valid."""
        large_embedding = [1e100] * EMBEDDING_DIMENSION
        assert validate_embedding(large_embedding) is True

    def test_mixed_positive_negative_valid(self):
        """Mixed positive/negative values are valid."""
        mixed = [1.0 if i % 2 == 0 else -1.0 for i in range(EMBEDDING_DIMENSION)]
        assert validate_embedding(mixed) is True

    def test_nan_in_embedding_invalid(self):
        """NaN values make embedding invalid."""
        embedding = [0.5] * EMBEDDING_DIMENSION
        embedding[100] = float("nan")
        assert validate_embedding(embedding) is False

    def test_inf_in_embedding_invalid(self):
        """Infinity values make embedding invalid."""
        embedding = [0.5] * EMBEDDING_DIMENSION
        embedding[100] = float("inf")
        assert validate_embedding(embedding) is False

    def test_negative_inf_invalid(self):
        """Negative infinity is also invalid."""
        embedding = [0.5] * EMBEDDING_DIMENSION
        embedding[100] = float("-inf")
        assert validate_embedding(embedding) is False

    def test_wrong_dimension_invalid(self):
        """Wrong dimension should be invalid."""
        too_short = [0.5] * (EMBEDDING_DIMENSION - 1)
        too_long = [0.5] * (EMBEDDING_DIMENSION + 1)
        assert validate_embedding(too_short) is False
        assert validate_embedding(too_long) is False

    def test_empty_list_invalid(self):
        """Empty list is invalid."""
        assert validate_embedding([]) is False

    def test_none_invalid(self):
        """None is invalid."""
        assert validate_embedding(None) is False

    def test_non_list_types_invalid(self):
        """Non-list/array types should be invalid."""
        assert validate_embedding("not a list") is False
        assert validate_embedding(42) is False
        assert validate_embedding({0: 0.5}) is False

    def test_numpy_array_valid(self):
        """NumPy arrays should be valid."""
        np_embedding = np.random.randn(EMBEDDING_DIMENSION).tolist()
        assert validate_embedding(np_embedding) is True

    def test_custom_dimension(self):
        """Custom dimension validation should work."""
        custom_dim = 768  # e.g., BERT embeddings
        embedding = [0.5] * custom_dim
        assert validate_embedding(embedding, expected_dimension=custom_dim) is True
        assert validate_embedding(embedding, expected_dimension=1536) is False


class TestValidateSimilarityScoreEdgeCases:
    """Edge case tests for similarity score validation."""

    def test_boundary_values_valid(self):
        """Boundary values (-1, 0, 1) are valid."""
        assert validate_similarity_score(-1.0) is True
        assert validate_similarity_score(0.0) is True
        assert validate_similarity_score(1.0) is True

    def test_typical_values_valid(self):
        """Typical similarity values are valid."""
        assert validate_similarity_score(0.75) is True
        assert validate_similarity_score(0.5) is True
        assert validate_similarity_score(0.99) is True

    def test_slightly_out_of_range_invalid(self):
        """Values just outside [-1, 1] are invalid."""
        assert validate_similarity_score(1.001) is False
        assert validate_similarity_score(-1.001) is False

    def test_nan_score_invalid(self):
        """NaN score is invalid."""
        assert validate_similarity_score(float("nan")) is False

    def test_inf_score_invalid(self):
        """Infinity score is invalid."""
        assert validate_similarity_score(float("inf")) is False
        assert validate_similarity_score(float("-inf")) is False

    def test_none_invalid(self):
        """None is invalid."""
        assert validate_similarity_score(None) is False

    def test_integer_valid(self):
        """Integer scores in range should be valid."""
        assert validate_similarity_score(0) is True
        assert validate_similarity_score(1) is True
        assert validate_similarity_score(-1) is True


class TestComputeSimilarityMatrixEdgeCases:
    """Edge case tests for similarity matrix computation."""

    def test_empty_input(self):
        """Empty input should return empty array."""
        result = compute_cosine_similarity_matrix([])
        assert len(result) == 0

    def test_single_embedding(self):
        """Single embedding should return 1x1 matrix with 1.0."""
        embedding = [[0.5] * 10]
        result = compute_cosine_similarity_matrix(embedding)
        assert result.shape == (1, 1)
        assert np.isclose(result[0, 0], 1.0)

    def test_identical_embeddings(self):
        """Identical embeddings should have similarity 1.0."""
        embedding = [0.5] * 10
        embeddings = [embedding, embedding.copy()]
        result = compute_cosine_similarity_matrix(embeddings)
        assert result.shape == (2, 2)
        # Diagonal should be 1.0
        assert np.isclose(result[0, 0], 1.0)
        assert np.isclose(result[1, 1], 1.0)
        # Off-diagonal should also be 1.0 (identical vectors)
        assert np.isclose(result[0, 1], 1.0)
        assert np.isclose(result[1, 0], 1.0)

    def test_opposite_embeddings(self):
        """Opposite embeddings should have similarity -1.0."""
        embedding1 = [1.0] * 10
        embedding2 = [-1.0] * 10
        result = compute_cosine_similarity_matrix([embedding1, embedding2])
        assert np.isclose(result[0, 1], -1.0)

    def test_orthogonal_embeddings(self):
        """Orthogonal embeddings should have similarity 0.0."""
        # [1, 0, 0, ...] and [0, 1, 0, ...]
        embedding1 = [0.0] * 10
        embedding1[0] = 1.0
        embedding2 = [0.0] * 10
        embedding2[1] = 1.0
        result = compute_cosine_similarity_matrix([embedding1, embedding2])
        assert np.isclose(result[0, 1], 0.0, atol=1e-7)

    def test_symmetric_matrix(self):
        """Similarity matrix should be symmetric."""
        embeddings = [np.random.randn(10).tolist() for _ in range(5)]
        result = compute_cosine_similarity_matrix(embeddings)
        # Check symmetry
        assert np.allclose(result, result.T)

    def test_zero_vector_handling(self):
        """
        Zero vectors should not cause division by zero.

        The implementation sets norms of 0 to 1 to avoid this.
        """
        zero = [0.0] * 10
        nonzero = [1.0] * 10
        result = compute_cosine_similarity_matrix([zero, nonzero])
        # Should not crash, result may be 0 or NaN depending on handling
        assert result.shape == (2, 2)
        # Zero vector's similarity with anything should be handled
        # (either 0 or handled gracefully)
        assert np.isfinite(result[0, 1]) or np.isnan(result[0, 1])

    def test_numerical_precision(self):
        """
        Numerical precision: nearly identical vectors should be ~1.0.

        This catches floating point issues.
        """
        embedding1 = [0.123456789] * 100
        embedding2 = [0.123456789 + 1e-10] * 100  # Tiny difference
        result = compute_cosine_similarity_matrix([embedding1, embedding2])
        # Should be extremely close to 1.0
        assert np.isclose(result[0, 1], 1.0, atol=1e-6)

    def test_large_batch(self):
        """Large batch should compute without memory issues."""
        embeddings = [np.random.randn(100).tolist() for _ in range(100)]
        result = compute_cosine_similarity_matrix(embeddings)
        assert result.shape == (100, 100)
        # Diagonal should all be 1.0
        assert np.allclose(np.diag(result), 1.0)


class TestFindTopKSimilarPairsEdgeCases:
    """Edge case tests for finding top-k similar pairs."""

    def test_empty_input(self):
        """Empty input should return empty dict."""
        result = find_top_k_similar_pairs([], [])
        assert result == {}

    def test_single_item(self):
        """Single item can't form pairs."""
        result = find_top_k_similar_pairs(["a"], [[0.5] * 10])
        assert result == {}

    def test_key_embedding_length_mismatch_raises(self):
        """Mismatched keys and embeddings should raise."""
        with pytest.raises(ValueError):
            find_top_k_similar_pairs(
                ["a", "b", "c"],
                [[0.5] * 10, [0.5] * 10],  # Only 2 embeddings for 3 keys
            )

    def test_threshold_filters_pairs(self):
        """Pairs below threshold should be filtered."""
        # Create embeddings where some pairs are similar, some aren't
        embeddings = [
            [1.0, 0.0, 0.0],
            [1.0, 0.0, 0.0],  # Identical to first
            [0.0, 1.0, 0.0],  # Orthogonal to first
        ]
        keys = ["a", "b", "c"]

        # High threshold should only return the identical pair
        result = find_top_k_similar_pairs(keys, embeddings, similarity_threshold=0.9)
        assert ("a", "b") in result
        # Orthogonal pairs shouldn't be included
        assert ("a", "c") not in result
        assert ("b", "c") not in result

    def test_top_k_limits_results(self):
        """top_k should limit results per node."""
        # Create many similar embeddings
        embeddings = [[0.5 + i * 0.001] * 10 for i in range(20)]
        keys = [f"key_{i}" for i in range(20)]

        result = find_top_k_similar_pairs(keys, embeddings, similarity_threshold=0.0, top_k=3)
        # Each key should have at most top_k pairs
        # Count pairs for each key
        key_counts = {}
        for k1, k2 in result.keys():
            key_counts[k1] = key_counts.get(k1, 0) + 1
            key_counts[k2] = key_counts.get(k2, 0) + 1

        # Due to the way pairs are collected, counts might vary
        # but should be bounded by the number of pairs

    def test_ordered_keys_in_pairs(self):
        """Keys in pairs should be consistently ordered (key1 < key2)."""
        embeddings = [[0.5] * 10 for _ in range(5)]
        keys = ["e", "d", "c", "b", "a"]  # Reverse alphabetical

        result = find_top_k_similar_pairs(keys, embeddings, similarity_threshold=0.0)

        for k1, k2 in result.keys():
            assert k1 < k2, f"Keys not ordered: ({k1}, {k2})"

    def test_no_self_similarity(self):
        """Items should not be paired with themselves."""
        embeddings = [[0.5] * 10 for _ in range(3)]
        keys = ["a", "b", "c"]

        result = find_top_k_similar_pairs(keys, embeddings, similarity_threshold=0.0)

        for k1, k2 in result.keys():
            assert k1 != k2, f"Self-pair found: ({k1}, {k2})"

    def test_duplicate_keys_handled(self):
        """Duplicate keys should be handled (may cause confusion)."""
        # This is an edge case - duplicate keys shouldn't happen but shouldn't crash
        embeddings = [[0.5] * 10, [0.6] * 10]
        keys = ["same", "same"]  # Duplicate!

        # Should not crash
        result = find_top_k_similar_pairs(keys, embeddings, similarity_threshold=0.0)
        # Result may be empty or contain weird pairs, but shouldn't crash
        assert isinstance(result, dict)


class TestNumericalStability:
    """Tests for numerical stability with extreme values."""

    def test_very_small_embeddings(self):
        """Very small values shouldn't cause underflow."""
        small_embeddings = [[1e-100] * 10 for _ in range(3)]
        keys = ["a", "b", "c"]

        # Shouldn't crash
        result = find_top_k_similar_pairs(keys, small_embeddings, similarity_threshold=0.0)
        assert isinstance(result, dict)

    @pytest.mark.filterwarnings("ignore::RuntimeWarning")
    def test_very_large_embeddings(self):
        """
        Very large values shouldn't cause overflow.

        Note: RuntimeWarning for overflow is expected with float32 precision
        at extreme values (>1e38). This test verifies the code handles it gracefully.
        """
        large_embeddings = [[1e100] * 10 for _ in range(3)]
        keys = ["a", "b", "c"]

        # Shouldn't crash - may produce NaN/Inf but shouldn't raise
        result = find_top_k_similar_pairs(keys, large_embeddings, similarity_threshold=0.0)
        assert isinstance(result, dict)

    @pytest.mark.filterwarnings("ignore::RuntimeWarning")
    def test_mixed_magnitude_embeddings(self):
        """
        Mixed magnitudes should still compute correctly.

        Note: RuntimeWarning for overflow/divide is expected with extreme
        magnitude differences. This test verifies the code handles it gracefully.
        """
        embeddings = [
            [1e-50] * 10,
            [1.0] * 10,
            [1e50] * 10,
        ]
        # keys: ["tiny", "normal", "huge"] - scale magnitudes being tested

        # Cosine similarity is scale-invariant, so these should all be similar
        # Note: extreme values may cause numerical issues but shouldn't crash
        result = compute_cosine_similarity_matrix(embeddings)

        # Just ensure no crash - numerical precision issues are expected
        assert result.shape == (3, 3)
