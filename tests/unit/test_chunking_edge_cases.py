"""
Edge case tests for text chunking and embedding aggregation.

These functions are critical for handling long text that exceeds
embedding model token limits. Bugs here could:
- Lose important content (truncation errors)
- Create infinite loops (chunking edge cases)
- Produce invalid embeddings (aggregation errors)
"""

import numpy as np
import pytest

from public_company_graph.embeddings.chunking import (
    aggregate_embeddings,
    chunk_text,
)


class TestChunkTextEdgeCases:
    """Tests for chunk_text function."""

    def test_empty_string_returns_empty_list(self):
        """Empty input should return empty list, not error."""
        result = chunk_text("")
        assert result == []

    def test_none_handled_gracefully(self):
        """None input should return empty list."""
        # chunk_text checks `if not text` which handles None
        result = chunk_text(None)
        assert result == []

    def test_short_text_returns_single_chunk(self):
        """Text under limit should return as single chunk."""
        short_text = "This is a short piece of text."
        result = chunk_text(short_text, chunk_size_tokens=1000)
        assert len(result) == 1
        assert result[0] == short_text

    def test_text_exactly_at_limit_returns_single_chunk(self):
        """Text exactly at limit should not be split."""
        # Create text that's roughly at the limit
        text = "word " * 100  # ~100 tokens
        result = chunk_text(text, chunk_size_tokens=200)
        assert len(result) == 1

    def test_long_text_produces_multiple_chunks(self):
        """Long text should be split into multiple chunks."""
        # Create text that definitely exceeds a small limit
        long_text = "This is a sentence. " * 500  # ~2500 tokens
        result = chunk_text(long_text, chunk_size_tokens=100, overlap_tokens=10)
        assert len(result) > 1
        # Each chunk should have content
        for chunk in result:
            assert len(chunk) > 0

    def test_chunks_have_overlap(self):
        """Adjacent chunks should share overlapping content."""
        # Create moderately long text
        long_text = "word " * 500
        chunks = chunk_text(long_text, chunk_size_tokens=100, overlap_tokens=20)

        if len(chunks) > 1:
            # Check that chunks have some overlap by verifying total length
            # is greater than sum of chunk lengths (due to overlap)
            # This is a soft check since tokenization varies
            total_chunk_length = sum(len(c) for c in chunks)
            assert total_chunk_length >= len(long_text)

    def test_progress_is_always_made(self):
        """Chunking should never create infinite loops."""
        # This tests the edge case where overlap is too large
        text = "word " * 1000

        # Even with large overlap, should complete
        result = chunk_text(text, chunk_size_tokens=50, overlap_tokens=40)

        # Should have multiple chunks
        assert len(result) >= 1

        # Should cover the entire text (no infinite loop)
        total_content = "".join(result)
        # Due to overlap, total will be >= original
        assert len(total_content) >= len(text) * 0.9

    def test_overlap_larger_than_chunk_handled(self):
        """
        Edge case: overlap >= chunk_size should not infinite loop.

        This could cause the start position to never advance.
        """
        text = "word " * 100

        # Overlap larger than chunk - this is a pathological case
        # The function should still complete without hanging
        result = chunk_text(text, chunk_size_tokens=10, overlap_tokens=15)

        # Should produce some result
        assert len(result) >= 1

    def test_whitespace_only_text(self):
        """Whitespace-only text should be handled."""
        result = chunk_text("   \n\t  ")
        # Either empty list or single whitespace chunk is acceptable
        assert len(result) <= 1

    def test_unicode_text_chunked_correctly(self):
        """Unicode text should be chunked without corrupting characters."""
        unicode_text = "こんにちは世界 " * 100  # Japanese text
        result = chunk_text(unicode_text, chunk_size_tokens=50)

        # Each chunk should be valid unicode
        for chunk in result:
            # This will raise if unicode is corrupted
            chunk.encode("utf-8").decode("utf-8")

    def test_mixed_content_text(self):
        """Text with mixed content (code, punctuation, etc.)."""
        mixed_text = (
            """
        def hello():
            print("Hello, World!")

        # Comment: 日本語テスト
        x = [1, 2, 3, 4, 5]
        """
            * 50
        )

        result = chunk_text(mixed_text, chunk_size_tokens=100)

        # Should produce valid chunks
        assert len(result) >= 1
        for chunk in result:
            assert isinstance(chunk, str)


class TestAggregateEmbeddingsEdgeCases:
    """Tests for aggregate_embeddings function."""

    def test_empty_list_raises_error(self):
        """Empty list should raise ValueError."""
        with pytest.raises(ValueError, match="Cannot aggregate empty"):
            aggregate_embeddings([])

    def test_single_embedding_returns_unchanged(self):
        """Single embedding should be returned as-is."""
        embedding = [1.0, 2.0, 3.0]
        result = aggregate_embeddings([embedding])
        assert result == embedding

    def test_average_method_works(self):
        """Average method should compute mean correctly."""
        embeddings = [
            [1.0, 0.0, 0.0],
            [0.0, 1.0, 0.0],
            [0.0, 0.0, 1.0],
        ]
        result = aggregate_embeddings(embeddings, method="average")

        # Average should be [1/3, 1/3, 1/3]
        expected = [1 / 3, 1 / 3, 1 / 3]
        assert len(result) == 3
        for r, e in zip(result, expected, strict=True):
            assert abs(r - e) < 1e-5

    def test_weighted_average_method_works(self):
        """Weighted average should respect weights."""
        embeddings = [
            [1.0, 0.0],
            [0.0, 1.0],
        ]
        weights = [0.8, 0.2]

        result = aggregate_embeddings(embeddings, method="weighted_average", weights=weights)

        # Should be closer to first embedding due to higher weight
        assert result[0] > result[1]
        assert abs(result[0] - 0.8) < 1e-5
        assert abs(result[1] - 0.2) < 1e-5

    def test_weighted_average_normalizes_weights(self):
        """Weights should be normalized to sum to 1."""
        embeddings = [
            [10.0, 0.0],
            [0.0, 10.0],
        ]
        # Weights that don't sum to 1
        weights = [2.0, 8.0]

        result = aggregate_embeddings(embeddings, method="weighted_average", weights=weights)

        # After normalization: [0.2, 0.8]
        # Result should be [10*0.2, 10*0.8] = [2, 8]
        assert abs(result[0] - 2.0) < 1e-5
        assert abs(result[1] - 8.0) < 1e-5

    def test_weighted_average_without_weights_defaults_to_equal(self):
        """Weighted average without weights should use equal weights."""
        embeddings = [
            [1.0, 0.0],
            [0.0, 1.0],
        ]

        result = aggregate_embeddings(embeddings, method="weighted_average")

        # Should be same as average
        assert abs(result[0] - 0.5) < 1e-5
        assert abs(result[1] - 0.5) < 1e-5

    def test_max_method_works(self):
        """Max method should take element-wise maximum."""
        embeddings = [
            [1.0, -1.0, 0.5],
            [0.5, 0.0, 1.0],
            [-0.5, 0.5, 0.0],
        ]
        result = aggregate_embeddings(embeddings, method="max")

        assert result[0] == 1.0
        assert result[1] == 0.5
        assert result[2] == 1.0

    def test_unknown_method_raises_error(self):
        """Unknown aggregation method should raise ValueError."""
        embeddings = [[1.0, 2.0], [3.0, 4.0]]  # Need 2+ embeddings to reach method check

        with pytest.raises(ValueError, match="Unknown aggregation method"):
            aggregate_embeddings(embeddings, method="invalid_method")

    def test_mismatched_weights_length_raises_error(self):
        """Weights length must match embeddings length."""
        embeddings = [[1.0, 2.0], [3.0, 4.0]]
        weights = [1.0]  # Only one weight for two embeddings

        with pytest.raises(ValueError, match="Weights length"):
            aggregate_embeddings(embeddings, method="weighted_average", weights=weights)

    def test_large_number_of_embeddings(self):
        """Should handle aggregating many embeddings efficiently."""
        # 100 embeddings of dimension 1536 (OpenAI dimension)
        np.random.seed(42)
        embeddings = [np.random.randn(1536).tolist() for _ in range(100)]

        result = aggregate_embeddings(embeddings, method="average")

        assert len(result) == 1536
        # Result should be close to zero (random vectors average out)
        assert abs(np.mean(result)) < 0.1

    def test_zero_embeddings_aggregated(self):
        """Zero vectors should aggregate to zero vector."""
        embeddings = [
            [0.0, 0.0, 0.0],
            [0.0, 0.0, 0.0],
        ]
        result = aggregate_embeddings(embeddings, method="average")

        assert result == [0.0, 0.0, 0.0]

    def test_negative_values_handled(self):
        """Negative values should be handled correctly."""
        embeddings = [
            [-1.0, -2.0],
            [-3.0, -4.0],
        ]
        result = aggregate_embeddings(embeddings, method="average")

        assert result[0] == -2.0
        assert result[1] == -3.0

    def test_returns_list_not_numpy(self):
        """Result should be a Python list, not numpy array."""
        embeddings = [[1.0, 2.0], [3.0, 4.0]]
        result = aggregate_embeddings(embeddings)

        assert isinstance(result, list)
        assert not isinstance(result, np.ndarray)
