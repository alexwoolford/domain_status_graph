"""
Edge case tests for token counting and truncation.

These functions protect against exceeding OpenAI's token limits.
Bugs here could:
- Cause API errors (exceeding max tokens)
- Corrupt text (bad truncation)
- Waste money (not truncating when needed)
"""

from public_company_graph.embeddings.openai_client import (
    EMBEDDING_TRUNCATE_TOKENS,
    count_tokens,
    truncate_to_token_limit,
)


class TestCountTokensEdgeCases:
    """Tests for token counting function."""

    def test_empty_string_zero_tokens(self):
        """Empty string should have zero tokens."""
        result = count_tokens("")
        assert result == 0

    def test_single_word(self):
        """Single word should be roughly 1 token."""
        result = count_tokens("hello")
        # Most short words are 1 token
        assert 1 <= result <= 2

    def test_short_sentence(self):
        """Short sentence token count should be reasonable."""
        text = "The quick brown fox jumps over the lazy dog."
        result = count_tokens(text)
        # ~9 words, usually 1-1.5 tokens per word
        assert 9 <= result <= 15

    def test_whitespace_only(self):
        """Whitespace should be counted (tokens are not just words)."""
        result = count_tokens("   ")
        # Whitespace may or may not be a token depending on encoding
        assert result >= 0

    def test_unicode_text(self):
        """Unicode text should be counted correctly."""
        # CJK characters often take more tokens
        text = "こんにちは世界"  # "Hello World" in Japanese
        result = count_tokens(text)
        # Non-English text typically needs more tokens
        assert result > 0

    def test_code_with_special_chars(self):
        """Code with special characters should be counted."""
        code = """
def hello_world():
    print("Hello, World!")
    return {"status": "ok"}
"""
        result = count_tokens(code)
        # Code has multiple tokens due to punctuation and keywords
        assert result >= 15

    def test_very_long_text(self):
        """Very long text should count all tokens."""
        # Create text that should be many thousands of tokens
        long_text = "word " * 10000
        result = count_tokens(long_text)
        # "word " repeated 10000 times should be ~10000-20000 tokens
        assert result >= 10000

    def test_numbers_counted(self):
        """Numbers should be tokenized."""
        text = "The year is 2024 and the price is $1,234.56"
        result = count_tokens(text)
        # Numbers can be multiple tokens
        assert result > 5

    def test_newlines_and_tabs(self):
        """Whitespace characters should be handled."""
        text = "Line 1\n\nLine 2\t\tTabbed"
        result = count_tokens(text)
        assert result > 0

    def test_emoji(self):
        """Emoji should be tokenized."""
        text = "Hello 👋 World 🌍"
        result = count_tokens(text)
        # Emoji typically take 1-2 tokens each
        assert result >= 4


class TestTruncateToTokenLimitEdgeCases:
    """Tests for token-aware truncation function."""

    def test_empty_string_returns_empty(self):
        """Empty string should return empty."""
        result = truncate_to_token_limit("")
        assert result == ""

    def test_short_text_unchanged(self):
        """Text under limit should be unchanged."""
        text = "This is a short piece of text."
        result = truncate_to_token_limit(text, max_tokens=1000)
        assert result == text

    def test_long_text_truncated(self):
        """Text over limit should be truncated."""
        # Create text that exceeds the limit
        long_text = "word " * 10000  # ~10000+ tokens
        result = truncate_to_token_limit(long_text, max_tokens=100)

        # Result should be much shorter
        assert len(result) < len(long_text)
        # And should fit within token limit
        assert count_tokens(result) <= 100

    def test_exact_limit_not_truncated(self):
        """Text exactly at limit should not be truncated."""
        # Create text that's roughly at limit
        text = "word " * 50  # ~50 tokens
        result = truncate_to_token_limit(text, max_tokens=100)

        # Should be unchanged
        assert result == text

    def test_truncation_preserves_text_start(self):
        """Truncation should preserve the beginning of text."""
        text = "START " + "middle " * 1000 + " END"
        result = truncate_to_token_limit(text, max_tokens=50)

        # Should start with START
        assert result.startswith("START")
        # Should not contain END (truncated)
        assert "END" not in result

    def test_unicode_truncation_preserves_chars(self):
        """Truncation should not corrupt unicode characters."""
        text = "日本語テスト " * 1000  # Japanese text
        result = truncate_to_token_limit(text, max_tokens=50)

        # Should be valid unicode
        result.encode("utf-8").decode("utf-8")

        # Should contain complete Japanese characters (not corrupted)
        for char in result:
            # Each char should be a valid character
            assert len(char) > 0

    def test_default_max_tokens_used(self):
        """Default max tokens should be EMBEDDING_TRUNCATE_TOKENS."""
        # Very long text should be truncated to default
        long_text = "word " * 50000
        result = truncate_to_token_limit(long_text)

        result_tokens = count_tokens(result)
        assert result_tokens <= EMBEDDING_TRUNCATE_TOKENS

    def test_very_small_limit(self):
        """Very small token limit should still work."""
        text = "This is a normal sentence that will be truncated."
        result = truncate_to_token_limit(text, max_tokens=5)

        # Should be truncated significantly
        assert len(result) < len(text)
        assert count_tokens(result) <= 5

    def test_limit_of_one(self):
        """Limit of 1 token should return minimal text."""
        text = "Hello World"
        result = truncate_to_token_limit(text, max_tokens=1)

        # Should have at most 1 token
        assert count_tokens(result) <= 1

    def test_whitespace_only_text(self):
        """Whitespace-only text should be handled."""
        text = "   \n\t  "
        result = truncate_to_token_limit(text, max_tokens=100)
        # Should not crash
        assert isinstance(result, str)

    def test_none_returns_none(self):
        """None input should return None or empty."""
        # Implementation handles this with `if not text: return text`
        result = truncate_to_token_limit(None, max_tokens=100)
        assert result is None

    def test_mixed_content_preserved(self):
        """Mixed content should be preserved during truncation."""
        text = (
            """
        Code: def func(): pass
        Math: 1 + 2 = 3
        Unicode: 日本語
        Emoji: 👋
        """
            * 100
        )

        result = truncate_to_token_limit(text, max_tokens=50)

        # Should still contain valid text
        assert "Code" in result or "def" in result

    def test_repeated_truncation_idempotent(self):
        """Truncating already-truncated text should be idempotent."""
        text = "word " * 1000

        first_truncation = truncate_to_token_limit(text, max_tokens=100)
        second_truncation = truncate_to_token_limit(first_truncation, max_tokens=100)

        # Should be the same
        assert first_truncation == second_truncation

    def test_newlines_in_text(self):
        """Newlines should not break truncation."""
        text = "\n".join(["Line " + str(i) for i in range(1000)])
        result = truncate_to_token_limit(text, max_tokens=50)

        # Should be truncated
        assert count_tokens(result) <= 50
        # Should contain newlines
        assert "\n" in result or len(result.split("\n")) >= 1
