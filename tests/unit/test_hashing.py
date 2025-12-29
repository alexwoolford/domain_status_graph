"""
Unit tests for public_company_graph.utils.hashing module.
"""

from public_company_graph.utils.hashing import compute_text_hash


def test_compute_text_hash():
    """Test text hash computation."""
    text1 = "Hello, world!"
    text2 = "Hello, world!"
    text3 = "Hello, world"

    hash1 = compute_text_hash(text1)
    hash2 = compute_text_hash(text2)
    hash3 = compute_text_hash(text3)

    # Same text should produce same hash
    assert hash1 == hash2
    # Different text should produce different hash
    assert hash1 != hash3
    # Hash should be a hex string
    assert len(hash1) == 64  # SHA256 produces 64-char hex string


def test_compute_text_hash_empty():
    """Test text hash computation with empty string."""
    assert compute_text_hash("") == ""


def test_compute_text_hash_none():
    """Test text hash computation with None."""
    assert compute_text_hash(None) == ""


def test_compute_text_hash_strips_whitespace():
    """Text is stripped before hashing."""
    hash1 = compute_text_hash("hello")
    hash2 = compute_text_hash("  hello  ")
    assert hash1 == hash2
