"""
Edge case tests for Neo4j utility functions.

Focus areas:
- Relationship type validation (security-critical for Cypher injection prevention)
- Batch deletion edge cases
- Error handling for connection failures
"""

import pytest

from public_company_graph.neo4j.utils import (
    REL_TYPE_PATTERN,
    _validate_relationship_type,
)


class TestRelationshipTypeValidation:
    """
    Tests for relationship type validation.

    This is SECURITY-CRITICAL: relationship types are interpolated into
    Cypher queries. Invalid types could enable injection attacks.
    """

    # === Valid relationship types ===

    def test_valid_simple_type(self):
        """Simple uppercase type should be valid."""
        _validate_relationship_type("USES")
        _validate_relationship_type("HAS_MX")
        _validate_relationship_type("BELONGS_TO")

    def test_valid_with_numbers(self):
        """Types with numbers should be valid."""
        _validate_relationship_type("HAS_IP4")
        _validate_relationship_type("USES_V2")
        _validate_relationship_type("TYPE123")

    def test_valid_with_underscores(self):
        """Types with underscores should be valid."""
        _validate_relationship_type("LIKELY_TO_ADOPT")
        _validate_relationship_type("HAS_SIMILAR_TECH")
        _validate_relationship_type("A_B_C_D")

    def test_valid_single_letter(self):
        """Single uppercase letter is technically valid."""
        _validate_relationship_type("A")
        _validate_relationship_type("Z")

    # === Invalid relationship types (must raise) ===

    def test_lowercase_rejected(self):
        """Lowercase types should be rejected."""
        with pytest.raises(ValueError, match="Invalid relationship type"):
            _validate_relationship_type("uses")

    def test_mixed_case_rejected(self):
        """Mixed case types should be rejected."""
        with pytest.raises(ValueError, match="Invalid relationship type"):
            _validate_relationship_type("Uses")
        with pytest.raises(ValueError, match="Invalid relationship type"):
            _validate_relationship_type("USES_tech")

    def test_starting_with_number_rejected(self):
        """Types starting with numbers should be rejected."""
        with pytest.raises(ValueError, match="Invalid relationship type"):
            _validate_relationship_type("123TYPE")
        with pytest.raises(ValueError, match="Invalid relationship type"):
            _validate_relationship_type("1A")

    def test_starting_with_underscore_rejected(self):
        """Types starting with underscore should be rejected."""
        with pytest.raises(ValueError, match="Invalid relationship type"):
            _validate_relationship_type("_USES")
        with pytest.raises(ValueError, match="Invalid relationship type"):
            _validate_relationship_type("_")

    def test_empty_string_rejected(self):
        """Empty string should be rejected."""
        with pytest.raises(ValueError, match="Invalid relationship type"):
            _validate_relationship_type("")

    def test_space_in_type_rejected(self):
        """Spaces in type should be rejected."""
        with pytest.raises(ValueError, match="Invalid relationship type"):
            _validate_relationship_type("USES THIS")
        with pytest.raises(ValueError, match="Invalid relationship type"):
            _validate_relationship_type(" USES")

    # === Security-critical injection attempts ===

    def test_cypher_injection_semicolon_rejected(self):
        """
        Cypher injection attempt with semicolon should be rejected.

        Attack: USES; MATCH (n) DELETE n
        """
        with pytest.raises(ValueError, match="Invalid relationship type"):
            _validate_relationship_type("USES; MATCH (n) DELETE n")

    def test_cypher_injection_bracket_rejected(self):
        """
        Cypher injection attempt with brackets should be rejected.

        Attack: USES]->() DELETE r//
        """
        with pytest.raises(ValueError, match="Invalid relationship type"):
            _validate_relationship_type("USES]->() DELETE r//")

    def test_cypher_injection_dash_rejected(self):
        """
        Cypher injection with dash (common in Cypher) should be rejected.

        Attack: USES-[:ADMIN]->
        """
        with pytest.raises(ValueError, match="Invalid relationship type"):
            _validate_relationship_type("USES-[:ADMIN]->")

    def test_newline_injection_rejected(self):
        """Newline characters should be rejected."""
        with pytest.raises(ValueError, match="Invalid relationship type"):
            _validate_relationship_type("USES\nMATCH")

    def test_comment_injection_rejected(self):
        """Comment syntax should be rejected."""
        with pytest.raises(ValueError, match="Invalid relationship type"):
            _validate_relationship_type("USES//comment")
        with pytest.raises(ValueError, match="Invalid relationship type"):
            _validate_relationship_type("USES/*comment*/")

    def test_quote_injection_rejected(self):
        """Quote characters should be rejected."""
        with pytest.raises(ValueError, match="Invalid relationship type"):
            _validate_relationship_type("USES'")
        with pytest.raises(ValueError, match="Invalid relationship type"):
            _validate_relationship_type('USES"')

    def test_backtick_injection_rejected(self):
        """Backtick (Neo4j identifier escape) should be rejected."""
        with pytest.raises(ValueError, match="Invalid relationship type"):
            _validate_relationship_type("USES`")
        with pytest.raises(ValueError, match="Invalid relationship type"):
            _validate_relationship_type("`USES`")

    def test_dollar_sign_rejected(self):
        """Dollar sign (parameter syntax) should be rejected."""
        with pytest.raises(ValueError, match="Invalid relationship type"):
            _validate_relationship_type("$USES")
        with pytest.raises(ValueError, match="Invalid relationship type"):
            _validate_relationship_type("USES$param")

    def test_curly_brace_rejected(self):
        """Curly braces (map syntax) should be rejected."""
        with pytest.raises(ValueError, match="Invalid relationship type"):
            _validate_relationship_type("USES{}")
        with pytest.raises(ValueError, match="Invalid relationship type"):
            _validate_relationship_type("{USES}")

    def test_parentheses_rejected(self):
        """Parentheses (node syntax) should be rejected."""
        with pytest.raises(ValueError, match="Invalid relationship type"):
            _validate_relationship_type("USES()")
        with pytest.raises(ValueError, match="Invalid relationship type"):
            _validate_relationship_type("(USES)")

    def test_unicode_rejected(self):
        """Unicode characters should be rejected."""
        with pytest.raises(ValueError, match="Invalid relationship type"):
            _validate_relationship_type("USÉS")  # Accented E
        with pytest.raises(ValueError, match="Invalid relationship type"):
            _validate_relationship_type("使用")  # Chinese characters


class TestRelTypePattern:
    """Test the regex pattern directly for completeness."""

    def test_pattern_matches_valid(self):
        """Pattern should match valid types."""
        assert REL_TYPE_PATTERN.match("USES")
        assert REL_TYPE_PATTERN.match("HAS_MX")
        assert REL_TYPE_PATTERN.match("A123_B456")

    def test_pattern_rejects_invalid(self):
        """Pattern should reject invalid types."""
        assert REL_TYPE_PATTERN.match("uses") is None
        assert REL_TYPE_PATTERN.match("123ABC") is None
        assert REL_TYPE_PATTERN.match("_ABC") is None
        assert REL_TYPE_PATTERN.match("") is None


class TestRealWorldRelationshipTypes:
    """
    Test actual relationship types used in the public_company_graph schema.

    These are the relationship types defined in the architecture docs.
    """

    @pytest.mark.parametrize(
        "rel_type",
        [
            "USES",
            "HAS_NAMESERVER",
            "HAS_MX",
            "HOSTED_ON",
            "BELONGS_TO_ASN",
            "SECURED_BY",
            "REDIRECTS_TO",
            "HAS_SOCIAL",
            "SIMILAR_KEYWORD",
            "LIKELY_TO_ADOPT",
        ],
    )
    def test_schema_relationship_types_valid(self, rel_type):
        """All schema relationship types should be valid."""
        _validate_relationship_type(rel_type)  # Should not raise
