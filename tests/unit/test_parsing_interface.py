"""
Unit tests for the pluggable parsing interface.

Tests verify that:
1. The interface works correctly
2. New parsers can be easily added
3. The pattern is repeatable and testable
"""

from pathlib import Path

import pytest

from domain_status_graph.parsing.base import (
    BusinessDescriptionParser,
    CompetitorParser,
    TenKParser,
    WebsiteParser,
    parse_10k_with_parsers,
)


class TestTenKParser:
    """Test the base parser interface."""

    def test_interface_requires_implementation(self):
        """Test that TenKParser cannot be instantiated directly."""
        with pytest.raises(TypeError):
            TenKParser()  # Should fail - abstract class

    def test_parser_field_names_are_distinct(self):
        """Test that each parser has a unique field name."""
        parsers = [WebsiteParser(), BusinessDescriptionParser(), CompetitorParser()]
        field_names = [p.field_name for p in parsers]
        assert len(field_names) == len(set(field_names)), "Parser field names must be unique"

    def test_competitor_parser_returns_list(self):
        """Test that CompetitorParser returns a list (different from other parsers)."""
        parser = CompetitorParser()
        result = parser.extract(Path("/fake/path.html"))
        assert isinstance(result, list)


class TestCustomParser:
    """Test creating a custom parser."""

    def test_custom_parser_implementation(self):
        """Test that a custom parser can be easily created."""

        class TestParser(TenKParser):
            @property
            def field_name(self) -> str:
                return "test_field"

            def extract(self, file_path: Path, file_content=None, **kwargs):
                return "test_value"

        parser = TestParser()
        assert parser.field_name == "test_field"
        assert parser.extract(Path("/fake/path.html")) == "test_value"
        assert parser.validate("test_value") is True


class TestParseWithParsers:
    """Test the parse_10k_with_parsers function."""

    def test_parse_with_multiple_parsers(self):
        """Test that multiple parsers can be used together."""

        class Parser1(TenKParser):
            @property
            def field_name(self) -> str:
                return "field1"

            def extract(self, file_path: Path, file_content=None, **kwargs):
                return "value1"

        class Parser2(TenKParser):
            @property
            def field_name(self) -> str:
                return "field2"

            def extract(self, file_path: Path, file_content=None, **kwargs):
                return "value2"

        parsers = [Parser1(), Parser2()]
        result = parse_10k_with_parsers(
            Path("/fake/path.html"),
            parsers,
        )

        assert result["field1"] == "value1"
        assert result["field2"] == "value2"
        assert "file_path" in result

    def test_parser_failure_doesnt_stop_others(self):
        """Test that one parser failing doesn't stop others."""

        class FailingParser(TenKParser):
            @property
            def field_name(self) -> str:
                return "failing"

            def extract(self, file_path: Path, file_content=None, **kwargs):
                raise ValueError("Parser failed")

        class WorkingParser(TenKParser):
            @property
            def field_name(self) -> str:
                return "working"

            def extract(self, file_path: Path, file_content=None, **kwargs):
                return "success"

        parsers = [FailingParser(), WorkingParser()]
        result = parse_10k_with_parsers(
            Path("/fake/path.html"),
            parsers,
        )

        # Failing parser should not be in result
        assert "failing" not in result
        # Working parser should be in result
        assert result["working"] == "success"

    def test_validation_filters_invalid_values(self):
        """Test that invalid values are filtered out."""

        class InvalidParser(TenKParser):
            @property
            def field_name(self) -> str:
                return "invalid"

            def extract(self, file_path: Path, file_content=None, **kwargs):
                return None  # Invalid value

            def validate(self, value):
                return value is not None  # Will return False

        parsers = [InvalidParser()]
        result = parse_10k_with_parsers(
            Path("/fake/path.html"),
            parsers,
        )

        # Invalid value should not be in result
        assert "invalid" not in result
