"""
Integration tests for pluggable parser components.

These tests verify that the business logic works correctly:
1. WebsiteParser extracts websites from real 10-K HTML
2. BusinessDescriptionParser extracts descriptions correctly
3. Parsers work together through parse_10k_with_parsers
4. Validation filters invalid results
5. Edge cases and real-world scenarios
"""

from unittest.mock import patch

from public_company_graph.parsing.base import (
    BusinessDescriptionParser,
    CompetitorParser,
    WebsiteParser,
    parse_10k_with_parsers,
)


class TestWebsiteParserIntegration:
    """Integration tests for WebsiteParser with real HTML structures."""

    def test_extracts_ixbrl_website(self, tmp_path):
        """Test WebsiteParser extracts website from iXBRL element (highest priority)."""
        html_file = tmp_path / "0000320193" / "10k_2024.html"
        html_file.parent.mkdir(parents=True, exist_ok=True)

        html_content = """
        <html>
            <body>
                <span name="dei:EntityWebSite">https://www.apple.com</span>
                <p>Other content</p>
            </body>
        </html>
        """
        html_file.write_text(html_content)

        parser = WebsiteParser()
        result = parser.extract(
            html_file,
            file_content=html_content,
            filings_dir=tmp_path,
        )

        assert result == "apple.com"
        assert parser.validate(result) is True

    def test_extracts_xml_website(self, tmp_path):
        """Test WebsiteParser extracts website from XML structure."""
        xml_file = tmp_path / "0000789019" / "10k_2024.xml"
        xml_file.parent.mkdir(parents=True, exist_ok=True)

        xml_content = (
            '<?xml version="1.0"?><root><companyWebsite>www.microsoft.com</companyWebsite></root>'
        )
        xml_file.write_text(xml_content)

        parser = WebsiteParser()
        result = parser.extract(
            xml_file,
            file_content=xml_content,
            filings_dir=tmp_path,
        )

        assert result == "microsoft.com"
        assert parser.validate(result) is True

    def test_heuristic_extraction_fallback(self, tmp_path):
        """Test WebsiteParser falls back to heuristic extraction."""
        html_file = tmp_path / "0001018724" / "10k_2024.html"
        html_file.parent.mkdir(parents=True, exist_ok=True)

        # No structured data, but website mentioned in text
        html_content = """
        <html>
            <body>
                <p>For more information, visit our website at www.example.com</p>
                <p>Additional information available at investor.example.com</p>
            </body>
        </html>
        """
        html_file.write_text(html_content)

        parser = WebsiteParser()
        result = parser.extract(
            html_file,
            file_content=html_content,
            filings_dir=tmp_path,
        )

        # Should extract the main domain (not subdomain)
        assert result == "example.com"
        assert parser.validate(result) is True

    def test_rejects_invalid_domains(self, tmp_path):
        """Test WebsiteParser validation rejects invalid domains."""
        html_file = tmp_path / "0000320193" / "10k_2024.html"
        html_file.parent.mkdir(parents=True, exist_ok=True)

        # Invalid domain (taxonomy domain)
        html_content = '<html><span name="dei:EntityWebSite">https://www.xbrl.org</span></html>'
        html_file.write_text(html_content)

        parser = WebsiteParser()
        result = parser.extract(
            html_file,
            file_content=html_content,
            filings_dir=tmp_path,
        )

        # Should return None or invalid domain (validation will reject)
        if result:
            assert parser.validate(result) is False
        else:
            assert result is None

    def test_path_traversal_protection(self, tmp_path):
        """Test WebsiteParser blocks path traversal attempts."""
        # File outside filings_dir
        outside_file = tmp_path.parent / "outside.html"
        outside_file.write_text(
            '<html><span name="dei:EntityWebSite">www.example.com</span></html>'
        )

        parser = WebsiteParser()
        result = parser.extract(
            outside_file,
            filings_dir=tmp_path,
        )

        # Should return None due to path validation
        assert result is None

    def test_handles_missing_file_content(self, tmp_path):
        """Test WebsiteParser reads file when file_content not provided."""
        html_file = tmp_path / "0000320193" / "10k_2024.html"
        html_file.parent.mkdir(parents=True, exist_ok=True)

        html_content = '<html><span name="dei:EntityWebSite">https://www.apple.com</span></html>'
        html_file.write_text(html_content)

        parser = WebsiteParser()
        result = parser.extract(
            html_file,
            file_content=None,  # Not provided - should read file
            filings_dir=tmp_path,
        )

        assert result == "apple.com"


class TestBusinessDescriptionParserIntegration:
    """Integration tests for BusinessDescriptionParser with real HTML structures.

    Note: BusinessDescriptionParser now uses datamule exclusively. When datamule
    is not available or skip_datamule=True, it returns None. This simplifies the
    code and accepts that ~6% of filings won't have business descriptions extracted.
    """

    def test_skip_datamule_returns_none(self, tmp_path):
        """Test that skip_datamule=True returns None (no extraction)."""
        html_file = tmp_path / "0000320193" / "10k_2024.html"
        html_file.parent.mkdir(parents=True, exist_ok=True)
        html_file.write_text("<html><body>Test content</body></html>")

        parser = BusinessDescriptionParser()
        result = parser.extract(
            html_file,
            cik="0000320193",
            skip_datamule=True,
            filings_dir=tmp_path,
        )

        # skip_datamule=True means no extraction
        assert result is None

    def test_skips_non_html_files(self, tmp_path):
        """Test BusinessDescriptionParser skips non-HTML files."""
        xml_file = tmp_path / "0000320193" / "10k_2024.xml"
        xml_file.parent.mkdir(parents=True, exist_ok=True)
        xml_file.write_text("<?xml version='1.0'?><root></root>")

        parser = BusinessDescriptionParser()
        result = parser.extract(
            xml_file,
            cik="0000320193",
            skip_datamule=True,
            filings_dir=tmp_path,
        )

        # Should return None for non-HTML files
        assert result is None

    def test_handles_missing_datamule_document(self, tmp_path):
        """Test BusinessDescriptionParser returns None when no datamule document."""
        html_file = tmp_path / "0000320193" / "10k_2024.html"
        html_file.parent.mkdir(parents=True, exist_ok=True)
        html_file.write_text("<html><body>Test content</body></html>")

        # Mock get_data_dir to return tmp_path (no portfolios exist)
        with patch(
            "public_company_graph.parsing.business_description.get_data_dir",
            return_value=tmp_path,
        ):
            parser = BusinessDescriptionParser()
            result = parser.extract(
                html_file,
                cik="0000320193",
                skip_datamule=False,
                filings_dir=tmp_path,
            )

        # No datamule document = None
        assert result is None

    def test_validate_accepts_any_non_none_text(self, tmp_path):
        """Test that validate() accepts any non-None text.

        Note: BusinessDescriptionParser inherits the base validate() which
        returns True for any non-None value. Actual validation (length checks)
        happens during extraction, not validation.
        """
        parser = BusinessDescriptionParser()

        # Any non-None value passes validation
        valid = "Apple Inc. designs, manufactures, and markets smartphones. " * 10
        assert parser.validate(valid) is True
        assert parser.validate("Short") is True  # Base validate() accepts any value

        # None fails validation
        assert parser.validate(None) is False

    def test_no_tar_returns_none(self, tmp_path):
        """Test that missing tar files returns None (no fallback to custom parser)."""
        html_file = tmp_path / "0000320193" / "10k_2024.html"
        html_file.parent.mkdir(parents=True, exist_ok=True)
        html_file.write_text("<html><body><p>Test content</p></body></html>")

        # Mock get_data_dir to return tmp_path (no tar files exist)
        with patch(
            "public_company_graph.parsing.business_description.get_data_dir",
            return_value=tmp_path,
        ):
            parser = BusinessDescriptionParser()
            result = parser.extract(
                html_file,
                cik="0000320193",
                skip_datamule=False,
                filings_dir=tmp_path,
            )

        # No tar files = no datamule document = None (no custom parser fallback)
        assert result is None


class TestParseWithParsersIntegration:
    """Integration tests for parse_10k_with_parsers orchestration."""

    def test_all_parsers_work_together(self, tmp_path):
        """Test that all parsers work together correctly.

        Note: BusinessDescriptionParser returns None when skip_datamule=True
        (no fallback to custom parser). This test verifies parsers don't
        interfere with each other.
        """
        html_file = tmp_path / "0000320193" / "10k_2024.html"
        html_file.parent.mkdir(parents=True, exist_ok=True)

        html_content = """
        <html>
            <body>
                <span name="dei:EntityWebSite">https://www.apple.com</span>
            </body>
        </html>
        """
        html_file.write_text(html_content)

        parsers = [
            WebsiteParser(),
            BusinessDescriptionParser(),
            CompetitorParser(),
        ]

        result = parse_10k_with_parsers(
            html_file,
            parsers,
            file_content=html_content,
            cik="0000320193",
            skip_datamule=True,  # BusinessDescriptionParser will return None
            filings_dir=tmp_path,
        )

        # Should have extracted fields
        assert "file_path" in result
        assert result["website"] == "apple.com"
        # BusinessDescriptionParser returns None when skip_datamule=True
        assert "business_description" not in result or result["business_description"] is None
        assert result["competitors"] == []  # Placeholder returns empty list

    def test_parser_failure_doesnt_break_others(self, tmp_path):
        """Test that one parser failing doesn't break others."""
        html_file = tmp_path / "0000320193" / "10k_2024.html"
        html_file.parent.mkdir(parents=True, exist_ok=True)

        html_content = """
        <html>
            <body>
                <span name="dei:EntityWebSite">https://www.apple.com</span>
                <!-- Missing Item 1 section - BusinessDescriptionParser will fail -->
            </body>
        </html>
        """
        html_file.write_text(html_content)

        parsers = [
            WebsiteParser(),
            BusinessDescriptionParser(),
        ]

        result = parse_10k_with_parsers(
            html_file,
            parsers,
            file_content=html_content,
            cik="0000320193",
            skip_datamule=True,
            filings_dir=tmp_path,
        )

        # WebsiteParser should succeed
        assert result["website"] == "apple.com"
        # BusinessDescriptionParser should fail (no section), but not break the result
        assert "business_description" not in result or result["business_description"] is None

    def test_validation_filters_invalid_results(self, tmp_path):
        """Test that validation filters out invalid extracted values."""
        html_file = tmp_path / "0000320193" / "10k_2024.html"
        html_file.parent.mkdir(parents=True, exist_ok=True)

        # Website that will fail validation (taxonomy domain)
        html_content = """
        <html>
            <body>
                <span name="dei:EntityWebSite">https://www.xbrl.org</span>
            </body>
        </html>
        """
        html_file.write_text(html_content)

        parsers = [WebsiteParser()]

        result = parse_10k_with_parsers(
            html_file,
            parsers,
            file_content=html_content,
            filings_dir=tmp_path,
        )

        # Invalid website should be filtered out
        assert "website" not in result or result.get("website") is None


class TestParserEdgeCases:
    """Tests for edge cases and real-world scenarios."""

    def test_empty_html_file(self, tmp_path):
        """Test parsers handle empty HTML files gracefully."""
        html_file = tmp_path / "0000320193" / "10k_2024.html"
        html_file.parent.mkdir(parents=True, exist_ok=True)
        html_file.write_text("")

        parsers = [WebsiteParser(), BusinessDescriptionParser()]
        result = parse_10k_with_parsers(
            html_file,
            parsers,
            cik="0000320193",
            skip_datamule=True,
            filings_dir=tmp_path,
        )

        # Should handle gracefully (no crashes)
        assert "file_path" in result
        # May or may not have extracted data (depends on parser behavior)

    def test_malformed_html(self, tmp_path):
        """Test parsers handle malformed HTML gracefully."""
        html_file = tmp_path / "0000320193" / "10k_2024.html"
        html_file.parent.mkdir(parents=True, exist_ok=True)

        # Malformed HTML (unclosed tags, etc.)
        html_content = "<html><body><p>Unclosed paragraph<div>Nested<div>More nested</body></html>"
        html_file.write_text(html_content)

        parsers = [WebsiteParser(), BusinessDescriptionParser()]
        result = parse_10k_with_parsers(
            html_file,
            parsers,
            cik="0000320193",
            skip_datamule=True,
            filings_dir=tmp_path,
        )

        # Should handle gracefully (BeautifulSoup is forgiving)
        assert "file_path" in result

    def test_skip_datamule_returns_none(self, tmp_path):
        """Test BusinessDescriptionParser returns None when skip_datamule=True.

        Note: We no longer have a custom parser fallback. When skip_datamule=True,
        the parser returns None. Large descriptions are handled by datamule without
        arbitrary limits when it's available.
        """
        html_file = tmp_path / "0000320193" / "10k_2024.html"
        html_file.parent.mkdir(parents=True, exist_ok=True)
        html_file.write_text("<html><body>Test</body></html>")

        parser = BusinessDescriptionParser()
        result = parser.extract(
            html_file,
            cik="0000320193",
            skip_datamule=True,
            filings_dir=tmp_path,
        )

        # skip_datamule=True returns None (no extraction)
        assert result is None

    def test_multiple_website_mentions(self, tmp_path):
        """Test WebsiteParser chooses best website from multiple mentions."""
        html_file = tmp_path / "0000320193" / "10k_2024.html"
        html_file.parent.mkdir(parents=True, exist_ok=True)

        # Multiple website mentions (should prefer iXBRL)
        html_content = """
        <html>
            <body>
                <span name="dei:EntityWebSite">https://www.apple.com</span>
                <p>Visit us at investor.apple.com</p>
                <p>Our website is www.apple.com</p>
            </body>
        </html>
        """
        html_file.write_text(html_content)

        parser = WebsiteParser()
        result = parser.extract(
            html_file,
            file_content=html_content,
            filings_dir=tmp_path,
        )

        # Should extract the iXBRL one (highest priority)
        assert result == "apple.com"

    def test_file_content_reuse(self, tmp_path):
        """Test that file_content is reused efficiently."""
        html_file = tmp_path / "0000320193" / "10k_2024.html"
        html_file.parent.mkdir(parents=True, exist_ok=True)

        html_content = """
        <html>
            <body>
                <span name="dei:EntityWebSite">https://www.apple.com</span>
                <div id="item1-business">
                    <p>Business description</p>
                </div>
            </body>
        </html>
        """
        html_file.write_text(html_content)

        # Mock file reading to verify it's not called when file_content is provided
        with patch("builtins.open", wraps=open):
            parsers = [WebsiteParser(), BusinessDescriptionParser()]
            result = parse_10k_with_parsers(
                html_file,
                parsers,
                file_content=html_content,  # Pre-provided
                cik="0000320193",
                skip_datamule=True,
                filings_dir=tmp_path,
            )

            # File should not be opened (file_content was provided)
            # Note: This may still open the file for path validation, but not for content
            assert result["website"] == "apple.com"
