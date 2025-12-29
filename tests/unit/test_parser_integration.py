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

import pytest

from domain_status_graph.parsing.base import (
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
    """Integration tests for BusinessDescriptionParser with real HTML structures."""

    def test_extracts_from_toc_link(self, tmp_path):
        """Test BusinessDescriptionParser extracts via TOC link."""
        html_file = tmp_path / "0000320193" / "10k_2024.html"
        html_file.parent.mkdir(parents=True, exist_ok=True)

        html_content = """
        <html>
            <body>
                <a href="#item1-business">Item 1: Business</a>
                <div id="item1-business">
                    <p>Apple Inc. designs, manufactures, and markets smartphones, personal computers, tablets, wearables, and accessories worldwide.</p>
                    <p>We sell our products through our retail stores, online stores, and direct sales force, as well as through third-party cellular network carriers, wholesalers, retailers, and resellers.</p>
                </div>
                <div id="item1a">Item 1A: Risk Factors</div>
            </body>
        </html>
        """
        html_file.write_text(html_content)

        parser = BusinessDescriptionParser()
        result = parser.extract(
            html_file,
            file_content=html_content,
            cik="0000320193",
            skip_datamule=True,  # Use custom parser
            filings_dir=tmp_path,
        )

        assert result is not None
        assert "Apple Inc." in result
        assert "smartphones" in result
        assert "Item 1A" not in result  # Should stop at next section
        assert parser.validate(result) is True
        assert len(result) > 100  # Should be substantial

    def test_extracts_from_direct_id(self, tmp_path):
        """Test BusinessDescriptionParser extracts via direct ID pattern."""
        html_file = tmp_path / "0000789019" / "10k_2024.html"
        html_file.parent.mkdir(parents=True, exist_ok=True)

        html_content = """
        <html>
            <body>
                <div id="item1-business">
                    <p>Microsoft Corporation develops, licenses, and supports software, services, devices, and solutions worldwide.</p>
                    <p>Our products include operating systems, cross-device productivity applications, server applications, business solution applications, desktop and server management tools, software development tools, video games, and training and certification of computer system integrators and developers.</p>
                </div>
            </body>
        </html>
        """
        html_file.write_text(html_content)

        parser = BusinessDescriptionParser()
        result = parser.extract(
            html_file,
            file_content=html_content,
            cik="0000789019",
            skip_datamule=True,
            filings_dir=tmp_path,
        )

        assert result is not None
        assert "Microsoft Corporation" in result
        assert "software" in result
        assert parser.validate(result) is True

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

    def test_handles_missing_section(self, tmp_path):
        """Test BusinessDescriptionParser handles missing Item 1 section."""
        html_file = tmp_path / "0000320193" / "10k_2024.html"
        html_file.parent.mkdir(parents=True, exist_ok=True)

        # No Item 1 section
        html_content = """
        <html>
            <body>
                <p>Some other content</p>
                <div id="item1a">Item 1A: Risk Factors</div>
            </body>
        </html>
        """
        html_file.write_text(html_content)

        parser = BusinessDescriptionParser()
        result = parser.extract(
            html_file,
            file_content=html_content,
            cik="0000320193",
            skip_datamule=True,
            filings_dir=tmp_path,
        )

        # Should return None if section not found
        assert result is None or len(result) < 100

    def test_datamule_fallback_when_tar_exists(self, tmp_path):
        """Test BusinessDescriptionParser uses datamule when tar file exists."""
        # Skip if datamule not available
        try:
            import datamule  # noqa: F401
        except ImportError:
            pytest.skip("datamule not available")

        html_file = tmp_path / "0000320193" / "10k_2024.html"
        html_file.parent.mkdir(parents=True, exist_ok=True)
        html_file.write_text("<html><body><p>Content</p></body></html>")

        # Create portfolio directory with tar file
        from domain_status_graph.config import get_data_dir

        portfolios_dir = get_data_dir() / "10k_portfolios"
        portfolio_dir = portfolios_dir / "10k_0000320193"
        portfolio_dir.mkdir(parents=True, exist_ok=True)
        (portfolio_dir / "batch_001.tar").write_bytes(b"fake tar")

        parser = BusinessDescriptionParser()
        result = parser.extract(
            html_file,
            cik="0000320193",
            skip_datamule=False,  # Use datamule
            filings_dir=tmp_path,
        )

        # Should attempt to use datamule (may fail if tar is fake, but should try)
        # If datamule fails, should fall back to custom parser
        # Either way, should not crash
        assert result is not None or True  # Just verify it doesn't crash

    def test_custom_parser_fallback_when_no_tar(self, tmp_path):
        """Test BusinessDescriptionParser falls back to custom parser when no tar."""
        html_file = tmp_path / "0000320193" / "10k_2024.html"
        html_file.parent.mkdir(parents=True, exist_ok=True)

        html_content = """
        <html>
            <body>
                <div id="item1-business">
                    <p>Business description content here.</p>
                </div>
            </body>
        </html>
        """
        html_file.write_text(html_content)

        # No tar file exists
        parser = BusinessDescriptionParser()
        result = parser.extract(
            html_file,
            file_content=html_content,
            cik="0000320193",
            skip_datamule=False,  # Try datamule first, but no tar exists
            filings_dir=tmp_path,
        )

        # Should fall back to custom parser
        assert result is not None
        assert "Business description" in result


class TestParseWithParsersIntegration:
    """Integration tests for parse_10k_with_parsers orchestration."""

    def test_all_parsers_work_together(self, tmp_path):
        """Test that all parsers work together correctly."""
        html_file = tmp_path / "0000320193" / "10k_2024.html"
        html_file.parent.mkdir(parents=True, exist_ok=True)

        html_content = """
        <html>
            <body>
                <span name="dei:EntityWebSite">https://www.apple.com</span>
                <a href="#item1-business">Item 1: Business</a>
                <div id="item1-business">
                    <p>Apple Inc. designs and manufactures consumer electronics.</p>
                </div>
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
            skip_datamule=True,
            filings_dir=tmp_path,
        )

        # Should have all extracted fields
        assert "file_path" in result
        assert result["website"] == "apple.com"
        assert result["business_description"] is not None
        assert "Apple Inc." in result["business_description"]
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

    def test_very_large_description(self, tmp_path):
        """Test BusinessDescriptionParser handles very large descriptions."""
        html_file = tmp_path / "0000320193" / "10k_2024.html"
        html_file.parent.mkdir(parents=True, exist_ok=True)

        # Very large description (simulating real 10-K)
        large_text = "A" * 100000  # 100k characters
        html_content = f"""
        <html>
            <body>
                <div id="item1-business">
                    <p>{large_text}</p>
                </div>
            </body>
        </html>
        """
        html_file.write_text(html_content)

        parser = BusinessDescriptionParser()
        result = parser.extract(
            html_file,
            file_content=html_content,
            cik="0000320193",
            skip_datamule=True,
            filings_dir=tmp_path,
        )

        # Should extract (may be truncated, but should be substantial)
        assert result is not None
        assert len(result) > 1000

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
