"""
Business-outcome-focused tests for 10-K parsing.

These tests verify what the parsing should achieve, not how it works.
When tests fail, question the implementation - don't just fix the test.

Note: BusinessDescriptionParser and RiskFactorsParser now use datamule exclusively.
When skip_datamule=True or datamule is not available, they return None.
Tests for extraction quality require real datamule portfolios.
"""

from public_company_graph.domain.validation import is_valid_domain
from public_company_graph.parsing.base import (
    BusinessDescriptionParser,
    RiskFactorsParser,
    WebsiteParser,
    parse_10k_with_parsers,
)


class TestParsingBusinessOutcomes:
    """
    Business Outcome: Extract complete, accurate company information from 10-K filings.

    These tests verify that parsing achieves business goals, not implementation details.

    Note: Business description and risk factor extraction now require datamule.
    Tests that use skip_datamule=True will get None for these fields.
    """

    def test_parsing_extracts_complete_company_data(self, tmp_path):
        """
        Business Outcome: Website extraction works, other fields depend on datamule.

        Given: A 10-K file with website and other content
        When: We parse it with skip_datamule=True (no datamule portfolios)
        Then: Website is extracted; business_description and risk_factors are None
        """
        # Create test file with all data
        html_file = tmp_path / "0000320193" / "10k_2024.html"
        html_file.parent.mkdir(parents=True, exist_ok=True)

        html_content = """
        <html>
            <body>
                <ix:nonNumeric contextRef="..." name="dei:EntityWebSite">https://www.apple.com</ix:nonNumeric>
                <div id="item1-business">
                    <p>Apple Inc. designs, manufactures, and markets smartphones.</p>
                </div>
            </body>
        </html>
        """
        html_file.write_text(html_content)

        # Parse using all parsers
        parsers = [
            WebsiteParser(),
            BusinessDescriptionParser(),
            RiskFactorsParser(),
        ]

        result = parse_10k_with_parsers(
            html_file,
            parsers,
            file_content=html_content,
            cik="0000320193",
            skip_datamule=True,  # No datamule available
            filings_dir=tmp_path,
        )

        # Business Outcome: Website always extracted
        assert result.get("website") is not None, "Website should be extracted"

        # Business Outcome: Website is valid
        website = result["website"]
        assert is_valid_domain(website), f"Website should be valid domain: {website}"
        assert "apple.com" in website.lower(), "Website should match company"

        # Note: business_description and risk_factors are None when skip_datamule=True
        # They require real datamule portfolios to extract content

    def test_parsing_extracts_accurate_website(self, tmp_path):
        """
        Business Outcome: Extracted website is accurate.

        Given: A 10-K file with known company website
        When: We parse it
        Then: Extracted website matches the known correct value
        """
        # Test with Apple (known correct domain: apple.com)
        html_file = tmp_path / "0000320193" / "10k_2024.html"
        html_file.parent.mkdir(parents=True, exist_ok=True)

        html_content = """
        <html>
            <body>
                <ix:nonNumeric contextRef="..." name="dei:EntityWebSite">https://www.apple.com</ix:nonNumeric>
            </body>
        </html>
        """
        html_file.write_text(html_content)

        parser = WebsiteParser()
        result = parser.extract(html_file, file_content=html_content)

        # Business Outcome: Accurate domain extracted
        assert result is not None, "Website should be extracted"
        assert is_valid_domain(result), f"Website should be valid: {result}"

        # Normalize for comparison (www.apple.com -> apple.com)
        normalized = (
            result.lower()
            .replace("www.", "")
            .replace("https://", "")
            .replace("http://", "")
            .split("/")[0]
        )
        assert "apple.com" in normalized, f"Website should be apple.com, got: {result}"

    def test_parsing_handles_malformed_files_gracefully(self, tmp_path):
        """
        Business Outcome: Malformed files don't break parsing.

        Given: A malformed 10-K file
        When: We parse it
        Then: Parsing completes without crashing, returns partial results
        """
        # Create malformed HTML
        html_file = tmp_path / "0000320193" / "10k_2024.html"
        html_file.parent.mkdir(parents=True, exist_ok=True)

        html_content = """
        <html>
            <body>
                <!-- Malformed: missing closing tags, invalid structure -->
                <div id="item1-business">
                    <p>Some description text
                <!-- Missing closing tags -->
            </body>
        </html>
        """
        html_file.write_text(html_content)

        parsers = [
            WebsiteParser(),
            BusinessDescriptionParser(),
            RiskFactorsParser(),
        ]

        # Business Outcome: Parsing doesn't crash
        result = parse_10k_with_parsers(
            html_file,
            parsers,
            file_content=html_content,
            cik="0000320193",
            skip_datamule=True,
            filings_dir=tmp_path,
        )

        # Business Outcome: Returns partial results (some fields may be None)
        assert isinstance(result, dict), "Should return dict even if parsing fails"
        # At least one field might be extracted (graceful degradation)
        # Don't require all fields - that's the business outcome: graceful handling

    def test_parsing_extracts_valid_domains_only(self, tmp_path):
        """
        Business Outcome: Only valid domains are extracted.

        Given: A 10-K file with invalid domain references
        When: We parse it
        Then: Invalid domains are rejected, only valid domains returned
        """
        html_file = tmp_path / "0000320193" / "10k_2024.html"
        html_file.parent.mkdir(parents=True, exist_ok=True)

        html_content = """
        <html>
            <body>
                <!-- Invalid domains should be rejected -->
                <ix:nonNumeric name="dei:EntityWebSite">https://xbrl.org</ix:nonNumeric>
                <ix:nonNumeric name="dei:EntityWebSite">https://sec.gov</ix:nonNumeric>
                <!-- Valid domain -->
                <ix:nonNumeric name="dei:EntityWebSite">https://www.apple.com</ix:nonNumeric>
            </body>
        </html>
        """
        html_file.write_text(html_content)

        parser = WebsiteParser()
        result = parser.extract(html_file, file_content=html_content)

        # Business Outcome: Invalid domains rejected
        if result:
            assert is_valid_domain(result), f"Extracted domain should be valid: {result}"
            assert "xbrl.org" not in result.lower(), "Should reject taxonomy domains"
            assert "sec.gov" not in result.lower(), "Should reject SEC domains"
            assert "apple.com" in result.lower(), "Should extract valid domain"

    def test_parsing_data_quality_meets_standards(self, tmp_path):
        """
        Business Outcome: Website extraction meets quality standards.

        Given: Parsed data from 10-K
        When: We validate website extraction
        Then: Website is valid and accurate

        Note: Business description quality is tested separately with real datamule data.
        """
        html_file = tmp_path / "0000320193" / "10k_2024.html"
        html_file.parent.mkdir(parents=True, exist_ok=True)

        html_content = """
        <html>
            <body>
                <ix:nonNumeric name="dei:EntityWebSite">https://www.apple.com</ix:nonNumeric>
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

        # Quality Standard: Website always present
        assert result.get("website") is not None, "Website should be present"

        # Quality Standard: Website validity
        assert is_valid_domain(result["website"]), "Website should be valid domain"
        assert "apple.com" in result["website"].lower(), "Website should match company"

        # Note: business_description is None when skip_datamule=True
        # Quality tests for business descriptions require real datamule portfolios


class TestParsingCriticalPaths:
    """
    Critical Path Tests: Edge cases that could break business outcomes.
    """

    def test_parsing_handles_missing_sections(self, tmp_path):
        """
        Critical Path: Missing sections don't break parsing.

        Business Outcome: Parsing completes even when some sections are missing.
        """
        html_file = tmp_path / "0000320193" / "10k_2024.html"
        html_file.parent.mkdir(parents=True, exist_ok=True)

        # HTML with no business description section
        html_content = """
        <html>
            <body>
                <ix:nonNumeric name="dei:EntityWebSite">https://www.apple.com</ix:nonNumeric>
                <!-- No Item 1 section -->
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

        # Business Outcome: Parsing completes, returns what it can
        assert isinstance(result, dict), "Should return dict even with missing sections"
        assert result.get("website") is not None, "Website should still be extracted"
        # Business description may be None (acceptable - section missing)

    def test_parsing_handles_empty_files(self, tmp_path):
        """
        Critical Path: Empty files don't crash parsing.

        Business Outcome: Parsing handles edge cases gracefully.
        """
        html_file = tmp_path / "0000320193" / "10k_2024.html"
        html_file.parent.mkdir(parents=True, exist_ok=True)

        html_file.write_text("")  # Empty file

        parsers = [WebsiteParser(), BusinessDescriptionParser()]

        # Business Outcome: Doesn't crash
        result = parse_10k_with_parsers(
            html_file,
            parsers,
            file_content="",
            cik="0000320193",
            skip_datamule=True,
            filings_dir=tmp_path,
        )

        assert isinstance(result, dict), "Should return dict even for empty file"
        # All fields may be None (acceptable - no data to extract)
