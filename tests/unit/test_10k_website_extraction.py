"""
Unit tests for 10-K website extraction.

Tests ensure the parser correctly extracts company websites and rejects invalid domains.
"""

# Add project root to path
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from domain_status_graph.domain.validation import is_valid_domain
from domain_status_graph.parsing.website_extraction import (
    extract_website_from_cover_page,
    extract_website_from_ixbrl_element,
    normalize_website_url,
)


class TestDomainValidation:
    """Test domain validation logic."""

    def test_valid_domains(self):
        """Test that valid domains pass validation."""
        valid_domains = [
            "apple.com",
            "microsoft.com",
            "www.google.com",
            "investor.apple.com",
            "subdomain.example.org",
            "company.co.uk",
            "test.io",
            "example.net",
        ]
        for domain in valid_domains:
            assert is_valid_domain(domain), f"{domain} should be valid"

    def test_invalid_domains(self):
        """Test that invalid domains are rejected."""
        invalid_domains = [
            "a.member",  # Single char prefix, invalid TLD
            "c.member",
            "f.member",
            "p.member",
            "s.member",
            "member",  # No domain part
            "a",  # Too short
            "101.ins",  # Invalid TLD (not in Public Suffix List)
            "360.dd",  # Invalid TLD
            "accesscapitalinc.member",  # Invalid TLD
            "chaoyangxinmeihighpuritysemiconductormaterialsco.ltdmember",  # Invalid TLD, too long
            "xbrl.org",  # Taxonomy domain
            "fasb.org",  # Taxonomy domain
            "sec.gov",  # Taxonomy domain
            "",  # Empty
            ".com",  # No domain part
            "com",  # No domain part
        ]
        for domain in invalid_domains:
            assert not is_valid_domain(domain), f"{domain} should be invalid"


class TestIXBRLExtraction:
    """Test extraction from official iXBRL dei:EntityWebSite element."""

    def test_extract_from_ixbrl_element_with_name_attr(self):
        """Test extraction when EntityWebSite is in name attribute."""
        html = """
        <html>
        <body>
            <span name="dei:EntityWebSite">https://www.apple.com</span>
        </body>
        </html>
        """
        result = extract_website_from_ixbrl_element(html)
        assert result == "apple.com"

    def test_extract_from_ixbrl_element_with_id_attr(self):
        """Test extraction when EntityWebSite is in id attribute."""
        html = """
        <html>
        <body>
            <div id="dei-EntityWebSite">www.microsoft.com</div>
        </body>
        </html>
        """
        result = extract_website_from_ixbrl_element(html)
        assert result == "microsoft.com"

    def test_extract_from_ixbrl_element_with_data_ixbrl_attr(self):
        """Test extraction when EntityWebSite is in data-ixbrl attribute."""
        html = """
        <html>
        <body>
            <ix:nonNumeric data-ixbrl="dei:EntityWebSite">https://www.google.com</ix:nonNumeric>
        </body>
        </html>
        """
        result = extract_website_from_ixbrl_element(html)
        assert result == "google.com"

    def test_extract_from_ixbrl_element_not_found(self):
        """Test when EntityWebSite element is not present."""
        html = """
        <html>
        <body>
            <p>No website element here</p>
        </body>
        </html>
        """
        result = extract_website_from_ixbrl_element(html)
        assert result is None

    def test_extract_from_ixbrl_element_invalid_domain(self):
        """Test that invalid domains from iXBRL element are rejected."""
        html = """
        <html>
        <body>
            <span name="dei:EntityWebSite">a.member</span>
        </body>
        </html>
        """
        result = extract_website_from_ixbrl_element(html)
        assert result is None  # Should reject invalid domain


class TestNormalizeWebsiteURL:
    """Test URL normalization."""

    def test_normalize_with_protocol(self):
        """Test normalization removes protocol."""
        assert normalize_website_url("https://www.apple.com") == "apple.com"
        assert normalize_website_url("http://microsoft.com") == "microsoft.com"

    def test_normalize_with_www(self):
        """Test normalization removes www."""
        assert normalize_website_url("www.google.com") == "google.com"
        assert normalize_website_url("www.example.org") == "example.org"

    def test_normalize_with_subdomain(self):
        """Test normalization extracts root domain from subdomain."""
        assert normalize_website_url("investor.apple.com") == "apple.com"
        assert normalize_website_url("www.subdomain.example.com") == "example.com"

    def test_normalize_with_trailing_slash(self):
        """Test normalization removes trailing slash."""
        assert normalize_website_url("apple.com/") == "apple.com"
        assert normalize_website_url("https://www.apple.com/") == "apple.com"

    def test_normalize_invalid(self):
        """Test normalization rejects invalid URLs."""
        assert normalize_website_url("") is None
        assert normalize_website_url("a.member") is None
        assert normalize_website_url("member") is None


class TestProblematicCases:
    """Test cases for known problematic extractions.

    These tests use minimal HTML samples that reproduce real-world issues where
    invalid domains like "a.member", "c.member" appear in 10-K filings and could
    be incorrectly extracted. The extraction logic should reject these invalid
    domains and extract valid ones instead.
    """

    @pytest.mark.parametrize(
        "bad_domain,valid_domain,html_sample",
        [
            # Bank of America case: "a.member" appears in text, should extract bankofamerica.com
            (
                "a.member",
                "bankofamerica.com",
                """
            <html>
            <head>
                <span name="dei:EntityWebSite">https://www.bankofamerica.com</span>
            </head>
            <body>
                <p>Our website is a.member of the financial services industry.</p>
                <p>Visit us at www.bankofamerica.com for more information.</p>
            </body>
            </html>
            """,
            ),
            # Case with "c.member" in text
            (
                "c.member",
                "example.com",
                """
            <html>
            <head>
                <span name="dei:EntityWebSite">https://www.example.com</span>
            </head>
            <body>
                <p>We are a c.member of the trade association.</p>
                <p>Our website: www.example.com</p>
            </body>
            </html>
            """,
            ),
            # Case with "f.member" in text
            (
                "f.member",
                "example.com",
                """
            <html>
            <head>
                <span name="dei:EntityWebSite">https://www.example.com</span>
            </head>
            <body>
                <p>As an f.member of the organization, we maintain www.example.com</p>
            </body>
            </html>
            """,
            ),
            # Case with "p.member" in text
            (
                "p.member",
                "example.com",
                """
            <html>
            <head>
                <span name="dei:EntityWebSite">https://www.example.com</span>
            </head>
            <body>
                <p>We are a p.member of the partnership. Visit example.com</p>
            </body>
            </html>
            """,
            ),
            # Case with "s.member" in text
            (
                "s.member",
                "example.com",
                """
            <html>
            <head>
                <span name="dei:EntityWebSite">https://www.example.com</span>
            </head>
            <body>
                <p>As an s.member, we operate www.example.com</p>
            </body>
            </html>
            """,
            ),
        ],
    )
    def test_problematic_ciks_do_not_extract_bad_domains(
        self, tmp_path, bad_domain, valid_domain, html_sample
    ):
        """Test that invalid domains like 'a.member' are not extracted from problematic HTML."""
        # Create a temporary HTML file with the problematic content
        html_file = tmp_path / "test_10k.html"
        html_file.write_text(html_sample)

        result = extract_website_from_cover_page(html_file, filings_dir=tmp_path)

        # Should not extract the bad domain
        assert result != bad_domain, f"Extracted invalid domain {bad_domain} - should be rejected"

        # Should extract the valid domain
        assert result == valid_domain, f"Expected {valid_domain}, got {result}"

        # Verify it's a valid domain
        assert is_valid_domain(result), f"Extracted domain should be valid: {result}"

    def test_bank_of_america_extracts_correctly(self, tmp_path):
        """Test that Bank of America extracts bankofamerica.com even when 'a.member' appears in text."""
        # Minimal HTML sample that reproduces the Bank of America issue
        html_sample = """
        <html>
        <head>
            <span name="dei:EntityWebSite">https://www.bankofamerica.com</span>
        </head>
        <body>
            <p>Bank of America Corporation is a.member of the Federal Reserve System.</p>
            <p>For investor information, visit www.bankofamerica.com</p>
            <p>Our website: https://www.bankofamerica.com</p>
        </body>
        </html>
        """

        html_file = tmp_path / "test_10k.html"
        html_file.write_text(html_sample)

        result = extract_website_from_cover_page(html_file, filings_dir=tmp_path)

        # Should extract bankofamerica.com (or similar valid domain)
        assert result is not None, "Should extract a website"
        assert is_valid_domain(result), f"Extracted domain should be valid: {result}"
        assert (
            "bankofamerica" in result.lower() or "bofa" in result.lower()
        ), f"Should extract Bank of America domain, got: {result}"

        # Should NOT extract "a.member"
        assert result != "a.member", "Should not extract invalid domain 'a.member'"


class TestExtractionPriority:
    """Test that extraction uses correct priority order."""

    def test_ixbrl_element_takes_priority(self):
        """Test that iXBRL element extraction takes priority over heuristics."""

        html = """
        <html>
        <body>
            <span name="dei:EntityWebSite">https://www.correct.com</span>
            <p>Also mentions incorrect.com and a.member in text</p>
        </body>
        </html>
        """
        # Create a temporary file in the expected directory structure
        from domain_status_graph.config import get_data_dir

        FILINGS_DIR = get_data_dir() / "10k_filings"
        test_dir = FILINGS_DIR / "test_cik"
        test_dir.mkdir(parents=True, exist_ok=True)
        test_file = test_dir / "10k_2024.html"

        try:
            test_file.write_text(html)
            result = extract_website_from_cover_page(test_file)
            # Should use iXBRL element, not text extraction
            assert result == "correct.com"
        finally:
            if test_file.exists():
                test_file.unlink()
            if test_dir.exists():
                test_dir.rmdir()

    def test_fallback_to_heuristics_when_ixbrl_missing(self):
        """Test that heuristics are used when iXBRL element is missing."""
        # HTML content defined but test requires actual file interaction
        # which is tested in integration tests
        # This test documents the expected fallback behavior
        pass
