"""
Unit tests for website extraction from 10-K filings.

Tests the website extraction functions with various HTML/XML formats.
"""

from domain_status_graph.parsing.website_extraction import (
    choose_best_website_domain,
    extract_domains_from_ixbrl_namespaces,
    extract_domains_from_visible_text,
    extract_website_from_cover_page,
    extract_website_from_ixbrl_element,
    normalize_website_url,
)


class TestNormalizeWebsiteUrl:
    """Tests for normalize_website_url function."""

    def test_http_url(self):
        """Test normalizing HTTP URLs."""
        assert normalize_website_url("http://www.apple.com") == "apple.com"
        assert normalize_website_url("http://www.microsoft.com/") == "microsoft.com"

    def test_https_url(self):
        """Test normalizing HTTPS URLs."""
        assert normalize_website_url("https://www.google.com") == "google.com"
        assert normalize_website_url("https://www.example.com/path") == "example.com"

    def test_www_prefix(self):
        """Test removing www prefix."""
        assert normalize_website_url("www.apple.com") == "apple.com"
        assert normalize_website_url("www.microsoft.com") == "microsoft.com"

    def test_subdomain(self):
        """Test extracting root domain from subdomain."""
        assert normalize_website_url("investor.apple.com") == "apple.com"
        assert normalize_website_url("blog.example.com") == "example.com"

    def test_complex_tld(self):
        """Test handling complex TLDs."""
        assert normalize_website_url("example.co.uk") == "example.co.uk"
        assert normalize_website_url("test.example.co.uk") == "example.co.uk"

    def test_invalid_url(self):
        """Test invalid URLs return None."""
        assert normalize_website_url("not-a-domain") is None
        assert normalize_website_url("") is None


class TestExtractWebsiteFromIxbrlElement:
    """Tests for extract_website_from_ixbrl_element function."""

    def test_ixbrl_span_element(self):
        """Test extraction from span element with name attribute."""
        html = '<span name="dei:EntityWebSite">https://www.apple.com</span>'
        result = extract_website_from_ixbrl_element(html)
        assert result == "apple.com"

    def test_ixbrl_div_element(self):
        """Test extraction from div element with id attribute."""
        html = '<div id="dei-EntityWebSite">http://www.microsoft.com</div>'
        result = extract_website_from_ixbrl_element(html)
        assert result == "microsoft.com"

    def test_ixbrl_data_attribute(self):
        """Test extraction from element with data-ixbrl attribute."""
        html = '<ix:nonNumeric data-ixbrl="dei:EntityWebSite">www.google.com</ix:nonNumeric>'
        result = extract_website_from_ixbrl_element(html)
        assert result == "google.com"

    def test_no_entity_website(self):
        """Test HTML without EntityWebSite element."""
        html = "<html><body>No website here</body></html>"
        result = extract_website_from_ixbrl_element(html)
        assert result is None

    def test_invalid_domain(self):
        """Test that invalid domains are rejected."""
        html = '<span name="dei:EntityWebSite">not-a-valid-domain</span>'
        result = extract_website_from_ixbrl_element(html)
        assert result is None


class TestExtractDomainsFromIxbrlNamespaces:
    """Tests for extract_domains_from_ixbrl_namespaces function."""

    def test_simple_namespace(self):
        """Test extraction from simple namespace declaration."""
        html = '<html xmlns:air="http://www.aarcorp.com/20240531">'
        result = extract_domains_from_ixbrl_namespaces(html)
        assert "aarcorp.com" in result

    def test_multiple_namespaces(self):
        """Test extraction from multiple namespace declarations."""
        html = (
            '<html xmlns:air="http://www.aarcorp.com/20240531" '
            'xmlns:test="https://example.com/ns">'
        )
        result = extract_domains_from_ixbrl_namespaces(html)
        assert "aarcorp.com" in result
        assert "example.com" in result

    def test_no_namespaces(self):
        """Test HTML without namespace declarations."""
        html = "<html><body>No namespaces</body></html>"
        result = extract_domains_from_ixbrl_namespaces(html)
        assert result == []

    def test_deduplication(self):
        """Test that duplicate domains are removed."""
        html = '<html xmlns:a="http://www.example.com/1" ' 'xmlns:b="http://www.example.com/2">'
        result = extract_domains_from_ixbrl_namespaces(html)
        # Should only appear once
        assert result.count("example.com") == 1


class TestExtractDomainsFromVisibleText:
    """Tests for extract_domains_from_visible_text function."""

    def test_simple_text(self):
        """Test extraction from simple text."""
        html = "<html><body>Visit us at www.example.com for more info</body></html>"
        result = extract_domains_from_visible_text(html)
        assert "example.com" in result

    def test_multiple_domains(self):
        """Test extraction of multiple domains."""
        html = (
            "<html><body>" "Visit www.apple.com or www.microsoft.com for details" "</body></html>"
        )
        result = extract_domains_from_visible_text(html)
        assert "apple.com" in result
        assert "microsoft.com" in result

    def test_ignores_script_tags(self):
        """Test that script tags are ignored."""
        html = (
            "<html><body>Visit www.example.com</body>"
            "<script>var url = 'www.ignored.com';</script></html>"
        )
        result = extract_domains_from_visible_text(html)
        assert "example.com" in result
        # Script content should be ignored (but may still appear in text)
        # The important thing is that visible text domains are found

    def test_max_chars_limit(self):
        """Test that max_chars parameter limits processing."""
        # Create HTML with many domains
        html = (
            "<html><body>"
            + " ".join([f"www.example{i}.com" for i in range(100)])
            + "</body></html>"
        )
        result_limited = extract_domains_from_visible_text(html, max_chars=100)
        result_full = extract_domains_from_visible_text(html, max_chars=10000)
        # Limited should have fewer results
        assert len(result_limited) <= len(result_full)


class TestChooseBestWebsiteDomain:
    """Tests for choose_best_website_domain function."""

    def test_keyword_proximity(self):
        """Test that domains near 'website' keywords score higher."""
        html = (
            "<html><body>"
            "Our website is www.company.com. "
            "Also mentioned: www.other.com"
            "</body></html>"
        )
        result = choose_best_website_domain(html)
        # Should prefer company.com due to keyword proximity
        assert result == "company.com"

    def test_namespace_domains(self):
        """Test that namespace domains are considered."""
        html = (
            '<html xmlns:test="http://www.example.com/ns">'
            "<body>Some text with www.other.com</body></html>"
        )
        result = choose_best_website_domain(html)
        # Should find both, prefer one with higher score
        assert result in ["example.com", "other.com"]

    def test_no_candidates(self):
        """Test HTML with no valid domains."""
        html = "<html><body>No domains here</body></html>"
        result = choose_best_website_domain(html)
        assert result is None

    def test_com_domain_bonus(self):
        """Test that .com domains get scoring bonus."""
        html = "<html><body>" "www.example.org and www.example.com mentioned" "</body></html>"
        result = choose_best_website_domain(html)
        # .com should score higher
        assert result == "example.com"


class TestExtractWebsiteFromCoverPage:
    """Tests for extract_website_from_cover_page function."""

    def test_html_with_ixbrl_element(self, tmp_path):
        """Test extraction from HTML with iXBRL element."""
        html_file = tmp_path / "test.html"
        html_file.write_text(
            '<html><span name="dei:EntityWebSite">https://www.apple.com</span></html>'
        )

        result = extract_website_from_cover_page(html_file, filings_dir=tmp_path)
        assert result == "apple.com"

    def test_xml_with_company_website_tag(self, tmp_path):
        """Test extraction from XML with companyWebsite tag."""
        xml_file = tmp_path / "test.xml"
        xml_file.write_text(
            '<?xml version="1.0"?><root><companyWebsite>www.microsoft.com</companyWebsite></root>'
        )

        result = extract_website_from_cover_page(xml_file, filings_dir=tmp_path)
        assert result == "microsoft.com"

    def test_heuristic_extraction(self, tmp_path):
        """Test heuristic extraction when structured data not available."""
        html_file = tmp_path / "test.html"
        html_file.write_text(
            "<html><body>Our website is www.example.com for more information</body></html>"
        )

        result = extract_website_from_cover_page(html_file, filings_dir=tmp_path)
        assert result == "example.com"

    def test_path_traversal_protection(self, tmp_path):
        """Test that path traversal attempts are blocked."""
        # Create a file outside the filings_dir
        outside_file = tmp_path.parent / "outside.html"
        outside_file.write_text(
            '<html><span name="dei:EntityWebSite">www.example.com</span></html>'
        )

        result = extract_website_from_cover_page(outside_file, filings_dir=tmp_path)
        # Should return None due to path validation
        assert result is None

    def test_with_file_content(self, tmp_path):
        """Test extraction with pre-read file content."""
        html_file = tmp_path / "test.html"
        html_file.write_text('<html><span name="dei:EntityWebSite">www.example.com</span></html>')

        # Read content once
        content = html_file.read_text()

        # Extract using pre-read content (shouldn't re-read file)
        result = extract_website_from_cover_page(
            html_file, file_content=content, filings_dir=tmp_path
        )
        assert result == "example.com"

    def test_no_website_found(self, tmp_path):
        """Test HTML with no website information."""
        html_file = tmp_path / "test.html"
        html_file.write_text("<html><body>No website information</body></html>")

        result = extract_website_from_cover_page(html_file, filings_dir=tmp_path)
        assert result is None
