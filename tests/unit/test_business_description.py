"""
Unit tests for business description extraction from 10-K filings.

Tests the business description extraction functions with various HTML formats.
"""

from unittest.mock import patch

from public_company_graph.parsing.business_description import (
    extract_between_anchors,
    extract_business_description,
    extract_business_description_with_datamule_fallback,
    extract_section_text,
)


class TestExtractBetweenAnchors:
    """Tests for extract_between_anchors function."""

    def test_extract_between_anchors(self):
        """Test extracting text between two anchor elements."""
        from bs4 import BeautifulSoup

        html = """
        <html>
            <body>
                <a id="item1-business">Item 1: Business</a>
                <p>First paragraph of business description.</p>
                <p>Second paragraph with more details.</p>
                <a id="item1a">Item 1A: Risk Factors</a>
            </body>
        </html>
        """
        soup = BeautifulSoup(html, "html.parser")
        start_el = soup.find(id="item1-business")
        end_el = soup.find(id="item1a")

        result = extract_between_anchors(start_el, end_el)
        assert "First paragraph" in result
        assert "Second paragraph" in result
        assert "Item 1A" not in result

    def test_max_chars_limit(self):
        """Test that max_chars parameter limits extraction."""
        from bs4 import BeautifulSoup

        html = (
            "<html><body><a id='start'></a>"
            + "<p>Text</p>" * 1000
            + "<a id='end'></a></body></html>"
        )
        soup = BeautifulSoup(html, "html.parser")
        start_el = soup.find(id="start")
        end_el = soup.find(id="end")

        result = extract_between_anchors(start_el, end_el, max_chars=100)
        assert len(result) <= 100


class TestExtractSectionText:
    """Tests for extract_section_text function."""

    def test_extract_until_stop_pattern(self):
        """Test extraction stops at Item 1A or Item 2."""
        from bs4 import BeautifulSoup

        html = """
        <html>
            <body>
                <h2 id="item1">Item 1: Business</h2>
                <p>Business description paragraph 1.</p>
                <p>Business description paragraph 2.</p>
                <h2>Item 1A: Risk Factors</h2>
                <p>Risk factors content.</p>
            </body>
        </html>
        """
        soup = BeautifulSoup(html, "html.parser")
        start_el = soup.find(id="item1")

        result = extract_section_text(start_el, soup)
        assert "Business description" in result
        assert "Risk factors" not in result

    def test_no_stop_pattern(self):
        """Test extraction when no stop pattern is found."""
        from bs4 import BeautifulSoup

        html = """
        <html>
            <body>
                <h2 id="item1">Item 1: Business</h2>
                <p>Business description paragraph 1.</p>
                <p>Business description paragraph 2.</p>
            </body>
        </html>
        """
        soup = BeautifulSoup(html, "html.parser")
        start_el = soup.find(id="item1")

        result = extract_section_text(start_el, soup)
        assert "Business description" in result


class TestExtractBusinessDescription:
    """Tests for extract_business_description function."""

    def test_toc_link_extraction(self, tmp_path):
        """Test extraction using TOC link."""
        html_file = tmp_path / "test.html"
        html_file.write_text(
            """
            <html>
                <body>
                    <a href="#item1-business">Item 1: Business</a>
                    <div id="item1-business">
                        <p>Our company operates in the technology sector.</p>
                        <p>We provide software solutions to enterprises.</p>
                    </div>
                    <div id="item1a">Item 1A</div>
                </body>
            </html>
            """
        )

        result = extract_business_description(html_file, filings_dir=tmp_path)
        assert result is not None
        assert "technology sector" in result
        assert "software solutions" in result

    def test_direct_id_extraction(self, tmp_path):
        """Test extraction using direct id pattern."""
        html_file = tmp_path / "test.html"
        html_file.write_text(
            """
            <html>
                <body>
                    <div id="item1-business">
                        <p>Business description content.</p>
                    </div>
                    <div id="item1a">Item 1A</div>
                </body>
            </html>
            """
        )

        result = extract_business_description(html_file, filings_dir=tmp_path)
        assert result is not None
        assert "Business description" in result

    def test_text_node_extraction(self, tmp_path):
        """Test extraction using text node search."""
        html_file = tmp_path / "test.html"
        html_file.write_text(
            """
            <html>
                <body>
                    <div>
                        <h2>ITEM 1: BUSINESS</h2>
                        <p>Business description from text node.</p>
                    </div>
                    <div id="item1a">Item 1A</div>
                </body>
            </html>
            """
        )

        result = extract_business_description(html_file, filings_dir=tmp_path)
        assert result is not None
        assert "Business description" in result

    def test_path_traversal_protection(self, tmp_path):
        """Test that path traversal attempts are blocked."""
        # Create a file outside the filings_dir
        outside_file = tmp_path.parent / "outside.html"
        outside_file.write_text("<html><body>Item 1: Business content</body></html>")

        result = extract_business_description(outside_file, filings_dir=tmp_path)
        # Should return None due to path validation
        assert result is None

    def test_with_file_content(self, tmp_path):
        """Test extraction with pre-read file content."""
        html_file = tmp_path / "test.html"
        html_file.write_text(
            """
            <html>
                <body>
                    <div id="item1-business">
                        <p>Business description.</p>
                    </div>
                    <div id="item1a">Item 1A</div>
                </body>
            </html>
            """
        )

        # Read content once
        content = html_file.read_text()

        # Extract using pre-read content (shouldn't re-read file)
        result = extract_business_description(html_file, file_content=content, filings_dir=tmp_path)
        assert result is not None
        assert "Business description" in result

    def test_no_item1_found(self, tmp_path):
        """Test HTML with no Item 1 section."""
        html_file = tmp_path / "test.html"
        html_file.write_text("<html><body>No Item 1 here</body></html>")

        result = extract_business_description(html_file, filings_dir=tmp_path)
        assert result is None


class TestExtractBusinessDescriptionWithDatamuleFallback:
    """Tests for extract_business_description_with_datamule_fallback function.

    Note: This function now relies on datamule and returns None when datamule
    fails (no custom fallback). This keeps the code simple and accepts that
    ~6% of filings won't have business descriptions extracted.
    """

    def test_skip_datamule_flag(self, tmp_path):
        """Test that skip_datamule flag returns None (no extraction)."""
        html_file = tmp_path / "test.html"
        html_file.write_text("<html><body>Test</body></html>")

        result = extract_business_description_with_datamule_fallback(
            html_file, cik="0000123456", skip_datamule=True, filings_dir=tmp_path
        )
        # skip_datamule=True means no extraction
        assert result is None

    def test_no_tar_files_returns_none(self, tmp_path):
        """Test that missing tar files returns None (no fallback)."""
        html_file = tmp_path / "test.html"
        html_file.write_text("<html><body>Test</body></html>")

        # Mock get_data_dir to return tmp_path (no tar files exist)
        with patch(
            "public_company_graph.parsing.business_description.get_data_dir", return_value=tmp_path
        ):
            result = extract_business_description_with_datamule_fallback(
                html_file, cik="0000123456", filings_dir=tmp_path
            )
            # No tar files = no datamule document = None
            assert result is None

    def test_datamule_not_available_returns_none(self, tmp_path):
        """Test that missing datamule library returns None."""
        html_file = tmp_path / "test.html"
        html_file.write_text("<html><body>Test</body></html>")

        # Mock ImportError for get_cached_parsed_doc (imported inside the function)
        with patch(
            "public_company_graph.utils.datamule.get_cached_parsed_doc",
            side_effect=ImportError("No module named 'datamule'"),
        ):
            with patch(
                "public_company_graph.parsing.business_description.get_data_dir",
                return_value=tmp_path,
            ):
                result = extract_business_description_with_datamule_fallback(
                    html_file, cik="0000123456", filings_dir=tmp_path
                )
                # datamule not available = None
                assert result is None

    def test_no_cik_returns_none(self, tmp_path):
        """Test that missing CIK returns None."""
        # Create file in a non-numeric directory (can't determine CIK)
        html_file = tmp_path / "test.html"
        html_file.write_text("<html><body>Test</body></html>")

        result = extract_business_description_with_datamule_fallback(
            html_file, cik=None, filings_dir=tmp_path
        )
        # Can't determine CIK from path (non-numeric parent) = None
        assert result is None
