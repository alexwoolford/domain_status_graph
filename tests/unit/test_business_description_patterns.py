"""
Focused test cases for business description extraction patterns.

These tests cover the specific HTML patterns identified from analyzing
207 companies with missing descriptions. Each pattern represents a
real-world 10-K structure that the parser must handle.

Pattern Categories (from analysis):
1. Split tags in TOC - "Item 1." and "Business" in separate table cells
2. BUSINESS header only - no "Item 1" prefix, just "BUSINESS" section
3. TOC-first structure - Item 1 appears first in TOC table
4. Anchor-based with complex IDs - Has anchor IDs but unusual formats
5. Raw extraction pattern - Standard format that raw regex can extract
6. Foreign/Exhibit files - Non-10K content (should return None)
"""

from public_company_graph.parsing.business_description import (
    extract_business_description,
)


# Helper to generate realistic length content
def generate_business_content(company_desc: str, length: int = 600) -> str:
    """Generate business description content of specified minimum length."""
    base = f"""
        {company_desc}
        The Company operates through multiple business segments and geographic regions.
        Our products and services are designed to meet the evolving needs of our customers.
        We maintain a strong focus on innovation, quality, and customer satisfaction.
        Our strategy emphasizes sustainable growth and long-term value creation.
        We continue to invest in research and development to maintain competitive advantages.
        The Company has established partnerships with leading organizations worldwide.
        Our financial performance reflects disciplined execution of our strategic initiatives.
        We are committed to corporate responsibility and environmental sustainability.
        The management team brings extensive industry experience and proven leadership.
        Our operations leverage advanced technology and efficient processes.
        We serve customers across diverse industries and market segments.
        The Company maintains a strong balance sheet and generates consistent cash flows.
    """
    # Repeat to ensure minimum length
    while len(base) < length:
        base += " Additional business information and strategic context continues here."
    return base


class TestSplitTagsPattern:
    """
    Pattern: Item 1. and Business in separate table cells (TOC format).

    Real-world example: Apple, Aptiv PLC, AbbVie

    Structure:
    <td>Item 1.</td>
    <td><a href="#section_id">Business</a></td>
    ...
    <div id="section_id">Actual content here</div>
    """

    def test_split_tags_toc_with_anchor_link(self, tmp_path):
        """Item 1. and Business split across cells, Business is a link to section."""
        content = generate_business_content(
            "Apple Inc. designs, manufactures and markets smartphones, "
            "personal computers, tablets, wearables and accessories. "
            "The Company's products include iPhone, Mac, iPad, and "
            "wearables, home and accessories."
        )
        html_file = tmp_path / "test.html"
        html_file.write_text(f"""
            <html>
            <body>
                <!-- TOC with split cells -->
                <table>
                    <tr>
                        <td>Item 1.</td>
                        <td><a href="#item1_business_section">Business</a></td>
                        <td>4</td>
                    </tr>
                    <tr>
                        <td>Item 1A.</td>
                        <td><a href="#item1a_risks">Risk Factors</a></td>
                        <td>10</td>
                    </tr>
                </table>

                <!-- Actual content section -->
                <div id="item1_business_section">
                    <p style="font-weight:bold">BUSINESS</p>
                    <p>{content}</p>
                </div>

                <div id="item1a_risks">
                    <p>Item 1A. Risk Factors</p>
                    <p>Risk content here.</p>
                </div>
            </body>
            </html>
        """)

        result = extract_business_description(html_file, filings_dir=tmp_path)
        assert result is not None, "Should extract from split-tag TOC with anchor link"
        assert "designs, manufactures and markets" in result
        assert "Risk Factors" not in result


class TestBusinessHeaderOnlyPattern:
    """
    Pattern: No "Item 1" prefix, just "BUSINESS" as section header.

    Real-world example: Many filings use just "BUSINESS" in uppercase
    """

    def test_uppercase_business_header(self, tmp_path):
        """BUSINESS header without Item 1 prefix."""
        content = generate_business_content(
            "We are a leading independent natural gas producer operating "
            "primarily in the Haynesville shale, a premier natural gas basin. "
            "Our operations focus on maximizing the value of our assets "
            "through efficient development and production."
        )
        html_file = tmp_path / "test.html"
        html_file.write_text(f"""
            <html>
            <body>
                <div>
                    <p style="font-weight:bold">BUSINESS</p>
                    <p>{content}</p>
                </div>
                <div>
                    <p style="font-weight:bold">ITEM 1A. RISK FACTORS</p>
                    <p>Investing in our securities involves risks.</p>
                </div>
            </body>
            </html>
        """)

        result = extract_business_description(html_file, filings_dir=tmp_path)
        assert result is not None, "Should extract from BUSINESS header"
        assert "natural gas producer" in result
        assert "RISK FACTORS" not in result or "Investing" not in result

    def test_business_with_span_decoration(self, tmp_path):
        """BUSINESS header with underline decoration in span."""
        content = generate_business_content(
            "AbbVie Inc. is a global research-based biopharmaceutical company. "
            "We develop and market advanced therapies for patients worldwide."
        )
        html_file = tmp_path / "test.html"
        html_file.write_text(f"""
            <html>
            <body>
                <p>Item 1. <span style="text-decoration:underline">Business</span></p>
                <div>
                    <p>{content}</p>
                </div>
                <p>Item 1A. <span style="text-decoration:underline">Risk Factors</span></p>
                <p>Risk content.</p>
            </body>
            </html>
        """)

        result = extract_business_description(html_file, filings_dir=tmp_path)
        assert result is not None, "Should extract from decorated Business header"
        assert "biopharmaceutical" in result


class TestTOCFirstPattern:
    """
    Pattern: Item 1 Business appears in TOC table first, actual content later.

    Parser must skip TOC matches and find actual content section.
    """

    def test_skip_toc_find_actual_section(self, tmp_path):
        """Item 1 in TOC table should be skipped, actual section found."""
        content = generate_business_content(
            "American Eagle Outfitters, Inc. is a leading global specialty "
            "retailer offering high-quality, on-trend clothing. "
            "We operate stores and e-commerce sites under multiple brands."
        )
        html_file = tmp_path / "test.html"
        html_file.write_text(f"""
            <html>
            <body>
                <!-- Table of Contents -->
                <table>
                    <tr>
                        <td>Item 1. Business</td>
                        <td>Page 4</td>
                    </tr>
                    <tr>
                        <td>Item 1A. Risk Factors</td>
                        <td>Page 15</td>
                    </tr>
                </table>

                <!-- Lots of other content -->
                <div>Some preliminary information here.</div>

                <!-- Actual Item 1 section (not in table) -->
                <div>
                    <h2>Item 1. Business</h2>
                    <p>{content}</p>
                </div>

                <div>
                    <h2>Item 1A. Risk Factors</h2>
                    <p>Our business faces various risks.</p>
                </div>
            </body>
            </html>
        """)

        result = extract_business_description(html_file, filings_dir=tmp_path)
        assert result is not None, "Should skip TOC and find actual section"
        assert "specialty retailer" in result
        assert "Page 4" not in result  # TOC page number should not be in result


class TestRawExtractionPattern:
    """
    Pattern: Standard format where raw regex between markers works.

    Structure: Item 1...BUSINESS...content...Item 1A
    """

    def test_standard_item1_business_format(self, tmp_path):
        """Standard Item 1. Business format with content until Item 1A."""
        content = generate_business_content(
            "Our company is a leading provider of technology solutions "
            "for enterprise customers worldwide. We have operations in 50 countries "
            "and serve over 10,000 customers globally. Our key products include "
            "software platforms, cloud services, and professional consulting."
        )
        html_file = tmp_path / "test.html"
        html_file.write_text(f"""
            <html>
            <body>
                <div>
                    <p><b>Item 1.</b> <span>BUSINESS</span></p>
                    <p>{content}</p>
                </div>
                <div>
                    <p><b>Item 1A.</b> <span>RISK FACTORS</span></p>
                    <p>The following risk factors may affect our business.</p>
                </div>
            </body>
            </html>
        """)

        result = extract_business_description(html_file, filings_dir=tmp_path)
        assert result is not None, "Should extract standard Item 1 Business format"
        assert "technology solutions" in result
        assert len(result) > 100, "Should have substantial content"


class TestAnchorBasedPattern:
    """
    Pattern: TOC uses anchor hrefs that link to section IDs.
    """

    def test_anchor_href_navigation(self, tmp_path):
        """TOC link href points to section ID."""
        content = generate_business_content(
            "Align Technology develops innovative dental solutions including "
            "Invisalign clear aligners and iTero intraoral scanners. "
            "We serve orthodontists and dentists worldwide."
        )
        html_file = tmp_path / "test.html"
        html_file.write_text(f"""
            <html>
            <body>
                <!-- TOC with anchor links -->
                <div>
                    <a href="#i1234567_item1_business">Item 1. Business</a>
                    <a href="#i1234567_item1a">Item 1A. Risk Factors</a>
                </div>

                <!-- Content sections -->
                <div id="i1234567_item1_business">
                    <p>{content}</p>
                </div>

                <div id="i1234567_item1a">
                    <p>Risk factors discussion.</p>
                </div>
            </body>
            </html>
        """)

        result = extract_business_description(html_file, filings_dir=tmp_path)
        assert result is not None, "Should extract using anchor href navigation"
        assert "Invisalign" in result or "dental solutions" in result


class TestETFAndSpecialFilings:
    """
    Pattern: ETFs and special investment vehicles have different structures.
    """

    def test_etf_trust_description(self, tmp_path):
        """ETF 10-K with trust description format."""
        content = generate_business_content(
            "The ARK 21Shares Bitcoin ETF is an exchange-traded fund that "
            "issues common shares of beneficial interest. "
            "The Trust's investment objective is to seek to track the "
            "performance of bitcoin as an asset class."
        )
        html_file = tmp_path / "test.html"
        html_file.write_text(f"""
            <html>
            <body>
                <p>Item 1. <span style="text-decoration:underline">Business</span></p>
                <div>
                    <p><b>DESCRIPTION OF THE TRUST</b></p>
                    <p>{content}</p>
                </div>
                <p>Item 1A. <span style="text-decoration:underline">Risk Factors</span></p>
                <p>Investing involves significant risks.</p>
            </body>
            </html>
        """)

        result = extract_business_description(html_file, filings_dir=tmp_path)
        assert result is not None, "Should extract ETF trust description"
        assert "exchange-traded fund" in result or "Bitcoin ETF" in result


class TestNonTenKFiles:
    """
    Pattern: Files that are not actual 10-K content (exhibits, wrappers).

    These should return None or minimal content.
    """

    def test_exhibit_file_returns_none_or_minimal(self, tmp_path):
        """Exhibit file (not a 10-K) should return None or very short content."""
        html_file = tmp_path / "test.html"
        html_file.write_text("""
            <html>
            <body>
                <div style="text-align: center; font-weight: bold;">
                    <u>EXHIBIT 33.1</u>
                </div>
                <div>
                    Report on Assessment of Compliance with Applicable Servicing
                    Criteria for Covered Bonds.
                </div>
                <div>
                    The Bank is responsible for assessing compliance with the
                    servicing criteria applicable to it.
                </div>
            </body>
            </html>
        """)

        result = extract_business_description(html_file, filings_dir=tmp_path)
        # Should return None or very short content (not a real business description)
        if result is not None:
            assert len(result) < 500, "Exhibit file should have minimal or no business description"


class TestMinimumContentLength:
    """
    Test that extracted content meets minimum length requirements.
    """

    def test_minimum_length_requirement(self, tmp_path):
        """Very short content should be rejected."""
        html_file = tmp_path / "test.html"
        html_file.write_text("""
            <html>
            <body>
                <div id="item1-business">
                    <p>Short.</p>
                </div>
                <div id="item1a">Item 1A</div>
            </body>
            </html>
        """)

        result = extract_business_description(html_file, filings_dir=tmp_path)
        # Very short content should be rejected
        assert result is None or len(result) > 20


class TestMultipleExtractionStrategies:
    """
    Test that parser tries multiple strategies and picks best result.
    """

    def test_fallback_to_raw_extraction(self, tmp_path):
        """When anchor-based fails, raw extraction should be used."""
        content = generate_business_content(
            "This is a substantial business description that explains "
            "what our company does and how we operate in the market. "
            "We provide valuable services to our customers across "
            "multiple industries and geographic regions."
        )
        html_file = tmp_path / "test.html"
        # Create HTML where anchor-based would fail but raw works
        html_file.write_text(f"""
            <html>
            <body>
                <div>
                    <span>ITEM 1.</span><span>BUSINESS</span>
                </div>
                <p>{content}</p>
                <div>
                    <span>ITEM 1A.</span><span>RISK FACTORS</span>
                </div>
                <p>Risk content.</p>
            </body>
            </html>
        """)

        result = extract_business_description(html_file, filings_dir=tmp_path)
        assert result is not None, "Should fall back to raw extraction"
        assert "business description" in result.lower() or "company does" in result


class TestRealWorldPatterns:
    """
    Test patterns extracted from actual problematic 10-K files.
    """

    def test_part_i_item_1_structure(self, tmp_path):
        """Part I / Item 1 structure common in many filings."""
        content = generate_business_content(
            "We are a Delaware corporation engaged in the exploration "
            "and production of natural gas and crude oil. "
            "Our operations are primarily focused in the Permian Basin "
            "and the Eagle Ford Shale."
        )
        html_file = tmp_path / "test.html"
        html_file.write_text(f"""
            <html>
            <body>
                <div>
                    <p style="font-weight:bold">PART I</p>
                </div>
                <div>
                    <p><b>ITEM 1.</b> <b>BUSINESS</b></p>
                    <p>{content}</p>
                </div>
                <div>
                    <p><b>ITEM 1A.</b> <b>RISK FACTORS</b></p>
                    <p>Our business is subject to various risks.</p>
                </div>
            </body>
            </html>
        """)

        result = extract_business_description(html_file, filings_dir=tmp_path)
        assert result is not None, "Should extract from Part I / Item 1 structure"
        assert "Delaware corporation" in result or "natural gas" in result
