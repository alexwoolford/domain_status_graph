"""
Unit tests for risk factors extraction from 10-K filings.

These tests use REAL HTML structures extracted from actual 10-K filings
to ensure the parser handles real-world patterns correctly.
"""

from public_company_graph.parsing.risk_factors import (
    extract_risk_factors,
    extract_risk_factors_with_datamule_fallback,
)


class TestExtractRiskFactors:
    """Tests for extract_risk_factors function using real 10-K structures."""

    def test_extracts_from_toc_link(self, tmp_path):
        """Test extraction using TOC link (real pattern from 10-Ks)."""
        html_file = tmp_path / "0001001385" / "10k_2024.html"
        html_file.parent.mkdir(parents=True, exist_ok=True)

        # Real structure from CIK 0001001385
        html_content = """
        <html>
            <body>
                <table>
                    <tr>
                        <td>Item 1A</td>
                        <td><a href="#item1a">Risk Factors</a></td>
                    </tr>
                </table>
                <div id="item1a">
                    <p>Investing in our common stock is risky. In addition to the other information
                    contained in this Annual Report on Form 10-K, you should consider carefully the
                    following risk factors in evaluating our business and us.</p>
                    <p><b>We have incurred substantial operating losses since our inception.</b></p>
                    <p>Since our inception, we have been engaged primarily in the research and
                    development of our technologies. As a result of these activities, we incurred
                    significant losses and experienced negative cash flow since our inception.</p>
                </div>
                <div id="item1b">Item 1B</div>
            </body>
        </html>
        """
        html_file.write_text(html_content)

        result = extract_risk_factors(html_file, filings_dir=tmp_path)

        assert result is not None
        assert "risk factors" in result.lower()
        assert "operating losses" in result
        assert "Item 1B" not in result  # Should stop at Item 1B

    def test_extracts_from_direct_id(self, tmp_path):
        """Test extraction using direct ID pattern (real pattern from 10-Ks)."""
        html_file = tmp_path / "0001325964" / "10k_2024.html"
        html_file.parent.mkdir(parents=True, exist_ok=True)

        # Real structure from CIK 0001325964
        html_content = """
        <html>
            <body>
                <table>
                    <tr>
                        <td><b>Item 1A.</b></td>
                        <td><b><span id="item1a"></span>Risk Factors.</b></td>
                    </tr>
                </table>
                <p>Investing in our common stock is risky. In addition to the other information
                contained in this Annual Report on Form 10-K, you should consider carefully the
                following risk factors in evaluating our business and us.</p>
                <p><b>We have incurred substantial operating losses since our inception and will
                continue to incur substantial operating losses for the foreseeable future.</b></p>
                <p>Since our inception, we have been engaged primarily in the research and
                development of our electro-optic polymer materials technologies and potential
                products. As a result of these activities, we incurred significant losses and
                experienced negative cash flow since our inception.</p>
                <div id="item1b">Item 1B</div>
            </body>
        </html>
        """
        html_file.write_text(html_content)

        result = extract_risk_factors(html_file, filings_dir=tmp_path)

        assert result is not None
        assert (
            "risk" in result.lower() or "operating losses" in result.lower()
        )  # Risk content present
        assert len(result) > 100  # Should be substantial

    def test_extracts_from_text_search(self, tmp_path):
        """Test extraction using text node search (real pattern from 10-Ks)."""
        html_file = tmp_path / "0001456857" / "10k_2024.html"
        html_file.parent.mkdir(parents=True, exist_ok=True)

        # Real structure from CIK 0001456857
        html_content = """
        <html>
            <body>
                <p><b>Item 1A. Risk Factors</b></p>
                <p><i>You should carefully consider the risks, uncertainties and other factors
                described below, in addition to the other information set forth in this Annual
                Report on Form 10-K, including our consolidated financial statements and the
                related notes thereto. Any of these risks, uncertainties and other factors could
                materially and adversely affect our business, financial condition, results of
                operations, cash flows, or prospects.</i></p>
                <p><b>We are subject to the risks frequently experienced by early stage companies.</b></p>
                <p>The likelihood of our success must be considered in light of the risks frequently
                encountered by early stage companies, especially those formed to develop and market
                new technologies.</p>
                <p><b>ITEM 1B</b></p>
            </body>
        </html>
        """
        html_file.write_text(html_content)

        result = extract_risk_factors(html_file, filings_dir=tmp_path)

        assert result is not None
        assert "risk" in result.lower() or "factors" in result.lower()  # Risk content present
        assert "early stage companies" in result
        assert "ITEM 1B" not in result  # Should stop at Item 1B

    def test_stops_at_item1b_or_item2(self, tmp_path):
        """Test that extraction stops at Item 1B or Item 2 (real boundary patterns)."""
        html_file = tmp_path / "0001325964" / "10k_2024.html"
        html_file.parent.mkdir(parents=True, exist_ok=True)

        html_content = """
        <html>
            <body>
                <table>
                    <tr>
                        <td><b>Item 1A.</b></td>
                        <td><b><span id="item1a"></span>Risk Factors.</b></td>
                    </tr>
                </table>
                <p>Investing in our common stock involves risks. You should consider carefully
                the following risk factors in evaluating our business and us. If any of the
                following events actually occur, our business, operating results, prospects or
                financial condition could be materially and adversely affected.</p>
                <p><b>We have incurred substantial operating losses since our inception and will
                continue to incur substantial operating losses for the foreseeable future.</b></p>
                <p>Since our inception, we have been engaged primarily in the research and
                development of our technologies. As a result of these activities, we incurred
                significant losses and experienced negative cash flow since our inception. We
                incurred a net loss of $17,230,480 for the year ended December 31, 2022,
                $18,631,381 for the year ended December 31, 2021 and $6,715,564 for the year
                ended December 31, 2020.</p>
                <p>More risk content here with additional details about potential risks and
                uncertainties that could affect our business operations and financial results.</p>
                <div id="item1b">Item 1B: Unresolved Staff Comments</div>
                <p>This should not be included.</p>
            </body>
        </html>
        """
        html_file.write_text(html_content)

        result = extract_risk_factors(html_file, filings_dir=tmp_path)

        assert result is not None
        assert "risk" in result.lower() or "operating losses" in result.lower()
        assert "Item 1B" not in result
        assert "This should not be included" not in result

    def test_handles_missing_section(self, tmp_path):
        """Test handling of missing Item 1A section."""
        html_file = tmp_path / "0000320193" / "10k_2024.html"
        html_file.parent.mkdir(parents=True, exist_ok=True)

        # No Item 1A section
        html_content = """
        <html>
            <body>
                <p>Some other content</p>
                <div id="item2">Item 2: Properties</div>
            </body>
        </html>
        """
        html_file.write_text(html_content)

        result = extract_risk_factors(html_file, filings_dir=tmp_path)

        # Should return None if section not found
        assert result is None or len(result) < 100

    def test_path_traversal_protection(self, tmp_path):
        """Test that path traversal attempts are blocked."""
        # File outside filings_dir
        outside_file = tmp_path.parent / "outside.html"
        outside_file.write_text('<html><div id="item1a">Risk Factors</div><p>Content</p></html>')

        result = extract_risk_factors(outside_file, filings_dir=tmp_path)

        # Should return None due to path validation
        assert result is None

    def test_with_file_content(self, tmp_path):
        """Test extraction with pre-read file content."""
        html_file = tmp_path / "0001325964" / "10k_2024.html"
        html_file.parent.mkdir(parents=True, exist_ok=True)

        html_content = """
        <html>
            <body>
                <table>
                    <tr>
                        <td><b>Item 1A.</b></td>
                        <td><b><span id="item1a"></span>Risk Factors.</b></td>
                    </tr>
                </table>
                <p>Investing in our common stock involves risks. You should consider carefully
                the following risk factors in evaluating our business and us. If any of the
                following events actually occur, our business, operating results, prospects or
                financial condition could be materially and adversely affected.</p>
                <p><b>We have incurred substantial operating losses since our inception and will
                continue to incur substantial operating losses for the foreseeable future.</b></p>
                <p>Since our inception, we have been engaged primarily in the research and
                development of our technologies. As a result of these activities, we incurred
                significant losses and experienced negative cash flow since our inception. We
                incurred a net loss of $17,230,480 for the year ended December 31, 2022,
                $18,631,381 for the year ended December 31, 2021 and $6,715,564 for the year
                ended December 31, 2020.</p>
                <p>More risk content here with additional details about potential risks and
                uncertainties that could affect our business operations and financial results.</p>
                <div id="item1b">Item 1B</div>
            </body>
        </html>
        """
        html_file.write_text(html_content)

        # Pass file_content to avoid re-reading
        result = extract_risk_factors(html_file, file_content=html_content, filings_dir=tmp_path)

        assert result is not None
        assert "risk" in result.lower() or "operating losses" in result.lower()


class TestExtractRiskFactorsWithDatamuleFallback:
    """Tests for extract_risk_factors_with_datamule_fallback function.

    Note: This function now relies on datamule and returns None when datamule
    fails (no custom fallback). This keeps the code simple and accepts that
    ~6% of filings won't have risk factors extracted.
    """

    def test_skip_datamule_flag(self, tmp_path):
        """Test that skip_datamule flag returns None (no extraction)."""
        html_file = tmp_path / "0001325964" / "10k_2024.html"
        html_file.parent.mkdir(parents=True, exist_ok=True)
        html_file.write_text("<html><body>Test</body></html>")

        result = extract_risk_factors_with_datamule_fallback(
            html_file,
            cik="0001325964",
            skip_datamule=True,
            filings_dir=tmp_path,
        )

        # skip_datamule=True means no extraction
        assert result is None

    def test_no_tar_files_returns_none(self, tmp_path):
        """Test that missing tar files returns None (no fallback)."""
        html_file = tmp_path / "0001325964" / "10k_2024.html"
        html_file.parent.mkdir(parents=True, exist_ok=True)
        html_file.write_text("<html><body>Test</body></html>")

        # No tar file exists - mock get_data_dir to return tmp_path
        from unittest.mock import patch

        with patch("public_company_graph.parsing.risk_factors.get_data_dir", return_value=tmp_path):
            result = extract_risk_factors_with_datamule_fallback(
                html_file,
                cik="0001325964",
                skip_datamule=False,
                filings_dir=tmp_path,
            )

        # No tar files = no datamule document = None
        assert result is None

    def test_datamule_not_available_returns_none(self, tmp_path):
        """Test that missing datamule library returns None."""
        html_file = tmp_path / "0001325964" / "10k_2024.html"
        html_file.parent.mkdir(parents=True, exist_ok=True)
        html_file.write_text("<html><body>Test</body></html>")

        from unittest.mock import patch

        # Mock ImportError for get_cached_parsed_doc (imported inside the function)
        with patch(
            "public_company_graph.utils.datamule.get_cached_parsed_doc",
            side_effect=ImportError("No module named 'datamule'"),
        ):
            with patch(
                "public_company_graph.parsing.risk_factors.get_data_dir", return_value=tmp_path
            ):
                result = extract_risk_factors_with_datamule_fallback(
                    html_file,
                    cik="0001325964",
                    filings_dir=tmp_path,
                )

        # datamule not available = None
        assert result is None

    def test_no_cik_returns_none(self, tmp_path):
        """Test that missing CIK returns None."""
        # Create file in a non-numeric directory (can't determine CIK)
        html_file = tmp_path / "test.html"
        html_file.write_text("<html><body>Test</body></html>")

        result = extract_risk_factors_with_datamule_fallback(
            html_file,
            cik=None,
            filings_dir=tmp_path,
        )

        # Can't determine CIK from path (non-numeric parent) = None
        assert result is None
