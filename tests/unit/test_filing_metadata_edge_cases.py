"""
Edge case tests for filing metadata extraction.

Tests the FilingMetadataParser which extracts:
- Accession numbers (SEC document IDs)
- Filing dates
- Fiscal year end dates

Focus areas:
- Various real-world HTML formats from SEC filings
- Date format variations
- Missing/malformed metadata
"""

from datetime import datetime

from domain_status_graph.parsing.filing_metadata import (
    FilingMetadataParser,
    extract_filing_metadata,
)


class TestAccessionNumberExtraction:
    """Test extraction of SEC accession numbers."""

    def test_standard_accession_format(self, tmp_path):
        """Standard SEC accession format: CCCCCCCCCC-YY-NNNNNN"""
        html_content = """
        <html>
        <body>
        ACCESSION NUMBER: 0000004962-24-000001
        </body>
        </html>
        """
        file_path = tmp_path / "test.htm"
        file_path.write_text(html_content)

        parser = FilingMetadataParser()
        result = parser.extract(file_path)

        assert result is not None
        assert result.get("accession_number") == "0000004962-24-000001"

    def test_accession_with_colon(self, tmp_path):
        """Accession number with various label formats."""
        html_content = """
        <html>
        <body>
        Accession: Number: 0000789019-24-000042
        </body>
        </html>
        """
        file_path = tmp_path / "test.htm"
        file_path.write_text(html_content)

        parser = FilingMetadataParser()
        result = parser.extract(file_path)

        assert result is not None
        assert result.get("accession_number") == "0000789019-24-000042"

    def test_accession_in_middle_of_text(self, tmp_path):
        """Accession number embedded in text."""
        html_content = """
        <html>
        <body>
        <p>This document (0001193125-24-123456) was filed with the SEC.</p>
        </body>
        </html>
        """
        file_path = tmp_path / "test.htm"
        file_path.write_text(html_content)

        parser = FilingMetadataParser()
        result = parser.extract(file_path)

        assert result is not None
        assert result.get("accession_number") == "0001193125-24-123456"

    def test_no_accession_number_returns_none(self, tmp_path):
        """No accession number in document."""
        html_content = """
        <html>
        <body>
        <p>This is a 10-K filing without accession info visible.</p>
        </body>
        </html>
        """
        file_path = tmp_path / "test.htm"
        file_path.write_text(html_content)

        parser = FilingMetadataParser()
        result = parser.extract(file_path)

        # Result may be None or dict without accession_number
        if result:
            assert "accession_number" not in result or result["accession_number"] is None

    def test_invalid_accession_format_rejected(self, tmp_path):
        """Invalid format shouldn't be captured as accession number."""
        html_content = """
        <html>
        <body>
        <p>Phone: 123-456-7890</p>
        <p>Date: 2024-12-31</p>
        </body>
        </html>
        """
        file_path = tmp_path / "test.htm"
        file_path.write_text(html_content)

        parser = FilingMetadataParser()
        result = parser.extract(file_path)

        # Should not capture phone numbers or dates as accession numbers
        if result and "accession_number" in result:
            # If captured, should match SEC format
            assert len(result["accession_number"]) == 20  # CIK(10)-YY(2)-SEQ(6)


class TestFilingDateExtraction:
    """Test extraction of filing dates from HTML."""

    def test_filing_date_yyyy_mm_dd(self, tmp_path):
        """Filing date in YYYY-MM-DD format."""
        html_content = """
        <html>
        <body>
        Filing Date: 2024-03-15
        </body>
        </html>
        """
        file_path = tmp_path / "test.htm"
        file_path.write_text(html_content)

        parser = FilingMetadataParser()
        result = parser.extract(file_path)

        assert result is not None
        assert result.get("filing_date") == "2024-03-15"
        assert result.get("filing_year") == 2024

    def test_filed_date_mm_dd_yyyy(self, tmp_path):
        """Filing date in MM/DD/YYYY format."""
        html_content = """
        <html>
        <body>
        Filed: 03/15/2024
        </body>
        </html>
        """
        file_path = tmp_path / "test.htm"
        file_path.write_text(html_content)

        parser = FilingMetadataParser()
        result = parser.extract(file_path)

        assert result is not None
        assert result.get("filing_date") == "2024-03-15"

    def test_date_of_report_format(self, tmp_path):
        """Date of Report format found in some filings."""
        html_content = """
        <html>
        <body>
        Date of Report: 2024-06-30
        </body>
        </html>
        """
        file_path = tmp_path / "test.htm"
        file_path.write_text(html_content)

        parser = FilingMetadataParser()
        result = parser.extract(file_path)

        assert result is not None
        assert result.get("filing_date") == "2024-06-30"

    def test_no_filing_date_returns_none(self, tmp_path):
        """No filing date in document."""
        html_content = """
        <html>
        <body>
        <p>This is content without any filing date.</p>
        </body>
        </html>
        """
        file_path = tmp_path / "test.htm"
        file_path.write_text(html_content)

        parser = FilingMetadataParser()
        result = parser.extract(file_path)

        # Result may be None or dict without filing_date
        if result:
            assert "filing_date" not in result

    def test_future_date_rejected(self, tmp_path):
        """Dates far in the future should be rejected."""
        html_content = """
        <html>
        <body>
        Filing Date: 2099-12-31
        </body>
        </html>
        """
        file_path = tmp_path / "test.htm"
        file_path.write_text(html_content)

        parser = FilingMetadataParser()
        result = parser.extract(file_path)

        # Should not capture far-future dates
        if result and "filing_date" in result:
            date = datetime.strptime(result["filing_date"], "%Y-%m-%d")
            assert date.year <= datetime.now().year + 1

    def test_ancient_date_rejected(self, tmp_path):
        """Dates before EDGAR (pre-1990) should be rejected."""
        html_content = """
        <html>
        <body>
        Filing Date: 1985-01-15
        </body>
        </html>
        """
        file_path = tmp_path / "test.htm"
        file_path.write_text(html_content)

        parser = FilingMetadataParser()
        result = parser.extract(file_path)

        # Should not capture pre-EDGAR dates
        if result and "filing_date" in result:
            date = datetime.strptime(result["filing_date"], "%Y-%m-%d")
            assert date.year >= 1990


class TestFiscalYearEndExtraction:
    """Test extraction of fiscal year end dates."""

    def test_fiscal_year_end_full_date(self, tmp_path):
        """Fiscal year end with full date."""
        html_content = """
        <html>
        <body>
        Fiscal Year End: 2024-12-31
        </body>
        </html>
        """
        file_path = tmp_path / "test.htm"
        file_path.write_text(html_content)

        parser = FilingMetadataParser()
        result = parser.extract(file_path)

        assert result is not None
        assert result.get("fiscal_year_end") == "2024-12-31"

    def test_fiscal_year_end_year_only(self, tmp_path):
        """Fiscal year end with just year (assumes Dec 31)."""
        html_content = """
        <html>
        <body>
        Fiscal Year End: 2024
        </body>
        </html>
        """
        file_path = tmp_path / "test.htm"
        file_path.write_text(html_content)

        parser = FilingMetadataParser()
        result = parser.extract(file_path)

        assert result is not None
        assert result.get("fiscal_year_end") == "2024-12-31"

    def test_fiscal_year_end_mm_dd_yyyy(self, tmp_path):
        """Fiscal year end in MM/DD/YYYY format."""
        html_content = """
        <html>
        <body>
        Fiscal Year End: 06/30/2024
        </body>
        </html>
        """
        file_path = tmp_path / "test.htm"
        file_path.write_text(html_content)

        parser = FilingMetadataParser()
        result = parser.extract(file_path)

        assert result is not None
        assert result.get("fiscal_year_end") == "2024-06-30"


class TestFileContentHandling:
    """Test handling of file content passed directly vs read from path."""

    def test_extract_with_file_content(self, tmp_path):
        """Extract with pre-read file content."""
        html_content = """
        <html>
        <body>
        ACCESSION NUMBER: 0000004962-24-000001
        Filing Date: 2024-03-15
        </body>
        </html>
        """
        # Create a dummy file path (won't be read)
        file_path = tmp_path / "dummy.htm"

        parser = FilingMetadataParser()
        result = parser.extract(file_path, file_content=html_content)

        assert result is not None
        assert result.get("accession_number") == "0000004962-24-000001"
        assert result.get("filing_date") == "2024-03-15"

    def test_extract_with_soup_reuse(self, tmp_path):
        """Extract with pre-parsed BeautifulSoup (performance optimization)."""
        from bs4 import BeautifulSoup

        html_content = """
        <html>
        <body>
        ACCESSION NUMBER: 0000004962-24-000001
        </body>
        </html>
        """
        file_path = tmp_path / "dummy.htm"
        soup = BeautifulSoup(html_content, "html.parser")

        parser = FilingMetadataParser()
        result = parser.extract(file_path, file_content=html_content, soup=soup)

        assert result is not None
        assert result.get("accession_number") == "0000004962-24-000001"


class TestConvenienceFunction:
    """Test the extract_filing_metadata convenience function."""

    def test_basic_extraction(self, tmp_path):
        """Basic extraction using convenience function."""
        html_content = """
        <html>
        <body>
        ACCESSION NUMBER: 0000004962-24-000001
        Filing Date: 2024-03-15
        Fiscal Year End: 2023-12-31
        </body>
        </html>
        """
        file_path = tmp_path / "test.htm"
        file_path.write_text(html_content)

        result = extract_filing_metadata(file_path)

        assert result is not None
        assert "accession_number" in result
        assert "filing_date" in result
        assert "fiscal_year_end" in result


class TestEdgeCasesAndErrorHandling:
    """Test edge cases and error handling."""

    def test_empty_file(self, tmp_path):
        """Empty file should return None or empty dict."""
        file_path = tmp_path / "empty.htm"
        file_path.write_text("")

        parser = FilingMetadataParser()
        result = parser.extract(file_path)

        # Should not crash, may return None or empty dict
        assert result is None or isinstance(result, dict)

    def test_binary_content_handled(self, tmp_path):
        """Binary/non-text content should be handled gracefully."""
        file_path = tmp_path / "binary.htm"
        file_path.write_bytes(b"\x00\x01\x02\x03\x04\x05")

        parser = FilingMetadataParser()
        # Should not crash
        try:
            result = parser.extract(file_path)
            assert result is None or isinstance(result, dict)
        except Exception:
            # Acceptable to raise on truly binary content
            pass

    def test_malformed_html(self, tmp_path):
        """Malformed HTML should be handled gracefully."""
        html_content = """
        <html>
        <body>
        <p>Unclosed tag
        <div>Nested wrong</p></div>
        ACCESSION NUMBER: 0000004962-24-000001
        </body>
        """  # Missing </html>
        file_path = tmp_path / "malformed.htm"
        file_path.write_text(html_content)

        parser = FilingMetadataParser()
        result = parser.extract(file_path)

        # Should still extract what it can
        # BeautifulSoup is tolerant of malformed HTML
        assert result is not None
        assert result.get("accession_number") == "0000004962-24-000001"

    def test_very_large_file_truncated(self, tmp_path):
        """
        Very large files should be handled (date extraction searches first 20K chars).

        This ensures we don't waste time searching entire multi-MB documents.
        """
        # Create content with date info at the start
        html_start = """
        <html>
        <body>
        Filing Date: 2024-03-15
        """
        # Add a lot of filler
        filler = "<p>Content paragraph.</p>\n" * 10000  # ~240KB

        html_end = """
        Filing Date: 2025-12-31
        </body>
        </html>
        """

        html_content = html_start + filler + html_end
        file_path = tmp_path / "large.htm"
        file_path.write_text(html_content)

        parser = FilingMetadataParser()
        result = parser.extract(file_path)

        # Should find the date at the start, not the one at the end
        # (since it only searches first 20K chars)
        assert result is not None
        assert result.get("filing_date") == "2024-03-15"

    def test_nonexistent_file_handled(self, tmp_path):
        """Non-existent file should be handled gracefully."""
        file_path = tmp_path / "nonexistent.htm"

        parser = FilingMetadataParser()
        # Should handle gracefully
        try:
            result = parser.extract(file_path)
            # May return None
            assert result is None
        except FileNotFoundError:
            # Also acceptable
            pass


class TestFieldNameProperty:
    """Test the field_name property for parser registration."""

    def test_field_name_is_filing_metadata(self):
        """Parser should report 'filing_metadata' as its field name."""
        parser = FilingMetadataParser()
        assert parser.field_name == "filing_metadata"
