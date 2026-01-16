"""
Parser for extracting filing metadata from 10-K filings.

Extracts:
- Filing date (when the 10-K was filed with SEC)
- Accession number (SEC document ID)
- Fiscal year end (company's fiscal year end date)

The primary source is metadata.json from the tar archive (most reliable).
Falls back to HTML parsing if metadata.json is not available.
"""

import json
import logging
import re
import tarfile
from datetime import datetime
from pathlib import Path
from typing import Any

from bs4 import BeautifulSoup

from public_company_graph.parsing.base import TenKParser

logger = logging.getLogger(__name__)


class FilingMetadataParser(TenKParser):
    """
    Parser for extracting filing metadata from 10-K files.

    Extracts:
    - filing_date: Date the 10-K was filed with SEC
    - accession_number: SEC document ID (e.g., "0000004962-24-000001")
    - fiscal_year_end: Company's fiscal year end date
    """

    @property
    def field_name(self) -> str:
        return "filing_metadata"

    def extract(
        self, file_path: Path, file_content: str | None = None, **kwargs
    ) -> dict[str, Any] | None:
        """
        Extract filing metadata from a 10-K file.

        Priority order for extraction:
        1. metadata.json from tar archive (most authoritative - SEC's own metadata)
        2. HTML content parsing (fallback)

        Args:
            file_path: Path to 10-K HTML file
            file_content: Optional pre-read file content
            **kwargs: Additional context (e.g., filings_dir, tar_file, soup)

        Returns:
            Dictionary with filing_date, accession_number, fiscal_year_end, or None
        """
        result = {}

        # 1. PRIMARY SOURCE: Extract from metadata.json in tar file
        tar_file = kwargs.get("tar_file")
        if tar_file and Path(tar_file).exists():
            tar_metadata = self._extract_from_tar_metadata(Path(tar_file))
            if tar_metadata:
                result.update(tar_metadata)
                result["filing_date_source"] = "metadata.json"

        # 2. FALLBACK: Extract from HTML content if no tar metadata
        if not result.get("filing_date"):
            try:
                # Read file if content not provided
                if file_content is None:
                    with open(file_path, encoding="utf-8", errors="ignore") as f:
                        file_content = f.read()

                # PERFORMANCE: Reuse soup if provided, otherwise parse
                soup = kwargs.get("soup")
                if soup is None:
                    try:
                        soup = BeautifulSoup(file_content, "lxml")
                    except Exception:
                        soup = BeautifulSoup(file_content, "html.parser")
                text = soup.get_text()

                # Extract accession number if not already found
                if not result.get("accession_number"):
                    accession_number = self._extract_accession_number(text)
                    if accession_number:
                        result["accession_number"] = accession_number

                # Extract filing date from HTML
                filing_date = self._extract_filing_date_from_html(text, file_content)
                if filing_date:
                    result["filing_date"] = filing_date.strftime("%Y-%m-%d")
                    result["filing_year"] = filing_date.year
                    result["filing_date_source"] = "html"

                # Extract fiscal year end if not already found
                if not result.get("fiscal_year_end"):
                    fiscal_year_end = self._extract_fiscal_year_end(text)
                    if fiscal_year_end:
                        result["fiscal_year_end"] = fiscal_year_end.strftime("%Y-%m-%d")

            except Exception as e:
                logger.debug(f"Error extracting metadata from HTML {file_path.name}: {e}")

        return result if result else None

    def _extract_from_tar_metadata(self, tar_file: Path) -> dict[str, Any] | None:
        """
        Extract filing metadata from metadata.json inside tar archive.

        This is the most authoritative source - it's SEC's own filing metadata.

        Args:
            tar_file: Path to tar archive containing metadata.json

        Returns:
            Dictionary with filing_date, accession_number, fiscal_year_end, period
        """
        try:
            with tarfile.open(tar_file, "r") as tar:
                # Find metadata.json in the archive
                for member in tar.getmembers():
                    if member.name.endswith("metadata.json"):
                        # Prevent Tar Slip: Validate member path before extraction
                        # Note: tar.extractfile() only reads from the archive, it doesn't write files
                        # This is safe from Tar Slip perspective, but we validate the path anyway for defense in depth
                        from pathlib import Path

                        member_path = Path(member.name)
                        # Check for path traversal attempts
                        if ".." in member.name or member_path.is_absolute():
                            logger.warning(f"Path traversal attempt in tar: {member.name}")
                            continue
                        # Only allow simple filenames or paths within expected structure
                        # SEC metadata.json files are typically in subdirectories like "company-cik-12345/metadata.json"
                        # Allow this but reject anything with suspicious patterns
                        if member.name.count("/") > 10:  # Reject deeply nested paths
                            logger.warning(f"Suspicious deeply nested path in tar: {member.name}")
                            continue
                        f = tar.extractfile(member)
                        if f:
                            metadata = json.loads(f.read().decode("utf-8"))
                            result = {}

                            # Extract filing date (format: YYYYMMDD)
                            filing_date_str = metadata.get("filing-date")
                            if filing_date_str:
                                try:
                                    filing_date = datetime.strptime(filing_date_str, "%Y%m%d")
                                    result["filing_date"] = filing_date.strftime("%Y-%m-%d")
                                    result["filing_year"] = filing_date.year
                                except ValueError:
                                    pass

                            # Extract accession number
                            accession = metadata.get("accession-number")
                            if accession:
                                result["accession_number"] = accession

                            # Extract period/fiscal year end (format: YYYYMMDD)
                            period_str = metadata.get("period")
                            if period_str:
                                try:
                                    period = datetime.strptime(period_str, "%Y%m%d")
                                    result["fiscal_year_end"] = period.strftime("%Y-%m-%d")
                                except ValueError:
                                    pass

                            # Also extract fiscal year end month/day from filer data
                            # Note: 'filer' can be a dict or a list (for multiple filers)
                            filer = metadata.get("filer", {})
                            if isinstance(filer, list):
                                filer = filer[0] if filer else {}
                            company_data = (
                                filer.get("company-data", {}) if isinstance(filer, dict) else {}
                            )
                            fy_end = company_data.get("fiscal-year-end")
                            if fy_end and len(fy_end) == 4:
                                result["fiscal_year_end_mmdd"] = fy_end  # e.g., "1231"

                            return result if result else None
        except Exception as e:
            logger.debug(f"Error reading metadata.json from {tar_file.name}: {e}")

        return None

    def _extract_accession_number(self, text: str) -> str | None:
        """
        Extract SEC accession number from text.

        Format: CIK-YY-NNNNNN (e.g., "0000004962-24-000001")
        """
        # Pattern 1: Standard SEC format (CIK-YY-NNNNNN)
        patterns = [
            r"(\d{10}-\d{2}-\d{6})",  # Standard format
            r"Accession[:\s]+Number[:\s]+(\d{10}-\d{2}-\d{6})",  # With label
            r"ACCESSION[:\s]+NUMBER[:\s]+(\d{10}-\d{2}-\d{6})",  # Uppercase label
        ]

        for pattern in patterns:
            match = re.search(pattern, text[:20000], re.I)
            if match:
                accession = match.group(1)
                # Validate format (should be CIK-YY-NNNNNN)
                if re.match(r"\d{10}-\d{2}-\d{6}", accession):
                    return accession

        return None

    def _extract_filing_date_from_html(
        self, text: str, raw_html: str | None = None
    ) -> datetime | None:
        """
        Extract filing date from HTML content.

        Tries multiple date formats commonly found in 10-K filings:
        1. HTML comments (e.g., <!--Created on: 2/20/2020 2:48:35 PM-->)
        2. Text patterns (e.g., "Filing Date: 2020-02-20")

        Args:
            text: Plain text extracted from HTML
            raw_html: Raw HTML content (for comment extraction)
        """
        # Pattern 1: HTML comments - most reliable in many SEC filings
        # Format: <!--Created on: 2/20/2020 2:48:35 PM-->
        if raw_html:
            html_comment_patterns = [
                r"<!--\s*Created on:\s*(\d{1,2}/\d{1,2}/\d{4})",  # M/D/YYYY
                r"<!--\s*Generated:\s*(\d{1,2}/\d{1,2}/\d{4})",  # M/D/YYYY
                r"<!--\s*Document created:\s*(\d{1,2}/\d{1,2}/\d{4})",
            ]
            for pattern in html_comment_patterns:
                match = re.search(pattern, raw_html[:5000], re.I)
                if match:
                    date_str = match.group(1)
                    try:
                        date_obj = datetime.strptime(date_str, "%m/%d/%Y")
                        if 1990 <= date_obj.year <= datetime.now().year + 1:
                            return date_obj
                    except ValueError:
                        pass

        # Pattern 2: Text patterns
        text_patterns = [
            (r"Filing[:\s]+Date[:\s]+(\d{4}-\d{2}-\d{2})", "%Y-%m-%d"),
            (r"Filed[:\s]+(\d{4}-\d{2}-\d{2})", "%Y-%m-%d"),
            (r"Date[:\s]+of[:\s]+Report[:\s]+(\d{4}-\d{2}-\d{2})", "%Y-%m-%d"),
            (r"Filing[:\s]+Date[:\s]+(\d{2}/\d{2}/\d{4})", "%m/%d/%Y"),
            (r"Filed[:\s]+(\d{2}/\d{2}/\d{4})", "%m/%d/%Y"),
            (r"Filed[:\s]+(\d{1,2}/\d{1,2}/\d{4})", "%m/%d/%Y"),  # Single digit month/day
        ]

        for pattern, date_format in text_patterns:
            match = re.search(pattern, text[:20000], re.I)
            if match:
                date_str = match.group(1)
                try:
                    date_obj = datetime.strptime(date_str, date_format)
                    # Validate it's a reasonable date
                    if 1990 <= date_obj.year <= datetime.now().year + 1:
                        return date_obj
                except ValueError:
                    continue

        return None

    def _extract_fiscal_year_end(self, text: str) -> datetime | None:
        """
        Extract fiscal year end date from HTML text.
        """
        patterns = [
            (r"Fiscal[:\s]+Year[:\s]+End[:\s]+(\d{4}-\d{2}-\d{2})", "%Y-%m-%d"),
            (r"Fiscal[:\s]+Year[:\s]+End[:\s]+(\d{2}/\d{2}/\d{4})", "%m/%d/%Y"),
            (r"Fiscal[:\s]+Year[:\s]+End[:\s]+(\d{4})", "%Y"),  # Just year, assume Dec 31
        ]

        for pattern, date_format in patterns:
            match = re.search(pattern, text[:20000], re.I)
            if match:
                date_str = match.group(1)
                try:
                    if date_format == "%Y":
                        # Just year, assume December 31
                        date_obj = datetime(int(date_str), 12, 31)
                    else:
                        date_obj = datetime.strptime(date_str, date_format)
                    # Validate it's a reasonable date
                    if 1990 <= date_obj.year <= datetime.now().year + 1:
                        return date_obj
                except ValueError:
                    continue

        return None


def extract_filing_metadata(
    file_path: Path, file_content: str | None = None, tar_file: Path | None = None, **kwargs
) -> dict[str, Any] | None:
    """
    Convenience function to extract filing metadata.

    Args:
        file_path: Path to 10-K HTML file
        file_content: Optional pre-read file content
        tar_file: Optional tar file path (for fallback date extraction)
        **kwargs: Additional context

    Returns:
        Dictionary with filing_date, accession_number, fiscal_year_end, or None
    """
    parser = FilingMetadataParser()
    if tar_file:
        kwargs["tar_file"] = tar_file
    return parser.extract(file_path, file_content=file_content, **kwargs)
