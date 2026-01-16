"""
Base interface for 10-K parsers.

This module provides a pluggable interface pattern for extracting data from 10-K filings.
Parsers can be easily added by implementing the TenKParser interface.
"""

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any


class TenKParser(ABC):
    """
    Base interface for 10-K parsers.

    This is the "plug-in" interface - implement this to add extraction logic.

    Example:
        class CompetitorParser(TenKParser):
            def extract(self, file_path: Path, file_content: Optional[str] = None, **kwargs) -> Optional[List[str]]:
                # Extract competitor mentions
                return ["Competitor 1", "Competitor 2"]
    """

    @property
    @abstractmethod
    def field_name(self) -> str:
        """
        The field name in the result dictionary.

        Returns:
            String name for this parser's output (e.g., "website", "business_description")
        """
        pass

    @abstractmethod
    def extract(self, file_path: Path, file_content: str | None = None, **kwargs) -> Any | None:
        """
        Extract data from a 10-K file.

        Args:
            file_path: Path to 10-K HTML/XML file
            file_content: Optional pre-read file content (for performance)
            **kwargs: Additional context (e.g., cik, filings_dir, skip_datamule)

        Returns:
            Extracted data (type depends on parser) or None if extraction failed
        """
        pass

    def validate(self, value: Any) -> bool:
        """
        Validate extracted value.

        Override this to add custom validation logic.

        Args:
            value: Extracted value

        Returns:
            True if value is valid, False otherwise
        """
        return value is not None


class WebsiteParser(TenKParser):
    """Parser for extracting company website from 10-K cover page."""

    @property
    def field_name(self) -> str:
        return "website"

    def extract(self, file_path: Path, file_content: str | None = None, **kwargs) -> str | None:
        """Extract website from cover page."""
        from public_company_graph.parsing.website_extraction import extract_website_from_cover_page

        filings_dir = kwargs.get("filings_dir")
        soup = kwargs.get("soup")  # Shared soup for performance
        return extract_website_from_cover_page(
            file_path,
            file_content=file_content,
            filings_dir=filings_dir,
            soup=soup,
        )


class BusinessDescriptionParser(TenKParser):
    """Parser for extracting Item 1: Business description."""

    @property
    def field_name(self) -> str:
        return "business_description"

    def extract(self, file_path: Path, file_content: str | None = None, **kwargs) -> str | None:
        """Extract business description with datamule fallback."""
        from public_company_graph.parsing.business_description import (
            extract_business_description_with_datamule_fallback,
        )

        cik = kwargs.get("cik")
        skip_datamule = kwargs.get("skip_datamule", False)
        filings_dir = kwargs.get("filings_dir")
        soup = kwargs.get("soup")  # Shared soup for performance

        if file_path.suffix != ".html":
            return None

        return extract_business_description_with_datamule_fallback(
            file_path,
            cik=cik,
            file_content=file_content,
            skip_datamule=skip_datamule,
            filings_dir=filings_dir,
            soup=soup,
        )


class RiskFactorsParser(TenKParser):
    """Parser for extracting Item 1A: Risk Factors section."""

    @property
    def field_name(self) -> str:
        return "risk_factors"

    def extract(self, file_path: Path, file_content: str | None = None, **kwargs) -> str | None:
        """Extract risk factors with datamule fallback."""
        from public_company_graph.parsing.risk_factors import (
            extract_risk_factors_with_datamule_fallback,
        )

        cik = kwargs.get("cik")
        skip_datamule = kwargs.get("skip_datamule", False)
        filings_dir = kwargs.get("filings_dir")
        soup = kwargs.get("soup")  # Shared soup for performance

        if file_path.suffix != ".html":
            return None

        return extract_risk_factors_with_datamule_fallback(
            file_path,
            cik=cik,
            file_content=file_content,
            skip_datamule=skip_datamule,
            filings_dir=filings_dir,
            soup=soup,
        )


class CompetitorParser(TenKParser):
    """
    Parser for extracting competitor mentions from Item 1A: Risk Factors.

    NOTE: This is a two-pass process:
    1. First pass: Extract risk_factors text (via RiskFactorsParser)
    2. Second pass: Perform entity resolution on risk_factors text to identify competitors

    This parser is a placeholder for the second-pass entity resolution step.
    """

    @property
    def field_name(self) -> str:
        return "competitors"

    def extract(self, file_path: Path, file_content: str | None = None, **kwargs) -> list | None:
        """
        Extract competitor mentions from 10-K.

        # Note: Entity resolution on risk_factors text can be implemented if needed
        This requires:
        1. Risk factors text (from RiskFactorsParser)
        2. Named entity recognition (NER) to find company names
        3. Entity resolution to match names to tickers/CIKs
        4. Filtering to identify actual competitors (not just mentions)

        For now, returns empty list as placeholder.
        """
        # Future implementation:
        # - Get risk_factors text from previous parser result
        # - Use NER to extract company names
        # - Resolve entities to tickers/CIKs
        # - Return list of competitor tickers/CIKs
        return []

    def validate(self, value: Any) -> bool:
        """Validate that competitors is a list."""
        return isinstance(value, list)


def parse_10k_with_parsers(
    file_path: Path, parsers: list[TenKParser], file_content: str | None = None, **kwargs
) -> dict[str, Any]:
    """
    Parse a 10-K file using a list of pluggable parsers.

    This is the main entry point for extensible parsing.
    Add parsers by implementing TenKParser and adding them to the list.

    PERFORMANCE: HTML is parsed ONCE with BeautifulSoup and shared across all parsers.
    This avoids redundant parsing which was the main bottleneck (3x parsing per file).

    Args:
        file_path: Path to 10-K HTML/XML file
        parsers: List of TenKParser instances to run
        file_content: Optional pre-read file content
        **kwargs: Additional context for parsers

    Returns:
        Dictionary with extracted data (keys from parser.field_name)

    Example:
        parsers = [
            WebsiteParser(),
            BusinessDescriptionParser(),
            CompetitorParser(),
        ]
        result = parse_10k_with_parsers(file_path, parsers, cik="0000320193")
    """
    result = {
        "file_path": str(file_path),
    }

    # PERFORMANCE: Parse HTML once and share across all parsers
    # This reduces BeautifulSoup parsing from N times to 1 time per file
    soup = None
    if file_path.suffix == ".html" and file_content:
        try:
            import warnings

            from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning

            # Suppress the XML-as-HTML warning (some 10-Ks are XHTML)
            with warnings.catch_warnings():
                warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)
                # lxml is ~25% faster than html.parser for large files
                try:
                    soup = BeautifulSoup(file_content, "lxml")
                except Exception:
                    soup = BeautifulSoup(file_content, "html.parser")
            # Pass soup to parsers via kwargs
            kwargs["soup"] = soup
        except Exception:
            pass  # Fall back to per-parser parsing

    # Run each parser
    for parser in parsers:
        try:
            value = parser.extract(file_path, file_content=file_content, **kwargs)
            if parser.validate(value):
                result[parser.field_name] = value  # type: ignore[assignment]
        except Exception as e:
            import logging

            logger = logging.getLogger(__name__)
            logger.debug(f"Parser {parser.field_name} failed for {file_path.name}: {e}")
            # Continue with other parsers even if one fails

    return result


def get_default_parsers() -> list:
    """
    Get the default list of parsers for 10-K parsing.

    This is the SINGLE SOURCE OF TRUTH for which parsers are active.
    Used by parse_10k_filings.py and the worker functions in tenk_workers.py.

    To add a parser:
    1. Implement TenKParser interface
    2. Add to the list below
    3. See docs/adding_parser.md for details

    Returns:
        List of TenKParser instances
    """
    from public_company_graph.parsing.filing_metadata import FilingMetadataParser

    return [
        WebsiteParser(),
        BusinessDescriptionParser(),
        RiskFactorsParser(),
        CompetitorParser(),
        FilingMetadataParser(),
    ]
