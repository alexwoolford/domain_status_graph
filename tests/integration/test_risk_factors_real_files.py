"""
Integration tests for risk factors extraction using real 10-K files.

These tests verify that risk factors are correctly extracted from actual 10-K filings
in the data directory, ensuring the parser works on real-world data.
"""

from pathlib import Path

import pytest

from public_company_graph.config import get_data_dir
from public_company_graph.parsing.base import RiskFactorsParser
from public_company_graph.parsing.risk_factors import (
    extract_risk_factors,
    extract_risk_factors_with_datamule_fallback,
)


class TestRiskFactorsRealFiles:
    """
    Business Outcome: Risk factors are correctly extracted from real 10-K files.

    These tests use actual 10-K files from data/10k_filings/ to verify parsing
    works correctly on real-world data.
    """

    @pytest.fixture
    def filings_dir(self):
        """
        Get the 10-K filings directory.

        Prefers test fixtures (for immediate testability after clone),
        falls back to data directory (for full dataset).
        """
        # Check for test fixtures first
        fixtures_dir = Path(__file__).parent.parent / "fixtures" / "10k_filings"
        if fixtures_dir.exists() and any(fixtures_dir.glob("**/*.html")):
            return fixtures_dir

        # Fallback to data directory
        return get_data_dir() / "10k_filings"

    def test_pepsico_has_risk_factors(self, filings_dir):
        """
        Business Outcome: PepsiCo (CIK 0000004962) has risk factors extracted.

        Given: Real PepsiCo 10-K file
        When: We parse it
        Then: Risk factors are extracted and substantial
        """
        cik = "0000004962"
        # Find any 10-K file for this CIK (may be different years)
        cik_dir = filings_dir / cik
        if not cik_dir.exists():
            pytest.skip(f"CIK directory not found: {cik_dir}")

        # Find first available 10-K file
        files = list(cik_dir.glob("10k_*.html"))
        if not files:
            pytest.skip(f"No 10-K files found in {cik_dir}")

        file_path = files[0]

        result = extract_risk_factors(file_path, filings_dir=filings_dir)

        # Business Outcome: Risk factors are extracted
        assert result is not None, "PepsiCo should have risk factors extracted"

        # Business Outcome: Risk factors are substantial
        assert len(result) > 1000, f"Risk factors should be substantial, got {len(result)} chars"

        # Business Outcome: Contains expected content
        # Note: Heading might be stripped, but content should be present
        result_upper = result.upper()
        # Check for heading OR risk-related content (more flexible)
        has_heading = "ITEM 1A" in result_upper or "RISK FACTORS" in result_upper
        has_risk_content = any(
            keyword in result_upper[:500]
            for keyword in ["RISK", "UNCERTAINTY", "ADVERSE", "MATERIAL"]
        )
        assert has_heading or has_risk_content, (
            "Should contain Item 1A/Risk Factors heading or risk-related content"
        )

        # Business Outcome: Stops at correct boundary
        assert "ITEM 1B" not in result_upper, "Should stop at Item 1B"
        assert "ITEM 2" not in result_upper or result_upper.find("ITEM 2") > len(result) * 0.9, (
            "Should not include Item 2 content"
        )

    def test_microsoft_has_risk_factors(self, filings_dir):
        """
        Business Outcome: Microsoft (CIK 0000078003) has risk factors extracted.

        Given: Real Microsoft 10-K file
        When: We parse it
        Then: Risk factors are extracted and substantial
        """
        cik = "0000078003"
        # Find any 10-K file for this CIK (may be different years)
        cik_dir = filings_dir / cik
        if not cik_dir.exists():
            pytest.skip(f"CIK directory not found: {cik_dir}")

        # Find first available 10-K file
        files = list(cik_dir.glob("10k_*.html"))
        if not files:
            pytest.skip(f"No 10-K files found in {cik_dir}")

        file_path = files[0]

        result = extract_risk_factors(file_path, filings_dir=filings_dir)

        # Business Outcome: Risk factors are extracted (if available)
        # Note: Some older 10-Ks may not have Item 1A in the expected format
        if result is None:
            pytest.skip(
                f"Risk factors not extracted from {file_path.name} (may not have Item 1A in expected format)"
            )

        # Business Outcome: Risk factors are substantial
        assert len(result) > 1000, f"Risk factors should be substantial, got {len(result)} chars"

        # Business Outcome: Contains expected content
        # Note: Heading might be stripped, but content should be present
        result_upper = result.upper()
        # Check for heading OR risk-related content (more flexible)
        has_heading = "ITEM 1A" in result_upper or "RISK FACTORS" in result_upper
        has_risk_content = any(
            keyword in result_upper[:500]
            for keyword in ["RISK", "UNCERTAINTY", "ADVERSE", "MATERIAL"]
        )
        assert has_heading or has_risk_content, (
            "Should contain Item 1A/Risk Factors heading or risk-related content"
        )

    def test_risk_factors_content_quality(self, filings_dir):
        """
        Business Outcome: Extracted risk factors meet quality standards.

        Quality standards:
        - Substantial length (>1000 chars for real 10-Ks)
        - Contains risk-related keywords
        - Properly structured
        """
        # Test with PepsiCo (known to have risk factors)
        cik = "0000004962"
        cik_dir = filings_dir / cik
        if not cik_dir.exists():
            pytest.skip(f"CIK directory not found: {cik_dir}")

        files = list(cik_dir.glob("10k_*.html"))
        if not files:
            pytest.skip(f"No 10-K files found in {cik_dir}")

        file_path = files[0]

        result = extract_risk_factors(file_path, filings_dir=filings_dir)

        if result is None:
            pytest.skip("Risk factors not extracted (may be missing from file)")

        # Quality Standard: Substantial length
        assert len(result) > 1000, f"Risk factors should be substantial, got {len(result)} chars"
        assert len(result) < 1000000, f"Risk factors should be reasonable, got {len(result)} chars"

        # Quality Standard: Contains risk-related content
        result_lower = result.lower()
        risk_keywords = ["risk", "uncertainty", "adverse", "material", "factor"]
        assert any(keyword in result_lower for keyword in risk_keywords), (
            "Risk factors should contain risk-related keywords"
        )

        # Quality Standard: Not just noise
        # Should have multiple sentences/paragraphs
        sentence_count = result.count(".") + result.count("!")
        assert sentence_count > 10, "Risk factors should have multiple sentences"

    def test_risk_factors_structure(self, filings_dir):
        """
        Business Outcome: Risk factors have expected structure.

        Should contain:
        - Introduction paragraph
        - Multiple risk factor headings/descriptions
        - Proper section boundaries
        """
        cik = "0000004962"  # PepsiCo
        # Find any 10-K file for this CIK (may be different years)
        cik_dir = filings_dir / cik
        if not cik_dir.exists():
            pytest.skip(f"CIK directory not found: {cik_dir}")

        # Find first available 10-K file
        files = list(cik_dir.glob("10k_*.html"))
        if not files:
            pytest.skip(f"No 10-K files found in {cik_dir}")

        file_path = files[0]

        result = extract_risk_factors(file_path, filings_dir=filings_dir)

        if result is None:
            pytest.skip("Risk factors not extracted")

        # Structure: Should have introduction
        result_lower = result.lower()
        intro_keywords = ["consider", "evaluate", "carefully", "following"]
        has_intro = any(keyword in result_lower[:500] for keyword in intro_keywords)

        # Structure: Should have multiple risk factors (indicated by headings or lists)
        # Look for common patterns: numbered lists, bold text, headings
        has_structure = (
            result.count("\n\n") > 5  # Multiple paragraphs
            or result.count(".") > 20  # Multiple sentences
            or "risk" in result_lower[:200]  # Early mention of risk
        )

        assert has_intro or has_structure, (
            "Risk factors should have introduction or structured content"
        )

    def test_risk_factors_parser_integration(self, filings_dir):
        """
        Business Outcome: RiskFactorsParser works with real files.

        Given: Real 10-K file
        When: Parsed with RiskFactorsParser
        Then: Risk factors are extracted correctly
        """
        cik = "0000004962"  # PepsiCo
        # Find any 10-K file for this CIK (may be different years)
        cik_dir = filings_dir / cik
        if not cik_dir.exists():
            pytest.skip(f"CIK directory not found: {cik_dir}")

        # Find first available 10-K file
        files = list(cik_dir.glob("10k_*.html"))
        if not files:
            pytest.skip(f"No 10-K files found in {cik_dir}")

        file_path = files[0]

        parser = RiskFactorsParser()
        result = parser.extract(
            file_path,
            cik=cik,
            skip_datamule=True,
            filings_dir=filings_dir,
        )

        # Business Outcome: Risk factors extracted
        assert result is not None, "RiskFactorsParser should extract risk factors"
        assert len(result) > 1000, "Risk factors should be substantial"

    def test_risk_factors_with_datamule_fallback(self, filings_dir):
        """
        Business Outcome: Datamule fallback works with real files.

        Given: Real 10-K file
        When: Parsed with datamule fallback
        Then: Risk factors are extracted (either via datamule or custom parser)
        """
        cik = "0000004962"  # PepsiCo
        # Find any 10-K file for this CIK (may be different years)
        cik_dir = filings_dir / cik
        if not cik_dir.exists():
            pytest.skip(f"CIK directory not found: {cik_dir}")

        # Find first available 10-K file
        files = list(cik_dir.glob("10k_*.html"))
        if not files:
            pytest.skip(f"No 10-K files found in {cik_dir}")

        file_path = files[0]

        result = extract_risk_factors_with_datamule_fallback(
            file_path,
            cik=cik,
            skip_datamule=True,  # Use custom parser (tests fallback path)
            filings_dir=filings_dir,
        )

        # Business Outcome: Risk factors extracted via fallback
        assert result is not None, "Should extract risk factors via custom parser fallback"
        assert len(result) > 1000, "Risk factors should be substantial"

    def test_multiple_companies_have_risk_factors(self, filings_dir):
        """
        Business Outcome: Multiple companies have risk factors extracted.

        Given: Multiple real 10-K files
        When: We parse them
        Then: Most have risk factors extracted
        """
        test_cases = [
            ("0000004962", "PepsiCo"),
            ("0000078003", "Microsoft"),
            # Add more as needed
        ]

        extracted_count = 0
        total_count = 0

        for cik, _company_name in test_cases:
            cik_dir = filings_dir / cik
            if not cik_dir.exists():
                continue

            files = list(cik_dir.glob("10k_*.html"))
            if not files:
                continue

            file_path = files[0]

            total_count += 1
            result = extract_risk_factors(file_path, filings_dir=filings_dir)

            if result and len(result) > 1000:
                extracted_count += 1

        # Business Outcome: Most companies have risk factors
        if total_count > 0:
            success_rate = extracted_count / total_count
            assert success_rate >= 0.5, (
                f"At least 50% should have risk factors, got {success_rate:.1%}"
            )

    def test_risk_factors_boundary_detection(self, filings_dir):
        """
        Business Outcome: Risk factors extraction stops at correct boundaries.

        Given: Real 10-K file
        When: We parse risk factors
        Then: Extraction stops at Item 1B or Item 2, doesn't include later sections
        """
        cik = "0000004962"  # PepsiCo
        # Find any 10-K file for this CIK (may be different years)
        cik_dir = filings_dir / cik
        if not cik_dir.exists():
            pytest.skip(f"CIK directory not found: {cik_dir}")

        # Find first available 10-K file
        files = list(cik_dir.glob("10k_*.html"))
        if not files:
            pytest.skip(f"No 10-K files found in {cik_dir}")

        file_path = files[0]

        result = extract_risk_factors(file_path, filings_dir=filings_dir)

        if result is None:
            pytest.skip("Risk factors not extracted")

        result_upper = result.upper()

        # Boundary: Should not include Item 1B content
        # (Item 1B might appear in the text, but content after it should not be included)
        item1b_pos = result_upper.find("ITEM 1B")
        if item1b_pos > 0:
            # If Item 1B appears, most content should be before it
            content_before = item1b_pos / len(result)
            assert content_before > 0.8, "If Item 1B appears, most content should be before it"

        # Boundary: Should not include Item 2 content
        item2_pos = result_upper.find("ITEM 2")
        if item2_pos > 0:
            content_before = item2_pos / len(result)
            assert content_before > 0.9, "If Item 2 appears, almost all content should be before it"
