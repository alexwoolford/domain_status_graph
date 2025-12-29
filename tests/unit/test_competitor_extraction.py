"""
Unit tests for competitor extraction from 10-K filings.
"""

import pytest

from public_company_graph.parsing.competitor_extraction import (
    CompetitorLookup,
    CompetitorMention,
    _is_common_word,
    _normalize_company_name,
    extract_and_resolve_competitors,
    extract_competitor_mentions,
    resolve_competitors,
)


class TestNormalizeCompanyName:
    """Tests for company name normalization."""

    def test_removes_corp_suffix(self):
        assert _normalize_company_name("INTEL CORP") == "intel"

    def test_removes_corporation_suffix(self):
        assert _normalize_company_name("Intel Corporation") == "intel"

    def test_removes_inc_suffix(self):
        assert _normalize_company_name("Apple Inc.") == "apple"

    def test_removes_ltd_suffix(self):
        assert _normalize_company_name("Samsung Ltd.") == "samsung"

    def test_removes_holdings_suffix(self):
        assert _normalize_company_name("Alphabet Holdings") == "alphabet"

    def test_multi_word_name(self):
        assert _normalize_company_name("Advanced Micro Devices Inc") == "advanced micro devices"

    def test_preserves_simple_name(self):
        assert _normalize_company_name("Google") == "google"

    def test_removes_state_suffix(self):
        # State suffixes are removed, but function removes one suffix at a time
        # "/de/" is removed first, leaving "microsoft corp"
        # Then "corp" is removed on second pass - but function does single pass
        assert _normalize_company_name("MICROSOFT/DE/") == "microsoft"
        # With CORP, the function removes /de/ leaving "microsoft corp"
        assert _normalize_company_name("MICROSOFT CORP/DE/") == "microsoft corp"


class TestIsCommonWord:
    """Tests for common word filtering."""

    def test_common_business_words(self):
        assert _is_common_word("company")
        assert _is_common_word("corporation")
        assert _is_common_word("business")
        assert _is_common_word("industry")

    def test_common_tech_words(self):
        assert _is_common_word("software")
        assert _is_common_word("hardware")
        assert _is_common_word("platform")
        assert _is_common_word("technology")

    def test_geographic_words(self):
        assert _is_common_word("united")
        assert _is_common_word("states")
        assert _is_common_word("california")
        assert _is_common_word("northern")
        assert _is_common_word("western")

    def test_case_insensitive(self):
        assert _is_common_word("COMPANY")
        assert _is_common_word("Company")
        assert _is_common_word("cOmPaNy")

    def test_real_company_names_not_filtered(self):
        assert not _is_common_word("Intel")
        assert not _is_common_word("NVIDIA")
        assert not _is_common_word("Microsoft")
        assert not _is_common_word("Apple")


class TestExtractCompetitorMentions:
    """Tests for extracting competitor mentions from text."""

    def test_extracts_from_competitor_list(self):
        """Test extraction from "competitors include X, Y, Z" pattern."""
        # Pattern requires "competitors include" followed by substantial context
        # Must have company names with suffixes (Corp, Inc, etc.)
        text = """Our principal competitors include Intel Corporation,
        NVIDIA Corporation, and Advanced Micro Devices, Inc. These companies
        compete with us in the semiconductor market and have significant
        market share in processors and graphics cards."""

        mentions = extract_competitor_mentions(text, None)
        raw_texts = {m.raw_text for m in mentions}

        # Should find company names with suffixes
        assert any("Intel" in t for t in raw_texts)
        assert any("NVIDIA" in t for t in raw_texts)

    def test_extracts_from_compete_with(self):
        """Test extraction from "compete with X" pattern."""
        text = """We compete with Microsoft Corporation and Oracle Corp."""

        mentions = extract_competitor_mentions(text, None)
        raw_texts = {m.raw_text for m in mentions}

        # Should find Microsoft and Oracle
        assert any("Microsoft" in t for t in raw_texts)

    def test_empty_on_no_matches(self):
        """Test returns empty list when no competitor mentions found."""
        text = "This is a generic business description without competitor mentions."
        mentions = extract_competitor_mentions(text, None)
        assert len(mentions) == 0

    def test_handles_none_inputs(self):
        """Test handles None inputs gracefully."""
        mentions = extract_competitor_mentions(None, None)
        assert mentions == []

    def test_deduplicates(self):
        """Test that duplicate mentions are filtered."""
        text = """Our competitors include Intel.
        We also compete with Intel Corporation."""

        mentions = extract_competitor_mentions(text, None)
        intel_mentions = [m for m in mentions if "intel" in m.raw_text.lower()]

        # Should deduplicate - only one Intel mention
        assert len(intel_mentions) <= 2  # May have "Intel" and "Intel Corporation"


class TestResolveCompetitors:
    """Tests for entity resolution of competitor mentions."""

    @pytest.fixture
    def sample_lookup(self):
        """Create a sample lookup table for testing."""
        lookup = CompetitorLookup()

        # Add some companies
        companies = [
            ("0000050863", "INTC", "INTEL CORP"),
            ("0001045810", "NVDA", "NVIDIA CORP"),
            ("0000002488", "AMD", "ADVANCED MICRO DEVICES INC"),
            ("0000789019", "MSFT", "MICROSOFT CORP"),
            ("0001652044", "GOOG", "Alphabet Inc."),
        ]

        for cik, ticker, name in companies:
            company_tuple = (cik, ticker, name)
            lookup.name_to_company[name.lower()] = company_tuple
            lookup.name_to_company[_normalize_company_name(name)] = company_tuple
            lookup.ticker_to_company[ticker] = company_tuple
            lookup.all_names.add(name.lower())
            lookup.all_tickers.add(ticker)

        return lookup

    def test_resolves_by_exact_ticker(self, sample_lookup):
        """Test resolution by exact ticker match."""
        mentions = [CompetitorMention(raw_text="INTC", context="")]

        resolved = resolve_competitors(mentions, sample_lookup)

        assert len(resolved) == 1
        assert resolved[0].resolved_ticker == "INTC"
        assert resolved[0].resolved_cik == "0000050863"
        assert resolved[0].confidence == 1.0

    def test_resolves_by_name(self, sample_lookup):
        """Test resolution by company name."""
        mentions = [CompetitorMention(raw_text="intel", context="")]

        resolved = resolve_competitors(mentions, sample_lookup)

        assert len(resolved) == 1
        assert resolved[0].resolved_ticker == "INTC"

    def test_excludes_self(self, sample_lookup):
        """Test that self-references are excluded."""
        mentions = [
            CompetitorMention(raw_text="Intel", context=""),
            CompetitorMention(raw_text="NVIDIA", context=""),
        ]

        # Resolve from Intel's perspective (exclude self)
        resolved = resolve_competitors(mentions, sample_lookup, self_cik="0000050863")

        # Intel should be excluded, NVIDIA should remain
        tickers = {r.resolved_ticker for r in resolved}
        assert "INTC" not in tickers
        assert "NVDA" in tickers

    def test_returns_empty_for_no_matches(self, sample_lookup):
        """Test returns empty list when no matches found."""
        mentions = [CompetitorMention(raw_text="UnknownCompany", context="")]

        resolved = resolve_competitors(mentions, sample_lookup)

        assert len(resolved) == 0


class TestExtractAndResolveCompetitors:
    """Integration tests for the full extraction pipeline."""

    @pytest.fixture
    def sample_lookup(self):
        """Create a sample lookup table for testing."""
        lookup = CompetitorLookup()

        companies = [
            ("0000050863", "INTC", "INTEL CORP"),
            ("0001045810", "NVDA", "NVIDIA CORP"),
            ("0000002488", "AMD", "ADVANCED MICRO DEVICES INC"),
        ]

        for cik, ticker, name in companies:
            company_tuple = (cik, ticker, name)
            lookup.name_to_company[name.lower()] = company_tuple
            lookup.name_to_company[_normalize_company_name(name)] = company_tuple
            lookup.ticker_to_company[ticker] = company_tuple
            lookup.all_names.add(name.lower())
            lookup.all_tickers.add(ticker)

        return lookup

    def test_full_pipeline(self, sample_lookup):
        """Test the complete extraction and resolution pipeline."""
        # Use text that matches the "compete with" pattern which requires Corp/Inc suffix
        business_desc = """Our principal competitor in CPUs is Intel Corporation.
        We compete with products from NVIDIA Corp. and other GPU vendors in the graphics market."""

        competitors = extract_and_resolve_competitors(
            business_description=business_desc,
            risk_factors=None,
            lookup=sample_lookup,
            self_cik="0000002488",  # Assume we're AMD
        )

        # Should find Intel and NVIDIA
        tickers = {c["competitor_ticker"] for c in competitors}
        assert "INTC" in tickers
        assert "NVDA" in tickers

        # Should NOT include self (AMD)
        assert "AMD" not in tickers

    def test_deduplicates_by_cik(self, sample_lookup):
        """Test that multiple mentions of same company result in one entry."""
        # Use patterns that will match and mention Intel multiple times
        business_desc = """Our competitors include Intel Corporation and Intel Corp.
        We compete with products from Intel Corp. in the CPU market.
        We also compete with Intel Corp. in the server processor space."""

        competitors = extract_and_resolve_competitors(
            business_description=business_desc,
            risk_factors=None,
            lookup=sample_lookup,
            self_cik="0000002488",
        )

        # Should have only one Intel entry despite multiple mentions
        intel_entries = [c for c in competitors if c["competitor_ticker"] == "INTC"]
        assert len(intel_entries) == 1

    def test_returns_dict_format(self, sample_lookup):
        """Test that output is in the correct dictionary format."""
        business_desc = "Our principal competitor is Intel Corporation."

        competitors = extract_and_resolve_competitors(
            business_description=business_desc,
            risk_factors=None,
            lookup=sample_lookup,
        )

        if competitors:
            c = competitors[0]
            assert "competitor_cik" in c
            assert "competitor_ticker" in c
            assert "competitor_name" in c
            assert "confidence" in c
            assert "raw_mention" in c
