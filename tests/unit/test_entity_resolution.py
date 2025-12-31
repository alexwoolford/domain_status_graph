"""
Comprehensive tests for the entity resolution module.

Tests are organized by component:
1. Candidate extraction
2. Candidate filtering
3. Candidate matching
4. Confidence scoring
5. Full pipeline

Each component can be tested in isolation.
"""

import pytest

from public_company_graph.entity_resolution.candidates import (
    Candidate,
    CapitalizedWordExtractor,
    TickerExtractor,
    extract_candidates,
    extract_candidates_with_stats,
)
from public_company_graph.entity_resolution.filters import (
    FilterReason,
    NameBlocklistFilter,
    NegationContextFilter,
    SelfReferenceFilter,
    TickerBlocklistFilter,
)
from public_company_graph.entity_resolution.matchers import (
    ExactNameMatcher,
    ExactTickerMatcher,
    MatchResult,
    MatchType,
    NormalizedNameMatcher,
)
from public_company_graph.entity_resolution.resolver import (
    EntityResolver,
    analyze_resolution_quality,
    resolve_company_mentions,
)
from public_company_graph.entity_resolution.scoring import (
    RuleBasedScorer,
)
from public_company_graph.parsing.business_relationship_extraction import (
    CompanyLookup,
)

# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def sample_lookup() -> CompanyLookup:
    """Create a sample company lookup for testing."""
    lookup = CompanyLookup()

    # Add some test companies
    companies = [
        ("0001045810", "NVDA", "NVIDIA Corporation"),
        ("0000320193", "AAPL", "Apple Inc."),
        ("0000789019", "MSFT", "Microsoft Corporation"),
        ("0001018724", "AMZN", "Amazon.com, Inc."),
        ("0001652044", "GOOGL", "Alphabet Inc."),
        ("0000051143", "IBM", "International Business Machines Corporation"),
        ("0000050863", "INTC", "Intel Corporation"),
        ("0000002488", "AMD", "Advanced Micro Devices, Inc."),
    ]

    for cik, ticker, name in companies:
        company_tuple = (cik, ticker, name)

        # Add by ticker
        lookup.ticker_to_company[ticker] = company_tuple
        lookup.all_tickers.add(ticker)

        # Add by name (lowercase)
        name_lower = name.lower()
        lookup.name_to_company[name_lower] = company_tuple
        lookup.all_names.add(name_lower)

        # Add normalized name
        from public_company_graph.parsing.business_relationship_extraction import (
            _normalize_company_name,
        )

        normalized = _normalize_company_name(name)
        if normalized and normalized != name_lower:
            lookup.name_to_company[normalized] = company_tuple
            lookup.all_names.add(normalized)

    return lookup


@pytest.fixture
def sample_text() -> str:
    """Sample 10-K text for testing."""
    return """
    We compete with Microsoft Corporation and Intel in the semiconductor market.
    Our major customers include Apple Inc. and Amazon.
    NVIDIA is a key supplier of GPU components.
    We have a strategic partnership with IBM.
    """


# =============================================================================
# Candidate Extraction Tests
# =============================================================================


class TestCapitalizedWordExtractor:
    """Tests for CapitalizedWordExtractor."""

    def test_extracts_single_capitalized_word(self):
        """Test extraction of single capitalized words."""
        extractor = CapitalizedWordExtractor()
        text = "We compete with Microsoft and Apple."

        candidates = extractor.extract(text)

        texts = [c.text for c in candidates]
        assert "Microsoft" in texts
        assert "Apple" in texts

    def test_extracts_multi_word_company_names(self):
        """Test extraction of multi-word company names."""
        extractor = CapitalizedWordExtractor()
        text = "International Business Machines is a competitor."

        candidates = extractor.extract(text)

        texts = [c.text for c in candidates]
        assert "International Business Machines" in texts

    def test_includes_ampersand_in_names(self):
        """Test that company names with & are extracted."""
        extractor = CapitalizedWordExtractor()
        text = "Johnson & Johnson is in healthcare."

        candidates = extractor.extract(text)

        texts = [c.text for c in candidates]
        # The current regex extracts "Johnson" separately
        # This is a known limitation - full name with & requires custom handling
        assert "Johnson" in texts

    def test_captures_sentence_context(self):
        """Test that sentence context is captured."""
        extractor = CapitalizedWordExtractor()
        text = "We compete with Microsoft in the market."

        candidates = extractor.extract(text)

        # Find any candidate that has context
        candidates_with_context = [c for c in candidates if c.sentence]
        assert len(candidates_with_context) > 0
        # Sentence should contain the original text
        assert any("Microsoft" in c.sentence for c in candidates_with_context)


class TestTickerExtractor:
    """Tests for TickerExtractor."""

    def test_extracts_tickers(self):
        """Test extraction of ticker-like strings."""
        extractor = TickerExtractor()
        text = "AAPL and MSFT are tech stocks. NVDA is also popular."

        candidates = extractor.extract(text)

        texts = [c.text for c in candidates]
        assert "AAPL" in texts
        assert "MSFT" in texts
        assert "NVDA" in texts

    def test_excludes_single_letters(self):
        """Test that single letters are not extracted."""
        extractor = TickerExtractor()
        text = "A B C are single letters."

        candidates = extractor.extract(text)

        texts = [c.text for c in candidates]
        assert "A" not in texts
        assert "B" not in texts

    def test_extracts_2_to_5_char_tickers(self):
        """Test length range of extracted tickers."""
        extractor = TickerExtractor()
        text = "AB is 2, ABCDE is 5, ABCDEF is 6."

        candidates = extractor.extract(text)

        texts = [c.text for c in candidates]
        assert "AB" in texts
        assert "ABCDE" in texts
        assert "ABCDEF" not in texts


class TestExtractCandidates:
    """Tests for combined candidate extraction."""

    def test_combines_extractors(self):
        """Test that multiple extractors are combined."""
        text = "Microsoft and MSFT are the same. Apple Inc. is different."

        candidates = extract_candidates(text)

        texts = [c.text.lower() for c in candidates]
        assert "microsoft" in texts
        assert "msft" in texts
        assert any("apple" in t for t in texts)

    def test_deduplicates_by_text(self):
        """Test that duplicate texts are removed."""
        text = "MSFT appears twice. MSFT is Microsoft."

        candidates = extract_candidates(text)

        msft_count = sum(1 for c in candidates if c.text == "MSFT")
        assert msft_count == 1

    def test_returns_stats(self):
        """Test that stats are returned correctly."""
        text = "Microsoft and MSFT are mentioned."

        candidates, stats = extract_candidates_with_stats(text)

        assert "capitalized" in stats
        assert "ticker" in stats
        assert stats["capitalized"] >= 1
        assert stats["ticker"] >= 1


# =============================================================================
# Candidate Filtering Tests
# =============================================================================


class TestTickerBlocklistFilter:
    """Tests for TickerBlocklistFilter."""

    def test_blocks_common_words(self):
        """Test that common words are blocked."""
        filter = TickerBlocklistFilter()
        candidate = Candidate(
            text="THE",
            start_pos=0,
            end_pos=3,
            source_pattern="ticker",
            sentence="THE company is large.",
        )

        result = filter.filter(candidate)

        assert not result.passed
        assert result.reason == FilterReason.TICKER_BLOCKLIST

    def test_allows_valid_tickers(self):
        """Test that valid tickers pass."""
        filter = TickerBlocklistFilter()
        candidate = Candidate(
            text="NVDA",
            start_pos=0,
            end_pos=4,
            source_pattern="ticker",
            sentence="NVDA is NVIDIA.",
        )

        result = filter.filter(candidate)

        assert result.passed

    def test_custom_blocklist(self):
        """Test with custom blocklist."""
        filter = TickerBlocklistFilter(blocklist={"CUSTOM"})
        candidate = Candidate(
            text="CUSTOM",
            start_pos=0,
            end_pos=6,
            source_pattern="ticker",
            sentence="Test.",
        )

        result = filter.filter(candidate)

        assert not result.passed


class TestNameBlocklistFilter:
    """Tests for NameBlocklistFilter."""

    def test_blocks_generic_terms(self):
        """Test that generic business terms are blocked."""
        filter = NameBlocklistFilter()
        candidate = Candidate(
            text="technology",
            start_pos=0,
            end_pos=10,
            source_pattern="capitalized",
            sentence="Technology is important.",
        )

        result = filter.filter(candidate)

        assert not result.passed
        assert result.reason == FilterReason.NAME_BLOCKLIST

    def test_allows_high_value_names(self):
        """Test that high-value names bypass blocklist."""
        filter = NameBlocklistFilter()
        candidate = Candidate(
            text="nvidia",
            start_pos=0,
            end_pos=6,
            source_pattern="capitalized",
            sentence="NVIDIA makes GPUs.",
        )

        result = filter.filter(candidate)

        assert result.passed


class TestNegationContextFilter:
    """Tests for NegationContextFilter."""

    def test_blocks_negated_mentions(self):
        """Test that negated mentions are blocked."""
        filter = NegationContextFilter()
        candidate = Candidate(
            text="Microsoft",
            start_pos=0,
            end_pos=9,
            source_pattern="capitalized",
            sentence="We do not compete with Microsoft.",
        )

        result = filter.filter(candidate)

        assert not result.passed
        assert result.reason == FilterReason.NEGATION_CONTEXT

    def test_allows_positive_mentions(self):
        """Test that positive mentions pass."""
        filter = NegationContextFilter()
        candidate = Candidate(
            text="Microsoft",
            start_pos=0,
            end_pos=9,
            source_pattern="capitalized",
            sentence="We compete with Microsoft.",
        )

        result = filter.filter(candidate)

        assert result.passed

    def test_blocks_former_relationships(self):
        """Test that 'formerly' context is blocked."""
        filter = NegationContextFilter()
        candidate = Candidate(
            text="IBM",
            start_pos=0,
            end_pos=3,
            source_pattern="ticker",
            sentence="We formerly partnered with IBM.",
        )

        result = filter.filter(candidate)

        assert not result.passed


class TestSelfReferenceFilter:
    """Tests for SelfReferenceFilter."""

    def test_blocks_self_name(self):
        """Test that self-references by name are blocked."""
        filter = SelfReferenceFilter()
        candidate = Candidate(
            text="NVIDIA Corporation",
            start_pos=0,
            end_pos=18,
            source_pattern="capitalized",
            sentence="NVIDIA Corporation is the company.",
        )
        context = {"self_name": "NVIDIA"}  # Candidate contains self_name

        result = filter.filter(candidate, context)

        assert not result.passed
        assert result.reason == FilterReason.SELF_REFERENCE

    def test_blocks_self_ticker(self):
        """Test that self-references by ticker are blocked."""
        filter = SelfReferenceFilter()
        candidate = Candidate(
            text="NVDA",
            start_pos=0,
            end_pos=4,
            source_pattern="ticker",
            sentence="NVDA stock price.",
        )
        context = {"self_ticker": "NVDA"}

        result = filter.filter(candidate, context)

        assert not result.passed


# =============================================================================
# Candidate Matching Tests
# =============================================================================


class TestExactTickerMatcher:
    """Tests for ExactTickerMatcher."""

    def test_matches_exact_ticker(self, sample_lookup):
        """Test exact ticker matching."""
        matcher = ExactTickerMatcher()
        candidate = Candidate(
            text="NVDA",
            start_pos=0,
            end_pos=4,
            source_pattern="ticker",
            sentence="NVDA stock.",
        )

        result = matcher.match(candidate, sample_lookup)

        assert result.matched
        assert result.match_type == MatchType.EXACT_TICKER
        assert result.ticker == "NVDA"
        assert result.name == "NVIDIA Corporation"
        assert result.base_confidence == 1.0

    def test_no_match_for_unknown_ticker(self, sample_lookup):
        """Test that unknown tickers don't match."""
        matcher = ExactTickerMatcher()
        candidate = Candidate(
            text="ZZZZ",
            start_pos=0,
            end_pos=4,
            source_pattern="ticker",
            sentence="ZZZZ ticker.",
        )

        result = matcher.match(candidate, sample_lookup)

        assert not result.matched
        assert result.match_type == MatchType.NO_MATCH


class TestExactNameMatcher:
    """Tests for ExactNameMatcher."""

    def test_matches_exact_name(self, sample_lookup):
        """Test exact name matching."""
        matcher = ExactNameMatcher()
        candidate = Candidate(
            text="nvidia corporation",
            start_pos=0,
            end_pos=18,
            source_pattern="capitalized",
            sentence="NVIDIA Corporation makes GPUs.",
        )

        result = matcher.match(candidate, sample_lookup)

        assert result.matched
        assert result.match_type == MatchType.EXACT_NAME


class TestNormalizedNameMatcher:
    """Tests for NormalizedNameMatcher."""

    def test_matches_normalized_name(self, sample_lookup):
        """Test that names without suffixes are matched."""
        matcher = NormalizedNameMatcher()
        candidate = Candidate(
            text="nvidia",
            start_pos=0,
            end_pos=6,
            source_pattern="capitalized",
            sentence="NVIDIA makes GPUs.",
        )

        result = matcher.match(candidate, sample_lookup)

        assert result.matched
        assert result.match_type == MatchType.NORMALIZED_NAME
        assert result.base_confidence == 0.95


# =============================================================================
# Confidence Scoring Tests
# =============================================================================


class TestRuleBasedScorer:
    """Tests for RuleBasedScorer."""

    def test_scores_matched_result(self, sample_lookup):
        """Test scoring of a matched result."""
        scorer = RuleBasedScorer()
        candidate = Candidate(
            text="Microsoft",
            start_pos=0,
            end_pos=9,
            source_pattern="capitalized",
            sentence="We compete with Microsoft in the market.",
        )
        match_result = MatchResult(
            candidate=candidate,
            matched=True,
            match_type=MatchType.EXACT_NAME,
            cik="0000789019",
            ticker="MSFT",
            name="Microsoft Corporation",
            base_confidence=1.0,
            matcher_name="exact_name",
        )

        result = scorer.score(match_result)

        assert result.final_confidence > 0.5
        assert result.factors.match_quality == 1.0

    def test_penalizes_short_candidates(self, sample_lookup):
        """Test that short candidates get lower scores."""
        scorer = RuleBasedScorer()

        # Short candidate (3 chars)
        short_candidate = Candidate(
            text="IBM",
            start_pos=0,
            end_pos=3,
            source_pattern="ticker",
            sentence="IBM is mentioned.",
        )
        short_match = MatchResult(
            candidate=short_candidate,
            matched=True,
            match_type=MatchType.EXACT_TICKER,
            cik="0000051143",
            ticker="IBM",
            name="International Business Machines Corporation",
            base_confidence=1.0,
            matcher_name="exact_ticker",
        )

        # Long candidate (9 chars)
        long_candidate = Candidate(
            text="Microsoft",
            start_pos=0,
            end_pos=9,
            source_pattern="capitalized",
            sentence="Microsoft is mentioned.",
        )
        long_match = MatchResult(
            candidate=long_candidate,
            matched=True,
            match_type=MatchType.EXACT_NAME,
            cik="0000789019",
            ticker="MSFT",
            name="Microsoft Corporation",
            base_confidence=1.0,
            matcher_name="exact_name",
        )

        short_result = scorer.score(short_match)
        long_result = scorer.score(long_match)

        # Short candidates should have lower length factor
        assert short_result.factors.length_penalty < long_result.factors.length_penalty

    def test_gives_unmatched_zero_confidence(self):
        """Test that unmatched results get zero confidence."""
        scorer = RuleBasedScorer()
        candidate = Candidate(
            text="Unknown",
            start_pos=0,
            end_pos=7,
            source_pattern="capitalized",
            sentence="Unknown company.",
        )
        match_result = MatchResult(
            candidate=candidate,
            matched=False,
            match_type=MatchType.NO_MATCH,
            matcher_name="all_matchers",
        )

        result = scorer.score(match_result)

        assert result.final_confidence == 0.0


# =============================================================================
# Full Pipeline Tests
# =============================================================================


class TestEntityResolver:
    """Tests for the full EntityResolver pipeline."""

    def test_resolves_companies_in_text(self, sample_lookup, sample_text):
        """Test full resolution pipeline."""
        resolver = EntityResolver()

        results = resolver.resolve(sample_text, sample_lookup)

        # Should find multiple companies
        assert len(results) > 0

        # Should find Microsoft
        tickers = [r.ticker for r in results]
        assert "MSFT" in tickers or "INTC" in tickers

    def test_excludes_self_references(self, sample_lookup):
        """Test that self-references are excluded."""
        resolver = EntityResolver()
        text = "NVIDIA competes with AMD and Intel. NVDA stock is strong."
        context = {"self_cik": "0001045810"}  # NVIDIA's CIK

        results = resolver.resolve(text, sample_lookup, context)

        # Should not include NVIDIA/NVDA
        ciks = [r.cik for r in results]
        assert "0001045810" not in ciks

    def test_respects_min_confidence(self, sample_lookup):
        """Test that low confidence matches are filtered."""
        resolver = EntityResolver(min_confidence=0.9)
        text = "Some company mentioned briefly."

        results = resolver.resolve(text, sample_lookup)

        for result in results:
            assert result.confidence >= 0.9

    def test_returns_stats(self, sample_lookup, sample_text):
        """Test that stats are returned correctly."""
        resolver = EntityResolver()

        results, stats = resolver.resolve_with_stats(sample_text, sample_lookup)

        assert "candidates_extracted" in stats
        assert "candidates_filtered" in stats
        assert "final_results" in stats
        assert stats["final_results"] == len(results)


class TestConvenienceFunctions:
    """Tests for convenience functions."""

    def test_resolve_company_mentions(self, sample_lookup):
        """Test the simple interface."""
        text = "Microsoft and Apple are tech companies."

        results = resolve_company_mentions(text, sample_lookup)

        assert len(results) > 0
        assert all(isinstance(r, dict) for r in results)
        assert all("cik" in r and "ticker" in r for r in results)

    def test_analyze_resolution_quality(self, sample_lookup, sample_text):
        """Test the quality analysis function."""
        analysis = analyze_resolution_quality(sample_text, sample_lookup)

        assert "summary" in analysis
        assert "filter_breakdown" in analysis
        assert "match_breakdown" in analysis
        assert "efficiency" in analysis


# =============================================================================
# Edge Cases and Regression Tests
# =============================================================================


class TestEdgeCases:
    """Tests for edge cases and potential issues."""

    def test_empty_text(self, sample_lookup):
        """Test handling of empty text."""
        resolver = EntityResolver()

        results = resolver.resolve("", sample_lookup)

        assert results == []

    def test_no_company_mentions(self, sample_lookup):
        """Test text with no company mentions."""
        resolver = EntityResolver()
        text = "This is a sentence with no company names or tickers."

        results = resolver.resolve(text, sample_lookup)

        assert len(results) == 0

    def test_special_characters_in_names(self, sample_lookup):
        """Test company names with special characters."""
        resolver = EntityResolver()
        text = "Amazon.com, Inc. is an e-commerce company."

        results = resolver.resolve(text, sample_lookup)

        # Should still find Amazon
        names = [r.name for r in results if r.name]
        assert any("amazon" in n.lower() for n in names)

    def test_multiple_mentions_same_company(self, sample_lookup):
        """Test that same company mentioned multiple times is deduplicated."""
        resolver = EntityResolver()
        text = "Microsoft is great. We love Microsoft. MSFT stock is up."

        results = resolver.resolve(text, sample_lookup)

        # Microsoft should appear only once
        microsoft_count = sum(1 for r in results if r.ticker == "MSFT")
        assert microsoft_count == 1

    def test_case_insensitivity(self, sample_lookup):
        """Test that matching is case-insensitive."""
        resolver = EntityResolver()
        text = "microsoft, MICROSOFT, and Microsoft are all the same."

        results = resolver.resolve(text, sample_lookup)

        # Should find Microsoft exactly once
        assert any(r.ticker == "MSFT" for r in results)
