"""Unit tests for supply chain risk scoring module."""

from unittest.mock import MagicMock

from public_company_graph.supply_chain.risk_scoring import (
    RiskIndicators,
    SupplyChainRisk,
    analyze_supply_chain_risk,
    compute_concentration_risk,
    compute_overall_risk,
    compute_specificity_risk,
    extract_risk_indicators,
)


class TestExtractRiskIndicators:
    """Tests for extract_risk_indicators function."""

    def test_sole_source_detection(self):
        """Should detect sole source language."""
        context = "Regis is our sole source for API manufacturing."
        indicators = extract_risk_indicators(context)
        assert indicators.is_sole_source is True
        assert indicators.is_single_source is True
        assert any("sole_source" in p for p in indicators.raw_patterns_matched)

    def test_single_source_detection(self):
        """Should detect single source language."""
        context = "We rely on a single source supplier for critical components."
        indicators = extract_risk_indicators(context)
        assert indicators.is_sole_source is True
        assert indicators.is_single_source is True

    def test_primary_supplier_detection(self):
        """Should detect primary/key supplier language."""
        context = "Intel is our primary supplier of processors."
        indicators = extract_risk_indicators(context)
        assert indicators.is_primary is True
        assert any("primary" in p for p in indicators.raw_patterns_matched)

    def test_key_supplier_detection(self):
        """Should detect key supplier language."""
        context = "TSMC is a key supplier for our semiconductor needs."
        indicators = extract_risk_indicators(context)
        assert indicators.is_primary is True

    def test_dependency_language_detection(self):
        """Should detect dependency language."""
        context = "We depend on third-party manufacturers for production."
        indicators = extract_risk_indicators(context)
        assert indicators.dependency_mentioned is True
        assert any("dependency" in p for p in indicators.raw_patterns_matched)

    def test_reliance_language_detection(self):
        """Should detect reliance language."""
        context = "Our business relies on continued access to cloud services."
        indicators = extract_risk_indicators(context)
        assert indicators.dependency_mentioned is True

    def test_percentage_extraction(self):
        """Should extract concentration percentages."""
        context = "Sysco accounted for approximately 15.1% of food costs."
        indicators = extract_risk_indicators(context)
        assert indicators.concentration_pct == 15.1

    def test_percentage_extraction_integer(self):
        """Should extract integer percentages."""
        context = "This customer represents 10% of our revenue."
        indicators = extract_risk_indicators(context)
        assert indicators.concentration_pct == 10.0

    def test_percentage_extraction_with_qualifier(self):
        """Should extract percentages with qualifiers like 'approximately'."""
        context = "Cummins accounted for approximately 17% of net sales."
        indicators = extract_risk_indicators(context)
        assert indicators.concentration_pct == 17.0

    def test_no_indicators_found(self):
        """Should return empty indicators when no patterns match."""
        context = "We have a diversified supplier base across multiple regions."
        indicators = extract_risk_indicators(context)
        assert indicators.is_sole_source is False
        assert indicators.is_primary is False
        assert indicators.dependency_mentioned is False
        assert indicators.concentration_pct is None
        assert len(indicators.raw_patterns_matched) == 0

    def test_empty_context(self):
        """Should handle empty context gracefully."""
        indicators = extract_risk_indicators("")
        assert indicators.is_sole_source is False
        assert indicators.concentration_pct is None

    def test_none_context(self):
        """Should handle None context gracefully."""
        indicators = extract_risk_indicators(None)
        assert indicators.is_sole_source is False
        assert indicators.concentration_pct is None

    def test_multiple_indicators(self):
        """Should extract multiple indicators from same context."""
        context = "We depend on a sole source supplier that represents 25% of our costs."
        indicators = extract_risk_indicators(context)
        assert indicators.is_sole_source is True
        assert indicators.dependency_mentioned is True
        assert indicators.concentration_pct == 25.0


class TestComputeConcentrationRisk:
    """Tests for compute_concentration_risk function."""

    def test_sole_source_max_risk(self):
        """Sole source should return maximum concentration risk."""
        indicators = RiskIndicators(is_sole_source=True)
        risk = compute_concentration_risk(5, indicators)
        assert risk == 1.0

    def test_explicit_percentage_scaling(self):
        """Should scale risk based on explicit percentage."""
        indicators = RiskIndicators(concentration_pct=50.0)
        risk = compute_concentration_risk(10, indicators)
        # 50% → 0.5^0.7 ≈ 0.616
        assert 0.6 < risk < 0.65

    def test_primary_supplier_moderate_risk(self):
        """Primary supplier should have moderate concentration risk."""
        indicators = RiskIndicators(is_primary=True)
        risk = compute_concentration_risk(10, indicators)
        assert risk == 0.6

    def test_many_suppliers_low_risk(self):
        """Many suppliers should mean low concentration risk."""
        indicators = RiskIndicators()
        risk = compute_concentration_risk(10, indicators)
        assert risk == 0.1

    def test_few_suppliers_high_risk(self):
        """Few suppliers should mean higher concentration risk."""
        indicators = RiskIndicators()
        risk = compute_concentration_risk(2, indicators)
        assert risk == 0.5

    def test_single_supplier_max_risk(self):
        """Single supplier should be maximum concentration risk."""
        indicators = RiskIndicators()
        risk = compute_concentration_risk(1, indicators)
        assert risk == 1.0

    def test_unknown_supplier_count(self):
        """Unknown supplier count should default to moderate risk."""
        indicators = RiskIndicators()
        risk = compute_concentration_risk(0, indicators)
        assert risk == 0.5


class TestComputeSpecificityRisk:
    """Tests for compute_specificity_risk function."""

    def test_sole_source_high_specificity(self):
        """Sole source implies high specificity risk."""
        indicators = RiskIndicators(is_sole_source=True)
        risk = compute_specificity_risk("3674", "3571", indicators)
        assert risk == 0.9

    def test_dependency_language_moderate_specificity(self):
        """Explicit dependency language indicates moderate specificity."""
        indicators = RiskIndicators(dependency_mentioned=True)
        risk = compute_specificity_risk("3674", "3571", indicators)
        assert risk == 0.7

    def test_primary_supplier_moderate_specificity(self):
        """Primary supplier has moderate specificity."""
        indicators = RiskIndicators(is_primary=True)
        risk = compute_specificity_risk("3674", "3571", indicators)
        assert risk == 0.5

    def test_same_industry_lower_specificity(self):
        """Same 2-digit SIC = likely commodity supplier = lower specificity."""
        indicators = RiskIndicators()
        # Both in 36xx (Electronic Equipment)
        risk = compute_specificity_risk("3674", "3651", indicators)
        assert risk == 0.3

    def test_different_industry_higher_specificity(self):
        """Different industries = potentially specialized input."""
        indicators = RiskIndicators()
        # 36xx (Electronic) vs 28xx (Chemicals)
        risk = compute_specificity_risk("3674", "2834", indicators)
        assert risk == 0.5

    def test_missing_sic_codes(self):
        """Should handle missing SIC codes gracefully."""
        indicators = RiskIndicators()
        risk = compute_specificity_risk(None, None, indicators)
        assert risk == 0.4


class TestComputeOverallRisk:
    """Tests for compute_overall_risk function."""

    def test_weights_sum_to_one(self):
        """Verify weights are applied correctly."""
        # All components at 1.0 should give 1.0 overall
        overall = compute_overall_risk(1.0, 1.0, 1.0)
        assert overall == 1.0

    def test_all_zeros(self):
        """All zero components should give zero overall."""
        overall = compute_overall_risk(0.0, 0.0, 0.0)
        assert overall == 0.0

    def test_concentration_weight(self):
        """Concentration should have 40% weight."""
        # Only concentration risk at 1.0
        overall = compute_overall_risk(1.0, 0.0, 0.0)
        assert overall == 0.40

    def test_specificity_weight(self):
        """Specificity should have 35% weight."""
        # Only specificity risk at 1.0
        overall = compute_overall_risk(0.0, 1.0, 0.0)
        assert overall == 0.35

    def test_dependency_weight(self):
        """Dependency should have 25% weight."""
        # Only dependency risk at 1.0
        overall = compute_overall_risk(0.0, 0.0, 1.0)
        assert overall == 0.25


class TestSupplyChainRiskDataclass:
    """Tests for SupplyChainRisk dataclass."""

    def test_dataclass_creation(self):
        """Should create SupplyChainRisk with all fields."""
        risk = SupplyChainRisk(
            company_ticker="AAPL",
            supplier_ticker="TSMC",
            company_name="Apple Inc.",
            supplier_name="Taiwan Semiconductor",
            concentration_risk=0.8,
            specificity_risk=0.7,
            dependency_risk=0.6,
            overall_score=0.72,
            is_sole_source=True,
            is_primary=False,
            concentration_pct=25.0,
        )
        assert risk.company_ticker == "AAPL"
        assert risk.overall_score == 0.72
        assert risk.is_sole_source is True


class TestAnalyzeSupplyChainRisk:
    """Tests for analyze_supply_chain_risk function."""

    def test_empty_results(self):
        """Should return empty list when no suppliers found."""
        mock_driver = MagicMock()
        mock_session = MagicMock()
        mock_driver.session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_driver.session.return_value.__exit__ = MagicMock(return_value=False)
        mock_session.run.return_value = []

        risks = analyze_supply_chain_risk(mock_driver, "UNKNOWN", database="test")
        assert risks == []

    def test_processes_supplier_relationships(self):
        """Should process supplier relationships and compute risk."""
        mock_driver = MagicMock()
        mock_session = MagicMock()
        mock_driver.session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_driver.session.return_value.__exit__ = MagicMock(return_value=False)

        # Mock a single supplier relationship
        mock_record = {
            "company_ticker": "XERS",
            "company_name": "Xeris Biopharma",
            "company_sic": "2834",
            "supplier_ticker": "RGS",
            "supplier_name": "Regis Corp",
            "supplier_sic": "2834",
            "context": "Regis is our sole source for API manufacturing.",
            "confidence": 1.0,
            "supplier_count": 1,
        }
        mock_session.run.return_value = [mock_record]

        risks = analyze_supply_chain_risk(mock_driver, "XERS", database="test")

        assert len(risks) == 1
        assert risks[0].company_ticker == "XERS"
        assert risks[0].supplier_ticker == "RGS"
        assert risks[0].is_sole_source is True
        # Sole source should have high overall risk
        assert risks[0].overall_score > 0.7


class TestRiskIndicatorsDataclass:
    """Tests for RiskIndicators dataclass."""

    def test_default_values(self):
        """Should have sensible defaults."""
        indicators = RiskIndicators()
        assert indicators.is_sole_source is False
        assert indicators.is_single_source is False
        assert indicators.is_primary is False
        assert indicators.concentration_pct is None
        assert indicators.dependency_mentioned is False
        assert indicators.raw_patterns_matched == []

    def test_pattern_tracking(self):
        """Should track matched patterns."""
        indicators = RiskIndicators(
            is_sole_source=True,
            raw_patterns_matched=["sole_source: \\bsole\\s+source\\b"],
        )
        assert len(indicators.raw_patterns_matched) == 1
        assert "sole_source" in indicators.raw_patterns_matched[0]
