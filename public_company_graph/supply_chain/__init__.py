"""Supply chain risk analysis module.

This module provides supply chain risk scoring based on research from:
- P25: Cohen & Frazzini (2008) "Economic Links and Predictable Returns"
- P26: Barrot & Sauvagnat (2016) "Input Specificity and Propagation of Idiosyncratic Shocks"
"""

from public_company_graph.supply_chain.risk_scoring import (
    SupplyChainRisk,
    analyze_supply_chain_exposure,
    analyze_supply_chain_risk,
    compute_concentration_risk,
    compute_specificity_risk,
    extract_risk_indicators,
)

__all__ = [
    "SupplyChainRisk",
    "analyze_supply_chain_exposure",
    "analyze_supply_chain_risk",
    "compute_concentration_risk",
    "compute_specificity_risk",
    "extract_risk_indicators",
]
