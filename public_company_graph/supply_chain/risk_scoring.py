"""Supply chain risk scoring.

Research foundation:
- P25: Cohen & Frazzini (2008) - Supplier concentration predicts returns
- P26: Barrot & Sauvagnat (2016) - Input specificity amplifies shock propagation

Key insights:
1. Companies with concentrated supplier bases are more exposed to supply chain shocks
2. "Specific" suppliers (hard to replace) create more vulnerability
3. Multi-hop effects: Supplier problems propagate to customers and their customers
"""

import logging
import re
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


@dataclass
class RiskIndicators:
    """Risk indicators extracted from relationship context."""

    is_sole_source: bool = False
    is_single_source: bool = False
    is_primary: bool = False
    concentration_pct: float | None = None  # If % of revenue/sales mentioned
    dependency_mentioned: bool = False  # "depend on", "reliance on", etc.
    raw_patterns_matched: list[str] = field(default_factory=list)


@dataclass
class SupplyChainRisk:
    """Computed supply chain risk for a company-supplier relationship."""

    company_ticker: str
    supplier_ticker: str
    company_name: str
    supplier_name: str

    # Individual risk components (0-1 scale)
    concentration_risk: float  # Risk from supplier concentration
    specificity_risk: float  # Risk from input specificity (hard to replace)
    dependency_risk: float  # Risk from explicit dependency language

    # Aggregated score
    overall_score: float  # Weighted combination (0-1)

    # Evidence
    is_sole_source: bool = False
    is_primary: bool = False
    concentration_pct: float | None = None
    indicators: RiskIndicators | None = None


# =============================================================================
# PATTERN DEFINITIONS FOR RISK EXTRACTION
# =============================================================================

# Patterns indicating sole/single source dependency
SOLE_SOURCE_PATTERNS = [
    r"\bsole\s+source\b",
    r"\bsingle\s+source\b",
    r"\bonly\s+source\b",
    r"\bsole\s+supplier\b",
    r"\bsingle\s+supplier\b",
    r"\bonly\s+supplier\b",
    r"\bexclusive\s+supplier\b",
    r"\bexclusive\s+source\b",
]

# Patterns indicating primary/major relationship
PRIMARY_PATTERNS = [
    r"\bprimary\s+supplier\b",
    r"\bprincipal\s+supplier\b",
    r"\bmajor\s+supplier\b",
    r"\bkey\s+supplier\b",
    r"\bprimary\s+source\b",
    r"\bprincipal\s+source\b",
]

# Patterns indicating dependency
DEPENDENCY_PATTERNS = [
    r"\bdepend(?:s|ed|ent|ence|ency)?\s+(?:on|upon)\b",
    r"\breli(?:es|ed|ance|ant)\s+(?:on|upon)\b",
    r"\bcritical\s+(?:to|for)\b",
    r"\bessential\s+(?:to|for)\b",
]

# Pattern to extract percentages (e.g., "15.1%", "approximately 17%")
PERCENTAGE_PATTERN = (
    r"(?:approximately|about|roughly|around|over|under|nearly)?\s*(\d+(?:\.\d+)?)\s*%"
)


def extract_risk_indicators(context: str | None) -> RiskIndicators:
    """
    Extract supply chain risk indicators from relationship context.

    Args:
        context: The context string from the HAS_SUPPLIER/HAS_CUSTOMER relationship

    Returns:
        RiskIndicators with extracted signals
    """
    if not context:
        return RiskIndicators()

    context_lower = context.lower()
    indicators = RiskIndicators()

    # Check for sole/single source patterns
    for pattern in SOLE_SOURCE_PATTERNS:
        if re.search(pattern, context_lower):
            indicators.is_sole_source = True
            indicators.is_single_source = True
            indicators.raw_patterns_matched.append(f"sole_source: {pattern}")
            break

    # Check for primary supplier patterns
    for pattern in PRIMARY_PATTERNS:
        if re.search(pattern, context_lower):
            indicators.is_primary = True
            indicators.raw_patterns_matched.append(f"primary: {pattern}")
            break

    # Check for dependency language
    for pattern in DEPENDENCY_PATTERNS:
        if re.search(pattern, context_lower):
            indicators.dependency_mentioned = True
            indicators.raw_patterns_matched.append(f"dependency: {pattern}")
            break

    # Extract concentration percentage
    pct_match = re.search(PERCENTAGE_PATTERN, context_lower)
    if pct_match:
        try:
            pct = float(pct_match.group(1))
            # Only consider reasonable percentages (1-100%)
            if 1 <= pct <= 100:
                indicators.concentration_pct = pct
                indicators.raw_patterns_matched.append(f"percentage: {pct}%")
        except ValueError:
            pass

    return indicators


def compute_concentration_risk(
    supplier_count: int,
    indicators: RiskIndicators,
    is_customer_rel: bool = False,
) -> float:
    """
    Compute concentration risk score.

    Factors:
    - Fewer suppliers = higher concentration risk
    - Explicit percentage mentioned = use that
    - Sole source = maximum concentration risk

    Args:
        supplier_count: Number of suppliers this company has
        indicators: Extracted risk indicators
        is_customer_rel: True if this is a HAS_CUSTOMER relationship

    Returns:
        Concentration risk score (0-1)
    """
    if indicators.is_sole_source:
        return 1.0

    # If we have an explicit percentage
    if indicators.concentration_pct:
        # 10% concentration = 0.1 risk, 50% = 0.5 risk, etc.
        # Apply non-linear scaling: higher concentrations are disproportionately risky
        pct = indicators.concentration_pct / 100.0
        # Use square root to emphasize higher concentrations
        return min(1.0, pct**0.7)

    # If primary/key supplier, moderate concentration risk
    if indicators.is_primary:
        return 0.6

    # Default: base on inverse of supplier count
    # 1 supplier = 1.0, 5 suppliers = 0.2, 10+ = 0.1
    if supplier_count <= 0:
        return 0.5  # Unknown
    return min(1.0, 1.0 / supplier_count)


def compute_specificity_risk(
    supplier_sic: str | None,
    customer_sic: str | None,
    indicators: RiskIndicators,
) -> float:
    """
    Compute input specificity risk.

    "Specific" inputs are harder to replace, creating more vulnerability.

    Factors:
    - Same industry (same 2-digit SIC) = lower specificity (commodity inputs)
    - Different industries = potentially more specific
    - Sole source language = high specificity

    Args:
        supplier_sic: Supplier's SIC code
        customer_sic: Customer's SIC code
        indicators: Extracted risk indicators

    Returns:
        Specificity risk score (0-1)
    """
    # Sole source implies high specificity
    if indicators.is_sole_source:
        return 0.9

    # Explicit dependency language
    if indicators.dependency_mentioned:
        return 0.7

    # Primary supplier = moderate specificity
    if indicators.is_primary:
        return 0.5

    # Industry-based heuristic
    if supplier_sic and customer_sic:
        # Same 2-digit SIC = similar industry = lower specificity
        if supplier_sic[:2] == customer_sic[:2]:
            return 0.3  # Likely commodity supplier
        # Different industries = potentially specialized input
        return 0.5

    # Default moderate specificity
    return 0.4


def compute_overall_risk(
    concentration_risk: float,
    specificity_risk: float,
    dependency_risk: float,
) -> float:
    """
    Compute overall supply chain risk score.

    Weights based on research:
    - Concentration: 40% (Cohen & Frazzini)
    - Specificity: 35% (Barrot & Sauvagnat)
    - Dependency: 25% (explicit language signals)

    Args:
        concentration_risk: Concentration risk component
        specificity_risk: Specificity risk component
        dependency_risk: Dependency language risk component

    Returns:
        Overall risk score (0-1)
    """
    return 0.40 * concentration_risk + 0.35 * specificity_risk + 0.25 * dependency_risk


def analyze_supply_chain_risk(
    driver,
    company_ticker: str,
    database: str | None = None,
) -> list[SupplyChainRisk]:
    """
    Analyze supply chain risk for a company.

    Args:
        driver: Neo4j driver
        company_ticker: Ticker of company to analyze
        database: Neo4j database name

    Returns:
        List of SupplyChainRisk objects for each supplier relationship
    """
    risks: list[SupplyChainRisk] = []

    with driver.session(database=database) as session:
        # Get all supplier relationships and context
        result = session.run(
            """
            MATCH (c:Company {ticker: $ticker})-[r:HAS_SUPPLIER]->(s:Company)
            OPTIONAL MATCH (c)-[all_suppliers:HAS_SUPPLIER]->(:Company)
            WITH c, r, s, count(DISTINCT all_suppliers) as supplier_count
            RETURN c.ticker as company_ticker,
                   c.name as company_name,
                   c.sic_code as company_sic,
                   s.ticker as supplier_ticker,
                   s.name as supplier_name,
                   s.sic_code as supplier_sic,
                   r.context as context,
                   r.confidence as confidence,
                   supplier_count
            """,
            ticker=company_ticker,
        )

        for record in result:
            context = record["context"]
            indicators = extract_risk_indicators(context)

            concentration_risk = compute_concentration_risk(
                supplier_count=record["supplier_count"],
                indicators=indicators,
                is_customer_rel=False,
            )

            specificity_risk = compute_specificity_risk(
                supplier_sic=record["supplier_sic"],
                customer_sic=record["company_sic"],
                indicators=indicators,
            )

            dependency_risk = 0.8 if indicators.dependency_mentioned else 0.3

            overall = compute_overall_risk(concentration_risk, specificity_risk, dependency_risk)

            risks.append(
                SupplyChainRisk(
                    company_ticker=record["company_ticker"],
                    supplier_ticker=record["supplier_ticker"],
                    company_name=record["company_name"],
                    supplier_name=record["supplier_name"],
                    concentration_risk=round(concentration_risk, 3),
                    specificity_risk=round(specificity_risk, 3),
                    dependency_risk=round(dependency_risk, 3),
                    overall_score=round(overall, 3),
                    is_sole_source=indicators.is_sole_source,
                    is_primary=indicators.is_primary,
                    concentration_pct=indicators.concentration_pct,
                    indicators=indicators,
                )
            )

    return risks


def analyze_supply_chain_exposure(
    driver,
    supplier_ticker: str,
    database: str | None = None,
    max_hops: int = 2,
) -> dict:
    """
    Analyze downstream exposure if a supplier has problems.

    Based on P26: Shock propagation through supply chains.

    Args:
        driver: Neo4j driver
        supplier_ticker: Ticker of the supplier with potential problems
        database: Neo4j database name
        max_hops: Maximum supply chain depth to analyze

    Returns:
        Dict with affected companies and propagation scores
    """
    affected = {"direct_customers": [], "indirect_customers": [], "total_exposure": 0}

    with driver.session(database=database) as session:
        # Direct customers (1-hop)
        result = session.run(
            """
            MATCH (supplier:Company {ticker: $ticker})<-[r:HAS_SUPPLIER]-(customer:Company)
            RETURN customer.ticker as ticker,
                   customer.name as name,
                   r.context as context,
                   1 as hops
            """,
            ticker=supplier_ticker,
        )

        for record in result:
            indicators = extract_risk_indicators(record["context"])
            impact_score = 1.0 if indicators.is_sole_source else 0.5
            affected["direct_customers"].append(
                {
                    "ticker": record["ticker"],
                    "name": record["name"],
                    "hops": record["hops"],
                    "impact_score": impact_score,
                    "is_sole_source": indicators.is_sole_source,
                }
            )

        # Indirect customers (2-hop) - customers of customers
        if max_hops >= 2:
            result = session.run(
                """
                MATCH (supplier:Company {ticker: $ticker})
                      <-[:HAS_SUPPLIER]-(direct:Company)
                      <-[:HAS_SUPPLIER]-(indirect:Company)
                WHERE indirect.ticker <> $ticker
                RETURN DISTINCT indirect.ticker as ticker,
                       indirect.name as name,
                       2 as hops
                """,
                ticker=supplier_ticker,
            )

            for record in result:
                # 2nd order effects are dampened
                affected["indirect_customers"].append(
                    {
                        "ticker": record["ticker"],
                        "name": record["name"],
                        "hops": record["hops"],
                        "impact_score": 0.25,  # Dampened propagation
                    }
                )

        affected["total_exposure"] = len(affected["direct_customers"]) + len(
            affected["indirect_customers"]
        )

    return affected
