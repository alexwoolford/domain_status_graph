#!/usr/bin/env python
"""
Analyze supply chain risk for a company or compute risks across all relationships.

Research foundation:
- P25: Cohen & Frazzini (2008) "Economic Links and Predictable Returns"
- P26: Barrot & Sauvagnat (2016) "Input Specificity and Propagation of Idiosyncratic Shocks"

Usage:
    # Analyze a specific company's supply chain risk
    python scripts/analyze_supply_chain.py AVGO

    # Analyze downstream exposure if a supplier has problems
    python scripts/analyze_supply_chain.py --exposure TSMC

    # Output as JSON
    python scripts/analyze_supply_chain.py NVDA --json

    # Compute supply chain risk properties for all relationships
    python scripts/analyze_supply_chain.py --compute-all --execute
"""

import argparse
import json
import logging
import sys
from datetime import UTC, datetime

from public_company_graph.config import get_neo4j_database
from public_company_graph.neo4j.connection import get_neo4j_driver
from public_company_graph.supply_chain import (
    SupplyChainRisk,
    analyze_supply_chain_exposure,
    analyze_supply_chain_risk,
    extract_risk_indicators,
)

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


def format_risk_output(risks: list[SupplyChainRisk]) -> str:
    """Format risk analysis as human-readable text."""
    if not risks:
        return "No supplier relationships found."

    lines = []
    lines.append("=" * 70)
    lines.append(f"SUPPLY CHAIN RISK ANALYSIS: {risks[0].company_ticker}")
    lines.append("=" * 70)
    lines.append(f"\nCompany: {risks[0].company_name}")
    lines.append(f"Suppliers analyzed: {len(risks)}")

    # Sort by overall risk score
    risks_sorted = sorted(risks, key=lambda r: r.overall_score, reverse=True)

    # Summary
    high_risk = [r for r in risks if r.overall_score >= 0.6]
    sole_source = [r for r in risks if r.is_sole_source]

    lines.append(f"\nHigh-risk suppliers: {len(high_risk)}")
    lines.append(f"Sole/single source suppliers: {len(sole_source)}")

    if sole_source:
        lines.append("\n⚠️  SOLE SOURCE DEPENDENCIES:")
        for r in sole_source:
            lines.append(f"   • {r.supplier_ticker}: {r.supplier_name}")

    lines.append("\n" + "-" * 70)
    lines.append("SUPPLIER RISK BREAKDOWN")
    lines.append("-" * 70)

    for risk in risks_sorted[:10]:  # Top 10
        bar_len = int(risk.overall_score * 20)
        bar = "█" * bar_len + "░" * (20 - bar_len)

        lines.append(f"\n  {risk.supplier_ticker}: {risk.supplier_name}")
        lines.append(f"    [{bar}] Overall: {risk.overall_score:.2f}")
        lines.append(
            f"    Components: concentration={risk.concentration_risk:.2f}, "
            f"specificity={risk.specificity_risk:.2f}, "
            f"dependency={risk.dependency_risk:.2f}"
        )

        flags = []
        if risk.is_sole_source:
            flags.append("⚠️ SOLE SOURCE")
        if risk.is_primary:
            flags.append("★ PRIMARY")
        if risk.concentration_pct:
            flags.append(f"{risk.concentration_pct:.1f}% concentration")
        if flags:
            lines.append(f"    Flags: {', '.join(flags)}")

    if len(risks) > 10:
        lines.append(f"\n  ... and {len(risks) - 10} more suppliers")

    lines.append("\n" + "=" * 70)
    return "\n".join(lines)


def format_exposure_output(exposure: dict, supplier_ticker: str) -> str:
    """Format exposure analysis as human-readable text."""
    lines = []
    lines.append("=" * 70)
    lines.append(f"SUPPLY CHAIN EXPOSURE ANALYSIS: {supplier_ticker}")
    lines.append("=" * 70)
    lines.append(
        f"\nIf {supplier_ticker} experiences disruption, the following companies are affected:"
    )
    lines.append(f"\nTotal downstream exposure: {exposure['total_exposure']} companies")

    if exposure["direct_customers"]:
        lines.append(f"\n🔴 DIRECT CUSTOMERS ({len(exposure['direct_customers'])})")
        for c in sorted(
            exposure["direct_customers"], key=lambda x: x["impact_score"], reverse=True
        ):
            sole = " ⚠️ SOLE SOURCE" if c.get("is_sole_source") else ""
            lines.append(f"   • {c['ticker']}: {c['name']} (impact: {c['impact_score']:.2f}){sole}")

    if exposure["indirect_customers"]:
        lines.append(f"\n🟡 INDIRECT CUSTOMERS ({len(exposure['indirect_customers'])} - 2nd order)")
        for c in exposure["indirect_customers"][:10]:
            lines.append(f"   • {c['ticker']}: {c['name']} (impact: {c['impact_score']:.2f})")
        if len(exposure["indirect_customers"]) > 10:
            lines.append(f"   ... and {len(exposure['indirect_customers']) - 10} more")

    lines.append("\n" + "=" * 70)
    return "\n".join(lines)


def compute_all_supply_chain_risks(driver, database: str, execute: bool = False):
    """Compute and store supply chain risk properties for all relationships."""
    with driver.session(database=database) as session:
        # Count relationships
        result = session.run("MATCH ()-[r:HAS_SUPPLIER]->() RETURN count(r) as count")
        supplier_count = result.single()["count"]

        result = session.run("MATCH ()-[r:HAS_CUSTOMER]->() RETURN count(r) as count")
        customer_count = result.single()["count"]

        logger.info(f"HAS_SUPPLIER relationships: {supplier_count}")
        logger.info(f"HAS_CUSTOMER relationships: {customer_count}")

        if not execute:
            logger.info("\nDRY RUN - Pass --execute to update relationships")
            logger.info("This will add risk properties to all supply chain relationships")
            return

        # Process HAS_SUPPLIER relationships in batches
        logger.info("\nProcessing HAS_SUPPLIER relationships...")
        batch_size = 500
        processed = 0

        result = session.run(
            """
            MATCH (c:Company)-[r:HAS_SUPPLIER]->(s:Company)
            OPTIONAL MATCH (c)-[all_suppliers:HAS_SUPPLIER]->(:Company)
            WITH c, r, s, count(DISTINCT all_suppliers) as supplier_count
            RETURN id(r) as rel_id,
                   r.context as context,
                   c.sic_code as customer_sic,
                   s.sic_code as supplier_sic,
                   supplier_count
            """
        )

        updates = []
        for record in result:
            indicators = extract_risk_indicators(record["context"])

            # Compute risk components
            concentration = compute_concentration_risk_simple(record["supplier_count"], indicators)
            specificity = compute_specificity_risk_simple(
                record["supplier_sic"], record["customer_sic"], indicators
            )
            dependency = 0.8 if indicators.dependency_mentioned else 0.3

            overall = 0.40 * concentration + 0.35 * specificity + 0.25 * dependency

            updates.append(
                {
                    "rel_id": record["rel_id"],
                    "concentration_risk": round(concentration, 3),
                    "specificity_risk": round(specificity, 3),
                    "dependency_risk": round(dependency, 3),
                    "overall_risk": round(overall, 3),
                    "is_sole_source": indicators.is_sole_source,
                    "is_primary": indicators.is_primary,
                    "concentration_pct": indicators.concentration_pct,
                    "computed_at": datetime.now(UTC).isoformat(),
                }
            )

            if len(updates) >= batch_size:
                _apply_updates(session, updates)
                processed += len(updates)
                logger.info(f"  Processed {processed}/{supplier_count}")
                updates = []

        # Final batch
        if updates:
            _apply_updates(session, updates)
            processed += len(updates)
            logger.info(f"  Processed {processed}/{supplier_count}")

        logger.info(f"\n✓ Updated {processed} HAS_SUPPLIER relationships")


def _apply_updates(session, updates: list[dict]):
    """Apply batch updates to relationships."""
    session.run(
        """
        UNWIND $updates as update
        MATCH ()-[r]->() WHERE id(r) = update.rel_id
        SET r.concentration_risk = update.concentration_risk,
            r.specificity_risk = update.specificity_risk,
            r.dependency_risk = update.dependency_risk,
            r.supply_chain_risk = update.overall_risk,
            r.is_sole_source = update.is_sole_source,
            r.is_primary = update.is_primary,
            r.concentration_pct = update.concentration_pct,
            r.risk_computed_at = update.computed_at
        """,
        updates=updates,
    )


def compute_concentration_risk_simple(supplier_count: int, indicators) -> float:
    """Simple concentration risk computation."""
    if indicators.is_sole_source:
        return 1.0
    if indicators.concentration_pct:
        pct = indicators.concentration_pct / 100.0
        return min(1.0, pct**0.7)
    if indicators.is_primary:
        return 0.6
    if supplier_count <= 0:
        return 0.5
    return min(1.0, 1.0 / supplier_count)


def compute_specificity_risk_simple(supplier_sic, customer_sic, indicators) -> float:
    """Simple specificity risk computation."""
    if indicators.is_sole_source:
        return 0.9
    if indicators.dependency_mentioned:
        return 0.7
    if indicators.is_primary:
        return 0.5
    if supplier_sic and customer_sic:
        if supplier_sic[:2] == customer_sic[:2]:
            return 0.3
        return 0.5
    return 0.4


def main():
    parser = argparse.ArgumentParser(description="Analyze supply chain risk for companies")
    parser.add_argument("ticker", nargs="?", help="Company ticker to analyze")
    parser.add_argument(
        "--exposure",
        action="store_true",
        help="Analyze downstream exposure if this supplier has problems",
    )
    parser.add_argument("--json", action="store_true", help="Output as JSON")
    parser.add_argument(
        "--compute-all",
        action="store_true",
        help="Compute risk properties for all relationships",
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Execute changes (required for --compute-all)",
    )

    args = parser.parse_args()

    if not args.ticker and not args.compute_all:
        parser.error("Either provide a ticker or use --compute-all")

    try:
        driver = get_neo4j_driver()
        database = get_neo4j_database()
    except Exception as e:
        logger.error(f"Failed to connect to Neo4j: {e}")
        sys.exit(1)

    try:
        if args.compute_all:
            compute_all_supply_chain_risks(driver, database, args.execute)
        elif args.exposure:
            exposure = analyze_supply_chain_exposure(driver, args.ticker, database=database)
            if args.json:
                print(json.dumps(exposure, indent=2))
            else:
                print(format_exposure_output(exposure, args.ticker))
        else:
            risks = analyze_supply_chain_risk(driver, args.ticker, database=database)
            if args.json:
                output = [
                    {
                        "company_ticker": r.company_ticker,
                        "supplier_ticker": r.supplier_ticker,
                        "supplier_name": r.supplier_name,
                        "overall_score": r.overall_score,
                        "concentration_risk": r.concentration_risk,
                        "specificity_risk": r.specificity_risk,
                        "dependency_risk": r.dependency_risk,
                        "is_sole_source": r.is_sole_source,
                        "is_primary": r.is_primary,
                        "concentration_pct": r.concentration_pct,
                    }
                    for r in risks
                ]
                print(json.dumps(output, indent=2))
            else:
                print(format_risk_output(risks))
    finally:
        driver.close()


if __name__ == "__main__":
    main()
