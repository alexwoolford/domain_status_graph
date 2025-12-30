#!/usr/bin/env python3
"""
Optimize similarity weights to maximize validation score.

⚠️ EXPERIMENTAL / FUTURE USE: This script is kept for future use when data quality improves.
Currently, weight optimization shows no improvement because the issue is missing relationships
(30.4% of validation pairs have no relationships), not suboptimal weights.

Once better data sources are integrated (e.g., datamule for comprehensive SEC filings),
this tool will be useful for systematically finding optimal weight combinations.

This script:
1. Loads the famous pairs validation set
2. Tests different weight combinations
3. Scores each combination based on validation results
4. Finds the optimal weights
5. Displays a live leaderboard of top weight combinations

Usage:
    python scripts/optimize_similarity_weights.py [--method grid|random|bayesian] [--iterations N]
"""

import argparse
import logging
import sys
import time
from dataclasses import dataclass

from public_company_graph.cli import (
    get_driver_and_database,
    setup_logging,
    verify_neo4j_connection,
)
from public_company_graph.company.queries import (
    DEFAULT_SIMILARITY_WEIGHTS,
    get_top_similar_companies_query_extended,
)


@dataclass
class WeightResult:
    """Result of evaluating a weight configuration."""

    weights: dict[str, float]
    score: float
    passed: int
    failed: int
    not_found: int
    missing_company: int

    def summary(self) -> str:
        """Return a summary string for this result."""
        total = self.passed + self.failed + self.not_found
        pass_rate = self.passed / total * 100 if total > 0 else 0
        return (
            f"Score: {self.score:6.2f} | "
            f"Pass: {self.passed:2d}/{total:2d} ({pass_rate:5.1f}%) | "
            f"Fail: {self.failed:2d} | NotFound: {self.not_found:2d}"
        )

    def weights_str(self) -> str:
        """Return a string of weight values."""
        return " | ".join(f"{k[:4]}={v:.1f}" for k, v in sorted(self.weights.items()))


# Famous pairs for validation
FAMOUS_PAIRS = [
    # Beverages
    ("KO", "PEP", 1),
    ("PEP", "KO", 1),
    ("KO", "KDP", 3),
    ("PEP", "KDP", 3),
    # Retail - Big Box
    ("WMT", "TGT", 1),
    ("TGT", "WMT", 1),
    ("WMT", "COST", 3),
    ("COST", "WMT", 3),
    # Retail - Home Improvement
    ("HD", "LOW", 1),
    ("LOW", "HD", 1),
    # Technology - Software
    ("AAPL", "MSFT", 1),
    ("MSFT", "AAPL", 1),
    ("GOOG", "MSFT", 3),
    ("META", "GOOG", 3),
    # Technology - Semiconductors
    ("NVDA", "AMD", 1),
    ("AMD", "NVDA", 1),
    ("INTC", "AMD", 1),
    ("AMD", "INTC", 1),
    # Financial - Credit Cards
    ("V", "MA", 1),
    ("MA", "V", 1),
    ("AXP", "V", 3),
    # Restaurants
    ("MCD", "SBUX", 1),
    ("SBUX", "MCD", 1),
    ("YUM", "MCD", 3),
    ("CMG", "SBUX", 3),
    # Healthcare - Pharma
    ("JNJ", "PFE", 1),
    ("PFE", "JNJ", 1),
    ("ABBV", "JNJ", 3),
    # Healthcare - Insurance
    ("UNH", "CVS", 3),
    # Energy
    ("XOM", "CVX", 1),
    ("CVX", "XOM", 1),
    ("COP", "XOM", 3),
    # Automotive
    ("TSLA", "GM", 3),
    ("GM", "TSLA", 3),
    # Aerospace
    ("LMT", "RTX", 1),
    ("RTX", "LMT", 1),
    ("NOC", "LMT", 3),
    # Media
    ("DIS", "NFLX", 3),
    ("NFLX", "DIS", 3),
    # Apparel
    ("NKE", "UA", 1),
    ("UA", "NKE", 1),
    ("LULU", "NKE", 3),
    # Consumer Goods
    ("PG", "CL", 1),
    ("CL", "PG", 1),
    # E-commerce
    ("AMZN", "WMT", 3),
    ("WMT", "AMZN", 3),
]

logger = logging.getLogger(__name__)


def get_company_rank(
    driver, ticker1: str, ticker2: str, weights: dict[str, float], top_n: int, database: str
) -> int | None:
    """Get the rank of ticker2 in ticker1's similar companies list."""
    query = get_top_similar_companies_query_extended(ticker1, limit=top_n, weights=weights)
    with driver.session(database=database) as session:
        result = session.run(query)
        for i, record in enumerate(result, 1):
            ticker_key = "ticker" if "ticker" in record.keys() else "c2.ticker"
            ticker = record[ticker_key]
            if ticker == ticker2:
                return i
    return None


def score_weights(
    driver, weights: dict[str, float], pairs: list[tuple[str, str, int]], database: str
) -> WeightResult:
    """
    Score a set of weights based on validation pairs.

    Returns:
        WeightResult with score and statistics
    """
    stats = {"passed": 0, "failed": 0, "not_found": 0, "missing_company": 0}

    for ticker1, ticker2, expected_rank in pairs:
        # Check if companies exist
        with driver.session(database=database) as session:
            result1 = session.run(
                "MATCH (c:Company {ticker: $ticker}) RETURN c.ticker AS ticker",
                ticker=ticker1,
            )
            company1 = result1.single()
            result2 = session.run(
                "MATCH (c:Company {ticker: $ticker}) RETURN c.ticker AS ticker",
                ticker=ticker2,
            )
            company2 = result2.single()

        if not company1 or not company2:
            stats["missing_company"] += 1
            continue

        # Get rank
        rank = get_company_rank(driver, ticker1, ticker2, weights, top_n=20, database=database)

        if rank is None:
            stats["not_found"] += 1
        elif rank <= expected_rank:
            stats["passed"] += 1
        else:
            stats["failed"] += 1

    # Calculate score: passed pairs get points, failed/not_found get penalties
    # Higher score is better
    total_tested = stats["passed"] + stats["failed"] + stats["not_found"]
    if total_tested == 0:
        score = -1000.0
    else:
        # Score: passed pairs get +2, failed get -1, not_found get -2
        # This prioritizes getting pairs in the top-N over just ranking them
        score = stats["passed"] * 2.0 - stats["failed"] * 1.0 - stats["not_found"] * 2.0

        # Bonus for high pass rate (up to 20 points for 100% pass rate)
        pass_rate = stats["passed"] / total_tested if total_tested > 0 else 0
        score += pass_rate * 20

        # Penalty for not_found (these are worst - no relationships at all)
        not_found_rate = stats["not_found"] / total_tested if total_tested > 0 else 0
        score -= not_found_rate * 10

    return WeightResult(
        weights=weights.copy(),
        score=score,
        passed=stats["passed"],
        failed=stats["failed"],
        not_found=stats["not_found"],
        missing_company=stats["missing_company"],
    )


def display_leaderboard(
    leaderboard: list[WeightResult], logger_instance: logging.Logger, max_display: int = 10
) -> None:
    """Display the current leaderboard of top weight configurations."""
    logger_instance.info("")
    logger_instance.info("=" * 100)
    logger_instance.info(
        f"{'LEADERBOARD - TOP ' + str(min(len(leaderboard), max_display)) + ' WEIGHT CONFIGURATIONS':^100}"
    )
    logger_instance.info("=" * 100)
    logger_instance.info(
        f"{'Rank':<5} {'Score':>8} {'Pass':>6} {'Fail':>6} {'NotF':>6} "
        f"{'RISK':>6} {'DESC':>6} {'IND':>6} {'TECH':>6} {'SIZE':>6} {'COMP':>6}"
    )
    logger_instance.info("-" * 100)
    for i, result in enumerate(leaderboard[:max_display], 1):
        w = result.weights
        logger_instance.info(
            f"{i:<5} {result.score:>8.2f} {result.passed:>6} {result.failed:>6} {result.not_found:>6} "
            f"{w.get('SIMILAR_RISK', 0):>6.1f} {w.get('SIMILAR_DESCRIPTION', 0):>6.1f} "
            f"{w.get('SIMILAR_INDUSTRY', 0):>6.1f} {w.get('SIMILAR_TECHNOLOGY', 0):>6.1f} "
            f"{w.get('SIMILAR_SIZE', 0):>6.1f} {w.get('HAS_COMPETITOR', 0):>6.1f}"
        )
    logger_instance.info("=" * 100)
    logger_instance.info("")


def grid_search_weights(
    driver, pairs: list[tuple[str, str, int]], database: str, logger_instance: logging.Logger
) -> tuple[dict[str, float], float, dict[str, int]]:
    """Grid search over weight combinations with live leaderboard."""
    # Leaderboard to track top configurations
    leaderboard: list[WeightResult] = []
    max_leaderboard_size = 20

    # Define search space for REAL Company-Company relationships in the graph:
    # - SIMILAR_RISK (197K) - 10-K risk factor embedding similarity
    # - SIMILAR_DESCRIPTION (210K) - Business description embedding similarity
    # - SIMILAR_TECHNOLOGY (124K) - Technology stack similarity (Company-Company)
    # - SIMILAR_INDUSTRY (260K) - SIC/Industry/Sector match
    # - SIMILAR_SIZE (207K) - Revenue/market cap similarity
    # - HAS_COMPETITOR - Direct competitor relationships (strong signal!)
    #
    # NOT using:
    # - SIMILAR_KEYWORD (71) - Too sparse, fixed at 0.1

    risk_weights = [0.6, 0.8, 1.0, 1.2]  # Risk factors from 10-K
    description_weights = [0.4, 0.6, 0.8, 1.0]  # Business description embedding
    industry_weights = [0.4, 0.6, 0.8]  # SIC/Industry classification
    tech_weights = [0.2, 0.4, 0.6]  # Technology stack similarity
    size_weights = [0.1, 0.2, 0.4]  # Size is common, less discriminative
    competitor_weights = [0.0, 2.0, 4.0, 6.0]  # HAS_COMPETITOR - strong direct signal!

    total_combinations = (
        len(risk_weights)
        * len(description_weights)
        * len(industry_weights)
        * len(tech_weights)
        * len(size_weights)
        * len(competitor_weights)
    )

    # Estimate time: ~0.3 seconds per combination
    estimated_seconds = total_combinations * 0.3
    logger_instance.info(f"Search space: {total_combinations:,} weight combinations")
    logger_instance.info(f"Estimated time: ~{estimated_seconds / 60:.1f} minutes")
    logger_instance.info("")
    logger_instance.info("Weight ranges being tested:")
    logger_instance.info(f"  SIMILAR_RISK:        {risk_weights}")
    logger_instance.info(f"  SIMILAR_DESCRIPTION: {description_weights}")
    logger_instance.info(f"  SIMILAR_INDUSTRY:    {industry_weights}")
    logger_instance.info(f"  SIMILAR_TECHNOLOGY:  {tech_weights}")
    logger_instance.info(f"  SIMILAR_SIZE:        {size_weights}")
    logger_instance.info(f"  HAS_COMPETITOR:      {competitor_weights}")
    logger_instance.info("")

    tested = 0
    start_time = time.time()
    last_leaderboard_display = 0

    for risk_w in risk_weights:
        for desc_w in description_weights:
            for ind_w in industry_weights:
                for tech_w in tech_weights:
                    for size_w in size_weights:
                        for comp_w in competitor_weights:
                            tested += 1
                            weights = {
                                "SIMILAR_RISK": risk_w,
                                "SIMILAR_DESCRIPTION": desc_w,
                                "SIMILAR_INDUSTRY": ind_w,
                                "SIMILAR_TECHNOLOGY": tech_w,
                                "SIMILAR_SIZE": size_w,
                                "HAS_COMPETITOR": comp_w,
                                "SIMILAR_KEYWORD": 0.1,  # Fixed low weight
                            }

                            result = score_weights(driver, weights, pairs, database)

                            # Update leaderboard
                            leaderboard.append(result)
                            leaderboard.sort(key=lambda x: x.score, reverse=True)
                            leaderboard = leaderboard[:max_leaderboard_size]

                            # Progress update with ETA every 50 combinations
                            if tested % 50 == 0 or tested == total_combinations:
                                elapsed = time.time() - start_time
                                rate = tested / elapsed if elapsed > 0 else 0
                                remaining = total_combinations - tested
                                eta_seconds = remaining / rate if rate > 0 else 0
                                eta_minutes = eta_seconds / 60
                                progress_pct = tested / total_combinations * 100

                                logger_instance.info(
                                    f"Progress: {tested:,}/{total_combinations:,} "
                                    f"({progress_pct:.1f}%) | "
                                    f"Rate: {rate * 60:.0f}/min | "
                                    f"ETA: {eta_minutes:.1f}m | "
                                    f"Best: {leaderboard[0].score:.2f} "
                                    f"({leaderboard[0].passed} passed)"
                                )

                            # Display full leaderboard every 200 combinations
                            if (
                                tested - last_leaderboard_display >= 200
                                or tested == total_combinations
                            ):
                                display_leaderboard(leaderboard, logger_instance)
                                last_leaderboard_display = tested

    # Final leaderboard
    logger_instance.info("")
    logger_instance.info("FINAL RESULTS")
    display_leaderboard(leaderboard, logger_instance)

    best = leaderboard[0]
    return (
        best.weights,
        best.score,
        {
            "passed": best.passed,
            "failed": best.failed,
            "not_found": best.not_found,
            "missing_company": best.missing_company,
        },
    )


def optimize_weights(
    driver,
    pairs: list[tuple[str, str, int]],
    method: str = "grid",
    iterations: int = 100,
    database: str = "neo4j",
    logger_instance: logging.Logger | None = None,
) -> tuple[dict[str, float], float, dict[str, int]]:
    """
    Optimize similarity weights.

    Args:
        driver: Neo4j driver
        pairs: List of (ticker1, ticker2, expected_rank) tuples
        method: Optimization method ('grid', 'random', 'bayesian')
        iterations: Number of iterations for random/bayesian
        database: Database name
        logger_instance: Logger to use

    Returns:
        (best_weights, best_score, best_stats)
    """
    log = logger_instance or logger
    if method == "grid":
        return grid_search_weights(driver, pairs, database, log)
    else:
        log.error(f"Method '{method}' not yet implemented. Use 'grid'.")
        sys.exit(1)


def main():
    """Run the optimization script."""
    parser = argparse.ArgumentParser(description="Optimize similarity weights")
    parser.add_argument(
        "--method",
        choices=["grid", "random", "bayesian"],
        default="grid",
        help="Optimization method (default: grid)",
    )
    parser.add_argument(
        "--iterations",
        type=int,
        default=100,
        help="Number of iterations for random/bayesian (default: 100)",
    )
    parser.add_argument(
        "--limit-pairs",
        type=int,
        help="Limit number of pairs to test (for quick testing)",
    )

    args = parser.parse_args()

    script_logger = setup_logging("optimize_similarity_weights", execute=True)

    driver, database = get_driver_and_database(script_logger)

    try:
        if not verify_neo4j_connection(driver, database, script_logger):
            sys.exit(1)

        pairs_to_test = FAMOUS_PAIRS
        if args.limit_pairs:
            pairs_to_test = FAMOUS_PAIRS[: args.limit_pairs]
            script_logger.info(
                f"Testing first {args.limit_pairs} pairs (of {len(FAMOUS_PAIRS)} total)"
            )

        script_logger.info("=" * 100)
        script_logger.info(f"{'SIMILARITY WEIGHT OPTIMIZATION':^100}")
        script_logger.info("=" * 100)
        script_logger.info(f"Testing {len(pairs_to_test)} validation pairs")
        script_logger.info(f"Method: {args.method}")
        script_logger.info("")

        # Test current weights first
        script_logger.info("Testing current (baseline) weights...")
        current_result = score_weights(driver, DEFAULT_SIMILARITY_WEIGHTS, pairs_to_test, database)
        script_logger.info(f"Baseline: {current_result.summary()}")
        script_logger.info(f"Baseline weights: {DEFAULT_SIMILARITY_WEIGHTS}")
        script_logger.info("")

        # Optimize
        best_weights, best_score, best_stats = optimize_weights(
            driver,
            pairs_to_test,
            method=args.method,
            iterations=args.iterations,
            database=database,
            logger_instance=script_logger,
        )

        script_logger.info("")
        script_logger.info("=" * 100)
        script_logger.info(f"{'FINAL OPTIMIZATION RESULTS':^100}")
        script_logger.info("=" * 100)
        script_logger.info("")
        script_logger.info(f"Best score: {best_score:.2f}")
        script_logger.info(f"  Passed:       {best_stats['passed']}")
        script_logger.info(f"  Failed:       {best_stats['failed']}")
        script_logger.info(f"  Not Found:    {best_stats['not_found']}")
        script_logger.info(f"  Missing Co:   {best_stats['missing_company']}")
        script_logger.info("")
        script_logger.info("Optimal weights:")
        for key, value in sorted(best_weights.items()):
            script_logger.info(f"  {key}: {value}")
        script_logger.info("")
        script_logger.info("Improvement over baseline:")
        score_diff = best_score - current_result.score
        script_logger.info(
            f"  Score:  {current_result.score:.2f} → {best_score:.2f} ({score_diff:+.2f})"
        )
        passed_diff = best_stats["passed"] - current_result.passed
        script_logger.info(
            f"  Passed: {current_result.passed} → {best_stats['passed']} ({passed_diff:+d})"
        )
        script_logger.info("")
        script_logger.info("-" * 100)
        script_logger.info("To apply these weights, update DEFAULT_SIMILARITY_WEIGHTS in:")
        script_logger.info("  public_company_graph/company/queries.py")
        script_logger.info("")
        script_logger.info("Example update:")
        script_logger.info("  DEFAULT_SIMILARITY_WEIGHTS = {")
        for key, value in sorted(best_weights.items()):
            script_logger.info(f'    "{key}": {value},')
        script_logger.info("  }")
        script_logger.info("=" * 100)

    finally:
        driver.close()


if __name__ == "__main__":
    main()
