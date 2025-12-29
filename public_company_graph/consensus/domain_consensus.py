"""
Domain consensus logic with weighted voting and early stopping.

This module implements the consensus algorithm for determining the correct
company domain from multiple sources using weighted voting and early stopping.
"""

import logging
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

from public_company_graph.domain.models import CompanyResult, DomainResult
from public_company_graph.sources import (
    get_domain_from_finnhub,
    get_domain_from_finviz,
    get_domain_from_sec,
    get_domain_from_yfinance,
)

logger = logging.getLogger(__name__)

# Source weights (higher = more reliable)
# These weights are used in weighted voting to determine the final domain
SOURCE_WEIGHTS = {
    "yfinance": 3.0,
    "sec_edgar": 2.5,
    "finviz": 2.0,
    "finnhub": 1.0,
}


def collect_domains(
    session,
    cik: str,
    ticker: str,
    company_name: str,
    early_stop_confidence: float = 0.75,
) -> CompanyResult:
    """
    Collect domains from all sources in parallel with early stopping.

    Strategy:
    1. Launch all sources concurrently
    2. As results arrive, check for consensus
    3. Stop early if 2+ high-confidence sources agree (weighted score >= threshold)
    4. Use weighted voting to determine final domain

    Args:
        session: HTTP session for requests
        cik: Company CIK
        ticker: Stock ticker symbol
        company_name: Company name
        early_stop_confidence: Stop early if weighted confidence >= this (default 0.75)

    Returns:
        CompanyResult with domain, sources, confidence, etc.
    """
    # Execute all sources concurrently
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = {
            executor.submit(get_domain_from_yfinance, ticker, company_name): "yfinance",
            executor.submit(get_domain_from_finviz, session, ticker): "finviz",
            executor.submit(get_domain_from_sec, session, cik, ticker, company_name): "sec",
            executor.submit(get_domain_from_finnhub, ticker): "finnhub",
        }

        results: list[DomainResult] = []
        domain_scores: dict[str, float] = defaultdict(float)

        # Collect results as they complete, with early stopping
        # Each source has individual timeout, and we break early when confident
        for future in as_completed(futures):
            try:
                result = future.result(timeout=30)  # 30s per individual source
                if result.domain:
                    results.append(result)

                    # Update scores (weighted by source reliability and result confidence)
                    weight = SOURCE_WEIGHTS.get(result.source, 1.0)
                    domain_scores[result.domain] += weight * result.confidence

                    # Early stopping: if we have high confidence, stop waiting
                    if domain_scores:
                        max_score = max(domain_scores.values())
                        max_possible = sum(SOURCE_WEIGHTS.values())  # All sources agree
                        current_confidence = max_score / max_possible

                        # If we have 2+ sources agreeing on same domain, we're confident
                        if len(results) >= 2:
                            domains_found = [r.domain for r in results]
                            unique_domains = set(domains_found)

                            # All sources agree on same domain - very high confidence
                            if len(unique_domains) == 1:
                                # High confidence - stop waiting for other sources
                                break

                            # Or if weighted confidence exceeds threshold
                            if current_confidence >= early_stop_confidence:
                                break
            except TimeoutError:
                logger.debug(f"Timeout collecting domain for {ticker} from one source")
            except Exception as e:
                logger.debug(f"Error collecting domain for {ticker}: {e}")

    if not results or not domain_scores:
        return CompanyResult(
            cik=cik,
            ticker=ticker,
            name=company_name,
            domain=None,
            sources=[],
            confidence=0.0,
            votes=0,
            all_candidates={},
            description=None,
            description_source=None,
        )

    # Build domain votes for reporting
    domain_votes: dict[str, list[str]] = defaultdict(list)
    for result in results:
        if result.domain:
            domain_votes[result.domain].append(result.source)

    # Collect descriptions from all sources (weighted by source reliability)
    description_scores: dict[str, tuple[float, str]] = {}  # description -> (score, source)
    for result in results:
        if result.description:
            weight = SOURCE_WEIGHTS.get(result.source, 1.0)
            # Use existing score if description already seen, otherwise add new
            if result.description in description_scores:
                description_scores[result.description] = (
                    description_scores[result.description][0] + weight * result.confidence,
                    description_scores[result.description][1],  # Keep first source
                )
            else:
                description_scores[result.description] = (
                    weight * result.confidence,
                    result.source,
                )

    # Get winner (already calculated during early stopping)
    winner_domain = max(domain_scores.items(), key=lambda x: x[1])[0]
    winner_sources = domain_votes[winner_domain]
    total_score = domain_scores[winner_domain]

    # Get best description (highest weighted score)
    best_description = None
    best_description_source = None
    if description_scores:
        best_description, (_, best_description_source) = max(
            description_scores.items(), key=lambda x: x[1][0]
        )

    # Calculate confidence: normalize by sources that actually responded
    # Sum of weights for sources that provided results (not just winner)
    sources_that_responded = set()
    for result in results:
        if result.domain:
            sources_that_responded.add(result.source)

    # Max possible score given the sources that actually responded
    max_possible_given_sources = sum(
        SOURCE_WEIGHTS.get(source, 1.0) for source in sources_that_responded
    )

    # Confidence: how much of the available sources agree on this domain?
    # If all responding sources agree: confidence = 1.0
    # If only some agree: confidence = their_score / max_possible_from_responders
    if max_possible_given_sources > 0:
        confidence = min(total_score / max_possible_given_sources, 1.0)
    else:
        confidence = 0.0

    return CompanyResult(
        cik=cik,
        ticker=ticker,
        name=company_name,
        domain=winner_domain,
        sources=winner_sources,
        confidence=confidence,
        votes=len(winner_sources),
        all_candidates=dict(domain_votes.items()),
        description=best_description,
        description_source=best_description_source,
    )
