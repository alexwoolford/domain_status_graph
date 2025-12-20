"""
Cypher queries for finding similar companies using composite similarity scores.

These queries aggregate all similarity relationship types to find the most
similar companies to a given company.
"""

from typing import Dict, Optional

# Default weights for different similarity types
# Higher weight = more important signal
DEFAULT_SIMILARITY_WEIGHTS = {
    "SIMILAR_INDUSTRY": 1.0,  # Same industry is very important
    "SIMILAR_SIZE": 0.8,  # Similar size is important
    "SIMILAR_DESCRIPTION": 0.9,  # Similar descriptions are very important
    "SIMILAR_TECHNOLOGY": 0.7,  # Similar tech stack is important
    "SIMILAR_KEYWORDS": 0.6,  # Shared keywords are moderately important
    "SIMILAR_MARKET": 0.5,  # Same market is moderately important
    "COMMON_EXECUTIVE": 0.4,  # Shared executives are less important
    "MERGED_OR_ACQUIRED": 0.3,  # M&A relationships are least important
}


def get_top_similar_companies_query(
    ticker: str,
    limit: int = 20,
    weights: Optional[Dict[str, float]] = None,
    min_score: float = 0.0,
) -> str:
    """
    Generate a Cypher query to find top similar companies using composite scoring.

    Args:
        ticker: Company ticker symbol
        limit: Maximum number of results
        weights: Optional custom weights for relationship types
        min_score: Minimum composite score to include

    Returns:
        Cypher query string
    """
    if weights is None:
        weights = DEFAULT_SIMILARITY_WEIGHTS

    # Build the CASE statement for weighted scoring
    case_parts = []
    for rel_type, weight in weights.items():
        case_parts.append(f"WHEN '{rel_type}' THEN {weight}")

    case_statement = "\n".join(
        [
            "     sum(CASE type(r)",
            *[f"         {part}" for part in case_parts],
            "         ELSE 0.0",
            "     END) as weighted_score",
        ]
    )

    rel_types_list = ", ".join([f"'{rt}'" for rt in weights.keys()])

    query = f"""
    MATCH (c1:Company {{ticker: '{ticker}'}})-[r]-(c2:Company)
    WHERE type(r) IN [{rel_types_list}]
    WITH c2,
         count(r) as edge_count,
{case_statement}
    WHERE weighted_score >= {min_score}
    RETURN c2.ticker, c2.name, c2.sector, c2.industry, edge_count, weighted_score
    ORDER BY weighted_score DESC, edge_count DESC
    LIMIT {limit}
    """

    return query


def get_similarity_breakdown_query(ticker1: str, ticker2: str) -> str:
    """
    Generate a Cypher query to see all similarity relationships between two companies.

    Args:
        ticker1: First company ticker
        ticker2: Second company ticker

    Returns:
        Cypher query string
    """
    query = f"""
    MATCH (c1:Company {{ticker: '{ticker1}'}})-[r]-(c2:Company {{ticker: '{ticker2}'}})
    WHERE type(r) STARTS WITH 'SIMILAR' OR type(r) IN ['COMMON_EXECUTIVE', 'MERGED_OR_ACQUIRED']
    RETURN type(r) as rel_type, properties(r) as props
    ORDER BY rel_type
    """
    return query
