"""
Cypher queries for finding similar companies using composite similarity scores.

These queries aggregate all similarity relationship types to find the most
similar companies to a given company.

Similarity signals used:
1. SIMILAR_RISK - Cosine similarity of 10-K risk factor embeddings (strongest signal)
2. SIMILAR_DESCRIPTION - Cosine similarity of business description embeddings
3. SIMILAR_INDUSTRY - SIC code / industry / sector matches
4. SIMILAR_SIZE - Company size similarity
5. Shared Technologies - Via Company -> Domain -> Technology path (indirect signal)
"""

# Default weights for different similarity types (Company-Company relationships)
# Higher weight = more important signal
#
# Relationship counts in graph:
#   SIMILAR_INDUSTRY: 260,336 (method: SIC=1.2, INDUSTRY=0.8, SECTOR=0.6)
#   SIMILAR_DESCRIPTION: 210,267 (uses r.score from cosine similarity)
#   SIMILAR_SIZE: 207,048 (uses r.score)
#   SIMILAR_RISK: 197,186 (uses r.score from 10-K risk factor embeddings)
#   SIMILAR_TECHNOLOGY: 124,584 (uses r.score from tech stack similarity)
#   SIMILAR_KEYWORD: 71 (very sparse)
#   HAS_COMPETITOR: ~1K (direct competitor mentions from 10-K filings - very strong signal!)
#
# Optimized 2025-12-29: Grid search over 46 famous competitor pairs
# Results: 10/46 passed (22%), 16 failed, 16 not found (coverage gap)
#
DEFAULT_SIMILARITY_WEIGHTS = {
    "SIMILAR_DESCRIPTION": 0.8,  # Business description embedding - most important!
    "SIMILAR_RISK": 0.8,  # Risk factor embedding similarity
    "SIMILAR_INDUSTRY": 0.6,  # SIC/Industry classification match
    "SIMILAR_TECHNOLOGY": 0.3,  # Technology stack similarity
    "SIMILAR_SIZE": 0.2,  # Size similarity - common, less discriminative
    "SIMILAR_KEYWORD": 0.1,  # Shared keywords - very sparse (71 edges)
    "HAS_COMPETITOR": 4.0,  # Direct competitor from 10-K - very strong signal!
}

# Weight for shared technologies (via Domain path)
# Each shared technology adds this much to the score
SHARED_TECHNOLOGY_WEIGHT = 0.05


def get_top_similar_companies_query(
    ticker: str,
    limit: int = 20,
    weights: dict[str, float] | None = None,
    min_score: float = 0.0,
    include_shared_tech: bool = True,
    shared_tech_weight: float = SHARED_TECHNOLOGY_WEIGHT,
) -> str:
    """
    Generate a Cypher query to find top similar companies using composite scoring.

    The query combines multiple similarity signals:
    1. Direct Company-Company relationships (SIMILAR_RISK, SIMILAR_DESCRIPTION, etc.)
    2. Shared technologies via Domain path (Company -> Domain -> Technology)

    Relationship weights:
    - SIMILAR_RISK uses cosine similarity score (0.6-1.0) from 10-K risk factors
    - SIMILAR_DESCRIPTION uses cosine similarity score from business descriptions
    - SIMILAR_INDUSTRY is weighted by method specificity (SIC > INDUSTRY > SECTOR)

    Args:
        ticker: Company ticker symbol
        limit: Maximum number of results
        weights: Optional custom weights for relationship types
        min_score: Minimum composite score to include
        include_shared_tech: Whether to include shared technologies via Domain path
        shared_tech_weight: Weight per shared technology (default 0.05)

    Returns:
        Cypher query string
    """
    if weights is None:
        weights = DEFAULT_SIMILARITY_WEIGHTS

    # Build the shared technology clause if enabled
    if include_shared_tech:
        shared_tech_clause = f"""
    // Count shared technologies via Domain path
    OPTIONAL MATCH (c1)-[:HAS_DOMAIN]->(:Domain)-[:USES]->(t:Technology)<-[:USES]-(:Domain)<-[:HAS_DOMAIN]-(c2)
    WITH c2, risk_score, desc_score, risk_matches, desc_matches,
         count(DISTINCT t) AS shared_tech_count
    WITH c2,
         risk_score + desc_score AS direct_score,
         risk_score, desc_score,
         risk_matches, desc_matches,
         shared_tech_count,
         shared_tech_count * {shared_tech_weight} AS tech_bonus
    WITH c2,
         (risk_score + desc_score + shared_tech_count * {shared_tech_weight}) AS weighted_score,
         risk_score, desc_score, shared_tech_count,
         risk_matches, desc_matches
    WHERE weighted_score >= {min_score}
    """
        return_cols = (
            "c2.ticker AS ticker, c2.name AS name, "
            "weighted_score, risk_score, desc_score, shared_tech_count, "
            "risk_matches, desc_matches"
        )
        order_by = (
            "weighted_score DESC, risk_matches DESC, shared_tech_count DESC, desc_matches DESC"
        )
    else:
        shared_tech_clause = f"""
    WITH c2,
         (risk_score + desc_score) AS weighted_score,
         risk_score, desc_score,
         risk_matches, desc_matches
    WHERE weighted_score >= {min_score}
    """
        return_cols = (
            "c2.ticker AS ticker, c2.name AS name, "
            "weighted_score, risk_score, desc_score, "
            "risk_matches, desc_matches"
        )
        order_by = "weighted_score DESC, risk_matches DESC, desc_matches DESC"

    query = f"""
    // Find direct Company-Company similarity relationships
    MATCH (c1:Company {{ticker: '{ticker}'}})-[r]-(c2:Company)
    WHERE type(r) IN ['SIMILAR_RISK', 'SIMILAR_DESCRIPTION']
    WITH c1, c2,
         sum(CASE WHEN type(r) = 'SIMILAR_RISK'
                  THEN {weights.get("SIMILAR_RISK", 1.0)} * r.score
                  ELSE 0.0 END) AS risk_score,
         sum(CASE WHEN type(r) = 'SIMILAR_DESCRIPTION'
                  THEN {weights.get("SIMILAR_DESCRIPTION", 0.5)} * r.score
                  ELSE 0.0 END) AS desc_score,
         sum(CASE WHEN type(r) = 'SIMILAR_RISK' THEN 1 ELSE 0 END) AS risk_matches,
         sum(CASE WHEN type(r) = 'SIMILAR_DESCRIPTION' THEN 1 ELSE 0 END) AS desc_matches
    {shared_tech_clause}
    RETURN {return_cols}
    ORDER BY {order_by}
    LIMIT {limit}
    """

    return query


def get_top_similar_companies_query_extended(
    ticker: str,
    limit: int = 20,
    weights: dict[str, float] | None = None,
    min_score: float = 0.0,
) -> str:
    """
    Generate a Cypher query with all Company-Company similarity relationship types.

    Uses these real relationships from the graph:
    - SIMILAR_RISK (197K) - 10-K risk factor embedding similarity
    - SIMILAR_DESCRIPTION (210K) - Business description embedding similarity
    - SIMILAR_TECHNOLOGY (124K) - Technology stack similarity
    - SIMILAR_INDUSTRY (260K) - SIC/Industry/Sector classification match
    - SIMILAR_SIZE (207K) - Revenue/market cap similarity
    - SIMILAR_KEYWORD (71) - Keyword embedding similarity (sparse)

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

    rel_types_list = ", ".join([f"'{rt}'" for rt in weights.keys()])

    query = f"""
    MATCH (c1:Company {{ticker: '{ticker}'}})-[r]-(c2:Company)
    WHERE type(r) IN [{rel_types_list}]
    WITH c1, c2, collect(r) as rels
    UNWIND rels as r
    WITH c1, c2, rels, r,
         // Weight each relationship type, using r.score for embedding-based similarities
         CASE
           // HAS_COMPETITOR: direct competitor mention from 10-K - strongest signal!
           WHEN type(r) = 'HAS_COMPETITOR' THEN
             {weights.get("HAS_COMPETITOR", 4.0)} * coalesce(r.confidence, 1.0)
           // SIMILAR_RISK: cosine similarity from 10-K risk factor embeddings
           WHEN type(r) = 'SIMILAR_RISK' THEN
             {weights.get("SIMILAR_RISK", 1.0)} * coalesce(r.score, 1.0)
           // SIMILAR_INDUSTRY: weight by method specificity (SIC > INDUSTRY > SECTOR)
           WHEN type(r) = 'SIMILAR_INDUSTRY' AND r.method = 'SIC' THEN
             {weights.get("SIMILAR_INDUSTRY", 0.8)} * 1.2
           WHEN type(r) = 'SIMILAR_INDUSTRY' AND r.method = 'INDUSTRY' THEN
             {weights.get("SIMILAR_INDUSTRY", 0.8)} * 0.8
           WHEN type(r) = 'SIMILAR_INDUSTRY' AND r.method = 'SECTOR' THEN
             {weights.get("SIMILAR_INDUSTRY", 0.8)} * 0.6
           WHEN type(r) = 'SIMILAR_INDUSTRY' THEN
             {weights.get("SIMILAR_INDUSTRY", 0.8)} * 0.7
           // SIMILAR_DESCRIPTION: cosine similarity from business description embeddings
           WHEN type(r) = 'SIMILAR_DESCRIPTION' THEN
             {weights.get("SIMILAR_DESCRIPTION", 0.6)} * coalesce(r.score, 1.0)
           // SIMILAR_TECHNOLOGY: Jaccard similarity from tech stack
           WHEN type(r) = 'SIMILAR_TECHNOLOGY' THEN
             {weights.get("SIMILAR_TECHNOLOGY", 0.5)} * coalesce(r.score, 1.0)
           // SIMILAR_SIZE: size bucket match
           WHEN type(r) = 'SIMILAR_SIZE' THEN
             {weights.get("SIMILAR_SIZE", 0.4)} * coalesce(r.score, 1.0)
           // SIMILAR_KEYWORD: keyword embedding similarity (very sparse)
           WHEN type(r) = 'SIMILAR_KEYWORD' THEN
             {weights.get("SIMILAR_KEYWORD", 0.1)} * coalesce(r.score, 1.0)
           ELSE 0.0
         END as rel_score
    WITH c1, c2, sum(rel_score) as base_score,
         size([r IN rels WHERE type(r) = 'SIMILAR_RISK']) as risk_matches,
         size([r IN rels WHERE type(r) = 'SIMILAR_INDUSTRY' AND r.method = 'SIC'])
           as sic_matches,
         size([r IN rels WHERE type(r) = 'SIMILAR_SIZE']) as size_matches,
         size([r IN rels WHERE type(r) = 'SIMILAR_DESCRIPTION']) as desc_matches,
         size([r IN rels WHERE type(r) = 'SIMILAR_TECHNOLOGY']) as tech_matches,
         size([r IN rels WHERE type(r) = 'HAS_COMPETITOR']) as competitor_matches,
         size(rels) as edge_count
    // Bonus: SIC + SIZE combination is strong signal
    // Bonus: RISK + SIC is even stronger (same industry AND same risks)
    // Bonus: HAS_COMPETITOR is a strong direct signal (no additional bonus needed)
    WITH c1, c2, base_score, risk_matches, sic_matches, size_matches,
         desc_matches, tech_matches, competitor_matches, edge_count,
         CASE WHEN sic_matches > 0 AND size_matches > 0 THEN 0.2 ELSE 0.0 END
           as sic_size_bonus,
         CASE WHEN risk_matches > 0 AND sic_matches > 0 THEN 0.2 ELSE 0.0 END
           as risk_sic_bonus
    WITH c1, c2, (base_score + sic_size_bonus + risk_sic_bonus) as weighted_score,
         risk_matches, sic_matches, size_matches, desc_matches, tech_matches,
         competitor_matches, edge_count
    WHERE weighted_score >= {min_score}
    // Tie-breaker: exact industry name match (most specific)
    WITH c1, c2, weighted_score, risk_matches, sic_matches, desc_matches,
         tech_matches, competitor_matches, edge_count,
         CASE WHEN c1.industry IS NOT NULL AND c1.industry = c2.industry THEN 1
              ELSE 0 END as exact_industry_match
    RETURN c2.ticker AS ticker, c2.name AS name, c2.sector AS sector,
           c2.industry AS industry, edge_count, weighted_score,
           risk_matches, sic_matches, desc_matches, tech_matches, competitor_matches
    ORDER BY weighted_score DESC, competitor_matches DESC, exact_industry_match DESC,
             risk_matches DESC, sic_matches DESC, desc_matches DESC, tech_matches DESC,
             edge_count DESC
    LIMIT {limit}
    """

    return query


def find_similar_companies(
    driver,
    ticker: str,
    limit: int = 20,
    database: str | None = None,
    include_shared_tech: bool = True,
) -> list[dict]:
    """
    Find the most similar companies to a given ticker.

    This is a convenience function that executes the weighted similarity query
    and returns results directly.

    Args:
        driver: Neo4j driver instance
        ticker: Company ticker symbol to find similar companies for
        limit: Maximum number of results (default: 20)
        database: Neo4j database name (optional)
        include_shared_tech: Include shared technologies via Domain path (default: True)

    Returns:
        List of dictionaries with similar companies and their scores:
        [
            {
                "ticker": "PEP",
                "name": "PEPSICO INC",
                "weighted_score": 1.316,
                "risk_score": 0.872,
                "desc_score": 0.344,
                "shared_tech_count": 2,
                "risk_matches": 1,
                "desc_matches": 1
            },
            ...
        ]

    Example:
        >>> from neo4j import GraphDatabase
        >>> driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "password"))
        >>> similar = find_similar_companies(driver, "KO", limit=10)
        >>> for company in similar:
        ...     print(f"{company['ticker']}: {company['weighted_score']:.3f}")
        COKE: 1.405
        MNST: 1.336
        PEP: 1.316
        ...
    """
    query = get_top_similar_companies_query(
        ticker=ticker,
        limit=limit,
        include_shared_tech=include_shared_tech,
    )

    with driver.session(database=database) as session:
        result = session.run(query)
        return [dict(record) for record in result]


def get_similarity_breakdown(
    driver,
    ticker1: str,
    ticker2: str,
    database: str | None = None,
) -> dict:
    """
    Get a detailed breakdown of similarity signals between two companies.

    Args:
        driver: Neo4j driver instance
        ticker1: First company ticker
        ticker2: Second company ticker
        database: Neo4j database name (optional)

    Returns:
        Dictionary with:
        {
            "company1": "KO",
            "company2": "PEP",
            "direct_relationships": [
                {"rel_type": "SIMILAR_RISK", "score": 0.872, "method": null},
                {"rel_type": "SIMILAR_DESCRIPTION", "score": 0.688, "method": null}
            ],
            "shared_technologies": ["Google Tag Manager", "HSTS"],
            "shared_tech_count": 2
        }

    Example:
        >>> breakdown = get_similarity_breakdown(driver, "KO", "PEP")
        >>> print(f"Risk similarity: {breakdown['direct_relationships'][0]['score']}")
        Risk similarity: 0.872
        >>> print(f"Shared tech: {breakdown['shared_technologies']}")
        Shared tech: ['Google Tag Manager', 'HSTS']
    """
    query = get_similarity_breakdown_query(ticker1, ticker2)

    with driver.session(database=database) as session:
        result = session.run(query)
        record = result.single()
        return dict(record) if record else {}


def get_similarity_breakdown_query(ticker1: str, ticker2: str) -> str:
    """
    Generate a Cypher query to see all similarity signals between two companies.

    Shows:
    - Direct Company-Company relationships (SIMILAR_RISK, SIMILAR_DESCRIPTION, etc.)
    - Shared technologies via Domain path

    Args:
        ticker1: First company ticker
        ticker2: Second company ticker

    Returns:
        Cypher query string
    """
    query = f"""
    // Get direct Company-Company relationships
    MATCH (c1:Company {{ticker: '{ticker1}'}}), (c2:Company {{ticker: '{ticker2}'}})
    OPTIONAL MATCH (c1)-[r]-(c2)
    WHERE type(r) STARTS WITH 'SIMILAR'
       OR type(r) IN ['COMMON_EXECUTIVE', 'MERGED_OR_ACQUIRED']
    WITH c1, c2, collect({{rel_type: type(r), score: r.score, method: r.method}}) AS rels
    // Get shared technologies via Domain path
    OPTIONAL MATCH (c1)-[:HAS_DOMAIN]->(:Domain)-[:USES]->(t:Technology)<-[:USES]-(:Domain)<-[:HAS_DOMAIN]-(c2)
    WITH c1, c2, rels, collect(DISTINCT t.name) AS shared_technologies
    RETURN c1.ticker AS company1, c2.ticker AS company2,
           rels AS direct_relationships,
           shared_technologies,
           size(shared_technologies) AS shared_tech_count
    """
    return query
