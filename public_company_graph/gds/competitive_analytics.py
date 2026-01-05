"""
Competitive Graph Analytics using GDS.

Computes:
- PageRank: Most "central" companies in competitive network
- Louvain: Competitive communities/clusters
- Degree Centrality: Most threatened/threatening companies
- Betweenness Centrality: Bridge companies connecting industries
"""

import logging

from public_company_graph.gds.utils import safe_drop_graph

logger = logging.getLogger(__name__)


def compute_competitive_pagerank(
    gds,
    driver,
    database: str | None = None,
    max_iterations: int = 20,
    damping_factor: float = 0.85,
    logger: logging.Logger | None = None,
) -> int:
    """
    Compute PageRank on competitive graph.

    Measures how "central" each company is in the competitive network.
    High PageRank = frequently cited as competitor, or cited by companies that are themselves central.

    Args:
        gds: GDS client instance
        driver: Neo4j driver instance
        database: Neo4j database name
        max_iterations: Max PageRank iterations
        damping_factor: PageRank damping factor
        logger: Optional logger instance

    Returns:
        Number of companies with PageRank scores
    """
    if logger is None:
        logger = logging.getLogger(__name__)

    logger.info("")
    logger.info("=" * 70)
    logger.info("Competitive PageRank Analysis")
    logger.info("=" * 70)
    logger.info("   Measures: Centrality in competitive network")
    logger.info("   Property: Company.competitive_pagerank")

    try:
        # Create competitive graph projection
        graph_name = f"competitive_graph_{database or 'default'}"
        safe_drop_graph(gds, graph_name)

        logger.info("   Creating competitive graph projection...")
        G_comp, result = gds.graph.project.cypher(
            graph_name,
            """
            MATCH (c:Company)
            RETURN id(c) AS id
            """,
            """
            MATCH (a:Company)-[:HAS_COMPETITOR]->(b:Company)
            RETURN id(a) AS source, id(b) AS target
            """,
        )
        node_count = result["nodeCount"]
        rel_count = result["relationshipCount"]
        logger.info(f"   ✓ Created graph: {node_count} nodes, {rel_count} relationships")

        # Compute PageRank
        logger.info("   Computing PageRank...")
        result = gds.pageRank.write(
            G_comp,
            maxIterations=max_iterations,
            dampingFactor=damping_factor,
            writeProperty="competitive_pagerank",
        )
        # Result is a Series - get the value
        if hasattr(result, "get"):
            nodes_written = result.get("nodesWritten", node_count)
        elif hasattr(result, "iloc"):
            nodes_written = node_count  # Assume all nodes if we can't read it
        else:
            nodes_written = node_count
        logger.info(f"   ✓ Computed PageRank for {nodes_written} companies")

        # Clean up
        G_comp.drop()
        logger.info("   ✓ Complete")

        return nodes_written

    except Exception as e:
        logger.error(f"   ✗ Error: {e}")
        import traceback

        logger.error(traceback.format_exc())
        return 0


def compute_competitive_communities(
    gds,
    driver,
    database: str | None = None,
    max_levels: int = 10,
    max_iterations: int = 10,
    logger: logging.Logger | None = None,
) -> int:
    """
    Compute Louvain communities on competitive graph.

    Finds competitive clusters - groups of companies that compete with each other.

    Args:
        gds: GDS client instance
        driver: Neo4j driver instance
        database: Neo4j database name
        max_levels: Max Louvain levels
        max_iterations: Max iterations per level
        logger: Optional logger instance

    Returns:
        Number of communities found
    """
    if logger is None:
        logger = logging.getLogger(__name__)

    logger.info("")
    logger.info("=" * 70)
    logger.info("Competitive Community Detection (Louvain)")
    logger.info("=" * 70)
    logger.info("   Measures: Competitive clusters")
    logger.info("   Property: Company.competitive_community")

    try:
        # Create competitive graph projection
        graph_name = f"competitive_graph_{database or 'default'}"
        safe_drop_graph(gds, graph_name)

        logger.info("   Creating competitive graph projection...")
        G_comp, result = gds.graph.project.cypher(
            graph_name,
            """
            MATCH (c:Company)
            RETURN id(c) AS id
            """,
            """
            MATCH (a:Company)-[:HAS_COMPETITOR]->(b:Company)
            RETURN id(a) AS source, id(b) AS target
            """,
        )
        node_count = result["nodeCount"]
        rel_count = result["relationshipCount"]
        logger.info(f"   ✓ Created graph: {node_count} nodes, {rel_count} relationships")

        # Compute Louvain communities
        logger.info("   Computing Louvain communities...")
        result = gds.louvain.write(
            G_comp,
            maxLevels=max_levels,
            maxIterations=max_iterations,
            writeProperty="competitive_community",
        )
        # Result is a Series - get the values
        if hasattr(result, "get"):
            nodes_written = result.get("nodesWritten", node_count)
            communities = result.get("communityCount", 0)
        elif hasattr(result, "iloc"):
            nodes_written = node_count
            communities = 0  # Will query later
        else:
            nodes_written = node_count
            communities = 0
        logger.info(f"   ✓ Found {communities} communities across {nodes_written} companies")

        # Clean up
        G_comp.drop()
        logger.info("   ✓ Complete")

        return communities

    except Exception as e:
        logger.error(f"   ✗ Error: {e}")
        import traceback

        logger.error(traceback.format_exc())
        return 0


def compute_degree_centrality(
    driver,
    database: str | None = None,
    logger: logging.Logger | None = None,
) -> int:
    """
    Compute degree centrality (in-degree and out-degree) for competitive graph.

    In-degree = how many companies cite you as competitor (threatened)
    Out-degree = how many competitors you cite (threatening)

    Args:
        driver: Neo4j driver instance
        database: Neo4j database name
        logger: Optional logger instance

    Returns:
        Number of companies with centrality scores
    """
    if logger is None:
        logger = logging.getLogger(__name__)

    logger.info("")
    logger.info("=" * 70)
    logger.info("Competitive Degree Centrality")
    logger.info("=" * 70)
    logger.info("   Measures: In-degree (threatened) and out-degree (threatening)")
    logger.info("   Properties: Company.competitive_in_degree, Company.competitive_out_degree")

    try:
        with driver.session(database=database) as session:
            # Compute in-degree (cited as competitor)
            logger.info("   Computing in-degree (threatened)...")
            result = session.run(
                """
                MATCH (c:Company)<-[:HAS_COMPETITOR]-(:Company)
                WITH c, count(*) as in_degree
                SET c.competitive_in_degree = in_degree
                RETURN count(c) as updated
                """
            )
            in_degree_count = result.single()["updated"]
            logger.info(f"   ✓ Updated in-degree for {in_degree_count} companies")

            # Compute out-degree (cites competitors)
            logger.info("   Computing out-degree (threatening)...")
            result = session.run(
                """
                MATCH (c:Company)-[:HAS_COMPETITOR]->(:Company)
                WITH c, count(*) as out_degree
                SET c.competitive_out_degree = out_degree
                RETURN count(c) as updated
                """
            )
            out_degree_count = result.single()["updated"]
            logger.info(f"   ✓ Updated out-degree for {out_degree_count} companies")

        logger.info("   ✓ Complete")
        return max(in_degree_count, out_degree_count)

    except Exception as e:
        logger.error(f"   ✗ Error: {e}")
        import traceback

        logger.error(traceback.format_exc())
        return 0


def compute_betweenness_centrality(
    gds,
    driver,
    database: str | None = None,
    logger: logging.Logger | None = None,
) -> int:
    """
    Compute betweenness centrality on competitive graph.

    Measures which companies act as "bridges" connecting different competitive clusters.

    Args:
        gds: GDS client instance
        driver: Neo4j driver instance
        database: Neo4j database name
        logger: Optional logger instance

    Returns:
        Number of companies with betweenness scores
    """
    if logger is None:
        logger = logging.getLogger(__name__)

    logger.info("")
    logger.info("=" * 70)
    logger.info("Competitive Betweenness Centrality")
    logger.info("=" * 70)
    logger.info("   Measures: Bridge companies connecting competitive clusters")
    logger.info("   Property: Company.competitive_betweenness")

    try:
        # Create competitive graph projection
        graph_name = f"competitive_graph_{database or 'default'}"
        safe_drop_graph(gds, graph_name)

        logger.info("   Creating competitive graph projection...")
        G_comp, result = gds.graph.project.cypher(
            graph_name,
            """
            MATCH (c:Company)
            RETURN id(c) AS id
            """,
            """
            MATCH (a:Company)-[:HAS_COMPETITOR]->(b:Company)
            RETURN id(a) AS source, id(b) AS target
            """,
        )
        node_count = result["nodeCount"]
        rel_count = result["relationshipCount"]
        logger.info(f"   ✓ Created graph: {node_count} nodes, {rel_count} relationships")

        # Compute betweenness centrality
        logger.info("   Computing betweenness centrality...")
        result = gds.betweenness.write(
            G_comp,
            writeProperty="competitive_betweenness",
        )
        # Result is a Series - get the value
        if hasattr(result, "get"):
            nodes_written = result.get("nodesWritten", node_count)
        elif hasattr(result, "iloc"):
            nodes_written = node_count
        else:
            nodes_written = node_count
        logger.info(f"   ✓ Computed betweenness for {nodes_written} companies")

        # Clean up
        G_comp.drop()
        logger.info("   ✓ Complete")

        return nodes_written

    except Exception as e:
        logger.error(f"   ✗ Error: {e}")
        import traceback

        logger.error(traceback.format_exc())
        return 0


def compute_all_competitive_analytics(
    gds,
    driver,
    database: str | None = None,
    logger: logging.Logger | None = None,
) -> dict:
    """
    Compute all competitive graph analytics.

    Args:
        gds: GDS client instance
        driver: Neo4j driver instance
        database: Neo4j database name
        logger: Optional logger instance

    Returns:
        Dictionary with counts of computed metrics
    """
    if logger is None:
        logger = logging.getLogger(__name__)

    logger.info("")
    logger.info("=" * 70)
    logger.info("Computing All Competitive Graph Analytics")
    logger.info("=" * 70)

    results = {}

    # PageRank
    results["pagerank"] = compute_competitive_pagerank(
        gds, driver, database=database, logger=logger
    )

    # Louvain communities
    results["communities"] = compute_competitive_communities(
        gds, driver, database=database, logger=logger
    )

    # Degree centrality
    results["degree"] = compute_degree_centrality(driver, database=database, logger=logger)

    # Betweenness centrality
    results["betweenness"] = compute_betweenness_centrality(
        gds, driver, database=database, logger=logger
    )

    logger.info("")
    logger.info("=" * 70)
    logger.info("Competitive Analytics Complete!")
    logger.info("=" * 70)
    logger.info(f"PageRank: {results['pagerank']} companies")
    logger.info(f"Communities: {results['communities']} clusters")
    logger.info(f"Degree Centrality: {results['degree']} companies")
    logger.info(f"Betweenness Centrality: {results['betweenness']} companies")

    return results
