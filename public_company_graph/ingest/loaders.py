"""
Neo4j data loaders for Domain and Technology nodes.

This module provides functions to load data structures into Neo4j.
"""

import logging

from public_company_graph.neo4j.utils import clean_properties_batch

logger = logging.getLogger(__name__)


def load_domains(
    driver,
    domains: list[dict],
    batch_size: int = 1000,
    database: str | None = None,
    log: logging.Logger | None = None,
):
    """
    Load Domain nodes into Neo4j.

    Args:
        driver: Neo4j driver instance
        domains: List of domain dictionaries from read_domains()
        batch_size: Number of domains to process per batch
        database: Neo4j database name
        log: Optional logger instance (uses module logger if not provided)
    """
    _logger = log or logger

    with driver.session(database=database) as session:
        for i in range(0, len(domains), batch_size):
            batch = domains[i : i + batch_size]

            # Clean empty strings and None values from properties
            # Neo4j doesn't store nulls; empty strings are semantically equivalent
            cleaned_batch = clean_properties_batch(batch)

            # Use SET d += row.props to merge only non-empty properties
            # The loaded_at is set separately to ensure it's always updated
            query = """
            UNWIND $batch AS row
            MERGE (d:Domain {final_domain: row.final_domain})
            SET d += row,
                d.loaded_at = datetime()
            """

            session.run(query, batch=cleaned_batch)

            if (i // batch_size + 1) % 10 == 0:
                _logger.info(f"  Processed {i + len(batch)}/{len(domains)} domains...")


def load_technologies(
    driver,
    tech_mappings: list[dict],
    batch_size: int = 1000,
    database: str | None = None,
    log: logging.Logger | None = None,
):
    """
    Load Technology nodes and USES relationships into Neo4j.

    Args:
        driver: Neo4j driver instance
        tech_mappings: List of technology mappings from read_technologies()
        batch_size: Number of relationships to process per batch
        database: Neo4j database name
        log: Optional logger instance (uses module logger if not provided)
    """
    _logger = log or logger

    # Extract unique technologies (filter out empty names/categories)
    unique_techs = {
        (row["technology_name"], row["technology_category"])
        for row in tech_mappings
        if row.get("technology_name") and row["technology_name"].strip()
    }

    with driver.session(database=database) as session:
        # Create Technology nodes
        # Clean properties to omit empty categories
        tech_data = [
            {"name": name, "category": category}
            for name, category in unique_techs
            if name  # name is required
        ]
        cleaned_tech_data = clean_properties_batch(tech_data)

        query = """
        UNWIND $techs AS tech
        MERGE (t:Technology {name: tech.name})
        SET t += tech,
            t.loaded_at = datetime()
        """
        session.run(query, techs=cleaned_tech_data)
        _logger.info(f"  ✓ Created {len(unique_techs)} Technology nodes")

        # Create USES relationships
        for i in range(0, len(tech_mappings), batch_size):
            batch = tech_mappings[i : i + batch_size]

            query = """
            UNWIND $batch AS row
            MATCH (d:Domain {final_domain: row.final_domain})
            MATCH (t:Technology {name: row.technology_name})
            MERGE (d)-[r:USES]->(t)
            SET r.loaded_at = datetime()
            """

            session.run(query, batch=batch)

            if (i // batch_size + 1) % 10 == 0:
                _logger.info(f"  Processed {i + len(batch)}/{len(tech_mappings)} relationships...")
