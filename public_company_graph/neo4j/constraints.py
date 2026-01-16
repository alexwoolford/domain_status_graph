"""
Neo4j constraint and index creation.

This module provides functions to create constraints and indexes
for Domain, Technology, Company, and Document nodes.
"""

import logging

logger = logging.getLogger(__name__)


def _run_constraints(
    driver,
    constraints: list[str],
    database: str | None = None,
    log: logging.Logger | None = None,
) -> None:
    """
    Run a list of constraint/index creation statements.

    Args:
        driver: Neo4j driver instance
        constraints: List of Cypher constraint statements
        database: Neo4j database name
        log: Logger instance (defaults to module logger)
    """
    if log is None:
        log = logger

    with driver.session(database=database) as session:
        for constraint in constraints:
            try:
                session.run(constraint)
                log.info(f"✓ Created: {constraint[:50]}...")
            except Exception as e:
                error_str = str(e).lower()
                # Constraint already exists - this is fine
                if "already exists" in error_str or "equivalent" in error_str:
                    log.debug(f"Constraint already exists: {constraint[:50]}")
                else:
                    log.warning(f"⚠ Warning creating constraint: {e}")


def create_domain_constraints(
    driver, database: str | None = None, logger: logging.Logger | None = None
) -> None:
    """
    Create constraints and indexes for Domain nodes.

    Args:
        driver: Neo4j driver instance
        database: Neo4j database name
        logger: Optional logger instance
    """
    constraints = [
        (
            "CREATE CONSTRAINT domain_name IF NOT EXISTS "
            "FOR (d:Domain) REQUIRE d.final_domain IS UNIQUE"
        ),
        "CREATE INDEX domain_domain IF NOT EXISTS FOR (d:Domain) ON (d.domain)",
    ]
    _run_constraints(driver, constraints, database=database, log=logger)


def create_technology_constraints(
    driver, database: str | None = None, logger: logging.Logger | None = None
) -> None:
    """
    Create constraints for Technology nodes.

    Args:
        driver: Neo4j driver instance
        database: Neo4j database name
        logger: Optional logger instance
    """
    constraints = [
        (
            "CREATE CONSTRAINT technology_name IF NOT EXISTS "
            "FOR (t:Technology) REQUIRE t.name IS UNIQUE"
        ),
    ]
    _run_constraints(driver, constraints, database=database, log=logger)


def create_company_constraints(
    driver, database: str | None = None, logger: logging.Logger | None = None
) -> None:
    """
    Create constraints and indexes for Company nodes.

    Args:
        driver: Neo4j driver instance
        database: Neo4j database name
        logger: Optional logger instance
    """
    constraints = [
        "CREATE CONSTRAINT company_cik IF NOT EXISTS FOR (c:Company) REQUIRE c.cik IS UNIQUE",
        "CREATE INDEX company_ticker IF NOT EXISTS FOR (c:Company) ON (c.ticker)",
        # Indexes for enrichment properties (Phase 1)
        "CREATE INDEX company_sector IF NOT EXISTS FOR (c:Company) ON (c.sector)",
        "CREATE INDEX company_industry IF NOT EXISTS FOR (c:Company) ON (c.industry)",
        "CREATE INDEX company_sic_code IF NOT EXISTS FOR (c:Company) ON (c.sic_code)",
        "CREATE INDEX company_naics_code IF NOT EXISTS FOR (c:Company) ON (c.naics_code)",
        # Indexes for filing metadata
        "CREATE INDEX company_filing_date IF NOT EXISTS FOR (c:Company) ON (c.filing_date)",
        "CREATE INDEX company_filing_year IF NOT EXISTS FOR (c:Company) ON (c.filing_year)",
        "CREATE INDEX company_accession_number IF NOT EXISTS FOR (c:Company) ON (c.accession_number)",
    ]
    _run_constraints(driver, constraints, database=database, log=logger)


def create_document_constraints(
    driver, database: str | None = None, logger: logging.Logger | None = None
) -> None:
    """
    Create constraints and indexes for Document and Chunk nodes (GraphRAG layer).

    Document nodes represent filings (10-K HTML files).
    Chunk nodes represent individual text pieces from those filings.

    The unique constraints are critical for performance when using MERGE operations.

    Args:
        driver: Neo4j driver instance
        database: Neo4j database name
        logger: Optional logger instance
    """
    constraints = [
        # Document node constraints (filing-level)
        (
            "CREATE CONSTRAINT unique_doc_id IF NOT EXISTS "
            "FOR (d:Document) REQUIRE d.doc_id IS UNIQUE"
        ),
        "CREATE INDEX document_company_cik IF NOT EXISTS FOR (d:Document) ON (d.company_cik)",
        "CREATE INDEX document_section_type IF NOT EXISTS FOR (d:Document) ON (d.section_type)",
        # Chunk node constraints (text piece-level)
        (
            "CREATE CONSTRAINT unique_chunk_id IF NOT EXISTS "
            "FOR (c:Chunk) REQUIRE c.chunk_id IS UNIQUE"
        ),
        "CREATE INDEX chunk_chunk_index IF NOT EXISTS FOR (c:Chunk) ON (c.chunk_index)",
        # Vector index for fast similarity search (Neo4j 5+)
        (
            "CREATE VECTOR INDEX chunk_embedding_vector IF NOT EXISTS "
            "FOR (c:Chunk) ON c.embedding "
            "OPTIONS {indexConfig: {`vector.dimensions`: 1536, `vector.similarity_function`: 'cosine'}}"
        ),
    ]
    _run_constraints(driver, constraints, database=database, log=logger)


def create_bootstrap_constraints(
    driver, database: str | None = None, logger: logging.Logger | None = None
) -> None:
    """
    Create all constraints needed for bootstrap (Domain + Technology).

    Args:
        driver: Neo4j driver instance
        database: Neo4j database name
        logger: Optional logger instance
    """
    create_domain_constraints(driver, database=database, logger=logger)
    create_technology_constraints(driver, database=database, logger=logger)
