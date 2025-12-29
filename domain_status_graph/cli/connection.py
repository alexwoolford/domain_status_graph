"""
Neo4j connection utilities for domain_status_graph CLI.

Provides functions for getting Neo4j drivers and verifying connections.
"""

import logging
import sys

from domain_status_graph.config import get_neo4j_database
from domain_status_graph.neo4j import get_neo4j_driver, verify_connection


def get_driver_and_database(logger: logging.Logger | None = None) -> tuple:
    """
    Get Neo4j driver and database name with error handling.

    Args:
        logger: Optional logger instance

    Returns:
        Tuple of (driver, database)

    Raises:
        SystemExit: If driver cannot be created or database not configured
    """
    if logger is None:
        logger = logging.getLogger(__name__)

    try:
        driver = get_neo4j_driver()
        database = get_neo4j_database()
        return driver, database
    except (ImportError, ValueError) as e:
        logger.error(str(e))
        sys.exit(1)


def verify_neo4j_connection(driver, database: str, logger: logging.Logger | None = None) -> bool:
    """
    Verify Neo4j connection is working.

    Args:
        driver: Neo4j driver instance
        database: Database name
        logger: Optional logger instance

    Returns:
        True if connection is valid, False otherwise
    """
    if logger is None:
        logger = logging.getLogger(__name__)

    if verify_connection(driver):
        logger.info("✓ Connected to Neo4j")
        return True
    else:
        logger.error("✗ Could not connect to Neo4j")
        return False
