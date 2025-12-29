"""
CLI utilities for domain_status_graph.

This package provides shared functionality for scripts:
- Logging setup
- Argument parsing
- Neo4j connection management
- Command entry points
"""

# Import all public API for backward compatibility
from domain_status_graph.cli.args import add_execute_argument
from domain_status_graph.cli.commands import (
    run_all_pipelines,
    run_bootstrap,
    run_cache,
    run_compute_company_similarity,
    run_compute_company_similarity_via_domains,
    run_enrich_company_properties,
    run_gds_features,
    run_health_check,
    run_optimize_similarity_weights,
    run_validate_famous_pairs,
    run_validate_ranking_quality,
)
from domain_status_graph.cli.connection import (
    get_driver_and_database,
    verify_neo4j_connection,
)
from domain_status_graph.cli.logging import (
    print_dry_run_header,
    print_execute_header,
    setup_logging,
)

__all__ = [
    # Logging
    "setup_logging",
    "print_dry_run_header",
    "print_execute_header",
    # Arguments
    "add_execute_argument",
    # Connection
    "get_driver_and_database",
    "verify_neo4j_connection",
    # Commands
    "run_bootstrap",
    "run_gds_features",
    "run_health_check",
    "run_all_pipelines",
    "run_cache",
    "run_enrich_company_properties",
    "run_compute_company_similarity",
    "run_validate_ranking_quality",
    "run_validate_famous_pairs",
    "run_compute_company_similarity_via_domains",
    "run_optimize_similarity_weights",
]
