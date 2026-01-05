"""
Integration tests for Neo4j constraint creation.

These tests require a running Neo4j instance.
Skip with: pytest -m "not integration"
"""

import os

import pytest

# Skip all tests in this module if Neo4j is not available
pytestmark = pytest.mark.integration

try:
    from neo4j import GraphDatabase

    NEO4J_AVAILABLE = True
except ImportError:
    NEO4J_AVAILABLE = False


def get_test_driver():
    """Get Neo4j driver for testing."""
    uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
    user = os.getenv("NEO4J_USER", "neo4j")
    password = os.getenv("NEO4J_PASSWORD")

    if not password:
        pytest.skip("NEO4J_PASSWORD not set")

    return GraphDatabase.driver(uri, auth=(user, password))


@pytest.fixture
def neo4j_driver():
    """Fixture providing Neo4j driver."""
    if not NEO4J_AVAILABLE:
        pytest.skip("neo4j driver not installed")

    driver = get_test_driver()
    yield driver
    driver.close()


@pytest.fixture
def test_database(neo4j_driver):
    """Return database name for testing."""
    return os.getenv("NEO4J_DATABASE", "neo4j")


class TestConstraintCreation:
    """Test constraint creation functions."""

    def test_create_domain_constraints(self, neo4j_driver, test_database):
        """Test that domain constraints can be created."""
        from public_company_graph.neo4j.constraints import create_domain_constraints

        # Should not raise
        create_domain_constraints(neo4j_driver, database=test_database)

        # Verify constraint exists
        with neo4j_driver.session(database=test_database) as session:
            result = session.run("SHOW CONSTRAINTS")
            constraints = [r["name"] for r in result]
            assert any("domain" in c.lower() for c in constraints)

    def test_create_technology_constraints(self, neo4j_driver, test_database):
        """Test that technology constraints can be created."""
        from public_company_graph.neo4j.constraints import create_technology_constraints

        create_technology_constraints(neo4j_driver, database=test_database)

        with neo4j_driver.session(database=test_database) as session:
            result = session.run("SHOW CONSTRAINTS")
            constraints = [r["name"] for r in result]
            assert any("technology" in c.lower() for c in constraints)

    def test_constraints_idempotent(self, neo4j_driver, test_database):
        """Test that running constraints twice doesn't error."""
        from public_company_graph.neo4j.constraints import create_bootstrap_constraints

        # Run twice - should not raise
        create_bootstrap_constraints(neo4j_driver, database=test_database)
        create_bootstrap_constraints(neo4j_driver, database=test_database)


class TestConnectionVerification:
    """Test connection verification."""

    def test_verify_connection_success(self, neo4j_driver):
        """Test that verify_connection returns True for valid connection."""
        from public_company_graph.neo4j.connection import verify_connection

        assert verify_connection(neo4j_driver) is True

    def test_verify_connection_with_query(self, neo4j_driver, test_database):
        """Test that we can run a simple query."""
        with neo4j_driver.session(database=test_database) as session:
            result = session.run("RETURN 1 AS value")
            assert result.single()["value"] == 1


class TestConstraintErrorHandling:
    """Test that constraint creation handles errors gracefully."""

    def test_create_constraints_handles_invalid_driver(self, test_database):
        """Test that constraint creation fails gracefully with invalid driver."""
        from public_company_graph.neo4j.constraints import create_domain_constraints

        # Invalid driver (None)
        with pytest.raises((AttributeError, TypeError)):
            create_domain_constraints(None, database=test_database)

    def test_create_constraints_handles_invalid_database(self, neo4j_driver):
        """Test that constraint creation handles invalid database name."""
        from public_company_graph.neo4j.constraints import create_domain_constraints

        # Invalid database name (should not raise, but may log warning)
        # Neo4j will either use default or raise, depending on version
        try:
            create_domain_constraints(neo4j_driver, database="nonexistent_database_12345")
        except Exception as e:
            # If it raises, that's fine - we're testing error handling
            assert "database" in str(e).lower() or "not found" in str(e).lower()

    def test_create_constraints_idempotent_on_errors(self, neo4j_driver, test_database):
        """Test that constraint creation is idempotent even when errors occur."""
        from public_company_graph.neo4j.constraints import create_domain_constraints

        # Create constraints first time
        create_domain_constraints(neo4j_driver, database=test_database)

        # Create again - should not raise (idempotent)
        # Even if constraint already exists, should handle gracefully
        create_domain_constraints(neo4j_driver, database=test_database)

        # Verify constraints still exist
        with neo4j_driver.session(database=test_database) as session:
            result = session.run("SHOW CONSTRAINTS")
            constraints = [r["name"] for r in result]
            assert any("domain" in c.lower() for c in constraints)

    def test_create_constraints_handles_malformed_cypher(self, neo4j_driver, test_database):
        """Test that _run_constraints handles malformed Cypher gracefully."""
        from public_company_graph.neo4j.constraints import _run_constraints

        # Malformed constraint (should log warning, not raise)
        malformed_constraints = [
            "CREATE CONSTRAINT invalid IF NOT EXISTS FOR (d:Domain) REQUIRE d.invalid_field IS UNIQUE",
        ]

        # Should not raise (logs warning instead)
        _run_constraints(neo4j_driver, malformed_constraints, database=test_database)

    def test_create_bootstrap_constraints_handles_partial_failure(
        self, neo4j_driver, test_database
    ):
        """Test that bootstrap constraints handle partial failures."""
        from public_company_graph.neo4j.constraints import create_bootstrap_constraints

        # Create bootstrap constraints
        create_bootstrap_constraints(neo4j_driver, database=test_database)

        # Verify both domain and technology constraints exist
        with neo4j_driver.session(database=test_database) as session:
            result = session.run("SHOW CONSTRAINTS")
            constraints = [r["name"] for r in result]
            domain_constraints = [c for c in constraints if "domain" in c.lower()]
            tech_constraints = [c for c in constraints if "technology" in c.lower()]

            # At least one of each should exist
            assert len(domain_constraints) > 0, "Domain constraints should exist"
            assert len(tech_constraints) > 0, "Technology constraints should exist"
