"""
Unit tests for public_company_graph.config module.

Note: Since config uses pydantic_settings with lru_cache, these tests
verify functions return reasonable values rather than testing defaults
(which depend on .env file presence).
"""

from pathlib import Path

from public_company_graph.config import (
    Settings,
    get_data_dir,
    get_domain_status_db,
    get_neo4j_database,
    get_neo4j_uri,
    get_neo4j_user,
)


def test_get_neo4j_uri_returns_string():
    """Test that get_neo4j_uri returns a valid URI string."""
    uri = get_neo4j_uri()
    assert isinstance(uri, str)
    assert len(uri) > 0
    # Should be a valid neo4j URI scheme
    assert uri.startswith(("bolt://", "neo4j://", "neo4j+s://", "bolt+s://"))


def test_get_neo4j_user_returns_string():
    """Test that get_neo4j_user returns a non-empty string."""
    user = get_neo4j_user()
    assert isinstance(user, str)
    assert len(user) > 0


def test_get_neo4j_database_returns_string():
    """Test that get_neo4j_database returns a non-empty string."""
    db = get_neo4j_database()
    assert isinstance(db, str)
    assert len(db) > 0


def test_settings_has_neo4j_password_field():
    """Test that Settings model has neo4j_password field with validation."""
    # Verify the Settings model structure (doesn't test runtime values)
    assert "neo4j_password" in Settings.model_fields
    field = Settings.model_fields["neo4j_password"]
    assert field.default == ""  # Default is empty string


def test_get_data_dir():
    """Test that get_data_dir returns correct path."""
    data_dir = get_data_dir()
    assert isinstance(data_dir, Path)
    assert data_dir.name == "data"
    # Verify it's in the project root (not parent of project root)
    # The data dir should be a subdirectory of the project root
    project_root = data_dir.parent
    assert (project_root / "public_company_graph").exists(), "data dir should be in project root"


def test_get_domain_status_db():
    """Test that get_domain_status_db returns correct path."""
    db_path = get_domain_status_db()
    assert isinstance(db_path, Path)
    assert db_path.name == "domain_status.db"
