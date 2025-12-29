"""
Configuration management for public_company_graph.

Uses pydantic-settings for type-safe configuration with automatic
environment variable loading and validation.
"""

from functools import lru_cache
from pathlib import Path

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """
    Application settings loaded from environment variables.

    All settings are validated at startup. Required settings will raise
    an error if not provided, ensuring fail-fast behavior.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",  # Ignore extra env vars
    )

    # Neo4j Configuration
    neo4j_uri: str = Field(
        default="bolt://localhost:7687",
        description="Neo4j connection URI",
    )
    neo4j_user: str = Field(
        default="neo4j",
        description="Neo4j username",
    )
    neo4j_password: str = Field(
        default="",
        description="Neo4j password (required)",
    )
    neo4j_database: str = Field(
        default="neo4j",
        description="Neo4j database name",
    )

    # OpenAI Configuration
    openai_api_key: str = Field(
        default="",
        description="OpenAI API key for embeddings",
    )

    # Finnhub Configuration
    finnhub_api_key: str | None = Field(
        default=None,
        description="Finnhub API key (optional)",
    )

    # Datamule Configuration
    datamule_api_key: str | None = Field(
        default=None,
        description="Datamule API key (optional)",
    )

    @field_validator("neo4j_password", "openai_api_key", mode="before")
    @classmethod
    def strip_whitespace(cls, v: str) -> str:
        """Strip whitespace from string values."""
        if isinstance(v, str):
            return v.strip()
        return v

    @field_validator("finnhub_api_key", "datamule_api_key", mode="before")
    @classmethod
    def empty_string_to_none(cls, v: str | None) -> str | None:
        """Convert empty strings to None for optional fields."""
        if isinstance(v, str):
            v = v.strip()
            return v if v else None
        return v


@lru_cache
def get_settings() -> Settings:
    """
    Get cached settings instance.

    Uses lru_cache to ensure settings are only loaded once.
    """
    return Settings()


# Convenience functions for backwards compatibility
# These will raise clear errors if required values are missing


def get_neo4j_uri() -> str:
    """Get Neo4j URI from settings."""
    return get_settings().neo4j_uri


def get_neo4j_user() -> str:
    """Get Neo4j username from settings."""
    return get_settings().neo4j_user


def get_neo4j_password() -> str:
    """Get Neo4j password from settings."""
    password = get_settings().neo4j_password
    if not password:
        raise ValueError("NEO4J_PASSWORD not set in .env file")
    return password


def get_neo4j_database() -> str:
    """Get Neo4j database name from settings."""
    return get_settings().neo4j_database


def get_openai_api_key() -> str:
    """Get OpenAI API key from settings."""
    key = get_settings().openai_api_key
    if not key:
        raise ValueError("OPENAI_API_KEY not set in .env file")
    return key


def get_finnhub_api_key() -> str | None:
    """Get Finnhub API key from settings (optional)."""
    return get_settings().finnhub_api_key


def get_datamule_api_key() -> str | None:
    """Get Datamule API key from settings (optional)."""
    return get_settings().datamule_api_key


# Data paths - not loaded from env, computed from package location


def get_data_dir() -> Path:
    """Get data directory path (project root / data)."""
    project_root = Path(__file__).parent.parent
    return project_root / "data"


def get_domain_status_db() -> Path:
    """Get path to domain_status.db SQLite database."""
    # Try relative to current working directory first (for scripts)
    cwd_db = Path("data/domain_status.db")
    if cwd_db.exists():
        return cwd_db
    # Otherwise use absolute path from package
    return get_data_dir() / "domain_status.db"
