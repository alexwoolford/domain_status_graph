"""
SQLite data readers for domain status database.

This module provides functions to read data from the SQLite database
and return structured data that can be loaded into Neo4j.
"""

import sqlite3
from contextlib import closing
from datetime import UTC, datetime
from pathlib import Path


def read_domains(db_path: Path) -> list[dict]:
    """
    Read Domain data from SQLite url_status table.

    Args:
        db_path: Path to SQLite database

    Returns:
        List of domain dictionaries with all properties
    """
    # Use closing() to ensure connection is closed in Python 3.13+
    # The 'with sqlite3.connect()' context manager only manages transactions, not connection closure
    with closing(sqlite3.connect(db_path)) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT DISTINCT
                us.final_domain,
                us.initial_domain,
                us.http_status,
                us.http_status_text,
                us.response_time_seconds,
                us.observed_at_ms,
                us.is_mobile_friendly,
                us.spf_record,
                us.dmarc_record,
                us.title,
                us.keywords,
                us.description,
                w.creation_date_ms,
                w.expiration_date_ms,
                w.registrar,
                w.registrant_country,
                w.registrant_org
            FROM url_status us
            LEFT JOIN url_whois w ON us.id = w.url_status_id
            WHERE us.final_domain IS NOT NULL
            """
        )
        rows = cursor.fetchall()
        # Convert to dicts and normalize field names for backward compatibility
        domains = []
        for row in rows:
            domain_dict = dict(row)
            # Map new schema to expected field names for loaders
            domain_dict["domain"] = domain_dict.get("initial_domain", "")
            domain_dict["status"] = domain_dict.get("http_status", 0)
            domain_dict["status_description"] = domain_dict.get("http_status_text", "")
            domain_dict["response_time"] = domain_dict.get("response_time_seconds", 0.0)
            # Convert milliseconds to datetime string (ISO format)
            observed_ms = domain_dict.get("observed_at_ms")
            if observed_ms:
                domain_dict["timestamp"] = datetime.fromtimestamp(
                    observed_ms / 1000.0, tz=UTC
                ).isoformat()
            else:
                domain_dict["timestamp"] = None
            # Convert whois dates from milliseconds
            creation_ms = domain_dict.get("creation_date_ms")
            if creation_ms:
                domain_dict["creation_date"] = datetime.fromtimestamp(
                    creation_ms / 1000.0, tz=UTC
                ).isoformat()
            else:
                domain_dict["creation_date"] = None
            expiration_ms = domain_dict.get("expiration_date_ms")
            if expiration_ms:
                domain_dict["expiration_date"] = datetime.fromtimestamp(
                    expiration_ms / 1000.0, tz=UTC
                ).isoformat()
            else:
                domain_dict["expiration_date"] = None
            domains.append(domain_dict)
        return domains


def read_technologies(db_path: Path) -> list[dict]:
    """
    Read Technology data and domain-technology mappings from SQLite.

    Args:
        db_path: Path to SQLite database

    Returns:
        List of dictionaries with final_domain, technology_name, technology_category
    """
    # Use closing() to ensure connection is closed in Python 3.13+
    with closing(sqlite3.connect(db_path)) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT DISTINCT us.final_domain, ut.technology_name, ut.technology_category
            FROM url_status us
            JOIN url_technologies ut ON us.id = ut.url_status_id
            WHERE ut.technology_name IS NOT NULL AND ut.technology_name != ''
            """
        )
        return [dict(row) for row in cursor.fetchall()]


def get_domain_count(db_path: Path) -> int:
    """Get count of distinct domains in database."""
    # Use closing() to ensure connection is closed in Python 3.13+
    with closing(sqlite3.connect(db_path)) as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT COUNT(DISTINCT final_domain) FROM url_status WHERE final_domain IS NOT NULL"
        )
        result = cursor.fetchone()[0]
        return int(result) if result is not None else 0


def get_technology_count(db_path: Path) -> int:
    """Get count of distinct technologies in database."""
    # Use closing() to ensure connection is closed in Python 3.13+
    with closing(sqlite3.connect(db_path)) as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT COUNT(DISTINCT ut.technology_name)
            FROM url_technologies ut
            WHERE ut.technology_name IS NOT NULL AND ut.technology_name != ''
            """
        )
        result = cursor.fetchone()[0]
        return int(result) if result is not None else 0


def get_uses_relationship_count(db_path: Path) -> int:
    """Get count of domain-technology relationships in database."""
    # Use closing() to ensure connection is closed in Python 3.13+
    with closing(sqlite3.connect(db_path)) as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT COUNT(*)
            FROM url_technologies ut
            WHERE ut.technology_name IS NOT NULL AND ut.technology_name != ''
            """
        )
        result = cursor.fetchone()[0]
        return int(result) if result is not None else 0


def get_domain_metadata_counts(db_path: Path) -> dict[str, int]:
    """
    Get counts of domains with metadata (title, keywords, description).

    Returns:
        Dictionary with keys: total, with_title, with_keywords, with_description
    """
    # Use closing() to ensure connection is closed in Python 3.13+
    with closing(sqlite3.connect(db_path)) as conn:
        cursor = conn.cursor()

        cursor.execute(
            "SELECT COUNT(DISTINCT final_domain) FROM url_status "
            "WHERE title IS NOT NULL AND title != ''"
        )
        with_title = cursor.fetchone()[0]

        cursor.execute(
            "SELECT COUNT(DISTINCT final_domain) FROM url_status "
            "WHERE keywords IS NOT NULL AND keywords != ''"
        )
        with_keywords = cursor.fetchone()[0]

        cursor.execute(
            "SELECT COUNT(DISTINCT final_domain) FROM url_status "
            "WHERE description IS NOT NULL AND description != ''"
        )
        with_description = cursor.fetchone()[0]

        cursor.execute(
            "SELECT COUNT(DISTINCT final_domain) FROM url_status WHERE final_domain IS NOT NULL"
        )
        total = cursor.fetchone()[0]

        return {
            "total": total,
            "with_title": with_title,
            "with_keywords": with_keywords,
            "with_description": with_description,
        }
