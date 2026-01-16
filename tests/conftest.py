"""
Pytest configuration and shared fixtures for public_company_graph tests.
"""

import os
from pathlib import Path

import pytest

# Set test environment variables if not already set
if not os.getenv("NEO4J_URI"):
    os.environ["NEO4J_URI"] = "bolt://localhost:7687"
if not os.getenv("NEO4J_USER"):
    os.environ["NEO4J_USER"] = "neo4j"
if not os.getenv("NEO4J_DATABASE"):
    os.environ["NEO4J_DATABASE"] = "neo4j"


@pytest.fixture
def test_data_dir():
    """Get path to test data directory."""
    return Path(__file__).parent.parent / "data"


@pytest.fixture
def test_domain_status_db(test_data_dir):
    """Get path to test domain_status.db file."""
    db_path = test_data_dir / "domain_status.db"
    if not db_path.exists():
        pytest.skip(f"Test database not found at {db_path}")
    return db_path


class MockRecord:
    """Mock Neo4j record for testing."""

    def __init__(self, data: dict):
        self._data = data

    def __getitem__(self, key):
        return self._data[key]

    def get(self, key, default=None):
        return self._data.get(key, default)

    def __contains__(self, key):
        return key in self._data


class MockResult:
    """
    Shared Mock Neo4j result for testing.

    This class provides a consistent mock implementation of Neo4j's Result object
    across all test files, ensuring consistent behavior.

    Supports multiple initialization patterns:
    - MockResult([{"key": "value"}]) - List of dicts (most common)
    - MockResult({"key": "value"}) - Single dict
    - MockResult([record1, record2]) - List of MockRecord objects
    - MockResult(data) - Raw data (for backward compatibility)
    """

    def __init__(self, records: list[dict] | dict | list | None = None):
        """
        Initialize MockResult.

        Args:
            records: Can be:
                - List of dict records (most common)
                - Single dict record
                - List of MockRecord objects
                - Raw data (for backward compatibility with some tests)
                - None (empty result)
        """
        if records is None:
            self._records = []
        elif isinstance(records, dict):
            # Single record - wrap in list
            self._records = [MockRecord(records)]
        elif isinstance(records, list):
            if not records:
                self._records = []
            elif isinstance(records[0], MockRecord):
                # Already MockRecord objects
                self._records = records
            elif isinstance(records[0], dict):
                # List of dicts - convert to MockRecord
                self._records = [MockRecord(r) for r in records]
            else:
                # Raw data (backward compatibility)
                self._records = records
        else:
            # Raw data (backward compatibility)
            self._records = [records] if records else []

    def __iter__(self):
        """Iterate over records."""
        return iter(self._records)

    def single(self):
        """Return the first record or None if empty (matches Neo4j API)."""
        if not self._records:
            return None
        first = self._records[0]
        # If it's a MockRecord, return it; otherwise return as-is (for backward compatibility)
        return first
