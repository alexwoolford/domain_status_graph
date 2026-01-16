#!/usr/bin/env python3
"""
Ensure the vector index for Chunk embeddings exists and is online.

This script:
1. Creates the vector index if it doesn't exist
2. Waits for it to come online
3. Reports the index status

Usage:
    python scripts/ensure_vector_index.py
"""

import sys
import time
from pathlib import Path

from dotenv import load_dotenv

from public_company_graph.config import Settings
from public_company_graph.neo4j.connection import get_neo4j_driver
from public_company_graph.neo4j.constraints import create_document_constraints

load_dotenv(Path(__file__).parent.parent / ".env")


def check_index_status(driver, index_name: str, database: str | None = None):
    """Check if index exists and its status."""
    with driver.session(database=database) as session:
        result = session.run(
            "SHOW VECTOR INDEXES YIELD name, state, type WHERE name = $name RETURN name, state, type",
            name=index_name,
        )
        record = result.single()
        if record:
            return record["state"], record["type"]
        return None, None


def wait_for_index_online(
    driver, index_name: str, database: str | None = None, max_wait_seconds: int = 300
):
    """Wait for index to come online."""
    start_time = time.time()
    print(f"Waiting for index '{index_name}' to come online (max {max_wait_seconds}s)...")

    while time.time() - start_time < max_wait_seconds:
        state, index_type = check_index_status(driver, index_name, database)
        if state == "ONLINE":
            print("✓ Index is ONLINE!")
            return True
        elif state in ("POPULATING", "BUILDING"):
            elapsed = int(time.time() - start_time)
            print(f"  Index is {state}... ({elapsed}s elapsed)")
            time.sleep(5)
        elif state is None:
            print("  Index not found, creating...")
            return False
        else:
            print(f"  Index state: {state}")
            time.sleep(5)

    print("⚠ Timeout waiting for index to come online")
    return False


def main():
    settings = Settings()
    driver = get_neo4j_driver()

    index_name = "chunk_embedding_vector"

    try:
        print("=" * 70)
        print("Vector Index Status Check")
        print("=" * 70)
        print()

        # Check current status
        state, index_type = check_index_status(driver, index_name, settings.neo4j_database)

        if state == "ONLINE":
            print(f"✓ Vector index '{index_name}' is already ONLINE")
            print(f"  Type: {index_type}")
            return 0
        elif state in ("POPULATING", "BUILDING"):
            print(f"Index '{index_name}' is {state}, waiting for it to come online...")
            if wait_for_index_online(driver, index_name, settings.neo4j_database):
                return 0
            else:
                return 1
        else:
            print(f"Index '{index_name}' not found. Creating...")
            print()

            # Create the index via constraints function
            create_document_constraints(driver, database=settings.neo4j_database)

            print()
            print("Waiting for index to be created and come online...")
            if wait_for_index_online(driver, index_name, settings.neo4j_database):
                return 0
            else:
                print()
                print("⚠ Index creation may still be in progress.")
                print("  You can check status with: SHOW VECTOR INDEXES")
                return 1

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback

        traceback.print_exc()
        return 1
    finally:
        driver.close()


if __name__ == "__main__":
    sys.exit(main())
