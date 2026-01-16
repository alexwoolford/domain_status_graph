#!/usr/bin/env python3
"""
Test all read-only Cypher queries in the codebase.

Extracts Cypher queries from:
1. Python source files
2. Documentation files (markdown)
3. Example queries

Only tests read-only queries (no CREATE, MERGE, DELETE, SET, REMOVE, etc.)

Usage:
    python scripts/test_all_cypher_queries.py
"""

import re
import sys
from pathlib import Path
from typing import Any

from public_company_graph.cli import get_driver_and_database, setup_logging


def is_read_only_query(query: str) -> bool:
    """Check if a Cypher query is read-only (no mutations)."""
    query_upper = query.upper()
    # Mutating keywords
    mutating_keywords = [
        "CREATE",
        "MERGE",
        "DELETE",
        "DETACH DELETE",
        "SET",
        "REMOVE",
        "DROP",
        "CALL.*WRITE",
        "CALL.*CREATE",
        "CALL.*DELETE",
    ]
    for keyword in mutating_keywords:
        if re.search(rf"\b{keyword}\b", query_upper):
            return False
    return True


def extract_queries_from_python_file(file_path: Path) -> list[dict[str, Any]]:
    """Extract Cypher queries from a Python file."""
    queries = []
    try:
        with open(file_path, encoding="utf-8") as f:
            content = f.read()

        # Pattern 1: Triple-quoted strings with MATCH/RETURN (but not docstrings)
        # Look for patterns like: query = """...MATCH...""" or session.run("""...MATCH...""")
        pattern1 = r'(?:query\s*=|session\.run\(|result\s*=\s*session\.run\()\s*"""(.*?MATCH.*?RETURN.*?)"""'
        for match in re.finditer(pattern1, content, re.DOTALL):
            query = match.group(1).strip()
            # Skip if it looks like a docstring (starts with description, has "Args:", etc.)
            if not re.match(
                r"^[A-Z][a-z]+.*(?:Args|Returns|Description|Example)", query, re.MULTILINE
            ):
                if is_read_only_query(query) and "{" not in query:  # Skip template queries
                    queries.append(
                        {
                            "query": query,
                            "file": str(file_path),
                            "line": content[: match.start()].count("\n") + 1,
                            "type": "triple_quoted",
                        }
                    )

        # Pattern 2: session.run("...") or session.run('...') - single line queries
        pattern2 = r'session\.run\(["\']([^"\']*MATCH[^"\']*RETURN[^"\']*)["\']'
        for match in re.finditer(pattern2, content):
            query = match.group(1).strip()
            if "MATCH" in query or "RETURN" in query or "SHOW" in query or "CALL" in query:
                if is_read_only_query(query) and "{" not in query:  # Skip template queries
                    queries.append(
                        {
                            "query": query,
                            "file": str(file_path),
                            "line": content[: match.start()].count("\n") + 1,
                            "type": "session_run",
                        }
                    )

        # Pattern 3: query = """...""" (multi-line) - but not docstrings
        pattern3 = r'query\s*=\s*"""(.*?)"""'
        for match in re.finditer(pattern3, content, re.DOTALL):
            query = match.group(1).strip()
            # Skip if it looks like a docstring
            if not re.match(
                r"^[A-Z][a-z]+.*(?:Args|Returns|Description|Example)", query, re.MULTILINE
            ):
                if (
                    "MATCH" in query or "RETURN" in query or "SHOW" in query or "CALL" in query
                ) and "{" not in query:
                    if is_read_only_query(query):
                        queries.append(
                            {
                                "query": query,
                                "file": str(file_path),
                                "line": content[: match.start()].count("\n") + 1,
                                "type": "query_var",
                            }
                        )

    except Exception as e:
        print(f"  ⚠ Error reading {file_path}: {e}")
    return queries


def extract_queries_from_markdown(file_path: Path) -> list[dict[str, Any]]:
    """Extract Cypher queries from markdown files (code blocks)."""
    queries = []
    try:
        with open(file_path, encoding="utf-8") as f:
            content = f.read()

        # Pattern: ```cypher ... ``` or ``` ... ``` (assume Cypher if MATCH/RETURN present)
        pattern = r"```(?:cypher|cypher)?\n(.*?)```"
        for match in re.finditer(pattern, content, re.DOTALL):
            query = match.group(1).strip()
            # Skip if it looks like documentation text (starts with **, contains "Expected", etc.)
            if (
                query.startswith("**")
                or "Expected" in query[:50]
                or not any(
                    keyword in query
                    for keyword in ["MATCH", "RETURN", "SHOW", "CALL", "WITH", "WHERE"]
                )
            ):
                continue
            if "MATCH" in query or "RETURN" in query or "SHOW" in query or "CALL" in query:
                if is_read_only_query(query):
                    queries.append(
                        {
                            "query": query,
                            "file": str(file_path),
                            "line": content[: match.start()].count("\n") + 1,
                            "type": "markdown",
                        }
                    )
    except Exception as e:
        print(f"  ⚠ Error reading {file_path}: {e}")
    return queries


def normalize_query(query: str) -> str:
    """Normalize query for deduplication (remove comments, normalize whitespace)."""
    # Remove comments
    query = re.sub(r"//.*?$", "", query, flags=re.MULTILINE)
    # Normalize whitespace
    query = re.sub(r"\s+", " ", query)
    return query.strip()


def test_query(driver, database: str, query_info: dict[str, Any], logger) -> tuple[bool, str]:
    """Test a single Cypher query."""
    query = query_info["query"]

    try:
        # Check if query has parameters that need values
        # Simple check: if it has $param, we might need to provide it
        # For now, we'll try to run it and catch errors
        with driver.session(database=database) as session:
            # Try to execute with a small limit if it doesn't have one
            test_query = query
            if "LIMIT" not in query.upper() and "RETURN" in query.upper():
                # Add a small limit to avoid huge results
                test_query = query.rstrip(";").rstrip() + " LIMIT 10"

            result = session.run(test_query)
            # Consume the result (important!)
            records = list(result)
            return True, f"✓ {len(records)} records"
    except Exception as e:
        error_msg = str(e)
        # Some errors are expected (e.g., missing parameters, missing data)
        if "parameter" in error_msg.lower() or "missing" in error_msg.lower():
            return None, f"⚠ Parameter/missing data: {error_msg[:100]}"
        return False, f"✗ Error: {error_msg[:200]}"


def main():
    """Main function to test all Cypher queries."""
    logger = setup_logging("test_cypher_queries", execute=False)

    print("=" * 80)
    print("CYPHER QUERY VALIDATION")
    print("=" * 80)
    print()

    # Get Neo4j connection
    try:
        driver, database = get_driver_and_database(logger)
        print(f"✓ Connected to Neo4j (database: {database})")
        print()
    except Exception as e:
        print(f"✗ Failed to connect to Neo4j: {e}")
        sys.exit(1)

    # Find all Python files
    project_root = Path(__file__).parent.parent
    python_files = list(project_root.rglob("*.py"))
    markdown_files = list(project_root.rglob("*.md"))

    # Filter out test files, venv, archive, and validation report
    python_files = [
        f
        for f in python_files
        if "test" not in str(f)
        and "venv" not in str(f)
        and ".venv" not in str(f)
        and "archive" not in str(f)
    ]
    markdown_files = [
        f
        for f in markdown_files
        if "CYPHER_QUERY_VALIDATION_REPORT" not in str(f)
        and "VALIDATION" not in str(f)
        and "archive" not in str(f)
    ]

    print(f"Scanning {len(python_files)} Python files and {len(markdown_files)} markdown files...")
    print()

    all_queries = []

    # Extract from Python files
    for py_file in python_files:
        queries = extract_queries_from_python_file(py_file)
        all_queries.extend(queries)

    # Extract from markdown files
    for md_file in markdown_files:
        queries = extract_queries_from_markdown(md_file)
        all_queries.extend(queries)

    print(f"Found {len(all_queries)} read-only Cypher queries")
    print()

    # Deduplicate queries (by normalized content)
    seen = set()
    unique_queries = []
    for q in all_queries:
        normalized = normalize_query(q["query"])
        if normalized not in seen:
            seen.add(normalized)
            unique_queries.append(q)

    print(f"After deduplication: {len(unique_queries)} unique queries")
    print()

    # Test each query
    results = {"passed": [], "failed": [], "skipped": []}

    for i, query_info in enumerate(unique_queries, 1):
        file_path = query_info["file"]
        line = query_info.get("line", "?")
        query_preview = query_info["query"][:100].replace("\n", " ")

        print(f"[{i}/{len(unique_queries)}] {Path(file_path).name}:{line}")
        print(f"  Query: {query_preview}...")

        status, message = test_query(driver, database, query_info, logger)

        if status is True:
            results["passed"].append(query_info)
            print(f"  {message}")
        elif status is False:
            results["failed"].append(query_info)
            print(f"  {message}")
        else:
            results["skipped"].append(query_info)
            print(f"  {message}")

        print()

    # Summary
    print("=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print(f"Total queries tested: {len(unique_queries)}")
    print(f"✓ Passed: {len(results['passed'])}")
    print(f"✗ Failed: {len(results['failed'])}")
    print(f"⚠ Skipped (parameters/missing data): {len(results['skipped'])}")
    print()

    if results["failed"]:
        print("FAILED QUERIES:")
        print("-" * 80)
        for q in results["failed"]:
            print(f"  {Path(q['file']).name}:{q.get('line', '?')}")
            print(f"    {q['query'][:200]}...")
            print()

    driver.close()

    return 0 if len(results["failed"]) == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
