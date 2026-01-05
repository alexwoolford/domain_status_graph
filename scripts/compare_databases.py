#!/usr/bin/env python3
"""Compare two Neo4j databases to check for material differences."""

import os

from dotenv import load_dotenv
from neo4j import GraphDatabase

load_dotenv()


def get_database_stats(driver, database_name: str) -> dict:
    """Get statistics for a database."""
    stats = {
        "database": database_name,
        "node_labels": {},
        "relationship_types": {},
        "total_nodes": 0,
        "total_relationships": 0,
        "node_properties": set(),
        "relationship_properties": set(),
    }

    with driver.session(database=database_name) as session:
        # Get node labels and counts
        result = session.run(
            "MATCH (n) RETURN labels(n)[0] AS label, count(*) AS count ORDER BY count DESC"
        )
        for record in result:
            label = record["label"]
            count = record["count"]
            stats["node_labels"][label] = count
            stats["total_nodes"] += count

        # Get relationship types and counts
        result = session.run(
            "MATCH ()-[r]->() RETURN type(r) AS type, count(*) AS count ORDER BY count DESC"
        )
        for record in result:
            rel_type = record["type"]
            count = record["count"]
            stats["relationship_types"][rel_type] = count
            stats["total_relationships"] += count

        # Get node property keys
        result = session.run("CALL db.propertyKeys() YIELD propertyKey RETURN propertyKey")
        for record in result:
            stats["node_properties"].add(record["propertyKey"])

        # Get a sample of properties per label
        stats["label_properties"] = {}
        for label in stats["node_labels"].keys():
            if label:
                result = session.run(f"MATCH (n:`{label}`) WITH n LIMIT 1 RETURN keys(n) AS props")
                record = result.single()
                if record:
                    stats["label_properties"][label] = sorted(record["props"])

        # Get relationship properties per type
        stats["rel_type_properties"] = {}
        for rel_type in stats["relationship_types"].keys():
            result = session.run(
                f"MATCH ()-[r:`{rel_type}`]->() WITH r LIMIT 1 RETURN keys(r) AS props"
            )
            record = result.single()
            if record and record["props"]:
                stats["rel_type_properties"][rel_type] = sorted(record["props"])

    return stats


def check_key_entities(driver, db1: str, db2: str) -> None:
    """Check for specific key entities that should exist in both databases."""
    print("\n" + "-" * 40)
    print("KEY ENTITY CHECKS")
    print("-" * 40)

    checks = [
        (
            "Company nodes with CIK",
            "MATCH (c:Company) WHERE c.cik IS NOT NULL RETURN count(c) AS count",
        ),
        (
            "Company nodes with ticker",
            "MATCH (c:Company) WHERE c.ticker IS NOT NULL RETURN count(c) AS count",
        ),
        (
            "Company nodes with description",
            "MATCH (c:Company) WHERE c.description IS NOT NULL RETURN count(c) AS count",
        ),
        (
            "Company nodes with embeddings",
            "MATCH (c:Company) WHERE c.description_embedding IS NOT NULL RETURN count(c) AS count",
        ),
        ("Domain nodes", "MATCH (d:Domain) RETURN count(d) AS count"),
        ("Technology nodes", "MATCH (t:Technology) RETURN count(t) AS count"),
        (
            "Company-Domain relationships",
            "MATCH (c:Company)-[:HAS_DOMAIN]->(d:Domain) RETURN count(*) AS count",
        ),
        (
            "Company-Technology relationships",
            "MATCH (c:Company)-[:USES]->(t:Technology) RETURN count(*) AS count",
        ),
        (
            "Domain-Technology relationships",
            "MATCH (d:Domain)-[:USES]->(t:Technology) RETURN count(*) AS count",
        ),
        (
            "Business relationships (HAS_COMPETITOR)",
            "MATCH ()-[r:HAS_COMPETITOR]->() RETURN count(r) AS count",
        ),
        (
            "Business relationships (HAS_SUPPLIER)",
            "MATCH ()-[r:HAS_SUPPLIER]->() RETURN count(r) AS count",
        ),
        (
            "Business relationships (HAS_CUSTOMER)",
            "MATCH ()-[r:HAS_CUSTOMER]->() RETURN count(r) AS count",
        ),
        (
            "Similarity relationships (SIMILAR_INDUSTRY)",
            "MATCH ()-[r:SIMILAR_INDUSTRY]->() RETURN count(r) AS count",
        ),
        (
            "Similarity relationships (SIMILAR_SIZE)",
            "MATCH ()-[r:SIMILAR_SIZE]->() RETURN count(r) AS count",
        ),
    ]

    print(f"\n{'Check':<50} {'domain':>15} {'domain1':>15} {'Diff':>15}")
    print("-" * 95)

    for check_name, query in checks:
        with driver.session(database=db1) as session:
            result = session.run(query)
            count1 = result.single()["count"] if result.peek() else 0

        with driver.session(database=db2) as session:
            result = session.run(query)
            count2 = result.single()["count"] if result.peek() else 0

        diff = count2 - count1
        diff_str = f"+{diff}" if diff > 0 else str(diff)
        status = "✓" if count1 == count2 else "⚠"
        print(f"{status} {check_name:<48} {count1:>15,} {count2:>15,} {diff_str:>15}")


def compare_databases(stats1: dict, stats2: dict) -> None:
    """Compare two database stats and print differences."""
    print("=" * 80)
    print(f"DATABASE COMPARISON: {stats1['database']} vs {stats2['database']}")
    print("=" * 80)

    # Node label comparison
    print("\n" + "-" * 40)
    print("NODE LABELS")
    print("-" * 40)

    labels1 = set(stats1["node_labels"].keys())
    labels2 = set(stats2["node_labels"].keys())

    only_in_1 = labels1 - labels2
    only_in_2 = labels2 - labels1
    common = labels1 & labels2

    if only_in_1:
        print(f"\nOnly in {stats1['database']}: {only_in_1}")
    if only_in_2:
        print(f"\nOnly in {stats2['database']}: {only_in_2}")

    print(f"\n{'Label':<25} {stats1['database']:>15} {stats2['database']:>15} {'Diff':>15}")
    print("-" * 70)
    for label in sorted(common):
        count1 = stats1["node_labels"].get(label, 0)
        count2 = stats2["node_labels"].get(label, 0)
        diff = count2 - count1
        diff_str = f"+{diff}" if diff > 0 else str(diff)
        print(f"{label:<25} {count1:>15,} {count2:>15,} {diff_str:>15}")

    for label in sorted(only_in_1):
        count1 = stats1["node_labels"].get(label, 0)
        print(f"{label:<25} {count1:>15,} {'N/A':>15} {'-':>15}")

    for label in sorted(only_in_2):
        count2 = stats2["node_labels"].get(label, 0)
        print(f"{label:<25} {'N/A':>15} {count2:>15,} {'-':>15}")

    print(f"\n{'TOTAL NODES':<25} {stats1['total_nodes']:>15,} {stats2['total_nodes']:>15,}")

    # Relationship type comparison
    print("\n" + "-" * 40)
    print("RELATIONSHIP TYPES")
    print("-" * 40)

    types1 = set(stats1["relationship_types"].keys())
    types2 = set(stats2["relationship_types"].keys())

    only_in_1 = types1 - types2
    only_in_2 = types2 - types1
    common = types1 & types2

    if only_in_1:
        print(f"\nOnly in {stats1['database']}: {sorted(only_in_1)}")
    if only_in_2:
        print(f"\nOnly in {stats2['database']}: {sorted(only_in_2)}")

    print(f"\n{'Relationship':<25} {stats1['database']:>15} {stats2['database']:>15} {'Diff':>15}")
    print("-" * 70)
    for rel_type in sorted(common):
        count1 = stats1["relationship_types"].get(rel_type, 0)
        count2 = stats2["relationship_types"].get(rel_type, 0)
        diff = count2 - count1
        diff_str = f"+{diff}" if diff > 0 else str(diff)
        print(f"{rel_type:<25} {count1:>15,} {count2:>15,} {diff_str:>15}")

    for rel_type in sorted(only_in_1):
        count1 = stats1["relationship_types"].get(rel_type, 0)
        print(f"{rel_type:<25} {count1:>15,} {'N/A':>15} {'-':>15}")

    for rel_type in sorted(only_in_2):
        count2 = stats2["relationship_types"].get(rel_type, 0)
        print(f"{rel_type:<25} {'N/A':>15} {count2:>15,} {'-':>15}")

    print(
        f"\n{'TOTAL RELATIONSHIPS':<25} {stats1['total_relationships']:>15,} {stats2['total_relationships']:>15,}"
    )

    # Properties comparison
    print("\n" + "-" * 40)
    print("NODE PROPERTIES BY LABEL")
    print("-" * 40)

    all_labels = set(stats1.get("label_properties", {}).keys()) | set(
        stats2.get("label_properties", {}).keys()
    )
    for label in sorted(all_labels):
        props1 = set(stats1.get("label_properties", {}).get(label, []))
        props2 = set(stats2.get("label_properties", {}).get(label, []))

        if props1 != props2:
            print(f"\n{label}:")
            only_in_1 = props1 - props2
            only_in_2 = props2 - props1
            if only_in_1:
                print(f"  Only in {stats1['database']}: {sorted(only_in_1)}")
            if only_in_2:
                print(f"  Only in {stats2['database']}: {sorted(only_in_2)}")
        else:
            print(f"\n{label}: identical ({len(props1)} properties)")

    # Relationship properties comparison
    print("\n" + "-" * 40)
    print("RELATIONSHIP PROPERTIES BY TYPE")
    print("-" * 40)

    all_rel_types = set(stats1.get("rel_type_properties", {}).keys()) | set(
        stats2.get("rel_type_properties", {}).keys()
    )
    for rel_type in sorted(all_rel_types):
        props1 = set(stats1.get("rel_type_properties", {}).get(rel_type, []))
        props2 = set(stats2.get("rel_type_properties", {}).get(rel_type, []))

        if props1 or props2:
            if props1 != props2:
                print(f"\n{rel_type}:")
                only_in_1 = props1 - props2
                only_in_2 = props2 - props1
                if only_in_1:
                    print(f"  Only in {stats1['database']}: {sorted(only_in_1)}")
                if only_in_2:
                    print(f"  Only in {stats2['database']}: {sorted(only_in_2)}")
            else:
                print(f"\n{rel_type}: identical ({len(props1)} properties)")


def main():
    uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
    user = os.getenv("NEO4J_USER", "neo4j")
    password = os.getenv("NEO4J_PASSWORD")

    if not password:
        print("Error: NEO4J_PASSWORD not set")
        return

    driver = GraphDatabase.driver(uri, auth=(user, password))

    try:
        # Test connection
        driver.verify_connectivity()
        print("Connected to Neo4j successfully\n")

        # Get stats for both databases
        print("Fetching stats for 'domain' database...")
        domain_stats = get_database_stats(driver, "domain")

        print("Fetching stats for 'domain2' database...")
        domain2_stats = get_database_stats(driver, "domain2")

        # Compare
        compare_databases(domain_stats, domain2_stats)

        # Check key entities
        check_key_entities(driver, "domain", "domain2")

    except Exception as e:
        print(f"Error: {e}")
    finally:
        driver.close()


if __name__ == "__main__":
    main()
