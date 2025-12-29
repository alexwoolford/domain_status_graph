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

        print("Fetching stats for 'domain3' database...")
        domain3_stats = get_database_stats(driver, "domain3")

        # Compare
        compare_databases(domain_stats, domain3_stats)

    except Exception as e:
        print(f"Error: {e}")
    finally:
        driver.close()


if __name__ == "__main__":
    main()
