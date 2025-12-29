#!/usr/bin/env python3
"""
Quick diagnostic to check cache status before running pipelines.

Simulates exactly what each pipeline step would do with the cache,
reporting expected cache hits/misses without making any changes.

Usage:
    python scripts/check_cache_status.py
"""

from public_company_graph.cache import get_cache
from public_company_graph.cli import get_driver_and_database


def check_company_properties_cache():
    """Check if company_properties would be served from cache."""
    cache = get_cache()

    # Get companies from Neo4j (same query as enrich_company_properties.py)
    driver, db = get_driver_and_database(None)
    with driver.session(database=db) as session:
        result = session.run(
            """
            MATCH (c:Company)
            RETURN c.cik AS cik, c.ticker AS ticker
            ORDER BY c.ticker
            """
        )
        companies = [dict(row) for row in result]
    driver.close()

    if not companies:
        print("⚠ No Company nodes in Neo4j")
        return

    # Simulate cache lookups (exactly as enrich_company does)
    cache_hits = 0
    cache_misses = 0
    sample_misses = []

    for company in companies:
        cik = company.get("cik")
        if not cik:
            continue

        cached = cache.get("company_properties", cik)
        if cached:
            cache_hits += 1
        else:
            cache_misses += 1
            if len(sample_misses) < 5:
                sample_misses.append((cik, company.get("ticker")))

    hit_pct = cache_hits / len(companies) * 100 if companies else 0

    print(f"\n{'=' * 60}")
    print("Company Properties Cache Status")
    print("=" * 60)
    print(f"  Companies in Neo4j: {len(companies)}")
    print(f"  Would be CACHED:    {cache_hits} ({hit_pct:.1f}%)")
    print(f"  Would be FETCHED:   {cache_misses} ({100 - hit_pct:.1f}%)")

    if sample_misses:
        print(f"\n  Sample uncached companies: {sample_misses[:5]}")

    if hit_pct >= 90:
        print(f"\n  ✅ Cache is healthy - {hit_pct:.0f}% cache hits expected")
    elif hit_pct >= 50:
        print(f"\n  ⚠️  Partial cache - {100 - hit_pct:.0f}% will need API calls")
    else:
        print(
            f"\n  ❌ Cache mostly empty - {cache_misses} API calls needed (~{cache_misses // 10} seconds)"
        )


def main():
    """Run cache diagnostics."""
    cache = get_cache()

    print("=" * 60)
    print("Cache Diagnostic Report")
    print("=" * 60)

    # Overall stats
    stats = cache.stats()
    print(f"\nCache location: {stats['cache_dir']}")
    print(f"Current size: {stats['size_mb']:.1f} MB", end="")
    if stats.get("size_limit_mb"):
        print(f" / {stats['size_limit_mb']:.0f} MB limit ({stats['size_pct']:.0f}% used)")
        if stats["size_pct"] > 90:
            print("  ⚠️  WARNING: Cache near capacity - eviction may occur!")
    else:
        print(" (no limit)")
    print("\nEntries by namespace:")
    for ns, count in sorted(stats["by_namespace"].items()):
        print(f"  {ns}: {count:,}")
    print(f"  TOTAL: {stats['total']:,}")

    # Check company_properties specifically
    check_company_properties_cache()

    print("\n" + "=" * 60)


if __name__ == "__main__":
    main()
