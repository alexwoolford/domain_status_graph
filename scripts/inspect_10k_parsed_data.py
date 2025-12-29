#!/usr/bin/env python3
"""
Inspect what's been parsed from 10-K filings.

Shows sample data from the cache so you can see what's being extracted.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from domain_status_graph.cache import get_cache

CACHE_NAMESPACE = "10k_extracted"


def show_sample_data(limit: int = 5):
    """Show sample parsed data from cache."""
    cache = get_cache()
    total = cache.count(CACHE_NAMESPACE)

    print("=" * 80)
    print("10-K PARSED DATA INSPECTION")
    print("=" * 80)
    print()
    print(f"Total companies parsed: {total:,}")
    print()

    if total == 0:
        print("⚠ No parsed data found in cache.")
        print("Run: python scripts/parse_10k_filings.py --execute")
        return

    # Get sample keys
    sample_keys = cache.keys(CACHE_NAMESPACE, limit=limit)

    print(f"Showing {len(sample_keys)} sample(s):")
    print()

    for i, cik in enumerate(sample_keys, 1):
        data = cache.get(CACHE_NAMESPACE, cik)
        if not data:
            continue

        print("=" * 80)
        print(f"SAMPLE {i}: CIK {cik}")
        print("=" * 80)
        print()

        # Website
        if data.get("website"):
            print(f"Website: {data['website']}")
        else:
            print("Website: (not found)")
        print()

        # Business Description
        if data.get("business_description"):
            desc = data["business_description"]
            # Show first 500 chars
            preview = desc[:500] + "..." if len(desc) > 500 else desc
            print(f"Business Description ({len(desc):,} chars):")
            print("-" * 80)
            print(preview)
            print()
        else:
            print("Business Description: (not found)")
            print()

        # Competitors
        if data.get("competitors"):
            print(f"Competitors: {len(data['competitors'])} found")
            for comp in data["competitors"][:5]:
                print(f"  - {comp}")
            if len(data["competitors"]) > 5:
                print(f"  ... and {len(data['competitors']) - 5} more")
        else:
            print("Competitors: (not extracted yet)")
        print()

        # Other fields
        other_fields = {
            k: v
            for k, v in data.items()
            if k not in ["website", "business_description", "competitors"]
        }
        if other_fields:
            print("Other fields:")
            for key, value in other_fields.items():
                if isinstance(value, str) and len(value) > 100:
                    print(f"  {key}: {value[:100]}...")
                else:
                    print(f"  {key}: {value}")
        print()


def show_statistics():
    """Show statistics about parsed data."""
    cache = get_cache()
    total = cache.count(CACHE_NAMESPACE)

    if total == 0:
        print("No data to analyze.")
        return

    print("=" * 80)
    print("PARSING STATISTICS")
    print("=" * 80)
    print()

    # Get ALL keys for accurate statistics (not just a sample)
    all_keys = cache.keys(CACHE_NAMESPACE, limit=total)

    with_website = 0
    with_description = 0
    with_competitors = 0
    total_desc_length = 0

    print(f"Analyzing {len(all_keys):,} companies...")
    print()

    for cik in all_keys:
        data = cache.get(CACHE_NAMESPACE, cik)
        if not data:
            continue

        if data.get("website"):
            with_website += 1
        if data.get("business_description"):
            with_description += 1
            total_desc_length += len(data["business_description"])
        if data.get("competitors"):
            with_competitors += 1

    print(f"Total parsed: {total:,}")
    print()
    print(f"With website: {with_website:,} ({with_website / total * 100:.1f}%)")
    print(
        f"With business description: {with_description:,} ({with_description / total * 100:.1f}%)"
    )
    if with_description > 0:
        avg_length = total_desc_length / with_description
        print(f"  Average description length: {avg_length:,.0f} characters")
    print(f"With competitors: {with_competitors:,} ({with_competitors / total * 100:.1f}%)")
    print()


def main():
    """Main entry point."""
    import argparse

    parser = argparse.ArgumentParser(description="Inspect parsed 10-K data")
    parser.add_argument(
        "--samples",
        type=int,
        default=5,
        help="Number of samples to show (default: 5)",
    )
    parser.add_argument(
        "--stats",
        action="store_true",
        help="Show statistics instead of samples",
    )
    args = parser.parse_args()

    if args.stats:
        show_statistics()
    else:
        show_sample_data(limit=args.samples)


if __name__ == "__main__":
    main()
