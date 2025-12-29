#!/usr/bin/env python3
"""
Export all domains from parsed 10-K filings to a text file.

This script:
1. Reads all parsed 10-K data from cache (10k_extracted namespace)
2. Extracts company websites
3. Writes unique domains to a text file (one per line)
4. Output can be used with domain_status Rust tool

Usage:
    python scripts/export_10k_domains.py                    # Dry-run (show count)
    python scripts/export_10k_domains.py --execute           # Export to file
    python scripts/export_10k_domains.py --execute --output domains.txt  # Custom output file
"""

import argparse
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from domain_status_graph.cache import get_cache
from domain_status_graph.cli import setup_logging
from domain_status_graph.domain.validation import normalize_domain

logger = None


def extract_domains_from_cache(cache, output_file: Path = None) -> dict:
    """
    Extract all unique domains from 10-K cache.

    Returns:
        Dict with counts: total, with_domains, unique_domains, domains_list
    """
    # Get all 10-K extracted keys
    keys = cache.keys("10k_extracted", limit=100000)

    total = len(keys)
    domains = set()
    companies_with_domains = 0
    companies_without_domains = []

    logger.info(f"Processing {total} companies from 10-K cache...")

    for cik in keys:
        data = cache.get("10k_extracted", cik)
        if not data:
            continue

        website = data.get("website")
        if website:
            # Use centralized normalization (handles www, protocols, validation, etc.)
            normalized = normalize_domain(website)
            if normalized:
                domains.add(normalized)
                companies_with_domains += 1
        else:
            companies_without_domains.append(cik)

    unique_domains = sorted(domains)

    stats = {
        "total": total,
        "with_domains": companies_with_domains,
        "without_domains": len(companies_without_domains),
        "unique_domains": len(unique_domains),
        "domains_list": unique_domains,
    }

    return stats


def main():
    """Run the domain export script."""
    global logger

    parser = argparse.ArgumentParser(
        description="Export domains from parsed 10-K filings to text file"
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Actually export to file (default is dry-run)",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="data/10k_domains.txt",
        help="Output file path (default: data/10k_domains.txt)",
    )

    args = parser.parse_args()

    logger = setup_logging("export_10k_domains", execute=args.execute)

    cache = get_cache()

    # Extract domains
    stats = extract_domains_from_cache(cache)

    logger.info("=" * 80)
    logger.info("Domain Export Summary")
    logger.info("=" * 80)
    logger.info(f"Total companies in 10-K cache: {stats['total']}")
    logger.info(f"Companies with domains: {stats['with_domains']}")
    logger.info(f"Companies without domains: {stats['without_domains']}")
    logger.info(f"Unique domains: {stats['unique_domains']}")
    logger.info("")

    if not args.execute:
        logger.info("DRY RUN MODE")
        logger.info("=" * 80)
        logger.info(f"Would export {stats['unique_domains']} unique domains to: {args.output}")
        logger.info("To execute, run: python scripts/export_10k_domains.py --execute")
        logger.info("=" * 80)
        return

    # Write to file
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    logger.info(f"Writing {stats['unique_domains']} domains to: {output_path}")

    with open(output_path, "w") as f:
        for domain in stats["domains_list"]:
            f.write(f"{domain}\n")

    logger.info("=" * 80)
    logger.info("✓ Export Complete!")
    logger.info("=" * 80)
    logger.info(f"File: {output_path}")
    logger.info(f"Domains: {stats['unique_domains']}")
    logger.info("")
    logger.info("Next steps:")
    logger.info("  1. Use this file with domain_status Rust tool")
    logger.info(f"  2. Example: domain_status scan --domains-file {output_path}")


if __name__ == "__main__":
    main()
