#!/usr/bin/env python3
"""
CLI for managing the embedding cache.

Uses the unified AppCache (diskcache-based) with the 'embeddings' namespace.

Usage:
    python scripts/embedding_cache.py stats           # Show cache statistics
    python scripts/embedding_cache.py list            # List recent keys
    python scripts/embedding_cache.py list --limit 20
    python scripts/embedding_cache.py clear --all     # Clear all embeddings
"""

import argparse

from public_company_graph.cache import get_cache

# Namespace for embeddings in the unified cache
EMBEDDINGS_NAMESPACE = "embeddings"


def main():
    parser = argparse.ArgumentParser(description="Manage embedding cache")

    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    # stats command
    subparsers.add_parser("stats", help="Show cache statistics")

    # list command
    list_parser = subparsers.add_parser("list", help="List cache keys")
    list_parser.add_argument(
        "--limit",
        type=int,
        default=20,
        help="Maximum keys to show (default: 20)",
    )

    # clear command
    clear_parser = subparsers.add_parser("clear", help="Clear cache entries")
    clear_parser.add_argument(
        "--all",
        action="store_true",
        required=True,
        help="Clear all embeddings",
    )
    clear_parser.add_argument(
        "--yes",
        action="store_true",
        help="Skip confirmation prompt",
    )

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return

    cache = get_cache()

    if args.command == "stats":
        stats = cache.stats()
        embeddings_count = stats["by_namespace"].get(EMBEDDINGS_NAMESPACE, 0)
        print(f"Embedding Cache: {stats['cache_dir']}")
        print(f"  Total cache entries: {stats['total']}")
        print(f"  Embeddings: {embeddings_count}")
        print(f"  Database size: {stats['size_mb']} MB")
        print("  By namespace:")
        for ns, count in sorted(stats["by_namespace"].items()):
            print(f"    {ns}: {count}")

    elif args.command == "list":
        keys = cache.keys(namespace=EMBEDDINGS_NAMESPACE, limit=args.limit)
        if not keys:
            print("No embeddings found")
            return
        print(f"Recent embeddings ({len(keys)} shown):")
        for key in keys:
            print(f"  {key}")

    elif args.command == "clear":
        count = cache.count(namespace=EMBEDDINGS_NAMESPACE)
        if count == 0:
            print("No embeddings found in cache")
            return
        if not args.yes:
            confirm = input(f"Delete ALL {count} embeddings? [y/N] ")
            if confirm.lower() != "y":
                print("Cancelled")
                return
        deleted = cache.clear_namespace(EMBEDDINGS_NAMESPACE)
        print(f"Deleted {deleted} embeddings")


if __name__ == "__main__":
    main()
