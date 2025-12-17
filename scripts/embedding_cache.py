#!/usr/bin/env python3
"""
CLI for managing the embedding cache.

Usage:
    python scripts/embedding_cache.py stats           # Show cache statistics
    python scripts/embedding_cache.py list            # List recent keys
    python scripts/embedding_cache.py list --type keywords --limit 20
    python scripts/embedding_cache.py clear --type keywords  # Clear keyword embeddings
    python scripts/embedding_cache.py clear --all     # Clear all embeddings
"""

import argparse
from pathlib import Path

from domain_status_graph.embeddings import SQLiteEmbeddingCache


def main():
    parser = argparse.ArgumentParser(description="Manage embedding cache")
    parser.add_argument(
        "--db",
        type=Path,
        default=Path("data/embeddings.db"),
        help="Path to cache database (default: data/embeddings.db)",
    )

    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    # stats command
    subparsers.add_parser("stats", help="Show cache statistics")

    # list command
    list_parser = subparsers.add_parser("list", help="List cache keys")
    list_parser.add_argument(
        "--type",
        choices=["description", "keywords"],
        help="Filter by embedding type",
    )
    list_parser.add_argument(
        "--limit",
        type=int,
        default=20,
        help="Maximum keys to show (default: 20)",
    )

    # clear command
    clear_parser = subparsers.add_parser("clear", help="Clear cache entries")
    clear_group = clear_parser.add_mutually_exclusive_group(required=True)
    clear_group.add_argument(
        "--type",
        choices=["description", "keywords"],
        help="Clear specific embedding type",
    )
    clear_group.add_argument(
        "--all",
        action="store_true",
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

    if not args.db.exists():
        print(f"Cache database not found: {args.db}")
        return

    cache = SQLiteEmbeddingCache(args.db)

    if args.command == "stats":
        stats = cache.stats()
        print(f"Embedding Cache: {args.db}")
        print(f"  Total embeddings: {stats['total']}")
        print(f"  Database size: {stats['db_size_mb']} MB")
        print(f"  Models: {stats['models']}")
        print(f"  Oldest: {stats['oldest']}")
        print(f"  Newest: {stats['newest']}")
        print("  By type:")
        for typ, count in stats["by_type"].items():
            print(f"    {typ}: {count}")

    elif args.command == "list":
        keys = cache.list_keys(embedding_type=args.type, limit=args.limit)
        if not keys:
            print("No embeddings found")
            return
        print("Recent embeddings ({} shown):".format(len(keys)))
        for key in keys:
            print(f"  {key}")

    elif args.command == "clear":
        if args.all:
            count = cache.count()
            if count == 0:
                print("Cache is already empty")
                return
            if not args.yes:
                confirm = input(f"Delete ALL {count} embeddings? [y/N] ")
                if confirm.lower() != "y":
                    print("Cancelled")
                    return
            deleted = cache.clear_all()
            print(f"Deleted {deleted} embeddings")
        else:
            count = cache.count_by_type().get(args.type, 0)
            if count == 0:
                print(f"No {args.type} embeddings found")
                return
            if not args.yes:
                confirm = input(f"Delete {count} {args.type} embeddings? [y/N] ")
                if confirm.lower() != "y":
                    print("Cancelled")
                    return
            deleted = cache.clear_by_type(args.type)
            print(f"Deleted {deleted} {args.type} embeddings")


if __name__ == "__main__":
    main()
