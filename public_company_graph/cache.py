"""
Unified caching layer using diskcache.

Provides a consistent interface for caching any data with optional TTL.
Uses namespaced keys to separate different types of cached data.
"""

import logging
from pathlib import Path
from typing import Any, Optional

import diskcache

logger = logging.getLogger(__name__)

DEFAULT_CACHE_DIR = Path("data/cache")
_cache: Optional["AppCache"] = None


# 45GB cache size limit - large enough for 2.8M+ chunk embeddings + parsed 10-K data
# Each chunk embedding: ~13.5 KB (12.3 KB vector + 1 KB text + metadata)
# 2.8M embeddings = ~38 GB, plus other cached data (10-K extracts, etc.)
# 45 GB provides comfortable headroom for all embeddings + other cached data
# Default diskcache limit is 1GB which causes eviction during embedding creation
DEFAULT_CACHE_SIZE_LIMIT = 45 * 1024 * 1024 * 1024  # 45 GB


class AppCache:
    """Unified cache using diskcache (SQLite-backed)."""

    def __init__(
        self,
        cache_dir: Path = DEFAULT_CACHE_DIR,
        timeout: float = 30.0,
        size_limit: int = DEFAULT_CACHE_SIZE_LIMIT,
    ):
        """
        Initialize cache.

        Args:
            cache_dir: Directory for cache files
            timeout: Timeout in seconds for acquiring database lock (default: 30.0)
                     Higher timeout needed for high concurrency (many workers)
            size_limit: Maximum cache size in bytes (default: 10GB)
                        Set to 0 for unlimited. When limit is reached, oldest entries
                        are evicted using least-recently-stored policy.
        """
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        # Use timeout to handle database locks gracefully
        # Higher timeout for high concurrency scenarios (SQLite serializes writes)
        # Size limit prevents disk exhaustion while allowing room for all cached data
        self._cache = diskcache.Cache(
            str(self.cache_dir),
            timeout=timeout,
            size_limit=size_limit,
        )

    def _make_key(self, namespace: str, key: str) -> str:
        return f"{namespace}:{key}"

    def get(self, namespace: str, key: str) -> Any | None:
        """Get a value from cache."""
        full_key = self._make_key(namespace, key)
        return self._cache.get(full_key)

    def get_many(self, namespace: str, keys: list[str]) -> dict[str, Any]:
        """
        Get multiple values from cache efficiently.

        This is much faster than individual get() calls. For reads, we don't need
        a transaction (transactions are for writes). Just batch the gets directly.

        Args:
            namespace: Cache namespace
            keys: List of cache keys to retrieve

        Returns:
            Dict mapping cache keys to their values (only includes keys that exist)
        """
        if not keys:
            return {}

        # Build full keys
        full_keys = [self._make_key(namespace, key) for key in keys]

        # Batch gets directly (no transaction needed for reads)
        # diskcache.get() is already optimized for reads
        results = {}
        namespace_prefix = f"{namespace}:"

        # Direct batch gets - much faster than transaction wrapper
        for full_key in full_keys:
            value = self._cache.get(full_key)
            if value is not None:
                # Strip namespace prefix to get original key
                original_key = full_key[len(namespace_prefix) :]
                results[original_key] = value

        return results

    def set(
        self,
        namespace: str,
        key: str,
        value: Any,
        ttl_days: int | None = None,
    ) -> None:
        """Set a value in cache with optional TTL."""
        full_key = self._make_key(namespace, key)
        expire = ttl_days * 86400 if ttl_days else None
        self._cache.set(full_key, value, expire=expire)

    def delete(self, namespace: str, key: str) -> bool:
        """Delete a value from cache."""
        full_key = self._make_key(namespace, key)
        return bool(self._cache.delete(full_key))

    def clear_namespace(self, namespace: str) -> int:
        """Clear all keys in a namespace."""
        prefix = f"{namespace}:"
        # Use iterator directly instead of loading all keys into memory
        keys_to_delete = [key for key in self._cache if key.startswith(prefix)]
        count = len(keys_to_delete)
        for key in keys_to_delete:
            self._cache.delete(key)
        return count

    def count(self, namespace: str | None = None) -> int:
        """Count entries, optionally filtered by namespace."""
        if namespace is None:
            return len(self._cache)
        # Use iterator directly (more memory efficient than loading all keys)
        prefix = f"{namespace}:"
        return sum(1 for key in self._cache if key.startswith(prefix))

    def stats(self) -> dict:
        """Get cache statistics."""
        namespaces: dict[str, int] = {}
        # Use iterator directly (more memory efficient)
        for key in self._cache:
            ns = key.split(":")[0] if ":" in key else "unknown"
            namespaces[ns] = namespaces.get(ns, 0) + 1

        # Get actual disk usage from diskcache
        volume_bytes = self._cache.volume()
        size_limit = self._cache.size_limit

        return {
            "total": len(self._cache),
            "by_namespace": namespaces,
            "size_mb": round(volume_bytes / (1024 * 1024), 2),
            "size_limit_mb": round(size_limit / (1024 * 1024), 2) if size_limit else None,
            "size_pct": round(volume_bytes / size_limit * 100, 1) if size_limit else None,
            "cache_dir": str(self.cache_dir),
        }

    def keys(self, namespace: str | None = None, limit: int = 100) -> list[str]:
        """Get keys, optionally filtered by namespace."""
        prefix = f"{namespace}:" if namespace else ""
        keys = []
        for key in self._cache:
            if key.startswith(prefix):
                keys.append(key[len(prefix) :] if prefix else key)
                if len(keys) >= limit:
                    break
        return keys

    def close(self):
        """Close the cache."""
        self._cache.close()


def get_cache(cache_dir: Path = DEFAULT_CACHE_DIR, timeout: float = 30.0) -> AppCache:
    """
    Get or create the global cache instance.

    Args:
        cache_dir: Directory for cache files
        timeout: Timeout in seconds for acquiring database lock (default: 30.0)
                 Higher timeout needed for high concurrency (many workers)
    """
    global _cache
    if _cache is None:
        _cache = AppCache(cache_dir, timeout=timeout)
    return _cache
