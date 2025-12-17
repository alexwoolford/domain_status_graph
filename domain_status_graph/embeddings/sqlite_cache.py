"""
SQLite-based embedding cache.

Stores embeddings in a SQLite database for efficient caching and retrieval.
Replaces the JSON-based cache with a more scalable solution.

Schema:
    CREATE TABLE embeddings (
        key TEXT PRIMARY KEY,      -- "apple.com:keywords"
        text TEXT,                 -- Original text that was embedded
        text_hash TEXT,            -- SHA256 for invalidation
        model TEXT,                -- "text-embedding-3-small"
        dimension INTEGER,         -- 1536
        embedding BLOB,            -- numpy array as bytes
        created_at TEXT
    );
"""

import hashlib
import logging
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, List, Optional

import numpy as np

logger = logging.getLogger(__name__)


def compute_text_hash(text: str) -> str:
    """Compute SHA256 hash of text for change detection."""
    if not text:
        return ""
    normalized = text.strip()
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


class SQLiteEmbeddingCache:
    """
    SQLite-based embedding cache.

    Stores embeddings in a SQLite database with automatic invalidation
    when source text changes.
    """

    def __init__(self, db_path: Path):
        """
        Initialize SQLite embedding cache.

        Args:
            db_path: Path to SQLite database file
        """
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _init_db(self):
        """Initialize database schema."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS embeddings (
                    key TEXT PRIMARY KEY,
                    text TEXT,
                    text_hash TEXT,
                    model TEXT,
                    dimension INTEGER,
                    embedding BLOB,
                    created_at TEXT
                )
                """
            )
            conn.execute("CREATE INDEX IF NOT EXISTS idx_text_hash ON embeddings(text_hash)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_model ON embeddings(model)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_created_at ON embeddings(created_at)")
            conn.commit()

    def _serialize_embedding(self, embedding: List[float]) -> bytes:
        """Serialize embedding to bytes."""
        return np.array(embedding, dtype=np.float32).tobytes()

    def _deserialize_embedding(self, data: bytes) -> List[float]:
        """Deserialize embedding from bytes."""
        return np.frombuffer(data, dtype=np.float32).tolist()

    def get(
        self,
        key: str,
        text: str,
        model: str,
        check_text_hash: bool = True,
        expected_dimension: int = 1536,
    ) -> Optional[List[float]]:
        """
        Get embedding from cache.

        Args:
            key: Cache key (e.g., "apple.com:keywords")
            text: Original text (for hash validation)
            model: Expected model name
            check_text_hash: If True, invalidate if text changed
            expected_dimension: Expected embedding dimension (validates on read)

        Returns:
            Embedding vector or None if not cached/invalid
        """
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                "SELECT embedding, text_hash, model, dimension FROM embeddings WHERE key = ?",
                (key,),
            )
            row = cursor.fetchone()

        if not row:
            return None

        embedding_bytes, stored_hash, stored_model, stored_dimension = row

        # Check model matches
        if stored_model != model:
            logger.debug(f"Model mismatch for {key}: {stored_model} != {model}")
            return None

        # Check dimension matches
        if stored_dimension != expected_dimension:
            logger.debug(
                f"Dimension mismatch for {key}: {stored_dimension} != {expected_dimension}"
            )
            return None

        # Check text hash if requested
        if check_text_hash:
            current_hash = compute_text_hash(text)
            if stored_hash != current_hash:
                logger.debug(f"Text changed for {key}, invalidating cache")
                return None

        embedding = self._deserialize_embedding(embedding_bytes)

        # Validate deserialized dimension
        if len(embedding) != expected_dimension:
            logger.warning(
                f"Corrupted embedding for {key}: got {len(embedding)}, "
                f"expected {expected_dimension}"
            )
            return None

        return embedding

    def set(
        self,
        key: str,
        text: str,
        model: str,
        dimension: int,
        embedding: List[float],
    ) -> None:
        """
        Store embedding in cache.

        Args:
            key: Cache key
            text: Original text
            model: Model name
            dimension: Embedding dimension
            embedding: Embedding vector
        """
        text_hash = compute_text_hash(text)
        embedding_bytes = self._serialize_embedding(embedding)
        created_at = datetime.now(timezone.utc).isoformat()

        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO embeddings
                (key, text, text_hash, model, dimension, embedding, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (key, text, text_hash, model, dimension, embedding_bytes, created_at),
            )
            conn.commit()

    def get_or_create(
        self,
        key: str,
        text: str,
        model: str,
        dimension: int,
        create_fn: Callable[[str, str], Optional[List[float]]],
    ) -> Optional[List[float]]:
        """
        Get embedding from cache or create it.

        Args:
            key: Cache key
            text: Text to embed
            model: Model name
            dimension: Expected dimension
            create_fn: Function to create embedding: (text, model) -> embedding

        Returns:
            Embedding vector or None if creation failed
        """
        # Try cache first
        embedding = self.get(key, text, model, expected_dimension=dimension)
        if embedding is not None:
            return embedding

        # Create new embedding
        embedding = create_fn(text, model)
        if embedding is not None:
            self.set(key, text, model, dimension, embedding)

        return embedding

    def count(self) -> int:
        """Get total number of cached embeddings."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("SELECT COUNT(*) FROM embeddings")
            return cursor.fetchone()[0]

    def count_by_type(self) -> dict:
        """Get counts grouped by embedding type (from key suffix)."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                """
                SELECT
                    CASE
                        WHEN key LIKE '%:description' THEN 'description'
                        WHEN key LIKE '%:keywords' THEN 'keywords'
                        ELSE 'other'
                    END AS type,
                    COUNT(*) as count
                FROM embeddings
                GROUP BY type
                """
            )
            return {row[0]: row[1] for row in cursor.fetchall()}

    def delete(self, key: str) -> bool:
        """Delete a specific embedding."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("DELETE FROM embeddings WHERE key = ?", (key,))
            conn.commit()
            return cursor.rowcount > 0

    def clear_by_type(self, embedding_type: str) -> int:
        """
        Clear all embeddings of a specific type.

        Args:
            embedding_type: Type suffix (e.g., "description", "keywords")

        Returns:
            Number of embeddings deleted
        """
        pattern = f"%:{embedding_type}"
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("DELETE FROM embeddings WHERE key LIKE ?", (pattern,))
            conn.commit()
            return cursor.rowcount

    def clear_all(self) -> int:
        """Clear all embeddings from cache."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("DELETE FROM embeddings")
            conn.commit()
            return cursor.rowcount

    def stats(self) -> dict:
        """Get cache statistics."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                """
                SELECT
                    COUNT(*) as total,
                    COUNT(DISTINCT model) as models,
                    MIN(created_at) as oldest,
                    MAX(created_at) as newest
                FROM embeddings
                """
            )
            row = cursor.fetchone()

            # Get size of database file
            db_size_bytes = self.db_path.stat().st_size if self.db_path.exists() else 0

            return {
                "total": row[0],
                "models": row[1],
                "oldest": row[2],
                "newest": row[3],
                "by_type": self.count_by_type(),
                "db_size_mb": round(db_size_bytes / (1024 * 1024), 2),
            }

    def list_keys(self, embedding_type: Optional[str] = None, limit: int = 100) -> List[str]:
        """
        List cache keys.

        Args:
            embedding_type: Filter by type (e.g., "description", "keywords")
            limit: Maximum number of keys to return

        Returns:
            List of cache keys
        """
        with sqlite3.connect(self.db_path) as conn:
            if embedding_type:
                pattern = f"%:{embedding_type}"
                cursor = conn.execute(
                    "SELECT key FROM embeddings WHERE key LIKE ? ORDER BY created_at DESC LIMIT ?",
                    (pattern, limit),
                )
            else:
                cursor = conn.execute(
                    "SELECT key FROM embeddings ORDER BY created_at DESC LIMIT ?",
                    (limit,),
                )
            return [row[0] for row in cursor.fetchall()]
