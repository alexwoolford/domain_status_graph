"""Embedding utilities for creating and caching embeddings."""

from domain_status_graph.constants import EMBEDDING_DIMENSION, EMBEDDING_MODEL
from domain_status_graph.embeddings.create import create_embeddings_for_nodes
from domain_status_graph.embeddings.openai_client import (
    create_embedding,
    get_openai_client,
    suppress_http_logging,
)

__all__ = [
    "create_embeddings_for_nodes",
    "create_embedding",
    "get_openai_client",
    "suppress_http_logging",
    "EMBEDDING_MODEL",
    "EMBEDDING_DIMENSION",
]
