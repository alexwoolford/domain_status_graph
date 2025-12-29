"""Embedding utilities for creating and caching embeddings."""

from public_company_graph.constants import EMBEDDING_DIMENSION, EMBEDDING_MODEL
from public_company_graph.embeddings.create import create_embeddings_for_nodes
from public_company_graph.embeddings.openai_client import (
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
