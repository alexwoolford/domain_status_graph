"""
Similarity computation utilities.

Provides shared functions for computing cosine similarity on embeddings.
"""

from domain_status_graph.similarity.cosine import (
    compute_cosine_similarity_matrix,
    compute_similarity_for_node_type,
    find_top_k_similar_pairs,
    validate_embedding,
    validate_similarity_score,
    write_similarity_relationships,
)

__all__ = [
    "compute_cosine_similarity_matrix",
    "compute_similarity_for_node_type",
    "find_top_k_similar_pairs",
    "validate_embedding",
    "validate_similarity_score",
    "write_similarity_relationships",
]
