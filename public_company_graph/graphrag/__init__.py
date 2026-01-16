"""
GraphRAG module for question-answering over company knowledge graph.

This module layers GraphRAG capabilities on top of the existing structured graph,
enabling natural language queries while preserving all existing relationships.

Architecture:
- Document nodes: Chunked text from 10-K filings (business descriptions, risk factors)
- Document → Company links: Connect chunks to source companies
- Vector search: Semantic search over document embeddings
- Graph traversal: Combine vector search with structured relationships
- Q&A: Natural language question answering
"""

from public_company_graph.graphrag.chunking import (
    chunk_filing_sections,
)
from public_company_graph.graphrag.documents import (
    create_chunk_embeddings,
    create_documents_and_chunks,
    link_documents_to_companies,
)
from public_company_graph.graphrag.filing_text import (
    extract_full_text_from_html,
    extract_full_text_with_datamule,
    find_10k_file_for_company,
)
from public_company_graph.graphrag.queries import (
    answer_question,
    search_documents,
    search_with_graph_context,
)

__all__ = [
    # Chunking
    "chunk_filing_sections",
    # Text extraction
    "extract_full_text_from_html",
    "extract_full_text_with_datamule",
    "find_10k_file_for_company",
    # Document management
    "create_documents_and_chunks",
    "create_chunk_embeddings",
    "link_documents_to_companies",
    # Querying
    "search_documents",
    "search_with_graph_context",
    "answer_question",
]
