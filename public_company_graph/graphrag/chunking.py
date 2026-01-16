"""
Text chunking for GraphRAG document creation.

Chunks company text (business descriptions, risk factors) into documents
suitable for vector search and retrieval.
"""

import logging
from dataclasses import dataclass
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class DocumentChunk:
    """A chunk of text from a company filing."""

    text: str
    chunk_index: int
    section_type: str  # "business_description", "risk_factors"
    company_cik: str
    company_ticker: str | None = None
    company_name: str | None = None
    filing_year: int | None = None
    metadata: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for storage."""
        return {
            "text": self.text,
            "chunk_index": self.chunk_index,
            "section_type": self.section_type,
            "company_cik": self.company_cik,
            "company_ticker": self.company_ticker,
            "company_name": self.company_name,
            "filing_year": self.filing_year,
            "metadata": self.metadata or {},
        }


def chunk_text(
    text: str,
    chunk_size: int = 1000,
    chunk_overlap: int = 200,
    min_chunk_size: int = 100,
) -> list[str]:
    """
    Split text into chunks with overlap.

    DETERMINISTIC: Chunk boundaries are deterministic based on:
    - Fixed chunk_size and chunk_overlap parameters
    - Character positions (not sentence boundaries, which could vary)
    - This ensures re-runs produce identical chunks (important for caching)

    Args:
        text: Text to chunk
        chunk_size: Target chunk size in characters
        chunk_overlap: Overlap between chunks in characters
        min_chunk_size: Minimum chunk size (smaller chunks are merged)

    Returns:
        List of text chunks (deterministic for same input)
    """
    if not text or len(text) < min_chunk_size:
        return [text] if text else []

    chunks = []
    start = 0
    text_length = len(text)
    last_start = -1  # Track to detect infinite loops

    while start < text_length:
        # Safety check: ensure we make progress
        if start == last_start:
            logger.warning(f"Chunking stalled at position {start}, breaking")
            break
        last_start = start

        # Calculate end position (deterministic: fixed chunk_size)
        end = min(start + chunk_size, text_length)

        # NOTE: We don't break at sentence boundaries to ensure determinism
        # Sentence boundary detection could vary slightly between runs
        # Fixed character positions ensure identical chunks

        chunk = text[start:end].strip()
        if len(chunk) >= min_chunk_size:
            chunks.append(chunk)

        # Move start position with overlap (deterministic)
        # Ensure we always make progress
        new_start = end - chunk_overlap
        if new_start <= start:
            # Would not make progress, advance by at least chunk_size - overlap
            new_start = start + (chunk_size - chunk_overlap)

        start = min(new_start, text_length)

        # Final check
        if start >= text_length:
            break

    return chunks


def chunk_company_text(
    business_description: str | None,
    risk_factors: str | None,
    company_cik: str,
    company_ticker: str | None = None,
    company_name: str | None = None,
    filing_year: int | None = None,
    chunk_size: int = 1000,
    chunk_overlap: int = 200,
) -> list[DocumentChunk]:
    """
    Chunk company text into documents for GraphRAG.

    Args:
        business_description: Business description text (Item 1)
        risk_factors: Risk factors text (Item 1A)
        company_cik: Company CIK
        company_ticker: Company ticker symbol
        company_name: Company name
        filing_year: Filing year
        chunk_size: Target chunk size in characters
        chunk_overlap: Overlap between chunks

    Returns:
        List of DocumentChunk objects
    """
    chunks = []

    # Chunk business description
    if business_description:
        text_chunks = chunk_text(business_description, chunk_size, chunk_overlap)
        for i, chunk_content in enumerate(text_chunks):
            chunks.append(
                DocumentChunk(
                    text=chunk_content,
                    chunk_index=i,
                    section_type="business_description",
                    company_cik=company_cik,
                    company_ticker=company_ticker,
                    company_name=company_name,
                    filing_year=filing_year,
                    metadata={"section": "Item 1: Business"},
                )
            )

    # Chunk risk factors
    if risk_factors:
        text_chunks = chunk_text(risk_factors, chunk_size, chunk_overlap)
        for i, chunk_content in enumerate(text_chunks):
            chunks.append(
                DocumentChunk(
                    text=chunk_content,
                    chunk_index=i,
                    section_type="risk_factors",
                    company_cik=company_cik,
                    company_ticker=company_ticker,
                    company_name=company_name,
                    filing_year=filing_year,
                    metadata={"section": "Item 1A: Risk Factors"},
                )
            )

    return chunks


def chunk_filing_sections(
    sections: dict[str, str | None],
    company_cik: str,
    company_ticker: str | None = None,
    company_name: str | None = None,
    filing_year: int | None = None,
    chunk_size: int = 1000,
    chunk_overlap: int = 200,
) -> list[DocumentChunk]:
    """
    Chunk multiple filing sections into documents.

    Args:
        sections: Dictionary mapping section names to text
        company_cik: Company CIK
        company_ticker: Company ticker symbol
        company_name: Company name
        filing_year: Filing year
        chunk_size: Target chunk size in characters
        chunk_overlap: Overlap between chunks

    Returns:
        List of DocumentChunk objects
    """
    chunks = []

    for section_name, section_text in sections.items():
        if not section_text:
            continue

        text_chunks = chunk_text(section_text, chunk_size, chunk_overlap)
        for i, chunk_content in enumerate(text_chunks):
            chunks.append(
                DocumentChunk(
                    text=chunk_content,
                    chunk_index=i,
                    section_type=section_name,
                    company_cik=company_cik,
                    company_ticker=company_ticker,
                    company_name=company_name,
                    filing_year=filing_year,
                    metadata={"section": section_name},
                )
            )

    return chunks
