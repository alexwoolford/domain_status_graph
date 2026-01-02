"""
Embedding-based Entity Similarity Scorer.

Uses pre-trained embeddings (OpenAI text-embedding-3-small) to validate
that a mention context semantically matches the candidate company.

Based on P58 (Zeakis 2023): Pre-trained embeddings can effectively
disambiguate entities without fine-tuning.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING

import numpy as np

if TYPE_CHECKING:
    from openai import OpenAI

logger = logging.getLogger(__name__)


@dataclass
class EmbeddingSimilarityResult:
    """Result of embedding-based similarity check."""

    similarity: float  # Cosine similarity 0-1
    context_snippet: str  # What was embedded
    company_description: str  # What it was compared to
    passed: bool  # Above threshold?
    threshold: float


class EmbeddingSimilarityScorer:
    """
    Scores entity matches using embedding similarity.

    Compares the semantic meaning of the mention context to the
    company's business description. Low similarity suggests the
    mention refers to something else (e.g., "Target" as a noun,
    not Target Corp).
    """

    DEFAULT_THRESHOLD = 0.30
    EMBEDDING_MODEL = "text-embedding-3-small"

    def __init__(
        self,
        client: OpenAI | None = None,
        threshold: float = DEFAULT_THRESHOLD,
    ):
        """
        Initialize the scorer.

        Args:
            client: OpenAI client (created if not provided)
            threshold: Minimum similarity to pass (default 0.30)
        """
        self.threshold = threshold

        if client is None:
            from openai import OpenAI

            self._client = OpenAI()
        else:
            self._client = client

        # Cache for company descriptions (in production, these would come from Neo4j)
        self._description_cache: dict[str, str] = {}

    def _get_embedding(self, text: str) -> list[float]:
        """Get embedding vector for text."""
        response = self._client.embeddings.create(
            model=self.EMBEDDING_MODEL,
            input=text[:8000],  # Truncate to fit context window
        )
        return response.data[0].embedding

    @staticmethod
    def _cosine_similarity(a: list[float], b: list[float]) -> float:
        """Calculate cosine similarity between two vectors."""
        a_arr = np.array(a)
        b_arr = np.array(b)
        return float(np.dot(a_arr, b_arr) / (np.linalg.norm(a_arr) * np.linalg.norm(b_arr)))

    def get_company_description(self, ticker: str, name: str) -> str:
        """
        Get company description for embedding comparison.

        In production, this would query Neo4j for pre-stored descriptions.
        For now, generates a brief description using GPT.
        """
        cache_key = ticker

        if cache_key not in self._description_cache:
            # In production: query Neo4j for c.description
            # For prototype: generate using GPT
            try:
                response = self._client.chat.completions.create(
                    model="gpt-4o-mini",
                    messages=[
                        {
                            "role": "system",
                            "content": "Generate a one-sentence business description.",
                        },
                        {
                            "role": "user",
                            "content": f"Describe what {name} ({ticker}) does in one sentence.",
                        },
                    ],
                    max_tokens=100,
                )
                self._description_cache[cache_key] = response.choices[0].message.content or ""
            except Exception as e:
                logger.warning(f"Failed to get description for {ticker}: {e}")
                self._description_cache[cache_key] = name  # Fallback to name

        return self._description_cache[cache_key]

    def score(
        self,
        context: str,
        ticker: str,
        company_name: str,
        description: str | None = None,
    ) -> EmbeddingSimilarityResult:
        """
        Score how well the context matches the candidate company.

        Args:
            context: The sentence/snippet where the company was mentioned
            ticker: Candidate company ticker
            company_name: Candidate company name
            description: Company description (fetched if not provided)

        Returns:
            EmbeddingSimilarityResult with similarity score and pass/fail
        """
        # Get company description if not provided
        if description is None:
            description = self.get_company_description(ticker, company_name)

        # Get embeddings
        context_emb = self._get_embedding(context[:500])
        desc_emb = self._get_embedding(description)

        # Calculate similarity
        similarity = self._cosine_similarity(context_emb, desc_emb)

        return EmbeddingSimilarityResult(
            similarity=similarity,
            context_snippet=context[:200],
            company_description=description[:200],
            passed=similarity >= self.threshold,
            threshold=self.threshold,
        )


# Convenience function
def score_embedding_similarity(
    context: str,
    ticker: str,
    company_name: str,
    description: str | None = None,
    threshold: float = EmbeddingSimilarityScorer.DEFAULT_THRESHOLD,
) -> EmbeddingSimilarityResult:
    """
    Convenience function to score embedding similarity.

    Creates a scorer instance and scores. For batch operations,
    create an EmbeddingSimilarityScorer instance directly.
    """
    scorer = EmbeddingSimilarityScorer(threshold=threshold)
    return scorer.score(context, ticker, company_name, description)
