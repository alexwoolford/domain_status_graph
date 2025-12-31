"""
Candidate Extraction Module.

Extracts potential company name candidates from text.
Each extraction strategy is isolated and testable.
"""

from __future__ import annotations

import re
from abc import ABC, abstractmethod
from dataclasses import dataclass


@dataclass(frozen=True)
class Candidate:
    """A potential company mention extracted from text."""

    text: str  # The extracted text
    start_pos: int  # Start position in source text
    end_pos: int  # End position in source text
    source_pattern: str  # Which pattern extracted it ("capitalized", "ticker", etc.)
    sentence: str  # The containing sentence (for context)


class CandidateExtractor(ABC):
    """Abstract base class for candidate extraction strategies."""

    @abstractmethod
    def extract(self, text: str) -> list[Candidate]:
        """Extract candidates from text."""
        ...

    @property
    @abstractmethod
    def pattern_name(self) -> str:
        """Name of this extraction pattern for debugging."""
        ...


class CapitalizedWordExtractor(CandidateExtractor):
    """
    Extracts capitalized multi-word sequences (1-4 words).

    Examples:
    - "Microsoft Corporation"
    - "International Business Machines"
    - "NVIDIA"
    """

    PATTERN = re.compile(r"\b([A-Z][a-zA-Z&.\-]*(?:\s+[A-Z][a-zA-Z&.\-]*){0,3})\b")

    @property
    def pattern_name(self) -> str:
        return "capitalized"

    def extract(self, text: str) -> list[Candidate]:
        """Extract capitalized word sequences."""
        candidates = []
        for match in self.PATTERN.finditer(text):
            candidate_text = match.group(1).strip()
            if len(candidate_text) >= 2:  # Minimum length
                candidates.append(
                    Candidate(
                        text=candidate_text,
                        start_pos=match.start(),
                        end_pos=match.end(),
                        source_pattern=self.pattern_name,
                        sentence=_get_containing_sentence(text, match.start()),
                    )
                )
        return candidates


class TickerExtractor(CandidateExtractor):
    """
    Extracts all-caps sequences that look like stock tickers.

    Examples:
    - "AAPL"
    - "MSFT"
    - "NVDA"
    """

    PATTERN = re.compile(r"\b([A-Z]{2,5})\b")

    @property
    def pattern_name(self) -> str:
        return "ticker"

    def extract(self, text: str) -> list[Candidate]:
        """Extract ticker-like sequences."""
        candidates = []
        for match in self.PATTERN.finditer(text):
            candidate_text = match.group(1)
            candidates.append(
                Candidate(
                    text=candidate_text,
                    start_pos=match.start(),
                    end_pos=match.end(),
                    source_pattern=self.pattern_name,
                    sentence=_get_containing_sentence(text, match.start()),
                )
            )
        return candidates


class QuotedNameExtractor(CandidateExtractor):
    """
    Extracts company names in quotes.

    Examples:
    - '"Apple Inc."'
    - '"the Company"' (would be filtered later)
    """

    PATTERN = re.compile(r'"([^"]{2,50})"')

    @property
    def pattern_name(self) -> str:
        return "quoted"

    def extract(self, text: str) -> list[Candidate]:
        """Extract quoted strings that might be company names."""
        candidates = []
        for match in self.PATTERN.finditer(text):
            candidate_text = match.group(1).strip()
            # Only extract if it has at least one capital letter
            if candidate_text and any(c.isupper() for c in candidate_text):
                candidates.append(
                    Candidate(
                        text=candidate_text,
                        start_pos=match.start(),
                        end_pos=match.end(),
                        source_pattern=self.pattern_name,
                        sentence=_get_containing_sentence(text, match.start()),
                    )
                )
        return candidates


def _get_containing_sentence(text: str, position: int) -> str:
    """Get the sentence containing the given position."""
    # Find sentence boundaries
    sentence_pattern = re.compile(r"[.!?]\s+")

    # Find start of sentence
    start = 0
    for match in sentence_pattern.finditer(text[:position]):
        start = match.end()

    # Find end of sentence
    end = len(text)
    sentence_end = sentence_pattern.search(text[position:])
    if sentence_end:
        end = position + sentence_end.end()

    return text[start:end].strip()[:500]  # Limit context length


def extract_candidates(
    text: str,
    extractors: list[CandidateExtractor] | None = None,
) -> list[Candidate]:
    """
    Extract all candidate company mentions from text.

    Args:
        text: Source text to extract from
        extractors: List of extractors to use (default: all standard extractors)

    Returns:
        List of unique candidates (deduplicated by text)
    """
    if extractors is None:
        extractors = [
            CapitalizedWordExtractor(),
            TickerExtractor(),
        ]

    all_candidates: dict[str, Candidate] = {}

    for extractor in extractors:
        for candidate in extractor.extract(text):
            # Keep the first occurrence of each unique text
            key = candidate.text.lower()
            if key not in all_candidates:
                all_candidates[key] = candidate

    return list(all_candidates.values())


# =============================================================================
# Testable Utilities
# =============================================================================


def extract_candidates_with_stats(
    text: str,
    extractors: list[CandidateExtractor] | None = None,
) -> tuple[list[Candidate], dict[str, int]]:
    """
    Extract candidates and return statistics for testing/debugging.

    Returns:
        Tuple of (candidates, stats_dict)
        stats_dict has pattern_name → count mapping
    """
    if extractors is None:
        extractors = [
            CapitalizedWordExtractor(),
            TickerExtractor(),
        ]

    stats: dict[str, int] = {}
    all_candidates: dict[str, Candidate] = {}

    for extractor in extractors:
        pattern_name = extractor.pattern_name
        stats[pattern_name] = 0

        for candidate in extractor.extract(text):
            stats[pattern_name] += 1
            key = candidate.text.lower()
            if key not in all_candidates:
                all_candidates[key] = candidate

    return list(all_candidates.values()), stats
