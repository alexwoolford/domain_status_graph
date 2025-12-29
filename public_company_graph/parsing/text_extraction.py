"""
Unified text extraction utilities for SEC filing parsing.

Provides common functionality for extracting text between HTML elements.
"""

# Default block-level HTML tags used for text extraction
BLOCK_TAGS = {"p", "h1", "h2", "h3", "h4", "li", "div"}

# Extended block tags including table elements
BLOCK_TAGS_WITH_TABLES = {"p", "h1", "h2", "h3", "h4", "li", "div", "table", "tr", "td"}


def extract_text_between_elements(
    start_el,
    end_el,
    max_chars: int = 120000,
    include_start: bool = False,
    include_tables: bool = False,
    min_text_length: int = 0,
) -> str:
    """
    Extract text between two HTML elements.

    Args:
        start_el: Starting element
        end_el: Ending element (None = extract until end of document)
        max_chars: Maximum characters to extract
        include_start: Whether to include text from start_el itself
        include_tables: Whether to include table elements in block_tags
        min_text_length: Minimum length for text to be included (0 = include all)

    Returns:
        Extracted text with paragraphs joined by newlines
    """
    block_tags = BLOCK_TAGS_WITH_TABLES if include_tables else BLOCK_TAGS
    parts: list[str] = []
    seen: set[str] = set()
    total = 0

    # Optionally include start_el itself if it has text
    if include_start and start_el and getattr(start_el, "name", None) in block_tags:
        start_text = start_el.get_text(" ", strip=True)
        if start_text and len(start_text) > min_text_length and start_text not in seen:
            parts.append(start_text)
            seen.add(start_text)
            total += len(start_text)

    # Iterate through next elements
    for el in start_el.next_elements:
        if el == end_el:
            break
        if include_start and el == start_el:
            continue  # Skip start_el itself (already included if include_start)
        if getattr(el, "name", None) in block_tags:
            txt = el.get_text(" ", strip=True)
            if txt and txt not in seen and len(txt) > min_text_length:
                parts.append(txt)
                seen.add(txt)
                total += len(txt)
                if total >= max_chars:
                    break

    return "\n".join(parts)[:max_chars].strip()


# Backwards-compatible aliases
def extract_between_anchors(start_el, end_el, max_chars: int = 120000) -> str:
    """
    Extract text between two anchor elements.

    This is a backwards-compatible alias for extract_text_between_elements
    with default settings suitable for business description extraction.

    Args:
        start_el: Starting element (anchor with id)
        end_el: Ending element (anchor with id for next section)
        max_chars: Maximum characters to extract

    Returns:
        Extracted text
    """
    return extract_text_between_elements(
        start_el=start_el,
        end_el=end_el,
        max_chars=max_chars,
        include_start=False,
        include_tables=False,
        min_text_length=0,
    )
