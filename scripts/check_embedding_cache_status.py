#!/usr/bin/env python3
"""
Diagnostic script to check embedding cache status before running the embedding pipeline.

Shows:
- How many companies have embeddings cached
- How many need to be created
- Estimated time for completion
- Why re-runs might still take a long time
"""

import argparse

from public_company_graph.cache import get_cache
from public_company_graph.cli import get_driver_and_database, setup_logging
from public_company_graph.constants import MIN_DESCRIPTION_LENGTH_FOR_SIMILARITY
from public_company_graph.embeddings.openai_client import (
    EMBEDDING_TRUNCATE_TOKENS,
    count_tokens,
)

EXPECTED_MODEL = "text-embedding-3-small"
EXPECTED_DIM = 1536


def main():
    """Check embedding cache status."""
    parser = argparse.ArgumentParser(description="Check embedding cache status")
    parser.add_argument("--verbose", "-v", action="store_true", help="Show detailed output")
    args = parser.parse_args()

    logger = setup_logging("check_embedding_cache", execute=False)

    driver, database = get_driver_and_database(logger)
    cache = get_cache()

    print("=" * 70)
    print("EMBEDDING CACHE STATUS CHECK")
    print("=" * 70)
    print()

    # Get all companies eligible for embedding
    with driver.session(database=database) as session:
        result = session.run(
            """
            MATCH (c:Company)
            WHERE c.description IS NOT NULL
              AND c.description <> ''
              AND size(c.description) >= $min_length
            RETURN c.cik AS cik, c.description AS desc, c.name AS name
            ORDER BY c.cik
            """,
            min_length=MIN_DESCRIPTION_LENGTH_FOR_SIMILARITY,
        )

        companies = [(r["cik"], r["desc"], r["name"]) for r in result]

    print(f"Companies eligible for embedding: {len(companies)}")
    print(f"Min description length filter: {MIN_DESCRIPTION_LENGTH_FOR_SIMILARITY} chars")
    print()

    # Analyze cache status
    cached_good = []
    need_creation = []
    wrong_model = []
    wrong_dim = []

    for cik, desc, name in companies:
        cache_key = f"{cik}:description"
        cached = cache.get("embeddings", cache_key)

        if cached is None:
            tokens = count_tokens(desc, EXPECTED_MODEL)
            need_creation.append((cik, name, tokens, tokens > EMBEDDING_TRUNCATE_TOKENS))
        elif "embedding" not in cached:
            tokens = count_tokens(desc, EXPECTED_MODEL)
            need_creation.append((cik, name, tokens, tokens > EMBEDDING_TRUNCATE_TOKENS))
        elif cached.get("model") != EXPECTED_MODEL:
            tokens = count_tokens(desc, EXPECTED_MODEL)
            wrong_model.append((cik, cached.get("model")))
            need_creation.append((cik, name, tokens, tokens > EMBEDDING_TRUNCATE_TOKENS))
        elif len(cached.get("embedding", [])) != EXPECTED_DIM:
            tokens = count_tokens(desc, EXPECTED_MODEL)
            wrong_dim.append((cik, len(cached.get("embedding", []))))
            need_creation.append((cik, name, tokens, tokens > EMBEDDING_TRUNCATE_TOKENS))
        else:
            cached_good.append(cik)

    # Calculate hit rate
    hit_rate = 100 * len(cached_good) / len(companies) if companies else 0

    print("=" * 70)
    print("CACHE STATUS")
    print("=" * 70)
    print(f"✓ Will use cache (skip API): {len(cached_good):,}")
    print(f"✗ Need new embeddings:       {len(need_creation):,}")
    if wrong_model:
        print(f"  - Wrong model:             {len(wrong_model)}")
    if wrong_dim:
        print(f"  - Wrong dimension:         {len(wrong_dim)}")
    print()
    print(f"Cache hit rate: {hit_rate:.1f}%")
    print()

    if not need_creation:
        print("🎉 All embeddings are cached! Re-run will be very fast.")
        driver.close()
        cache.close()
        return

    # Analyze what needs to be created
    short_texts = [x for x in need_creation if not x[3]]  # x[3] is is_long
    long_texts = [x for x in need_creation if x[3]]

    print("=" * 70)
    print("WORK REMAINING")
    print("=" * 70)
    print(f"Short texts (can batch, fast):        {len(short_texts):,}")
    print(f"Long texts (need chunking, SLOW):     {len(long_texts):,}")
    print()

    # Time estimation WITH BATCHED CHUNKING
    # Short: ~20 per batch, ~1.5 sec per batch
    short_batches = (len(short_texts) + 19) // 20
    short_time_sec = short_batches * 1.5

    # Long texts: Now use BATCHED chunking (~30 chunks per API call)
    if long_texts:
        avg_tokens = sum(x[2] for x in long_texts) / len(long_texts)
        avg_chunks = max(1, avg_tokens / 7000)  # 7000 tokens per chunk
        total_chunks = int(len(long_texts) * avg_chunks)
    else:
        avg_chunks = 0
        total_chunks = 0

    chunks_per_batch = 40
    long_api_calls = (total_chunks + chunks_per_batch - 1) // chunks_per_batch
    long_time_sec = long_api_calls * 1.5  # ~1.5 sec per batch

    total_time_sec = short_time_sec + long_time_sec
    total_time_min = total_time_sec / 60

    print("=" * 70)
    print("TIME ESTIMATE (with batched chunking)")
    print("=" * 70)
    print(
        f"Short texts: {len(short_texts)} texts → {short_batches} API calls → ~{short_time_sec:.0f} sec"
    )
    print(
        f"Long texts:  {len(long_texts)} texts × ~{avg_chunks:.1f} chunks = {total_chunks} chunks"
    )
    print(
        f"             Batched: {total_chunks} ÷ {chunks_per_batch} = ~{long_api_calls} API calls → ~{long_time_sec:.0f} sec"
    )
    print()
    print(f"⏱️  ESTIMATED TOTAL TIME: ~{total_time_min:.0f} minutes")
    print()

    if args.verbose and need_creation:
        print("=" * 70)
        print("SAMPLE OF COMPANIES NEEDING EMBEDDINGS")
        print("=" * 70)
        for cik, _name, tokens, is_long in need_creation[:10]:
            long_marker = " [LONG]" if is_long else ""
            print(f"  {cik}: {tokens:,} tokens{long_marker}")

    driver.close()
    cache.close()


if __name__ == "__main__":
    main()
