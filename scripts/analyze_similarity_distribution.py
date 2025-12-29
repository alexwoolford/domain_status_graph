#!/usr/bin/env python3
"""
Analyze the distribution of similarity scores across all company pairs.

This script computes cosine similarity for all pairs of companies with embeddings
and provides statistical analysis to help determine an appropriate threshold.

Usage:
    python scripts/analyze_similarity_distribution.py
"""

import sys
from pathlib import Path

import numpy as np

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from domain_status_graph.cli import get_driver_and_database, setup_logging


def compute_all_pairwise_similarities(driver, database, sample_size=None):
    """Compute cosine similarity for all company pairs."""
    logger = setup_logging("analyze_similarity")

    with driver.session(database=database) as session:
        # Load all companies with embeddings
        logger.info("Loading companies with embeddings...")
        result = session.run(
            """
            MATCH (c:Company)
            WHERE c.description_embedding IS NOT NULL
            RETURN c.cik AS cik, c.ticker AS ticker, c.name AS name,
                   c.description_embedding AS embedding
            ORDER BY c.cik
            """
        )

        companies = []
        for record in result:
            if record["embedding"]:
                companies.append(
                    {
                        "cik": record["cik"],
                        "ticker": record["ticker"],
                        "name": record["name"],
                        "embedding": np.array(record["embedding"], dtype=np.float32),
                    }
                )

        logger.info(f"Found {len(companies)} companies with embeddings")

        # Sample if requested
        if sample_size and sample_size < len(companies):
            import random

            companies = random.sample(companies, sample_size)
            logger.info(f"Sampling {sample_size} companies for analysis")

        # Compute pairwise similarities
        logger.info("Computing pairwise similarities...")
        similarities = []

        embeddings_matrix = np.array([c["embedding"] for c in companies])
        ciks = [c["cik"] for c in companies]
        tickers = [c["ticker"] for c in companies]
        names = [c["name"] for c in companies]

        # Normalize embeddings
        norms = np.linalg.norm(embeddings_matrix, axis=1, keepdims=True)
        norms[norms == 0] = 1
        embeddings_normalized = embeddings_matrix / norms

        # Compute similarity matrix
        similarity_matrix = np.dot(embeddings_normalized, embeddings_normalized.T)

        # Collect all pairs (excluding self-similarity)
        for i in range(len(companies)):
            for j in range(i + 1, len(companies)):
                similarity = float(similarity_matrix[i, j])
                similarities.append(
                    {
                        "cik1": ciks[i],
                        "ticker1": tickers[i],
                        "name1": names[i],
                        "cik2": ciks[j],
                        "ticker2": tickers[j],
                        "name2": names[j],
                        "similarity": similarity,
                    }
                )

        logger.info(f"Computed {len(similarities):,} pairwise similarities")
        return similarities


def analyze_distribution(similarities):
    """Analyze the distribution of similarity scores."""
    scores = [s["similarity"] for s in similarities]
    scores = np.array(scores)

    print("=" * 80)
    print("Similarity Score Distribution Analysis")
    print("=" * 80)
    print()

    # Basic statistics
    print("Basic Statistics:")
    print(f"  Total pairs: {len(scores):,}")
    print(f"  Min: {np.min(scores):.4f}")
    print(f"  Max: {np.max(scores):.4f}")
    print(f"  Mean: {np.mean(scores):.4f}")
    print(f"  Median: {np.median(scores):.4f}")
    print(f"  Std Dev: {np.std(scores):.4f}")
    print()

    # Percentiles
    print("Percentiles:")
    percentiles = [1, 5, 10, 25, 50, 75, 90, 95, 99, 99.5, 99.9]
    for p in percentiles:
        value = np.percentile(scores, p)
        count_above = np.sum(scores >= value)
        pct_above = (count_above / len(scores)) * 100
        print(f"  {p:5.1f}th percentile: {value:.4f} ({count_above:,} pairs, {pct_above:.2f}%)")
    print()

    # Threshold analysis
    print("Threshold Analysis:")
    print(
        f"{'Threshold':<12} {'Pairs Above':<15} {'% of Total':<12} {'Top-K (50)':<12} {'Top-K (100)':<12}"
    )
    print("-" * 80)

    thresholds = [0.50, 0.55, 0.60, 0.65, 0.68, 0.70, 0.75, 0.80, 0.85, 0.90, 0.95]
    for threshold in thresholds:
        count_above = np.sum(scores >= threshold)
        pct_above = (count_above / len(scores)) * 100

        # Estimate top-k relationships
        # For each company, count how many pairs above threshold
        # This is approximate - actual top-k would be per company
        top_k_50_estimate = min(count_above, len(scores) // (len(scores) ** 0.5) * 50)
        top_k_100_estimate = min(count_above, len(scores) // (len(scores) ** 0.5) * 100)

        print(
            f"  {threshold:>6.2f}      {count_above:>12,}    {pct_above:>8.2f}%    {top_k_50_estimate:>10,}    {top_k_100_estimate:>10,}"
        )
    print()

    # Histogram
    print("Histogram (bins of 0.05):")
    bins = np.arange(0.0, 1.05, 0.05)
    hist, bin_edges = np.histogram(scores, bins=bins)
    for i in range(len(hist)):
        bin_start = bin_edges[i]
        bin_end = bin_edges[i + 1]
        count = hist[i]
        pct = (count / len(scores)) * 100
        bar = "█" * int(pct / 2)  # Scale bar to fit
        print(f"  [{bin_start:>4.2f} - {bin_end:>4.2f}): {count:>8,} ({pct:>5.2f}%) {bar}")
    print()

    return scores


def find_top_pairs(similarities, top_n=50):
    """Find the top N most similar pairs."""
    sorted_pairs = sorted(similarities, key=lambda x: x["similarity"], reverse=True)

    print("=" * 80)
    print(f"Top {top_n} Most Similar Company Pairs")
    print("=" * 80)
    print(f"{'Rank':<6} {'Ticker1':<8} {'Name1':<35} {'Ticker2':<8} {'Name2':<35} {'Score':<8}")
    print("-" * 110)

    for i, pair in enumerate(sorted_pairs[:top_n], 1):
        name1 = (pair["name1"] or "")[:33]
        name2 = (pair["name2"] or "")[:33]
        print(
            f"{i:<6} {pair['ticker1']:<8} {name1:<35} {pair['ticker2']:<8} {name2:<35} {pair['similarity']:.4f}"
        )
    print()


def analyze_famous_pairs(similarities):
    """Check similarity scores for known competitor pairs."""
    famous_pairs = [
        ("PEP", "KO", "PepsiCo", "Coca-Cola"),
        ("HD", "LOW", "Home Depot", "Lowes"),
        ("WMT", "TGT", "Walmart", "Target"),
        ("AAPL", "MSFT", "Apple", "Microsoft"),
        ("V", "MA", "Visa", "Mastercard"),
        ("JPM", "BAC", "JPMorgan", "Bank of America"),
        ("NVDA", "AMD", "NVIDIA", "AMD"),
    ]

    print("=" * 80)
    print("Famous Competitor Pairs")
    print("=" * 80)
    print(f"{'Ticker1':<8} {'Ticker2':<8} {'Name1':<30} {'Name2':<30} {'Score':<8} {'Status':<15}")
    print("-" * 110)

    for ticker1, ticker2, name1, name2 in famous_pairs:
        # Find pair (bidirectional)
        pair = None
        for s in similarities:
            if (s["ticker1"] == ticker1 and s["ticker2"] == ticker2) or (
                s["ticker1"] == ticker2 and s["ticker2"] == ticker1
            ):
                pair = s
                break

        if pair:
            score = pair["similarity"]
            status = "✓ Included" if score >= 0.7 else "✗ Below 0.7"
            print(f"{ticker1:<8} {ticker2:<8} {name1:<30} {name2:<30} {score:.4f} {status:<15}")
        else:
            print(f"{ticker1:<8} {ticker2:<8} {name1:<30} {name2:<30} {'N/A':<8} {'Not found':<15}")
    print()


def main():
    """Run the similarity distribution analysis."""
    logger = setup_logging("analyze_similarity")

    driver, database = get_driver_and_database(logger)

    try:
        # Compute all pairwise similarities
        # For large datasets, we might want to sample
        # But let's try full first - it should be manageable
        similarities = compute_all_pairwise_similarities(driver, database)

        # Analyze distribution
        scores = analyze_distribution(similarities)

        # Show top pairs
        find_top_pairs(similarities, top_n=50)

        # Check famous pairs
        analyze_famous_pairs(similarities)

        # Recommendations
        print("=" * 80)
        print("Recommendations")
        print("=" * 80)

        # Find threshold that captures top 1% of pairs
        top_1_pct_threshold = np.percentile(scores, 99)
        top_5_pct_threshold = np.percentile(scores, 95)
        top_10_pct_threshold = np.percentile(scores, 90)

        print("\nThresholds by percentile:")
        print(f"  Top 1%:  {top_1_pct_threshold:.4f}")
        print(f"  Top 5%:  {top_5_pct_threshold:.4f}")
        print(f"  Top 10%: {top_10_pct_threshold:.4f}")

        # Check how many famous pairs would be included
        for threshold in [0.65, 0.68, 0.70, 0.75]:
            count = np.sum(scores >= threshold)
            pct = (count / len(scores)) * 100
            print(f"\nThreshold {threshold:.2f}:")
            print(f"  Pairs above: {count:,} ({pct:.2f}% of all pairs)")
            print(f"  Estimated relationships (top-50): ~{min(count, 50000):,}")

    finally:
        driver.close()


if __name__ == "__main__":
    main()
