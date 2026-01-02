#!/usr/bin/env python3
"""
Prototype: Test if embedding similarity can distinguish correct vs incorrect matches.

This tests the hypothesis from P58 (Zeakis 2023) that pre-trained embeddings
can be used for entity resolution without fine-tuning.

Approach:
1. Embed the context sentence from 10-K
2. Embed the target company name (proxy for company description)
3. Calculate cosine similarity
4. Compare distributions between correct and incorrect matches
"""

import csv
from pathlib import Path

import numpy as np
from dotenv import load_dotenv
from openai import OpenAI

# Load environment
load_dotenv(Path(__file__).parent.parent / ".env")

client = OpenAI()


def get_embedding(text: str) -> list[float]:
    """Get embedding from OpenAI."""
    response = client.embeddings.create(
        model="text-embedding-3-small",
        input=text[:8000],
    )
    return response.data[0].embedding


def cosine_similarity(a: list[float], b: list[float]) -> float:
    """Calculate cosine similarity between two vectors."""
    a_arr = np.array(a)
    b_arr = np.array(b)
    return float(np.dot(a_arr, b_arr) / (np.linalg.norm(a_arr) * np.linalg.norm(b_arr)))


def get_company_description(ticker: str, name: str) -> str:
    """Get a company description using GPT (simulating what we'd store in Neo4j)."""
    # In production, this would come from Yahoo Finance or Neo4j
    # For prototype, generate a brief description
    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": "Generate a one-sentence business description."},
            {"role": "user", "content": f"Describe what {name} ({ticker}) does in one sentence."},
        ],
        max_tokens=100,
    )
    return response.choices[0].message.content


# Cache for company descriptions
_description_cache: dict[str, str] = {}


def get_cached_description(ticker: str, name: str) -> str:
    """Get cached company description."""
    if ticker not in _description_cache:
        _description_cache[ticker] = get_company_description(ticker, name)
    return _description_cache[ticker]


def main():
    # Load ground truth
    data_file = Path(__file__).parent.parent / "data" / "er_ai_audit.csv"
    with open(data_file) as f:
        records = list(csv.DictReader(f))

    # Sample: correct and incorrect
    correct = [r for r in records if r.get("ai_label") == "correct"][:15]
    incorrect = [r for r in records if r.get("ai_label") == "incorrect"][:15]

    print("Testing embedding similarity approach...")
    print("Comparing: context_embedding vs company_DESCRIPTION_embedding")
    print("=" * 60)

    correct_similarities = []
    incorrect_similarities = []

    for label, samples in [("CORRECT", correct), ("INCORRECT", incorrect)]:
        print(f"\n{label} matches:")
        for r in samples:
            context = r.get("context", "")[:500]
            target_ticker = r.get("target_ticker", "")
            target_name = r.get("target_name", "")

            if not context or not target_name:
                continue

            try:
                # Get company description (what we'd store in Neo4j)
                description = get_cached_description(target_ticker, target_name)

                context_emb = get_embedding(context)
                desc_emb = get_embedding(description)
                sim = cosine_similarity(context_emb, desc_emb)

                if label == "CORRECT":
                    correct_similarities.append(sim)
                else:
                    incorrect_similarities.append(sim)

                mention = r["raw_mention"][:25]
                print(
                    f'  {r["source_ticker"]:>5}→{r["target_ticker"]:<5}: sim={sim:.3f}  "{mention}"'
                )
            except Exception as e:
                print(f"  Error: {e}")

    print("\n" + "=" * 60)
    print("RESULTS")
    print("=" * 60)

    if correct_similarities and incorrect_similarities:
        correct_mean = np.mean(correct_similarities)
        incorrect_mean = np.mean(incorrect_similarities)

        print(f"Correct matches - mean similarity:   {correct_mean:.3f}")
        print(f"Incorrect matches - mean similarity: {incorrect_mean:.3f}")
        print(f"Difference: {correct_mean - incorrect_mean:+.3f}")
        print(
            f"\nCorrect range:   [{min(correct_similarities):.3f}, {max(correct_similarities):.3f}]"
        )
        print(
            f"Incorrect range: [{min(incorrect_similarities):.3f}, {max(incorrect_similarities):.3f}]"
        )

        # Test threshold effectiveness
        for threshold in [0.2, 0.25, 0.3, 0.35]:
            fn = sum(1 for s in correct_similarities if s < threshold)
            fp = sum(1 for s in incorrect_similarities if s >= threshold)
            print(
                f"\nThreshold {threshold}: FN={fn}/{len(correct_similarities)}, FP={fp}/{len(incorrect_similarities)}"
            )


if __name__ == "__main__":
    main()
