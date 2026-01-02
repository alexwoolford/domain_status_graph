#!/usr/bin/env python3
"""
Evaluate the LayeredEntityValidator against ground truth.

Compares three approaches:
1. No validation (baseline)
2. Pattern-only (filters + relationship verifier)
3. Layered (embeddings + filters + relationship verifier)
"""

import csv
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / ".env")

from public_company_graph.entity_resolution.layered_validator import (
    LayeredEntityValidator,
)


def evaluate_pattern_only(records: list[dict]) -> dict:
    """Evaluate using only pattern-based filters (no embeddings)."""
    validator = LayeredEntityValidator(skip_embedding=True)

    correct = [r for r in records if r.get("ai_label") == "correct"]
    incorrect = [r for r in records if r.get("ai_label") == "incorrect"]

    # Test on correct (should accept)
    correct_accepted = 0
    correct_rejected = 0
    correct_rejections = []

    for r in correct:
        result = validator.validate(
            context=r.get("context", ""),
            mention=r.get("raw_mention", ""),
            ticker=r.get("target_ticker", ""),
            company_name=r.get("target_name", ""),
            relationship_type=r.get("relationship_type", ""),
        )
        if result.accepted:
            correct_accepted += 1
        else:
            correct_rejected += 1
            correct_rejections.append((r, result))

    # Test on incorrect (should reject)
    incorrect_rejected = 0
    incorrect_accepted = 0
    rejection_reasons = {}

    for r in incorrect:
        result = validator.validate(
            context=r.get("context", ""),
            mention=r.get("raw_mention", ""),
            ticker=r.get("target_ticker", ""),
            company_name=r.get("target_name", ""),
            relationship_type=r.get("relationship_type", ""),
        )
        if not result.accepted:
            incorrect_rejected += 1
            reason = result.rejection_reason.value
            rejection_reasons[reason] = rejection_reasons.get(reason, 0) + 1
        else:
            incorrect_accepted += 1

    return {
        "approach": "Pattern-only",
        "correct_accepted": correct_accepted,
        "correct_rejected": correct_rejected,
        "incorrect_rejected": incorrect_rejected,
        "incorrect_accepted": incorrect_accepted,
        "rejection_reasons": rejection_reasons,
        "precision": correct_accepted / (correct_accepted + incorrect_accepted)
        if (correct_accepted + incorrect_accepted) > 0
        else 0,
        "false_positives": correct_rejected,
        "false_negatives": incorrect_accepted,
    }


def evaluate_layered(records: list[dict], sample_size: int = 30) -> dict:
    """Evaluate using full layered approach (with embeddings)."""
    validator = LayeredEntityValidator(
        embedding_threshold=0.30,
        skip_embedding=False,
    )

    # Sample to reduce API costs
    correct = [r for r in records if r.get("ai_label") == "correct"][:sample_size]
    incorrect = [r for r in records if r.get("ai_label") == "incorrect"][:sample_size]

    print(f"\nTesting {len(correct)} correct + {len(incorrect)} incorrect samples...")

    # Test on correct (should accept)
    correct_accepted = 0
    correct_rejected = 0
    correct_rejections = []

    for i, r in enumerate(correct):
        result = validator.validate(
            context=r.get("context", ""),
            mention=r.get("raw_mention", ""),
            ticker=r.get("target_ticker", ""),
            company_name=r.get("target_name", ""),
            relationship_type=r.get("relationship_type", ""),
        )
        status = "✓" if result.accepted else "✗"
        sim = f"sim={result.embedding_similarity:.2f}" if result.embedding_similarity else "no-emb"
        print(
            f"  [{i + 1}/{len(correct)}] {status} {r['source_ticker']}→{r['target_ticker']} ({sim})"
        )

        if result.accepted:
            correct_accepted += 1
        else:
            correct_rejected += 1
            correct_rejections.append((r, result))

    # Test on incorrect (should reject)
    incorrect_rejected = 0
    incorrect_accepted = 0
    rejection_reasons = {}

    print("\nTesting incorrect matches...")
    for i, r in enumerate(incorrect):
        result = validator.validate(
            context=r.get("context", ""),
            mention=r.get("raw_mention", ""),
            ticker=r.get("target_ticker", ""),
            company_name=r.get("target_name", ""),
            relationship_type=r.get("relationship_type", ""),
        )
        status = "✓" if not result.accepted else "✗"
        sim = f"sim={result.embedding_similarity:.2f}" if result.embedding_similarity else "no-emb"
        reason = result.rejection_reason.value if not result.accepted else "accepted"
        print(
            f"  [{i + 1}/{len(incorrect)}] {status} {r['source_ticker']}→{r['target_ticker']} ({sim}) [{reason}]"
        )

        if not result.accepted:
            incorrect_rejected += 1
            reason = result.rejection_reason.value
            rejection_reasons[reason] = rejection_reasons.get(reason, 0) + 1
        else:
            incorrect_accepted += 1

    return {
        "approach": "Layered (embeddings + patterns)",
        "correct_accepted": correct_accepted,
        "correct_rejected": correct_rejected,
        "incorrect_rejected": incorrect_rejected,
        "incorrect_accepted": incorrect_accepted,
        "rejection_reasons": rejection_reasons,
        "precision": correct_accepted / (correct_accepted + incorrect_accepted)
        if (correct_accepted + incorrect_accepted) > 0
        else 0,
        "false_positives": correct_rejected,
        "false_negatives": incorrect_accepted,
    }


def print_results(results: dict):
    """Print evaluation results."""
    print(f"\n{'=' * 60}")
    print(f"RESULTS: {results['approach']}")
    print("=" * 60)
    print(
        f"Correct matches:   {results['correct_accepted']} accepted, {results['correct_rejected']} rejected (FP)"
    )
    print(
        f"Incorrect matches: {results['incorrect_rejected']} rejected, {results['incorrect_accepted']} accepted (FN)"
    )
    print(f"\nPrecision: {results['precision']:.1%}")
    print(f"False positives: {results['false_positives']}")
    print(f"False negatives: {results['false_negatives']}")

    if results["rejection_reasons"]:
        print("\nRejection reasons:")
        for reason, count in sorted(results["rejection_reasons"].items(), key=lambda x: -x[1]):
            print(f"  {reason}: {count}")


def main():
    # Load ground truth
    data_file = Path(__file__).parent.parent / "data" / "er_ai_audit.csv"
    with open(data_file) as f:
        records = [r for r in csv.DictReader(f) if r.get("ai_label") in ("correct", "incorrect")]

    print(f"Loaded {len(records)} labeled records")

    # Baseline
    correct_count = sum(1 for r in records if r.get("ai_label") == "correct")
    incorrect_count = sum(1 for r in records if r.get("ai_label") == "incorrect")
    baseline_precision = correct_count / (correct_count + incorrect_count)
    print(f"\nBaseline (no validation): {baseline_precision:.1%} precision")

    # Pattern-only evaluation (fast, no API calls)
    print("\n" + "=" * 60)
    print("Evaluating PATTERN-ONLY approach...")
    print("=" * 60)
    pattern_results = evaluate_pattern_only(records)
    print_results(pattern_results)

    # Layered evaluation (uses OpenAI API)
    print("\n" + "=" * 60)
    print("Evaluating LAYERED approach (with embeddings)...")
    print("=" * 60)
    layered_results = evaluate_layered(records, sample_size=25)
    print_results(layered_results)

    # Comparison
    print("\n" + "=" * 60)
    print("COMPARISON")
    print("=" * 60)
    print(f"{'Approach':<35} {'Precision':>10} {'FP':>5} {'FN':>5}")
    print("-" * 60)
    print(f"{'Baseline (no validation)':<35} {baseline_precision:>10.1%} {'-':>5} {'-':>5}")
    print(
        f"{'Pattern-only':<35} {pattern_results['precision']:>10.1%} "
        f"{pattern_results['false_positives']:>5} {pattern_results['false_negatives']:>5}"
    )
    print(
        f"{'Layered (embeddings + patterns)':<35} {layered_results['precision']:>10.1%} "
        f"{layered_results['false_positives']:>5} {layered_results['false_negatives']:>5}"
    )


if __name__ == "__main__":
    main()
