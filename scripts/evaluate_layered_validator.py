#!/usr/bin/env python3
"""
Evaluate the TieredDecisionSystem against ground truth.

NOTE: This script is for evaluation only.
For production extraction, use TieredDecisionSystem (see extract_with_llm_verification.py).

Compares three approaches:
1. No validation (baseline)
2. Pattern-only (Tier 1 + Tier 2, no embeddings)
3. Full tiered system (Tier 1 + Tier 2 + Tier 3 with embeddings)
"""

import csv
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / ".env")

from public_company_graph.config import Settings
from public_company_graph.entity_resolution.candidates import Candidate
from public_company_graph.entity_resolution.embedding_scorer import (
    EmbeddingSimilarityScorer,
)
from public_company_graph.entity_resolution.tiered_decision import (
    Decision,
    TieredDecisionSystem,
)
from public_company_graph.neo4j.connection import get_neo4j_driver


def evaluate_pattern_only(records: list[dict]) -> dict:
    """Evaluate using only pattern-based filters (Tier 1 + Tier 2, no embeddings)."""
    decision_system = TieredDecisionSystem(
        use_tier1=True,
        use_tier2=True,
        use_tier3=False,  # No embeddings
        use_tier4=False,
    )

    correct = [r for r in records if r.get("ai_label") == "correct"]
    incorrect = [r for r in records if r.get("ai_label") == "incorrect"]

    # Test on correct (should accept)
    correct_accepted = 0
    correct_rejected = 0
    correct_rejections = []

    for r in correct:
        mention = r.get("raw_mention", "")
        context = r.get("context", "")
        candidate = Candidate(
            text=mention,
            sentence=context,
            start_pos=0,
            end_pos=len(mention),
            source_pattern="evaluation",
        )

        decision = decision_system.decide(
            candidate=candidate,
            context=context,
            relationship_type=r.get("relationship_type", ""),
            company_name=r.get("target_name", ""),
            embedding_similarity=None,  # No embeddings
        )

        if decision.decision == Decision.ACCEPT:
            correct_accepted += 1
        else:
            correct_rejected += 1
            correct_rejections.append((r, decision))

    # Test on incorrect (should reject)
    incorrect_rejected = 0
    incorrect_accepted = 0
    rejection_reasons = {}

    for r in incorrect:
        mention = r.get("raw_mention", "")
        context = r.get("context", "")
        candidate = Candidate(
            text=mention,
            sentence=context,
            start_pos=0,
            end_pos=len(mention),
            source_pattern="evaluation",
        )

        decision = decision_system.decide(
            candidate=candidate,
            context=context,
            relationship_type=r.get("relationship_type", ""),
            company_name=r.get("target_name", ""),
            embedding_similarity=None,  # No embeddings
        )

        if decision.decision != Decision.ACCEPT:
            incorrect_rejected += 1
            reason = f"{decision.tier.value}: {decision.reason}"
            rejection_reasons[reason] = rejection_reasons.get(reason, 0) + 1
        else:
            incorrect_accepted += 1

    return {
        "approach": "Pattern-only (Tier 1 + Tier 2)",
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
    """Evaluate using full tiered approach (with embeddings)."""
    # Initialize TieredDecisionSystem with embedding support
    settings = Settings()
    driver = get_neo4j_driver()
    embedding_scorer = EmbeddingSimilarityScorer(
        threshold=0.30,
        neo4j_driver=driver,
        database=settings.neo4j_database,
    )

    decision_system = TieredDecisionSystem(
        use_tier1=True,
        use_tier2=True,
        use_tier3=True,
        use_tier4=False,  # Skip LLM for evaluation
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
        mention = r.get("raw_mention", "")
        context = r.get("context", "")
        candidate = Candidate(
            text=mention,
            sentence=context,
            start_pos=0,
            end_pos=len(mention),
            source_pattern="evaluation",
        )

        # Get embedding similarity
        embedding_similarity = None
        try:
            emb_result = embedding_scorer.score(
                context=context,
                ticker=r.get("target_ticker", ""),
                company_name=r.get("target_name", ""),
            )
            embedding_similarity = emb_result.similarity
        except Exception:
            pass

        decision = decision_system.decide(
            candidate=candidate,
            context=context,
            relationship_type=r.get("relationship_type", ""),
            company_name=r.get("target_name", ""),
            embedding_similarity=embedding_similarity,
        )

        status = "✓" if decision.decision == Decision.ACCEPT else "✗"
        sim = f"sim={embedding_similarity:.2f}" if embedding_similarity is not None else "no-emb"
        print(
            f"  [{i + 1}/{len(correct)}] {status} {r['source_ticker']}→{r['target_ticker']} ({sim})"
        )

        if decision.decision == Decision.ACCEPT:
            correct_accepted += 1
        else:
            correct_rejected += 1
            correct_rejections.append((r, decision))

    # Test on incorrect (should reject)
    incorrect_rejected = 0
    incorrect_accepted = 0
    rejection_reasons = {}

    print("\nTesting incorrect matches...")
    for i, r in enumerate(incorrect):
        mention = r.get("raw_mention", "")
        context = r.get("context", "")
        candidate = Candidate(
            text=mention,
            sentence=context,
            start_pos=0,
            end_pos=len(mention),
            source_pattern="evaluation",
        )

        # Get embedding similarity
        embedding_similarity = None
        try:
            emb_result = embedding_scorer.score(
                context=context,
                ticker=r.get("target_ticker", ""),
                company_name=r.get("target_name", ""),
            )
            embedding_similarity = emb_result.similarity
        except Exception:
            pass

        decision = decision_system.decide(
            candidate=candidate,
            context=context,
            relationship_type=r.get("relationship_type", ""),
            company_name=r.get("target_name", ""),
            embedding_similarity=embedding_similarity,
        )

        status = "✓" if decision.decision != Decision.ACCEPT else "✗"
        sim = f"sim={embedding_similarity:.2f}" if embedding_similarity is not None else "no-emb"
        reason = (
            f"{decision.tier.value}: {decision.reason}"
            if decision.decision != Decision.ACCEPT
            else "accepted"
        )
        print(
            f"  [{i + 1}/{len(incorrect)}] {status} {r['source_ticker']}→{r['target_ticker']} ({sim}) [{reason}]"
        )

        if decision.decision != Decision.ACCEPT:
            incorrect_rejected += 1
            reason = f"{decision.tier.value}: {decision.reason}"
            rejection_reasons[reason] = rejection_reasons.get(reason, 0) + 1
        else:
            incorrect_accepted += 1

    return {
        "approach": "Tiered System (Tier 1 + Tier 2 + Tier 3)",
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

    # Tiered evaluation (uses OpenAI API)
    print("\n" + "=" * 60)
    print("Evaluating TIERED SYSTEM (with embeddings)...")
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
        f"{'Pattern-only (Tier 1+2)':<35} {pattern_results['precision']:>10.1%} "
        f"{pattern_results['false_positives']:>5} {pattern_results['false_negatives']:>5}"
    )
    print(
        f"{'Tiered System (Tier 1+2+3)':<35} {layered_results['precision']:>10.1%} "
        f"{layered_results['false_positives']:>5} {layered_results['false_negatives']:>5}"
    )


if __name__ == "__main__":
    main()
