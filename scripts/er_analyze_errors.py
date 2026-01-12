#!/usr/bin/env python3
"""
Analyze errors on a specific split to guide improvements.

ONLY run this on train or validation sets during development.
Use this to understand patterns and develop additional logic.

Usage:
  python scripts/er_analyze_errors.py --split train
  python scripts/er_analyze_errors.py --split train --type false-positives
  python scripts/er_analyze_errors.py --split train --type false-negatives
"""

import argparse
import csv
from collections import Counter
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

# NOTE: This script is for analysis/evaluation only
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

DATA_DIR = Path(__file__).parent.parent / "data"


def load_split(split: str) -> list[dict]:
    """Load a specific split file."""
    filepath = DATA_DIR / f"er_{split}.csv"
    if not filepath.exists():
        raise FileNotFoundError(f"{filepath} not found. Run: python scripts/er_train_test_split.py")

    with open(filepath, encoding="utf-8") as f:
        return list(csv.DictReader(f))


def analyze_errors(records: list[dict], error_type: str | None = None):
    """Analyze false positives and false negatives."""
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

    false_positives = []  # Kept but incorrect
    false_negatives = []  # Rejected but correct

    for r in records:
        # Create candidate from mention
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
                ticker=r["target_ticker"],
                company_name=r.get("target_name", ""),
            )
            embedding_similarity = emb_result.similarity
        except Exception as e:
            print(f"Warning: Embedding check failed for {r['target_ticker']}: {e}")

        # Make decision
        decision = decision_system.decide(
            candidate=candidate,
            context=context,
            relationship_type=r["relationship_type"],
            company_name=r.get("target_name", ""),
            embedding_similarity=embedding_similarity,
        )

        # Convert Decision to accepted boolean for compatibility
        accepted = decision.decision == Decision.ACCEPT

        is_correct = r["ai_label"] == "correct"

        if accepted and not is_correct:
            false_positives.append(
                {
                    "record": r,
                    "decision": decision,
                    "embedding_similarity": embedding_similarity,
                }
            )
        elif not accepted and is_correct:
            false_negatives.append(
                {
                    "record": r,
                    "decision": decision,
                    "embedding_similarity": embedding_similarity,
                }
            )

    if error_type == "false-positives" or error_type is None:
        print("=" * 70)
        print(f"FALSE POSITIVES ({len(false_positives)}) - Kept but should be rejected")
        print("=" * 70)
        print("These are the ones hurting precision. Look for patterns to filter.")
        print()

        # Group by AI reasoning
        reasons = Counter()
        for fp in false_positives:
            reason = fp["record"].get("ai_business_logic", "unknown")[:50]
            reasons[reason] += 1

        print("Top reasons (from AI):")
        for reason, count in reasons.most_common(10):
            print(f"  {count}x: {reason}...")
        print()

        # Show examples
        for i, fp in enumerate(false_positives[:5], 1):
            r = fp["record"]
            decision = fp["decision"]
            emb_sim = fp.get("embedding_similarity")
            print(
                f"--- FP {i}: {r['source_ticker']} -> {r['target_ticker']} ({r['relationship_type']}) ---"
            )
            print(f"Mention: {r.get('raw_mention', 'N/A')}")
            print(f"Context: {r.get('context', 'N/A')[:200]}...")
            print(f"AI reason: {r.get('ai_business_logic', 'N/A')}")
            print(f"Decision: {decision.decision.value} (tier: {decision.tier.value})")
            print(f"Embedding score: {emb_sim:.3f}" if emb_sim is not None else "N/A")
            print()

    if error_type == "false-negatives" or error_type is None:
        print("=" * 70)
        print(f"FALSE NEGATIVES ({len(false_negatives)}) - Rejected but should be kept")
        print("=" * 70)
        print("These are hurting recall. Are filters too aggressive?")
        print()

        # Group by rejection reason
        rejection_reasons = Counter()
        for fn in false_negatives:
            decision = fn["decision"]
            # Use tier and reason to categorize
            tier_name = decision.tier.value
            reason = decision.reason
            rejection_reasons[f"{tier_name}: {reason}"] += 1

        print("Rejection reasons:")
        for reason, count in rejection_reasons.items():
            print(f"  {count}x: {reason}")
        print()

        # Show examples
        for i, fn in enumerate(false_negatives[:5], 1):
            r = fn["record"]
            decision = fn["decision"]
            emb_sim = fn.get("embedding_similarity")
            print(
                f"--- FN {i}: {r['source_ticker']} -> {r['target_ticker']} ({r['relationship_type']}) ---"
            )
            print(f"Mention: {r.get('raw_mention', 'N/A')}")
            print(f"Context: {r.get('context', 'N/A')[:200]}...")
            print(f"Decision: {decision.decision.value} (tier: {decision.tier.value})")
            print(f"Reason: {decision.reason}")
            print(f"Embedding: {emb_sim:.3f}" if emb_sim is not None else "N/A")
            print()

    print("=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"False Positives: {len(false_positives)} (hurting precision)")
    print(f"False Negatives: {len(false_negatives)} (hurting recall)")
    print()
    print("Next steps:")
    print("  - For FPs: Find patterns that can be filtered (additional rules)")
    print("  - For FNs: Check if filters are too aggressive (relax thresholds)")
    print("  - After changes, validate on: python scripts/er_evaluate_split.py --split validation")


def main():
    parser = argparse.ArgumentParser(description="Analyze ER errors")
    parser.add_argument(
        "--split",
        choices=["train", "validation"],
        default="train",
        help="Which split to analyze (default: train)",
    )
    parser.add_argument(
        "--type",
        choices=["false-positives", "false-negatives"],
        help="Specific error type to analyze",
    )
    args = parser.parse_args()

    if args.split == "test":
        print("⚠️  ERROR: Do not analyze test set errors!")
        print("   Use train or validation only.")
        return

    records = load_split(args.split)
    print(f"Analyzing {len(records)} records from er_{args.split}.csv")
    print()

    analyze_errors(records, args.type)


if __name__ == "__main__":
    main()
