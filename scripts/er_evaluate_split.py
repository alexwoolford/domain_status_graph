#!/usr/bin/env python3
"""
Evaluate entity resolution on a specific split.

Usage:
  python scripts/er_evaluate_split.py --split train       # Develop/analyze
  python scripts/er_evaluate_split.py --split validation  # Check for overfitting
  python scripts/er_evaluate_split.py --split test        # FINAL evaluation only!
"""

import argparse
import csv
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

# NOTE: This script is for evaluation only
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


def evaluate_baseline(records: list[dict]) -> dict:
    """Baseline: trust all relationships."""
    correct = sum(1 for r in records if r["ai_label"] == "correct")
    total = len(records)

    return {
        "name": "Baseline (no validation)",
        "correct": correct,
        "total": total,
        "precision": correct / total if total > 0 else 0,
        "rejected": 0,
    }


def evaluate_layered(records: list[dict], embedding_threshold: float) -> dict:
    """Evaluate tiered decision system."""
    # Initialize TieredDecisionSystem with embedding support
    settings = Settings()
    driver = get_neo4j_driver()
    embedding_scorer = EmbeddingSimilarityScorer(
        threshold=embedding_threshold,
        neo4j_driver=driver,
        database=settings.neo4j_database,
    )

    decision_system = TieredDecisionSystem(
        use_tier1=True,
        use_tier2=True,
        use_tier3=True,
        use_tier4=False,  # Skip LLM for evaluation
    )

    kept_correct = 0
    kept_incorrect = 0
    rejected_correct = 0
    rejected_incorrect = 0

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
        except Exception:
            pass  # Continue without embedding if it fails

        # Make decision
        decision = decision_system.decide(
            candidate=candidate,
            context=context,
            relationship_type=r["relationship_type"],
            company_name=r.get("target_name", ""),
            embedding_similarity=embedding_similarity,
        )

        # Convert Decision to accepted boolean
        accepted = decision.decision == Decision.ACCEPT
        is_correct = r["ai_label"] == "correct"

        if accepted:
            if is_correct:
                kept_correct += 1
            else:
                kept_incorrect += 1
        else:
            if is_correct:
                rejected_correct += 1
            else:
                rejected_incorrect += 1

    kept_total = kept_correct + kept_incorrect
    precision = kept_correct / kept_total if kept_total > 0 else 0

    # Recall: what fraction of correct records did we keep?
    total_correct = kept_correct + rejected_correct
    recall = kept_correct / total_correct if total_correct > 0 else 0

    return {
        "name": "Tiered Decision System",
        "kept_correct": kept_correct,
        "kept_incorrect": kept_incorrect,
        "rejected_correct": rejected_correct,
        "rejected_incorrect": rejected_incorrect,
        "precision": precision,
        "recall": recall,
        "f1": 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0,
    }


def main():
    parser = argparse.ArgumentParser(description="Evaluate ER on a split")
    parser.add_argument(
        "--split",
        choices=["train", "validation", "test"],
        required=True,
        help="Which split to evaluate on",
    )
    parser.add_argument(
        "--embedding-threshold",
        type=float,
        default=0.30,
        help="Embedding similarity threshold (default: 0.30)",
    )
    args = parser.parse_args()

    # Warning for test set
    if args.split == "test":
        print("⚠️  WARNING: Evaluating on TEST set.")
        print("   Only do this for FINAL evaluation!")
        print("   Do NOT tune parameters based on these results.")
        response = input("   Continue? (yes/no): ")
        if response.lower() != "yes":
            print("Aborted.")
            return
        print()

    records = load_split(args.split)
    print(f"Loaded {len(records)} records from er_{args.split}.csv")
    print()

    # Baseline
    baseline = evaluate_baseline(records)
    print("BASELINE (no validation)")
    print(f"  Precision: {baseline['precision']:.1%} ({baseline['correct']}/{baseline['total']})")
    print()

    # Tiered decision system
    layered = evaluate_layered(records, args.embedding_threshold)

    print(f"TIERED DECISION SYSTEM (threshold={args.embedding_threshold})")
    print(f"  Kept:     {layered['kept_correct']} correct, {layered['kept_incorrect']} incorrect")
    print(
        f"  Rejected: {layered['rejected_correct']} correct (FN), {layered['rejected_incorrect']} incorrect (TN)"
    )
    print(f"  Precision: {layered['precision']:.1%}")
    print(f"  Recall:    {layered['recall']:.1%}")
    print(f"  F1:        {layered['f1']:.1%}")
    print()

    # Improvement
    improvement = layered["precision"] - baseline["precision"]
    print(f"IMPROVEMENT: {improvement:+.1%} precision vs baseline")


if __name__ == "__main__":
    main()
