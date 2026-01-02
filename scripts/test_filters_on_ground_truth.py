#!/usr/bin/env python3
"""
Test new filters against ground truth to measure improvement.

This script applies the BiographicalContextFilter, ExchangeReferenceFilter,
and RelationshipVerifier to our labeled ground truth to see how many errors
would be caught.
"""

import csv
import sys
from pathlib import Path

# Force reimport of all public_company_graph modules to get latest changes
mods_to_remove = [m for m in list(sys.modules.keys()) if "public_company_graph" in m]
for m in mods_to_remove:
    del sys.modules[m]

sys.path.insert(0, str(Path(__file__).parent.parent))

from public_company_graph.entity_resolution.candidates import Candidate
from public_company_graph.entity_resolution.filters import (
    BiographicalContextFilter,
    ExchangeReferenceFilter,
)
from public_company_graph.entity_resolution.relationship_verifier import (
    RelationshipVerifier,
    VerificationResult,
)


def load_ground_truth(filepath: str) -> list[dict]:
    """Load the labeled ground truth CSV."""
    records = []
    with open(filepath, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            records.append(row)
    return records


def test_filters_on_record(
    record: dict,
    bio_filter: BiographicalContextFilter,
    exchange_filter: ExchangeReferenceFilter,
    rel_verifier: RelationshipVerifier,
) -> dict:
    """Test filters and relationship verifier on a single ground truth record."""
    # Create a candidate from the record
    raw_mention = record.get("raw_mention", "")
    context = record.get("context", "")
    relationship_type = record.get("relationship_type", "")

    # Use context as the sentence for filter checking
    candidate = Candidate(
        text=raw_mention,
        sentence=context,
        start_pos=0,
        end_pos=len(raw_mention),
        source_pattern="ground_truth",
    )

    # Test biographical filter
    bio_result = bio_filter.filter(candidate)

    # Test exchange filter
    exchange_result = exchange_filter.filter(candidate)

    # Test relationship verifier
    rel_verification = rel_verifier.verify(relationship_type, context, raw_mention)
    rel_contradicted = rel_verification.result == VerificationResult.CONTRADICTED

    return {
        "bio_filtered": not bio_result.passed,
        "bio_reason": bio_result.reason.value if not bio_result.passed else None,
        "exchange_filtered": not exchange_result.passed,
        "exchange_reason": (exchange_result.reason.value if not exchange_result.passed else None),
        "rel_contradicted": rel_contradicted,
        "rel_suggested_type": (
            rel_verification.suggested_type.value if rel_verification.suggested_type else None
        ),
        "rel_explanation": rel_verification.explanation,
        "any_filtered": (not bio_result.passed or not exchange_result.passed or rel_contradicted),
    }


def main():
    print("=" * 70)
    print("TESTING FILTERS + RELATIONSHIP VERIFIER ON GROUND TRUTH")
    print("=" * 70)

    # Load ground truth
    records = load_ground_truth("data/er_ground_truth_labeled.csv")
    print(f"\nLoaded {len(records)} ground truth records")

    # Initialize filters and verifier
    bio_filter = BiographicalContextFilter()
    exchange_filter = ExchangeReferenceFilter()
    rel_verifier = RelationshipVerifier()

    # Categorize records
    correct = [r for r in records if r["ai_label"] == "correct"]
    incorrect = [r for r in records if r["ai_label"] == "incorrect"]
    ambiguous = [r for r in records if r["ai_label"] == "ambiguous"]

    print(f"  Correct: {len(correct)}")
    print(f"  Incorrect: {len(incorrect)}")
    print(f"  Ambiguous: {len(ambiguous)}")

    # Test filters on all records
    results = []
    for record in records:
        filter_result = test_filters_on_record(record, bio_filter, exchange_filter, rel_verifier)
        results.append({**record, **filter_result})

    # Analyze results
    print("\n" + "=" * 70)
    print("FILTER + VERIFIER EFFECTIVENESS")
    print("=" * 70)

    # How many incorrect records would be filtered?
    incorrect_filtered = [r for r in results if r["ai_label"] == "incorrect" and r["any_filtered"]]
    incorrect_bio = [r for r in results if r["ai_label"] == "incorrect" and r["bio_filtered"]]
    incorrect_exchange = [
        r for r in results if r["ai_label"] == "incorrect" and r["exchange_filtered"]
    ]
    incorrect_rel = [r for r in results if r["ai_label"] == "incorrect" and r["rel_contradicted"]]

    print("\n✓ INCORRECT matches caught:")
    print(
        f"  Biographical filter:     {len(incorrect_bio)}/{len(incorrect)} ({100 * len(incorrect_bio) / len(incorrect):.0f}%)"
    )
    print(
        f"  Exchange filter:         {len(incorrect_exchange)}/{len(incorrect)} ({100 * len(incorrect_exchange) / len(incorrect):.0f}%)"
    )
    print(
        f"  Relationship verifier:   {len(incorrect_rel)}/{len(incorrect)} ({100 * len(incorrect_rel) / len(incorrect):.0f}%)"
    )
    print(
        f"  Combined (any):          {len(incorrect_filtered)}/{len(incorrect)} ({100 * len(incorrect_filtered) / len(incorrect):.0f}%)"
    )

    # How many correct records would be (wrongly) filtered? (false positives)
    correct_filtered = [r for r in results if r["ai_label"] == "correct" and r["any_filtered"]]

    print("\n⚠️  CORRECT matches wrongly filtered (false positives):")
    print(
        f"  Count: {len(correct_filtered)}/{len(correct)} ({100 * len(correct_filtered) / len(correct):.1f}%)"
    )
    if correct_filtered:
        for r in correct_filtered[:5]:
            reason = "REL" if r["rel_contradicted"] else "FILTER"
            print(f"    [{reason}] {r['raw_mention']} → {r['target_ticker']}")
            if r["rel_contradicted"]:
                print(f"          {r['rel_explanation']}")

    # Show which incorrect records were caught
    print("\n✓ DETAILS: Incorrect matches caught:")
    for r in incorrect_filtered:
        if r["bio_filtered"]:
            filter_type = "BIO"
        elif r["exchange_filtered"]:
            filter_type = "EXCHANGE"
        elif r["rel_contradicted"]:
            filter_type = "REL_TYPE"
        else:
            filter_type = "???"
        print(
            f'  [{filter_type}] {r["source_ticker"]} → {r["target_ticker"]}: "{r["raw_mention"]}"'
        )
        if r["rel_contradicted"] and r["rel_suggested_type"]:
            print(f"           Suggested: {r['rel_suggested_type']}")

    # Show which incorrect records were NOT caught
    incorrect_missed = [
        r for r in results if r["ai_label"] == "incorrect" and not r["any_filtered"]
    ]
    print(f"\n✗ DETAILS: Incorrect matches NOT caught ({len(incorrect_missed)}):")
    for r in incorrect_missed:
        print(
            f'  {r["source_ticker"]} → {r["target_ticker"]}: "{r["raw_mention"]}" ({r["relationship_type"]})'
        )

    # Calculate new precision
    print("\n" + "=" * 70)
    print("IMPACT SUMMARY")
    print("=" * 70)

    old_correct = len(correct)
    old_incorrect = len(incorrect)
    old_precision = old_correct / (old_correct + old_incorrect)

    # After filtering: we remove both true positives (wrongly filtered correct)
    # and true negatives (correctly filtered incorrect)
    new_correct = old_correct - len(correct_filtered)
    new_incorrect = old_incorrect - len(incorrect_filtered)
    new_total = new_correct + new_incorrect

    if new_total > 0:
        new_precision = new_correct / new_total
    else:
        new_precision = 0

    print("\n  BEFORE filters:")
    print(f"    Correct:   {old_correct}")
    print(f"    Incorrect: {old_incorrect}")
    print(f"    Precision: {old_precision:.1%}")

    print("\n  AFTER filters:")
    print(f"    Correct:   {new_correct} (-{len(correct_filtered)} false positives)")
    print(f"    Incorrect: {new_incorrect} (-{len(incorrect_filtered)} caught)")
    print(f"    Precision: {new_precision:.1%}")

    improvement = new_precision - old_precision
    print(f"\n  IMPROVEMENT: {improvement:+.1%}")

    print("\n" + "=" * 70)


if __name__ == "__main__":
    main()
