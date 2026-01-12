#!/usr/bin/env python3
"""
Extract rules from curated examples.

This script analyzes curated examples (er_train.csv, er_validation.csv) to:
1. Identify common patterns in correct vs incorrect examples
2. Suggest rules that could catch errors
3. Measure rule coverage and precision

Usage:
    # Analyze train set and suggest rules
    python scripts/extract_rules_from_examples.py --split train

    # Test suggested rules on validation set
    python scripts/extract_rules_from_examples.py --split validation --test-rules

    # Export rules to Python code
    python scripts/extract_rules_from_examples.py --split train --export-rules
"""

import argparse
import csv
import re
from collections import Counter
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

load_dotenv()

DATA_DIR = Path(__file__).parent.parent / "data"


def load_split(split: str) -> list[dict[str, Any]]:
    """Load a specific split file."""
    filepath = DATA_DIR / f"er_{split}.csv"
    if not filepath.exists():
        raise FileNotFoundError(f"{filepath} not found. Run: python scripts/er_train_test_split.py")

    with open(filepath, encoding="utf-8") as f:
        return list(csv.DictReader(f))


def analyze_false_positives(records: list[dict]) -> dict[str, Any]:
    """Analyze false positives to find patterns that could be rules."""
    false_positives = [r for r in records if r.get("ai_label") == "incorrect"]

    patterns = {
        "generic_words": Counter(),
        "exchange_mentions": Counter(),
        "biographical": Counter(),
        "corporate_structure": Counter(),
        "wrong_relationship_type": Counter(),
        "short_mentions": Counter(),
    }

    for fp in false_positives:
        mention = fp.get("raw_mention", "").lower()
        context = fp.get("context", "").lower()
        rel_type = fp.get("relationship_type", "")

        # Generic words - but only when NOT in a company list
        # Context-aware: "Target, Walmart" = correct, "target market" = wrong
        generic_words = ["target", "master", "apple", "amazon", "google", "microsoft"]
        for word in generic_words:
            if word in mention:
                # Check if it's in a company list (correct) vs generic use (wrong)
                # Use the actual mention, not the generic word
                is_in_list = any(
                    pattern in context
                    for pattern in [
                        f"{mention},",  # "Target, Walmart"
                        f"{mention} and",  # "Target and Walmart"
                        f"such as {mention}",  # "customers such as Target"
                        f"including {mention}",  # "including Target"
                    ]
                )
                if not is_in_list:
                    patterns["generic_words"][mention] += 1
                break  # Only count once per mention

        # Exchange mentions
        if any(word in context for word in ["listed on", "trades on", "exchange"]):
            patterns["exchange_mentions"][mention] += 1

        # Biographical
        if any(pattern in context for pattern in ["serves as", "director", "formerly", "joined"]):
            patterns["biographical"][mention] += 1

        # Corporate structure
        if any(
            pattern in context
            for pattern in ["subsidiary", "affiliate", "parent company", "spin-off"]
        ):
            patterns["corporate_structure"][mention] += 1

        # Wrong relationship type (from AI reasoning)
        ai_reason = fp.get("ai_business_logic", "").lower()
        if any(
            phrase in ai_reason
            for phrase in [
                "not a",
                "wrong type",
                "relationship",
                "not established",
                "not supported",
            ]
        ):
            # Extract what it actually is
            if "director" in context or "serves as" in context:
                patterns["wrong_relationship_type"][f"{rel_type}->biographical"] += 1
            elif "subsidiary" in context or "affiliate" in context:
                patterns["wrong_relationship_type"][f"{rel_type}->corporate_structure"] += 1
            else:
                patterns["wrong_relationship_type"][rel_type] += 1

        # Short mentions (high false positive rate)
        if len(mention) <= 4:
            patterns["short_mentions"][mention] += 1

    return patterns


def suggest_rules(patterns: dict[str, Counter]) -> list[dict[str, Any]]:
    """Suggest rules based on patterns."""
    rules = []

    # Rule 1: Generic word blocklist
    if patterns["generic_words"]:
        words = [word for word, count in patterns["generic_words"].most_common(10)]
        rules.append(
            {
                "name": "GenericWordBlocklist",
                "type": "blocklist",
                "pattern": words,
                "coverage": sum(patterns["generic_words"].values()),
                "description": f"Block generic words: {', '.join(words[:5])}...",
            }
        )

    # Rule 2: Exchange reference filter
    if patterns["exchange_mentions"]:
        rules.append(
            {
                "name": "ExchangeReferenceFilter",
                "type": "pattern",
                "pattern": [
                    r"listed\s+on\s+(?:the\s+)?(?:nasdaq|nyse|amex)",
                    r"trades?\s+on\s+(?:the\s+)?(?:nasdaq|nyse|amex)",
                ],
                "coverage": sum(patterns["exchange_mentions"].values()),
                "description": "Filter stock exchange references",
            }
        )

    # Rule 3: Biographical filter
    if patterns["biographical"]:
        rules.append(
            {
                "name": "BiographicalFilter",
                "type": "pattern",
                "pattern": [
                    r"serves?\s+as\s+(?:director|officer|executive)",
                    r"formerly\s+(?:at|with|of)",
                    r"joined\s+(?:the\s+)?(?:board|company)",
                ],
                "coverage": sum(patterns["biographical"].values()),
                "description": "Filter biographical/career mentions",
            }
        )

    # Rule 4: Short mention filter (with exceptions)
    if patterns["short_mentions"]:
        rules.append(
            {
                "name": "ShortMentionFilter",
                "type": "heuristic",
                "pattern": {"max_length": 4, "exceptions": ["IBM", "HP", "GE"]},
                "coverage": sum(patterns["short_mentions"].values()),
                "description": "Filter very short mentions (except known companies)",
            }
        )

    return rules


def test_rules(rules: list[dict], test_records: list[dict]) -> dict[str, Any]:
    """Test rules on test set and measure coverage/precision."""

    results = {}

    def is_in_company_list(context: str, mention: str) -> bool:
        """Check if mention appears in a company list (context-aware)."""
        patterns = [
            rf"{re.escape(mention)},",  # "Target, Walmart"
            rf"{re.escape(mention)}\s+and",  # "Target and Walmart"
            rf"such\s+as\s+{re.escape(mention)}",  # "customers such as Target"
            rf"including\s+{re.escape(mention)}",  # "including Target"
        ]
        return any(re.search(p, context, re.IGNORECASE) for p in patterns)

    for rule in rules:
        if rule["type"] == "blocklist":
            # Test blocklist rule (context-aware)
            blocked = []
            for record in test_records:
                mention = record.get("raw_mention", "").lower()
                context = record.get("context", "").lower()

                # Check if mention matches generic word
                matches_generic = any(word in mention for word in rule["pattern"])

                if matches_generic:
                    # Context-aware: Don't block if in company list
                    if not is_in_company_list(context, mention):
                        blocked.append(record)

            # Measure precision
            correct_blocked = sum(1 for r in blocked if r.get("ai_label") == "incorrect")
            precision = (
                correct_blocked / len(blocked) if blocked else 0.0
            )  # How many were actually wrong

            results[rule["name"]] = {
                "coverage": len(blocked),
                "precision": precision,
                "false_positives": len(blocked) - correct_blocked,
            }

        elif rule["type"] == "pattern":
            # Test pattern rule
            matched = []
            for record in test_records:
                context = record.get("context", "").lower()
                for pattern in rule["pattern"]:
                    if re.search(pattern, context, re.I):
                        matched.append(record)
                        break

            correct_matched = sum(1 for r in matched if r.get("ai_label") == "incorrect")
            precision = correct_matched / len(matched) if matched else 0.0

            results[rule["name"]] = {
                "coverage": len(matched),
                "precision": precision,
                "false_positives": len(matched) - correct_matched,
            }

    return results


def export_rules_to_python(rules: list[dict], output_file: Path) -> None:
    """Export rules as Python code."""
    with open(output_file, "w") as f:
        f.write('"""\n')
        f.write("Rules extracted from curated examples.\n")
        f.write("\n")
        f.write("These rules are automatically generated from error analysis.\n")
        f.write("Review and refine before using in production.\n")
        f.write('"""\n\n')
        f.write("from typing import set\n\n")

        for rule in rules:
            f.write(f"# {rule['description']}\n")
            f.write(f"# Coverage: {rule['coverage']} cases\n")
            f.write(f"{rule['name'].upper()} = {{\n")

            if rule["type"] == "blocklist":
                for word in rule["pattern"][:20]:  # Limit to 20
                    f.write(f'    "{word}",\n')

            f.write("}\n\n")


def main():
    parser = argparse.ArgumentParser(description="Extract rules from curated examples")
    parser.add_argument(
        "--split",
        choices=["train", "validation"],
        default="train",
        help="Which split to analyze (default: train)",
    )
    parser.add_argument(
        "--test-rules",
        action="store_true",
        help="Test suggested rules on validation set",
    )
    parser.add_argument(
        "--export-rules",
        action="store_true",
        help="Export rules as Python code",
    )
    args = parser.parse_args()

    # Load data
    records = load_split(args.split)
    print(f"Loaded {len(records)} records from er_{args.split}.csv")

    # Analyze false positives
    print("\nAnalyzing false positives to find patterns...")
    patterns = analyze_false_positives(records)

    # Suggest rules
    print("\nSuggesting rules based on patterns...")
    rules = suggest_rules(patterns)

    # Print results
    print("\n" + "=" * 80)
    print("SUGGESTED RULES")
    print("=" * 80)

    for rule in rules:
        print(f"\n{rule['name']}:")
        print(f"  Type: {rule['type']}")
        print(f"  Coverage: {rule['coverage']} cases")
        print(f"  Description: {rule['description']}")

    # Test rules if requested
    if args.test_rules:
        print("\n" + "=" * 80)
        print("TESTING RULES ON VALIDATION SET")
        print("=" * 80)

        validation_records = load_split("validation")
        results = test_rules(rules, validation_records)

        for rule_name, result in results.items():
            print(f"\n{rule_name}:")
            print(f"  Coverage: {result['coverage']} cases")
            print(f"  Precision: {result['precision']:.1%}")
            print(f"  False Positives: {result['false_positives']}")

    # Export rules if requested
    if args.export_rules:
        output_file = (
            Path(__file__).parent.parent
            / "public_company_graph"
            / "entity_resolution"
            / "extracted_rules.py"
        )
        export_rules_to_python(rules, output_file)
        print(f"\n✓ Exported rules to {output_file}")

    print("\n" + "=" * 80)
    print("NEXT STEPS")
    print("=" * 80)
    print("1. Review suggested rules")
    print("2. Test on validation set: --test-rules")
    print("3. Export to code: --export-rules")
    print("4. Integrate into extraction pipeline")
    print("5. Measure improvement on test set")


if __name__ == "__main__":
    main()
