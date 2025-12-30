#!/usr/bin/env python3
"""
Evaluate alternative SEC parsers (sec-parser, edgartools) for extracting
Item 1: Business descriptions from 10-K filings.

Tests on companies that our current datamule+fallback parser fails on.
"""

import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from public_company_graph.config import get_data_dir


def get_problematic_ciks():
    """Get CIKs of companies without descriptions in Neo4j."""
    # Sample of known problematic CIKs from our previous analysis
    return [
        "0000320193",  # Apple Inc.
        "0001551152",  # AbbVie Inc.
        "0001097149",  # Align Technology Inc
        "0000919012",  # American Eagle Outfitters Inc
        "0001521332",  # Aptiv PLC
    ]


def find_10k_file(cik: str) -> Path | None:
    """Find the 10-K HTML file for a CIK."""
    filings_dir = get_data_dir() / "10k_filings" / cik
    if not filings_dir.exists():
        return None

    html_files = list(filings_dir.glob("*.html"))
    if html_files:
        return html_files[0]
    return None


def test_sec_parser(file_path: Path) -> dict:
    """Test sec-parser library on a 10-K file."""
    result = {"success": False, "length": 0, "error": None, "preview": None}

    try:
        from sec_parser import TreeBuilder

        content = file_path.read_text(encoding="utf-8", errors="ignore")
        builder = TreeBuilder()
        tree = builder.build(content)

        # Find Item 1 Business section in the tree
        item1_found = False
        item1_text_parts = []

        for node in tree:
            node_text = str(node).lower()
            # Check if this is Item 1 section header
            if "item 1" in node_text and "business" in node_text:
                item1_found = True
                continue

            # If we found Item 1, collect text until next major section
            if item1_found:
                if "item 1a" in node_text or "item 2" in node_text:
                    break
                item1_text_parts.append(str(node))

        if item1_text_parts:
            section_text = "\n".join(item1_text_parts)
            if len(section_text) > 500:
                result["success"] = True
                result["length"] = len(section_text)
                result["preview"] = section_text[:200] + "..."

        if not result["success"]:
            result["error"] = "Item 1 Business section not found or too short"

    except Exception as e:
        result["error"] = str(e)[:200]

    return result


def test_edgartools(cik: str) -> dict:
    """Test edgartools library for a company (fetches from SEC)."""
    result = {"success": False, "length": 0, "error": None, "preview": None}

    try:
        from edgar import Company, set_identity

        # Set identity for SEC access (required)
        set_identity("alex woolford alex@woolford.io")

        # Remove leading zeros and get company
        cik_int = int(cik)
        company = Company(cik_int)

        # Get latest 10-K
        filings = company.get_filings(form="10-K")
        if filings and len(filings) > 0:
            tenk = filings[0]
            # Get the TenK object
            filing_obj = tenk.obj()

            # Use bracket notation to get Item 1
            try:
                item1 = filing_obj["Item 1"]
                if item1:
                    text = str(item1)
                    if len(text) > 500:
                        result["success"] = True
                        result["length"] = len(text)
                        result["preview"] = text[:200] + "..."
            except (KeyError, TypeError):
                pass

        if not result["success"] and not result["error"]:
            result["error"] = "Item 1 Business section not found"

    except Exception as e:
        result["error"] = str(e)[:200]

    return result


def test_current_parser(file_path: Path, cik: str) -> dict:
    """Test our current datamule+fallback parser."""
    result = {"success": False, "length": 0, "error": None, "preview": None}

    try:
        from public_company_graph.parsing.business_description import (
            extract_business_description_with_datamule_fallback,
        )

        text = extract_business_description_with_datamule_fallback(
            file_path=file_path,
            cik=cik,
            filings_dir=file_path.parent,
        )

        if text and len(text) > 500:
            result["success"] = True
            result["length"] = len(text)
            result["preview"] = text[:200] + "..."
        else:
            result["error"] = "Extraction returned None or too short"

    except Exception as e:
        result["error"] = str(e)[:200]

    return result


def main():
    print("=" * 80)
    print("SEC Parser Evaluation - Testing on Problematic Companies")
    print("=" * 80)

    ciks = get_problematic_ciks()

    results = {
        "current": {"success": 0, "fail": 0},
        "sec-parser": {"success": 0, "fail": 0},
        "edgartools": {"success": 0, "fail": 0},
    }

    for cik in ciks:
        print(f"\n{'─' * 80}")
        print(f"Testing CIK: {cik}")
        print("─" * 80)

        file_path = find_10k_file(cik)

        # Test current parser
        if file_path:
            current = test_current_parser(file_path, cik)
            status = "✅" if current["success"] else "❌"
            print(f"  Current (datamule+fallback): {status} ({current['length']:,} chars)")
            if current["error"]:
                print(f"    Error: {current['error'][:80]}")
            results["current"]["success" if current["success"] else "fail"] += 1

            # Test sec-parser
            sec = test_sec_parser(file_path)
            status = "✅" if sec["success"] else "❌"
            print(f"  sec-parser:                   {status} ({sec['length']:,} chars)")
            if sec["error"]:
                print(f"    Error: {sec['error'][:80]}")
            results["sec-parser"]["success" if sec["success"] else "fail"] += 1
        else:
            print(f"  ⚠️  No local 10-K file found for CIK {cik}")

        # Test edgartools (needs network)
        print("  edgartools:                   (requires network, testing...)")
        edgar = test_edgartools(cik)
        status = "✅" if edgar["success"] else "❌"
        print(f"  edgartools:                   {status} ({edgar['length']:,} chars)")
        if edgar["error"]:
            print(f"    Error: {edgar['error'][:80]}")
        results["edgartools"]["success" if edgar["success"] else "fail"] += 1

    # Summary
    print(f"\n{'=' * 80}")
    print("SUMMARY")
    print("=" * 80)
    for parser, counts in results.items():
        total = counts["success"] + counts["fail"]
        pct = 100 * counts["success"] / total if total > 0 else 0
        print(f"  {parser:25s}: {counts['success']}/{total} ({pct:.0f}% success)")


if __name__ == "__main__":
    main()
