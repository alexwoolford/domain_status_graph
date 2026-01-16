#!/usr/bin/env python3
"""
Query GraphRAG layer for semantic search and Q&A.

Examples:
    # Search for documents about competition
    python scripts/query_graphrag.py "What are the main competitive threats?"

    # Search within a specific company's context
    python scripts/query_graphrag.py "What risks does Tesla face?" --company TSLA

    # Get answer context for a question
    python scripts/query_graphrag.py "Who are Apple's competitors?" --company AAPL --answer
"""

import argparse
from pathlib import Path

from dotenv import load_dotenv

from public_company_graph.config import Settings
from public_company_graph.embeddings.openai_client import get_openai_client
from public_company_graph.graphrag.queries import (
    answer_question,
    search_documents,
    search_with_graph_context,
)
from public_company_graph.neo4j.connection import get_neo4j_driver

# Load environment
load_dotenv(Path(__file__).parent.parent / ".env")


def create_query_embedding(query_text: str) -> list[float]:
    """Create embedding for query text."""
    client = get_openai_client()

    from public_company_graph.embeddings.openai_client import create_embedding

    result = create_embedding(client, query_text, model="text-embedding-3-small")
    if not result:
        raise ValueError("Failed to create query embedding")
    return result


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Query GraphRAG layer for semantic search")
    parser.add_argument(
        "query",
        type=str,
        help="Query text to search for",
    )
    parser.add_argument(
        "--company",
        type=str,
        help="Company ticker to focus search (optional)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=5,
        help="Maximum number of results (default: 5)",
    )
    parser.add_argument(
        "--min-similarity",
        type=float,
        default=0.5,
        help="Minimum similarity threshold (default: 0.5)",
    )
    parser.add_argument(
        "--answer",
        action="store_true",
        help="Return answer context (combines documents)",
    )

    args = parser.parse_args()

    settings = Settings()
    driver = get_neo4j_driver()

    try:
        print(f"🔍 Query: {args.query}")
        if args.company:
            print(f"   Company: {args.company}")
        print()

        # Create query embedding
        print("Creating query embedding...")
        query_embedding = create_query_embedding(args.query)
        print("✓ Embedding created")
        print()

        # Search
        if args.answer:
            print("Retrieving answer context...")
            result = answer_question(
                driver,
                args.query,
                query_embedding,
                company_ticker=args.company,
                max_documents=args.limit,
                database=settings.neo4j_database,
            )

            print(f"\n📄 Found {result['num_documents']} relevant documents")
            print(f"📊 Companies mentioned: {len(result['companies'])}")
            print()

            if result["companies"]:
                print("Companies:")
                for ticker, name in result["companies"][:10]:
                    print(f"  - {ticker}: {name}")
                print()

            print("=" * 80)
            print("CONTEXT (for LLM answer generation):")
            print("=" * 80)
            print(result["context"][:2000])  # First 2000 chars
            if len(result["context"]) > 2000:
                print(f"\n... ({len(result['context']) - 2000} more characters)")

        else:
            if args.company:
                print(f"Searching documents for {args.company} and related companies...")
                results = search_with_graph_context(
                    driver,
                    args.query,
                    query_embedding,
                    company_ticker=args.company,
                    limit=args.limit,
                    database=settings.neo4j_database,
                    min_similarity=args.min_similarity,
                )
            else:
                print("Searching all documents...")
                results = search_documents(
                    driver,
                    args.query,
                    query_embedding,
                    limit=args.limit,
                    database=settings.neo4j_database,
                    min_similarity=args.min_similarity,
                )

            print(f"\n📄 Found {len(results)} relevant documents")
            print()

            for i, doc in enumerate(results, 1):
                print("=" * 80)
                print(f"Document {i} (similarity: {doc['similarity']:.3f})")
                print("=" * 80)
                print(
                    f"Company: {doc.get('company_name', 'Unknown')} ({doc.get('company_ticker', 'N/A')})"
                )
                print(f"Section: {doc.get('section_type', 'unknown')}")
                print(f"Filing Year: {doc.get('filing_year', 'N/A')}")
                print()
                print("Text:")
                print(doc["text"][:500])  # First 500 chars
                if len(doc["text"]) > 500:
                    print(f"\n... ({len(doc['text']) - 500} more characters)")
                print()

    finally:
        driver.close()


if __name__ == "__main__":
    main()
