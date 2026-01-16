#!/usr/bin/env python3
"""
Ask a question to the GraphRAG layer and get an answer with context.

Example:
    python scripts/ask_graphrag.py "Which companies might be impacted by recent actions in Venezuela and why?"
"""

import argparse
import sys
from pathlib import Path

from dotenv import load_dotenv

from public_company_graph.config import Settings
from public_company_graph.embeddings.openai_client import create_embedding, get_openai_client
from public_company_graph.graphrag.queries import answer_question
from public_company_graph.neo4j.connection import get_neo4j_driver

load_dotenv(Path(__file__).parent.parent / ".env")


def synthesize_answer(
    client,
    question: str,
    context: str,
    companies: list[tuple[str, str]],
    traversal_paths: list[dict],
    model: str = "gpt-5.2-chat-latest",
) -> str:
    """
    Use LLM to synthesize an answer from retrieved GraphRAG context.

    Args:
        client: OpenAI client
        question: Original question
        context: Retrieved chunks context
        companies: List of (ticker, name) tuples
        traversal_paths: Graph relationship paths
        model: LLM model to use

    Returns:
        Synthesized answer text
    """
    # Build relationship summary
    relationship_summary = ""
    if traversal_paths:
        by_type = {}
        for path in traversal_paths:
            rel_type = path.get("relationship", "unknown")
            if rel_type not in by_type:
                by_type[rel_type] = []
            by_type[rel_type].append(path)

        relationship_summary = "\n\nGraph Relationships Found:\n"
        for rel_type, paths in by_type.items():
            rel_name = rel_type.replace("_", " ").title()
            relationship_summary += f"\n{rel_name}:\n"
            for path in paths[:5]:  # Limit to top 5 per type
                relationship_summary += f"  - {path.get('description', '')}\n"

    prompt = f"""You are a financial analyst analyzing company disclosures from SEC 10-K filings.

Question: {question}

Context from SEC filings and graph relationships:
{context}
{relationship_summary}

Instructions:
1. Synthesize a comprehensive answer based on the retrieved context
2. Explain which companies are directly impacted and why
3. Explain which companies are indirectly impacted through graph relationships (suppliers, customers, competitors, partners) and the mechanism of impact
4. Prioritize supplier/customer relationships as they indicate more direct indirect impact
5. Be specific about the nature of the impact (operational, financial, supply chain, etc.)
6. Cite specific companies and their relationships when relevant
7. If graph relationships show indirect exposure, explain the connection clearly

Answer:"""

    try:
        # GPT-5.2 models have different parameter requirements
        params = {
            "model": model,
            "messages": [
                {
                    "role": "system",
                    "content": "You are a financial analyst expert at analyzing SEC filings and company relationships. Provide clear, well-structured answers based on the provided context.",
                },
                {"role": "user", "content": prompt},
            ],
        }
        if model.startswith("gpt-5"):
            params["max_completion_tokens"] = 2000
            # GPT-5.2-chat-latest only supports default temperature (1), don't set it
        else:
            params["max_tokens"] = 2000
            params["temperature"] = 0.3  # Lower temperature for more factual answers

        response = client.chat.completions.create(**params)
        return response.choices[0].message.content.strip()
    except Exception as e:
        return f"Error generating answer: {e}"


def main():
    parser = argparse.ArgumentParser(description="Ask a question to the GraphRAG layer")
    parser.add_argument("question", type=str, help="Your question")
    parser.add_argument("--company", type=str, help="Focus on a specific company ticker")
    parser.add_argument(
        "--max-chunks", type=int, default=10, help="Maximum chunks to retrieve (default: 10)"
    )
    parser.add_argument(
        "--min-similarity",
        type=float,
        default=0.5,
        help="Minimum similarity threshold (default: 0.5)",
    )
    parser.add_argument(
        "--synthesize",
        action="store_true",
        default=True,
        help="Use LLM to synthesize answer (default: True)",
    )
    parser.add_argument(
        "--no-synthesize",
        dest="synthesize",
        action="store_false",
        help="Skip LLM synthesis, show raw chunks only",
    )
    parser.add_argument(
        "--model",
        type=str,
        default="gpt-5.2-chat-latest",
        help="LLM model for synthesis (default: gpt-5.2-chat-latest)",
    )

    args = parser.parse_args()

    settings = Settings()
    driver = get_neo4j_driver()

    try:
        print("=" * 80)
        print("GraphRAG Question Answering")
        print("=" * 80)
        print()
        print(f"❓ Question: {args.question}")
        if args.company:
            print(f"   Focus: {args.company}")
        print()

        # Create query embedding
        print("Creating query embedding...")
        client = get_openai_client()
        query_embedding = create_embedding(client, args.question, model="text-embedding-3-small")
        if not query_embedding:
            print("❌ Failed to create query embedding")
            return 1
        print("✓ Embedding created")
        print()

        # Retrieve answer context
        print("Searching graph for relevant information...")
        result = answer_question(
            driver,
            args.question,
            query_embedding,
            company_ticker=args.company,
            max_documents=args.max_chunks,
            database=settings.neo4j_database,
        )

        print(f"✓ Found {result['num_chunks']} relevant chunks")
        print()

        # Synthesize answer with LLM if requested
        if args.synthesize and result["chunks"]:
            print("=" * 80)
            print("SYNTHESIZING ANSWER WITH LLM...")
            print("=" * 80)
            print()
            print(f"Using model: {args.model}")
            print()

            answer = synthesize_answer(
                client,
                args.question,
                result["context"],
                result["companies"],
                result.get("traversal_paths", []),
                model=args.model,
            )

            print("=" * 80)
            print("SYNTHESIZED ANSWER:")
            print("=" * 80)
            print()
            print(answer)
            print()
            print("=" * 80)
            print()

        # Show companies mentioned
        if result["companies"]:
            print("📊 Companies Mentioned:")
            for ticker, name in result["companies"][:20]:
                print(f"   - {ticker}: {name}")
            print()

        # Show top chunks with similarity scores
        if result["chunks"]:
            print("=" * 80)
            print("TOP RELEVANT CHUNKS:")
            print("=" * 80)
            print()
            for i, chunk in enumerate(result["chunks"][:5], 1):
                print(f"Chunk {i} (similarity: {chunk.get('similarity', 0):.3f})")
                print(
                    f"  Company: {chunk.get('company_name', 'Unknown')} ({chunk.get('company_ticker', 'N/A')})"
                )
                print(f"  Section: {chunk.get('section_type', 'unknown')}")
                print(f"  Year: {chunk.get('filing_year', 'N/A')}")
                if chunk.get("source") == "graph_traversal" and chunk.get("graph_relationship"):
                    print(f"  🔗 Graph Relationship: {chunk['graph_relationship']}")
                print()
                print(f"  Text: {chunk['text'][:400]}...")
                print()
                print("-" * 80)
                print()

        # Show full context (for LLM answer generation)
        print("=" * 80)
        print("FULL CONTEXT (for LLM answer generation):")
        print("=" * 80)
        print()
        print(result["context"])
        print()

        # Show graph traversal info
        if result.get("related_companies"):
            print("=" * 80)
            print("GRAPH TRAVERSAL RESULTS:")
            print("=" * 80)
            print()
            print(
                f"Found {len(result['related_companies'])} related companies via graph relationships:"
            )
            print()
            # Group by relationship type for better readability
            # Priority order: suppliers/customers first (most impactful), then partners, then competitors
            priority_order = {
                "HAS_SUPPLIER": 1,
                "HAS_CUSTOMER": 2,
                "HAS_PARTNER": 3,
                "HAS_COMPETITOR": 4,
                "SIMILAR_DESCRIPTION": 5,
                "SIMILAR_RISK": 6,
                "SIMILAR_INDUSTRY": 7,
            }

            by_relationship = {}
            for path in result.get("traversal_paths", []):
                rel_type = path.get("relationship", "unknown")
                if rel_type not in by_relationship:
                    by_relationship[rel_type] = []
                by_relationship[rel_type].append(path)

            # Sort relationship types by priority
            sorted_rel_types = sorted(
                by_relationship.items(), key=lambda x: priority_order.get(x[0], 99)
            )

            for rel_type, paths in sorted_rel_types:
                rel_name = rel_type.replace("_", " ").title()
                print(f"  {rel_name}:")
                # Show all paths (not truncated) since we're showing pairs
                for path in paths:
                    desc = path.get("description", f"{path['name']} ({path['ticker']})")
                    print(f"    - {desc}")
                print()

        if not args.synthesize:
            print("=" * 80)
            print("💡 Tip: Use --synthesize (default) to generate an LLM answer")
            print("   from the retrieved context.")
            print("=" * 80)

    finally:
        driver.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())
