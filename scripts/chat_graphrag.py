#!/usr/bin/env python3
"""
Interactive chat interface for GraphRAG queries.

Allows multiple questions in a session with conversation history.

Example:
    python scripts/chat_graphrag.py
    python scripts/chat_graphrag.py --model gpt-4o
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
    conversation_history: list[dict],
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
        conversation_history: Previous Q&A pairs for context
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

    # Build conversation history context
    history_context = ""
    if conversation_history:
        history_context = "\n\nPrevious conversation:\n"
        for entry in conversation_history[-3:]:  # Last 3 Q&A pairs
            history_context += f"Q: {entry['question']}\n"
            history_context += f"A: {entry['answer'][:200]}...\n\n"

    prompt = f"""You are a financial analyst analyzing company disclosures from SEC 10-K filings.

Current Question: {question}
{history_context}
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
8. Reference previous conversation if relevant

Answer:"""

    try:
        messages = [
            {
                "role": "system",
                "content": "You are a financial analyst expert at analyzing SEC filings and company relationships. Provide clear, well-structured answers based on the provided context. You can reference previous questions in the conversation when relevant.",
            },
        ]

        # Add conversation history
        for entry in conversation_history[-5:]:  # Last 5 Q&A pairs
            messages.append({"role": "user", "content": entry["question"]})
            messages.append({"role": "assistant", "content": entry["answer"]})

        # Add current question
        messages.append({"role": "user", "content": prompt})

        # GPT-5.2 models have different parameter requirements
        params = {
            "model": model,
            "messages": messages,
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
    parser = argparse.ArgumentParser(description="Interactive chat interface for GraphRAG")
    parser.add_argument(
        "--model",
        type=str,
        default="gpt-5.2-chat-latest",
        help="LLM model (default: gpt-5.2-chat-latest)",
    )
    parser.add_argument(
        "--company", type=str, help="Focus on a specific company ticker for all queries"
    )
    parser.add_argument(
        "--max-chunks", type=int, default=10, help="Maximum chunks to retrieve (default: 10)"
    )

    args = parser.parse_args()

    settings = Settings()
    driver = get_neo4j_driver()
    client = get_openai_client()

    conversation_history = []

    try:
        print("=" * 80)
        print("GraphRAG Chat Interface")
        print("=" * 80)
        print()
        print(f"Model: {args.model}")
        if args.company:
            print(f"Focus: {args.company}")
        print()
        print("Type your questions below. Commands:")
        print("  /quit or /exit - Exit")
        print("  /clear - Clear conversation history")
        print("  /help - Show this help")
        print()
        print("-" * 80)
        print()

        while True:
            try:
                question = input("❓ Question: ").strip()

                if not question:
                    continue

                # Handle commands
                if question.lower() in ["/quit", "/exit", "quit", "exit"]:
                    print("\n👋 Goodbye!")
                    break

                if question.lower() in ["/clear", "clear"]:
                    conversation_history = []
                    print("✓ Conversation history cleared\n")
                    continue

                if question.lower() in ["/help", "help"]:
                    print("\nCommands:")
                    print("  /quit or /exit - Exit")
                    print("  /clear - Clear conversation history")
                    print("  /help - Show this help")
                    print()
                    continue

                print()
                print("🔍 Searching graph...")

                # Create query embedding
                query_embedding = create_embedding(client, question, model="text-embedding-3-small")
                if not query_embedding:
                    print("❌ Failed to create query embedding")
                    continue

                # Retrieve answer context
                result = answer_question(
                    driver,
                    question,
                    query_embedding,
                    company_ticker=args.company,
                    max_documents=args.max_chunks,
                    database=settings.neo4j_database,
                )

                if not result["chunks"]:
                    print("❌ No relevant information found")
                    print()
                    continue

                print(f"✓ Found {result['num_chunks']} relevant chunks")
                print("🤖 Synthesizing answer...")
                print()

                # Synthesize answer
                answer = synthesize_answer(
                    client,
                    question,
                    result["context"],
                    result["companies"],
                    result.get("traversal_paths", []),
                    conversation_history,
                    model=args.model,
                )

                print("=" * 80)
                print("ANSWER:")
                print("=" * 80)
                print()
                print(answer)
                print()
                print("=" * 80)
                print()

                # Show quick stats
                if result.get("related_companies"):
                    print(
                        f"📊 Found {len(result['related_companies'])} related companies via graph relationships"
                    )
                print()

                # Add to conversation history
                conversation_history.append(
                    {
                        "question": question,
                        "answer": answer,
                    }
                )

            except KeyboardInterrupt:
                print("\n\n👋 Goodbye!")
                break
            except EOFError:
                print("\n\n👋 Goodbye!")
                break
            except Exception as e:
                print(f"\n❌ Error: {e}")
                import traceback

                traceback.print_exc()
                print()

    finally:
        driver.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())
