#!/usr/bin/env python3
"""
Create GraphRAG layer from existing company data.

This script:
1. Loads company business descriptions and risk factors from Neo4j
2. Chunks them into Document nodes
3. Creates embeddings for documents
4. Links documents to companies

This layers GraphRAG capabilities on top of existing structured relationships.
"""

import argparse
import logging
import sys
from pathlib import Path

from dotenv import load_dotenv

from public_company_graph.cache import get_cache
from public_company_graph.cli import (
    add_execute_argument,
    get_driver_and_database,
    setup_logging,
    verify_neo4j_connection,
)
from public_company_graph.config import get_data_dir
from public_company_graph.embeddings.openai_client import get_openai_client
from public_company_graph.graphrag.chunking import chunk_filing_sections
from public_company_graph.graphrag.documents import (
    create_chunk_embeddings,
    create_documents_and_chunks,
    link_documents_to_companies,
)
from public_company_graph.graphrag.filing_text import (
    extract_full_text_with_datamule,
    find_10k_file_for_company,
)
from public_company_graph.neo4j.constraints import create_document_constraints
from public_company_graph.utils.security import validate_path_within_base

# Load environment
load_dotenv(Path(__file__).parent.parent / ".env")

logger = logging.getLogger(__name__)


def load_company_data(driver, database: str | None = None, limit: int | None = None):
    """
    Load company data from Neo4j (for finding 10-K files).

    Args:
        driver: Neo4j driver
        database: Database name
        limit: Optional limit for testing

    Returns:
        List of company data dicts
    """
    query = """
    MATCH (c:Company)
    RETURN c.cik AS cik,
           c.ticker AS ticker,
           c.name AS name,
           c.filing_year AS filing_year
    ORDER BY c.cik
    """
    if limit:
        query += f" LIMIT {limit}"

    with driver.session(database=database) as session:
        result = session.run(query)
        return [dict(record) for record in result]


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Create GraphRAG layer from existing company data")
    add_execute_argument(parser)
    parser.add_argument(
        "--limit",
        type=int,
        help="Limit number of companies to process (for testing)",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=1000,
        help="Chunk size in characters (default: 1000)",
    )
    parser.add_argument(
        "--chunk-overlap",
        type=int,
        default=200,
        help="Chunk overlap in characters (default: 200)",
    )
    parser.add_argument(
        "--skip-embeddings",
        action="store_true",
        help="Skip embedding creation (faster, but documents won't be searchable)",
    )
    parser.add_argument(
        "--skip-existing",
        action="store_true",
        help="Skip companies that already have documents (faster for incremental updates)",
    )
    parser.add_argument(
        "--skip-documents",
        action="store_true",
        help="Skip document extraction/chunking if Document nodes already exist (only create embeddings)",
    )

    args = parser.parse_args()

    script_logger = setup_logging("create_graphrag_layer", execute=args.execute)
    driver, database = get_driver_and_database(script_logger)

    try:
        if not verify_neo4j_connection(driver, database, script_logger):
            sys.exit(1)

        script_logger.info("=" * 80)
        script_logger.info("Creating GraphRAG Layer")
        script_logger.info("=" * 80)
        script_logger.info("")

        # Ensure Document constraints exist (critical for MERGE performance)
        # This must be done before creating any Document nodes
        script_logger.info("Ensuring Document constraints exist...")
        create_document_constraints(driver, database=database, logger=script_logger)
        script_logger.info("")

        # Check if documents already exist (handle case where label doesn't exist yet)
        with driver.session(database=database) as session:
            # Use OPTIONAL MATCH to avoid warning if Document label doesn't exist
            result = session.run("""
                CALL db.labels() YIELD label
                WHERE label = 'Document'
                RETURN count(*) as label_exists
            """)
            label_exists = result.single()["label_exists"] > 0

            if label_exists:
                existing_count = session.run(
                    "MATCH (d:Document) RETURN count(d) as count"
                ).single()["count"]
                if existing_count > 0:
                    script_logger.info(f"⚠️  Found {existing_count:,} existing Document nodes")
                    script_logger.info("   Re-running will update existing documents (idempotent)")
                    script_logger.info("")
            else:
                script_logger.info("No existing Document nodes (first run)")
                script_logger.info("")

        # Load company data
        script_logger.info("Loading company data from Neo4j...")
        companies = load_company_data(driver, database, limit=args.limit)
        script_logger.info(f"Found {len(companies)} companies")
        script_logger.info("")

        # Find 10-K files
        filings_dir = get_data_dir() / "10k_filings"
        script_logger.info(f"Looking for 10-K files in: {filings_dir}")

        companies_with_files = []
        for company in companies:
            cik = company["cik"]
            file_path = find_10k_file_for_company(cik, filings_dir)
            if file_path:
                company["file_path"] = file_path
                companies_with_files.append(company)

        script_logger.info(
            f"Found {len(companies_with_files)}/{len(companies)} companies with 10-K files"
        )
        script_logger.info("")

        if not companies_with_files:
            script_logger.error("No 10-K files found! Run download_10k_filings.py first.")
            sys.exit(1)

        # Estimate costs (sample a few files to estimate)
        script_logger.info("Estimating costs (sampling files)...")
        sample_size = min(5, len(companies_with_files))
        total_chars_sample = 0
        for company in companies_with_files[:sample_size]:
            # Validate file path is within filings_dir to prevent path traversal
            file_path = company["file_path"]
            if not validate_path_within_base(file_path, filings_dir, script_logger):
                continue
            text = extract_full_text_with_datamule(file_path, company["cik"], base_dir=filings_dir)
            if text:
                total_chars_sample += len(text)

        if total_chars_sample > 0:
            avg_chars_per_company = total_chars_sample / sample_size
            estimated_total_chars = avg_chars_per_company * len(companies_with_files)
            chars_per_chunk = args.chunk_size - args.chunk_overlap
            estimated_chunks = (
                int(estimated_total_chars / chars_per_chunk) if chars_per_chunk > 0 else 0
            )
            estimated_tokens = estimated_total_chars / 4  # Rough: 1 token ≈ 4 chars
            estimated_cost = (estimated_tokens / 1_000_000) * 0.02  # $0.02 per 1M tokens

            script_logger.info("Estimates (based on sample):")
            script_logger.info(f"  Avg chars per filing: {avg_chars_per_company:,.0f}")
            script_logger.info(f"  Estimated total characters: {estimated_total_chars:,.0f}")
            script_logger.info(f"  Estimated chunks: {estimated_chunks:,}")
            script_logger.info(f"  Estimated tokens: {estimated_tokens:,.0f}")
            if not args.skip_embeddings:
                # Show cost with appropriate precision
                if estimated_cost < 0.01:
                    script_logger.info(
                        f"  Estimated API cost: ${estimated_cost:.4f} (~${estimated_cost * 1000:.2f} per 1K companies)"
                    )
                else:
                    script_logger.info(f"  Estimated API cost: ${estimated_cost:.2f}")
            script_logger.info("")

        if not args.execute:
            script_logger.info("[DRY RUN] Would process companies and create Document nodes")
            script_logger.info(f"  Chunk size: {args.chunk_size} chars")
            script_logger.info(f"  Chunk overlap: {args.chunk_overlap} chars")
            if args.skip_embeddings:
                script_logger.info("  Would skip embedding creation")
            script_logger.info("")
            script_logger.info("💡 Tip: Use --limit 10 to test on a small subset first")
            return

        # Check if we should skip document creation entirely
        doc_count = 0
        if args.skip_documents:
            with driver.session(database=database) as session:
                result = session.run("MATCH (d:Document) RETURN count(d) AS count")
                doc_count = result.single()["count"]
                if doc_count > 0:
                    script_logger.info(f"✓ Found {doc_count:,} existing Document nodes")
                    script_logger.info("Skipping document extraction/chunking (--skip-documents)")
                    script_logger.info("")
                else:
                    script_logger.warning("--skip-documents specified but no Document nodes found!")
                    script_logger.warning("Proceeding with document creation...")
                    script_logger.info("")

        # Check existing documents if skip-existing
        existing_ciks = set()
        if args.skip_existing:
            with driver.session(database=database) as session:
                result = session.run("""
                    MATCH (c:Company)-[:HAS]->(d:Document)
                    RETURN DISTINCT c.cik AS cik
                """)
                existing_ciks = {record["cik"] for record in result}
                if existing_ciks:
                    script_logger.info(
                        f"Skipping {len(existing_ciks)} companies with existing documents"
                    )

        # Extract and chunk full 10-K text (skip if --skip-documents and nodes exist)
        if not (args.skip_documents and doc_count > 0):
            script_logger.info("Extracting full text from 10-K files and chunking...")
            all_chunks = []
            skipped = 0
            failed = 0

            # Progress logging (every 10 for small runs, every 100 for large)
            log_interval = 10 if len(companies_with_files) <= 100 else 100
            last_logged = -1  # Track last logged index to avoid duplicates

            for i, company in enumerate(companies_with_files):
                if args.skip_existing and company["cik"] in existing_ciks:
                    skipped += 1
                    continue

                # Progress logging (skip first company since header already shows we're starting)
                # Log every log_interval companies
                if (i + 1) % log_interval == 0 and i != last_logged:
                    script_logger.info(
                        f"  Processing {i + 1}/{len(companies_with_files)}: {company.get('ticker', company['cik'])}..."
                    )
                    last_logged = i

                # Extract full text from HTML file
                file_path = company["file_path"]
                # Validate file path is within filings_dir to prevent path traversal
                if not validate_path_within_base(file_path, filings_dir, script_logger):
                    failed += 1
                    continue
                script_logger.debug(f"Extracting text from {file_path.name}...")
                full_text = extract_full_text_with_datamule(
                    file_path, company["cik"], base_dir=filings_dir
                )
                script_logger.debug(f"Extracted {len(full_text) if full_text else 0} chars")

                if not full_text:
                    failed += 1
                    script_logger.warning(
                        f"Failed to extract text from {file_path.name} (CIK: {company['cik']})"
                    )
                    continue

                # Chunk the full filing text
                script_logger.debug("Chunking text...")
                chunks = chunk_filing_sections(
                    sections={"full_filing": full_text},  # Single section with full text
                    company_cik=company["cik"],
                    company_ticker=company.get("ticker"),
                    company_name=company.get("name"),
                    filing_year=company.get("filing_year"),
                    chunk_size=args.chunk_size,
                    chunk_overlap=args.chunk_overlap,
                )
                script_logger.debug(f"Created {len(chunks)} chunks")
                all_chunks.extend(chunks)

                # Log progress every 100 companies (less verbose)
                if (i + 1) % 100 == 0:
                    script_logger.info(
                        f"  Collected chunks from {i + 1}/{len(companies_with_files)} companies ({len(all_chunks):,} total chunks)"
                    )

            # Summary after loop
            if skipped > 0:
                script_logger.info(f"Skipped {skipped} companies with existing documents")
            if failed > 0:
                script_logger.warning(f"Failed to extract text from {failed} companies")

            script_logger.info(f"Created {len(all_chunks):,} document chunks total")
            script_logger.info("")

            # Create Document nodes (filings) and Chunk nodes (text pieces) - ONCE after all chunks collected
            script_logger.info("Creating Document and Chunk nodes...")
            docs_created, chunks_created = create_documents_and_chunks(
                driver, all_chunks, database=database, execute=args.execute
            )
            script_logger.info(
                f"✓ Created {docs_created} Document nodes and {chunks_created} Chunk nodes"
            )
            script_logger.info("")

            # Link documents to companies - ONCE after all nodes created
            script_logger.info("Linking documents to companies...")
            linked = link_documents_to_companies(driver, database=database, execute=args.execute)
            script_logger.info(f"✓ Linked {linked} Company-Document relationships")
            script_logger.info("")
        else:
            script_logger.info("Skipping document creation (--skip-documents)")
            script_logger.info("")

        # Create embeddings for chunks
        if not args.skip_embeddings:
            script_logger.info("Creating chunk embeddings...")
            cache = get_cache()
            openai_client = get_openai_client()

            processed, created_emb, cached = create_chunk_embeddings(
                driver,
                cache,
                openai_client,
                database=database,
                execute=args.execute,
                log=script_logger,
            )
            script_logger.info(
                f"✓ Embeddings: {processed} processed, {created_emb} created, {cached} cached"
            )
        else:
            script_logger.info("Skipping embedding creation (--skip-embeddings)")

        script_logger.info("")
        script_logger.info("=" * 80)
        script_logger.info("✓ GraphRAG layer creation complete!")
        script_logger.info("=" * 80)
        script_logger.info("")
        script_logger.info("Next steps:")
        script_logger.info("  - Use search_documents() for semantic search")
        script_logger.info("  - Use search_with_graph_context() for graph-aware search")
        script_logger.info("  - Use answer_question() for Q&A retrieval")

    finally:
        driver.close()


if __name__ == "__main__":
    main()
