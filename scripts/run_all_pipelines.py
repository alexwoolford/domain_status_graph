#!/usr/bin/env python3
"""
Orchestration script to run all data pipelines in the correct order.

This script recreates the graph from scratch in the correct order:
1. Bootstrap Graph: Load Domain and Technology nodes from SQLite
2. Load Company Data: Collect domains, create embeddings, load Company nodes
3. Compute GDS Features: Technology adoption, affinity, and company similarity

All steps run in sequence to ensure the graph is complete and correct.

Usage:
    python scripts/run_all_pipelines.py          # Dry-run (plan only)
    python scripts/run_all_pipelines.py --execute  # Actually run all pipelines
"""

import argparse
import subprocess
import sys
import time
from pathlib import Path

from public_company_graph.cli import setup_logging

# Script paths
SCRIPT_DIR = Path(__file__).parent
BOOTSTRAP_SCRIPT = SCRIPT_DIR / "bootstrap_graph.py"
COMPUTE_GDS_SCRIPT = SCRIPT_DIR / "compute_gds_features.py"
DOWNLOAD_10K_SCRIPT = SCRIPT_DIR / "download_10k_filings.py"
PARSE_10K_SCRIPT = SCRIPT_DIR / "parse_10k_filings.py"
# Note: collect_domains.py is not used in 10-K first pipeline (website comes from 10-K)
CREATE_COMPANY_EMBEDDINGS_SCRIPT = SCRIPT_DIR / "create_company_embeddings.py"
LOAD_COMPANY_DATA_SCRIPT = SCRIPT_DIR / "load_company_data.py"
ENRICH_COMPANY_IDENTIFIERS_SCRIPT = SCRIPT_DIR / "enrich_company_identifiers.py"
ENRICH_COMPANY_PROPERTIES_SCRIPT = SCRIPT_DIR / "enrich_company_properties.py"
COMPUTE_COMPANY_SIMILARITY_SCRIPT = SCRIPT_DIR / "compute_company_similarity.py"
CREATE_DOMAIN_EMBEDDINGS_SCRIPT = SCRIPT_DIR / "create_domain_embeddings.py"
COMPUTE_DOMAIN_SIMILARITY_SCRIPT = SCRIPT_DIR / "compute_domain_similarity.py"
COMPUTE_KEYWORD_SIMILARITY_SCRIPT = SCRIPT_DIR / "compute_keyword_similarity.py"
COMPUTE_COMPANY_SIMILARITY_VIA_DOMAINS_SCRIPT = (
    SCRIPT_DIR / "compute_company_similarity_via_domains.py"
)
EXTRACT_BUSINESS_RELATIONSHIPS_SCRIPT = SCRIPT_DIR / "extract_with_llm_verification.py"
CREATE_RISK_SIMILARITY_SCRIPT = SCRIPT_DIR / "create_risk_similarity_graph.py"


def run_script(
    script_path: Path,
    execute: bool = False,
    description: str = "",
    extra_args: list = None,
    logger=None,
):
    """Run a script and return success status."""
    if logger is None:
        import logging

        logger = logging.getLogger(__name__)

    if not script_path.exists():
        logger.error(f"Script not found: {script_path}")
        return False

    logger.info("")
    logger.info("=" * 70)
    logger.info(f"{description}")
    logger.info("=" * 70)
    logger.info(f"Running: {script_path.name}")

    cmd = [sys.executable, str(script_path)]
    if execute:
        cmd.append("--execute")
    if extra_args:
        cmd.extend(extra_args)

    start_time = time.time()
    try:
        result = subprocess.run(cmd, check=True, capture_output=False)
        elapsed = time.time() - start_time
        if result.returncode == 0:
            logger.info(f"✓ {script_path.name} completed successfully ({elapsed:.1f}s)")
            return True
        else:
            logger.error(f"✗ {script_path.name} failed with return code {result.returncode}")
            return False
    except subprocess.CalledProcessError as e:
        elapsed = time.time() - start_time
        logger.error(f"✗ {script_path.name} failed after {elapsed:.1f}s: {e}")
        return False
    except KeyboardInterrupt:
        logger.warning("Interrupted by user")
        return False


def main():
    """Run main orchestration function."""
    parser = argparse.ArgumentParser(description="Run all data pipelines in the correct order")
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Actually execute the pipelines (default is dry-run)",
    )
    parser.add_argument(
        "--fast",
        action="store_true",
        help="Fast mode: skip uncached companies in collect_domains.py",
    )
    args = parser.parse_args()

    # Set up logging - always use timestamped logs for the orchestrator
    logger = setup_logging("full_pipeline", execute=args.execute)

    if not args.execute:
        logger.info("=" * 70)
        logger.info("PIPELINE ORCHESTRATION PLAN (Dry Run)")
        logger.info("=" * 70)
        logger.info("")
        logger.info("This script will recreate the graph from scratch:")
        logger.info("")
        logger.info("Step 1: Bootstrap Graph")
        logger.info("  - bootstrap_graph.py - Load Domain and Technology nodes from SQLite")
        logger.info("")
        logger.info("Step 2: Download & Parse 10-K Filings (Start of pipeline)")
        logger.info("  - download_10k_filings.py - Download most recent 10-K per company")
        logger.info("  - parse_10k_filings.py - Extract websites, business descriptions")
        logger.info("")
        logger.info("Step 3: Load Company Data")
        logger.info("  - collect_domains.py - Collect company domains (fallback if 10-K missing)")
        logger.info(
            "  - create_company_embeddings.py - Create embeddings for "
            "Company descriptions (uses 10-K if available)"
        )
        logger.info("  - load_company_data.py - Load Company nodes and HAS_DOMAIN relationships")
        logger.info(
            "  - enrich_company_identifiers.py - Add name/ticker from SEC EDGAR (required!)"
        )
        logger.info("  - enrich_company_properties.py - Enrich Company nodes with properties")
        logger.info("  - compute_company_similarity.py - Create SIMILAR_INDUSTRY and SIMILAR_SIZE")
        logger.info(
            "  - extract_with_llm_verification.py - Extract business relationships from 10-K filings"
        )
        logger.info("    (HAS_COMPETITOR, HAS_CUSTOMER, HAS_SUPPLIER, HAS_PARTNER)")
        logger.info("    Uses embedding similarity + LLM verification for high precision")
        logger.info(
            "  - create_risk_similarity_graph.py - Create risk factor embeddings & SIMILAR_RISK"
        )
        logger.info("")
        logger.info("Step 4: Domain Embeddings & Similarity")
        logger.info("  - create_domain_embeddings.py - Create embeddings for Domain descriptions")
        logger.info(
            "  - compute_domain_similarity.py - Compute Domain-Domain description similarity"
        )
        logger.info("  - compute_keyword_similarity.py - Compute Domain-Domain keyword similarity")
        logger.info(
            "  - compute_company_similarity_via_domains.py - "
            "Create Company-Company edges from Domain similarity"
        )
        logger.info("")
        logger.info("Step 5: Compute GDS Features")
        logger.info("  - compute_gds_features.py - Compute all features:")
        logger.info("    * Technology adoption predictions")
        logger.info("    * Technology affinity/bundling")
        logger.info("    * Company description similarity")
        logger.info("")
        logger.info("=" * 70)
        logger.info("To execute, run: python scripts/run_all_pipelines.py --execute")
        logger.info("=" * 70)
        return

    # Execute mode
    pipeline_start = time.time()
    logger.info("=" * 70)
    logger.info("RUNNING ALL PIPELINES")
    logger.info("=" * 70)
    logger.info("")

    # Step 1: Bootstrap Graph (Domain + Technology nodes)
    logger.info("")
    logger.info("=" * 70)
    logger.info("STEP 1: Bootstrap Graph")
    logger.info("=" * 70)

    if not run_script(
        BOOTSTRAP_SCRIPT,
        execute=True,
        description="Loading Domain and Technology nodes from SQLite",
        logger=logger,
    ):
        logger.error("Failed at bootstrap step")
        return

    # Step 2: Download & Parse 10-K Filings (NEW - Start of pipeline)
    logger.info("")
    logger.info("=" * 70)
    logger.info("STEP 2: Download & Parse 10-K Filings")
    logger.info("=" * 70)
    logger.info("This is the START of the pipeline - everything cascades from 10-Ks:")
    logger.info("  - 10-Ks → Company websites → Domain collection")
    logger.info("  - 10-Ks → Business descriptions → Company embeddings")
    logger.info("  - 10-Ks → Competitor mentions → Direct competitor relationships")
    logger.info("")

    # Check if 10-Ks already downloaded
    from public_company_graph.cache import get_cache

    cache = get_cache()
    cached_10ks = cache.count("10k_extracted")

    if cached_10ks == 0 or not args.fast:
        # Download 10-Ks (pre-filter is enabled by default to save credits)
        if not run_script(
            DOWNLOAD_10K_SCRIPT,
            execute=True,
            description="Step 2.1: Download 10-K Filings",
            logger=logger,
        ):
            logger.error("Pipeline failed at download_10k_filings step")
            return

        # Parse 10-Ks
        if not run_script(
            PARSE_10K_SCRIPT,
            execute=True,
            description="Step 2.2: Parse 10-K Filings (extract websites, descriptions)",
            logger=logger,
        ):
            logger.error("Pipeline failed at parse_10k_filings step")
            return
    else:
        logger.info(f"✓ {cached_10ks} 10-Ks already parsed (fast mode: skipping)")
        if args.fast:
            logger.info("  Use without --fast to re-download/parse 10-Ks")

    # Step 3: Company Data (10-K first approach)
    # Website and business description come directly from 10-K filings
    # No external API calls needed (collect_domains.py is skipped)
    logger.info("")
    logger.info("=" * 70)
    logger.info("STEP 3: Load Company Data (from 10-K filings)")
    logger.info("=" * 70)

    from public_company_graph.cache import get_cache

    cache = get_cache()
    ten_k_count = cache.count("10k_extracted")
    logger.info(f"  Found {ten_k_count} companies with 10-K data")

    # Load Company nodes (uses 10k_extracted cache directly)
    if not run_script(
        LOAD_COMPANY_DATA_SCRIPT,
        execute=True,
        description="Step 2.2: Loading Company nodes and HAS_DOMAIN relationships",
        logger=logger,
    ):
        logger.error("Failed at load_company_data step")
        return

    # CRITICAL: Enrich Company nodes with name/ticker from SEC EDGAR
    # This is required for business relationship extraction (lookup table needs name/ticker)
    if not run_script(
        ENRICH_COMPANY_IDENTIFIERS_SCRIPT,
        execute=True,
        description="Step 2.3: Enrich Company Identifiers (name/ticker from SEC EDGAR)",
        logger=logger,
    ):
        logger.error("Failed at enrich_company_identifiers step")
        return

    # Enrich Company nodes with properties (SEC, Yahoo Finance, etc.)
    if not run_script(
        ENRICH_COMPANY_PROPERTIES_SCRIPT,
        execute=True,
        description="Step 2.4: Enrich Company Properties (Industry, Size, etc.)",
        logger=logger,
    ):
        logger.error("Failed at enrich_company_properties step")
        return

    # Compute company similarity relationships
    if not run_script(
        COMPUTE_COMPANY_SIMILARITY_SCRIPT,
        execute=True,
        description="Step 2.5: Compute Company Similarity (Industry & Size)",
        logger=logger,
    ):
        logger.error("Failed at compute_company_similarity step")
        return

    # Extract business relationships from 10-K filings (CompanyKG edge types)
    # This extracts: HAS_COMPETITOR, HAS_CUSTOMER, HAS_SUPPLIER, HAS_PARTNER
    # Uses embedding similarity + LLM verification for high precision
    # NOTE: Requires name/ticker from enrich_company_identifiers step!
    if not run_script(
        EXTRACT_BUSINESS_RELATIONSHIPS_SCRIPT,
        execute=True,
        description="Step 2.6: Extract Business Relationships from 10-K Filings",
        extra_args=["--clean"],  # Start fresh for reproducibility
        logger=logger,
    ):
        logger.error("Failed at extract_business_relationships step")
        return

    # Then create embeddings for the Company nodes
    if not run_script(
        CREATE_COMPANY_EMBEDDINGS_SCRIPT,
        execute=True,
        description="Step 2.7: Create Company Description Embeddings",
        logger=logger,
    ):
        logger.error("Pipeline 2 failed at create_embeddings step")
        return

    # Create risk factor embeddings and SIMILAR_RISK relationships
    if not run_script(
        CREATE_RISK_SIMILARITY_SCRIPT,
        execute=True,
        description="Step 2.8: Create Risk Factor Embeddings and SIMILAR_RISK Relationships",
        logger=logger,
    ):
        logger.error("Pipeline failed at create_risk_similarity step")
        return

    # Step 4: Domain Embeddings
    logger.info("")
    logger.info("=" * 70)
    logger.info("STEP 4: Domain Embeddings")
    logger.info("=" * 70)

    # Always run create_domain_embeddings (it checks cache internally)
    if not run_script(
        CREATE_DOMAIN_EMBEDDINGS_SCRIPT,
        execute=True,
        description="Step 3.1: Create Domain Description Embeddings",
        logger=logger,
    ):
        logger.error("Failed at create_domain_embeddings step")
        return

    if not run_script(
        COMPUTE_DOMAIN_SIMILARITY_SCRIPT,
        execute=True,
        description="Step 3.2: Compute Domain-Domain Description Similarity",
        logger=logger,
    ):
        logger.error("Failed at compute_domain_similarity step")
        return

    if not run_script(
        COMPUTE_KEYWORD_SIMILARITY_SCRIPT,
        execute=True,
        description="Step 3.3: Compute Domain-Domain Keyword Similarity",
        logger=logger,
    ):
        logger.error("Failed at compute_keyword_similarity step")
        return

    # Create company similarity from domain similarity
    if not run_script(
        COMPUTE_COMPANY_SIMILARITY_VIA_DOMAINS_SCRIPT,
        execute=True,
        description="Step 3.4: Create Company-Company edges from Domain similarity",
        logger=logger,
    ):
        logger.error("Failed at compute_company_similarity_via_domains step")
        return

    # Step 5: Compute all GDS features (tech + company similarity in one pass)
    logger.info("")
    logger.info("=" * 70)
    logger.info("STEP 5: Compute GDS Features")
    logger.info("=" * 70)
    logger.info("Computing all GDS features: Technology adoption, affinity, and company similarity")

    if not run_script(
        COMPUTE_GDS_SCRIPT,
        execute=True,
        description="Computing all GDS features (tech adoption, affinity, company similarity)",
        logger=logger,
    ):
        logger.error("Failed at GDS computation step")
        return

    # Summary
    total_elapsed = time.time() - pipeline_start
    minutes = int(total_elapsed // 60)
    seconds = int(total_elapsed % 60)

    logger.info("")
    logger.info("=" * 70)
    logger.info("ALL PIPELINES COMPLETE!")
    logger.info("=" * 70)
    logger.info(f"Total time: {minutes}m {seconds}s")
    logger.info("")
    logger.info("Graph is now ready for queries with:")
    logger.info("  ✓ Domain nodes with title/keywords/description metadata")
    logger.info("  ✓ Domain nodes with description embeddings")
    logger.info("  ✓ Technology nodes")
    logger.info("  ✓ USES relationships (Domain → Technology)")
    logger.info("  ✓ LIKELY_TO_ADOPT relationships (Domain → Technology)")
    logger.info("  ✓ CO_OCCURS_WITH relationships (Technology → Technology)")
    logger.info("  ✓ SIMILAR_DESCRIPTION relationships (Domain → Domain)")
    logger.info("  ✓ SIMILAR_KEYWORD relationships (Domain → Domain)")
    logger.info("  ✓ Company nodes with description embeddings")
    logger.info("  ✓ HAS_DOMAIN relationships (Company → Domain)")
    logger.info("  ✓ HAS_COMPETITOR relationships (Company → Company, from 10-K filings)")
    logger.info("  ✓ HAS_CUSTOMER relationships (Company → Company, from 10-K filings)")
    logger.info("  ✓ HAS_SUPPLIER relationships (Company → Company, from 10-K filings)")
    logger.info("  ✓ HAS_PARTNER relationships (Company → Company, from 10-K filings)")
    logger.info("  ✓ SIMILAR_DESCRIPTION relationships (Company → Company)")
    logger.info("  ✓ SIMILAR_KEYWORD relationships (Company → Company, from domains)")
    logger.info("  ✓ SIMILAR_RISK relationships (Company → Company, risk factor similarity)")
    logger.info("")


if __name__ == "__main__":
    main()
