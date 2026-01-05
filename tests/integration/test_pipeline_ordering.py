"""
Integration tests for pipeline ordering.

These tests ensure critical pipeline steps run in the correct order,
preventing bugs like the embedding/extraction ordering issue.
"""

from pathlib import Path

import pytest

# Skip all tests in this module if Neo4j is not available
pytestmark = pytest.mark.integration


class TestPipelineOrdering:
    """Test that pipeline scripts run in the correct order."""

    def test_embeddings_before_extraction(self):
        """
        Regression test: Ensure embeddings are created BEFORE extraction.

        This prevents the bug where extract_with_llm_verification.py
        ran before create_company_embeddings.py, causing all
        embedding_similarity values to be 1.0 (data corruption).

        Bug: If embeddings don't exist, EmbeddingSimilarityScorer defaults
        to similarity=1.0, causing all relationships to be marked HIGH
        confidence and preventing CANDIDATE relationships from being created.
        """
        pipeline_script = Path(__file__).parent.parent.parent / "scripts" / "run_all_pipelines.py"

        if not pipeline_script.exists():
            pytest.skip(f"Pipeline script not found: {pipeline_script}")

        # Read the pipeline script
        content = pipeline_script.read_text()

        # Find the line numbers for both scripts
        create_embeddings_line = None
        extract_relationships_line = None

        lines = content.splitlines()
        for i, line in enumerate(lines, 1):
            # Look for run_script calls with the script variable
            if "CREATE_COMPANY_EMBEDDINGS_SCRIPT" in line:
                # Check if this is in a run_script call (look at surrounding lines)
                context_start = max(0, i - 5)
                context_end = min(len(lines), i + 5)
                context = "\n".join(lines[context_start:context_end])
                if "run_script" in context:
                    create_embeddings_line = i
            if "EXTRACT_BUSINESS_RELATIONSHIPS_SCRIPT" in line:
                context_start = max(0, i - 5)
                context_end = min(len(lines), i + 5)
                context = "\n".join(lines[context_start:context_end])
                if "run_script" in context:
                    extract_relationships_line = i

        # Verify both scripts are present
        assert create_embeddings_line is not None, (
            "CREATE_COMPANY_EMBEDDINGS_SCRIPT not found in pipeline"
        )
        assert extract_relationships_line is not None, (
            "EXTRACT_BUSINESS_RELATIONSHIPS_SCRIPT not found in pipeline"
        )

        # CRITICAL: Embeddings must come BEFORE extraction
        assert create_embeddings_line < extract_relationships_line, (
            f"Pipeline ordering bug detected! "
            f"CREATE_COMPANY_EMBEDDINGS_SCRIPT (line {create_embeddings_line}) "
            f"must run BEFORE EXTRACT_BUSINESS_RELATIONSHIPS_SCRIPT (line {extract_relationships_line}). "
            f"If embeddings don't exist, extraction will default to similarity=1.0, "
            f"causing data corruption (all relationships marked HIGH confidence)."
        )

        # Verify the comment explains why order matters
        # Look for comment near the embeddings step (before the run_script call)
        lines = content.splitlines()
        embeddings_section = "\n".join(
            lines[max(0, create_embeddings_line - 5) : create_embeddings_line + 1]
        )
        assert "CRITICAL" in embeddings_section or "BEFORE" in embeddings_section, (
            "Missing comment explaining why embeddings must come before extraction. "
            "This helps prevent accidental reordering."
        )

        # Verify the comment near extraction mentions the dependency
        extraction_section = "\n".join(
            lines[max(0, extract_relationships_line - 10) : extract_relationships_line + 1]
        )
        assert "embedding" in extraction_section.lower(), (
            "Missing comment in extraction step explaining it requires embeddings. "
            "This helps prevent accidental reordering."
        )

    def test_enrich_identifiers_before_extraction(self):
        """
        Test that enrich_company_identifiers runs before extraction.

        Extraction requires name/ticker from SEC EDGAR for the lookup table.
        """
        pipeline_script = Path(__file__).parent.parent.parent / "scripts" / "run_all_pipelines.py"

        if not pipeline_script.exists():
            pytest.skip(f"Pipeline script not found: {pipeline_script}")

        content = pipeline_script.read_text()

        # Find line numbers
        enrich_line = None
        extract_line = None

        lines = content.splitlines()
        for i, line in enumerate(lines, 1):
            if "ENRICH_COMPANY_IDENTIFIERS_SCRIPT" in line:
                context_start = max(0, i - 5)
                context_end = min(len(lines), i + 5)
                context = "\n".join(lines[context_start:context_end])
                if "run_script" in context:
                    enrich_line = i
            if "EXTRACT_BUSINESS_RELATIONSHIPS_SCRIPT" in line:
                context_start = max(0, i - 5)
                context_end = min(len(lines), i + 5)
                context = "\n".join(lines[context_start:context_end])
                if "run_script" in context:
                    extract_line = i

        assert enrich_line is not None, "ENRICH_COMPANY_IDENTIFIERS_SCRIPT not found"
        assert extract_line is not None, "EXTRACT_BUSINESS_RELATIONSHIPS_SCRIPT not found"

        assert enrich_line < extract_line, (
            f"ENRICH_COMPANY_IDENTIFIERS_SCRIPT (line {enrich_line}) "
            f"must run BEFORE EXTRACT_BUSINESS_RELATIONSHIPS_SCRIPT (line {extract_line}). "
            f"Extraction requires name/ticker from SEC EDGAR."
        )

    def test_load_company_data_before_enrichment(self):
        """
        Test that load_company_data runs before enrichment steps.

        Company nodes must exist before they can be enriched.
        """
        pipeline_script = Path(__file__).parent.parent.parent / "scripts" / "run_all_pipelines.py"

        if not pipeline_script.exists():
            pytest.skip(f"Pipeline script not found: {pipeline_script}")

        content = pipeline_script.read_text()

        # Find line numbers
        load_line = None
        enrich_line = None

        lines = content.splitlines()
        for i, line in enumerate(lines, 1):
            if "LOAD_COMPANY_DATA_SCRIPT" in line:
                context_start = max(0, i - 5)
                context_end = min(len(lines), i + 5)
                context = "\n".join(lines[context_start:context_end])
                if "run_script" in context:
                    load_line = i
            if "ENRICH_COMPANY_IDENTIFIERS_SCRIPT" in line:
                context_start = max(0, i - 5)
                context_end = min(len(lines), i + 5)
                context = "\n".join(lines[context_start:context_end])
                if "run_script" in context:
                    enrich_line = i

        assert load_line is not None, "LOAD_COMPANY_DATA_SCRIPT not found"
        assert enrich_line is not None, "ENRICH_COMPANY_IDENTIFIERS_SCRIPT not found"

        assert load_line < enrich_line, (
            f"LOAD_COMPANY_DATA_SCRIPT (line {load_line}) "
            f"must run BEFORE ENRICH_COMPANY_IDENTIFIERS_SCRIPT (line {enrich_line}). "
            f"Company nodes must exist before enrichment."
        )
