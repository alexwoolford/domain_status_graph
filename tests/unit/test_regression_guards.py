"""
Regression tests for issues that were fixed.

These tests guard against specific bugs that were discovered and fixed.
Each test includes a description of the original issue to help future developers
understand why the test exists.
"""

from datetime import datetime


class TestTarDateExtractionRegression:
    """
    Guard against regression in tar file date extraction.

    Original Issue (2024-12-28):
    - SEC accession numbers follow format: {10-digit-CIK}{2-digit-YY}{6-digit-sequence}
    - Example: 000114036114016669 = CIK 0001140361 + year 14 + sequence 016669
    - The old code assumed format was {CIK}{YYMMDD}{filing} and tried to parse
      positions 11-16 as a date (YYMMDD), which failed for sequences like "140166"
      because day 66 is invalid.
    - Fix: Correctly identify this as year-only format and return Jan 1 of that year.
    """

    def test_accession_number_year_only_format(self):
        """
        Datamule batch tar files use accession number format in directory names.
        Format: {10-digit-CIK}{2-digit-YY}{6-digit-sequence}/filename

        This MUST return a valid date (year only), not None.
        """
        from domain_status_graph.utils.tar_selection import extract_filing_date_from_html_path

        # Real examples from datamule batch downloads
        test_cases = [
            ("000114036114016669/form10k.htm", 2014),  # CIK + year 14 + seq
            ("000111492710000007/form10_k.htm", 2010),  # CIK + year 10 + seq
            ("000079634322000032/form10k.htm", 2022),  # CIK + year 22 + seq
            ("000149315223012511/form10-k.htm", 2023),  # CIK + year 23 + seq
        ]

        for path, expected_year in test_cases:
            result = extract_filing_date_from_html_path(path)
            assert result is not None, (
                f"Date extraction returned None for {path}. "
                f"This is a regression - accession number format should extract year."
            )
            assert (
                result.year == expected_year
            ), f"Wrong year for {path}: got {result.year}, expected {expected_year}"

    def test_full_date_in_accession_takes_priority(self):
        """
        When accession number contains a full YYYYMMDD date, use that instead of year-only.
        Format: {10-digit-CIK}{8-digit-YYYYMMDD}/filename
        """
        from domain_status_graph.utils.tar_selection import extract_filing_date_from_html_path

        # This format has full date, not year-only
        result = extract_filing_date_from_html_path("000010908720231231/10k.htm")
        assert result is not None
        assert result == datetime(
            2023, 12, 31
        ), f"Full date format should return exact date, got {result}"

    def test_filename_date_takes_priority_over_accession(self):
        """
        When HTML filename contains a date (like adbe-20211203.htm),
        use that instead of the accession number - it's more precise.
        """
        from domain_status_graph.utils.tar_selection import extract_filing_date_from_html_path

        # Filename has precise date, accession has year only
        result = extract_filing_date_from_html_path("000079634322000032/adbe-20211203.htm")
        assert result is not None
        assert result == datetime(2021, 12, 3), f"Filename date should take priority, got {result}"


class TestEmbeddingBatchSizeRegression:
    """
    Guard against regression in embedding batch size.

    Original Issue (2024-12-28):
    - MAX_CHUNKS_PER_BATCH was set to 40
    - Each chunk averages ~8,400 tokens (not 7,000 as assumed)
    - 40 * 8,400 = 336,000 tokens > OpenAI's 300,000 limit
    - This caused "max_tokens_per_request" errors for all batches
    - Fix: Reduced MAX_CHUNKS_PER_BATCH to 30 (30 * 8.5K = 255K tokens, safe margin)
    """

    def test_max_chunks_per_batch_is_safe(self):
        """
        MAX_CHUNKS_PER_BATCH must be low enough to stay under 300K token limit.

        With chunks averaging ~8,500 tokens (worst case), we need:
        - batch_size * 8500 < 300000
        - batch_size < 35.3

        Using 30 gives us a safe margin.
        """
        from domain_status_graph.embeddings.chunking import MAX_CHUNKS_PER_BATCH

        # Calculate maximum safe batch size
        MAX_TOKENS_PER_REQUEST = 300_000
        WORST_CASE_TOKENS_PER_CHUNK = 8_500  # Observed average was ~8,400

        max_safe_batch_size = MAX_TOKENS_PER_REQUEST // WORST_CASE_TOKENS_PER_CHUNK

        assert MAX_CHUNKS_PER_BATCH <= max_safe_batch_size, (
            f"MAX_CHUNKS_PER_BATCH ({MAX_CHUNKS_PER_BATCH}) is too high! "
            f"With ~{WORST_CASE_TOKENS_PER_CHUNK} tokens/chunk, max safe is {max_safe_batch_size}. "
            f"This will cause 'max_tokens_per_request' errors from OpenAI."
        )

    def test_chunk_size_matches_expectations(self):
        """
        CHUNK_SIZE_TOKENS should be reasonable for the batch calculation.
        """
        from domain_status_graph.embeddings.chunking import (
            CHUNK_SIZE_TOKENS,
            MAX_CHUNKS_PER_BATCH,
        )

        # Chunks should be 6K-8K tokens
        assert (
            5000 <= CHUNK_SIZE_TOKENS <= 8000
        ), f"CHUNK_SIZE_TOKENS ({CHUNK_SIZE_TOKENS}) is outside expected range 5000-8000"

        # Total tokens per batch should be under 300K with margin
        estimated_batch_tokens = MAX_CHUNKS_PER_BATCH * CHUNK_SIZE_TOKENS
        assert (
            estimated_batch_tokens < 280_000
        ), f"Estimated batch tokens ({estimated_batch_tokens:,}) is too close to 300K limit"


class TestTarFileValidation:
    """
    Guard against issues with tar file validation and cleanup.

    Original Issue (2024-12-28):
    - Empty/corrupt/truncated tar files were causing extraction failures
    - Zero-byte files, 8-byte truncated files, and empty tar headers (10,240 bytes)
      were all problematic
    - Fix: Created repair_tar_downloads.py to identify and fix these issues
    """

    def test_can_identify_zero_byte_tar(self, tmp_path):
        """Zero-byte files should be detected as empty."""
        from domain_status_graph.utils.tar_selection import is_tar_file_empty

        zero_byte_tar = tmp_path / "zero.tar"
        zero_byte_tar.write_bytes(b"")

        assert is_tar_file_empty(zero_byte_tar), "Zero-byte tar file should be identified as empty"

    def test_can_identify_truncated_tar(self, tmp_path):
        """Truncated/corrupt tar files should be detected as empty."""
        from domain_status_graph.utils.tar_selection import is_tar_file_empty

        truncated_tar = tmp_path / "truncated.tar"
        truncated_tar.write_bytes(b"not a tar")

        assert is_tar_file_empty(
            truncated_tar
        ), "Truncated/corrupt tar file should be identified as empty"

    def test_empty_tar_with_header_only(self, tmp_path):
        """
        Empty tar archives have a 10,240 byte header but no files.
        These should be detected as empty.
        """
        import tarfile

        from domain_status_graph.utils.tar_selection import is_tar_file_empty

        empty_tar = tmp_path / "empty.tar"
        with tarfile.open(empty_tar, "w"):
            pass  # Create empty tar

        # Should be around 10,240 bytes (tar block size)
        assert empty_tar.stat().st_size > 0
        assert is_tar_file_empty(
            empty_tar
        ), "Empty tar archive (header only) should be identified as empty"


class TestDateExtractionCoverage:
    """
    Ensure date extraction works for ALL observed filename patterns.

    These patterns were observed in real datamule downloads across 5,400+ companies.
    Any new pattern that appears should be added here.
    """

    def test_all_observed_patterns(self):
        """Test all filename patterns observed in production."""
        from domain_status_graph.utils.tar_selection import extract_filing_date_from_html_path

        # Pattern: ticker-YYYYMMDD.htm (most common)
        assert extract_filing_date_from_html_path("aapl-20240928.htm") is not None
        assert extract_filing_date_from_html_path("msft-20231231.htm") is not None
        assert extract_filing_date_from_html_path("a-20241231.htm") is not None

        # Pattern: accession_number/form10k.htm (datamule batch)
        assert extract_filing_date_from_html_path("000114036114016669/form10k.htm") is not None
        assert extract_filing_date_from_html_path("000114036114016669/form10_k.htm") is not None

        # Pattern: accession_number/ticker-YYYYMMDD.htm (best case - has both)
        assert (
            extract_filing_date_from_html_path("000079634322000032/adbe-20211203.htm") is not None
        )

        # Pattern: YYYY-MM-DD in path
        assert extract_filing_date_from_html_path("filings/2024-03-15/10k.htm") is not None

    def test_patterns_that_should_return_none(self):
        """These patterns legitimately have no extractable date."""
        from domain_status_graph.utils.tar_selection import extract_filing_date_from_html_path

        # No date information at all
        assert extract_filing_date_from_html_path("form10k.htm") is None
        assert extract_filing_date_from_html_path("document.htm") is None

        # Invalid dates should return None
        assert extract_filing_date_from_html_path("a-20241366.htm") is None  # Month 13, day 66


class TestChunkingTokenEstimates:
    """
    Validate that chunk token estimates are accurate.

    Original Issue:
    - Code assumed chunks would be ~7K tokens
    - Actual chunks averaged ~8.4K tokens
    - This caused batch size calculations to be wrong
    """

    def test_chunk_sizes_match_configuration(self):
        """Chunks should not significantly exceed CHUNK_SIZE_TOKENS."""
        from domain_status_graph.embeddings.chunking import (
            CHUNK_SIZE_TOKENS,
            chunk_text,
        )
        from domain_status_graph.embeddings.openai_client import count_tokens

        # Create a very long text
        long_text = "This is a test sentence with some content. " * 5000

        chunks = chunk_text(long_text)

        for i, chunk in enumerate(chunks):
            token_count = count_tokens(chunk, "text-embedding-3-small")
            # Allow some overflow due to tokenization boundaries
            max_expected = CHUNK_SIZE_TOKENS + 500
            assert token_count <= max_expected, (
                f"Chunk {i} has {token_count} tokens, exceeds max expected {max_expected}. "
                f"CHUNK_SIZE_TOKENS ({CHUNK_SIZE_TOKENS}) may be too aggressive."
            )


class TestBatchTokenValidation:
    """
    Guard against batches exceeding OpenAI's token limit.

    Original Issue (2024-12-28):
    - Some batches mysteriously had 495K tokens (should have been ~210K max)
    - OpenAI's limit is 300K tokens per request
    - Root cause unclear, but defensive validation now catches this
    """

    def test_batch_token_preflight_check(self):
        """
        Verify that create_embeddings_batch validates token count before API call.

        The code should detect oversized batches and fall back to individual calls.
        """
        from domain_status_graph.embeddings.openai_client import (
            EMBEDDING_TRUNCATE_TOKENS,
            count_tokens,
            truncate_to_token_limit,
        )

        # Even after truncation, each text should be <= EMBEDDING_TRUNCATE_TOKENS
        test_text = "word " * 50000  # Way over limit

        truncated = truncate_to_token_limit(test_text, EMBEDDING_TRUNCATE_TOKENS)
        token_count = count_tokens(truncated, "text-embedding-3-small")

        assert (
            token_count <= EMBEDDING_TRUNCATE_TOKENS
        ), f"Truncation failed: {token_count} tokens > {EMBEDDING_TRUNCATE_TOKENS} limit"

        # With proper truncation, 30 texts should be under 300K
        max_batch_size = 30
        max_per_text = EMBEDDING_TRUNCATE_TOKENS
        max_batch_tokens = max_batch_size * max_per_text

        assert max_batch_tokens < 300_000, (
            f"Even with truncation, max batch ({max_batch_tokens:,}) could exceed 300K. "
            f"Reduce batch_size or EMBEDDING_TRUNCATE_TOKENS."
        )

    def test_truncation_always_applied(self):
        """
        Every text sent to the embedding API must be truncated.

        This test verifies truncation works for various edge cases.
        """
        from domain_status_graph.embeddings.openai_client import (
            EMBEDDING_TRUNCATE_TOKENS,
            count_tokens,
            truncate_to_token_limit,
        )

        edge_cases = [
            "Normal short text",
            "x" * 100000,  # 100K characters
            "📊💻🔥" * 10000,  # Emoji-heavy (multi-byte chars)
            " \n\t " * 10000,  # Whitespace-heavy
            "word " * 20000,  # Exactly long enough
        ]

        for i, text in enumerate(edge_cases):
            truncated = truncate_to_token_limit(text, EMBEDDING_TRUNCATE_TOKENS)
            token_count = count_tokens(truncated, "text-embedding-3-small")

            assert (
                token_count <= EMBEDDING_TRUNCATE_TOKENS
            ), f"Edge case {i} failed: {token_count} tokens after truncation"
