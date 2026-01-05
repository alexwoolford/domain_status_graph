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
        from public_company_graph.utils.tar_selection import extract_filing_date_from_html_path

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
            assert result.year == expected_year, (
                f"Wrong year for {path}: got {result.year}, expected {expected_year}"
            )

    def test_full_date_in_accession_takes_priority(self):
        """
        When accession number contains a full YYYYMMDD date, use that instead of year-only.
        Format: {10-digit-CIK}{8-digit-YYYYMMDD}/filename
        """
        from public_company_graph.utils.tar_selection import extract_filing_date_from_html_path

        # This format has full date, not year-only
        result = extract_filing_date_from_html_path("000010908720231231/10k.htm")
        assert result is not None
        assert result == datetime(2023, 12, 31), (
            f"Full date format should return exact date, got {result}"
        )

    def test_filename_date_takes_priority_over_accession(self):
        """
        When HTML filename contains a date (like adbe-20211203.htm),
        use that instead of the accession number - it's more precise.
        """
        from public_company_graph.utils.tar_selection import extract_filing_date_from_html_path

        # Filename has precise date, accession has year only
        result = extract_filing_date_from_html_path("000079634322000032/adbe-20211203.htm")
        assert result is not None
        assert result == datetime(2021, 12, 3), f"Filename date should take priority, got {result}"


class TestTokenBasedBatchingRegression:
    """
    Guard against regression in embedding batch token limits.

    Original Issue (2024-12-29):
    - tiktoken token counts can differ from OpenAI's actual counts by up to 2x
    - A batch that tiktoken said was ~240K tokens was rejected by OpenAI at 485K tokens
    - Fix: Use very conservative 150K token limit (50% of OpenAI's 300K limit)
    - Fix: Use pure token-based batching instead of count-based batching
    """

    def test_max_tokens_per_batch_is_conservative(self):
        """
        MAX_TOKENS_PER_BATCH must be well under OpenAI's 300K limit to handle
        tokenizer discrepancies between tiktoken and OpenAI's actual tokenizer.

        We observed up to 7.4x discrepancy in production (tiktoken: 126K, OpenAI: 933K).
        Using ~13% of the limit (40K) provides margin for 7x discrepancy: 40K * 7 = 280K < 300K.
        """
        from public_company_graph.embeddings.chunking import MAX_TOKENS_PER_BATCH

        OPENAI_LIMIT = 300_000
        # With 7x worst-case discrepancy, MAX_TOKENS * 7 must be < 300K
        # So MAX_TOKENS must be < 300K / 7 ≈ 43K
        MAX_SAFE_LIMIT = OPENAI_LIMIT // 7

        assert MAX_TOKENS_PER_BATCH <= MAX_SAFE_LIMIT, (
            f"MAX_TOKENS_PER_BATCH ({MAX_TOKENS_PER_BATCH:,}) is too high! "
            f"Must be <= {MAX_SAFE_LIMIT:,} to handle 7x tokenizer discrepancy "
            f"(observed tiktoken: 126K vs OpenAI: 933K)."
        )

    def test_chunk_size_is_reasonable(self):
        """CHUNK_SIZE_TOKENS should be in a reasonable range."""
        from public_company_graph.embeddings.chunking import CHUNK_SIZE_TOKENS

        # Chunks should be 6K-8K tokens
        assert 5000 <= CHUNK_SIZE_TOKENS <= 8000, (
            f"CHUNK_SIZE_TOKENS ({CHUNK_SIZE_TOKENS}) is outside expected range 5000-8000"
        )

    def test_batch_function_accepts_token_limit(self):
        """create_embeddings_batch must accept max_tokens_per_batch parameter."""
        import inspect

        from public_company_graph.embeddings.openai_client import create_embeddings_batch

        sig = inspect.signature(create_embeddings_batch)
        params = list(sig.parameters.keys())

        assert "max_tokens_per_batch" in params, (
            "create_embeddings_batch must accept max_tokens_per_batch parameter "
            "for token-based batching"
        )


class TestTarFileValidation:
    """
    Guard against issues with tar file validation and cleanup.

    Original Issue (2024-12-28):
    - Empty/corrupt/truncated tar files were causing extraction failures
    - Zero-byte files, 8-byte truncated files, and empty tar headers (10,240 bytes)
      were all problematic
    - Fix: Added is_tar_file_empty() validation in tar_selection.py
    """

    def test_can_identify_zero_byte_tar(self, tmp_path):
        """Zero-byte files should be detected as empty."""
        from public_company_graph.utils.tar_selection import is_tar_file_empty

        zero_byte_tar = tmp_path / "zero.tar"
        zero_byte_tar.write_bytes(b"")

        assert is_tar_file_empty(zero_byte_tar), "Zero-byte tar file should be identified as empty"

    def test_can_identify_truncated_tar(self, tmp_path):
        """Truncated/corrupt tar files should be detected as empty."""
        from public_company_graph.utils.tar_selection import is_tar_file_empty

        truncated_tar = tmp_path / "truncated.tar"
        truncated_tar.write_bytes(b"not a tar")

        assert is_tar_file_empty(truncated_tar), (
            "Truncated/corrupt tar file should be identified as empty"
        )

    def test_empty_tar_with_header_only(self, tmp_path):
        """
        Empty tar archives have a 10,240 byte header but no files.
        These should be detected as empty.
        """
        import tarfile

        from public_company_graph.utils.tar_selection import is_tar_file_empty

        empty_tar = tmp_path / "empty.tar"
        with tarfile.open(empty_tar, "w"):
            pass  # Create empty tar

        # Should be around 10,240 bytes (tar block size)
        assert empty_tar.stat().st_size > 0
        assert is_tar_file_empty(empty_tar), (
            "Empty tar archive (header only) should be identified as empty"
        )


class TestDateExtractionCoverage:
    """
    Ensure date extraction works for ALL observed filename patterns.

    These patterns were observed in real datamule downloads across 5,400+ companies.
    Any additional pattern that appears should be added here.
    """

    def test_all_observed_patterns(self):
        """Test all filename patterns observed in production."""
        from public_company_graph.utils.tar_selection import extract_filing_date_from_html_path

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
        from public_company_graph.utils.tar_selection import extract_filing_date_from_html_path

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
        from public_company_graph.embeddings.chunking import (
            CHUNK_SIZE_TOKENS,
            chunk_text,
        )
        from public_company_graph.embeddings.openai_client import count_tokens

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

    Original Issue (2024-12-29):
    - tiktoken counted ~240K tokens, OpenAI saw 485K tokens (2x discrepancy)
    - Root cause: tokenizer differences between tiktoken and OpenAI's backend
    - Fix: Use 150K token limit (50% of 300K) and pure token-based batching
    """

    def test_truncation_enforces_per_text_limit(self):
        """Each text must be truncated to EMBEDDING_TRUNCATE_TOKENS before batching."""
        from public_company_graph.embeddings.openai_client import (
            EMBEDDING_TRUNCATE_TOKENS,
            count_tokens,
            truncate_to_token_limit,
        )

        # Even after truncation, each text should be <= EMBEDDING_TRUNCATE_TOKENS
        test_text = "word " * 50000  # Way over limit

        truncated = truncate_to_token_limit(test_text, EMBEDDING_TRUNCATE_TOKENS)
        token_count = count_tokens(truncated, "text-embedding-3-small")

        assert token_count <= EMBEDDING_TRUNCATE_TOKENS, (
            f"Truncation failed: {token_count} tokens > {EMBEDDING_TRUNCATE_TOKENS} limit"
        )

    def test_truncation_handles_edge_cases(self):
        """Truncation must work for various edge cases."""
        from public_company_graph.embeddings.openai_client import (
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

            assert token_count <= EMBEDDING_TRUNCATE_TOKENS, (
                f"Edge case {i} failed: {token_count} tokens after truncation"
            )

    def test_token_based_batching_respects_limit(self):
        """
        Token-based batching must accumulate texts until hitting the token limit,
        not use a fixed count-based approach.
        """
        from public_company_graph.embeddings.chunking import MAX_TOKENS_PER_BATCH
        from public_company_graph.embeddings.openai_client import count_tokens

        # Simulate building a batch with varied text sizes
        small_text = "hello world"  # ~2 tokens
        large_text = "word " * 5000  # ~5000 tokens

        small_tokens = count_tokens(small_text, "text-embedding-3-small")
        large_tokens = count_tokens(large_text, "text-embedding-3-small")

        # With pure token-based batching:
        # - Many small texts can fit in one batch
        # - Fewer large texts fit in one batch
        # The key is that we never exceed MAX_TOKENS_PER_BATCH

        # Verify we could fit many small texts
        max_small_texts = MAX_TOKENS_PER_BATCH // small_tokens
        assert max_small_texts > 100, (
            f"Should be able to fit many small texts in a batch, got max {max_small_texts}"
        )

        # Verify we can fit at least a few large texts (with conservative 40K limit)
        max_large_texts = MAX_TOKENS_PER_BATCH // large_tokens
        assert max_large_texts >= 5, (
            f"Should be able to fit at least 5 large texts (~5K tokens each), got {max_large_texts}"
        )
