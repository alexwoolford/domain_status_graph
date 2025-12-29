"""
Unit tests for datamule utilities.

Tests the suppress_datamule_output context manager and _is_tqdm_progress_bar function.
"""

import sys

from public_company_graph.utils.datamule import _is_tqdm_progress_bar, suppress_datamule_output


class TestIsTqdmProgressBar:
    """Tests for _is_tqdm_progress_bar function."""

    def test_progress_bar_characters(self):
        """Test detection of Unicode progress bar characters."""
        assert _is_tqdm_progress_bar("█" * 50) is True
        assert _is_tqdm_progress_bar("▉▊▋▌▍▎▏") is True
        assert _is_tqdm_progress_bar("░░▒▒▓▓") is True

    def test_percentage_and_bar(self):
        """Test detection of percentage and bar pattern."""
        assert _is_tqdm_progress_bar("100%|████████████|") is True
        assert _is_tqdm_progress_bar("50%|█████     |") is True
        assert _is_tqdm_progress_bar("0%|          |") is True

    def test_rate_information(self):
        """Test detection of rate information."""
        assert _is_tqdm_progress_bar("[00:01<00:00, 192.97it/s]") is True
        assert _is_tqdm_progress_bar("[00:00<00:00, 1000.00s/it]") is True
        assert _is_tqdm_progress_bar("it/s]") is True
        assert _is_tqdm_progress_bar("s/it]") is True

    def test_ansi_escape_sequences(self):
        """Test detection of ANSI escape sequences."""
        assert _is_tqdm_progress_bar("\x1b[32m100%\x1b[0m") is True
        assert _is_tqdm_progress_bar("\x1b[1A\x1b[2K") is True

    def test_normal_text(self):
        """Test that normal text is not detected as progress bar."""
        assert _is_tqdm_progress_bar("This is normal text") is False
        assert _is_tqdm_progress_bar("Loading submissions") is False
        assert _is_tqdm_progress_bar("Error occurred") is False
        assert _is_tqdm_progress_bar("") is False
        assert _is_tqdm_progress_bar("   ") is False

    def test_mixed_content(self):
        """Test detection in mixed content."""
        assert _is_tqdm_progress_bar("Processing: 100%|████| 5/5 [00:01<00:00]") is True
        assert _is_tqdm_progress_bar("File: example.txt 100%|████|") is True


class TestSuppressDatamuleOutput:
    """Tests for suppress_datamule_output context manager."""

    def test_stdout_stderr_redirected_to_devnull(self):
        """Test that stdout and stderr are redirected to /dev/null during context."""
        original_stdout = sys.stdout
        original_stderr = sys.stderr

        with suppress_datamule_output():
            # During context, stdout/stderr should be different (redirected to devnull)
            assert sys.stdout is not original_stdout
            assert sys.stderr is not original_stderr
            # Writing should not raise errors
            print("Test stdout")
            print("Test stderr", file=sys.stderr)

        # After context, stdout/stderr should be restored
        assert sys.stdout is original_stdout
        assert sys.stderr is original_stderr

    def test_stdout_stderr_restored_after_exception(self):
        """Test that stdout/stderr are restored even if exception occurs."""
        original_stdout = sys.stdout
        original_stderr = sys.stderr

        try:
            with suppress_datamule_output():
                raise ValueError("Test exception")
        except ValueError:
            pass  # Expected

        # stdout/stderr should still be restored
        assert sys.stdout is original_stdout
        assert sys.stderr is original_stderr

    def test_warnings_are_suppressed(self):
        """Test that Python warnings are suppressed during context."""
        import warnings

        warning_raised = False

        def warning_handler(message, category, filename, lineno, file=None, line=None):
            nonlocal warning_raised
            warning_raised = True

        old_showwarning = warnings.showwarning
        try:
            warnings.showwarning = warning_handler

            with suppress_datamule_output():
                # These specific patterns should be suppressed
                warnings.warn("extract year from something", stacklevel=2)
                warnings.warn("original filename issue", stacklevel=2)

            # Warnings should not have been raised
            assert not warning_raised
        finally:
            warnings.showwarning = old_showwarning

    def test_context_manager_returns_nothing(self):
        """Test that the context manager yields nothing (just suppresses)."""
        with suppress_datamule_output() as result:
            assert result is None

    def test_nested_context_managers_work(self):
        """Test that nested suppress_datamule_output calls work correctly."""
        original_stdout = sys.stdout

        with suppress_datamule_output():
            first_stdout = sys.stdout
            with suppress_datamule_output():
                # Inner context has its own redirection
                assert sys.stdout is not first_stdout
            # After inner context, outer context's redirection is restored
            # Note: This may or may not be the same object depending on implementation

        # After all contexts, original stdout is restored
        assert sys.stdout is original_stdout
