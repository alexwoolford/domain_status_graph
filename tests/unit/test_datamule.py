"""
Unit tests for datamule utilities.

Tests the suppress_datamule_output context manager and _is_tqdm_progress_bar function.
"""

import io
import logging
import os
from unittest.mock import MagicMock, patch

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

    def test_environment_variable_set_and_restored(self):
        """Test that TQDM_DISABLE is set and restored correctly."""
        # Save original value
        original_value = os.environ.get("TQDM_DISABLE")

        try:
            # Remove if exists
            if "TQDM_DISABLE" in os.environ:
                del os.environ["TQDM_DISABLE"]

            # Test that it's set during context
            with suppress_datamule_output():
                assert os.environ.get("TQDM_DISABLE") == "1"

            # Test that it's removed after context
            assert "TQDM_DISABLE" not in os.environ

            # Test restoration of existing value
            os.environ["TQDM_DISABLE"] = "0"
            with suppress_datamule_output():
                assert os.environ.get("TQDM_DISABLE") == "1"
            assert os.environ.get("TQDM_DISABLE") == "0"
        finally:
            # Restore original value
            if original_value is None:
                os.environ.pop("TQDM_DISABLE", None)
            else:
                os.environ["TQDM_DISABLE"] = original_value

    def test_stdout_stderr_redirected(self):
        """Test that stdout and stderr are redirected during context."""
        with suppress_datamule_output():
            print("Test stdout")
            import sys

            print("Test stderr", file=sys.stderr)

        # Output should be captured (not visible in test output)
        # This is a basic smoke test - actual redirection is tested by usage

    def test_log_file_handler_writes_filtered_content(self):
        """Test that meaningful content is written to log file."""
        # Create a mock file handler
        mock_file = io.StringIO()
        mock_handler = MagicMock(spec=logging.FileHandler)
        mock_handler.stream = mock_file

        # Create logger with mock handler
        logger = logging.getLogger("test_datamule")
        logger.handlers = [mock_handler]
        logger.setLevel(logging.DEBUG)

        # Patch the logger in the datamule module
        with patch("public_company_graph.utils.datamule.logger", logger):
            with suppress_datamule_output():
                print("This is meaningful output")
                print("100%|████|")  # Progress bar - should be filtered
                print("Loading submissions")  # Noise - should be filtered
                print("Error: Something went wrong")  # Meaningful - should be logged

        # Check that meaningful content was written
        log_content = mock_file.getvalue()
        assert "meaningful output" in log_content or "Something went wrong" in log_content
        # Progress bars and noise should be filtered
        assert "100%|████|" not in log_content
        assert "Loading submissions" not in log_content

    def test_no_log_file_handler(self):
        """Test that context manager works without log file handler."""
        # Create logger without file handler
        logger = logging.getLogger("test_datamule_no_file")
        logger.handlers = []

        with patch("public_company_graph.utils.datamule.logger", logger):
            # Should not raise any errors
            with suppress_datamule_output():
                print("Test output")

        # No assertions needed - just verify no exceptions

    def test_exception_handling(self):
        """Test that environment is restored even if exception occurs."""
        original_value = os.environ.get("TQDM_DISABLE")

        try:
            if "TQDM_DISABLE" in os.environ:
                del os.environ["TQDM_DISABLE"]

            try:
                with suppress_datamule_output():
                    assert os.environ.get("TQDM_DISABLE") == "1"
                    raise ValueError("Test exception")
            except ValueError:
                pass  # Expected

            # Environment should still be restored
            assert "TQDM_DISABLE" not in os.environ
        finally:
            if original_value is None:
                os.environ.pop("TQDM_DISABLE", None)
            else:
                os.environ["TQDM_DISABLE"] = original_value

    def test_filters_short_lines(self):
        """Test that very short lines are filtered out."""
        mock_file = io.StringIO()
        mock_handler = MagicMock(spec=logging.FileHandler)
        mock_handler.stream = mock_file

        logger = logging.getLogger("test_datamule")
        logger.handlers = [mock_handler]

        with patch("public_company_graph.utils.datamule.logger", logger):
            with suppress_datamule_output():
                print("Short")  # Less than 10 chars - should be filtered
                print("This is a longer line")  # More than 10 chars - should be logged

        log_content = mock_file.getvalue()
        # Short lines should be filtered
        assert "Short" not in log_content
        # Longer lines should be logged
        assert "longer line" in log_content
