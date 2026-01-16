"""
Unit tests for security utilities.

Tests path validation functions that prevent path traversal attacks.
"""

import logging
from pathlib import Path

from public_company_graph.utils.security import validate_path_within_base


class TestValidatePathWithinBase:
    """Tests for validate_path_within_base function."""

    def test_valid_path_within_base(self, tmp_path):
        """Test that valid paths within base directory are accepted."""
        base_dir = tmp_path / "base"
        base_dir.mkdir()
        file_path = base_dir / "file.txt"
        file_path.write_text("test")

        assert validate_path_within_base(file_path, base_dir) is True

    def test_valid_path_in_subdirectory(self, tmp_path):
        """Test that paths in subdirectories are accepted."""
        base_dir = tmp_path / "base"
        base_dir.mkdir()
        subdir = base_dir / "subdir"
        subdir.mkdir()
        file_path = subdir / "file.txt"
        file_path.write_text("test")

        assert validate_path_within_base(file_path, base_dir) is True

    def test_rejects_path_outside_base(self, tmp_path):
        """Test that paths outside base directory are rejected."""
        base_dir = tmp_path / "base"
        base_dir.mkdir()
        outside_file = tmp_path / "outside.txt"
        outside_file.write_text("test")

        assert validate_path_within_base(outside_file, base_dir) is False

    def test_rejects_path_traversal_attempt(self, tmp_path):
        """Test that path traversal attempts are rejected."""
        base_dir = tmp_path / "base"
        base_dir.mkdir()
        # Try to access parent directory
        traversal_path = base_dir / ".." / "outside.txt"

        assert validate_path_within_base(traversal_path, base_dir) is False

    def test_rejects_absolute_path_outside_base(self):
        """Test that absolute paths outside base are rejected."""
        base_dir = Path("/tmp/test_base")
        file_path = Path("/etc/passwd")

        assert validate_path_within_base(file_path, base_dir) is False

    def test_accepts_absolute_path_inside_base(self, tmp_path):
        """Test that absolute paths inside base are accepted."""
        base_dir = tmp_path / "base"
        base_dir.mkdir()
        file_path = base_dir / "file.txt"
        file_path.write_text("test")

        # Use resolved paths (absolute)
        assert validate_path_within_base(file_path.resolve(), base_dir.resolve()) is True

    def test_logs_warning_on_rejection(self, tmp_path, caplog):
        """Test that warnings are logged when paths are rejected."""
        base_dir = tmp_path / "base"
        base_dir.mkdir()
        outside_file = tmp_path / "outside.txt"
        outside_file.write_text("test")

        # Create a custom logger that caplog can capture
        import logging

        test_logger = logging.getLogger("test_security_warning")
        test_logger.setLevel(logging.WARNING)
        # Ensure it propagates so caplog can capture it
        test_logger.propagate = True

        with caplog.at_level(logging.WARNING, logger="test_security_warning"):
            validate_path_within_base(outside_file, base_dir, logger_instance=test_logger)

        # Check that the warning was logged
        assert len(caplog.records) > 0, (
            f"No log records captured. caplog.text: {repr(caplog.text)}, records: {caplog.records}"
        )
        assert any(
            "Path traversal attempt detected" in record.message for record in caplog.records
        ), f"Warning not found in logs. Records: {[r.message for r in caplog.records]}"

    def test_uses_custom_logger(self, tmp_path, caplog):
        """Test that custom logger instance is used if provided."""
        base_dir = tmp_path / "base"
        base_dir.mkdir()
        outside_file = tmp_path / "outside.txt"
        outside_file.write_text("test")

        custom_logger = logging.getLogger("custom_test_logger")
        with caplog.at_level(logging.WARNING, logger="custom_test_logger"):
            validate_path_within_base(outside_file, base_dir, logger_instance=custom_logger)

        assert "Path traversal attempt detected" in caplog.text
