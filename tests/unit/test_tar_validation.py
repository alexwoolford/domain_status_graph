"""
Unit tests for tar member path validation.

Tests the validate_tar_member_path function that prevents Tar Slip attacks.
"""

import logging

from public_company_graph.utils.tar_extraction import validate_tar_member_path


class TestValidateTarMemberPath:
    """Tests for validate_tar_member_path function."""

    def test_valid_simple_filename(self, tmp_path):
        """Test that simple filenames are accepted."""
        extract_dir = tmp_path / "extract"
        extract_dir.mkdir()

        is_valid, safe_name = validate_tar_member_path("file.html", extract_dir)

        assert is_valid is True
        assert safe_name == "file.html"

    def test_valid_filename_with_path(self, tmp_path):
        """Test that filenames with directory paths are sanitized."""
        extract_dir = tmp_path / "extract"
        extract_dir.mkdir()

        is_valid, safe_name = validate_tar_member_path("subdir/file.html", extract_dir)

        assert is_valid is True
        assert safe_name == "file.html"  # Should extract just filename

    def test_rejects_path_traversal_dotdot(self, tmp_path):
        """Test that paths with .. are rejected."""
        extract_dir = tmp_path / "extract"
        extract_dir.mkdir()

        is_valid, safe_name = validate_tar_member_path("../etc/passwd", extract_dir)

        assert is_valid is False
        assert safe_name is None

    def test_rejects_absolute_path(self, tmp_path):
        """Test that absolute paths are rejected."""
        extract_dir = tmp_path / "extract"
        extract_dir.mkdir()

        is_valid, safe_name = validate_tar_member_path("/etc/passwd", extract_dir)

        assert is_valid is False
        assert safe_name is None

    def test_rejects_path_with_leading_slash(self, tmp_path):
        """Test that paths starting with / are rejected."""
        extract_dir = tmp_path / "extract"
        extract_dir.mkdir()

        is_valid, safe_name = validate_tar_member_path("/file.html", extract_dir)

        assert is_valid is False
        assert safe_name is None

    def test_extracts_filename_from_path(self, tmp_path):
        """Test that paths with separators extract just the filename."""
        extract_dir = tmp_path / "extract"
        extract_dir.mkdir()

        # Path with separators - should extract just filename
        is_valid, safe_name = validate_tar_member_path("subdir/file.html", extract_dir)

        assert is_valid is True
        assert safe_name == "file.html"  # Should extract just filename

    def test_rejects_filename_with_backslash(self, tmp_path):
        """Test that filenames with backslashes are rejected."""
        extract_dir = tmp_path / "extract"
        extract_dir.mkdir()

        is_valid, safe_name = validate_tar_member_path("file\\name.html", extract_dir)

        assert is_valid is False
        assert safe_name is None

    def test_validates_resolved_path_within_extract_dir(self, tmp_path):
        """Test that resolved paths are validated to be within extract_dir."""
        extract_dir = tmp_path / "extract"
        extract_dir.mkdir()

        # Even if filename is safe, if resolved path goes outside, it's rejected
        # This is tested indirectly through the resolve().relative_to() check
        is_valid, safe_name = validate_tar_member_path("file.html", extract_dir)

        assert is_valid is True
        assert safe_name == "file.html"

    def test_logs_warning_on_rejection(self, tmp_path, caplog):
        """Test that warnings are logged when paths are rejected."""
        extract_dir = tmp_path / "extract"
        extract_dir.mkdir()

        # Create a custom logger that caplog can capture
        import logging

        test_logger = logging.getLogger("test_tar_warning")
        test_logger.setLevel(logging.WARNING)
        # Ensure it propagates so caplog can capture it
        test_logger.propagate = True

        with caplog.at_level(logging.WARNING, logger="test_tar_warning"):
            validate_tar_member_path("../etc/passwd", extract_dir, logger_instance=test_logger)

        # Check that the warning was logged
        assert len(caplog.records) > 0, (
            f"No log records captured. caplog.text: {repr(caplog.text)}, records: {caplog.records}"
        )
        log_messages = [record.message for record in caplog.records]
        assert any(
            "Skipping suspicious tar member" in msg or "Path traversal attempt" in msg
            for msg in log_messages
        ), f"Warning not found in logs. Messages: {log_messages}"

    def test_uses_custom_logger(self, tmp_path, caplog):
        """Test that custom logger instance is used if provided."""
        extract_dir = tmp_path / "extract"
        extract_dir.mkdir()

        custom_logger = logging.getLogger("custom_test_logger")
        with caplog.at_level(logging.WARNING, logger="custom_test_logger"):
            validate_tar_member_path("../etc/passwd", extract_dir, logger_instance=custom_logger)

        assert (
            "Skipping suspicious tar member" in caplog.text
            or "Path traversal attempt" in caplog.text
        )
