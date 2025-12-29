"""
Unit tests for public_company_graph.utils.file_discovery module.
"""

from public_company_graph.utils.file_discovery import find_10k_files


class TestFind10kFiles:
    """Test find_10k_files function."""

    def test_find_files_in_directory(self, tmp_path):
        """Test finding HTML and XML files in a directory."""
        # Create test directory structure
        filings_dir = tmp_path / "filings"
        filings_dir.mkdir()

        # Create some files
        (filings_dir / "file1.html").write_text("test")
        (filings_dir / "file2.xml").write_text("test")
        (filings_dir / "subdir").mkdir()
        (filings_dir / "subdir" / "file3.html").write_text("test")

        files = find_10k_files(filings_dir)

        assert len(files) == 3
        assert all(f.suffix in [".html", ".xml"] for f in files)

    def test_nonexistent_directory(self, tmp_path):
        """Test with nonexistent directory."""
        nonexistent = tmp_path / "nonexistent"
        files = find_10k_files(nonexistent)
        assert files == []

    def test_limit(self, tmp_path):
        """Test limit parameter."""
        filings_dir = tmp_path / "filings"
        filings_dir.mkdir()

        # Create multiple files
        for i in range(10):
            (filings_dir / f"file{i}.html").write_text("test")

        files = find_10k_files(filings_dir, limit=5)
        assert len(files) == 5

    def test_custom_extensions(self, tmp_path):
        """Test with custom extensions."""
        filings_dir = tmp_path / "filings"
        filings_dir.mkdir()

        (filings_dir / "file1.html").write_text("test")
        (filings_dir / "file2.txt").write_text("test")
        (filings_dir / "file3.pdf").write_text("test")

        files = find_10k_files(filings_dir, extensions=[".txt", ".pdf"])
        assert len(files) == 2
        assert all(f.suffix in [".txt", ".pdf"] for f in files)

    def test_empty_directory(self, tmp_path):
        """Test with empty directory."""
        filings_dir = tmp_path / "filings"
        filings_dir.mkdir()

        files = find_10k_files(filings_dir)
        assert files == []
