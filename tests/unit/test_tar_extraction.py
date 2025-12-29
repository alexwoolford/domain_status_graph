"""
Unit tests for tar extraction utilities.

Tests the extract_from_tar and get_filing_date_from_tar_name functions,
including security validation (Tar Slip prevention).
"""

import tarfile

from public_company_graph.utils.tar_extraction import (
    extract_from_tar,
    get_filing_date_from_tar_name,
)


class TestGetFilingDateFromTarName:
    """Tests for get_filing_date_from_tar_name function."""

    def test_sec_direct_naming(self, tmp_path):
        """Test SEC direct naming format: {CIK}{YY}{filing_number}.tar"""
        # Create a dummy tar file with SEC naming
        tar_file = tmp_path / "000109087224000049.tar"
        tar_file.touch()

        year, filing_num = get_filing_date_from_tar_name(tar_file)
        assert year == 24
        assert filing_num == 49

    def test_datamule_batch_naming(self, tmp_path):
        """Test datamule batch naming format: batch_XXX_YYY.tar"""
        # Create a dummy tar file with batch naming
        tar_file = tmp_path / "batch_000_001.tar"
        tar_file.touch()

        year, filing_num = get_filing_date_from_tar_name(tar_file)
        assert year == 99  # Batch files use year 99
        assert isinstance(filing_num, int)  # Uses mtime

    def test_short_filename_fallback(self, tmp_path):
        """Test fallback to modification time for short filenames"""
        tar_file = tmp_path / "short.tar"
        tar_file.touch()

        year, filing_num = get_filing_date_from_tar_name(tar_file)
        assert year == 0  # Fallback year
        assert isinstance(filing_num, int)  # Uses mtime

    def test_invalid_format_fallback(self, tmp_path):
        """Test fallback for invalid format"""
        tar_file = tmp_path / "invalid_format.tar"
        tar_file.touch()

        year, filing_num = get_filing_date_from_tar_name(tar_file)
        assert year == 0  # Fallback year
        assert isinstance(filing_num, int)  # Uses mtime


class TestExtractFromTar:
    """Tests for extract_from_tar function."""

    def test_extract_simple_tar(self, tmp_path):
        """Test extracting a simple tar file with HTML content"""
        # Create a tar file with an HTML file
        # Use properly formatted filename: {CIK}{YY}{filing}.tar format
        # CIK: 0000123456 (10 chars), YY: 24 (2 chars), filing: 000049 (6 chars)
        tar_file = tmp_path / "000012345624000049.tar"
        company_dir = tmp_path / "company"
        company_dir.mkdir()

        # Create tar with HTML file
        with tarfile.open(tar_file, "w") as tar:
            # Create a simple HTML file
            html_content = b"<html><body>Test 10-K content</body></html>"
            info = tarfile.TarInfo(name="a-20241224.htm")
            info.size = len(html_content)
            import io

            fileobj = io.BytesIO(html_content)
            tar.addfile(info, fileobj=fileobj)

        # Extract
        success, file_path, error = extract_from_tar(tar_file, company_dir, "TEST", "0000123456")

        assert success is True
        assert file_path is not None
        assert file_path.exists()
        assert error is None
        assert file_path.name.startswith("10k_")

    def test_no_html_files(self, tmp_path):
        """Test handling of tar file with no HTML files"""
        tar_file = tmp_path / "000012345624000049.tar"
        company_dir = tmp_path / "company"
        company_dir.mkdir()

        # Create tar with only text file
        with tarfile.open(tar_file, "w") as tar:
            content = b"test content"
            info = tarfile.TarInfo(name="document.txt")
            info.size = len(content)
            import io

            fileobj = io.BytesIO(content)
            tar.addfile(info, fileobj=fileobj)

        # Extract should fail
        success, file_path, error = extract_from_tar(tar_file, company_dir, "TEST", "0000123456")

        assert success is False
        assert file_path is None
        assert error is not None
        assert "No HTML files" in error

    def test_tar_slip_protection(self, tmp_path):
        """Test that Tar Slip attacks are prevented"""
        tar_file = tmp_path / "000012345624000049.tar"
        company_dir = tmp_path / "company"
        company_dir.mkdir()

        # Create tar with path traversal attempt
        with tarfile.open(tar_file, "w") as tar:
            # Try to extract outside the target directory
            malicious_content = b"malicious"
            info = tarfile.TarInfo(name="../../etc/passwd")
            info.size = len(malicious_content)
            import io

            fileobj = io.BytesIO(malicious_content)
            tar.addfile(info, fileobj=fileobj)

            # Also add a legitimate HTML file
            html_content = b"<html><body>Test</body></html>"
            info2 = tarfile.TarInfo(name="a-20241224.htm")
            info2.size = len(html_content)
            tar.addfile(info2, fileobj=io.BytesIO(html_content))

        # Extract should succeed but skip malicious file
        success, file_path, error = extract_from_tar(tar_file, company_dir, "TEST", "0000123456")

        assert success is True
        assert file_path is not None
        # Verify malicious file was not extracted
        assert not (tmp_path / "etc" / "passwd").exists()

    def test_skips_exhibits(self, tmp_path):
        """Test that exhibit files are skipped"""
        tar_file = tmp_path / "test.tar"
        company_dir = tmp_path / "company"
        company_dir.mkdir()

        # Create tar with exhibit and main file
        with tarfile.open(tar_file, "w") as tar:
            # Exhibit file (should be skipped)
            exhibit_content = b"<html>Exhibit</html>"
            info1 = tarfile.TarInfo(name="a-20241224xexx01.htm")
            info1.size = len(exhibit_content)
            import io

            tar.addfile(info1, fileobj=io.BytesIO(exhibit_content))

            # Main file (should be extracted)
            main_content = b"<html><body>Main 10-K</body></html>"
            info2 = tarfile.TarInfo(name="a-20241224.htm")
            info2.size = len(main_content)
            tar.addfile(info2, fileobj=io.BytesIO(main_content))

        # Extract
        success, file_path, error = extract_from_tar(tar_file, company_dir, "TEST", "0000123456")

        assert success is True
        assert file_path is not None
        # Verify main file was extracted, not exhibit
        content = file_path.read_text()
        assert "Main 10-K" in content
        assert "Exhibit" not in content

    def test_cleanup_on_error(self, tmp_path):
        """Test that extracted directory is cleaned up on error"""
        tar_file = tmp_path / "test.tar"
        company_dir = tmp_path / "company"
        company_dir.mkdir()

        # Create invalid tar file (will cause error)
        tar_file.write_bytes(b"invalid tar content")

        # Extract should fail
        success, file_path, error = extract_from_tar(tar_file, company_dir, "TEST", "0000123456")

        assert success is False
        # Verify extracted directory was cleaned up
        extract_dir = company_dir / "extracted"
        assert not extract_dir.exists()

    def test_year_extraction_from_filename(self, tmp_path):
        """Test that year is correctly extracted from tar filename"""
        tar_file = tmp_path / "000109087224000049.tar"
        company_dir = tmp_path / "company"
        company_dir.mkdir()

        # Create tar with HTML file
        with tarfile.open(tar_file, "w") as tar:
            html_content = b"<html><body>Test</body></html>"
            info = tarfile.TarInfo(name="a-20241224.htm")
            info.size = len(html_content)
            import io

            tar.addfile(info, fileobj=io.BytesIO(html_content))

        # Extract
        success, file_path, error = extract_from_tar(tar_file, company_dir, "TEST", "0001090872")

        assert success is True
        assert file_path is not None
        # Verify filename includes year (2024 from 24 in tar name)
        assert "2024" in file_path.name
