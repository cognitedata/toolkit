import tempfile
from pathlib import Path
from zipfile import ZipFile

import pytest

from cognite_toolkit._cdf_tk.utils.file import create_temporary_zip, sanitize_filename


class TestCreateTemporaryZip:
    def test_create_temporary_zip(self) -> None:
        with tempfile.TemporaryDirectory() as test_dir:
            test_dir_path = Path(test_dir)
            # Create test directory structure
            subdir = test_dir_path / "subdir"
            subdir.mkdir()
            # Create some test files
            (test_dir_path / "file1.txt").write_text("file1 content")
            (test_dir_path / "file2.txt").write_text("file2 content")
            (subdir / "file3.txt").write_text("file3 content")

            # Use the context manager to create a zip
            original_dir = Path.cwd()
            with create_temporary_zip(test_dir_path, "test.zip") as zip_path:
                # Verify the zip file exists
                assert zip_path.exists()
                assert zip_path.name == "test.zip"

                # Verify the contents of the zip file
                with ZipFile(zip_path, "r") as zip_file:
                    zip_contents = zip_file.namelist()
                    expected_files = {"subdir/", "subdir/file3.txt", "./", "file2.txt", "file1.txt"}
                    assert set(zip_contents) == expected_files

            # Verify we're back in the original directory during zip creation
            assert Path.cwd() == original_dir


class TestSanitizeFilename:
    @pytest.mark.parametrize(
        "filename, expected",
        [
            ("valid_filename.yaml", "valid_filename.yaml"),
            ("another-valid_filename123.json", "another-valid_filename123.json"),
            ("invalid/filename.yaml", "invalid_filename.yaml"),
            ("invalid\\filename.json", "invalid_filename.json"),
            ("invalid:filename.yaml", "invalid_filename.yaml"),
            ("invalid*filename.json", "invalid_filename.json"),
            ("invalid?filename.yaml", "invalid_filename.yaml"),
            ('invalid"filename.json', "invalid_filename.json"),
            ("invalid<filename.yaml", "invalid_filename.yaml"),
            ("invalid>filename.json", "invalid_filename.json"),
            ("invalid|filename.yaml", "invalid_filename.yaml"),
            ("inva|lid:fi*le?name<.json", "inva_lid_fi_le_name_.json"),
        ],
    )
    def test_sanitize_filename(self, filename: str, expected: str) -> None:
        assert sanitize_filename(filename) == expected
