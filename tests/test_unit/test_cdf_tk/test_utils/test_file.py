import re
import tempfile
from pathlib import Path
from zipfile import ZipFile

import pytest

from cognite_toolkit._cdf_tk.utils.file import (
    YamlPosition,
    create_logfile_stem,
    create_temporary_zip,
    find_unique_match_position,
    read_yaml_content,
    sanitize_filename,
    yaml_find_unique_position,
    yaml_positions,
)


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


class TestCreateLogfileStem:
    @pytest.mark.parametrize(
        "existing_files, stem, expected",
        [
            ([], "migration_log", "migration_log-"),
            (["migration_log-part001.log"], "migration_log", "migration_log-run2-"),
            (["migration_log-part001.log", "migration_log-run3-part001.log"], "migration_log", "migration_log-run4-"),
        ],
    )
    def test_create_logfile_stem(self, existing_files: list[str], stem: str, expected: str, tmp_path: Path) -> None:
        for filename in existing_files:
            (tmp_path / filename).touch()
        actual = create_logfile_stem(tmp_path, stem)

        assert actual == expected


class TestReadYamlContent:
    def test_keyvault_tag_preserved_scalar(self) -> None:
        """Test that !keyvault tag is preserved along with the value."""
        yaml_content = "password: !keyvault value-secret-name"
        result = read_yaml_content(yaml_content)
        assert result == {"password": "!keyvault value-secret-name"}

    def test_keyvault_tag_preserved_in_nested_structure(self) -> None:
        """Test that !keyvault tag is preserved in nested YAML structures."""
        yaml_content = """azure-keyvault:
  authentication-method: client-secret
  password: !keyvault value-secret-name
databases:
-   connection-string: !keyvault db-connection-secret
    name: my_db"""
        result = read_yaml_content(yaml_content)
        assert result["azure-keyvault"]["password"] == "!keyvault value-secret-name"
        assert result["databases"][0]["connection-string"] == "!keyvault db-connection-secret"
        assert result["databases"][0]["name"] == "my_db"

    def test_keyvault_tag_preserved_multiple_values(self) -> None:
        """Test that multiple !keyvault tags are preserved."""
        yaml_content = """secret1: !keyvault secret-name-1
secret2: !keyvault secret-name-2
normal_value: regular-string"""
        result = read_yaml_content(yaml_content)
        assert result["secret1"] == "!keyvault secret-name-1"
        assert result["secret2"] == "!keyvault secret-name-2"
        assert result["normal_value"] == "regular-string"


class TestYamlPositions:
    def test_mapping_and_list_locations(self) -> None:
        yaml_content = """- dbName: first
  tableName: wrong
- dbName: second
  nested:
    key: value
"""
        assert yaml_positions(yaml_content) == {
            (0,): YamlPosition(1, 3),
            (0, "dbName"): YamlPosition(1, 3),
            (0, "tableName"): YamlPosition(2, 3),
            (1,): YamlPosition(3, 3),
            (1, "dbName"): YamlPosition(3, 3),
            (1, "nested"): YamlPosition(4, 3),
            (1, "nested", "key"): YamlPosition(5, 5),
        }

    def test_empty_content(self) -> None:
        assert yaml_positions("") == {}


class TestYamlFindUniquePosition:
    CONTENT = """- space: my_space
  name: first
- space: other
  nested:
    name: second
"""

    def test_finds_unique_value(self) -> None:
        assert yaml_find_unique_position(self.CONTENT, "other") == YamlPosition(3, 10)

    def test_ambiguous_key_at_same_depth_is_not_found(self) -> None:
        assert yaml_find_unique_position(self.CONTENT, "space", as_key=True) is None

    def test_key_at_multiple_depths_is_found_at_shallowest(self) -> None:
        assert yaml_find_unique_position(self.CONTENT, "name", as_key=True) == YamlPosition(2, 3)

    def test_unique_key_is_found(self) -> None:
        assert yaml_find_unique_position(self.CONTENT, "nested", as_key=True) == YamlPosition(4, 3)

    def test_invalid_yaml(self) -> None:
        assert yaml_find_unique_position("a: [", "a") is None


class TestFindUniqueMatchPosition:
    CONTENT = "a: 1\nb:\n  c: {{ space }}\n  d: {{ other }}\n  e: {{ other }}\n"

    def test_finds_unique_match(self) -> None:
        assert find_unique_match_position(self.CONTENT, re.compile(r"\{\{ space \}\}")) == YamlPosition(3, 6)

    def test_ambiguous_match_is_not_found(self) -> None:
        assert find_unique_match_position(self.CONTENT, re.compile(r"\{\{ other \}\}")) is None

    def test_no_match(self) -> None:
        assert find_unique_match_position(self.CONTENT, re.compile(r"\{\{ missing \}\}")) is None
