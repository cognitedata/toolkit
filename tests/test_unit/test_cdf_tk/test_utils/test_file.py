from pathlib import Path
from unittest.mock import MagicMock

import pytest

from cognite_toolkit._cdf_tk.exceptions import ToolkitFileNotFoundError
from cognite_toolkit._cdf_tk.utils.file import find_adjacent_files


class TestFindAdjacentFiles:
    @pytest.mark.parametrize(
        "datafile, suffix, available_files, expected_files",
        [
            pytest.param(
                "my_table-part0001.RawRows.ndjson",
                ".Table.yaml",
                ["my_table.Table.yaml", "my_table.Table.json"],
                ["my_table.Table.yaml"],
                id="single matching file",
            ),
            pytest.param(
                "my_table-part0001.RawRows.ndjson",
                ".Table.yaml",
                ["other_file.Table.yaml", "my_table.Table.json"],
                [],
                id="no matching file",
            ),
            pytest.param(
                "my_table-part0001.RawRows.ndjson",
                ".Table.yaml",
                ["my_table.Table.yaml", "my_table.Table.yaml", "my_table.Table.json"],
                ["my_table.Table.yaml", "my_table.Table.yaml"],
                id="multiple matching files",
            ),
            pytest.param(
                "my_table-part0001.RawRows.ndjson",
                ".Table.yaml",
                ["not_my_table.Table.yaml", "my_table2.Table.yaml"],
                [],
                id="similar names but no match",
            ),
            pytest.param(
                "my_table-part0001.RawRows.ndjson",
                ".DoesNotExist",
                ["my_table.Table.yaml", "my_table.Table.json"],
                [],
                id="no matching suffix",
            ),
        ],
    )
    def test_find_adjacent_files(
        self, datafile: str, suffix: str, available_files: list[str], expected_files: list[str]
    ) -> None:
        filepath = MagicMock(spec=Path)
        filepath.is_file.return_value = True
        filepath.name = datafile
        directory = MagicMock(spec=Path)
        filepath.parent = directory
        directory_files: list[Path] = []
        for file in available_files:
            mock_file = MagicMock(spec=Path)
            mock_file.name = file
            directory_files.append(mock_file)
        directory.glob.return_value = directory_files

        result = find_adjacent_files(filepath, suffix=suffix)

        actual = [file.name for file in result]
        assert actual == expected_files

    def test_find_adjacent_files_not_a_file(self):
        filepath = MagicMock(spec=Path)
        filepath.is_file.return_value = False
        filepath.name = "my_table-part0001.RawRows.ndjson"
        with pytest.raises(ToolkitFileNotFoundError):
            find_adjacent_files(filepath, suffix=".Table.yaml")
