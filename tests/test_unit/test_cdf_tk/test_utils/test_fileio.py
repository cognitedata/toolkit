from collections.abc import Iterator
from io import TextIOWrapper
from pathlib import Path

import pytest

from cognite_toolkit._cdf_tk.exceptions import ToolkitValueError
from cognite_toolkit._cdf_tk.utils._auxiliary import get_concrete_subclasses
from cognite_toolkit._cdf_tk.utils.fileio import (
    COMPRESSION_BY_NAME,
    COMPRESSION_BY_SUFFIX,
    FILE_READ_CLS_BY_FORMAT,
    Compression,
    FileReader,
    NoneCompression,
)
from cognite_toolkit._cdf_tk.utils.useful_types import JsonVal


class LineReader(FileReader):
    format = ".DummyFormat"

    def _read_chunks_from_file(self, file: TextIOWrapper) -> Iterator[JsonVal]:
        for line in file:
            if line.strip():
                yield {"line": line.strip()}


class TestCompression:
    @pytest.mark.parametrize(
        "compression_name",
        list(COMPRESSION_BY_NAME.keys()),
    )
    def test_read_write_compression_by_name(self, compression_name: str, tmp_path: Path) -> None:
        compression = Compression.from_name(compression_name)
        tmp_path = tmp_path / f"test_file.txt{compression.file_suffix}"
        with compression(tmp_path).open(mode="w") as file:
            file.write("Test content")

        with compression(tmp_path).open("r") as file:
            content = file.read()

        assert content == "Test content"

    @pytest.mark.parametrize(
        "compression_suffix",
        [*COMPRESSION_BY_SUFFIX.keys(), NoneCompression.file_suffix],
    )
    def test_read_write_compression_by_suffix(self, compression_suffix: str, tmp_path: Path) -> None:
        tmp_path = tmp_path / f"test_file.txt{compression_suffix}"
        with Compression.from_filepath(tmp_path).open(mode="w") as file:
            file.write("Test content")

        with Compression.from_filepath(tmp_path).open("r") as file:
            content = file.read()

        assert content == "Test content"

    def test_all_compression_classes_registered(self) -> None:
        expected_compressions = get_concrete_subclasses(Compression)

        assert set(COMPRESSION_BY_NAME.values()) == set(expected_compressions)
        assert set(COMPRESSION_BY_SUFFIX.values()) == set(expected_compressions)


class TestFileReader:
    def test_read_multiple_lines(self, tmp_path: Path) -> None:
        # Create a dummy file with multiple lines
        file_content = "line1\nline2\nline3\n"
        file_path = tmp_path / "dummy.txt"
        file_path.write_text(file_content, encoding="utf-8")
        reader = LineReader(file_path)
        chunks = list(reader.read_chunks())
        assert chunks == [
            {"line": "line1"},
            {"line": "line2"},
            {"line": "line3"},
        ]

    def test_read_empty_file(self, tmp_path: Path) -> None:
        file_path = tmp_path / "empty.txt"
        file_path.touch()
        reader = LineReader(file_path)
        chunks = list(reader.read_chunks())
        assert chunks == []

    def test_read_lines_with_whitespace(self, tmp_path: Path) -> None:
        file_content = "  line1  \n\n  line2\n   \nline3   \n"
        file_path = tmp_path / "whitespace.txt"
        file_path.write_text(file_content, encoding="utf-8")
        reader = LineReader(file_path)
        chunks = list(reader.read_chunks())
        assert chunks == [
            {"line": "line1"},
            {"line": "line2"},
            {"line": "line3"},
        ]

    def test_all_file_readers_registered(self) -> None:
        expected_readers = set(get_concrete_subclasses(FileReader)) - {LineReader}

        assert set(expected_readers) == set(FILE_READ_CLS_BY_FORMAT.values())

    @pytest.mark.parametrize(
        "filepath, expected_error",
        [
            (Path("non_existent_file.txt"), "Unknown file format: .txt. Available formats:"),
            (Path("file_without_format"), "File has no suffix. Available formats:"),
            (Path("file_with_multiple_suffixes.txt.gz"), "Unknown file format: .txt."),
            (Path("file_with_only_compression.gz"), "File has a compression suffix, but no file format suffix found."),
        ],
    )
    def test_from_filepath_raise(self, filepath: Path, expected_error: str) -> None:
        with pytest.raises(ToolkitValueError) as excinfo:
            FileReader.from_filepath(filepath)

        assert str(excinfo.value).startswith(expected_error)
