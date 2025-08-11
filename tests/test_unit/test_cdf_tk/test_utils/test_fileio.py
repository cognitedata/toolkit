from collections.abc import Iterable, Iterator
from io import TextIOWrapper
from pathlib import Path

import pytest

from cognite_toolkit._cdf_tk.exceptions import ToolkitValueError
from cognite_toolkit._cdf_tk.utils._auxiliary import get_concrete_subclasses
from cognite_toolkit._cdf_tk.utils.fileio import (
    COMPRESSION_BY_NAME,
    COMPRESSION_BY_SUFFIX,
    FILE_READ_CLS_BY_FORMAT,
    FILE_WRITE_CLS_BY_FORMAT,
    Chunk,
    Compression,
    FileReader,
    FileWriter,
    NoneCompression,
)
from cognite_toolkit._cdf_tk.utils.useful_types import JsonVal


class LineReader(FileReader):
    format = ".DummyFormat"

    def _read_chunks_from_file(self, file: TextIOWrapper) -> Iterator[JsonVal]:
        for line in file:
            if line.strip():
                yield {"line": line.strip()}


class DummyWriter(FileWriter[TextIOWrapper]):
    format = ".dummy"

    def __init__(self, output_dir: Path) -> None:
        super().__init__(output_dir=output_dir, kind="DummyKind", compression=NoneCompression)
        self.written_chunks: list[Chunk] = []
        self.opened_files: list[Path] = []

    def _create_writer(self, filepath: Path) -> TextIOWrapper:
        self.opened_files.append(filepath)
        writer = self.compression_cls(filepath).open(mode="w")
        return writer

    def _write(self, writer: TextIOWrapper, chunks: Iterable[Chunk]) -> None:
        self.written_chunks.extend(chunks)
        writer.write("Writing chunks\n")


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


class TestFileWriter:
    def test_file_split_on_limit(self, tmp_path: Path) -> None:
        dummy_writer = DummyWriter(tmp_path / "dummy")
        dummy_writer.max_file_size_bytes = 1  # Set a small limit for testing
        chunk1 = [{"a": 1}]
        chunk2 = [{"b": 2}]
        with dummy_writer:
            dummy_writer.write_chunks(chunk1, filestem="splitfile")
            dummy_writer.write_chunks(chunk2, filestem="splitfile")
        assert [file.relative_to(tmp_path) for file in dummy_writer.opened_files] == [
            Path("dummy/splitfile-part-0000.DummyKind.dummy"),
            Path("dummy/splitfile-part-0001.DummyKind.dummy"),
        ]
        assert dummy_writer.written_chunks == chunk1 + chunk2

    def test_multiple_filenames(self, tmp_path: Path) -> None:
        output_dir = tmp_path / "dummy"
        dummy_writer = DummyWriter(output_dir)
        chunk1 = [{"x": 1}]
        chunk2 = [{"y": 2}]
        with dummy_writer:
            dummy_writer.write_chunks(chunk1, filestem="file1")
            dummy_writer.write_chunks(chunk2, filestem="file2")
        assert [file.relative_to(tmp_path) for file in dummy_writer.opened_files] == [
            Path("dummy/file1-part-0000.DummyKind.dummy"),
            Path("dummy/file2-part-0000.DummyKind.dummy"),
        ]

    def test_all_file_writers_registered(self) -> None:
        expected_writers = set(get_concrete_subclasses(FileWriter)) - {DummyWriter}

        assert set(FILE_WRITE_CLS_BY_FORMAT.values()) == expected_writers

    def test_create_from_format_raises(self) -> None:
        with pytest.raises(ToolkitValueError) as excinfo:
            FileWriter.create_from_format("unknown_format", Path("."), "DummyKind", NoneCompression)
        assert str(excinfo.value).startswith("Unknown file format: unknown_format. Available formats: ")


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
