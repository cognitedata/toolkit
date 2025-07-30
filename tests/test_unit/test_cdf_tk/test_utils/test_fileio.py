import re
from collections.abc import Iterable, Iterator
from datetime import date, datetime, timezone
from io import TextIOWrapper
from itertools import product
from pathlib import Path

import pytest

from cognite_toolkit._cdf_tk.utils.fileio import (
    COMPRESSION_BY_NAME,
    COMPRESSION_BY_SUFFIX,
    FILE_WRITE_CLS_BY_FORMAT,
    Chunk,
    Compression,
    FileReader,
    FileWriter,
    NoneCompression,
    SchemaColumn,
)
from cognite_toolkit._cdf_tk.utils.useful_types import JsonVal


@pytest.fixture()
def json_chunks() -> tuple[list[dict[str, JsonVal]], list[SchemaColumn]]:
    chunks = [
        {"text": "value1", "integer": 123},
        {"text": "value4", "list": [1, 2, 3], "nested": {"key": "value"}},
        {"text": "value5", "boolean": True},
        {"text": "value6"},
        {"text": "value7", "float": 3.14, "empty_list": []},
    ]
    schema = [
        SchemaColumn(name="text", type="string"),
        SchemaColumn(name="integer", type="integer"),
        SchemaColumn(name="list", type="integer", is_array=True),
        SchemaColumn(name="nested", type="json"),
        SchemaColumn(name="boolean", type="boolean"),
        SchemaColumn(name="float", type="float"),
        SchemaColumn(name="empty_list", type="float", is_array=True),
    ]
    return chunks, schema


@pytest.fixture()
def cell_chunks(
    json_chunks: tuple[list[dict[str, JsonVal]], list[SchemaColumn]],
) -> tuple[list[Chunk], list[SchemaColumn]]:
    chunks, schema = json_chunks
    chunks = [
        *chunks,
        {"date": date(2023, 10, 1), "timestamp": datetime(2023, 10, 1, 12, 0, 0, tzinfo=timezone.utc)},
    ]
    schema = [
        *schema,
        SchemaColumn(name="date", type="date"),
        SchemaColumn(name="timestamp", type="timestamp"),
    ]
    return chunks, schema


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


class TestFileIO:
    @pytest.mark.parametrize(
        "format, compression_name",
        list(product(FILE_WRITE_CLS_BY_FORMAT.keys(), COMPRESSION_BY_NAME.keys())),
    )
    def test_write_read(
        self,
        format: str,
        compression_name: str,
        json_chunks: tuple[list[dict[str, JsonVal]], list[SchemaColumn]],
        tmp_path: Path,
    ) -> None:
        chunks, columns = json_chunks
        chunk_len = len(chunks)
        compression_cls = COMPRESSION_BY_NAME[compression_name]
        output_dir = tmp_path / "output"
        with FileWriter.create_from_format(
            format, output_dir, "Test", compression=compression_cls, columns=columns
        ) as writer:
            mid = chunk_len // 2
            writer.write_chunks(chunks[:mid])
            writer.write_chunks(chunks[mid:])

        file_path = list(output_dir.rglob(f"*{format}{compression_cls.file_suffix}"))
        assert len(file_path) == 1

        reader = FileReader.from_filepath(file_path[0])
        read_chunks = list(reader.read_chunks())

        assert read_chunks == chunks

    @pytest.mark.parametrize(
        "format, compression_name",
        list(product(FILE_WRITE_CLS_BY_FORMAT.keys(), COMPRESSION_BY_NAME.keys())),
    )
    def test_write_cell_chunks(
        self, format: str, compression_name: str, cell_chunks: tuple[list[Chunk], list[SchemaColumn]], tmp_path: Path
    ) -> None:
        chunks, columns = cell_chunks
        compression_cls = COMPRESSION_BY_NAME[compression_name]
        output_dir = tmp_path / "output"
        with FileWriter.create_from_format(
            format, output_dir, "Test", compression=compression_cls, columns=columns
        ) as writer:
            writer.write_chunks(chunks)

        file_path = list(output_dir.rglob(f"*{format}{compression_cls.file_suffix}"))
        assert len(file_path) == 1
        # We cannot read cell chunks directly, as these contain non-JSON values like dates and timestamps.

    @pytest.mark.parametrize(
        "format, compression_name",
        list(product(FILE_WRITE_CLS_BY_FORMAT.keys(), COMPRESSION_BY_NAME.keys())),
    )
    def test_write_split_files(
        self,
        format: str,
        compression_name: str,
        json_chunks: tuple[list[dict[str, JsonVal]], list[SchemaColumn]],
        tmp_path: Path,
    ) -> None:
        chunks, columns = json_chunks
        chunk_len = len(chunks)
        compression_cls = COMPRESSION_BY_NAME[compression_name]
        output_dir = tmp_path / "output"
        writer_inst = FileWriter.create_from_format(
            format, output_dir, "Test", compression=compression_cls, columns=columns
        )
        writer_inst.max_file_size_bytes = 1  # Small size to force splitting
        with writer_inst as writer:
            mid = chunk_len // 2
            writer.write_chunks(chunks[:mid])
            writer.write_chunks(chunks[mid:])

        def part_number(filepath: Path) -> int:
            match = re.match(r"part-(\d{4}).*", filepath.name)
            return int(match.group(1)) if match else -1

        file_path = sorted(output_dir.rglob(f"*{format}{compression_cls.file_suffix}"), key=part_number)
        assert len(file_path) == 2

        reader = FileReader.from_filepath(file_path[0])
        assert list(reader.read_chunks()) == chunks[:mid]
        reader = FileReader.from_filepath(file_path[1])
        assert list(reader.read_chunks()) == chunks[mid:]
