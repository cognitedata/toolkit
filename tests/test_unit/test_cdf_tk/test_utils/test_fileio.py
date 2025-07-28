import io
from collections.abc import Iterable, Iterator
from datetime import date, datetime
from itertools import product
from pathlib import Path

import pytest

from cognite_toolkit._cdf_tk.utils.fileio import (
    COMPRESSION_BY_NAME,
    FILE_WRITE_CLS_BY_FORMAT,
    Chunk,
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
def cell_chunks() -> tuple[list[Chunk], list[SchemaColumn]]:
    chunks = [
        {"text": "value1", "integer": 123},
        {"text": "value4", "list": [1, 2, 3], "nested": {"key": "value"}},
        {"text": "value5", "boolean": True},
        {"text": "value6"},
        {"text": "value7", "float": 3.14, "empty_list": []},
        {"date": date(2023, 10, 1), "timestamp": datetime(2023, 10, 1, 12, 0, 0)},
    ]
    schema = [
        SchemaColumn(name="text", type="string"),
        SchemaColumn(name="integer", type="integer"),
        SchemaColumn(name="list", type="integer", is_array=True),
        SchemaColumn(name="nested", type="json"),
        SchemaColumn(name="boolean", type="boolean"),
        SchemaColumn(name="null_value", type="string"),
        SchemaColumn(name="float", type="float"),
        SchemaColumn(name="empty_list", type="integer", is_array=True),
        SchemaColumn(name="date", type="date"),
        SchemaColumn(name="timestamp", type="timestamp"),
    ]
    return chunks, schema


class SimpleTextIO(io.TextIOBase):
    def __init__(self, lines: list[str] | None = None) -> None:
        self._lines = lines or []
        self._closed = False

    def write(self, s: str) -> int:
        self._lines.append(s)
        return len(s)

    def read(self, size=..., /):
        if size is ...:
            return "".join(self._lines)
        else:
            return "".join(self._lines)[:size]

    def close(self) -> None:
        self._closed = True

    @property
    def closed(self) -> bool:
        return self._closed


class DummyWriter(FileWriter[SimpleTextIO]):
    format = "dummy"

    def __init__(self, above_file_limit: list[bool] | None = None) -> None:
        super().__init__(Path("dummy"), "Dummy", compression=NoneCompression)
        self.above_file_limit = above_file_limit or []
        self.written_chunks: list[Chunk] = []
        self.opened_files: list[Path] = []

    def _create_writer(self, filepath: Path) -> SimpleTextIO:
        writer = SimpleTextIO()
        self.opened_files.append(filepath)
        return writer

    def _is_above_file_size_limit(self, filepath: Path, writer: SimpleTextIO) -> bool:
        return self.above_file_limit.pop(0) if self.above_file_limit else False

    def _write(self, writer: SimpleTextIO, chunks: Iterable[Chunk]) -> None:
        self.written_chunks.extend(chunks)


class DummyReader(FileReader):
    format = "dummy"

    def _read_chunks_from_file(self, file: io.IOBase) -> Iterator[JsonVal]:
        content = file.read()
        for line in content.splitlines():
            if line.strip():
                yield {"line": line.strip()}


class TestFileWriter:
    def test_file_split_on_limit(self):
        # Simulate file size limit reached after first chunk
        dummy_writer = DummyWriter(above_file_limit=[True])
        chunk1 = [{"a": 1}]
        chunk2 = [{"b": 2}]
        with dummy_writer:
            dummy_writer.write_chunks(chunk1, filename="splitfile")
            dummy_writer.write_chunks(chunk2, filename="splitfile")
        assert dummy_writer.opened_files == [
            Path("dummy/splitfile-part-0000.Dummy.dummy"),
            Path("dummy/splitfile-part-0001.Dummy.dummy"),
        ]
        assert dummy_writer.written_chunks == chunk1 + chunk2

    def test_multiple_filenames(self):
        dummy_writer = DummyWriter()
        chunk1 = [{"x": 1}]
        chunk2 = [{"y": 2}]
        with dummy_writer:
            dummy_writer.write_chunks(chunk1, filename="file1")
            dummy_writer.write_chunks(chunk2, filename="file2")
        assert dummy_writer.opened_files == [
            Path("dummy/file1-part-0000.Dummy.dummy"),
            Path("dummy/file2-part-0000.Dummy.dummy"),
        ]


class TestFileReader:
    def test_read_multiple_lines(self, tmp_path: Path) -> None:
        # Create a dummy file with multiple lines
        file_content = "line1\nline2\nline3\n"
        file_path = tmp_path / "dummy.txt"
        file_path.write_text(file_content, encoding="utf-8")
        reader = DummyReader(file_path)
        chunks = list(reader.read_chunks())
        assert chunks == [
            {"line": "line1"},
            {"line": "line2"},
            {"line": "line3"},
        ]

    def test_read_empty_file(self, tmp_path: Path) -> None:
        file_path = tmp_path / "empty.txt"
        file_path.touch()
        reader = DummyReader(file_path)
        chunks = list(reader.read_chunks())
        assert chunks == []

    def test_read_lines_with_whitespace(self, tmp_path: Path) -> None:
        file_content = "  line1  \n\n  line2\n   \nline3   \n"
        file_path = tmp_path / "whitespace.txt"
        file_path.write_text(file_content, encoding="utf-8")
        reader = DummyReader(file_path)
        chunks = list(reader.read_chunks())
        assert chunks == [
            {"line": "line1"},
            {"line": "line2"},
            {"line": "line3"},
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

        file_path = list(output_dir.rglob(f"*{format}{compression_cls.file_suffix}"))
        assert len(file_path) == 2

        reader = FileReader.from_filepath(file_path[0])
        read_chunks = list(reader.read_chunks())

        assert read_chunks == chunks[:mid]
