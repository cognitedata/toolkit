import re
from collections.abc import Iterable, Iterator
from datetime import date, datetime, timezone
from io import TextIOWrapper
from itertools import product
from pathlib import Path

import pytest

from cognite_toolkit._cdf_tk.exceptions import ToolkitError, ToolkitValueError
from cognite_toolkit._cdf_tk.utils._auxiliary import get_concrete_subclasses
from cognite_toolkit._cdf_tk.utils.fileio import (
    COMPRESSION_BY_NAME,
    COMPRESSION_BY_SUFFIX,
    FILE_READ_CLS_BY_FORMAT,
    FILE_WRITE_CLS_BY_FORMAT,
    Chunk,
    Compression,
    CSVReader,
    FailedParsing,
    FileReader,
    FileWriter,
    SchemaColumn,
    Uncompressed,
)
from cognite_toolkit._cdf_tk.utils.fileio._readers import YAMLBaseReader
from cognite_toolkit._cdf_tk.utils.fileio._writers import YAMLBaseWriter
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
        super().__init__(output_dir=output_dir, kind="DummyKind", compression=Uncompressed)
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
        [*COMPRESSION_BY_SUFFIX.keys(), Uncompressed.file_suffix],
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
        # YAMLBaseWriter is an abstract class, so we exclude it from the expected readers
        expected_writers = set(get_concrete_subclasses(FileWriter)) - {DummyWriter, YAMLBaseWriter}

        assert set(FILE_WRITE_CLS_BY_FORMAT.values()) == expected_writers

    def test_create_from_format_raises(self) -> None:
        with pytest.raises(ToolkitValueError) as excinfo:
            FileWriter.create_from_format("unknown_format", Path("."), "DummyKind", Uncompressed)
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
        # YAMLBaseReader is an abstract class, so we exclude it from the expected readers
        expected_readers = set(get_concrete_subclasses(FileReader)) - {LineReader, YAMLBaseReader}

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
        # The Table reader returns all columns, even if they are None, so we filter them out for comparison.
        read_chunks = [
            {key: value for key, value in chunk.items() if value is not None} for chunk in reader.read_chunks()
        ]

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
        # The Table reader returns all columns, even if they are None, so we filter them out for comparison.
        read_chunks = [
            {key: value for key, value in chunk.items() if value is not None} for chunk in reader.read_chunks()
        ]
        assert read_chunks == chunks[:mid]
        reader = FileReader.from_filepath(file_path[1])
        read_chunks = [
            {key: value for key, value in chunk.items() if value is not None} for chunk in reader.read_chunks()
        ]
        assert read_chunks == chunks[mid:]


class TestCSVReader:
    CSV_CONTENT = """text,integer,nested,boolean,float
value1,123,"{""key"": ""value""}",true,3.14
value2,456,"{""key"": ""value2""}",false,2.71
,,,,
value3,789,"{""key"": ""value3""}",true,1.41
310,false,31.2,text,20
"""
    EXPECTED_SCHEMA = (
        SchemaColumn(name="text", type="string"),
        SchemaColumn(name="integer", type="integer"),
        SchemaColumn(name="nested", type="json"),
        SchemaColumn(name="boolean", type="boolean"),
        SchemaColumn(name="float", type="float"),
    )

    def test_sniff_schema(self, tmp_path: Path) -> None:
        csv_path = tmp_path / "test.csv"
        csv_path.write_text(self.CSV_CONTENT, encoding="utf-8")

        schema = CSVReader.sniff_schema(csv_path, sniff_rows=100)

        assert schema == list(self.EXPECTED_SCHEMA)

    @pytest.mark.parametrize(
        "content,filename,expected_error",
        [
            pytest.param("", "my_file.csv", "No data found in the file: '{filepath}'.", id="empty file"),
            pytest.param("", None, "File not found: '{filepath}'.", id="missing file"),
            pytest.param(
                "some random text",
                "invalid.txt",
                "Expected a .csv file got a '.txt' file instead.",
                id="invalid format",
            ),
            pytest.param(
                "header1,header1\nvalue1,value2",
                "dup_headers.csv",
                "CSV file contains duplicate headers: header1",
                id="duplicate headers",
            ),
            pytest.param(
                "header1,header2\n", "no_data.csv", "No data found in the file: '{filepath}'.", id="no data rows"
            ),
        ],
    )
    def test_sniff_schema_error_cases(
        self, content: str, filename: str | None, expected_error: str, tmp_path: Path
    ) -> None:
        csv_path = tmp_path / (filename or "missing.csv")
        if filename is not None:
            csv_path.write_text(content, encoding="utf-8")
        if "{filepath}" in expected_error:
            expected_error = expected_error.format(filepath=csv_path.as_posix())

        with pytest.raises(ToolkitError) as excinfo:
            CSVReader.sniff_schema(csv_path, sniff_rows=100)

        assert str(excinfo.value) == expected_error

    def test_read_with_schema(self, tmp_path: Path) -> None:
        csv_path = tmp_path / "test.csv"
        csv_path.write_text(self.CSV_CONTENT, encoding="utf-8")

        reader = CSVReader(csv_path, schema=self.EXPECTED_SCHEMA, keep_failed_cells=True)
        chunks = list(reader.read_chunks())

        assert len(chunks) == 5
        assert chunks == [
            {
                "text": "value1",
                "integer": 123,
                "nested": {"key": "value"},
                "boolean": True,
                "float": 3.14,
            },
            {
                "text": "value2",
                "integer": 456,
                "nested": {"key": "value2"},
                "boolean": False,
                "float": 2.71,
            },
            {"boolean": None, "float": None, "integer": None, "nested": None, "text": None},
            {"boolean": True, "float": 1.41, "integer": 789, "nested": {"key": "value3"}, "text": "value3"},
            {"boolean": None, "float": 20.0, "integer": None, "nested": 31.2, "text": "310"},
        ]
        assert len(reader.failed_cell) == 2
        assert reader.failed_cell == [
            FailedParsing(row=5, column="integer", value="false", error="Cannot convert false to int64."),
            FailedParsing(row=5, column="boolean", value="text", error="Cannot convert text to boolean."),
        ]

    def test_read_unprocessed_csv(self, tmp_path: Path) -> None:
        csv_content = "id,space,externalId,number\n1,space1,id1,1.30\n2,space2,id2,42.0\n"
        csv_file = tmp_path / "test.csv"
        csv_file.write_text(csv_content, encoding="utf-8")

        chunks = list(CSVReader(csv_file).read_chunks_unprocessed())

        assert chunks == [
            {"id": "1", "space": "space1", "externalId": "id1", "number": "1.30"},
            {"id": "2", "space": "space2", "externalId": "id2", "number": "42.0"},
        ]
