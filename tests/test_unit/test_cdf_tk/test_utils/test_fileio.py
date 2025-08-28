import concurrent.futures
import json
import re
import threading
import time
from collections.abc import Iterable, Iterator
from datetime import date, datetime, timezone
from io import TextIOWrapper
from itertools import product
from pathlib import Path
from typing import Any
from unittest.mock import patch

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
    CSVReader,
    CSVWriter,
    FileReader,
    FileWriter,
    SchemaColumn,
    Uncompressed,
)
from cognite_toolkit._cdf_tk.utils.fileio._readers import YAMLBaseReader
from cognite_toolkit._cdf_tk.utils.fileio._writers import NDJsonWriter, YAMLBaseWriter
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


class TestFileWriterThreadSafety:
    """Test thread safety of FileWriter implementations."""

    def test_concurrent_write_chunks_same_filestem(self, tmp_path: Path) -> None:
        """Test that concurrent writes to the same filestem are thread-safe."""
        output_dir = tmp_path
        writer = NDJsonWriter(output_dir, "test", Uncompressed)

        # Prepare test data
        num_threads = 10
        chunks_per_thread = 100
        all_chunks = []

        def write_worker(thread_id: int) -> list[dict[str, Any]]:
            """Worker function that writes chunks and returns what it wrote."""
            chunks = [
                {"thread": thread_id, "chunk": i, "data": f"test_data_{thread_id}_{i}"}
                for i in range(chunks_per_thread)
            ]
            writer.write_chunks(chunks, "test_file")
            return chunks

        # Execute concurrent writes
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor, writer:
            futures = [executor.submit(write_worker, i) for i in range(num_threads)]
            for future in concurrent.futures.as_completed(futures):
                all_chunks.extend(future.result())

        # Verify all data was written correctly
        written_files = list(output_dir.glob("*.ndjson"))
        assert len(written_files) > 0

        written_data = []
        for file_path in written_files:
            with file_path.open("r") as file:
                for line in file:
                    written_data.append(json.loads(line.strip()))

        # Verify all chunks were written
        assert len(written_data) == len(all_chunks)

        # Verify data integrity - each chunk should appear exactly once
        written_keys = {(chunk["thread"], chunk["chunk"]) for chunk in written_data}
        expected_keys = {(chunk["thread"], chunk["chunk"]) for chunk in all_chunks}
        assert written_keys == expected_keys

    def test_concurrent_write_chunks_different_filestems(self, tmp_path: Path) -> None:
        """Test concurrent writes to different filestems are properly isolated."""
        output_dir = tmp_path
        writer = NDJsonWriter(output_dir, "test", Uncompressed)

        num_threads = 5
        chunks_per_thread = 50

        def write_worker(thread_id: int) -> str:
            """Worker function that writes to a unique filestem."""
            filestem = f"file_{thread_id}"
            chunks = [{"thread": thread_id, "chunk": i} for i in range(chunks_per_thread)]
            writer.write_chunks(chunks, filestem)
            return filestem

        with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor, writer:
            futures = [executor.submit(write_worker, i) for i in range(num_threads)]
            _ = [future.result() for future in concurrent.futures.as_completed(futures)]

        # Verify each filestem has its own files
        for thread_id in range(num_threads):
            thread_files = list(output_dir.glob(f"file_{thread_id}-*.ndjson"))
            assert len(thread_files) > 0

            # Verify data in thread-specific files
            written_data = []
            for file_path in thread_files:
                with file_path.open("r") as file:
                    for line in file:
                        data = json.loads(line.strip())
                        assert data["thread"] == thread_id
                        written_data.append(data)

            assert len(written_data) == chunks_per_thread

    def test_concurrent_file_size_limit_handling(self, tmp_path):
        """Test that file size limit handling is thread-safe."""
        output_dir = tmp_path
        # Use very small file size limit to force file rotation
        writer = NDJsonWriter(output_dir, "test", Uncompressed, max_file_size_bytes=1024)

        num_threads = 5
        large_chunks_per_thread = 20

        def write_worker(thread_id: int) -> None:
            """Worker that writes large chunks to trigger file rotation."""
            for i in range(large_chunks_per_thread):
                # Create a large chunk to exceed file size limit
                large_data = "x" * 200  # Make chunk large enough
                chunk = {"thread": thread_id, "chunk": i, "large_data": large_data}
                writer.write_chunks([chunk], "large_file")

        with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor, writer:
            futures = [executor.submit(write_worker, i) for i in range(num_threads)]
            for future in concurrent.futures.as_completed(futures):
                future.result()  # Wait for completion

        # Verify multiple files were created due to size limits
        written_files = list(output_dir.glob("large_file-*.ndjson"))
        assert len(written_files) > 1  # Should have multiple files due to size limit

        # Verify all data is present across files
        total_chunks = 0
        thread_counts = {}
        for file_path in written_files:
            with open(file_path) as f:
                for line in f:
                    data = json.loads(line.strip())
                    total_chunks += 1
                    thread_id = data["thread"]
                    thread_counts[thread_id] = thread_counts.get(thread_id, 0) + 1

        expected_total = num_threads * large_chunks_per_thread
        assert total_chunks == expected_total

        # Verify each thread's data is complete
        for thread_id in range(num_threads):
            assert thread_counts.get(thread_id, 0) == large_chunks_per_thread

    def test_concurrent_context_manager_operations(self, tmp_path):
        """Test that context manager operations (__enter__/__exit__) are thread-safe."""
        output_dir = tmp_path
        # Use a single, shared writer instance to test the instance-level lock.
        writer = NDJsonWriter(output_dir, "shared_worker", Uncompressed)

        def context_worker(worker_id: int) -> bool:
            """Worker that uses the shared writer in a context manager."""
            try:
                # The 'with' statement on the shared writer will be serialized by the lock.
                with writer:
                    chunks = [{"worker": worker_id, "data": f"test_{i}"} for i in range(10)]
                    # Use a unique filestem to ensure data from different workers is separated.
                    writer.write_chunks(chunks, f"context_test_{worker_id}")
                return True
            except Exception:
                return False

        num_workers = 10
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
            futures = [executor.submit(context_worker, i) for i in range(num_workers)]
            results = [future.result() for future in concurrent.futures.as_completed(futures)]

        # All workers should complete successfully.
        assert all(results)

        # Verify files were created for each worker.
        # As __enter__ clears state, each 'with' block creates a new file starting from part-0000.
        for worker_id in range(num_workers):
            worker_files = list(output_dir.glob(f"context_test_{worker_id}-part-0000.shared_worker.ndjson"))
            assert len(worker_files) == 1, f"Expected 1 file for worker {worker_id}, found {len(worker_files)}"

    def test_lock_reentrancy(self, tmp_path):
        """Test that the RLock allows reentrant calls."""
        output_dir = tmp_path
        writer = NDJsonWriter(output_dir, "test", Uncompressed)

        # Mock _write to call write_chunks recursively (simulating reentrant behavior)
        original_write = writer._write

        def reentrant_write(w, chunks):
            # First call the original write
            original_write(w, chunks)
            # Then make a reentrant call (should not deadlock)
            if not hasattr(writer, "_reentrant_called"):
                writer._reentrant_called = True
                writer.write_chunks([{"reentrant": True}], "reentrant_test")

        with patch.object(writer, "_write", reentrant_write):
            with writer:
                writer.write_chunks([{"initial": True}], "test")

        # Verify both original and reentrant writes occurred
        files = list(output_dir.glob("*.ndjson"))
        assert len(files) >= 1

    def test_csv_writer_thread_safety(self, tmp_path):
        """Test thread safety specifically for CSVWriter with its internal state."""
        output_dir = tmp_path
        columns = [
            SchemaColumn("id", "integer", False),
            SchemaColumn("name", "string", False),
            SchemaColumn("value", "float", False),
        ]
        writer = CSVWriter(output_dir, "test", Uncompressed, columns)

        num_threads = 8
        rows_per_thread = 25

        def csv_write_worker(thread_id: int) -> None:
            """Worker that writes CSV data."""
            chunks = [
                {"id": thread_id * 1000 + i, "name": f"name_{thread_id}_{i}", "value": float(i)}
                for i in range(rows_per_thread)
            ]
            writer.write_chunks(chunks, f"csv_test_{thread_id}")

        with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor, writer:
            futures = [executor.submit(csv_write_worker, i) for i in range(num_threads)]
            for future in concurrent.futures.as_completed(futures):
                future.result()

        # Verify CSV files were created and have correct structure
        csv_files = list(output_dir.glob("*.csv"))
        assert len(csv_files) > 0

        total_rows = 0
        for csv_file in csv_files:
            with open(csv_file) as f:
                lines = f.readlines()
                # First line should be header
                assert "id,name,value" in lines[0]
                # Count data rows (excluding header)
                total_rows += len(lines) - 1

        expected_rows = num_threads * rows_per_thread
        assert total_rows == expected_rows

    def test_race_condition_file_count_tracking(self, tmp_path):
        """Test that file count tracking doesn't have race conditions."""
        output_dir = tmp_path
        writer = NDJsonWriter(output_dir, "test", Uncompressed, max_file_size_bytes=512)

        # Track file counts from multiple threads
        file_counts = []
        lock = threading.Lock()

        def count_tracking_worker(thread_id: int) -> None:
            """Worker that tracks file counts during concurrent writes."""
            for i in range(20):
                # Write data that might trigger file rotation
                chunk = {"thread": thread_id, "index": i, "data": "x" * 100}
                writer.write_chunks([chunk], "count_test")

                # Record file count
                with lock:
                    file_counts.append(writer.file_count)

                # Small delay to increase chance of race conditions
                time.sleep(0.001)

        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor, writer:
            futures = [executor.submit(count_tracking_worker, i) for i in range(5)]
            for future in concurrent.futures.as_completed(futures):
                future.result()

        # File counts should be monotonically increasing (never decrease)
        assert file_counts == sorted(file_counts)

        # Final file count should match actual files created
        actual_files = list(output_dir.glob("*.ndjson"))
        # Note: writer.file_count might be 0 after context exit as it clears state
        assert len(actual_files) > 0

    @pytest.mark.parametrize(
        "num_threads,chunks_per_thread",
        [
            (2, 10),  # Light load
            (5, 50),  # Medium load
            (10, 100),  # Heavy load
        ],
    )
    def test_stress_concurrent_writes(self, tmp_path: Path, num_threads: int, chunks_per_thread: int):
        """Stress test concurrent writes with varying loads."""
        output_dir = tmp_path
        writer = NDJsonWriter(output_dir, "stress_test", Uncompressed)

        def stress_worker(thread_id: int) -> int:
            """Worker that performs stress writes."""
            chunks_written = 0
            for batch in range(5):  # Multiple batches per thread
                chunks = [
                    {"thread": thread_id, "batch": batch, "chunk": i, "timestamp": time.time()}
                    for i in range(chunks_per_thread // 5)
                ]
                writer.write_chunks(chunks, f"stress_{thread_id}")
                chunks_written += len(chunks)
            return chunks_written

        start_time = time.time()

        with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor, writer:
            futures = [executor.submit(stress_worker, i) for i in range(num_threads)]
            total_written = sum(future.result() for future in concurrent.futures.as_completed(futures))

        end_time = time.time()

        # Verify performance is reasonable (should complete within reasonable time)
        duration = end_time - start_time
        assert duration < 30.0, f"Stress test took too long: {duration}s"

        # Verify all data was written
        files = list(output_dir.glob("*.ndjson"))
        assert len(files) > 0

        written_count = 0
        for file_path in files:
            with open(file_path) as f:
                written_count += sum(1 for _ in f)

        assert written_count == total_written


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
    def test_read_unprocessed_csv(self, tmp_path: Path) -> None:
        csv_content = "id,space,externalId,number\n1,space1,id1,1.30\n2,space2,id2,42.0\n"
        csv_file = tmp_path / "test.csv"
        csv_file.write_text(csv_content, encoding="utf-8")

        chunks = list(CSVReader(csv_file).read_chunks_unprocessed())

        assert chunks == [
            {"id": "1", "space": "space1", "externalId": "id1", "number": "1.30"},
            {"id": "2", "space": "space2", "externalId": "id2", "number": "42.0"},
        ]
