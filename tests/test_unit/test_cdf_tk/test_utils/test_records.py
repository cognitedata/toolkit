from pathlib import Path
from typing import Literal

import pytest

from cognite_toolkit._cdf_tk.exceptions import ToolkitValueError
from cognite_toolkit._cdf_tk.utils.record import RecordReader, RecordWriter
from cognite_toolkit._cdf_tk.utils.useful_types import JsonVal


class TestRecords:
    @pytest.mark.parametrize(
        "filename, compression",
        [
            pytest.param("test_records.ndjson", "none", id="No compression"),
            pytest.param("test_records.ndjson.gz", "gzip", id="Gzip compression"),
        ],
    )
    def test_write_read_records(self, filename: str, compression: Literal["gzip", "none"], tmp_path: Path) -> None:
        first_batch = [{"key": "value1"}, {"key": "value2", "nested": {"list": [1, 2, 3]}}]
        second_batch = [{"key": "value3"}, {"key": "value4"}]
        filepath = tmp_path / filename

        with RecordWriter(filepath, compression=compression) as writer:
            writer.write_records(first_batch)
            writer.write_records(second_batch)

        read_records: list[dict[str, JsonVal]] = []
        with RecordReader(filepath) as reader:
            for record in reader.read_records():
                read_records.append(record)

        assert read_records == first_batch + second_batch, "Read records do not match written records."

    @pytest.mark.parametrize(
        "filename, compression, error_message",
        [
            pytest.param(
                "test_records.ndjson.gz",
                "none",
                "Invalid compression for file: 'test_records.ndjson.gz'. Expected no suffix for no compression.",
                id="none with .gz suffix",
            ),
            pytest.param(
                "test_records.ndjson",
                "gzip",
                "Invalid compression for file: 'test_records.ndjson'. Expected '.gz' suffix for gzip compression.",
                id="gzip with no .gz suffix",
            ),
            pytest.param(
                "test_records.ndjson.txt",
                "infer",
                "Cannot infer compression from filename: 'test_records.ndjson.txt'",
                id="infer with double suffix",
            ),
            pytest.param(
                "test_records.ndjson.txt",
                "gzip",
                "Invalid compression for file: 'test_records.ndjson.txt'. Expected '.gz' suffix for gzip compression.",
                id="gzip with wrong suffix",
            ),
            pytest.param(
                "test_records.ndjson.txt",
                "none",
                "Invalid compression for file: 'test_records.ndjson.txt'. Expected no suffix for no compression.",
                id="none with wrong suffix",
            ),
            pytest.param(
                "test_records.ndjson",
                "unsupported",
                "Unsupported compression type: unsupported",
                id="unsupported compression",
            ),
        ],
    )
    def test_invalid_compression(self, filename: str, compression: str, error_message: str) -> None:
        filepath = Path(filename)
        with pytest.raises(ToolkitValueError) as exc_info:
            RecordWriter.validate_compression(compression, filepath)
        assert str(exc_info.value) == error_message, f"Expected error message: {error_message}, got: {exc_info.value}"

    @pytest.mark.parametrize(
        "filename, format, error_message",
        [
            pytest.param(
                "test_records.txt",
                "infer",
                "Cannot infer format from file extension: .txt. Only '.ndjson' is supported.",
                id="infer with .txt",
            ),
            pytest.param(
                "test_records.txt",
                "ndjson",
                "Invalid format for file: test_records.txt. Expected '.ndjson' suffix.",
                id="ndjson with .txt",
            ),
            pytest.param(
                "test_records.ndjson", "unsupported", "Unsupported format: unsupported", id="unsupported format"
            ),
        ],
    )
    def test_invalid_format(self, filename: str, format: str, error_message: str) -> None:
        filepath = Path(filename)
        with pytest.raises(ToolkitValueError) as exc_info:
            RecordReader.validate_format(format, filepath)
        assert str(exc_info.value) == error_message, f"Expected error message: {error_message}, got: {exc_info.value}"

    @pytest.mark.parametrize(
        "filename, format, compression",
        [
            pytest.param("my_schema.my_table.ndjson", "ndjson", "none", id="Use period in filename no compression"),
            pytest.param(
                "my_schema.my_table.ndjson.gz", "ndjson", "gzip", id="Use period in filename gzip compression"
            ),
            pytest.param("my_schema.my_table.ndjson.gz", "infer", "infer", id="Use period in filename infer both"),
            pytest.param("test_records.ndjson", "infer", "infer", id="Infer both"),
            pytest.param("test_records.ndjson.gz", "ndjson", "gzip", id="Valid gzip"),
            pytest.param("test_records.ndjson", "ndjson", "none", id="Valid no compression"),
        ],
    )
    def test_valid_compression_and_format(
        self, filename: str, format: Literal["infer", "ndjson"], compression: Literal["infer", "gzip", "none"]
    ) -> None:
        filepath = Path(filename)
        RecordWriter.validate_compression(compression, filepath)
        RecordReader.validate_format(format, filepath)
