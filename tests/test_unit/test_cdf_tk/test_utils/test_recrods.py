from pathlib import Path
from typing import Literal

import pytest

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
