from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from cognite_toolkit._cdf_tk.client.testing import monkeypatch_toolkit_client
from cognite_toolkit._cdf_tk.storageio import DatapointsIO
from cognite_toolkit._cdf_tk.storageio.selectors import DataPointsFileSelector, ExternalIdColumn
from cognite_toolkit._cdf_tk.utils.fileio import MultiFileReader


class TestDataPointsIO:
    def test_iterate_chunks(self, tmp_path: Path):
        n_rows = 2_000
        n_columns = 10
        expected_iterations = ((n_rows * n_columns) // DatapointsIO.CHUNK_SIZE) + (
            1 if (n_rows * n_columns) % DatapointsIO.CHUNK_SIZE > 0 else 0
        )
        data = {
            "timestamp": pa.array(range(n_rows), type=pa.timestamp("ms")),
            **{f"col_{i}": pa.array(range(n_rows), type=pa.float64()) for i in range(n_columns)},
        }
        data_file = tmp_path / "data.Datapoints.parquet"
        table = pa.Table.from_pydict(data)
        pq.write_table(table, data_file)

        selector = DataPointsFileSelector(
            timestamp_column="timestamp",
            columns=tuple(
                [
                    ExternalIdColumn(dtype="numeric", column=f"col_{i}", external_id=f"my_timeseries_{i}")
                    for i in range(n_columns)
                ]
            ),
        )
        with monkeypatch_toolkit_client() as client:
            io = DatapointsIO(client)

            reader = MultiFileReader([data_file])
            item_count = io.count_items(reader)
            assert item_count == n_rows * n_columns

            actual_chunks = list(io.read_chunks(reader, selector))
            assert len(actual_chunks) == expected_iterations, (
                f"Expected {expected_iterations} chunk, got {len(actual_chunks)}"
            )
