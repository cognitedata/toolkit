from pathlib import Path

import pyarrow.parquet as pq
from cognite.client.data_classes.raw import Row

from cognite_toolkit._cdf_tk.client.cdf_client import PagedResponse
from cognite_toolkit._cdf_tk.client.resource_classes.asset import AssetAggregateItem, AssetResponse
from cognite_toolkit._cdf_tk.client.resource_classes.transformation import SQLQueryResponse
from cognite_toolkit._cdf_tk.client.testing import monkeypatch_toolkit_client
from cognite_toolkit._cdf_tk.commands import DownloadCommand
from cognite_toolkit._cdf_tk.dataio import AssetDataIO, RawIO
from cognite_toolkit._cdf_tk.dataio.selectors import DataSetSelector, RawTableSelector, SelectedTable
from cognite_toolkit._cdf_tk.utils.fileio import CSVReader


class TestDownloadCommand:
    def test_download_data_in_table_format(self, tmp_path: Path) -> None:
        cmd = DownloadCommand(silent=True, skip_tracking=True)
        with monkeypatch_toolkit_client() as client:
            dataset = "my/:_data_set"
            client.assets.count.return_value = 1
            client.assets.paginate.return_value = PagedResponse(
                items=[
                    AssetResponse(
                        id=123,
                        name="asset_123",
                        metadata={"key": "value"},
                        data_set_id=42,
                        aggregates=AssetAggregateItem(childCount=1, depth=0, path=[]),
                        created_time=0,
                        last_updated_time=0,
                        root_id=0,
                    )
                ]
            )

            client.lookup.data_sets.id.return_value = 42
            client.lookup.data_sets.external_id.return_value = dataset
            client.transformations.run_query_preview.return_value = SQLQueryResponse(
                schema_=[], results=[{"key": "key", "key_count": 1}]
            )

            cmd.download(
                selectors=[DataSetSelector(kind="Assets", data_set_external_id=dataset, download_dir_name="assets")],
                io=AssetDataIO(client=client),
                output_dir=tmp_path,
                verbose=True,
                file_format=".csv",
                compression="none",
                limit=-1,
            )

            csv_files = list(tmp_path.rglob("*.csv"))
            assert len(csv_files) == 1
            content = list(CSVReader(csv_files[0]).read_chunks_unprocessed())
            assert content == [
                {
                    "dataSetExternalId": dataset,
                    "description": "",
                    "externalId": "",
                    "geoLocation": "",
                    "labels": "",
                    "metadata.key": "value",
                    "name": "asset_123",
                    "parentExternalId": "",
                    "source": "",
                    "childCount": "1",
                    "depth": "0",
                    "path": "",
                }
            ]

    def test_download_raw_table_as_csv(self, tmp_path: Path) -> None:
        cmd = DownloadCommand(silent=True, skip_tracking=True)
        with monkeypatch_toolkit_client() as client:
            client.raw.tables.rows.iterate.return_value = [
                [
                    Row(
                        key="row-1",
                        columns={"name": "pump", "value": 3, "meta": {"unit": "bar"}},
                        last_updated_time=1,
                    ),
                    Row(key="row-2", columns={"name": "valve", "value": 4, "ratio": 1.5}, last_updated_time=2),
                ]
            ]
            cmd.download(
                selectors=[
                    RawTableSelector(
                        table=SelectedTable(db_name="db", table_name="pumps"),
                        download_dir_name="raw",
                    )
                ],
                io=RawIO(client),
                output_dir=tmp_path,
                verbose=False,
                file_format=".csv",
                compression="none",
            )

        csv_files = list(tmp_path.rglob("*.csv"))
        content = list(CSVReader(csv_files[0]).read_chunks_unprocessed()) if csv_files else []
        assert {
            "files": [path.name for path in csv_files],
            "ndjson": [path.name for path in tmp_path.rglob("*.RawRows.ndjson")],
            "columns": list(content[0]) if content else [],
            "rows": content,
        } == {
            "files": ["pumps-part-0000.RawRows.csv"],
            "ndjson": [],
            "columns": ["key", "meta", "name", "ratio", "value"],
            "rows": [
                {"key": "row-1", "meta": '{"unit": "bar"}', "name": "pump", "ratio": "", "value": "3"},
                {"key": "row-2", "meta": "", "name": "valve", "ratio": "1.5", "value": "4"},
            ],
        }

    def test_download_raw_table_as_parquet(self, tmp_path: Path) -> None:
        cmd = DownloadCommand(silent=True, skip_tracking=True)
        with monkeypatch_toolkit_client() as client:
            client.raw.tables.rows.iterate.return_value = [
                [
                    Row(
                        key="a",
                        columns={"count": 1, "ratio": 1.5, "active": True, "tags": ["x"], "scores": [1, 2]},
                        last_updated_time=1,
                    ),
                    Row(
                        key="b",
                        columns={"count": 2, "ratio": 2, "active": False, "tags": ["y", "z"], "scores": [3.5]},
                        last_updated_time=2,
                    ),
                ]
            ]
            cmd.download(
                selectors=[
                    RawTableSelector(
                        table=SelectedTable(db_name="db", table_name="events"),
                        download_dir_name="raw",
                    )
                ],
                io=RawIO(client),
                output_dir=tmp_path,
                verbose=False,
                file_format=".parquet",
                compression="none",
            )

        parquet_files = list(tmp_path.rglob("*.parquet"))
        rows = pq.read_table(parquet_files[0]).to_pylist() if parquet_files else []
        assert {
            "files": [path.name for path in parquet_files],
            "ndjson": [path.name for path in tmp_path.rglob("*.RawRows.ndjson")],
            "rows": rows,
        } == {
            "files": ["events-part-0000.RawRows.parquet"],
            "ndjson": [],
            "rows": [
                {
                    "key": "a",
                    "active": True,
                    "count": 1,
                    "ratio": 1.5,
                    "scores": [1.0, 2.0],
                    "tags": ["x"],
                },
                {
                    "key": "b",
                    "active": False,
                    "count": 2,
                    "ratio": 2.0,
                    "scores": [3.5],
                    "tags": ["y", "z"],
                },
            ],
        }

    def test_download_empty_raw_table_as_csv(self, tmp_path: Path) -> None:
        cmd = DownloadCommand(silent=True, skip_tracking=True)
        with monkeypatch_toolkit_client() as client:
            client.raw.tables.rows.iterate.return_value = []
            cmd.download(
                selectors=[
                    RawTableSelector(
                        table=SelectedTable(db_name="db", table_name="empty"),
                        download_dir_name="raw",
                    )
                ],
                io=RawIO(client),
                output_dir=tmp_path,
                verbose=False,
                file_format=".csv",
                compression="none",
            )

        assert list(tmp_path.rglob("*.csv")) == []
