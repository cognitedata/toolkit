import json
from pathlib import Path

import pytest
import respx
from cognite.client.data_classes.raw import Row, RowList

from cognite_toolkit._cdf_tk.client import ToolkitClientConfig
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient
from cognite_toolkit._cdf_tk.client.resource_classes.raw import RAWRowResponse
from cognite_toolkit._cdf_tk.client.testing import monkeypatch_toolkit_client
from cognite_toolkit._cdf_tk.commands import UploadCommand
from cognite_toolkit._cdf_tk.dataio import Page, RawIO
from cognite_toolkit._cdf_tk.dataio.selectors import RawTableSelector, SelectedTable
from cognite_toolkit._cdf_tk.exceptions import ToolkitRuntimeError
from cognite_toolkit._cdf_tk.utils.collection import chunker
from cognite_toolkit._cdf_tk.utils.fileio import SchemaColumn


@pytest.fixture()
def some_raw_tables() -> RowList:
    """Fixture to provide a sample RowList for testing."""
    return RowList(
        [
            Row(
                key=f"row{i}",
                columns={
                    "column1": f"value1_{i}",
                    "column2": f"value2_{i}",
                    "column3": f"value3_{i}",
                    "column4": {"nested_key": f"nested_value_{i}"},
                },
                last_updated_time=0,
            )
            for i in range(100)
        ]
    )


@pytest.mark.usefixtures("disable_gzip", "disable_pypi_check")
class TestRawStorageIO:
    def test_download_upload(
        self, toolkit_config: ToolkitClientConfig, some_raw_tables: RowList, respx_mock: respx.MockRouter
    ) -> None:
        config = toolkit_config
        respx_mock.post(
            config.create_api_url("/raw/dbs/test_db/tables/test_table/rows"),
        ).respond(status_code=200)
        selector = RawTableSelector(table=SelectedTable(db_name="test_db", table_name="test_table"))
        with monkeypatch_toolkit_client() as client:
            client.raw.tables.rows.iterate.return_value = [
                [
                    RAWRowResponse(
                        key=row.key,
                        columns=row.columns,
                        last_updated_time=row.last_updated_time or 0,
                    )
                    for row in page
                ]
                for page in chunker(some_raw_tables, 10)
            ]
            io = RawIO(client)

            assert io.count(selector) is None

            source = io.stream_data(selector, limit=100)
            json_chunks: list[Page] = []
            for chunk in source:
                json_chunk = io.data_to_json_chunk(chunk)
                assert isinstance(json_chunk, Page)
                assert len(json_chunk) == 10
                for item in json_chunk.items:
                    assert isinstance(item.item, dict)
                json_chunks.append(json_chunk)

            with HTTPClient(config) as upload_client:
                data_chunks = (io.json_chunk_to_data(chunk) for chunk in json_chunks)
                for data_chunk in data_chunks:
                    io.upload_items(data_chunk, upload_client, selector)

            assert respx_mock.calls.call_count == 10  # 100 rows in chunks of 10
            uploaded_rows = []
            for call in respx_mock.calls:
                uploaded_rows.extend(json.loads(call.request.content)["items"])

            assert uploaded_rows == some_raw_tables.as_write().dump()

    def test_upload_from_csv_raise_invalid_key(self, tmp_path: Path) -> None:
        selector = RawTableSelector(
            table=SelectedTable(db_name="test_db", table_name="test_table"), key="non_existing_column"
        )
        selector.dump_to_file(tmp_path)
        csv_content = "column1,column2,column3\nvalue1,value2,value3\nvalue4,value5,value6\n"
        csv_file = tmp_path / f"{selector!s}.{RawIO.KIND}.csv"
        csv_file.write_text(csv_content)

        cmd = UploadCommand(silent=True)
        with pytest.raises(ToolkitRuntimeError) as exc_info:
            with monkeypatch_toolkit_client() as client:
                cmd.upload(
                    input_dir=tmp_path,
                    client=client,
                    deploy_resources=False,
                    dry_run=False,
                    verbose=False,
                )

        assert "column 'non_existing_column' not found" in str(exc_info.value).casefold()

    def test_infers_table_schema_after_rows_are_read(self) -> None:
        selector = RawTableSelector(table=SelectedTable(db_name="db", table_name="events"))
        source_rows = [
            Row(
                key="a",
                columns={
                    "name": "alpha",
                    "count": 1,
                    "ratio": 1.5,
                    "active": True,
                    "tags": ["x", "y"],
                    "scores": [1, 2],
                    "meta": {"k": "v"},
                    "maybe": None,
                    "payload": "text",
                    "key": "column-key",
                    "notes": [],
                    "lastUpdatedTime": "stale",
                },
                last_updated_time=10,
            ),
            Row(
                key="b",
                columns={
                    "name": "beta",
                    "count": 2,
                    "ratio": 2,
                    "active": False,
                    "tags": ["z"],
                    "scores": [3.5],
                    "extra": "only-here",
                    "maybe": "later",
                    "payload": {"a": 1},
                    "notes": ["remember"],
                    "lastUpdatedTime": "fresh",
                },
                last_updated_time=20,
            ),
        ]
        with monkeypatch_toolkit_client() as client:
            client.raw.tables.rows.iterate.return_value = [source_rows]
            io = RawIO(client, api_format="response")
            initial_schema = io.get_schema(selector)
            json_rows = [
                item.item for chunk in io.stream_data(selector) for item in io.data_to_json_chunk(chunk, selector).items
            ]

        assert {
            "initial_schema": initial_schema,
            "schema": io.get_schema(selector),
            "rows": [io.json_to_row(row) for row in json_rows],
        } == {
            "initial_schema": None,
            "schema": [
                SchemaColumn(name="key", type="string"),
                SchemaColumn(name="lastUpdatedTime", type="integer"),
                SchemaColumn(name="active", type="boolean"),
                SchemaColumn(name="columns.key", type="string"),
                SchemaColumn(name="columns.lastUpdatedTime", type="string"),
                SchemaColumn(name="count", type="integer"),
                SchemaColumn(name="extra", type="string"),
                SchemaColumn(name="maybe", type="string"),
                SchemaColumn(name="meta", type="json"),
                SchemaColumn(name="name", type="string"),
                SchemaColumn(name="notes", type="string", is_array=True),
                SchemaColumn(name="payload", type="json"),
                SchemaColumn(name="ratio", type="float"),
                SchemaColumn(name="scores", type="float", is_array=True),
                SchemaColumn(name="tags", type="string", is_array=True),
            ],
            "rows": [
                {
                    "name": "alpha",
                    "count": 1,
                    "ratio": 1.5,
                    "active": True,
                    "tags": ["x", "y"],
                    "scores": [1.0, 2.0],
                    "meta": {"k": "v"},
                    "maybe": None,
                    "payload": "text",
                    "columns.key": "column-key",
                    "notes": [],
                    "columns.lastUpdatedTime": "stale",
                    "key": "a",
                    "lastUpdatedTime": 10,
                },
                {
                    "name": "beta",
                    "count": 2,
                    "ratio": 2.0,
                    "active": False,
                    "tags": ["z"],
                    "scores": [3.5],
                    "extra": "only-here",
                    "maybe": "later",
                    "payload": {"a": 1},
                    "notes": ["remember"],
                    "columns.lastUpdatedTime": "fresh",
                    "key": "b",
                    "lastUpdatedTime": 20,
                },
            ],
        }
