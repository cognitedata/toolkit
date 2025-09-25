import pytest
from cognite.client.data_classes.raw import Row, RowList, RowWriteList

from cognite_toolkit._cdf_tk.client.data_classes.raw import RawTable
from cognite_toolkit._cdf_tk.client.testing import monkeypatch_toolkit_client
from cognite_toolkit._cdf_tk.storageio import RawIO
from cognite_toolkit._cdf_tk.utils.collection import chunker
from cognite_toolkit._cdf_tk.utils.useful_types import JsonVal


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
            )
            for i in range(100)
        ]
    )


class TestRawStorageIO:
    def test_download_upload(self, some_raw_tables: RowList) -> None:
        table = RawTable("test_db", "test_table")
        with monkeypatch_toolkit_client() as client:
            client.raw.rows.return_value = chunker(some_raw_tables, 10)
            io = RawIO(client)

            assert io.count(table) is None

            source = io.stream_data(table, limit=100)
            json_chunks: list[list[dict[str, JsonVal]]] = []
            for chunk in source:
                json_chunk = io.data_to_json_chunk(chunk)
                assert isinstance(json_chunk, list)
                assert len(json_chunk) == 10
                for item in json_chunk:
                    assert isinstance(item, dict)
                json_chunks.append(json_chunk)
            data_chunks = (io.json_chunk_to_data(chunk) for chunk in json_chunks)
            for data_chunk in data_chunks:
                io.upload_items(data_chunk, table)

            assert client.raw.rows.insert.call_count == 10  # 100 rows in chunks of 10
            uploaded_rows = RowWriteList([])
            for call in client.raw.rows.insert.call_args_list:
                uploaded_rows.extend(call[1].get("row", []))

            assert uploaded_rows.dump() == some_raw_tables.as_write().dump()
