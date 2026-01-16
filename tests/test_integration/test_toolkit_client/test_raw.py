import pytest
from cognite.client.data_classes.raw import RowWriteList

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.http_client import ToolkitAPIError
from cognite_toolkit._cdf_tk.client.resource_classes.legacy.raw import (
    BooleanProfileColumn,
    NumberProfileColumn,
    ObjectProfileColumn,
    RawTable,
    StringProfileColumn,
    UnknownTypeProfileColumn,
    VectorProfileColumn,
)
from cognite_toolkit._cdf_tk.client.resource_classes.raw import RAWDatabase, RAWTable


@pytest.fixture(scope="module")
def persistent_raw_database(toolkit_client: ToolkitClient) -> RAWDatabase:
    db_name = "persistent_test_db"
    db = RAWDatabase(name=db_name)
    all_dbs = toolkit_client.tool.raw.databases.list(limit=None)
    if db_name in {d.name for d in all_dbs}:
        return db
    created_db = toolkit_client.tool.raw.databases.create([db])
    assert len(created_db) == 1
    created_db = created_db[0]
    return created_db


class TestRawProfile:
    def test_raw_profile(self, toolkit_client: ToolkitClient, populated_raw_table: RawTable, raw_data: RowWriteList):
        db_name, table_name = populated_raw_table.db_name, populated_raw_table.table_name

        limit = len(raw_data) // 2
        results = toolkit_client.raw.profile(db_name, table_name, limit=limit)
        assert results.row_count == limit
        assert results.column_count == len(raw_data[0].columns)
        assert isinstance(results.columns["StringCol"], StringProfileColumn)
        assert isinstance(results.columns["IntegerCol"], NumberProfileColumn)
        assert isinstance(results.columns["BooleanCol"], BooleanProfileColumn)
        assert isinstance(results.columns["FloatCol"], NumberProfileColumn)
        assert isinstance(results.columns["EmptyCol"], UnknownTypeProfileColumn)
        assert isinstance(results.columns["ArrayCol"], VectorProfileColumn)
        assert isinstance(results.columns["ObjectCol"], ObjectProfileColumn)


class TestRAWDatabasesAPI:
    def test_delete_unknown(self, toolkit_client: ToolkitClient) -> None:
        client = toolkit_client

        with pytest.raises(ToolkitAPIError) as exc_info:
            client.tool.raw.databases.delete([RAWDatabase(name="this_db_does_not_exist")], recursive=True)

        error = exc_info.value
        assert error.missing == [{"name": "this_db_does_not_exist"}]


class TestRAWTablesAPI:
    def test_delete_unknown(self, toolkit_client: ToolkitClient, persistent_raw_database: RAWDatabase) -> None:
        client = toolkit_client
        db = persistent_raw_database

        with pytest.raises(ToolkitAPIError) as exc_info:
            client.tool.raw.tables.delete([RAWTable(db_name=db.name, name="this_table_does_not_exist")])

        error = exc_info.value
        assert error.missing == [{"name": "this_table_does_not_exist"}]
