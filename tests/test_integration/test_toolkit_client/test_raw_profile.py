import pytest
from cognite.client.data_classes.raw import RowWrite, RowWriteList

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.data_classes.raw import (
    BooleanProfileColumn,
    NumberProfileColumn,
    ObjectProfileColumn,
    StringProfileColumn,
    UnknownTypeProfileColumn,
    VectorProfileColumn,
)


@pytest.fixture()
def raw_data() -> RowWriteList:
    return RowWriteList(
        [
            RowWrite(
                key=f"row{i}",
                columns={
                    "StringCol": f"value{i % 3}",
                    "IntegerCol": i % 5,
                    "BooleanCol": [True, False][i % 2],
                    "FloatCol": i * 0.1,
                    "EmptyCol": None,
                    "ArrayCol": [i, i + 1, i + 2] if i % 2 == 0 else None,
                    "ObjectCol": {"nested_key": f"nested_value_{i}" if i % 2 == 0 else None},
                },
            )
            for i in range(10)
        ]
    )


class TestRawProfile:
    def test_raw_profile(self, toolkit_client: ToolkitClient, raw_data: RowWriteList):
        db_name = "toolkit_test_db"
        table_name = "toolkit_test_profiling_table"
        existing_dbs = toolkit_client.raw.databases.list(limit=-1)
        existing_table_names: set[str] = set()
        if db_name in {db.name for db in existing_dbs}:
            existing_table_names = {table.name for table in toolkit_client.raw.tables.list(db_name=db_name, limit=-1)}
        if table_name not in existing_table_names:
            toolkit_client.raw.rows.insert(db_name, table_name, raw_data, ensure_parent=True)

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
