from cognite.client.data_classes.raw import RowWriteList

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.data_classes.raw import (
    BooleanProfileColumn,
    NumberProfileColumn,
    ObjectProfileColumn,
    RawTable,
    StringProfileColumn,
    UnknownTypeProfileColumn,
    VectorProfileColumn,
)


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
