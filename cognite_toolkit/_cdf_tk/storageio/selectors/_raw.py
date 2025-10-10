from typing import Literal

from cognite_toolkit._cdf_tk.resource_classes.raw_database_table import TableYAML

from ._base import DataSelector


class RawTableSelector(DataSelector):
    type: Literal["rawTable"] = "rawTable"
    table: TableYAML

    @property
    def group(self) -> str:
        return self.table.db_name

    def __str__(self) -> str:
        return self.table.table_name
