from typing import Literal

from ._base import DataSelector, SelectorObject


class SelectedTable(SelectorObject):
    """Selected RAW table"""

    db_name: str
    table_name: str


class RawTableSelector(DataSelector):
    type: Literal["rawTable"] = "rawTable"
    table: SelectedTable

    @property
    def group(self) -> str:
        return self.table.db_name

    def __str__(self) -> str:
        return self.table.table_name
