from typing import Literal

from ._base import DataSelector, SelectorObject


class SelectedTable(SelectorObject):
    """Selected RAW table"""

    db_name: str
    table_name: str

    def __str__(self) -> str:
        return f"{self.db_name}.{self.table_name}"


class RawTableSelector(DataSelector):
    type: Literal["rawTable"] = "rawTable"
    kind: Literal["RawRows"] = "RawRows"
    table: SelectedTable
    key: str | None = None

    def __str__(self) -> str:
        return self.table.table_name

    @property
    def display_name(self) -> str:
        message = f"{self.table!s}"
        if self.key:
            message += f" (key={self.key})"
        return message
