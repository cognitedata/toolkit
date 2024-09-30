from __future__ import annotations

from dataclasses import dataclass
from functools import total_ordering
from typing import Any

from cognite.client import CogniteClient
from cognite.client.data_classes._base import (
    CogniteResourceList,
    WriteableCogniteResource,
    WriteableCogniteResourceList,
)


@total_ordering
@dataclass(frozen=True)
class RawDatabaseTable(WriteableCogniteResource):
    db_name: str
    table_name: str | None = None

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> RawDatabaseTable:
        return cls(db_name=resource["dbName"], table_name=resource.get("tableName"))

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        dumped = {
            "dbName" if camel_case else "db_name": self.db_name,
        }
        if self.table_name is not None:
            dumped["tableName" if camel_case else "table_name"] = self.table_name
        return dumped

    def as_write(self) -> RawDatabaseTable:
        return self

    def __lt__(self, other: object) -> bool:
        if isinstance(other, RawDatabaseTable):
            if self.db_name == other.db_name:
                return (self.table_name or "") < (other.table_name or "")
            else:
                return self.db_name < other.db_name
        else:
            return NotImplemented

    def __eq__(self, other: object) -> bool:
        if isinstance(other, RawDatabaseTable):
            return self.db_name == other.db_name and self.table_name == other.table_name
        else:
            return NotImplemented

    def __str__(self) -> str:
        if self.table_name is None:
            return self.db_name
        else:
            return super().__str__()

    def __repr__(self) -> str:
        if self.table_name is None:
            return f"{type(self).__name__}(db_name='{self.db_name}')"
        else:
            return f"{type(self).__name__}(db_name='{self.db_name}', table_name='{self.table_name}')"


class RawTableList(WriteableCogniteResourceList[RawDatabaseTable, RawDatabaseTable]):
    _RESOURCE = RawDatabaseTable

    def as_write(self) -> CogniteResourceList[RawDatabaseTable]:
        return self

    def as_db_names(self) -> list[str]:
        return [table.db_name for table in self.data]
