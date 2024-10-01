from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from cognite.client import CogniteClient
from cognite.client.data_classes._base import (
    WriteableCogniteResource,
    WriteableCogniteResourceList,
)


@dataclass(frozen=True)
class RawDatabase(WriteableCogniteResource):
    db_name: str

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> RawDatabase:
        return cls(db_name=resource["dbName"])

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return {"dbName" if camel_case else "db_name": self.db_name}

    def as_write(self) -> RawDatabase:
        return self


@dataclass(frozen=True)
class RawTable(WriteableCogniteResource):
    db_name: str
    table_name: str

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> RawTable:
        return cls(db_name=resource["dbName"], table_name=resource["tableName"])

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return {
            "dbName" if camel_case else "db_name": self.db_name,
            "tableName" if camel_case else "table_name": self.table_name,
        }

    def as_write(self) -> RawTable:
        return self


class RawDatabaseList(WriteableCogniteResourceList[RawDatabase, RawDatabase]):
    _RESOURCE = RawDatabase

    def as_write(self) -> RawDatabaseList:
        return self


class RawTableList(WriteableCogniteResourceList[RawTable, RawTable]):
    _RESOURCE = RawTable

    def as_write(self) -> RawTableList:
        return self
