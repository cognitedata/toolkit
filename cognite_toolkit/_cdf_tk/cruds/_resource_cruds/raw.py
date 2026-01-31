# Copyright 2023 Cognite AS
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import itertools
from collections import defaultdict
from collections.abc import Hashable, Iterable, Sequence
from pathlib import Path
from typing import Any, final

from cognite.client.data_classes.capabilities import (
    Capability,
    RawAcl,
)
from cognite.client.exceptions import CogniteAPIError
from cognite.client.utils.useful_types import SequenceNotStr
from rich import print
from rich.console import Console

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.http_client import ToolkitAPIError
from cognite_toolkit._cdf_tk.client.resource_classes.identifiers import NameId, RawTableId
from cognite_toolkit._cdf_tk.client.resource_classes.raw import (
    RAWDatabaseRequest,
    RAWDatabaseResponse,
    RAWTableRequest,
    RAWTableResponse,
)
from cognite_toolkit._cdf_tk.cruds._base_cruds import ResourceContainerCRUD, ResourceCRUD
from cognite_toolkit._cdf_tk.resource_classes import DatabaseYAML, TableYAML

from .auth import GroupAllScopedCRUD


@final
class RawDatabaseCRUD(ResourceContainerCRUD[NameId, RAWDatabaseRequest, RAWDatabaseResponse]):
    item_name = "raw tables"
    folder_name = "raw"
    resource_cls = RAWDatabaseResponse
    resource_write_cls = RAWDatabaseRequest
    kind = "Database"
    yaml_cls = DatabaseYAML
    dependencies = frozenset({GroupAllScopedCRUD})
    support_update = False
    _doc_url = "Raw/operation/createDBs"

    def __init__(self, client: ToolkitClient, build_dir: Path, console: Console | None):
        super().__init__(client, build_dir, console)
        self._loaded_db_names: set[str] = set()

    @property
    def display_name(self) -> str:
        return "raw databases"

    @classmethod
    def get_required_capability(
        cls, items: Sequence[RAWDatabaseRequest] | None, read_only: bool
    ) -> Capability | list[Capability]:
        if not items and items is not None:
            return []

        actions = (
            [RawAcl.Action.Read, RawAcl.Action.List]
            if read_only
            else [RawAcl.Action.Read, RawAcl.Action.Write, RawAcl.Action.List]
        )

        scope: RawAcl.Scope.All | RawAcl.Scope.Table = RawAcl.Scope.All()  # type: ignore[valid-type]
        if items:
            tables_by_database: dict[str, list[str]] = {}
            for item in items:
                tables_by_database[item.name] = []

            scope = RawAcl.Scope.Table(dict(tables_by_database)) if tables_by_database else RawAcl.Scope.All()

        return RawAcl(actions, scope)

    @classmethod
    def get_id(cls, item: RAWDatabaseResponse | RAWDatabaseRequest | dict) -> NameId:
        if isinstance(item, dict):
            return NameId(name=item["name"])
        return NameId(name=item.name)

    @classmethod
    def dump_id(cls, id: NameId) -> dict[str, Any]:
        return id.dump()

    def create(self, items: Sequence[RAWDatabaseRequest]) -> list[RAWDatabaseResponse]:
        return self.client.tool.raw.databases.create(items)

    def retrieve(self, ids: SequenceNotStr[NameId]) -> list[RAWDatabaseResponse]:
        database_list = self.client.tool.raw.databases.list(limit=None)
        target_dbs = {db.name for db in ids}
        return [db for db in database_list if db.name in target_dbs]

    def delete(self, ids: SequenceNotStr[NameId]) -> int:
        ids_list = list(ids)
        try:
            self.client.tool.raw.databases.delete(ids_list)
        except ToolkitAPIError as e:
            # Bug in API, missing is returned as failed
            if e.missing and (remaining := [db for db in ids_list if db.name not in e.missing]):
                self.client.tool.raw.databases.delete(remaining)
            elif e.code == 404 and "not found" in e.message and "database" in e.message:
                return 0
            else:
                raise e
        return len(ids_list)

    def _iterate(
        self,
        data_set_external_id: str | None = None,
        space: str | None = None,
        parent_ids: list[Hashable] | None = None,
    ) -> Iterable[RAWDatabaseResponse]:
        for databases in self.client.tool.raw.databases.iterate():
            yield from databases

    def count(self, ids: SequenceNotStr[NameId]) -> int:
        nr_of_tables = 0
        for db_name, raw_tables in itertools.groupby(sorted(ids, key=lambda x: x.name), key=lambda x: x.name):
            try:
                tables = self.client.tool.raw.tables.list(db_name=db_name, limit=None)
            except CogniteAPIError as e:
                if db_name in {item.get("name") for item in e.missing or []}:
                    continue
                raise e
            nr_of_tables += len(tables)
        return nr_of_tables

    def drop_data(self, ids: SequenceNotStr[NameId]) -> int:
        nr_of_tables = 0
        for db_name, raw_tables in itertools.groupby(sorted(ids, key=lambda x: x.name), key=lambda x: x.name):
            try:
                existing = self.client.tool.raw.tables.list(db_name=db_name, limit=None)
            except ToolkitAPIError as e:
                if db_name in {item.get("name") for item in e.missing or []}:
                    continue
                raise e
            if existing:
                self.client.tool.raw.tables.delete([table.as_id() for table in existing])
                nr_of_tables += len(existing)
        return nr_of_tables


@final
class RawTableCRUD(ResourceContainerCRUD[RawTableId, RAWTableRequest, RAWTableResponse]):
    item_name = "raw rows"
    folder_name = "raw"
    resource_cls = RAWTableResponse
    resource_write_cls = RAWTableRequest
    kind = "Table"
    yaml_cls = TableYAML
    support_update = False
    dependencies = frozenset({RawDatabaseCRUD, GroupAllScopedCRUD})
    _doc_url = "Raw/operation/createTables"
    parent_resource = frozenset({RawDatabaseCRUD})

    def __init__(self, client: ToolkitClient, build_dir: Path, console: Console | None):
        super().__init__(client, build_dir, console)
        self._printed_warning = False

    @property
    def display_name(self) -> str:
        return "raw tables"

    @classmethod
    def get_required_capability(
        cls, items: Sequence[RAWTableRequest] | None, read_only: bool
    ) -> Capability | list[Capability]:
        if not items and items is not None:
            return []

        actions = (
            [RawAcl.Action.Read, RawAcl.Action.List]
            if read_only
            else [RawAcl.Action.Read, RawAcl.Action.Write, RawAcl.Action.List]
        )

        scope: RawAcl.Scope.All | RawAcl.Scope.Table = RawAcl.Scope.All()  # type: ignore[valid-type]
        if items:
            tables_by_database = defaultdict(list)
            for item in items:
                tables_by_database[item.db_name].append(item.name)

            scope = RawAcl.Scope.Table(dict(tables_by_database)) if tables_by_database else RawAcl.Scope.All()

        return RawAcl(actions, scope)

    @classmethod
    def get_id(cls, item: RAWTableResponse | RAWTableRequest | dict) -> RawTableId:
        if isinstance(item, dict):
            if missing := tuple(k for k in {"dbName", "tableName"} if k not in item):
                # We need to raise a KeyError with all missing keys to get the correct error message.
                raise KeyError(*missing)
            return RawTableId(db_name=item["dbName"], name=item["tableName"])
        return RawTableId(db_name=item.db_name, name=item.name)

    @classmethod
    def dump_id(cls, id: RawTableId) -> dict[str, Any]:
        return {"dbName": id.db_name, "tableName": id.name}

    @classmethod
    def get_dependent_items(cls, item: dict) -> Iterable[tuple[type[ResourceCRUD], Hashable]]:
        if "dbName" in item:
            yield RawDatabaseCRUD, NameId(name=item["dbName"])

    def create(self, items: Sequence[RAWTableRequest]) -> list[RAWTableResponse]:
        return self.client.tool.raw.tables.create(items)

    def retrieve(self, ids: SequenceNotStr[RawTableId]) -> list[RAWTableResponse]:
        retrieved: list[RAWTableResponse] = []
        for db_name, raw_tables in itertools.groupby(sorted(ids, key=lambda x: x.db_name), key=lambda x: x.db_name):
            expected_tables = {table.name for table in raw_tables}
            try:
                tables = self.client.tool.raw.tables.list(db_name=db_name, limit=None)
            except ToolkitAPIError as e:
                if db_name in {item.get("name") for item in e.missing or []}:
                    continue
                raise e
            retrieved.extend(
                RAWTableResponse(db_name=db_name, name=table.name) for table in tables if table.name in expected_tables
            )
        return retrieved

    def delete(self, ids: SequenceNotStr[RawTableId]) -> int:
        count = 0
        for db_name, raw_tables in itertools.groupby(sorted(ids, key=lambda x: x.db_name), key=lambda x: x.db_name):
            tables_to_delete = [table for table in raw_tables if table.name]
            if tables_to_delete:
                try:
                    self.client.tool.raw.tables.delete(tables_to_delete)
                except ToolkitAPIError as e:
                    if e.code != 404:
                        raise e
                    missing = {item.get("name") for item in (e.missing or [])}
                    if "not found" in e.message and "database" in e.message:
                        continue
                    elif remaining := [t for t in tables_to_delete if t.name not in missing]:
                        self.client.tool.raw.tables.delete(remaining)
                    elif not remaining:
                        # Table does not exist.
                        continue
                    else:
                        raise e
                count += len(tables_to_delete)
        return count

    def _iterate(
        self,
        data_set_external_id: str | None = None,
        space: str | None = None,
        parent_ids: list[Hashable] | None = None,
    ) -> Iterable[RAWTableResponse]:
        if parent_ids is None:
            # RAWDatabases are hashable, so this is safe.
            dbs = self.client.tool.raw.databases.list(limit=None)  # type: ignore[assignment]
            parent_ids = [NameId(name=db.name) for db in dbs]
        # MyPy complains about parent_ids None here, but we just set it above.
        for parent_id in parent_ids:  # type: ignore[union-attr]
            if not isinstance(parent_id, NameId):
                continue
            for tables in self.client.tool.raw.tables.iterate(db_name=parent_id.name):
                yield from tables

    def count(self, ids: SequenceNotStr[RawTableId]) -> int:
        if not self._printed_warning:
            print("  [bold green]INFO:[/] Raw rows do not support count (there is no aggregation method).")
            self._printed_warning = True
        return -1

    def drop_data(self, ids: SequenceNotStr[RawTableId]) -> int:
        for db_name, raw_tables in itertools.groupby(sorted(ids, key=lambda x: x.db_name), key=lambda x: x.db_name):
            try:
                existing_tables = self.client.tool.raw.tables.list(db_name=db_name, limit=None)
                existing_names = {table.name for table in existing_tables}
            except CogniteAPIError as e:
                if db_name in {item.get("name") for item in e.missing or []}:
                    continue
                raise e
            tables_to_delete = [
                RAWTableResponse(db_name=db_name, name=table.name)
                for table in raw_tables
                if table.name in existing_names
            ]
            if tables_to_delete:
                self.client.tool.raw.tables.delete([table.as_id() for table in tables_to_delete])
        return -1
