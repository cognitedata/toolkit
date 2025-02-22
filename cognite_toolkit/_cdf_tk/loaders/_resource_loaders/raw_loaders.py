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
from __future__ import annotations

import itertools
from collections import defaultdict
from collections.abc import Hashable, Iterable, Sequence
from pathlib import Path
from typing import Any, cast, final

from cognite.client.data_classes.capabilities import (
    Capability,
    RawAcl,
)
from cognite.client.exceptions import CogniteAPIError
from cognite.client.utils.useful_types import SequenceNotStr
from rich import print
from rich.console import Console

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.data_classes.raw import RawDatabase, RawDatabaseList, RawTable, RawTableList
from cognite_toolkit._cdf_tk.loaders._base_loaders import ResourceContainerLoader, ResourceLoader

from .auth_loaders import GroupAllScopedLoader


@final
class RawDatabaseLoader(
    ResourceContainerLoader[RawDatabase, RawDatabase, RawDatabase, RawDatabaseList, RawDatabaseList]
):
    item_name = "raw tables"
    folder_name = "raw"
    filename_pattern = r"^(?!.*Table$).*$"
    resource_cls = RawDatabase
    resource_write_cls = RawDatabase
    list_cls = RawDatabaseList
    list_write_cls = RawDatabaseList
    kind = "Database"
    dependencies = frozenset({GroupAllScopedLoader})
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
        cls, items: Sequence[RawDatabase] | None, read_only: bool
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
                tables_by_database[item.db_name] = []

            scope = RawAcl.Scope.Table(dict(tables_by_database)) if tables_by_database else RawAcl.Scope.All()  # type: ignore[arg-type]

        return RawAcl(actions, scope)  # type: ignore[arg-type]

    @classmethod
    def get_id(cls, item: RawDatabase | dict) -> RawDatabase:
        if isinstance(item, dict):
            return RawDatabase(item["dbName"])
        return item

    @classmethod
    def dump_id(cls, id: RawDatabase) -> dict[str, Any]:
        return {"dbName": id.db_name}

    def create(self, items: RawDatabaseList) -> RawDatabaseList:
        database_list = self.client.raw.databases.create([db.db_name for db in items])
        return RawDatabaseList([RawDatabase(db_name=db.name) for db in database_list if db.name])

    def retrieve(self, ids: SequenceNotStr[RawDatabase]) -> RawDatabaseList:
        database_list = self.client.raw.databases.list(limit=-1)
        target_dbs = {db.db_name for db in ids}
        return RawDatabaseList([RawDatabase(db_name=db.name) for db in database_list if db.name in target_dbs])

    def delete(self, ids: SequenceNotStr[RawDatabase]) -> int:
        db_names = [table.db_name for table in ids]
        try:
            self.client.raw.databases.delete(db_names)
        except CogniteAPIError as e:
            # Bug in API, missing is returned as failed
            if e.failed and (db_names := [name for name in db_names if name not in e.failed]):
                self.client.raw.databases.delete(db_names)
            elif e.code == 404 and "not found" in e.message and "database" in e.message:
                return 0
            else:
                raise e
        return len(db_names)

    def _iterate(
        self,
        data_set_external_id: str | None = None,
        space: str | None = None,
        parent_ids: list[Hashable] | None = None,
    ) -> Iterable[RawDatabase]:
        return (RawDatabase(db_name=cast(str, db.name)) for db in self.client.raw.databases)

    def count(self, ids: SequenceNotStr[RawDatabase]) -> int:
        nr_of_tables = 0
        for db_name, raw_tables in itertools.groupby(sorted(ids, key=lambda x: x.db_name), key=lambda x: x.db_name):
            try:
                tables = self.client.raw.tables.list(db_name=db_name, limit=-1)
            except CogniteAPIError as e:
                if db_name in {item.get("name") for item in e.missing or []}:
                    continue
                raise e
            nr_of_tables += len(tables.data)
        return nr_of_tables

    def drop_data(self, ids: SequenceNotStr[RawDatabase]) -> int:
        nr_of_tables = 0
        for db_name, raw_tables in itertools.groupby(sorted(ids, key=lambda x: x.db_name), key=lambda x: x.db_name):
            try:
                existing = self.client.raw.tables.list(db_name=db_name, limit=-1).as_names()
            except CogniteAPIError as e:
                if db_name in {item.get("name") for item in e.missing or []}:
                    continue
                raise e
            if existing:
                self.client.raw.tables.delete(db_name=db_name, name=existing)
                nr_of_tables += len(existing)
        return nr_of_tables


@final
class RawTableLoader(ResourceContainerLoader[RawTable, RawTable, RawTable, RawTableList, RawTableList]):
    item_name = "raw rows"
    folder_name = "raw"
    filename_pattern = r"^(?!.*Database$).*$"
    resource_cls = RawTable
    resource_write_cls = RawTable
    list_cls = RawTableList
    list_write_cls = RawTableList
    kind = "Table"
    support_update = False
    dependencies = frozenset({RawDatabaseLoader, GroupAllScopedLoader})
    _doc_url = "Raw/operation/createTables"
    parent_resource = frozenset({RawDatabaseLoader})

    def __init__(self, client: ToolkitClient, build_dir: Path, console: Console | None):
        super().__init__(client, build_dir, console)
        self._printed_warning = False

    @property
    def display_name(self) -> str:
        return "raw tables"

    @classmethod
    def get_required_capability(
        cls, items: Sequence[RawTable] | None, read_only: bool
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
                tables_by_database[item.db_name].append(item.table_name)

            scope = RawAcl.Scope.Table(dict(tables_by_database)) if tables_by_database else RawAcl.Scope.All()  # type: ignore[arg-type]

        return RawAcl(actions, scope)  # type: ignore[arg-type]

    @classmethod
    def get_id(cls, item: RawTable | dict) -> RawTable:
        if isinstance(item, dict):
            if missing := tuple(k for k in {"dbName", "tableName"} if k not in item):
                # We need to raise a KeyError with all missing keys to get the correct error message.
                raise KeyError(*missing)
            return RawTable(item["dbName"], item["tableName"])
        return item

    @classmethod
    def dump_id(cls, id: RawTable) -> dict[str, Any]:
        return {"dbName": id.db_name, "tableName": id.table_name}

    @classmethod
    def get_dependent_items(cls, item: dict) -> Iterable[tuple[type[ResourceLoader], Hashable]]:
        if "dbName" in item:
            yield RawDatabaseLoader, RawDatabase(item["dbName"])

    def create(self, items: RawTableList) -> RawTableList:
        created = RawTableList([])
        for db_name, raw_tables in itertools.groupby(sorted(items, key=lambda x: x.db_name), key=lambda x: x.db_name):
            tables = [table.table_name for table in raw_tables]
            new_tables = self.client.raw.tables.create(db_name=db_name, name=tables)
            created.extend([RawTable(db_name=db_name, table_name=cast(str, table.name)) for table in new_tables])
        return created

    def retrieve(self, ids: SequenceNotStr[RawTable]) -> RawTableList:
        retrieved = RawTableList([])
        for db_name, raw_tables in itertools.groupby(sorted(ids, key=lambda x: x.db_name), key=lambda x: x.db_name):
            expected_tables = {table.table_name for table in raw_tables}
            try:
                tables = self.client.raw.tables.list(db_name=db_name, limit=-1)
            except CogniteAPIError as e:
                if db_name in {item.get("name") for item in e.missing or []}:
                    continue
                raise e
            retrieved.extend(
                [RawTable(db_name=db_name, table_name=table.name) for table in tables if table.name in expected_tables]
            )
        return retrieved

    def delete(self, ids: SequenceNotStr[RawTable]) -> int:
        count = 0
        for db_name, raw_tables in itertools.groupby(sorted(ids, key=lambda x: x.db_name), key=lambda x: x.db_name):
            tables = [table.table_name for table in raw_tables if table.table_name]
            if tables:
                try:
                    self.client.raw.tables.delete(db_name=db_name, name=tables)
                except CogniteAPIError as e:
                    if e.code != 404:
                        raise e
                    # Missing is returned as failed
                    missing = {item.get("name") for item in (e.missing or [])}.union(set(e.failed or []))
                    if "not found" in e.message and "database" in e.message:
                        continue
                    elif tables := [name for name in tables if name not in missing]:
                        self.client.raw.tables.delete(db_name=db_name, name=tables)
                    elif not tables:
                        # Table does not exist.
                        continue
                    else:
                        raise e
                count += len(tables)
        return count

    def _iterate(
        self,
        data_set_external_id: str | None = None,
        space: str | None = None,
        parent_ids: list[Hashable] | None = None,
    ) -> Iterable[RawTable]:
        for parent_id in parent_ids or (RawDatabase(cast(str, db.name)) for db in self.client.raw.databases):
            if not isinstance(parent_id, RawDatabase):
                continue
            for table in self.client.raw.tables(cast(str, parent_id.db_name)):
                yield RawTable(db_name=cast(str, parent_id.db_name), table_name=cast(str, table.name))

    def count(self, ids: SequenceNotStr[RawTable]) -> int:
        if not self._printed_warning:
            print("  [bold green]INFO:[/] Raw rows do not support count (there is no aggregation method).")
            self._printed_warning = True
        return -1

    def drop_data(self, ids: SequenceNotStr[RawTable]) -> int:
        for db_name, raw_tables in itertools.groupby(sorted(ids, key=lambda x: x.db_name), key=lambda x: x.db_name):
            try:
                existing = set(self.client.raw.tables.list(db_name=db_name, limit=-1).as_names())
            except CogniteAPIError as e:
                if db_name in {item.get("name") for item in e.missing or []}:
                    continue
                raise e
            tables = [table.table_name for table in raw_tables if table.table_name in existing]
            if tables:
                self.client.raw.tables.delete(db_name=db_name, name=tables)
        return -1
