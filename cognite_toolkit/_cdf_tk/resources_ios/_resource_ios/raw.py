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
from typing import Any, Literal, final

from cognite.client.exceptions import CogniteAPIError
from rich import print
from rich.console import Console

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client._resource_base import Identifier
from cognite_toolkit._cdf_tk.client.http_client import ToolkitAPIError
from cognite_toolkit._cdf_tk.client.identifiers import NameId, RawDatabaseId, RawTableId
from cognite_toolkit._cdf_tk.client.resource_classes.group import (
    AclType,
    AllScope,
    RawAcl,
    ScopeDefinition,
    TableScope,
)
from cognite_toolkit._cdf_tk.client.resource_classes.raw import (
    RAWDatabaseRequest,
    RAWDatabaseResponse,
    RAWTableRequest,
    RAWTableResponse,
)
from cognite_toolkit._cdf_tk.resources_ios._base_cruds import ResourceContainerIO, ResourceIO
from cognite_toolkit._cdf_tk.utils.acl_helper import as_read_list_write_actions
from cognite_toolkit._cdf_tk.yaml_classes import DatabaseYAML, TableYAML

from .auth import GroupAllScopedCRUD


@final
class RawDatabaseCRUD(ResourceContainerIO[RawDatabaseId, RAWDatabaseRequest, RAWDatabaseResponse]):
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
    def get_minimum_scope(cls, items: Sequence[RAWDatabaseRequest]) -> ScopeDefinition:
        return TableScope(dbs_to_tables={item.name: [] for item in items})

    @classmethod
    def create_acl(cls, actions: set[Literal["READ", "WRITE"]], scope: ScopeDefinition) -> Iterable[AclType]:
        if isinstance(scope, AllScope | TableScope):
            yield RawAcl(actions=as_read_list_write_actions(actions), scope=scope)

    @classmethod
    def get_id(cls, item: RAWDatabaseResponse | RAWDatabaseRequest | dict) -> RawDatabaseId:
        if isinstance(item, dict):
            return RawDatabaseId.model_validate(item)
        return RawDatabaseId(name=item.name)

    @classmethod
    def dump_id(cls, id: RawDatabaseId) -> dict[str, Any]:
        return {"dbName": id.name}

    def dump_resource(self, resource: RAWDatabaseResponse, local: dict[str, Any] | None = None) -> dict[str, Any]:
        return {"dbName": resource.name}

    def create(self, items: Sequence[RAWDatabaseRequest]) -> list[RAWDatabaseResponse]:
        return self.client.tool.raw.databases.create(items)

    def retrieve(self, ids: Sequence[RawDatabaseId]) -> list[RAWDatabaseResponse]:
        database_list = self.client.tool.raw.databases.list(limit=None)
        target_dbs = {db.name for db in ids}
        return [db for db in database_list if db.name in target_dbs]

    def delete(self, ids: Sequence[RawDatabaseId]) -> int:
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
        parent_ids: Sequence[Hashable] | None = None,
    ) -> Iterable[RAWDatabaseResponse]:
        for databases in self.client.tool.raw.databases.iterate(limit=None):
            yield from databases

    def count(self, ids: Sequence[RawDatabaseId]) -> int:
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

    def drop_data(self, ids: Sequence[RawDatabaseId]) -> int:
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
class RawTableCRUD(ResourceContainerIO[RawTableId, RAWTableRequest, RAWTableResponse]):
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
    def get_minimum_scope(cls, items: Sequence[RAWTableRequest]) -> ScopeDefinition:
        tables_by_database: dict[str, list[str]] = defaultdict(list)
        for item in items:
            tables_by_database[item.db_name].append(item.name)
        return TableScope(dbs_to_tables=dict(tables_by_database))

    @classmethod
    def create_acl(cls, actions: set[Literal["READ", "WRITE"]], scope: ScopeDefinition) -> Iterable[AclType]:
        if isinstance(scope, AllScope | TableScope):
            yield RawAcl(actions=as_read_list_write_actions(actions), scope=scope)

    @classmethod
    def get_id(cls, item: RAWTableResponse | RAWTableRequest | dict) -> RawTableId:
        if isinstance(item, dict):
            missing: list[str] = []
            if "dbName" not in item:
                missing.append("dbName")
            if "tableName" in item:
                table_key = "tableName"
            elif "name" in item:
                table_key = "name"
            else:
                table_key = "<missing>"
                missing.append("tableName")

            if missing:
                # We need to raise a KeyError with all missing keys to get the correct error message.
                raise KeyError(*missing)
            return RawTableId(db_name=item["dbName"], name=item[table_key])
        return RawTableId(db_name=item.db_name, name=item.name)

    @classmethod
    def dump_id(cls, id: RawTableId) -> dict[str, Any]:
        return {"dbName": id.db_name, "tableName": id.name}

    @classmethod
    def get_dependent_items(cls, item: dict) -> Iterable[tuple[type[ResourceIO], Hashable]]:
        if "dbName" in item:
            yield RawDatabaseCRUD, RawDatabaseId(name=item["dbName"])

    @classmethod
    def get_dependencies(cls, resource: TableYAML) -> Iterable[tuple[type[ResourceIO], Identifier]]:
        yield RawDatabaseCRUD, RawDatabaseId(name=resource.db_name)

    def dump_resource(self, resource: RAWTableResponse, local: dict[str, Any] | None = None) -> dict[str, Any]:
        return {"dbName": resource.db_name, "tableName": resource.name}

    def create(self, items: Sequence[RAWTableRequest]) -> list[RAWTableResponse]:
        return self.client.tool.raw.tables.create(items)

    def retrieve(self, ids: Sequence[RawTableId]) -> list[RAWTableResponse]:
        retrieved: list[RAWTableResponse] = []
        for db_name, raw_tables in itertools.groupby(sorted(ids, key=lambda x: x.db_name), key=lambda x: x.db_name):
            expected_tables = {table.name for table in raw_tables}
            try:
                tables = self.client.tool.raw.tables.list(db_name=db_name, limit=None)
            except ToolkitAPIError as e:
                if db_name in {item.get("name") for item in e.missing or []}:
                    continue
                raise e
            retrieved.extend(table for table in tables if table.name in expected_tables)
        return retrieved

    def delete(self, ids: Sequence[RawTableId]) -> int:
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
        parent_ids: Sequence[Hashable] | None = None,
    ) -> Iterable[RAWTableResponse]:
        if parent_ids is None:
            dbs = self.client.tool.raw.databases.list(limit=None)
            parent_ids = [RawDatabaseId(name=db.name) for db in dbs]
        for parent_id in parent_ids:
            if not isinstance(parent_id, NameId):
                continue
            for tables in self.client.tool.raw.tables.iterate(db_name=parent_id.name, limit=None):
                yield from tables

    def count(self, ids: Sequence[RawTableId]) -> int:
        if not self._printed_warning:
            print("  [bold green]INFO:[/] Raw rows do not support count (there is no aggregation method).")
            self._printed_warning = True
        return -1

    def drop_data(self, ids: Sequence[RawTableId]) -> int:
        self.delete(ids)
        return -1
