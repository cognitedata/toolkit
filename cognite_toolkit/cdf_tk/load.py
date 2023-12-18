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

import io
import itertools
import json
import re
from abc import ABC, abstractmethod
from collections import Counter, UserList
from collections.abc import Iterable, Sequence, Sized
from contextlib import suppress
from dataclasses import dataclass
from functools import total_ordering
from pathlib import Path
from typing import Any, Generic, Literal, TypeVar, Union, final

import pandas as pd
import yaml
from cognite.client import CogniteClient
from cognite.client.data_classes import (
    DataSet,
    DataSetList,
    ExtractionPipeline,
    ExtractionPipelineConfig,
    ExtractionPipelineList,
    FileMetadata,
    FileMetadataList,
    OidcCredentials,
    TimeSeries,
    TimeSeriesList,
    Transformation,
    TransformationList,
    TransformationSchedule,
    TransformationScheduleList,
    capabilities,
)
from cognite.client.data_classes._base import (
    CogniteObject,
    CogniteResource,
    CogniteResourceList,
)
from cognite.client.data_classes.capabilities import (
    Capability,
    DataModelInstancesAcl,
    DataModelsAcl,
    DataSetsAcl,
    ExtractionPipelinesAcl,
    FilesAcl,
    GroupsAcl,
    RawAcl,
    TimeSeriesAcl,
    TransformationsAcl,
)
from cognite.client.data_classes.data_modeling import (
    ContainerApply,
    ContainerApplyList,
    DataModelApply,
    DataModelApplyList,
    EdgeApply,
    EdgeApplyList,
    NodeApply,
    NodeApplyList,
    SpaceApply,
    SpaceApplyList,
    ViewApply,
    ViewApplyList,
)
from cognite.client.data_classes.data_modeling.ids import ContainerId, DataModelId, EdgeId, NodeId, ViewId
from cognite.client.data_classes.iam import Group, GroupList
from cognite.client.exceptions import CogniteAPIError, CogniteDuplicatedError, CogniteNotFoundError
from rich import print
from rich.table import Table
from typing_extensions import Self

from .delete import delete_instances
from .utils import CDFToolConfig, load_yaml_inject_variables


@dataclass
class RawTable(CogniteObject):
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


@dataclass
class LoadableNodes(CogniteObject):
    """
    This is a helper class for nodes that contains arguments that are required for writing the
    nodes to CDF.
    """

    auto_create_direct_relations: bool
    skip_on_version_conflict: bool
    replace: bool
    nodes: NodeApplyList

    def __len__(self):
        return len(self.nodes)

    def __iter__(self):
        return iter(self.nodes)

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            auto_create_direct_relations=resource["autoCreateDirectRelations"],
            skip_on_version_conflict=resource["skipOnVersionConflict"],
            replace=resource["replace"],
            nodes=NodeApplyList.load(resource["nodes"]),
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return {
            "autoCreateDirectRelations"
            if camel_case
            else "auto_create_direct_relations": self.auto_create_direct_relations,
            "skipOnVersionConflict" if camel_case else "skip_on_version_conflict": self.skip_on_version_conflict,
            "replace": self.replace,
            "nodes": self.nodes.dump(camel_case),
        }


@dataclass
class LoadableEdges(CogniteObject):
    """
    This is a helper class for edges that contains arguments that are required for writing the
    edges to CDF.
    """

    auto_create_start_nodes: bool
    auto_create_end_nodes: bool
    skip_on_version_conflict: bool
    replace: bool
    edges: EdgeApplyList

    def __len__(self):
        return len(self.edges)

    def __iter__(self):
        return iter(self.edges)

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            auto_create_start_nodes=resource["autoCreateStartNodes"],
            auto_create_end_nodes=resource["autoCreateEndNodes"],
            skip_on_version_conflict=resource["skipOnVersionConflict"],
            replace=resource["replace"],
            edges=EdgeApplyList.load(resource["edges"]),
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return {
            "autoCreateStartNodes" if camel_case else "auto_create_start_nodes": self.auto_create_start_nodes,
            "autoCreateEndNodes" if camel_case else "auto_create_end_nodes": self.auto_create_end_nodes,
            "skipOnVersionConflict" if camel_case else "skip_on_version_conflict": self.skip_on_version_conflict,
            "replace": self.replace,
            "edges": self.edges.dump(camel_case),
        }


@dataclass
class Difference:
    added: list[CogniteResource]
    removed: list[CogniteResource]
    changed: list[CogniteResource]
    unchanged: list[CogniteResource]

    def __iter__(self):
        return iter([self.added, self.removed, self.changed, self.unchanged])

    def __next__(self):
        return next([self.added, self.removed, self.changed, self.unchanged])


T_ID = TypeVar("T_ID", bound=Union[str, int])
T_Resource = TypeVar("T_Resource")
T_ResourceList = TypeVar("T_ResourceList")


class Loader(ABC, Generic[T_ID, T_Resource, T_ResourceList]):
    """
    This is the base class for all loaders. It defines the interface that all loaders must implement.

    A loader is a class that describes how a resource is loaded from a file and uploaded to CDF.

    All resources supported by the cognite_toolkit should implement a loader.

    Class attributes:
        support_drop: Whether the resource supports the drop flag.
        filetypes: The filetypes that are supported by this loader. If empty, all files are supported.
        api_name: The name of the api that is in the cognite_client that is used to interact with the CDF API.
        folder_name: The name of the folder in the build directory where the files are located.
        resource_cls: The class of the resource that is loaded.
        list_cls: The list version of the resource class.
        dependencies: A set of loaders that must be loaded before this loader.
        _display_name: The name of the resource that is used when printing messages. If this is not set the
            api_name is used.
    """

    support_drop = True
    support_upsert = False
    filetypes = frozenset({"yaml", "yml"})
    filename_pattern = ""
    api_name: str
    folder_name: str
    resource_cls: type[CogniteResource]
    list_cls: type[CogniteResourceList]
    identifier_key: str = "externalId"
    dependencies: frozenset[Loader] = frozenset()
    _display_name: str = ""

    def __init__(self, client: CogniteClient, ToolGlobals: CDFToolConfig):
        self.client = client
        self.ToolGlobals = ToolGlobals
        try:
            self.api_class = self._get_api_class(client, self.api_name)
        except AttributeError:
            raise AttributeError(f"Invalid api_name {self.api_name}.")

    @property
    def display_name(self):
        if self._display_name:
            return self._display_name
        return self.api_name

    @staticmethod
    def _get_api_class(client, api_name: str):
        parent = client
        if (dot_count := Counter(api_name)["."]) == 1:
            parent_name, api_name = api_name.split(".")
            parent = getattr(client, parent_name)
        elif dot_count == 0:
            pass
        else:
            raise AttributeError(f"Invalid api_name {api_name}.")
        return getattr(parent, api_name)

    @classmethod
    def create_loader(cls, ToolGlobals: CDFToolConfig):
        client = ToolGlobals.verify_capabilities(capability=cls.get_required_capability(ToolGlobals))
        return cls(client, ToolGlobals)

    @classmethod
    @abstractmethod
    def get_required_capability(cls, ToolGlobals: CDFToolConfig) -> Capability:
        raise NotImplementedError(f"get_required_capability must be implemented for {cls.__name__}.")

    @classmethod
    @abstractmethod
    def get_id(cls, item: T_Resource) -> T_ID:
        raise NotImplementedError

    @staticmethod
    def fixup_resource(local: T_Resource, remote: T_Resource) -> T_Resource:
        """Takes the local (to be pushed) and remote (from CDF) resource and returns the
        local resource with properties from the remote resource copied over to make
        them equal if we should consider them equal (and skip writing to CDF)."""
        return local

    def remove_unchanged(self, local: T_Resource | Sequence[T_Resource]) -> T_Resource | Sequence[T_Resource]:
        if not isinstance(local, Sequence):
            local = [local]
        if len(local) == 0:
            return local
        try:
            remote = self.retrieve([self.get_id(item) for item in local])
        except CogniteNotFoundError:
            return local
        if len(remote) == 0:
            return local
        for l_resource in local:
            for r in remote:
                if self.get_id(l_resource) == self.get_id(r):
                    r_yaml = self.resource_cls.dump_yaml(r)
                    # To avoid that we mess up the original local resource, we use the
                    # "through yaml copy"-trick to create a copy of the local resource.
                    copy_l = self.resource_cls.load(self.resource_cls.dump_yaml(l_resource))
                    l_yaml = self.resource_cls.dump_yaml(self.fixup_resource(copy_l, r))
                    if l_yaml == r_yaml:
                        local.remove(l_resource)
                        break
        return local

    # Default implementations that can be overridden
    def create(self, items: Sequence[T_Resource], drop: bool, filepath: Path) -> T_ResourceList:
        try:
            created = self.api_class.create(items)
            return created
        except CogniteAPIError as e:
            if e.code == 409:
                print("  [bold yellow]WARNING:[/] Resource(s) already exist(s), skipping creation.")
                return []
            else:
                print(f"[bold red]ERROR:[/] Failed to create resource(s).\n{e}")
                self.ToolGlobals.failed = True
                return []
        except CogniteDuplicatedError as e:
            print(
                f"  [bold yellow]WARNING:[/] {len(e.duplicated)} out of {len(items)} resource(s) already exist(s). {len(e.successful or [])} resource(s) created."
            )
            return []
        except Exception as e:
            print(f"[bold red]ERROR:[/] Failed to create resource(s).\n{e}")
            self.ToolGlobals.failed = True
            return []

    def delete(self, ids: Sequence[T_ID], drop_data: bool) -> int:
        self.api_class.delete(ids)
        return len(ids)

    def retrieve(self, ids: Sequence[T_ID]) -> T_ResourceList:
        return self.api_class.retrieve(ids)

    def load_resource(self, filepath: Path, dry_run: bool) -> T_Resource | T_ResourceList:
        raw_yaml = load_yaml_inject_variables(filepath, self.ToolGlobals.environment_variables())
        if isinstance(raw_yaml, list):
            return self.list_cls.load(raw_yaml)
        return self.resource_cls.load(raw_yaml)


@final
class AuthLoader(Loader[int, Group, GroupList]):
    support_drop = False
    support_upsert = True
    api_name = "iam.groups"
    folder_name = "auth"
    resource_cls = Group
    list_cls = GroupList
    identifier_key = "name"
    resource_scopes = frozenset(
        {
            capabilities.IDScope,
            capabilities.SpaceIDScope,
            capabilities.DataSetScope,
            capabilities.TableScope,
            capabilities.AssetRootIDScope,
            capabilities.ExtractionPipelineScope,
            capabilities.IDScopeLowerCase,
        }
    )

    def __init__(
        self,
        client: CogniteClient,
        ToolGlobals: CDFToolConfig,
        target_scopes: Literal[
            "all", "all_skipped_validation", "all_scoped_skipped_validation", "resource_scoped_only", "all_scoped_only"
        ] = "all",
    ):
        super().__init__(client, ToolGlobals)
        self.target_scopes = target_scopes

    @property
    def display_name(self):
        if self.target_scopes.startswith("all"):
            scope = "all"
        else:
            scope = "resource scoped"
        return f"{self.api_name}({scope})"

    @staticmethod
    def fixup_resource(local: T_Resource, remote: T_Resource) -> T_Resource:
        local.id = remote.id
        local.is_deleted = False  # If remote is_deleted, this will fail the check.
        local.metadata = remote.metadata  # metadata has no order guarantee, so we exclude it from compare
        local.deleted_time = remote.deleted_time
        return local

    @classmethod
    def create_loader(
        cls,
        ToolGlobals: CDFToolConfig,
        target_scopes: Literal[
            "all", "all_skipped_validation", "all_scoped_skipped_validation", "resource_scoped_only", "all_scoped_only"
        ] = "all",
    ):
        client = ToolGlobals.verify_capabilities(capability=cls.get_required_capability(ToolGlobals))
        return cls(client, ToolGlobals, target_scopes)

    @classmethod
    def get_required_capability(cls, ToolGlobals: CDFToolConfig) -> Capability | list[Capability]:
        return GroupsAcl(
            [GroupsAcl.Action.Read, GroupsAcl.Action.List, GroupsAcl.Action.Create, GroupsAcl.Action.Delete],
            GroupsAcl.Scope.All(),
        )

    @classmethod
    def get_id(cls, item: Group) -> str:
        return item.name

    def load_resource(self, filepath: Path, dry_run: bool) -> Group:
        raw = load_yaml_inject_variables(filepath, self.ToolGlobals.environment_variables())
        for capability in raw.get("capabilities", []):
            for _, values in capability.items():
                if len(values.get("scope", {}).get("datasetScope", {}).get("ids", [])) > 0:
                    if not dry_run and self.target_scopes not in [
                        "all_skipped_validation",
                        "all_scoped_skipped_validation",
                    ]:
                        values["scope"]["datasetScope"]["ids"] = [
                            self.ToolGlobals.verify_dataset(ext_id)
                            for ext_id in values.get("scope", {}).get("datasetScope", {}).get("ids", [])
                        ]
                    else:
                        values["scope"]["datasetScope"]["ids"] = [-1]

                if len(values.get("scope", {}).get("extractionPipelineScope", {}).get("ids", [])) > 0:
                    if not dry_run and self.target_scopes not in [
                        "all_skipped_validation",
                        "all_scoped_skipped_validation",
                    ]:
                        values["scope"]["extractionPipelineScope"]["ids"] = [
                            self.ToolGlobals.verify_extraction_pipeline(ext_id)
                            for ext_id in values.get("scope", {}).get("extractionPipelineScope", {}).get("ids", [])
                        ]
                    else:
                        values["scope"]["extractionPipelineScope"]["ids"] = [-1]
        return Group.load(raw)

    def retrieve(self, ids: Sequence[int]) -> T_ResourceList:
        remote = self.client.iam.groups.list(all=True).data
        found = [g for g in remote if g.name in ids]
        return found

    def delete(self, ids: Sequence[int], drop_data: bool) -> int:
        # Let's prevent that we delete groups we belong to
        try:
            groups = self.client.iam.groups.list().data
        except Exception as e:
            print(
                f"[bold red]ERROR:[/] Failed to retrieve the current service principal's groups. Aborting group deletion.\n{e}"
            )
            return
        my_source_ids = set()
        for g in groups:
            if g.source_id not in my_source_ids:
                my_source_ids.add(g.source_id)
        groups = self.retrieve(ids)
        for g in groups:
            if g.source_id in my_source_ids:
                print(
                    f"  [bold yellow]WARNING:[/] Not deleting group {g.name} with sourceId {g.source_id} as it is used by the current service principal."
                )
                print("     If you want to delete this group, you must do it manually.")
                if g.name not in ids:
                    print(f"    [bold red]ERROR[/] You seem to have duplicate groups of name {g.name}.")
                else:
                    ids.remove(g.name)
        found = [g.id for g in groups if g.name in ids]
        self.client.iam.groups.delete(found)
        return len(found)

    def create(self, items: Sequence[Group], drop: bool, filepath: Path) -> GroupList:
        if self.target_scopes == "all":
            to_create = items
        elif self.target_scopes == "all_skipped_validation":
            raise ValueError("all_skipped_validation is not supported for group creation as scopes would be wrong.")
        elif self.target_scopes == "resource_scoped_only":
            to_create = []
            for item in items:
                item.capabilities = [
                    capability for capability in item.capabilities if type(capability.scope) in self.resource_scopes
                ]
                if item.capabilities:
                    to_create.append(item)
        elif self.target_scopes == "all_scoped_only" or self.target_scopes == "all_scoped_skipped_validation":
            to_create = []
            for item in items:
                item.capabilities = [
                    capability for capability in item.capabilities if type(capability.scope) not in self.resource_scopes
                ]
                if item.capabilities:
                    to_create.append(item)
        else:
            raise ValueError(f"Invalid load value {self.target_scopes}")

        if len(to_create) == 0:
            return []
        # We MUST retrieve all the old groups BEFORE we add the new, if not the new will be deleted
        old_groups = self.client.iam.groups.list(all=True).data
        created = self.client.iam.groups.create(to_create)
        created_names = {g.name for g in created}
        to_delete = [g.id for g in old_groups if g.name in created_names]
        self.client.iam.groups.delete(to_delete)
        return created


@final
class DataSetsLoader(Loader[str, DataSet, DataSetList]):
    support_drop = False
    support_upsert = True
    api_name = "data_sets"
    folder_name = "data_sets"
    resource_cls = DataSet
    list_cls = DataSetList

    @classmethod
    def get_required_capability(cls, ToolGlobals: CDFToolConfig) -> Capability:
        return DataSetsAcl(
            [DataSetsAcl.Action.Read, DataSetsAcl.Action.Write],
            DataSetsAcl.Scope.All(),
        )

    def get_id(self, item: DataSet) -> str:
        return item.external_id

    def delete(self, ids: Sequence[str], drop_data: bool) -> int:
        raise NotImplementedError("CDF does not support deleting data sets.")

    def retrieve(self, ids: Sequence[str]) -> DataSetList:
        return self.client.data_sets.retrieve_multiple(external_ids=ids)

    @staticmethod
    def fixup_resource(local: DataSet, remote: DataSet) -> DataSet:
        """Sets the read-only properties, id, created_time, and last_updated_time, that are set on the server side.
        This is needed to make the comparison work.
        """

        local.id = remote.id
        local.created_time = remote.created_time
        local.last_updated_time = remote.last_updated_time
        return local

    def load_resource(self, filepath: Path, dry_run: bool) -> DataSetList:
        resource = load_yaml_inject_variables(filepath, {})

        data_sets = [resource] if isinstance(resource, dict) else resource

        for data_set in data_sets:
            if data_set.get("metadata"):
                for key, value in data_set["metadata"].items():
                    data_set["metadata"][key] = json.dumps(value)
        return DataSetList.load(data_sets)

    def create(self, items: Sequence[T_Resource], drop: bool, filepath: Path) -> T_ResourceList | None:
        created = DataSetList([], cognite_client=self.client)
        # There is a bug in the data set API, so only one duplicated data set is returned at the time,
        # so we need to iterate.
        while len(items.data) > 0:
            try:
                created.extend(DataSetList(self.client.data_sets.create(items)))
                return created
            except CogniteDuplicatedError as e:
                if len(e.duplicated) < len(items):
                    for dup in e.duplicated:
                        ext_id = dup.get("externalId", None)
                        for item in items:
                            if item.external_id == ext_id:
                                items.remove(item)
                else:
                    items.data = []
            except Exception as e:
                print(f"[bold red]ERROR:[/] Failed to create data sets.\n{e}")
                self.ToolGlobals.failed = True
                return None
        if len(created) == 0:
            return None
        else:
            return created


@final
class RawLoader(Loader[RawTable, RawTable, list[RawTable]]):
    api_name = "raw.rows"
    folder_name = "raw"
    resource_cls = RawTable
    list_cls = list[RawTable]
    identifier_key = "table_name"
    data_file_types = frozenset({"csv", "parquet"})

    @classmethod
    def get_required_capability(cls, ToolGlobals: CDFToolConfig) -> Capability:
        return RawAcl([RawAcl.Action.Read, RawAcl.Action.Write], RawAcl.Scope.All())

    @classmethod
    def get_id(cls, item: RawTable) -> RawTable:
        return item

    def delete(self, ids: Sequence[RawTable], drop_data: bool) -> int:
        count = 0
        for db_name, raw_tables in itertools.groupby(sorted(ids, key=lambda x: x.db_name), key=lambda x: x.db_name):
            # Raw tables do not have ignore_unknowns_ids, so we need to catch the error
            with suppress(CogniteAPIError):
                tables = [table.table_name for table in raw_tables]
                self.client.raw.tables.delete(db_name=db_name, name=tables)
                count += len(tables)
            if len(self.client.raw.tables.list(db_name=db_name, limit=-1).data) == 0:
                with suppress(CogniteAPIError):
                    self.client.raw.databases.delete(name=db_name)
        return count

    def create(self, items: Sequence[RawTable], drop: bool, filepath: Path) -> list[RawTable]:
        if len(items) != 1:
            raise ValueError("Raw tables must be loaded one at a time.")
        table = items[0]
        datafile = next(
            (
                file
                for file_type in self.data_file_types
                if (file := filepath.parent / f"{table.table_name}.{file_type}").exists()
            ),
            None,
        )
        if datafile is None:
            raise ValueError(f"Failed to find data file for {table.table_name} in {filepath.parent}")
        elif datafile.suffix == ".csv":
            # The replacement is used to ensure that we read exactly the same file on Windows and Linux
            file_content = datafile.read_bytes().replace(b"\r\n", b"\n").decode("utf-8")
            data = pd.read_csv(io.StringIO(file_content), dtype=str)
            data.fillna("", inplace=True)
        elif datafile.suffix == ".parquet":
            data = pd.read_parquet(filepath)
        else:
            raise NotImplementedError(f"Unsupported file type {datafile.suffix} for {table.table_name}")

        self.client.raw.rows.insert_dataframe(
            db_name=table.db_name,
            table_name=table.table_name,
            dataframe=data,
            ensure_parent=True,
        )
        return [table]


@final
class TimeSeriesLoader(Loader[str, TimeSeries, TimeSeriesList]):
    api_name = "time_series"
    folder_name = "timeseries"
    resource_cls = TimeSeriesList
    list_cls = TimeSeriesList
    dependencies = frozenset({DataSetsLoader})

    @classmethod
    def get_required_capability(cls, ToolGlobals: CDFToolConfig) -> Capability:
        return TimeSeriesAcl(
            [TimeSeriesAcl.Action.Read, TimeSeriesAcl.Action.Write],
            TimeSeriesAcl.Scope.DataSet([ToolGlobals.data_set_id])
            if ToolGlobals.data_set_id
            else TimeSeriesAcl.Scope.All(),
        )

    def get_id(self, item: TimeSeries) -> str:
        return item.external_id

    def retrieve(self, ids: Sequence[str]) -> TimeSeriesList:
        return self.client.time_series.retrieve_multiple(external_ids=ids, ignore_unknown_ids=True)

    def delete(self, ids: Sequence[str], drop_data: bool) -> int:
        self.client.time_series.delete(external_id=ids, ignore_unknown_ids=True)
        return len(ids)

    def load_resource(self, filepath: Path, dry_run: bool) -> TimeSeries | TimeSeriesList:
        resources = load_yaml_inject_variables(filepath, {})
        if not isinstance(resources, list):
            resources = [resources]
        for resource in resources:
            if resource.get("dataSetExternalId") is not None:
                ds_external_id = resource.pop("dataSetExternalId")
                resource["dataSetId"] = self.ToolGlobals.verify_dataset(ds_external_id) if not dry_run else -1
        return TimeSeriesList.load(resources)


@final
class TransformationLoader(Loader[str, Transformation, TransformationList]):
    api_name = "transformations"
    folder_name = "transformations"
    filename_pattern = (
        r"^(?:(?!\.schedule).)*$"  # Matches all yaml files except file names who's stem contain *.schedule.
    )
    resource_cls = Transformation
    list_cls = TransformationList
    dependencies = frozenset({DataSetsLoader, RawLoader})

    @classmethod
    def get_required_capability(cls, ToolGlobals: CDFToolConfig) -> Capability:
        scope = (
            TransformationsAcl.Scope.DataSet([ToolGlobals.data_set_id])
            if ToolGlobals.data_set_id
            else TransformationsAcl.Scope.All()
        )
        return TransformationsAcl(
            [TransformationsAcl.Action.Read, TransformationsAcl.Action.Write],
            scope,
        )

    def get_id(self, item: Transformation) -> str:
        return item.external_id

    def load_resource(self, filepath: Path, dry_run: bool) -> Transformation:
        raw = load_yaml_inject_variables(filepath, self.ToolGlobals.environment_variables())
        # The `authentication` key is custom for this template:

        source_oidc_credentials = raw.get("authentication", {}).get("read") or raw.get("authentication") or {}
        destination_oidc_credentials = raw.get("authentication", {}).get("write") or raw.get("authentication") or {}
        if raw.get("dataSetExternalId") is not None:
            ds_external_id = raw.pop("dataSetExternalId")
            raw["dataSetId"] = self.ToolGlobals.verify_dataset(ds_external_id) if not dry_run else -1

        transformation = Transformation.load(raw)
        transformation.source_oidc_credentials = source_oidc_credentials and OidcCredentials.load(
            source_oidc_credentials
        )
        transformation.destination_oidc_credentials = destination_oidc_credentials and OidcCredentials.load(
            destination_oidc_credentials
        )
        # Find the non-integer prefixed filename
        file_name = filepath.stem.split(".", 2)[1]
        sql_file = filepath.parent / f"{file_name}.sql"
        if not sql_file.exists():
            sql_file = filepath.parent / f"{transformation.external_id}.sql"
            if not sql_file.exists():
                raise FileNotFoundError(
                    f"Could not find sql file belonging to transformation {filepath.name}. Please run build again."
                )
        transformation.query = sql_file.read_text()
        return transformation

    def delete(self, ids: Sequence[str], drop_data: bool) -> int:
        self.client.transformations.delete(external_id=ids, ignore_unknown_ids=True)
        return len(ids)

    def create(self, items: Sequence[Transformation], drop: bool, filepath: Path) -> TransformationList:
        try:
            created = self.client.transformations.create(items)
        except CogniteDuplicatedError as e:
            print(
                f"  [bold yellow]WARNING:[/] {len(e.duplicated)} transformation(s) out of {len(items)} transformation(s) already exist(s):"
            )
            for dup in e.duplicated:
                print(f"           {dup.get('externalId', 'N/A')}")
            return []
        except Exception as e:
            print(f"[bold red]ERROR:[/] Failed to create resource(s).\n{e}")
            self.ToolGlobals.failed = True
            return TransformationList([])
        return created


@final
class TransformationScheduleLoader(Loader[str, TransformationSchedule, TransformationScheduleList]):
    api_name = "transformations.schedules"
    folder_name = "transformations"
    filename_pattern = r"^.*\.schedule$"  # Matches all yaml files who's stem contain *.schedule.
    resource_cls = TransformationSchedule
    list_cls = TransformationScheduleList
    dependencies = frozenset({TransformationLoader})

    @classmethod
    def get_required_capability(cls, ToolGlobals: CDFToolConfig) -> Capability:
        scope = (
            TransformationsAcl.Scope.DataSet([ToolGlobals.data_set_id])
            if ToolGlobals.data_set_id
            else TransformationsAcl.Scope.All()
        )
        return TransformationsAcl(
            [TransformationsAcl.Action.Read, TransformationsAcl.Action.Write],
            scope,
        )

    def get_id(self, item: Transformation) -> str:
        return item.external_id

    def load_resource(self, filepath: Path, dry_run: bool) -> TransformationSchedule:
        raw = load_yaml_inject_variables(filepath, self.ToolGlobals.environment_variables())
        return TransformationSchedule.load(raw)

    def delete(self, ids: Sequence[str], drop_data: bool) -> int:
        try:
            self.client.transformations.schedules.delete(external_id=ids, ignore_unknown_ids=False)
            return len(ids)
        except CogniteNotFoundError as e:
            return len(ids) - len(e.not_found)

    def create(self, items: Sequence[TransformationSchedule], drop: bool, filepath: Path) -> TransformationScheduleList:
        try:
            return self.client.transformations.schedules.create(items)
        except CogniteDuplicatedError as e:
            existing = {external_id for dup in e.duplicated if (external_id := dup.get("externalId", None))}
            print(
                f"  [bold yellow]WARNING:[/] {len(e.duplicated)} transformation schedules already exist(s): {existing}"
            )
            new_items = [item for item in items if item.external_id not in existing]
            if len(new_items) == 0:
                return TransformationScheduleList([])
            try:
                return self.client.transformations.schedules.create(new_items)
            except CogniteAPIError as e:
                print(f"[bold red]ERROR:[/] Failed to create resource(s).\n{e}")
                self.ToolGlobals.failed = True
                return TransformationScheduleList([])
        except CogniteAPIError as e:
            print(f"[bold red]ERROR:[/] Failed to create resource(s).\n{e}")
            self.ToolGlobals.failed = True
            return TransformationScheduleList([])


@final
class DatapointsLoader(Loader[list[str], Path, TimeSeriesList]):
    support_drop = False
    filetypes = frozenset({"csv", "parquet"})
    api_name = "time_series.data"
    folder_name = "timeseries_datapoints"
    resource_cls = pd.DataFrame
    dependencies = frozenset({TimeSeriesLoader})

    @classmethod
    def get_required_capability(cls, ToolGlobals: CDFToolConfig) -> Capability:
        scope = (
            TimeSeriesAcl.Scope.DataSet([ToolGlobals.data_set_id])
            if ToolGlobals.data_set_id
            else TimeSeriesAcl.Scope.All()
        )

        return TimeSeriesAcl(
            [TimeSeriesAcl.Action.Read, TimeSeriesAcl.Action.Write],
            scope,
        )

    def load_resource(self, filepath: Path, dry_run: bool) -> Path:
        return filepath

    @classmethod
    def get_id(cls, item: Path) -> list[str]:
        raise NotImplementedError

    def delete(self, ids: Sequence[str], drop_data: bool) -> int:
        # Drop all datapoints?
        raise NotImplementedError()

    def create(self, items: Sequence[Path], drop: bool, filepath: Path) -> TimeSeriesList:
        if len(items) != 1:
            raise ValueError("Datapoints must be loaded one at a time.")
        datafile = items[0]
        if datafile.suffix == ".csv":
            data = pd.read_csv(datafile, parse_dates=True, dayfirst=True, index_col=0)
        elif datafile.suffix == ".parquet":
            data = pd.read_parquet(datafile, engine="pyarrow")
        else:
            raise ValueError(f"Unsupported file type {datafile.suffix} for {datafile.name}")
        self.client.time_series.data.insert_dataframe(data)
        external_ids = [col for col in data.columns if not pd.api.types.is_datetime64_any_dtype(data[col])]
        return TimeSeriesList([TimeSeries(external_id=external_id) for external_id in external_ids])


@final
class ExtractionPipelineLoader(Loader[str, ExtractionPipeline, ExtractionPipelineList]):
    support_drop = True
    api_name = "extraction_pipelines"
    folder_name = "extraction_pipelines"
    filename_pattern = r"^(?:(?!\.config).)*$"  # Matches all yaml files except file names who's stem contain *.config.
    resource_cls = ExtractionPipeline
    list_cls = ExtractionPipelineList
    dependencies = frozenset({DataSetsLoader, RawLoader})

    @classmethod
    def get_required_capability(cls, ToolGlobals: CDFToolConfig) -> Capability:
        return ExtractionPipelinesAcl(
            [ExtractionPipelinesAcl.Action.Read, ExtractionPipelinesAcl.Action.Write],
            ExtractionPipelinesAcl.Scope.All(),
        )

    def get_id(self, item: ExtractionPipeline) -> str:
        return item.external_id

    def delete(self, ids: Sequence[str], drop_data: bool) -> int:
        try:
            self.client.extraction_pipelines.delete(external_id=ids)
            return len(ids)
        except CogniteNotFoundError as e:
            print(
                f"  [bold yellow]WARNING:[/] {len(e.not_found)} out of {len(ids)} extraction pipelines do(es) not exist."
            )

            for dup in e.not_found:
                ext_id = dup.get("externalId", None)
                ids.remove(ext_id)

            if len(ids) > 0:
                self.client.extraction_pipelines.delete(external_id=ids)
                return len(ids)
            return 0

    def load_resource(self, filepath: Path, dry_run: bool) -> ExtractionPipeline:
        resource = load_yaml_inject_variables(filepath, {})

        if resource.get("dataSetExternalId") is not None:
            ds_external_id = resource.pop("dataSetExternalId")
            resource["dataSetId"] = self.ToolGlobals.verify_dataset(ds_external_id) if not dry_run else -1

        return ExtractionPipeline.load(resource)

    def create(self, items: Sequence[ExtractionPipeline], drop: bool, filepath: Path) -> ExtractionPipelineList:
        try:
            return self.client.extraction_pipelines.create(items)
        except CogniteDuplicatedError as e:
            if len(e.duplicated) < len(items):
                for dup in e.duplicated:
                    ext_id = dup.get("externalId", None)
                    for item in items:
                        if item.external_id == ext_id:
                            items.remove(item)
                try:
                    return self.client.extraction_pipelines.create(items)
                except Exception as e:
                    print(f"[bold red]ERROR:[/] Failed to create extraction pipelines.\n{e}")
                    self.ToolGlobals.failed = True
                    return ExtractionPipelineList([])


@final
class ExtractionPipelineConfigLoader(Loader[str, ExtractionPipelineConfig, list[ExtractionPipelineConfig]]):
    support_drop = True
    api_name = "extraction_pipelines.config"
    folder_name = "extraction_pipelines"
    filename_pattern = r"^.*\.config$"
    resource_cls = ExtractionPipelineConfig
    dependencies = frozenset({ExtractionPipelineLoader})

    @classmethod
    def get_required_capability(cls, ToolGlobals: CDFToolConfig) -> Capability:
        return ExtractionPipelinesAcl(
            [ExtractionPipelinesAcl.Action.Read, ExtractionPipelinesAcl.Action.Write],
            ExtractionPipelinesAcl.Scope.All(),
        )

    def get_id(self, item: ExtractionPipeline) -> str:
        return item.external_id

    def load_resource(self, filepath: Path, dry_run: bool) -> ExtractionPipelineConfig:
        resource = load_yaml_inject_variables(filepath, {})
        try:
            resource["config"] = yaml.dump(resource.get("config", ""), indent=4)
        except Exception:
            print(
                "[yellow]WARNING:[/] configuration could not be parsed as valid YAML, which is the recommended format.\n"
            )
            resource["config"] = resource.get("config", "")
        return ExtractionPipelineConfig.load(resource)

    def create(
        self, items: Sequence[ExtractionPipelineConfig], drop: bool, filepath: Path
    ) -> list[ExtractionPipelineConfig]:
        try:
            return [self.client.extraction_pipelines.config.create(items[0])]
        except Exception as e:
            print(f"[bold red]ERROR:[/] Failed to create extraction pipelines.\n{e}")
            self.ToolGlobals.failed = True
            return ExtractionPipelineConfig()

    def delete(self, ids: Sequence[str], drop_data: bool) -> int:
        configs = self.client.extraction_pipelines.config.list(external_id=ids)
        return len(configs)


@final
class FileLoader(Loader[str, FileMetadata, FileMetadataList]):
    api_name = "files"
    filetypes = frozenset({"yaml", "yml"})
    folder_name = "files"
    resource_cls = FileMetadata
    list_cls = FileMetadataList
    dependencies = frozenset({DataSetsLoader})

    @classmethod
    def get_required_capability(cls, ToolGlobals: CDFToolConfig) -> Capability:
        if ToolGlobals.data_set_id is None:
            scope = FilesAcl.Scope.All()
        else:
            scope = FilesAcl.Scope.DataSet([ToolGlobals.data_set_id])

        return FilesAcl([FilesAcl.Action.Read, FilesAcl.Action.Write], scope)

    @classmethod
    def get_id(cls, item: FileMetadata) -> str:
        return item.external_id

    def delete(self, ids: Sequence[str], drop_data: bool) -> int:
        self.client.files.delete(external_id=ids)
        return len(ids)

    def load_resource(self, filepath: Path, dry_run: bool) -> FileMetadata | FileMetadataList:
        try:
            resource = load_yaml_inject_variables(filepath, self.ToolGlobals.environment_variables())
            if resource.get("dataSetExternalId") is not None:
                ds_external_id = resource.pop("dataSetExternalId")
                resource["dataSetId"] = self.ToolGlobals.verify_dataset(ds_external_id) if not dry_run else -1
            files = FileMetadataList([FileMetadata.load(resource)])
        except Exception:
            files = FileMetadataList.load(
                load_yaml_inject_variables(filepath, self.ToolGlobals.environment_variables())
            )
        # If we have a file with exact one file config, check to see if this is a pattern to expand
        if len(files.data) == 1 and ("$FILENAME" in files.data[0].external_id or ""):
            # It is, so replace this file with all files in this folder using the same data
            file_data = files.data[0]
            ext_id_pattern = file_data.external_id
            files = FileMetadataList([], cognite_client=self.client)
            for file in filepath.parent.glob("*"):
                if file.suffix[1:] in ["yaml", "yml"]:
                    continue
                files.append(
                    FileMetadata(
                        name=file.name,
                        external_id=re.sub(r"\$FILENAME", file.name, ext_id_pattern),
                        data_set_id=file_data.data_set_id,
                        source=file_data.source,
                        metadata=file_data.metadata,
                        directory=file_data.directory,
                        asset_ids=file_data.asset_ids,
                        labels=file_data.labels,
                        geo_location=file_data.geo_location,
                        security_categories=file_data.security_categories,
                    )
                )
        for file in files.data:
            if not Path(filepath.parent / file.name).exists():
                raise FileNotFoundError(f"Could not find file {file.name} referenced in filepath {filepath.name}")
            if isinstance(file.data_set_id, str):
                # Replace external_id with internal id
                file.data_set_id = self.ToolGlobals.verify_dataset(file.data_set_id) if not dry_run else -1
        return files

    def create(self, items: Sequence[FileMetadata], drop: bool, filepath: Path) -> FileMetadataList:
        created = FileMetadataList([])
        for meta in items:
            datafile = filepath.parent / meta.name
            try:
                created.append(self.client.files.upload(path=datafile, overwrite=drop, **meta.dump(camel_case=False)))
            except CogniteAPIError as e:
                if e.code == 409:
                    print(f"  [bold yellow]WARNING:[/] File {meta.external_id} already exists, skipping upload.")
            except Exception as e:
                print(f"[bold red]ERROR:[/] Failed to upload file {datafile.name}.\n{e}")
                self.ToolGlobals.failed = True
                return created
        return created


@final
class SpaceLoader(Loader[str, SpaceApply, SpaceApplyList]):
    api_name = "data_modeling.spaces"
    folder_name = "data_models"
    filename_pattern = r"^.*\.?(space)$"
    resource_cls = SpaceApply
    list_cls = SpaceApplyList
    _display_name = "spaces"

    @classmethod
    def get_required_capability(cls, ToolGlobals: CDFToolConfig) -> list[Capability]:
        return [
            DataModelsAcl(
                [DataModelsAcl.Action.Read, DataModelsAcl.Action.Write],
                DataModelsAcl.Scope.All(),
            ),
            # Needed to delete instances
            DataModelInstancesAcl(
                [DataModelInstancesAcl.Action.Read, DataModelInstancesAcl.Action.Write],
                DataModelInstancesAcl.Scope.All(),
            ),
        ]

    @classmethod
    def get_id(cls, item: SpaceApply) -> str:
        return item.space

    def delete(self, ids: Sequence[str], drop_data: bool) -> int:
        if not drop_data:
            print("  [bold]INFO:[/] Skipping deletion of spaces as drop_data flag is not set...")
            return 0
        print("[bold]Deleting existing data...[/]")
        for space in ids:
            delete_instances(
                ToolGlobals=self.ToolGlobals,
                space_name=space,
            )

        deleted = self.client.data_modeling.spaces.delete(ids)
        return len(deleted)

    def create(self, items: Sequence[SpaceApply], drop: bool, filepath: Path) -> T_ResourceList:
        return self.client.data_modeling.spaces.apply(items)


class ContainerLoader(Loader[ContainerId, ContainerApply, ContainerApplyList]):
    api_name = "data_modeling.containers"
    folder_name = "data_models"
    filename_pattern = r"^.*\.?(container)$"
    resource_cls = ContainerApply
    list_cls = ContainerApplyList
    dependencies = frozenset({SpaceLoader})

    _display_name = "containers"

    @classmethod
    def get_required_capability(cls, ToolGlobals: CDFToolConfig) -> Capability:
        # Todo Scoped to spaces
        return DataModelsAcl(
            [DataModelsAcl.Action.Read, DataModelsAcl.Action.Write],
            DataModelsAcl.Scope.All(),
        )

    @classmethod
    def get_id(cls, item: ContainerApply) -> ContainerId:
        return item.as_id()

    def delete(self, ids: Sequence[ContainerId], drop_data: bool) -> int:
        if not drop_data:
            print("  [bold]INFO:[/] Skipping deletion of containers as drop_data flag is not set...")
            return 0
        deleted = self.client.data_modeling.containers.delete(ids)
        return len(deleted)

    def create(self, items: Sequence[ContainerApply], drop: bool, filepath: Path) -> T_ResourceList:
        self.ToolGlobals.verify_spaces(list({item.space for item in items}))

        return self.client.data_modeling.containers.apply(items)


class ViewLoader(Loader[ViewId, ViewApply, ViewApplyList]):
    api_name = "data_modeling.views"
    folder_name = "data_models"
    filename_pattern = r"^.*\.?(view)$"
    resource_cls = ViewApply
    list_cls = ViewApplyList
    dependencies = frozenset({SpaceLoader, ContainerLoader})

    _display_name = "views"

    @classmethod
    def get_required_capability(cls, ToolGlobals: CDFToolConfig) -> Capability:
        # Todo Scoped to spaces
        return DataModelsAcl(
            [DataModelsAcl.Action.Read, DataModelsAcl.Action.Write],
            DataModelsAcl.Scope.All(),
        )

    @classmethod
    def get_id(cls, item: ViewApply) -> ViewId:
        return item.as_id()

    def create(self, items: Sequence[T_Resource], drop: bool, filepath: Path) -> T_ResourceList:
        self.ToolGlobals.verify_spaces(list({item.space for item in items}))
        return self.client.data_modeling.views.apply(items)


@final
class DataModelLoader(Loader[DataModelId, DataModelApply, DataModelApplyList]):
    api_name = "data_modeling.data_models"
    folder_name = "data_models"
    filename_pattern = r"^.*\.?(datamodel)$"
    resource_cls = DataModelApply
    list_cls = DataModelApplyList
    dependencies = frozenset({SpaceLoader, ViewLoader})
    _display_name = "data models"

    @classmethod
    def get_required_capability(cls, ToolGlobals: CDFToolConfig) -> Capability:
        # Todo Scoped to spaces
        return DataModelsAcl(
            [DataModelsAcl.Action.Read, DataModelsAcl.Action.Write],
            DataModelsAcl.Scope.All(),
        )

    @classmethod
    def get_id(cls, item: DataModelApply) -> DataModelId:
        return item.as_id()

    def create(self, items: Sequence[T_Resource], drop: bool, filepath: Path) -> T_ResourceList:
        self.ToolGlobals.verify_spaces(list({item.space for item in items}))
        return self.client.data_modeling.data_models.apply(items)


@final
class NodeLoader(Loader[list[NodeId], NodeApply, LoadableNodes]):
    api_name = "data_modeling.instances"
    folder_name = "data_models"
    filename_pattern = r"^.*\.?(node)$"
    resource_cls = NodeApply
    list_cls = LoadableNodes
    dependencies = frozenset({SpaceLoader, ViewLoader, ContainerLoader})
    _display_name = "nodes"

    @classmethod
    def get_required_capability(cls, ToolGlobals: CDFToolConfig) -> Capability:
        # Todo Scoped to spaces
        return DataModelInstancesAcl(
            [DataModelInstancesAcl.Action.Read, DataModelInstancesAcl.Action.Write],
            DataModelInstancesAcl.Scope.All(),
        )

    def get_id(self, item: NodeApply) -> NodeId:
        return item.as_id()

    def load_resource(self, filepath: Path, dry_run: bool) -> LoadableNodes:
        raw = load_yaml_inject_variables(filepath, self.ToolGlobals.environment_variables())
        if isinstance(raw, list):
            raise ValueError(f"Unexpected node yaml file format {filepath.name}")
        return LoadableNodes.load(raw, cognite_client=self.client)

    def delete(self, ids: Sequence[NodeId], drop_data: bool) -> int:
        if not drop_data:
            print("  [bold]INFO:[/] Skipping deletion of nodes as drop_data flag is not set...")
            return 0
        deleted = self.client.data_modeling.instances.delete(nodes=ids)
        return len(deleted.nodes)

    def create(self, items: Sequence[LoadableNodes], drop: bool, filepath: Path) -> LoadableNodes:
        if not isinstance(items, LoadableNodes):
            raise ValueError("Unexpected node format file format")
        self.ToolGlobals.verify_spaces(list({item.space for item in items}))
        item = items
        _ = self.client.data_modeling.instances.apply(
            nodes=item.nodes,
            auto_create_direct_relations=item.auto_create_direct_relations,
            skip_on_version_conflict=item.skip_on_version_conflict,
            replace=item.replace,
        )
        return items


@final
class EdgeLoader(Loader[EdgeId, EdgeApply, LoadableEdges]):
    api_name = "data_modeling.instances"
    folder_name = "data_models"
    filename_pattern = r"^.*\.?(edge)$"
    resource_cls = EdgeApply
    list_cls = LoadableEdges
    _display_name = "edges"

    # Note edges do not need nodes to be created first, as they are created as part of the edge creation.
    # However, for deletion (reversed order) we need to delete edges before nodes.
    dependencies = frozenset({SpaceLoader, ViewLoader, NodeLoader})

    @classmethod
    def get_required_capability(cls, ToolGlobals: CDFToolConfig) -> Capability:
        # Todo Scoped to spaces
        return DataModelInstancesAcl(
            [DataModelInstancesAcl.Action.Read, DataModelInstancesAcl.Action.Write],
            DataModelInstancesAcl.Scope.All(),
        )

    def get_id(self, item: EdgeApply) -> EdgeId:
        return item.as_id()

    def load_resource(self, filepath: Path, dry_run: bool) -> LoadableEdges:
        raw = load_yaml_inject_variables(filepath, self.ToolGlobals.environment_variables())
        if isinstance(raw, list):
            raise ValueError(f"Unexpected edge yaml file format {filepath.name}")
        return LoadableEdges.load(raw, cognite_client=self.client)

    def delete(self, ids: Sequence[EdgeId], drop_data: bool) -> int:
        if not drop_data:
            print("  [bold]INFO:[/] Skipping deletion of edges as drop_data flag is not set...")
            return 0
        deleted = self.client.data_modeling.instances.delete(edges=ids)
        return len(deleted.edges)

    def create(self, items: Sequence[LoadableEdges], drop: bool, filepath: Path) -> LoadableEdges:
        if not isinstance(items, LoadableEdges):
            raise ValueError("Unexpected edge format file format")
        self.ToolGlobals.verify_spaces(list({item.space for item in items}))
        item = items
        _ = self.client.data_modeling.instances.apply(
            edges=item.edges,
            auto_create_start_nodes=item.auto_create_start_nodes,
            auto_create_end_nodes=item.auto_create_end_nodes,
            skip_on_version_conflict=item.skip_on_version_conflict,
            replace=item.replace,
        )
        return items


@total_ordering
@dataclass
class DeployResult:
    name: str
    created: int
    deleted: int
    skipped: int
    total: int

    def __lt__(self, other: DeployResult) -> bool:
        if isinstance(other, DeployResult):
            return self.name < other.name
        else:
            return NotImplemented

    def __eq__(self, other: DeployResult) -> bool:
        if isinstance(other, DeployResult):
            return self.name == other.name
        else:
            return NotImplemented


class DeployResults(UserList):
    def __init__(self, collection: Iterable[DeployResult], action: Literal["deploy", "clean"], dry_run: bool = False):
        super().__init__(collection)
        self.action = action
        self.dry_run = dry_run

    def create_rich_table(self) -> Table:
        table = Table(title=f"Summary of {self.action} command:")
        prefix = ""
        if self.dry_run:
            prefix = "Would have "
        table.add_column("Resource", justify="right")
        table.add_column(f"{prefix}Created", justify="right", style="green")
        table.add_column(f"{prefix}Deleted", justify="right", style="red")
        table.add_column(f"{prefix}Skipped", justify="right", style="yellow")
        table.add_column("Total", justify="right")
        for item in sorted(entry for entry in self.data if entry is not None):
            table.add_row(
                item.name,
                str(item.created),
                str(item.deleted),
                str(item.skipped),
                str(item.total),
            )

        return table


def deploy_or_clean_resources(
    loader: Loader,
    path: Path,
    ToolGlobals: CDFToolConfig,
    drop: bool = False,
    clean: bool = False,
    action: Literal["deploy", "clean"] = "deploy",
    dry_run: bool = False,
    drop_data: bool = False,
    verbose: bool = False,
) -> DeployResult:
    if action not in ["deploy", "clean"]:
        raise ValueError(f"Invalid action {action}")

    if path.is_file():
        if path.suffix not in loader.filetypes or not loader.filetypes:
            raise ValueError("Invalid file type")
        filepaths = [path]
    elif loader.filetypes:
        filepaths = [file for type_ in loader.filetypes for file in path.glob(f"**/*.{type_}")]
    else:
        filepaths = [file for file in path.glob("**/*")]

    if loader.filename_pattern:
        # This is used by data modelings resources to filter out files that are not of the correct type
        # as these resources share the same folder.
        pattern = re.compile(loader.filename_pattern)
        filepaths = [file for file in filepaths if pattern.match(file.stem)]
    if action == "clean":
        # If we do a clean, we do not want to verify that everything exists wrt data sets, spaces etc.
        items = [loader.load_resource(f, dry_run=True) for f in filepaths]
    else:
        items = [loader.load_resource(f, dry_run) for f in filepaths]
    items = [item for item in items if item is not None]
    nr_of_batches = len(items)
    nr_of_items = sum(len(item) if isinstance(item, Sized) else 1 for item in items)
    if nr_of_items == 0:
        return DeployResult(name=loader.display_name, created=0, deleted=0, skipped=0, total=0)
    if action == "deploy":
        action_word = "Loading" if dry_run else "Uploading"
        print(f"[bold]{action_word} {nr_of_items} {loader.display_name} in {nr_of_batches} batches to CDF...[/]")
    else:
        action_word = "Loading" if dry_run else "Cleaning"
        print(f"[bold]{action_word} {nr_of_items} {loader.display_name} in {nr_of_batches} batches to CDF...[/]")
    batches = [item if isinstance(item, Sized) else [item] for item in items]
    if drop and loader.support_drop and action == "deploy":
        if drop_data and (loader.api_name == "data_modeling.spaces" or loader.api_name == "data_modeling.containers"):
            print(
                f"  --drop-data is specified, will delete existing nodes and edges before before deleting {loader.display_name}."
            )
        else:
            print(f"  --drop is specified, will delete existing {loader.display_name} before uploading.")

    # Deleting resources.
    nr_of_deleted = 0
    if (drop and loader.support_drop) or clean:
        for batch in batches:
            drop_items = [loader.get_id(item) for item in batch]
            if dry_run:
                nr_of_deleted += len(drop_items)
                if verbose:
                    print(f"  Would have deleted {len(drop_items)} {loader.display_name}.")
            else:
                try:
                    nr_of_deleted += loader.delete(drop_items, drop_data)
                except CogniteAPIError as e:
                    if e.code == 404:
                        print(f"  [bold yellow]WARNING:[/] {len(drop_items)} {loader.display_name} do(es) not exist.")
                except CogniteNotFoundError:
                    print(f"  [bold yellow]WARNING:[/] {len(drop_items)} {loader.display_name} do(es) not exist.")
                except Exception as e:
                    print(
                        f"  [bold yellow]WARNING:[/] Failed to delete {len(drop_items)} {loader.display_name}. Error {e}."
                    )
                else:  # Delete succeeded
                    if verbose:
                        print(f"  Deleted {len(drop_items)} {loader.display_name}.")
        if dry_run and action == "clean" and verbose:
            # Only clean command prints this, if not we print it at the end
            print(f"  Would have deleted {nr_of_deleted} {loader.display_name} in total.")

    if action == "clean":
        # Clean Command, only delete.
        return DeployResult(name=loader.display_name, created=0, deleted=nr_of_deleted, skipped=0, total=nr_of_items)

    nr_of_created = 0
    nr_of_skipped = 0
    for batch, filepath in zip(batches, filepaths):
        if not drop and loader.support_upsert:
            if verbose:
                print(f"  Comparing {len(batch)} {loader.display_name} from {filepath}...")
            batch = loader.remove_unchanged(batch)
            if verbose:
                print(f"    {len(batch)} {loader.display_name} to be deployed...")

        if batch:
            if dry_run:
                nr_of_created += len(batch)
            else:
                try:
                    created = loader.create(batch, drop, filepath)
                except Exception as e:
                    print(f"  [bold yellow]WARNING:[/] Failed to upload {loader.display_name}. Error {e}.")
                    ToolGlobals.failed = True
                    return
                else:
                    newly_created = len(created) if created is not None else 0
                    nr_of_created += newly_created
                    nr_of_skipped += len(batch) - newly_created
                    if isinstance(loader, AuthLoader):
                        nr_of_deleted += len(created)
    if verbose:
        prefix = "Would have" if dry_run else ""
        print(
            f"  {prefix} Created {nr_of_created}, Deleted {nr_of_deleted}, Skipped {nr_of_skipped}, Total {nr_of_items}."
        )
    return DeployResult(
        name=loader.display_name,
        created=nr_of_created,
        deleted=nr_of_deleted,
        skipped=nr_of_skipped,
        total=nr_of_items,
    )


LOADER_BY_FOLDER_NAME: dict[str, list[type[Loader]]] = {}
for loader in Loader.__subclasses__():
    if loader.folder_name not in LOADER_BY_FOLDER_NAME:
        LOADER_BY_FOLDER_NAME[loader.folder_name] = []
    LOADER_BY_FOLDER_NAME[loader.folder_name].append(loader)
del loader  # cleanup module namespace
