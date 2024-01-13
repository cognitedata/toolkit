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

import inspect
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
from typing import Any, Generic, Literal, TypeVar, Union, cast, final

import pandas as pd
import yaml
from cognite.client import CogniteClient
from cognite.client.data_classes import (
    DataSet,
    DataSetList,
    DataSetWrite,
    DataSetWriteList,
    ExtractionPipeline,
    ExtractionPipelineConfig,
    ExtractionPipelineList,
    FileMetadata,
    FileMetadataList,
    FileMetadataWrite,
    FileMetadataWriteList,
    OidcCredentials,
    TimeSeries,
    TimeSeriesList,
    TimeSeriesWrite,
    TimeSeriesWriteList,
    Transformation,
    TransformationList,
    TransformationSchedule,
    TransformationScheduleList,
    TransformationScheduleWrite,
    TransformationScheduleWriteList,
    TransformationWrite,
    TransformationWriteList,
    capabilities,
)
from cognite.client.data_classes._base import (
    CogniteResourceList,
    T_CogniteResourceList,
    T_WritableCogniteResource,
    T_WriteClass,
    WriteableCogniteResource,
    WriteableCogniteResourceList,
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
    Container,
    ContainerApply,
    ContainerApplyList,
    ContainerList,
    DataModel,
    DataModelApply,
    DataModelApplyList,
    DataModelingId,
    DataModelList,
    Edge,
    EdgeApply,
    EdgeApplyList,
    EdgeApplyResultList,
    EdgeList,
    Node,
    NodeApply,
    NodeApplyList,
    NodeApplyResultList,
    NodeList,
    Space,
    SpaceApply,
    SpaceApplyList,
    SpaceList,
    View,
    ViewApply,
    ViewApplyList,
    ViewList,
)
from cognite.client.data_classes.data_modeling.ids import (
    ContainerId,
    DataModelId,
    EdgeId,
    InstanceId,
    NodeId,
    VersionedDataModelingId,
    ViewId,
)
from cognite.client.data_classes.extractionpipelines import (
    ExtractionPipelineConfigList,
    ExtractionPipelineConfigWrite,
    ExtractionPipelineConfigWriteList,
    ExtractionPipelineWrite,
    ExtractionPipelineWriteList,
)
from cognite.client.data_classes.iam import Group, GroupList, GroupWrite, GroupWriteList
from cognite.client.exceptions import CogniteAPIError, CogniteDuplicatedError, CogniteNotFoundError
from cognite.client.utils.useful_types import SequenceNotStr
from rich import print
from rich.table import Table
from typing_extensions import Self

from .delete import delete_instances
from .utils import CDFToolConfig, load_yaml_inject_variables


@dataclass
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


class RawTableList(WriteableCogniteResourceList[RawTable, RawTable]):
    _RESOURCE = RawTable

    def as_write(self) -> CogniteResourceList[RawTable]:
        return self


@dataclass
class LoadableNodes(NodeApplyList, Sequence[NodeApply]):
    """
    This is a helper class for nodes that contains arguments that are required for writing the
    nodes to CDF.
    """

    auto_create_direct_relations: bool
    skip_on_version_conflict: bool
    replace: bool
    nodes: NodeApplyList

    def __post_init__(self) -> None:
        self.data = self.nodes.data

    def __len__(self) -> int:
        return len(self.data)

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> LoadableNodes:  # type: ignore[override]
        return cls(
            auto_create_direct_relations=resource["autoCreateDirectRelations"],
            skip_on_version_conflict=resource["skipOnVersionConflict"],
            replace=resource["replace"],
            nodes=NodeApplyList.load(resource["nodes"]),
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:  # type: ignore[override]
        return {
            "autoCreateDirectRelations"
            if camel_case
            else "auto_create_direct_relations": self.auto_create_direct_relations,
            "skipOnVersionConflict" if camel_case else "skip_on_version_conflict": self.skip_on_version_conflict,
            "replace": self.replace,
            "nodes": self.nodes.dump(camel_case),
        }


@dataclass
class LoadableEdges(EdgeApplyList, Sequence[EdgeApply]):
    """
    This is a helper class for edges that contains arguments that are required for writing the
    edges to CDF.
    """

    auto_create_start_nodes: bool
    auto_create_end_nodes: bool
    skip_on_version_conflict: bool
    replace: bool
    edges: EdgeApplyList

    def __post_init__(self) -> None:
        self.data = self.edges.data

    def __len__(self) -> int:
        return len(self.data)

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:  # type: ignore[override]
        return cls(
            auto_create_start_nodes=resource["autoCreateStartNodes"],
            auto_create_end_nodes=resource["autoCreateEndNodes"],
            skip_on_version_conflict=resource["skipOnVersionConflict"],
            replace=resource["replace"],
            edges=EdgeApplyList.load(resource["edges"]),
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:  # type: ignore[override]
        return {
            "autoCreateStartNodes" if camel_case else "auto_create_start_nodes": self.auto_create_start_nodes,
            "autoCreateEndNodes" if camel_case else "auto_create_end_nodes": self.auto_create_end_nodes,
            "skipOnVersionConflict" if camel_case else "skip_on_version_conflict": self.skip_on_version_conflict,
            "replace": self.replace,
            "edges": self.edges.dump(camel_case),
        }


# Sequence.register(LoadableNodes)
# Sequence.register(LoadableEdges)

T_ID = TypeVar("T_ID", bound=Union[str, int, DataModelingId, InstanceId, VersionedDataModelingId, RawTable])

T_WritableCogniteResourceList = TypeVar("T_WritableCogniteResourceList", bound=WriteableCogniteResourceList)


class Loader(
    ABC, Generic[T_ID, T_WriteClass, T_WritableCogniteResource, T_CogniteResourceList, T_WritableCogniteResourceList]
):
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
    resource_write_cls: type[T_WriteClass]
    resource_cls: type[T_WritableCogniteResource]
    list_cls: type[T_WritableCogniteResourceList]
    list_write_cls: type[T_CogniteResourceList]
    identifier_key: str = "externalId"
    dependencies: frozenset[type[Loader]] = frozenset()
    _display_name: str = ""

    def __init__(self, client: CogniteClient, ToolGlobals: CDFToolConfig):
        self.client = client
        self.ToolGlobals = ToolGlobals
        try:
            self.api_class = self._get_api_class(client, self.api_name)
        except AttributeError:
            raise AttributeError(f"Invalid api_name {self.api_name}.")

    @property
    def display_name(self) -> str:
        if self._display_name:
            return self._display_name
        return self.api_name

    @staticmethod
    def _get_api_class(client: CogniteClient, api_name: str) -> Any:
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
    def create_loader(
        cls, ToolGlobals: CDFToolConfig
    ) -> Loader[T_ID, T_WriteClass, T_WritableCogniteResource, T_CogniteResourceList, T_WritableCogniteResourceList]:
        client = ToolGlobals.verify_capabilities(capability=cls.get_required_capability(ToolGlobals))
        return cls(client, ToolGlobals)

    @classmethod
    @abstractmethod
    def get_required_capability(cls, ToolGlobals: CDFToolConfig) -> Capability | list[Capability]:
        raise NotImplementedError(f"get_required_capability must be implemented for {cls.__name__}.")

    @classmethod
    @abstractmethod
    def get_id(cls, item: T_WriteClass | T_WritableCogniteResource) -> T_ID:
        raise NotImplementedError

    @classmethod
    def get_ids(cls, items: Sequence[T_WriteClass | T_WritableCogniteResource]) -> list[T_ID]:
        return [cls.get_id(item) for item in items]

    @classmethod
    def find_files(cls, dir_or_file: Path) -> list[Path]:
        """Find all files that are supported by this loader in the given directory or file.

        Args:
            dir_or_file (Path): The directory or file to search in.

        Returns:
            list[Path]: A list of all files that are supported by this loader.

        """
        if dir_or_file.is_file():
            if dir_or_file.suffix not in cls.filetypes or not cls.filetypes:
                raise ValueError("Invalid file type")
            return [dir_or_file]
        elif dir_or_file.is_dir():
            if cls.filetypes:
                file_paths = (file for type_ in cls.filetypes for file in dir_or_file.glob(f"**/*.{type_}"))
            else:
                file_paths = dir_or_file.glob("**/*")

            if cls.filename_pattern:
                pattern = re.compile(cls.filename_pattern)
                return [file for file in file_paths if pattern.match(file.stem)]
            else:
                return list(file_paths)
        else:
            return []

    # Default implementations that can be overridden
    def load_resource(self, filepath: Path, skip_validation: bool) -> T_WriteClass | T_CogniteResourceList:
        raw_yaml = load_yaml_inject_variables(filepath, self.ToolGlobals.environment_variables())
        if isinstance(raw_yaml, list):
            return self.list_write_cls.load(raw_yaml)
        return self.resource_write_cls.load(raw_yaml)

    def create(self, items: T_CogniteResourceList, drop: bool, filepath: Path) -> Sized:
        try:
            created = self.api_class.create(items)
            return created
        except CogniteAPIError as e:
            if e.code == 409:
                print("  [bold yellow]WARNING:[/] Resource(s) already exist(s), skipping creation.")
                return self.list_cls([])
            else:
                print(f"[bold red]ERROR:[/] Failed to create resource(s).\n{e}")
                self.ToolGlobals.failed = True
                return self.list_cls([])
        except CogniteDuplicatedError as e:
            print(
                f"  [bold yellow]WARNING:[/] {len(e.duplicated)} out of {len(items)} resource(s) already exist(s). {len(e.successful or [])} resource(s) created."
            )
            return self.list_cls([])
        except Exception as e:
            print(f"[bold red]ERROR:[/] Failed to create resource(s).\n{e}")
            self.ToolGlobals.failed = True
            return self.list_cls([])

    def retrieve(self, ids: SequenceNotStr[T_ID]) -> T_WritableCogniteResourceList:
        if inspect.signature(self.api_class.retrieve).parameters.get("ignore_unknown_ids"):
            return self.api_class.retrieve(ids, ignore_unknown_ids=True)
        else:
            return self.api_class.retrieve(ids)

    def update(self, items: Sequence[T_WriteClass], filepath: Path) -> T_WritableCogniteResourceList:
        return self.api_class.update(items)

    def delete(self, ids: SequenceNotStr[T_ID], drop_data: bool) -> int:
        self.api_class.delete(ids)
        return len(ids)


@final
class AuthLoader(Loader[str, GroupWrite, Group, GroupWriteList, GroupList]):
    support_drop = False
    support_upsert = True
    api_name = "iam.groups"
    folder_name = "auth"
    resource_cls = Group
    resource_write_cls = GroupWrite
    list_cls = GroupList
    list_write_cls = GroupWriteList
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
    def display_name(self) -> str:
        if self.target_scopes.startswith("all"):
            scope = "all"
        else:
            scope = "resource scoped"
        return f"{self.api_name}({scope})"

    @classmethod
    def create_loader(
        cls,
        ToolGlobals: CDFToolConfig,
        target_scopes: Literal[
            "all", "all_skipped_validation", "all_scoped_skipped_validation", "resource_scoped_only", "all_scoped_only"
        ] = "all",
    ) -> AuthLoader:
        client = ToolGlobals.verify_capabilities(capability=cls.get_required_capability(ToolGlobals))
        return AuthLoader(client, ToolGlobals, target_scopes)

    @classmethod
    def get_required_capability(cls, ToolGlobals: CDFToolConfig) -> Capability | list[Capability]:
        return GroupsAcl(
            [GroupsAcl.Action.Read, GroupsAcl.Action.List, GroupsAcl.Action.Create, GroupsAcl.Action.Delete],
            GroupsAcl.Scope.All(),
        )

    @classmethod
    def get_id(cls, item: GroupWrite | Group) -> str:
        return item.name

    def load_resource(self, filepath: Path, skip_validation: bool) -> GroupWrite:
        raw = load_yaml_inject_variables(
            filepath, self.ToolGlobals.environment_variables(), required_return_type="dict"
        )
        for capability in raw.get("capabilities", []):
            for _, values in capability.items():
                for scope in ["datasetScope", "idScope"]:
                    if len(ids := values.get("scope", {}).get(scope, {}).get("ids", [])) > 0:
                        if not skip_validation and self.target_scopes not in [
                            "all_skipped_validation",
                            "all_scoped_skipped_validation",
                        ]:
                            values["scope"][scope]["ids"] = [
                                self.ToolGlobals.verify_dataset(ext_id) if isinstance(ext_id, str) else ext_id
                                for ext_id in ids
                            ]
                        else:
                            values["scope"][scope]["ids"] = [-1] * len(ids)

                if len(values.get("scope", {}).get("extractionPipelineScope", {}).get("ids", [])) > 0:
                    if not skip_validation and self.target_scopes not in [
                        "all_skipped_validation",
                        "all_scoped_skipped_validation",
                    ]:
                        values["scope"]["extractionPipelineScope"]["ids"] = [
                            self.ToolGlobals.verify_extraction_pipeline(ext_id)
                            for ext_id in values.get("scope", {}).get("extractionPipelineScope", {}).get("ids", [])
                        ]
                    else:
                        values["scope"]["extractionPipelineScope"]["ids"] = [-1]
        return GroupWrite.load(raw)

    def create(self, items: Sequence[GroupWrite], drop: bool, filepath: Path) -> GroupList:
        if self.target_scopes == "all":
            to_create = items
        elif self.target_scopes == "all_skipped_validation":
            raise ValueError("all_skipped_validation is not supported for group creation as scopes would be wrong.")
        elif self.target_scopes == "resource_scoped_only":
            to_create = GroupWriteList([])
            for item in items:
                item.capabilities = [
                    capability
                    for capability in item.capabilities or []
                    if type(capability.scope) in self.resource_scopes
                ]
                if item.capabilities:
                    to_create.append(item)
        elif self.target_scopes == "all_scoped_only" or self.target_scopes == "all_scoped_skipped_validation":
            to_create = GroupWriteList([])
            for item in items:
                item.capabilities = [
                    capability
                    for capability in item.capabilities or []
                    if type(capability.scope) not in self.resource_scopes
                ]
                if item.capabilities:
                    to_create.append(item)
        else:
            raise ValueError(f"Invalid load value {self.target_scopes}")

        if len(to_create) == 0:
            return GroupList([])
        # We MUST retrieve all the old groups BEFORE we add the new, if not the new will be deleted
        old_groups = self.client.iam.groups.list(all=True)
        # Bug in SDK not setting cognite_client
        old_groups._cognite_client = self.client
        old_group_by_names = {g.name: g for g in old_groups.as_write()}
        changed = []
        for item in to_create:
            if (old := old_group_by_names.get(item.name)) and old == item:
                # Ship unchanged groups
                continue
            changed.append(item)
        if len(changed) == 0:
            return GroupList([])
        created = self.client.iam.groups.create(changed)
        created_names = {g.name for g in created}
        to_delete = [g.id for g in old_groups if g.name in created_names and g.id]
        self.client.iam.groups.delete(to_delete)
        return created

    def update(self, items: Sequence[GroupWrite], filepath: Path) -> GroupList:
        return self.client.iam.groups.create(items)

    def retrieve(self, ids: SequenceNotStr[str]) -> GroupList:
        remote = self.client.iam.groups.list(all=True)
        found = [g for g in remote if g.name in ids]
        return GroupList(found)

    def delete(self, ids: SequenceNotStr[str], drop_data: bool) -> int:
        id_list = list(ids)
        # Let's prevent that we delete groups we belong to
        try:
            groups = self.client.iam.groups.list()
        except Exception as e:
            print(
                f"[bold red]ERROR:[/] Failed to retrieve the current service principal's groups. Aborting group deletion.\n{e}"
            )
            return 0
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
                if g.name not in id_list:
                    print(f"    [bold red]ERROR[/] You seem to have duplicate groups of name {g.name}.")
                else:
                    id_list.remove(g.name)
        found = [g.id for g in groups if g.name in id_list and g.id]
        self.client.iam.groups.delete(found)
        return len(found)


@final
class DataSetsLoader(Loader[str, DataSetWrite, DataSet, DataSetWriteList, DataSetList]):
    support_drop = False
    support_upsert = True
    api_name = "data_sets"
    folder_name = "data_sets"
    resource_cls = DataSet
    resource_write_cls = DataSetWrite
    list_cls = DataSetList
    list_write_cls = DataSetWriteList

    @classmethod
    def get_required_capability(cls, ToolGlobals: CDFToolConfig) -> Capability:
        return DataSetsAcl(
            [DataSetsAcl.Action.Read, DataSetsAcl.Action.Write],
            DataSetsAcl.Scope.All(),
        )

    @classmethod
    def get_id(cls, item: DataSet | DataSetWrite) -> str:
        if item.external_id is None:
            raise ValueError("DataSet must have external_id set.")
        return item.external_id

    def load_resource(self, filepath: Path, skip_validation: bool) -> DataSetWriteList:
        resource = load_yaml_inject_variables(filepath, {})

        data_sets = [resource] if isinstance(resource, dict) else resource

        for data_set in data_sets:
            if data_set.get("metadata"):
                for key, value in data_set["metadata"].items():
                    data_set["metadata"][key] = json.dumps(value)
        return DataSetWriteList.load(data_sets)

    def create(self, items: Sequence[DataSetWrite], drop: bool, filepath: Path) -> DataSetList:
        items = list(items)
        created = DataSetList([], cognite_client=self.client)
        # There is a bug in the data set API, so only one duplicated data set is returned at the time,
        # so we need to iterate.
        while len(items) > 0:
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
                    items = []
            except Exception as e:
                print(f"[bold red]ERROR:[/] Failed to create data sets.\n{e}")
                self.ToolGlobals.failed = True
                return DataSetList([])
        return created

    def retrieve(self, ids: SequenceNotStr[str]) -> DataSetList:
        return self.client.data_sets.retrieve_multiple(external_ids=cast(Sequence, ids), ignore_unknown_ids=True)

    def delete(self, ids: SequenceNotStr[str], drop_data: bool) -> int:
        raise NotImplementedError("CDF does not support deleting data sets.")


@final
class RawLoader(Loader[RawTable, RawTable, RawTable, RawTableList, RawTableList]):
    api_name = "raw.rows"
    folder_name = "raw"
    resource_cls = RawTable
    resource_write_cls = RawTable
    list_cls = RawTableList
    list_write_cls = RawTableList
    identifier_key = "table_name"
    data_file_types = frozenset({"csv", "parquet"})

    @classmethod
    def get_required_capability(cls, ToolGlobals: CDFToolConfig) -> Capability:
        return RawAcl([RawAcl.Action.Read, RawAcl.Action.Write], RawAcl.Scope.All())

    @classmethod
    def get_id(cls, item: RawTable) -> RawTable:
        return item

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

    def delete(self, ids: SequenceNotStr[RawTable], drop_data: bool) -> int:
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


@final
class TimeSeriesLoader(Loader[str, TimeSeriesWrite, TimeSeries, TimeSeriesWriteList, TimeSeriesList]):
    api_name = "time_series"
    folder_name = "timeseries"
    resource_cls = TimeSeries
    resource_write_cls = TimeSeriesWrite
    list_cls = TimeSeriesList
    list_write_cls = TimeSeriesWriteList
    dependencies = frozenset({DataSetsLoader})

    @classmethod
    def get_required_capability(cls, ToolGlobals: CDFToolConfig) -> Capability:
        return TimeSeriesAcl(
            [TimeSeriesAcl.Action.Read, TimeSeriesAcl.Action.Write],
            TimeSeriesAcl.Scope.DataSet([ToolGlobals.data_set_id])
            if ToolGlobals.data_set_id
            else TimeSeriesAcl.Scope.All(),
        )

    @classmethod
    def get_id(cls, item: TimeSeries | TimeSeriesWrite) -> str:
        if item.external_id is None:
            raise ValueError("TimeSeries must have external_id set.")
        return item.external_id

    def load_resource(self, filepath: Path, skip_validation: bool) -> TimeSeriesWriteList:
        resources = load_yaml_inject_variables(filepath, {})
        if not isinstance(resources, list):
            resources = [resources]
        for resource in resources:
            if resource.get("dataSetExternalId") is not None:
                ds_external_id = resource.pop("dataSetExternalId")
                resource["dataSetId"] = self.ToolGlobals.verify_dataset(ds_external_id) if not skip_validation else -1
        return TimeSeriesWriteList.load(resources)

    def retrieve(self, ids: SequenceNotStr[str]) -> TimeSeriesList:
        return self.client.time_series.retrieve_multiple(external_ids=cast(Sequence, ids), ignore_unknown_ids=True)

    def delete(self, ids: SequenceNotStr[str], drop_data: bool) -> int:
        self.client.time_series.delete(external_id=cast(Sequence, ids), ignore_unknown_ids=True)
        return len(ids)


@final
class TransformationLoader(
    Loader[str, TransformationWrite, Transformation, TransformationWriteList, TransformationList]
):
    api_name = "transformations"
    folder_name = "transformations"
    filename_pattern = (
        r"^(?:(?!\.schedule).)*$"  # Matches all yaml files except file names who's stem contain *.schedule.
    )
    resource_cls = Transformation
    resource_write_cls = TransformationWrite
    list_cls = TransformationList
    list_write_cls = TransformationWriteList
    dependencies = frozenset({DataSetsLoader, RawLoader})

    @classmethod
    def get_required_capability(cls, ToolGlobals: CDFToolConfig) -> Capability:
        scope: capabilities.AllScope | capabilities.DataSetScope = (
            TransformationsAcl.Scope.DataSet([ToolGlobals.data_set_id])
            if ToolGlobals.data_set_id
            else TransformationsAcl.Scope.All()
        )
        return TransformationsAcl(
            [TransformationsAcl.Action.Read, TransformationsAcl.Action.Write],
            scope,
        )

    @classmethod
    def get_id(cls, item: Transformation | TransformationWrite) -> str:
        if item.external_id is None:
            raise ValueError("Transformation must have external_id set.")
        return item.external_id

    def load_resource(self, filepath: Path, skip_validation: bool) -> TransformationWrite:
        raw = load_yaml_inject_variables(
            filepath, self.ToolGlobals.environment_variables(), required_return_type="dict"
        )
        # The `authentication` key is custom for this template:

        source_oidc_credentials = raw.get("authentication", {}).get("read") or raw.get("authentication") or None
        destination_oidc_credentials = raw.get("authentication", {}).get("write") or raw.get("authentication") or None
        if raw.get("dataSetExternalId") is not None:
            ds_external_id = raw.pop("dataSetExternalId")
            raw["dataSetId"] = self.ToolGlobals.verify_dataset(ds_external_id) if not skip_validation else -1

        transformation = TransformationWrite.load(raw)
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

    def create(self, items: Sequence[TransformationWrite], drop: bool, filepath: Path) -> TransformationList:
        try:
            created = self.client.transformations.create(items)
        except CogniteDuplicatedError as e:
            print(
                f"  [bold yellow]WARNING:[/] {len(e.duplicated)} transformation(s) out of {len(items)} transformation(s) already exist(s):"
            )
            for dup in e.duplicated:
                print(f"           {dup.get('externalId', 'N/A')}")
            return TransformationList([])
        except Exception as e:
            print(f"[bold red]ERROR:[/] Failed to create resource(s).\n{e}")
            self.ToolGlobals.failed = True
            return TransformationList([])

        return created

    def delete(self, ids: SequenceNotStr[str], drop_data: bool) -> int:
        self.client.transformations.delete(external_id=cast(Sequence, ids), ignore_unknown_ids=True)
        return len(ids)


@final
class TransformationScheduleLoader(
    Loader[
        str,
        TransformationScheduleWrite,
        TransformationSchedule,
        TransformationScheduleWriteList,
        TransformationScheduleList,
    ]
):
    api_name = "transformations.schedules"
    folder_name = "transformations"
    filename_pattern = r"^.*\.schedule$"  # Matches all yaml files who's stem contain *.schedule.
    resource_cls = TransformationSchedule
    resource_write_cls = TransformationScheduleWrite
    list_cls = TransformationScheduleList
    list_write_cls = TransformationScheduleWriteList
    dependencies = frozenset({TransformationLoader})

    @classmethod
    def get_required_capability(cls, ToolGlobals: CDFToolConfig) -> Capability:
        scope: capabilities.AllScope | capabilities.DataSetScope = (
            TransformationsAcl.Scope.DataSet([ToolGlobals.data_set_id])
            if ToolGlobals.data_set_id
            else TransformationsAcl.Scope.All()
        )
        return TransformationsAcl(
            [TransformationsAcl.Action.Read, TransformationsAcl.Action.Write],
            scope,
        )

    @classmethod
    def get_id(cls, item: TransformationSchedule | TransformationScheduleWrite) -> str:
        if item.external_id is None:
            raise ValueError("TransformationSchedule must have external_id set.")
        return item.external_id

    def load_resource(self, filepath: Path, skip_validation: bool) -> TransformationScheduleWrite:
        raw = load_yaml_inject_variables(
            filepath, self.ToolGlobals.environment_variables(), required_return_type="dict"
        )
        return TransformationScheduleWrite.load(raw)

    def create(
        self, items: Sequence[TransformationScheduleWrite], drop: bool, filepath: Path
    ) -> TransformationScheduleList:
        try:
            return self.client.transformations.schedules.create(list(items))
        except CogniteDuplicatedError as e:
            existing = {external_id for dup in e.duplicated if (external_id := dup.get("externalId", None))}
            print(
                f"  [bold yellow]WARNING:[/] {len(e.duplicated)} transformation schedules already exist(s): {existing}"
            )
            new_items = [item for item in items if item.external_id not in existing]
            if len(new_items) == 0:
                return TransformationScheduleList([])
            try:
                return cast(TransformationScheduleList, self.client.transformations.schedules.create(new_items))
            except CogniteAPIError as e:
                print(f"[bold red]ERROR:[/] Failed to create resource(s).\n{e}")
                self.ToolGlobals.failed = True
                return TransformationScheduleList([])
        except CogniteAPIError as e:
            print(f"[bold red]ERROR:[/] Failed to create resource(s).\n{e}")
            self.ToolGlobals.failed = True
            return TransformationScheduleList([])

    def delete(self, ids: SequenceNotStr[str], drop_data: bool) -> int:
        try:
            self.client.transformations.schedules.delete(external_id=cast(Sequence, ids), ignore_unknown_ids=False)
            return len(ids)
        except CogniteNotFoundError as e:
            return len(ids) - len(e.not_found)


@final
class DatapointsLoader(Loader[list[str], Path, Path, TimeSeriesWriteList, TimeSeriesList]):  # type: ignore[type-var]
    support_drop = False
    filetypes = frozenset({"csv", "parquet"})
    api_name = "time_series.data"
    folder_name = "timeseries_datapoints"
    resource_cls = pd.DataFrame
    dependencies = frozenset({TimeSeriesLoader})

    @classmethod
    def get_required_capability(cls, ToolGlobals: CDFToolConfig) -> Capability:
        scope: capabilities.AllScope | capabilities.DataSetScope = (
            TimeSeriesAcl.Scope.DataSet([ToolGlobals.data_set_id])
            if ToolGlobals.data_set_id
            else TimeSeriesAcl.Scope.All()
        )

        return TimeSeriesAcl(
            [TimeSeriesAcl.Action.Read, TimeSeriesAcl.Action.Write],
            scope,
        )

    @classmethod
    def get_id(cls, item: Path) -> list[str]:
        raise NotImplementedError

    def load_resource(self, filepath: Path, skip_validation: bool) -> Path:
        return filepath

    def create(self, items: Sequence[Path], drop: bool, filepath: Path) -> TimeSeriesList:
        if len(items) != 1:
            raise ValueError("Datapoints must be loaded one at a time.")
        datafile = items[0]
        if datafile.suffix == ".csv":
            # The replacement is used to ensure that we read exactly the same file on Windows and Linux
            file_content = datafile.read_bytes().replace(b"\r\n", b"\n").decode("utf-8")
            data = pd.read_csv(io.StringIO(file_content), parse_dates=True, dayfirst=True, index_col=0)
            data.index = pd.DatetimeIndex(data.index)
        elif datafile.suffix == ".parquet":
            data = pd.read_parquet(datafile, engine="pyarrow")
        else:
            raise ValueError(f"Unsupported file type {datafile.suffix} for {datafile.name}")
        self.client.time_series.data.insert_dataframe(data)
        external_ids = [col for col in data.columns if not pd.api.types.is_datetime64_any_dtype(data[col])]
        return TimeSeriesList([TimeSeries(external_id=external_id) for external_id in external_ids])

    def delete(self, ids: SequenceNotStr[list[str]], drop_data: bool) -> int:
        # Drop all datapoints?
        raise NotImplementedError()


@final
class ExtractionPipelineLoader(
    Loader[str, ExtractionPipelineWrite, ExtractionPipeline, ExtractionPipelineWriteList, ExtractionPipelineList]
):
    support_drop = True
    api_name = "extraction_pipelines"
    folder_name = "extraction_pipelines"
    filename_pattern = r"^(?:(?!\.config).)*$"  # Matches all yaml files except file names who's stem contain *.config.
    resource_cls = ExtractionPipeline
    resource_write_cls = ExtractionPipelineWrite
    list_cls = ExtractionPipelineList
    list_write_cls = ExtractionPipelineWriteList
    dependencies = frozenset({DataSetsLoader, RawLoader})

    @classmethod
    def get_required_capability(cls, ToolGlobals: CDFToolConfig) -> Capability:
        return ExtractionPipelinesAcl(
            [ExtractionPipelinesAcl.Action.Read, ExtractionPipelinesAcl.Action.Write],
            ExtractionPipelinesAcl.Scope.All(),
        )

    @classmethod
    def get_id(cls, item: ExtractionPipeline | ExtractionPipelineWrite) -> str:
        if item.external_id is None:
            raise ValueError("ExtractionPipeline must have external_id set.")
        return item.external_id

    def load_resource(self, filepath: Path, skip_validation: bool) -> ExtractionPipelineWrite:
        resource = load_yaml_inject_variables(filepath, {}, required_return_type="dict")

        if resource.get("dataSetExternalId") is not None:
            ds_external_id = resource.pop("dataSetExternalId")
            resource["dataSetId"] = self.ToolGlobals.verify_dataset(ds_external_id) if not skip_validation else -1

        return ExtractionPipelineWrite.load(resource)

    def create(self, items: Sequence[ExtractionPipelineWrite], drop: bool, filepath: Path) -> ExtractionPipelineList:
        items = list(items)
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

    def delete(self, ids: SequenceNotStr[str], drop_data: bool) -> int:
        id_list = list(ids)
        try:
            self.client.extraction_pipelines.delete(external_id=id_list)
            return len(id_list)
        except CogniteNotFoundError as e:
            print(
                f"  [bold yellow]WARNING:[/] {len(e.not_found)} out of {len(ids)} extraction pipelines do(es) not exist."
            )

            for dup in e.not_found:
                ext_id = dup.get("externalId", None)
                id_list.remove(ext_id)

            if len(id_list) > 0:
                self.client.extraction_pipelines.delete(external_id=id_list)
                return len(id_list)
            return 0


@final
class ExtractionPipelineConfigLoader(
    Loader[
        str,
        ExtractionPipelineConfigWrite,
        ExtractionPipelineConfig,
        ExtractionPipelineConfigWriteList,
        ExtractionPipelineConfigList,
    ]
):
    support_drop = True
    api_name = "extraction_pipelines.config"
    folder_name = "extraction_pipelines"
    filename_pattern = r"^.*\.config$"
    resource_cls = ExtractionPipelineConfig
    resource_write_cls = ExtractionPipelineConfigWrite
    list_cls = ExtractionPipelineConfigList
    list_write_cls = ExtractionPipelineConfigWriteList
    dependencies = frozenset({ExtractionPipelineLoader})

    @classmethod
    def get_required_capability(cls, ToolGlobals: CDFToolConfig) -> Capability:
        return ExtractionPipelinesAcl(
            [ExtractionPipelinesAcl.Action.Read, ExtractionPipelinesAcl.Action.Write],
            ExtractionPipelinesAcl.Scope.All(),
        )

    @classmethod
    def get_id(cls, item: ExtractionPipelineConfig | ExtractionPipelineConfigWrite) -> str:
        if item.external_id is None:
            raise ValueError("ExtractionPipelineConfig must have external_id set.")
        return item.external_id

    def load_resource(self, filepath: Path, skip_validation: bool) -> ExtractionPipelineConfigWrite:
        resource = load_yaml_inject_variables(filepath, {}, required_return_type="dict")
        try:
            resource["config"] = yaml.dump(resource.get("config", ""), indent=4)
        except Exception:
            print(
                "[yellow]WARNING:[/] configuration could not be parsed as valid YAML, which is the recommended format.\n"
            )
            resource["config"] = resource.get("config", "")
        return ExtractionPipelineConfigWrite.load(resource)

    def create(
        self, items: Sequence[ExtractionPipelineConfigWrite], drop: bool, filepath: Path
    ) -> ExtractionPipelineConfigList:
        try:
            return ExtractionPipelineConfigList([self.client.extraction_pipelines.config.create(items[0])])
        except Exception as e:
            print(f"[bold red]ERROR:[/] Failed to create extraction pipelines.\n{e}")
            self.ToolGlobals.failed = True
            return ExtractionPipelineConfigList([])

    def delete(self, ids: SequenceNotStr[str], drop_data: bool) -> int:
        count = 0
        for id_ in ids:
            result = self.client.extraction_pipelines.config.list(external_id=id_)
            count += len(result)
        return count


@final
class FileLoader(Loader[str, FileMetadataWrite, FileMetadata, FileMetadataWriteList, FileMetadataList]):
    api_name = "files"
    filetypes = frozenset({"yaml", "yml"})
    folder_name = "files"
    resource_cls = FileMetadata
    resource_write_cls = FileMetadataWrite
    list_cls = FileMetadataList
    list_write_cls = FileMetadataWriteList
    dependencies = frozenset({DataSetsLoader})

    @classmethod
    def get_required_capability(cls, ToolGlobals: CDFToolConfig) -> Capability:
        scope: capabilities.AllScope | capabilities.DataSetScope
        if ToolGlobals.data_set_id is None:
            scope = FilesAcl.Scope.All()
        else:
            scope = FilesAcl.Scope.DataSet([ToolGlobals.data_set_id])

        return FilesAcl([FilesAcl.Action.Read, FilesAcl.Action.Write], scope)

    @classmethod
    def get_id(cls, item: FileMetadata | FileMetadataWrite) -> str:
        if item.external_id is None:
            raise ValueError("FileMetadata must have external_id set.")
        return item.external_id

    def load_resource(self, filepath: Path, skip_validation: bool) -> FileMetadataWrite | FileMetadataWriteList:
        try:
            resource = load_yaml_inject_variables(
                filepath, self.ToolGlobals.environment_variables(), required_return_type="dict"
            )
            if resource.get("dataSetExternalId") is not None:
                ds_external_id = resource.pop("dataSetExternalId")
                resource["dataSetId"] = self.ToolGlobals.verify_dataset(ds_external_id) if not skip_validation else -1
            files_metadata = FileMetadataWriteList([FileMetadataWrite.load(resource)])
        except Exception:
            files_metadata = FileMetadataWriteList.load(
                load_yaml_inject_variables(
                    filepath, self.ToolGlobals.environment_variables(), required_return_type="list"
                )
            )
        # If we have a file with exact one file config, check to see if this is a pattern to expand
        if len(files_metadata) == 1 and ("$FILENAME" in (files_metadata[0].external_id or "")):
            # It is, so replace this file with all files in this folder using the same data
            file_data = files_metadata.data[0]
            ext_id_pattern = file_data.external_id
            files_metadata = FileMetadataWriteList([], cognite_client=self.client)
            for file in filepath.parent.glob("*"):
                if file.suffix[1:] in ["yaml", "yml"]:
                    continue
                files_metadata.append(
                    FileMetadataWrite(
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
        for meta in files_metadata:
            if meta.name is None:
                raise ValueError(f"File {meta.external_id} has no name.")
            if not Path(filepath.parent / meta.name).exists():
                raise FileNotFoundError(f"Could not find file {meta.name} referenced in filepath {filepath.name}")
            if isinstance(meta.data_set_id, str):
                # Replace external_id with internal id
                meta.data_set_id = self.ToolGlobals.verify_dataset(meta.data_set_id) if not skip_validation else -1
        return files_metadata

    def create(self, items: Sequence[FileMetadataWrite], drop: bool, filepath: Path) -> FileMetadataList:
        created = FileMetadataList([])
        for meta in items:
            if meta.name is None:
                raise ValueError(f"File {meta.external_id} has no name.")
            datafile = filepath.parent / meta.name
            try:
                created.append(
                    self.client.files.upload(path=str(datafile), overwrite=drop, **meta.dump(camel_case=False))
                )
            except CogniteAPIError as e:
                if e.code == 409:
                    print(f"  [bold yellow]WARNING:[/] File {meta.external_id} already exists, skipping upload.")
            except Exception as e:
                print(f"[bold red]ERROR:[/] Failed to upload file {datafile.name}.\n{e}")
                self.ToolGlobals.failed = True
                return created
        return created

    def delete(self, ids: SequenceNotStr[str], drop_data: bool) -> int:
        self.client.files.delete(external_id=cast(Sequence, ids))
        return len(ids)


@final
class SpaceLoader(Loader[str, SpaceApply, Space, SpaceApplyList, SpaceList]):
    api_name = "data_modeling.spaces"
    folder_name = "data_models"
    filename_pattern = r"^.*\.?(space)$"
    resource_cls = Space
    resource_write_cls = SpaceApply
    list_write_cls = SpaceApplyList
    list_cls = SpaceList
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
    def get_id(cls, item: SpaceApply | Space) -> str:
        return item.space

    def create(self, items: Sequence[SpaceApply], drop: bool, filepath: Path) -> SpaceList:
        return self.client.data_modeling.spaces.apply(items)

    def delete(self, ids: SequenceNotStr[str], drop_data: bool) -> int:
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


class ContainerLoader(Loader[ContainerId, ContainerApply, Container, ContainerApplyList, ContainerList]):
    api_name = "data_modeling.containers"
    folder_name = "data_models"
    filename_pattern = r"^.*\.?(container)$"
    resource_cls = Container
    resource_write_cls = ContainerApply
    list_cls = ContainerList
    list_write_cls = ContainerApplyList
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
    def get_id(cls, item: ContainerApply | Container) -> ContainerId:
        return item.as_id()

    def create(self, items: Sequence[ContainerApply], drop: bool, filepath: Path) -> ContainerList:
        self.ToolGlobals.verify_spaces(list({item.space for item in items}))

        return self.client.data_modeling.containers.apply(items)

    def delete(self, ids: SequenceNotStr[ContainerId], drop_data: bool) -> int:
        if not drop_data:
            print("  [bold]INFO:[/] Skipping deletion of containers as drop_data flag is not set...")
            return 0
        deleted = self.client.data_modeling.containers.delete(cast(Sequence, ids))
        return len(deleted)


class ViewLoader(Loader[ViewId, ViewApply, View, ViewApplyList, ViewList]):
    api_name = "data_modeling.views"
    folder_name = "data_models"
    filename_pattern = r"^.*\.?(view)$"
    resource_cls = View
    resource_write_cls = ViewApply
    list_cls = ViewList
    list_write_cls = ViewApplyList
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
    def get_id(cls, item: ViewApply | View) -> ViewId:
        return item.as_id()

    def create(self, items: Sequence[ViewApply], drop: bool, filepath: Path) -> ViewList:
        self.ToolGlobals.verify_spaces(list({item.space for item in items}))
        return self.client.data_modeling.views.apply(items)


@final
class DataModelLoader(Loader[DataModelId, DataModelApply, DataModel, DataModelApplyList, DataModelList]):
    api_name = "data_modeling.data_models"
    folder_name = "data_models"
    filename_pattern = r"^.*\.?(datamodel)$"
    resource_cls = DataModel
    resource_write_cls = DataModelApply
    list_cls = DataModelList
    list_write_cls = DataModelApplyList
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
    def get_id(cls, item: DataModelApply | DataModel) -> DataModelId:
        return item.as_id()

    def create(self, items: DataModelApplyList, drop: bool, filepath: Path) -> DataModelList:
        self.ToolGlobals.verify_spaces(list({item.space for item in items}))
        return self.client.data_modeling.data_models.apply(items)


@final
class NodeLoader(Loader[NodeId, NodeApply, Node, LoadableNodes, NodeList]):
    api_name = "data_modeling.instances"
    folder_name = "data_models"
    filename_pattern = r"^.*\.?(node)$"
    resource_cls = Node
    resource_write_cls = NodeApply
    list_cls = NodeList
    list_write_cls = LoadableNodes
    dependencies = frozenset({SpaceLoader, ViewLoader, ContainerLoader})
    _display_name = "nodes"

    @classmethod
    def get_required_capability(cls, ToolGlobals: CDFToolConfig) -> Capability:
        # Todo Scoped to spaces
        return DataModelInstancesAcl(
            [DataModelInstancesAcl.Action.Read, DataModelInstancesAcl.Action.Write],
            DataModelInstancesAcl.Scope.All(),
        )

    @classmethod
    def get_id(cls, item: NodeApply | Node) -> NodeId:
        return item.as_id()

    def load_resource(self, filepath: Path, skip_validation: bool) -> LoadableNodes:
        raw = load_yaml_inject_variables(filepath, self.ToolGlobals.environment_variables())
        if isinstance(raw, dict):
            return LoadableNodes._load(raw, cognite_client=self.client)
        else:
            raise ValueError(f"Unexpected node yaml file format {filepath.name}")

    def create(self, items: LoadableNodes, drop: bool, filepath: Path) -> NodeApplyResultList:
        if not isinstance(items, LoadableNodes):
            raise ValueError("Unexpected node format file format")
        self.ToolGlobals.verify_spaces(list({item.space for item in items}))
        item = items
        result = self.client.data_modeling.instances.apply(
            nodes=item.nodes,
            auto_create_direct_relations=item.auto_create_direct_relations,
            skip_on_version_conflict=item.skip_on_version_conflict,
            replace=item.replace,
        )
        return result.nodes

    def delete(self, ids: SequenceNotStr[NodeId], drop_data: bool) -> int:
        if not drop_data:
            print("  [bold]INFO:[/] Skipping deletion of nodes as drop_data flag is not set...")
            return 0
        deleted = self.client.data_modeling.instances.delete(nodes=cast(Sequence, ids))
        return len(deleted.nodes)


@final
class EdgeLoader(Loader[EdgeId, EdgeApply, Edge, LoadableEdges, EdgeList]):
    api_name = "data_modeling.instances"
    folder_name = "data_models"
    filename_pattern = r"^.*\.?(edge)$"
    resource_cls = Edge
    resource_write_cls = EdgeApply
    list_cls = EdgeList
    list_write_cls = LoadableEdges
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

    @classmethod
    def get_id(cls, item: EdgeApply | Edge) -> EdgeId:
        return item.as_id()

    def load_resource(self, filepath: Path, skip_validation: bool) -> LoadableEdges:
        raw = load_yaml_inject_variables(filepath, self.ToolGlobals.environment_variables())
        if isinstance(raw, dict):
            return LoadableEdges._load(raw, cognite_client=self.client)
        else:
            raise ValueError(f"Unexpected edge yaml file format {filepath.name}")

    def create(self, items: LoadableEdges, drop: bool, filepath: Path) -> EdgeApplyResultList:
        if not isinstance(items, LoadableEdges):
            raise ValueError("Unexpected edge format file format")
        self.ToolGlobals.verify_spaces(list({item.space for item in items}))
        item = items
        result = self.client.data_modeling.instances.apply(
            edges=item.edges,
            auto_create_start_nodes=item.auto_create_start_nodes,
            auto_create_end_nodes=item.auto_create_end_nodes,
            skip_on_version_conflict=item.skip_on_version_conflict,
            replace=item.replace,
        )
        return result.edges

    def delete(self, ids: SequenceNotStr[EdgeId], drop_data: bool) -> int:
        if not drop_data:
            print("  [bold]INFO:[/] Skipping deletion of edges as drop_data flag is not set...")
            return 0
        deleted = self.client.data_modeling.instances.delete(edges=cast(Sequence, ids))
        return len(deleted.edges)


@total_ordering
@dataclass
class DeployResult:
    name: str
    created: int = 0
    deleted: int = 0
    changed: int = 0
    unchanged: int = 0
    skipped: int = 0
    total: int = 0

    @property
    def calculated_total(self) -> int:
        return self.created + self.deleted + self.changed + self.unchanged + self.skipped

    def __lt__(self, other: object) -> bool:
        if isinstance(other, DeployResult):
            return self.name < other.name
        else:
            return NotImplemented

    def __eq__(self, other: object) -> bool:
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
) -> DeployResult | None:
    if action not in ["deploy", "clean"]:
        raise ValueError(f"Invalid action {action}")

    filepaths = loader.find_files(path)

    # If we do a clean, we do not want to verify that everything exists wrt data sets, spaces etc.
    skip_validation = dry_run or action == "clean"
    batches = []
    for filepath in filepaths:
        try:
            resource = loader.load_resource(filepath, skip_validation)
        except KeyError as e:
            # KeyError means that we are missing a required field in the yaml file.
            print(
                f"[bold red]ERROR:[/] Failed to load {filepath.name} with {loader.display_name}. Missing required field: {e}."
            )
            ToolGlobals.failed = True
            return None
        if resource is None:
            print(f"[bold yellow]WARNING:[/] Skipping {filepath.name}. No data to load.")
            continue
        batches.append(resource if isinstance(resource, Sequence) else [resource])

    nr_of_batches = len(batches)
    nr_of_items = sum(len(batch) for batch in batches)
    if nr_of_items == 0:
        return DeployResult(name=loader.display_name)
    if action == "deploy":
        action_word = "Loading" if dry_run else "Uploading"
        print(f"[bold]{action_word} {nr_of_items} {loader.display_name} in {nr_of_batches} batches to CDF...[/]")
    else:
        action_word = "Loading" if dry_run else "Cleaning"
        print(f"[bold]{action_word} {nr_of_items} {loader.display_name} in {nr_of_batches} batches to CDF...[/]")

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
            batch_ids = loader.get_ids(batch)
            if dry_run:
                nr_of_deleted += len(batch_ids)
                if verbose:
                    print(f"  Would have deleted {len(batch_ids)} {loader.display_name}.")
            else:
                try:
                    nr_of_deleted += loader.delete(batch_ids, drop_data)
                except CogniteAPIError as e:
                    if e.code == 404:
                        print(f"  [bold yellow]WARNING:[/] {len(batch_ids)} {loader.display_name} do(es) not exist.")
                except CogniteNotFoundError:
                    print(f"  [bold yellow]WARNING:[/] {len(batch_ids)} {loader.display_name} do(es) not exist.")
                except Exception as e:
                    print(
                        f"  [bold yellow]WARNING:[/] Failed to delete {len(batch_ids)} {loader.display_name}. Error {e}."
                    )
                else:  # Delete succeeded
                    if verbose:
                        print(f"  Deleted {len(batch_ids)} {loader.display_name}.")
        if dry_run and action == "clean" and verbose:
            # Only clean command prints this, if not we print it at the end
            print(f"  Would have deleted {nr_of_deleted} {loader.display_name} in total.")

    if action == "clean":
        # Clean Command, only delete.
        nr_of_items = nr_of_deleted
        return DeployResult(name=loader.display_name, deleted=nr_of_deleted, total=nr_of_items)

    nr_of_created = 0
    nr_of_changed = 0
    nr_of_unchanged = 0
    nr_of_skipped = 0
    for batch, filepath in zip(batches, filepaths):
        if not drop and loader.support_upsert:
            if verbose:
                print(f"  Comparing {len(batch)} {loader.display_name} from {filepath}...")
            batch = loader.remove_unchanged(batch)  # type: ignore[attr-defined]
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
                    return None
                else:
                    newly_created = len(created) if created is not None else 0
                    nr_of_created += newly_created
                    nr_of_skipped += len(batch) - newly_created
                    # For timeseries.datapoints, we can load multiple timeseries in one file,
                    # so the number of created items can be larger than the number of items in the batch.
                    if nr_of_skipped < 0:
                        nr_of_items += -nr_of_skipped
                        nr_of_skipped = 0
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
        changed=nr_of_changed,
        unchanged=nr_of_unchanged,
        skipped=nr_of_skipped,
        total=nr_of_items,
    )


def deploy_resources(
    loader: Loader[T_ID, T_WriteClass, T_WritableCogniteResource, T_CogniteResourceList, T_WritableCogniteResourceList],
    path: Path,
    ToolGlobals: CDFToolConfig,
    drop: bool = False,
    clean: bool = False,
    dry_run: bool = False,
    drop_data: bool = False,
    verbose: bool = False,
) -> DeployResult | None:
    filepaths = loader.find_files(path)

    batches: list[list[T_WriteClass]] | None = _load_batches(loader, filepaths, skip_validation=dry_run)
    if batches is None:
        ToolGlobals.failed = True
        return None

    nr_of_batches = len(batches)
    nr_of_items = sum(len(batch) for batch in batches)
    if nr_of_items == 0:
        return DeployResult(name=loader.display_name)

    action_word = "Loading" if dry_run else "Uploading"
    print(f"[bold]{action_word} {nr_of_items} {loader.display_name} in {nr_of_batches} batches to CDF...[/]")

    if drop and loader.support_drop:
        if drop_data and (loader.api_name in ["data_modeling.spaces", "data_modeling.containers"]):
            print(
                f"  --drop-data is specified, will delete existing nodes and edges before before deleting {loader.display_name}."
            )
        else:
            print(f"  --drop is specified, will delete existing {loader.display_name} before uploading.")

    # Deleting resources.
    nr_of_deleted = 0
    if (drop and loader.support_drop) or clean:
        nr_of_deleted = _delete_resources(loader, batches, drop_data, dry_run, verbose)

    nr_of_created = 0
    nr_of_changed = 0
    nr_of_unchanged = 0
    nr_of_skipped = 0
    for batch_no, (batch, filepath) in enumerate(zip(batches, filepaths), 1):
        batch_ids = loader.get_ids(batch)
        cdf_resources = loader.retrieve(batch_ids).as_write()
        cdf_resource_by_id = {loader.get_id(resource): resource for resource in cdf_resources}

        to_create = loader.list_write_cls([])
        to_update = loader.list_write_cls([])
        for item in batch:
            if cdf_resource := cdf_resource_by_id.get(loader.get_id(item)):
                if item == cdf_resource:
                    nr_of_unchanged += 1
                else:
                    to_update.append(item)
            else:
                to_create.append(item)

        if dry_run:
            nr_of_created += len(to_create)
            nr_of_changed += len(to_update)
            if verbose:
                print(
                    f" {batch_no}/{len(batch)} {loader.display_name} would have: Changed {nr_of_changed},"
                    f" Created {nr_of_created}, and left {nr_of_unchanged} unchanged"
                )
            continue

        try:
            created = loader.create(to_create, drop, filepath)
        except Exception as e:
            print(f"  [bold yellow]WARNING:[/] Failed to upload {loader.display_name}. Error {e}.")
            ToolGlobals.failed = True
            return None
        else:
            newly_created = len(created) if created is not None else 0
            nr_of_created += newly_created
            nr_of_skipped += len(batch) - newly_created
            # For timeseries.datapoints, we can load multiple timeseries in one file,
            # so the number of created items can be larger than the number of items in the batch.
            if nr_of_skipped < 0:
                nr_of_items += -nr_of_skipped
                nr_of_skipped = 0

        try:
            updated = loader.update(to_update, filepath)
        except Exception as e:
            print(f"  [bold yellow]WARNING:[/] Failed to update {loader.display_name}. Error {e}.")
            ToolGlobals.failed = True
            return None
        else:
            nr_of_changed += len(updated)

    if verbose:
        print(
            f"  Created {nr_of_created}, Deleted {nr_of_deleted}, Changed {nr_of_changed}, Unchanged {nr_of_unchanged}, Skipped {nr_of_skipped}, Total {nr_of_items}."
        )
    return DeployResult(
        name=loader.display_name,
        created=nr_of_created,
        deleted=nr_of_deleted,
        changed=nr_of_changed,
        unchanged=nr_of_unchanged,
        skipped=nr_of_skipped,
        total=nr_of_items,
    )


def _load_batches(loader: Loader, filepaths: list[Path], skip_validation: bool) -> list[list[T_WriteClass]] | None:
    batches: list[list[T_WriteClass]] = []
    for filepath in filepaths:
        try:
            resource = loader.load_resource(filepath, skip_validation=skip_validation)
        except KeyError as e:
            # KeyError means that we are missing a required field in the yaml file.
            print(
                f"[bold red]ERROR:[/] Failed to load {filepath.name} with {loader.display_name}. Missing required field: {e}."
            )
            return None
        if resource is None:
            print(f"[bold yellow]WARNING:[/] Skipping {filepath.name}. No data to load.")
            continue
        batches.append(list(resource) if isinstance(resource, Sequence) else [resource])
    return batches


def _delete_resources(
    loader: Loader, batches: list[list[T_WriteClass]], drop_data: bool, dry_run: bool, verbose: bool
) -> int:
    nr_of_deleted = 0
    for batch in batches:
        batch_ids = loader.get_ids(batch)
        if dry_run:
            nr_of_deleted += len(batch_ids)
            if verbose:
                print(f"  Would have deleted {len(batch_ids)} {loader.display_name}.")
            continue

        try:
            nr_of_deleted += loader.delete(batch_ids, drop_data)
        except CogniteAPIError as e:
            if e.code == 404:
                print(f"  [bold yellow]WARNING:[/] {len(batch_ids)} {loader.display_name} do(es) not exist.")
        except CogniteNotFoundError:
            print(f"  [bold yellow]WARNING:[/] {len(batch_ids)} {loader.display_name} do(es) not exist.")
        except Exception as e:
            print(f"  [bold yellow]WARNING:[/] Failed to delete {len(batch_ids)} {loader.display_name}. Error {e}.")
        else:  # Delete succeeded
            if verbose:
                print(f"  Deleted {len(batch_ids)} {loader.display_name}.")
    return nr_of_deleted


LOADER_BY_FOLDER_NAME: dict[str, list[type[Loader]]] = {}
for _loader in Loader.__subclasses__():
    if _loader.folder_name not in LOADER_BY_FOLDER_NAME:
        LOADER_BY_FOLDER_NAME[_loader.folder_name] = []
    # MyPy bug: https://github.com/python/mypy/issues/4717
    LOADER_BY_FOLDER_NAME[_loader.folder_name].append(_loader)  # type: ignore[type-abstract]
del _loader  # cleanup module namespace
