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
import re
from abc import ABC, abstractmethod
from collections import Counter, defaultdict
from collections.abc import Sequence, Sized
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Generic, Literal, TypeVar, Union, final

import pandas as pd
from cognite.client import CogniteClient
from cognite.client.data_classes import (
    DataSet,
    DataSetList,
    ExtractionPipeline,
    ExtractionPipelineList,
    FileMetadata,
    FileMetadataList,
    OidcCredentials,
    TimeSeries,
    TimeSeriesList,
    Transformation,
    TransformationList,
    capabilities,
)
from cognite.client.data_classes._base import (
    CogniteObject,
    CogniteResource,
    CogniteResourceList,
)
from cognite.client.data_classes.capabilities import (
    Capability,
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
    ContainerProperty,
    DataModelApply,
    NodeApply,
    NodeApplyList,
    NodeOrEdgeData,
    SpaceApply,
    ViewApply,
    ViewId,
)
from cognite.client.data_classes.iam import Group, GroupList
from cognite.client.exceptions import CogniteAPIError, CogniteDuplicatedError, CogniteNotFoundError
from rich import print

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
    """

    support_drop = True
    support_upsert = False
    filetypes = frozenset({"yaml", "yml"})
    api_name: str
    folder_name: str
    resource_cls: type[CogniteResource]
    list_cls: type[CogniteResourceList]
    dependencies: frozenset[Loader] = frozenset()

    def __init__(self, client: CogniteClient):
        self.client = client
        try:
            self.api_class = self._get_api_class(client, self.api_name)
        except AttributeError:
            raise AttributeError(f"Invalid api_name {self.api_name}.")

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
        return cls(client)

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
    def create(
        self, items: Sequence[T_Resource], ToolGlobals: CDFToolConfig, drop: bool, filepath: Path
    ) -> T_ResourceList:
        try:
            created = self.api_class.create(items)
            return created
        except CogniteAPIError as e:
            if e.code == 409:
                print("  [bold yellow]WARNING:[/] Resource(s) already exist(s), skipping creation.")
                return []
            else:
                print(f"[bold red]ERROR:[/] Failed to create resource(s).\n{e}")
                ToolGlobals.failed = True
                return []
        except CogniteDuplicatedError as e:
            print(
                f"  [bold yellow]WARNING:[/] {len(e.duplicated)} out of {len(items)} resource(s) already exist(s). {len(e.successful or [])} resource(s) created."
            )
            return []
        except Exception as e:
            print(f"[bold red]ERROR:[/] Failed to create resource(s).\n{e}")
            ToolGlobals.failed = True
            return []

    def delete(self, ids: Sequence[T_ID]) -> int:
        self.api_class.delete(ids)
        return len(ids)

    def retrieve(self, ids: Sequence[T_ID]) -> T_ResourceList:
        return self.api_class.retrieve(ids)

    def load_resource(self, filepath: Path, ToolGlobals: CDFToolConfig) -> T_Resource | T_ResourceList:
        raw_yaml = load_yaml_inject_variables(filepath, ToolGlobals.environment_variables())
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
        target_scopes: Literal[
            "all", "all_skipped_validation", "all_scoped_skipped_validation", "resource_scoped_only", "all_scoped_only"
        ] = "all",
    ):
        super().__init__(client)
        self.load = target_scopes

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
        return cls(client, target_scopes)

    @classmethod
    def get_required_capability(cls, ToolGlobals: CDFToolConfig) -> Capability:
        return GroupsAcl(
            [GroupsAcl.Action.Read, GroupsAcl.Action.List, GroupsAcl.Action.Create, GroupsAcl.Action.Delete],
            GroupsAcl.Scope.All(),
        )

    @classmethod
    def get_id(cls, item: Group) -> str:
        return item.name

    def load_resource(self, filepath: Path, ToolGlobals: CDFToolConfig) -> Group:
        raw = load_yaml_inject_variables(filepath, ToolGlobals.environment_variables())
        for capability in raw.get("capabilities", []):
            for _, values in capability.items():
                if len(values.get("scope", {}).get("datasetScope", {}).get("ids", [])) > 0:
                    if self.load not in ["all_skipped_validation", "all_scoped_skipped_validation"]:
                        values["scope"]["datasetScope"]["ids"] = [
                            ToolGlobals.verify_dataset(ext_id)
                            for ext_id in values.get("scope", {}).get("datasetScope", {}).get("ids", [])
                        ]
                    else:
                        values["scope"]["datasetScope"]["ids"] = [-1]

                if len(values.get("scope", {}).get("extractionPipelineScope", {}).get("ids", [])) > 0:
                    if self.load not in ["all_skipped_validation", "all_scoped_skipped_validation"]:
                        values["scope"]["extractionPipelineScope"]["ids"] = [
                            ToolGlobals.verify_extraction_pipeline(ext_id)
                            for ext_id in values.get("scope", {}).get("extractionPipelineScope", {}).get("ids", [])
                        ]
                    else:
                        values["scope"]["extractionPipelineScope"]["ids"] = [-1]
        return Group.load(raw)

    def retrieve(self, ids: Sequence[int]) -> T_ResourceList:
        remote = self.client.iam.groups.list(all=True).data
        found = [g for g in remote if g.name in ids]
        return found

    def delete(self, ids: Sequence[int]) -> int:
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

    def create(self, items: Sequence[Group], ToolGlobals: CDFToolConfig, drop: bool, filepath: Path) -> GroupList:
        if self.load == "all":
            to_create = items
        elif self.load == "all_skipped_validation":
            raise ValueError("all_skipped_validation is not supported for group creation as scopes would be wrong.")
        elif self.load == "resource_scoped_only":
            to_create = []
            for item in items:
                item.capabilities = [
                    capability for capability in item.capabilities if type(capability.scope) in self.resource_scopes
                ]
                if item.capabilities:
                    to_create.append(item)
        elif self.load == "all_scoped_only" or self.load == "all_scoped_skipped_validation":
            to_create = []
            for item in items:
                item.capabilities = [
                    capability for capability in item.capabilities if type(capability.scope) not in self.resource_scopes
                ]
                if item.capabilities:
                    to_create.append(item)
        else:
            raise ValueError(f"Invalid load value {self.load}")

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

    def delete(self, ids: Sequence[str]) -> int:
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

    def create(
        self, items: Sequence[T_Resource], ToolGlobals: CDFToolConfig, drop: bool, filepath: Path
    ) -> T_ResourceList | None:
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
                ToolGlobals.failed = True
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
    data_file_types = frozenset({"csv", "parquet"})

    @classmethod
    def get_required_capability(cls, ToolGlobals: CDFToolConfig) -> Capability:
        return RawAcl([RawAcl.Action.Read, RawAcl.Action.Write], RawAcl.Scope.All())

    @classmethod
    def get_id(cls, item: RawTable) -> RawTable:
        return item

    def delete(self, ids: Sequence[RawTable]) -> int:
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

    def create(
        self, items: Sequence[RawTable], ToolGlobals: CDFToolConfig, drop: bool, filepath: Path
    ) -> list[RawTable]:
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

    def delete(self, ids: Sequence[str]) -> int:
        self.client.time_series.delete(external_id=ids, ignore_unknown_ids=True)
        return len(ids)

    def load_resource(self, filepath: Path, ToolGlobals: CDFToolConfig) -> TimeSeries | TimeSeriesList:
        resources = load_yaml_inject_variables(filepath, {})
        if not isinstance(resources, list):
            resources = [resources]
        for resource in resources:
            if resource.get("dataSetExternalId") is not None:
                resource["dataSetId"] = ToolGlobals.verify_dataset(resource.pop("dataSetExternalId"))
        return TimeSeriesList.load(resources)


@final
class TransformationLoader(Loader[str, Transformation, TransformationList]):
    api_name = "transformations"
    folder_name = "transformations"
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

    def load_resource(self, filepath: Path, ToolGlobals: CDFToolConfig) -> Transformation:
        raw = load_yaml_inject_variables(filepath, ToolGlobals.environment_variables())
        # The `authentication` key is custom for this template:
        source_oidc_credentials = raw.get("authentication", {}).get("read") or raw.get("authentication") or {}
        destination_oidc_credentials = raw.get("authentication", {}).get("write") or raw.get("authentication") or {}
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
        transformation.data_set_id = ToolGlobals.data_set_id
        return transformation

    def delete(self, ids: Sequence[str]) -> int:
        self.client.transformations.delete(external_id=ids, ignore_unknown_ids=True)
        return len(ids)

    def create(
        self, items: Sequence[Transformation], ToolGlobals: CDFToolConfig, drop: bool, filepath: Path
    ) -> TransformationList:
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
            ToolGlobals.failed = True
            return TransformationList([])
        for t in items if isinstance(items, Sequence) else [items]:
            if t.schedule.interval != "":
                t.schedule.external_id = t.external_id
                self.client.transformations.schedules.create(t.schedule)
        return created


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

    def load_resource(self, filepath: Path, ToolGlobals: CDFToolConfig) -> Path:
        return filepath

    @classmethod
    def get_id(cls, item: Path) -> list[str]:
        raise NotImplementedError

    def delete(self, ids: Sequence[str]) -> int:
        # Drop all datapoints?
        raise NotImplementedError()

    def create(self, items: Sequence[Path], ToolGlobals: CDFToolConfig, drop: bool, filepath: Path) -> TimeSeriesList:
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

    def delete(self, ids: Sequence[str]) -> int:
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

    def load_resource(self, filepath: Path, ToolGlobals: CDFToolConfig) -> ExtractionPipeline:
        resource = load_yaml_inject_variables(filepath, {})
        if resource.get("dataSetExternalId") is not None:
            resource["dataSetId"] = ToolGlobals.verify_dataset(resource.pop("dataSetExternalId"))
        return ExtractionPipeline.load(resource)

    def create(
        self, items: Sequence[T_Resource], ToolGlobals: CDFToolConfig, drop: bool, filepath: Path
    ) -> T_ResourceList | None:
        try:
            return ExtractionPipelineList(self.client.extraction_pipelines.create(items))

        except CogniteDuplicatedError as e:
            if len(e.duplicated) < len(items):
                for dup in e.duplicated:
                    ext_id = dup.get("externalId", None)
                    for item in items:
                        if item.external_id == ext_id:
                            items.remove(item)
                try:
                    return ExtractionPipelineList(self.client.extraction_pipelines.create(items))
                except Exception as e:
                    print(f"[bold red]ERROR:[/] Failed to create extraction pipelines.\n{e}")
                    ToolGlobals.failed = True
                    return None
            return None


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

    def delete(self, ids: Sequence[str]) -> int:
        self.client.files.delete(external_id=ids)
        return len(ids)

    def load_resource(self, filepath: Path, ToolGlobals: CDFToolConfig) -> FileMetadata | FileMetadataList:
        try:
            files = FileMetadataList(
                [FileMetadata.load(load_yaml_inject_variables(filepath, ToolGlobals.environment_variables()))]
            )
        except Exception:
            files = FileMetadataList.load(load_yaml_inject_variables(filepath, ToolGlobals.environment_variables()))
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
                file.data_set_id = ToolGlobals.verify_dataset(file.data_set_id)
        return files

    def create(
        self, items: Sequence[FileMetadata], ToolGlobals: CDFToolConfig, drop: bool, filepath: Path
    ) -> FileMetadataList:
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
                ToolGlobals.failed = True
                return created
        return created


def drop_load_resources(
    loader: Loader,
    path: Path,
    ToolGlobals: CDFToolConfig,
    drop: bool = False,
    clean: bool = False,
    load: bool = True,
    dry_run: bool = False,
    verbose: bool = False,
):
    if path.is_file():
        if path.suffix not in loader.filetypes or not loader.filetypes:
            raise ValueError("Invalid file type")
        filepaths = [path]
    elif loader.filetypes:
        filepaths = [file for type_ in loader.filetypes for file in path.glob(f"**/*.{type_}")]
    else:
        filepaths = [file for file in path.glob("**/*")]

    items = [loader.load_resource(f, ToolGlobals) for f in filepaths]
    nr_of_batches = len(items)
    nr_of_items = sum(len(item) if isinstance(item, Sized) else 1 for item in items)
    nr_of_deleted = 0
    nr_of_created = 0
    if load:
        print(f"[bold]Uploading {nr_of_items} {loader.api_name} in {nr_of_batches} batches to CDF...[/]")
    else:
        print(f"[bold]Cleaning {nr_of_items} {loader.api_name} in {nr_of_batches} batches to CDF...[/]")
    batches = [item if isinstance(item, Sized) else [item] for item in items]
    if drop and loader.support_drop and load:
        print(f"  --drop is specified, will delete existing {loader.api_name} before uploading.")
    if (drop and loader.support_drop) or clean:
        for batch in batches:
            drop_items: list = []
            for item in batch:
                # Set the context info for this CDF project
                if hasattr(item, "data_set_id") and ToolGlobals.data_set_id is not None:
                    item.data_set_id = ToolGlobals.data_set_id
                drop_items.append(loader.get_id(item))
            if not dry_run:
                try:
                    nr_of_deleted += loader.delete(drop_items)
                    if verbose:
                        print(f"  Deleted {len(drop_items)} {loader.api_name}.")
                except CogniteAPIError as e:
                    if e.code == 404:
                        print(f"  [bold yellow]WARNING:[/] {len(drop_items)} {loader.api_name} do(es) not exist.")
                except CogniteNotFoundError:
                    print(f"  [bold yellow]WARNING:[/] {len(drop_items)} {loader.api_name} do(es) not exist.")
                except Exception as e:
                    print(f"  [bold yellow]WARNING:[/] Failed to delete {len(drop_items)} {loader.api_name}. Error {e}")
            else:
                print(f"  Would have deleted {len(drop_items)} {loader.api_name}.")
    if not load:
        return
    try:
        if not dry_run:
            for batch, filepath in zip(batches, filepaths):
                if not drop and loader.support_upsert:
                    if verbose:
                        print(f"  Comparing {len(batch)} {loader.api_name} from {filepath}...")
                    batch = loader.remove_unchanged(batch)
                    if verbose:
                        print(f"    {len(batch)} {loader.api_name} to be deployed...")
                if len(batch) > 0:
                    created = loader.create(batch, ToolGlobals, drop, filepath)
                    nr_of_created += len(created) if created is not None else 0
                    if isinstance(loader, AuthLoader):
                        nr_of_deleted += len(created)
    except Exception as e:
        print(f"[bold red]ERROR:[/] Failed to upload {loader.api_name}.")
        print(e)
        ToolGlobals.failed = True
        return
    print(f"  Deleted {nr_of_deleted} out of {nr_of_items} {loader.api_name} from {len(filepaths)} config files.")
    print(f"  Created {nr_of_created} out of {nr_of_items} {loader.api_name} from {len(filepaths)} config files.")


LOADER_BY_FOLDER_NAME = {loader.folder_name: loader for loader in Loader.__subclasses__()}


def load_datamodel_graphql(
    ToolGlobals: CDFToolConfig,
    space_name: str | None = None,
    model_name: str | None = None,
    directory=None,
) -> None:
    """Load a graphql datamodel from file."""
    if space_name is None or model_name is None or directory is None:
        raise ValueError("space_name, model_name, and directory must be supplied.")
    with open(f"{directory}/datamodel.graphql") as file:
        # Read directly into a string.
        datamodel = file.read()
    # Clear any delete errors
    ToolGlobals.failed = False
    client = ToolGlobals.verify_client(
        capabilities={
            "dataModelsAcl": ["READ", "WRITE"],
            "dataModelInstancesAcl": ["READ", "WRITE"],
        }
    )
    print(f"[bold]Loading data model {model_name} into space {space_name} from {directory}...[/]")
    try:
        client.data_modeling.graphql.apply_dml(
            (space_name, model_name, "1"),
            dml=datamodel,
            name=model_name,
            description=f"Data model for {model_name}",
        )
    except Exception as e:
        print(f"[bold red]ERROR:[/] Failed to write data model {model_name} to space {space_name}.")
        print(e)
        ToolGlobals.failed = True
        return
    print(f"  Created data model {model_name}.")


def load_datamodel(
    ToolGlobals: CDFToolConfig,
    drop: bool = False,
    drop_data: bool = False,
    delete_removed: bool = True,
    delete_containers: bool = False,
    delete_spaces: bool = False,
    directory: Path | None = None,
    dry_run: bool = False,
    only_drop: bool = False,
) -> None:
    """Load containers, views, spaces, and data models from a directory

        Note that this function will never delete instances, but will delete all
        the properties found in containers if delete_containers is specified.
        delete_spaces will fail unless also the edges and nodes have been deleted,
        e.g. using the clean_out_datamodel() function.

        Note that if delete_spaces flag is True, an attempt will be made to delete the space,
        but if it fails, the loading will continue. If delete_containers is True, the loading
        will abort if deletion fails.
    Args:
        drop: Whether to drop all existing data model entities (default: apply just the diff).
        drop_data: Whether to drop all instances (nodes and edges) in all spaces.
        delete_removed: Whether to delete (previous) resources that are not in the directory.
        delete_containers: Whether to delete containers including data in the instances.
        delete_spaces: Whether to delete spaces (requires containers and instances to be deleted).
        directory: Directory to load from.
        dry_run: Whether to perform a dry run and only print out what will happen.
        only_drop: Whether to only drop existing resources and not load new ones.
    """
    if directory is None:
        raise ValueError("directory must be supplied.")
    if (delete_containers or delete_spaces) and not drop:
        raise ValueError("drop must be True if delete_containers or delete_spaces is True.")
    if (delete_spaces or delete_containers) and not drop_data:
        raise ValueError("drop_data must be True if delete_spaces or delete_containers is True.")
    model_files_by_type: dict[str, list[Path]] = defaultdict(list)
    models_pattern = re.compile(r"^.*\.?(space|container|view|datamodel)\.yaml$")
    for file in directory.rglob("*.yaml"):
        if not (match := models_pattern.match(file.name)):
            continue
        model_files_by_type[match.group(1)].append(file)
    print("[bold]Loading data model files from build directory...[/]")
    for type_, files in model_files_by_type.items():
        model_files_by_type[type_].sort()
        print(f"  {len(files)} of type {type_}s in {directory}")

    cognite_resources_by_type: dict[str, list[ContainerApply | ViewApply | DataModelApply | SpaceApply]] = defaultdict(
        list
    )
    for type_, files in model_files_by_type.items():
        resource_cls = {
            "space": SpaceApply,
            "container": ContainerApply,
            "view": ViewApply,
            "datamodel": DataModelApply,
        }[type_]
        for file in files:
            cognite_resources_by_type[type_].append(
                resource_cls.load(load_yaml_inject_variables(file, ToolGlobals.environment_variables()))
            )
    # Remove duplicates
    for type_ in list(cognite_resources_by_type):
        unique = {r.as_id(): r for r in cognite_resources_by_type[type_]}
        cognite_resources_by_type[type_] = list(unique.values())

    explicit_space_list = [s.space for s in cognite_resources_by_type["space"]]
    space_list = list({r.space for _, resources in cognite_resources_by_type.items() for r in resources})

    implicit_spaces = [SpaceApply(space=s, name=s, description="Imported space") for s in space_list]
    for s in implicit_spaces:
        if s.space not in [s2.space for s2 in cognite_resources_by_type["space"]]:
            print(
                f"  [bold red]ERROR[/] Space {s.name} is implicitly defined and may need it's own {s.name}.space.yaml file."
            )
            cognite_resources_by_type["space"].append(s)
    # Clear any delete errors
    ToolGlobals.failed = False
    client = ToolGlobals.verify_client(
        capabilities={
            "dataModelsAcl": ["READ", "WRITE"],
            "dataModelInstancesAcl": ["READ", "WRITE"],
        }
    )

    existing_resources_by_type: dict[str, list[ContainerApply | ViewApply | DataModelApply | SpaceApply]] = defaultdict(
        list
    )
    resource_api_by_type = {
        "container": client.data_modeling.containers,
        "view": client.data_modeling.views,
        "datamodel": client.data_modeling.data_models,
        "space": client.data_modeling.spaces,
    }
    for type_, resources in cognite_resources_by_type.items():
        attempts = 5
        while attempts > 0:
            try:
                existing_resources_by_type[type_] = (
                    resource_api_by_type[type_].retrieve(list({r.as_id() for r in resources})).as_apply()
                )
                attempts = 0
            except CogniteAPIError as e:
                attempts -= 1
                if e.code == 500 and attempts > 0:
                    continue
                print(f"[bold]ERROR:[/] Failed to retrieve {type_}(s):\n{e}")
                ToolGlobals.failed = True
                return
            except Exception as e:
                print(f"[bold]ERROR:[/] Failed to retrieve {type_}(s):\n{e}")
                ToolGlobals.failed = True
                return

    differences: dict[str, Difference] = {}
    for type_, resources in cognite_resources_by_type.items():
        new_by_id = {r.as_id(): r for r in resources}
        existing_by_id = {r.as_id(): r for r in existing_resources_by_type[type_]}

        added = [r for r in resources if r.as_id() not in existing_by_id]
        removed = [r for r in existing_resources_by_type[type_] if r.as_id() not in new_by_id]

        changed = []
        unchanged = []
        # Due to a bug in the SDK, we need to ensure that the new properties of the container
        # has set the default values as these will be set for the existing container and
        # the comparison will fail.
        for existing_id in set(new_by_id.keys()) & set(existing_by_id.keys()):
            new = new_by_id[existing_id]
            existing = existing_by_id[existing_id]
            if isinstance(new, ContainerApply):
                for p, _ in existing.properties.items():
                    new.properties[p] = ContainerProperty(
                        type=new.properties[p].type,
                        nullable=new.properties[p].nullable or True,
                        auto_increment=new.properties[p].auto_increment or False,
                        default_value=new.properties[p].default_value or None,
                        description=new.properties[p].description or None,
                    )

            if new_by_id[existing_id] == existing_by_id[existing_id]:
                unchanged.append(new_by_id[existing_id])
            else:
                changed.append(new_by_id[existing_id])

        differences[type_] = Difference(added, removed, changed, unchanged)

    creation_order = ["space", "container", "view", "datamodel"]

    if drop_data:
        print("[bold]Deleting existing data...[/]")
        deleted = 0
        for i in explicit_space_list:
            if not dry_run:
                delete_instances(
                    ToolGlobals,
                    space_name=i,
                    dry_run=dry_run,
                )
                if ToolGlobals.failed:
                    print(f"  [bold]ERROR:[/] Failed to delete instances in space {i}.")
                    return
            else:
                print(f"  Would have deleted instances in space {i}.")

    if drop:
        print("[bold]Deleting existing configurations...[/]")
        # Clean out all old resources
        for type_ in reversed(creation_order):
            items = cognite_resources_by_type.get(type_)
            if items is None:
                continue
            if type_ == "container" and not delete_containers:
                print("  [bold]INFO:[/] Skipping deletion of containers as delete_containers flag is not set...")
                continue
            if type_ == "space" and not delete_spaces:
                print("  [bold]INFO:[/] Skipping deletion of spaces as delete_spaces flag is not set...")
                continue
            deleted = 0
            if not dry_run:
                if type_ == "space":
                    for i2 in items:
                        # Only delete spaces that have been explicitly defined
                        if i2.space in explicit_space_list:
                            try:
                                ret = resource_api_by_type["space"].delete(i2.space)
                            except Exception:
                                ToolGlobals.failed = False
                                print(f"  [bold]INFO:[/] Deletion of space {i2.space} was not successful, continuing.")
                                continue
                            if len(ret) > 0:
                                deleted += 1
                else:
                    try:
                        ret = resource_api_by_type[type_].delete([i.as_id() for i in items])
                    except CogniteAPIError as e:
                        # Typically spaces can not be deleted if there are other
                        # resources in the space.
                        print(f"  [bold]ERROR:[/] Failed to delete {type_}(s):\n{e}")
                        return
                    deleted += len(ret)
                print(f"  Deleted {deleted} {type_}(s).")
            else:
                print(f"  Would have deleted {deleted} {type_}(s).")

    if not only_drop:
        print("[bold]Creating new configurations...[/]")
        for type_ in creation_order:
            if type_ not in differences:
                continue
            items = differences[type_]
            if items.added:
                print(f"  {len(items.added)} added {type_}(s) to be deployed...")
                if dry_run:
                    continue
                attempts = 5
                while attempts > 0:
                    try:
                        resource_api_by_type[type_].apply(items.added)
                        attempts = 0
                    except Exception as e:
                        attempts -= 1
                        if attempts > 0:
                            continue
                        print(f"[bold]ERROR:[/] Failed to create {type_}(s):\n{e}")
                        ToolGlobals.failed = True
                        return
                print(f"  Created {len(items.added)} {type_}(s).")
            elif items.changed:
                print(f"  {len(items.changed)} changed {type_}(s) to be deployed...")
                if dry_run:
                    continue
                attempts = 5
                while attempts > 0:
                    try:
                        resource_api_by_type[type_].apply(items.changed)
                        attempts = 0
                    except Exception as e:
                        attempts -= 1
                        if attempts > 0:
                            continue
                        print(f"[bold]ERROR:[/] Failed to create {type_}(s):\n{e}")
                        ToolGlobals.failed = True
                        return
                if drop:
                    print(
                        f"  Created {len(items.changed)} {type_}s that could have been updated instead (--drop specified)."
                    )
                else:
                    print(f"  Updated {len(items.changed)} {type_}(s).")
            elif items.unchanged:
                print(f"  {len(items.unchanged)} unchanged {type_}(s).")
                if drop:
                    attempts = 5
                    while attempts > 0:
                        try:
                            resource_api_by_type[type_].apply(items.unchanged)
                            attempts = 0
                        except Exception as e:
                            attempts -= 1
                            if attempts > 0:
                                continue
                            print(f"[bold]ERROR:[/] Failed to create {type_}(s):\n{e}")
                            ToolGlobals.failed = True
                            return
                    print(
                        f"  Created {len(items.unchanged)} unchanged {type_}(s) that could have been skipped (--drop specified)."
                    )

    if delete_removed and not drop:
        for type_ in reversed(creation_order):
            if type_ not in differences:
                continue
            items = differences[type_]
            if items.removed:
                if dry_run:
                    print(f"  Would have deleted {len(items.removed)} {type_}(s).")
                    continue
                try:
                    resource_api_by_type[type_].delete(items.removed)
                except CogniteAPIError as e:
                    # Typically spaces can not be deleted if there are other
                    # resources in the space.
                    print(f"[bold]ERROR:[/] Failed to delete {len(items.removed)} {type_}(s).")
                    print(e)
                    ToolGlobals.failed = True
                    continue
                print(f"  Deleted {len(items.removed)} {type_}(s) that were removed.")


def load_nodes(
    ToolGlobals: CDFToolConfig,
    directory: Path | None = None,
    dry_run: bool = False,
) -> None:
    """Insert nodes"""

    for file in directory.rglob("*.node.yaml"):
        if file.name == "config.yaml":
            continue

        client: CogniteClient = ToolGlobals.verify_client(
            capabilities={
                "dataModelsAcl": ["READ"],
                "dataModelInstancesAcl": ["READ", "WRITE"],
            }
        )

        nodes: dict = load_yaml_inject_variables(file, ToolGlobals.environment_variables())

        try:
            view = ViewId(
                space=nodes["view"]["space"],
                external_id=nodes["view"]["externalId"],
                version=nodes["view"]["version"],
            )
        except KeyError:
            raise KeyError(
                f"Expected view configuration not found in {file}:\nview:\n  space: <space>\n  externalId: <view_external_id>\n  version: <view_version>"
            )

        try:
            node_space: str = nodes["destination"]["space"]
        except KeyError:
            raise KeyError(
                f"Expected destination space configuration in {file}:\ndestination:\n  space: <destination_space_external_id>"
            )
        node_list: NodeApplyList = []
        try:
            for n in nodes.get("nodes", []):
                node_list.append(
                    NodeApply(
                        space=node_space,
                        external_id=n.pop("externalId"),
                        existing_version=n.pop("existingVersion", None),
                        sources=[NodeOrEdgeData(source=view, properties=n)],
                    )
                )
        except Exception as e:
            raise KeyError(f"Failed to parse node {n} in {file}:\n{e}")
        print(f"[bold]Loading {len(node_list)} node(s) from {directory}...[/]")
        if not dry_run:
            try:
                client.data_modeling.instances.apply(
                    nodes=node_list,
                    auto_create_direct_relations=nodes.get("autoCreateDirectRelations", True),
                    skip_on_version_conflict=nodes.get("skipOnVersionConflict", False),
                    replace=nodes.get("replace", False),
                )
                print(f"  Created {len(node_list)} node(s) in {node_space}.")
            except CogniteAPIError as e:
                print(f"[bold]ERROR:[/] Failed to create {len(node_list)} node(s) in {node_space}:\n{e}")
                ToolGlobals.failed = True
                return
