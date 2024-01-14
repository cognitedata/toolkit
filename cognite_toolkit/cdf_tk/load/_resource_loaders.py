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
from collections.abc import Iterable, Sequence
from contextlib import suppress
from pathlib import Path
from typing import Literal, cast, final

import pandas as pd
import yaml
from cognite.client import CogniteClient
from cognite.client.data_classes import (
    DatapointsList,
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
    filters,
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
    DataModelList,
    Edge,
    EdgeApply,
    EdgeApplyResultList,
    EdgeList,
    Node,
    NodeApply,
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
    NodeId,
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

from cognite_toolkit.cdf_tk.utils import CDFToolConfig, load_yaml_inject_variables

from ._base_loaders import ResourceContainerLoader, ResourceLoader
from .data_classes import LoadableEdges, LoadableNodes, RawTable, RawTableList

_MIN_TIMESTAMP_MS = -2208988800000  # 1900-01-01 00:00:00.000
_MAX_TIMESTAMP_MS = 4102444799999  # 2099-12-31 23:59:59.999


@final
class AuthLoader(ResourceLoader[str, GroupWrite, Group, GroupWriteList, GroupList]):
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
        target_scopes: Literal[
            "all", "all_skipped_validation", "all_scoped_skipped_validation", "resource_scoped_only", "all_scoped_only"
        ] = "all",
    ):
        super().__init__(client)
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
        return AuthLoader(client, target_scopes)

    @classmethod
    def get_required_capability(cls, ToolGlobals: CDFToolConfig) -> Capability | list[Capability]:
        return GroupsAcl(
            [GroupsAcl.Action.Read, GroupsAcl.Action.List, GroupsAcl.Action.Create, GroupsAcl.Action.Delete],
            GroupsAcl.Scope.All(),
        )

    @classmethod
    def get_id(cls, item: GroupWrite | Group) -> str:
        return item.name

    def load_resource(self, filepath: Path, ToolGlobals: CDFToolConfig, skip_validation: bool) -> GroupWrite:
        raw = load_yaml_inject_variables(filepath, ToolGlobals.environment_variables(), required_return_type="dict")
        for capability in raw.get("capabilities", []):
            for _, values in capability.items():
                for scope in ["datasetScope", "idScope"]:
                    if len(ids := values.get("scope", {}).get(scope, {}).get("ids", [])) > 0:
                        if not skip_validation and self.target_scopes not in [
                            "all_skipped_validation",
                            "all_scoped_skipped_validation",
                        ]:
                            values["scope"][scope]["ids"] = [
                                ToolGlobals.verify_dataset(ext_id) if isinstance(ext_id, str) else ext_id
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
                            ToolGlobals.verify_extraction_pipeline(ext_id)
                            for ext_id in values.get("scope", {}).get("extractionPipelineScope", {}).get("ids", [])
                        ]
                    else:
                        values["scope"]["extractionPipelineScope"]["ids"] = [-1]
        return GroupWrite.load(raw)

    def create(self, items: Sequence[GroupWrite]) -> GroupList:
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

    def update(self, items: Sequence[GroupWrite]) -> GroupList:
        return self.client.iam.groups.create(items)

    def retrieve(self, ids: SequenceNotStr[str]) -> GroupList:
        remote = self.client.iam.groups.list(all=True)
        found = [g for g in remote if g.name in ids]
        return GroupList(found)

    def delete(self, ids: SequenceNotStr[str]) -> int:
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
class DataSetsLoader(ResourceLoader[str, DataSetWrite, DataSet, DataSetWriteList, DataSetList]):
    support_drop = False
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

    def load_resource(self, filepath: Path, ToolGlobals: CDFToolConfig, skip_validation: bool) -> DataSetWriteList:
        resource = load_yaml_inject_variables(filepath, {})

        data_sets = [resource] if isinstance(resource, dict) else resource

        for data_set in data_sets:
            if data_set.get("metadata"):
                for key, value in data_set["metadata"].items():
                    data_set["metadata"][key] = json.dumps(value)
        return DataSetWriteList.load(data_sets)

    def create(self, items: Sequence[DataSetWrite]) -> DataSetList:
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
        return created

    def retrieve(self, ids: SequenceNotStr[str]) -> DataSetList:
        return self.client.data_sets.retrieve_multiple(external_ids=cast(Sequence, ids), ignore_unknown_ids=True)

    def delete(self, ids: SequenceNotStr[str]) -> int:
        raise NotImplementedError("CDF does not support deleting data sets.")


@final
class RawLoader(ResourceLoader[RawTable, RawTable, RawTable, RawTableList, RawTableList]):
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

    def create(self, items: Sequence[RawTable]) -> list[RawTable]:
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

    def update(self, items: Sequence[RawTable]) -> RawTableList:
        raise NotImplementedError("Raw tables do not support update.")

    def delete(self, ids: SequenceNotStr[RawTable]) -> int:
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
class TimeSeriesLoader(ResourceContainerLoader[str, TimeSeriesWrite, TimeSeries, TimeSeriesWriteList, TimeSeriesList]):
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

    def load_resource(self, filepath: Path, ToolGlobals: CDFToolConfig, skip_validation: bool) -> TimeSeriesWriteList:
        resources = load_yaml_inject_variables(filepath, {})
        if not isinstance(resources, list):
            resources = [resources]
        for resource in resources:
            if resource.get("dataSetExternalId") is not None:
                ds_external_id = resource.pop("dataSetExternalId")
                resource["dataSetId"] = ToolGlobals.verify_dataset(ds_external_id) if not skip_validation else -1
        return TimeSeriesWriteList.load(resources)

    def retrieve(self, ids: SequenceNotStr[str]) -> TimeSeriesList:
        return self.client.time_series.retrieve_multiple(external_ids=cast(Sequence, ids), ignore_unknown_ids=True)

    def delete(self, ids: SequenceNotStr[str]) -> int:
        self.client.time_series.delete(external_id=cast(Sequence, ids), ignore_unknown_ids=True)
        return len(ids)

    def count(self, ids: SequenceNotStr[str]) -> int:
        datapoints = cast(
            DatapointsList,
            self.client.time_series.data.retrieve(
                external_id=cast(Sequence, ids),
                start=_MIN_TIMESTAMP_MS,
                end=_MAX_TIMESTAMP_MS + 1,
                aggregates="count",
                granularity="1000d",
            ),
        )
        return sum(sum(data.count or []) for data in datapoints)

    def drop_data(self, ids: SequenceNotStr[str]) -> int:
        for external_id in ids:
            self.client.time_series.data.delete_range(
                external_id=external_id, start=_MIN_TIMESTAMP_MS, end=_MAX_TIMESTAMP_MS + 1
            )
        return len(ids)


@final
class TransformationLoader(
    ResourceLoader[str, TransformationWrite, Transformation, TransformationWriteList, TransformationList]
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

    def load_resource(self, filepath: Path, ToolGlobals: CDFToolConfig, skip_validation: bool) -> TransformationWrite:
        raw = load_yaml_inject_variables(filepath, ToolGlobals.environment_variables(), required_return_type="dict")
        # The `authentication` key is custom for this template:

        source_oidc_credentials = raw.get("authentication", {}).get("read") or raw.get("authentication") or None
        destination_oidc_credentials = raw.get("authentication", {}).get("write") or raw.get("authentication") or None
        if raw.get("dataSetExternalId") is not None:
            ds_external_id = raw.pop("dataSetExternalId")
            raw["dataSetId"] = ToolGlobals.verify_dataset(ds_external_id) if not skip_validation else -1

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

    def delete(self, ids: SequenceNotStr[str]) -> int:
        self.client.transformations.delete(external_id=cast(Sequence, ids), ignore_unknown_ids=True)
        return len(ids)


@final
class TransformationScheduleLoader(
    ResourceLoader[
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

    def load_resource(
        self, filepath: Path, ToolGlobals: CDFToolConfig, skip_validation: bool
    ) -> TransformationScheduleWrite:
        raw = load_yaml_inject_variables(filepath, ToolGlobals.environment_variables(), required_return_type="dict")
        return TransformationScheduleWrite.load(raw)

    def create(self, items: Sequence[TransformationScheduleWrite]) -> TransformationScheduleList:
        try:
            return self.client.transformations.schedules.create(list(items))
        except CogniteDuplicatedError as e:
            existing = {external_id for dup in e.duplicated if (external_id := dup.get("externalId", None))}
            print(
                f"  [bold yellow]WARNING:[/] {len(e.duplicated)} transformation schedules already exist(s): {existing}"
            )
            new_items = [item for item in items if item.external_id not in existing]
            return self.client.transformations.schedules.create(new_items)

    def delete(self, ids: SequenceNotStr[str]) -> int:
        try:
            self.client.transformations.schedules.delete(external_id=cast(Sequence, ids), ignore_unknown_ids=False)
            return len(ids)
        except CogniteNotFoundError as e:
            return len(ids) - len(e.not_found)


@final
class ExtractionPipelineLoader(
    ResourceLoader[
        str, ExtractionPipelineWrite, ExtractionPipeline, ExtractionPipelineWriteList, ExtractionPipelineList
    ]
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

    def load_resource(
        self, filepath: Path, ToolGlobals: CDFToolConfig, skip_validation: bool
    ) -> ExtractionPipelineWrite:
        resource = load_yaml_inject_variables(filepath, {}, required_return_type="dict")

        if resource.get("dataSetExternalId") is not None:
            ds_external_id = resource.pop("dataSetExternalId")
            resource["dataSetId"] = ToolGlobals.verify_dataset(ds_external_id) if not skip_validation else -1

        return ExtractionPipelineWrite.load(resource)

    def create(self, items: Sequence[ExtractionPipelineWrite]) -> ExtractionPipelineList:
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

                return self.client.extraction_pipelines.create(items)
        return ExtractionPipelineList([])

    def delete(self, ids: SequenceNotStr[str]) -> int:
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
    ResourceLoader[
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

    def load_resource(
        self, filepath: Path, ToolGlobals: CDFToolConfig, skip_validation: bool
    ) -> ExtractionPipelineConfigWrite:
        resource = load_yaml_inject_variables(filepath, {}, required_return_type="dict")
        try:
            resource["config"] = yaml.dump(resource.get("config", ""), indent=4)
        except Exception:
            print(
                "[yellow]WARNING:[/] configuration could not be parsed as valid YAML, which is the recommended format.\n"
            )
            resource["config"] = resource.get("config", "")
        return ExtractionPipelineConfigWrite.load(resource)

    def create(self, items: Sequence[ExtractionPipelineConfigWrite]) -> ExtractionPipelineConfigList:
        return ExtractionPipelineConfigList([self.client.extraction_pipelines.config.create(items[0])])

    def delete(self, ids: SequenceNotStr[str]) -> int:
        count = 0
        for id_ in ids:
            result = self.client.extraction_pipelines.config.list(external_id=id_)
            count += len(result)
        return count


@final
class FileLoader(ResourceLoader[str, FileMetadataWrite, FileMetadata, FileMetadataWriteList, FileMetadataList]):
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

    def load_resource(
        self, filepath: Path, ToolGlobals: CDFToolConfig, skip_validation: bool
    ) -> FileMetadataWrite | FileMetadataWriteList:
        try:
            resource = load_yaml_inject_variables(
                filepath, ToolGlobals.environment_variables(), required_return_type="dict"
            )
            if resource.get("dataSetExternalId") is not None:
                ds_external_id = resource.pop("dataSetExternalId")
                resource["dataSetId"] = ToolGlobals.verify_dataset(ds_external_id) if not skip_validation else -1
            files_metadata = FileMetadataWriteList([FileMetadataWrite.load(resource)])
        except Exception:
            files_metadata = FileMetadataWriteList.load(
                load_yaml_inject_variables(filepath, ToolGlobals.environment_variables(), required_return_type="list")
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
                meta.data_set_id = ToolGlobals.verify_dataset(meta.data_set_id) if not skip_validation else -1
        return files_metadata

    def create(self, items: Sequence[FileMetadataWrite]) -> FileMetadataList:
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
        return created

    def delete(self, ids: SequenceNotStr[str]) -> int:
        self.client.files.delete(external_id=cast(Sequence, ids))
        return len(ids)


@final
class SpaceLoader(ResourceContainerLoader[str, SpaceApply, Space, SpaceApplyList, SpaceList]):
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

    def create(self, items: Sequence[SpaceApply]) -> SpaceList:
        return self.client.data_modeling.spaces.apply(items)

    def update(self, items: Sequence[SpaceApply]) -> SpaceList:
        return self.client.data_modeling.spaces.apply(items)

    def delete(self, ids: SequenceNotStr[str]) -> int:
        deleted = self.client.data_modeling.spaces.delete(ids)
        return len(deleted)

    def count(self, ids: SequenceNotStr[str]) -> int:
        # Bug in spec of aggregate requiring view_id to be passed in, so we cannot use it.
        # When this bug is fixed, it will be much faster to use aggregate.
        return sum(len(batch) for batch in self._iterate_over_nodes(ids)) + sum(
            len(batch) for batch in self._iterate_over_edges(ids)
        )

    def drop_data(self, ids: SequenceNotStr[str]) -> int:
        print(f"[bold]Deleting existing data in spaces {ids}...[/]")
        nr_of_deleted = 0
        for node_ids in self._iterate_over_nodes(ids):
            self.client.data_modeling.instances.delete(nodes=node_ids)
            nr_of_deleted += len(node_ids)
        for edge_ids in self._iterate_over_edges(ids):
            self.client.data_modeling.instances.delete(edges=edge_ids)
            nr_of_deleted += len(edge_ids)
        return nr_of_deleted

    def _iterate_over_nodes(self, ids: SequenceNotStr[str]) -> Iterable[list[NodeId]]:
        is_space: filters.Filter
        if len(ids) == 1:
            is_space = filters.Equals(["node", "space"], ids[0])
        else:
            is_space = filters.In(["node", "space"], list(ids))
        for instances in self.client.data_modeling.instances(
            chunk_size=1000, instance_type="node", filter=is_space, limit=-1
        ):
            yield instances.as_ids()

    def _iterate_over_edges(self, ids: SequenceNotStr[str]) -> Iterable[list[EdgeId]]:
        is_space: filters.Filter
        if len(ids) == 1:
            is_space = filters.Equals(["edge", "space"], ids[0])
        else:
            is_space = filters.In(["edge", "space"], list(ids))
        for instances in self.client.data_modeling.instances(
            chunk_size=1000, instance_type="edge", limit=-1, filter=is_space
        ):
            yield instances.as_ids()


class ContainerLoader(
    ResourceContainerLoader[ContainerId, ContainerApply, Container, ContainerApplyList, ContainerList]
):
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

    def load_resource(
        self, filepath: Path, ToolGlobals: CDFToolConfig, skip_validation: bool
    ) -> ContainerApply | ContainerApplyList:
        loaded = super().load_resource(filepath, ToolGlobals, skip_validation)
        if not skip_validation:
            items = loaded if isinstance(loaded, ContainerApplyList) else [loaded]
            ToolGlobals.verify_spaces(list({item.space for item in items}))
        return loaded

    def create(self, items: Sequence[ContainerApply]) -> ContainerList:
        return self.client.data_modeling.containers.apply(items)

    def update(self, items: Sequence[ContainerApply]) -> ContainerList:
        return self.create(items)

    def delete(self, ids: SequenceNotStr[ContainerId]) -> int:
        deleted = self.client.data_modeling.containers.delete(cast(Sequence, ids))
        return len(deleted)

    def count(self, ids: SequenceNotStr[ContainerId]) -> int:
        # Bug in spec of aggregate requiring view_id to be passed in, so we cannot use it.
        # When this bug is fixed, it will be much faster to use aggregate.
        return sum(len(batch) for batch in self._iterate_over_nodes(ids)) + sum(
            len(batch) for batch in self._iterate_over_edges(ids)
        )

    def drop_data(self, ids: SequenceNotStr[ContainerId]) -> int:
        nr_of_deleted = 0
        for node_ids in self._iterate_over_nodes(ids):
            self.client.data_modeling.instances.delete(nodes=node_ids)
            nr_of_deleted += len(node_ids)
        for edge_ids in self._iterate_over_edges(ids):
            self.client.data_modeling.instances.delete(edges=edge_ids)
            nr_of_deleted += len(edge_ids)
        return nr_of_deleted

    def _iterate_over_nodes(self, ids: SequenceNotStr[ContainerId]) -> Iterable[list[NodeId]]:
        is_container = filters.HasData(containers=list(ids))
        for instances in self.client.data_modeling.instances(
            chunk_size=1000, instance_type="node", filter=is_container, limit=-1
        ):
            yield instances.as_ids()

    def _iterate_over_edges(self, ids: SequenceNotStr[ContainerId]) -> Iterable[list[EdgeId]]:
        is_container = filters.HasData(containers=list(ids))
        for instances in self.client.data_modeling.instances(
            chunk_size=1000, instance_type="edge", limit=-1, filter=is_container
        ):
            yield instances.as_ids()


class ViewLoader(ResourceLoader[ViewId, ViewApply, View, ViewApplyList, ViewList]):
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

    def load_resource(
        self, filepath: Path, ToolGlobals: CDFToolConfig, skip_validation: bool
    ) -> ViewApply | ViewApplyList:
        loaded = super().load_resource(filepath, ToolGlobals, skip_validation)
        if not skip_validation:
            items = loaded if isinstance(loaded, ViewApplyList) else [loaded]
            ToolGlobals.verify_spaces(list({item.space for item in items}))
        return loaded

    def create(self, items: Sequence[ViewApply]) -> ViewList:
        return self.client.data_modeling.views.apply(items)

    def update(self, items: Sequence[ViewApply]) -> ViewList:
        return self.create(items)


@final
class DataModelLoader(ResourceLoader[DataModelId, DataModelApply, DataModel, DataModelApplyList, DataModelList]):
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

    def load_resource(
        self, filepath: Path, ToolGlobals: CDFToolConfig, skip_validation: bool
    ) -> DataModelApply | DataModelApplyList:
        loaded = super().load_resource(filepath, ToolGlobals, skip_validation)
        if not skip_validation:
            items = loaded if isinstance(loaded, DataModelApplyList) else [loaded]
            ToolGlobals.verify_spaces(list({item.space for item in items}))
        return loaded

    def create(self, items: DataModelApplyList) -> DataModelList:
        return self.client.data_modeling.data_models.apply(items)

    def update(self, items: DataModelApplyList) -> DataModelList:
        return self.create(items)


@final
class NodeLoader(ResourceLoader[NodeId, NodeApply, Node, LoadableNodes, NodeList]):
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

    @classmethod
    def create_empty_of(cls, items: LoadableNodes) -> LoadableNodes:
        return cls.list_write_cls.create_empty_from(items)

    def load_resource(self, filepath: Path, ToolGlobals: CDFToolConfig, skip_validation: bool) -> LoadableNodes:
        raw = load_yaml_inject_variables(filepath, ToolGlobals.environment_variables())
        if isinstance(raw, dict):
            loaded = LoadableNodes._load(raw, cognite_client=self.client)
        else:
            raise ValueError(f"Unexpected node yaml file format {filepath.name}")
        if not skip_validation:
            ToolGlobals.verify_spaces(list({item.space for item in loaded}))
        return loaded

    def create(self, items: LoadableNodes) -> NodeApplyResultList:
        if not isinstance(items, LoadableNodes):
            raise ValueError("Unexpected node format file format")
        item = items
        result = self.client.data_modeling.instances.apply(
            nodes=item.nodes,
            auto_create_direct_relations=item.auto_create_direct_relations,
            skip_on_version_conflict=item.skip_on_version_conflict,
            replace=item.replace,
        )
        return result.nodes

    def update(self, items: LoadableNodes) -> NodeApplyResultList:
        return self.create(items)

    def delete(self, ids: SequenceNotStr[NodeId]) -> int:
        deleted = self.client.data_modeling.instances.delete(nodes=cast(Sequence, ids))
        return len(deleted.nodes)


@final
class EdgeLoader(ResourceLoader[EdgeId, EdgeApply, Edge, LoadableEdges, EdgeList]):
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

    @classmethod
    def create_empty_of(cls, items: LoadableEdges) -> LoadableEdges:
        return cls.list_write_cls.create_empty_from(items)

    def load_resource(self, filepath: Path, ToolGlobals: CDFToolConfig, skip_validation: bool) -> LoadableEdges:
        raw = load_yaml_inject_variables(filepath, ToolGlobals.environment_variables())
        if isinstance(raw, dict):
            loaded = LoadableEdges._load(raw, cognite_client=self.client)
        else:
            raise ValueError(f"Unexpected edge yaml file format {filepath.name}")
        if not skip_validation:
            ToolGlobals.verify_spaces(list({item.space for item in loaded}))
        return loaded

    def create(self, items: LoadableEdges) -> EdgeApplyResultList:
        if not isinstance(items, LoadableEdges):
            raise ValueError("Unexpected edge format file format")
        item = items
        result = self.client.data_modeling.instances.apply(
            edges=item.edges,
            auto_create_start_nodes=item.auto_create_start_nodes,
            auto_create_end_nodes=item.auto_create_end_nodes,
            skip_on_version_conflict=item.skip_on_version_conflict,
            replace=item.replace,
        )
        return result.edges

    def update(self, items: LoadableEdges) -> EdgeApplyResultList:
        return self.create(items)

    def delete(self, ids: SequenceNotStr[EdgeId]) -> int:
        deleted = self.client.data_modeling.instances.delete(edges=cast(Sequence, ids))
        return len(deleted.edges)
