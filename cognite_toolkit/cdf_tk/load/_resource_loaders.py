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
import json
import re
from collections.abc import Iterable, Sequence
from numbers import Number
from pathlib import Path
from time import sleep
from typing import Any, Literal, cast, final
from zipfile import ZipFile

import yaml
from cognite.client import CogniteClient
from cognite.client.credentials import OAuthClientCredentials
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
    Function,
    FunctionList,
    FunctionSchedule,
    FunctionSchedulesList,
    FunctionScheduleWrite,
    FunctionScheduleWriteList,
    FunctionWrite,
    FunctionWriteList,
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
    FunctionsAcl,
    GroupsAcl,
    RawAcl,
    SessionsAcl,
    TimeSeriesAcl,
    TransformationsAcl,
)
from cognite.client.data_classes.data_modeling import (
    Container,
    ContainerApply,
    ContainerApplyList,
    ContainerList,
    ContainerProperty,
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

from cognite_toolkit.cdf_tk.utils import (
    CDFToolConfig,
    calculate_directory_hash,
    get_oneshot_session,
    load_yaml_inject_variables,
)

from ._base_loaders import ResourceContainerLoader, ResourceLoader
from .data_classes import LoadableEdges, LoadableNodes, RawDatabaseTable, RawTableList

_MIN_TIMESTAMP_MS = -2208988800000  # 1900-01-01 00:00:00.000
_MAX_TIMESTAMP_MS = 4102444799999  # 2099-12-31 23:59:59.999


@final
class AuthLoader(ResourceLoader[str, GroupWrite, Group, GroupWriteList, GroupList]):
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
    resource_scope_names = frozenset({scope._scope_name for scope in resource_scopes})  # type: ignore[attr-defined]

    def __init__(
        self,
        client: CogniteClient,
        target_scopes: Literal[
            "all",
            "all_scoped_only",
            "resource_scoped_only",
        ] = "all",
    ):
        super().__init__(client)
        self.target_scopes = target_scopes

    @property
    def display_name(self) -> str:
        return f"{self.api_name}({self.target_scopes.removesuffix('_only')})"

    @classmethod
    def create_loader(
        cls,
        ToolGlobals: CDFToolConfig,
        target_scopes: Literal[
            "all",
            "all_scoped_only",
            "resource_scoped_only",
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

    @staticmethod
    def _substitute_scope_ids(group: dict, ToolGlobals: CDFToolConfig, skip_validation: bool) -> dict:
        for capability in group.get("capabilities", []):
            for acl, values in capability.items():
                scope = values.get("scope", {})

                for scope_name, verify_method in [
                    ("datasetScope", ToolGlobals.verify_dataset),
                    (
                        "idScope",
                        (
                            ToolGlobals.verify_extraction_pipeline
                            if acl == "extractionPipelinesAcl"
                            else ToolGlobals.verify_dataset
                        ),
                    ),
                    ("extractionPipelineScope", ToolGlobals.verify_extraction_pipeline),
                ]:
                    if ids := scope.get(scope_name, {}).get("ids", []):
                        values["scope"][scope_name]["ids"] = [
                            verify_method(ext_id, skip_validation) if isinstance(ext_id, str) else ext_id
                            for ext_id in ids
                        ]
        return group

    def load_resource(
        self, filepath: Path, ToolGlobals: CDFToolConfig, skip_validation: bool
    ) -> GroupWrite | GroupWriteList | None:
        raw = load_yaml_inject_variables(filepath, ToolGlobals.environment_variables())

        group_write_list = GroupWriteList([])

        if isinstance(raw, dict):
            raw = [raw]

        for group in raw:

            is_resource_scoped = any(
                any(scope_name in capability.get(acl, {}).get("scope", {}) for scope_name in self.resource_scope_names)
                for capability in group.get("capabilities", [])
                for acl in capability
            )

            if self.target_scopes == "all_scoped_only" and is_resource_scoped:
                continue

            if self.target_scopes == "resource_scoped_only" and not is_resource_scoped:
                continue

            substituted = self._substitute_scope_ids(group, ToolGlobals, skip_validation)
            group_write_list.append(GroupWrite.load(substituted))

        if len(group_write_list) == 0:
            return None
        if len(group_write_list) == 1:
            return group_write_list[0]
        return group_write_list

    def create(self, items: Sequence[GroupWrite]) -> GroupList:
        if len(items) == 0:
            return GroupList([])
        # We MUST retrieve all the old groups BEFORE we add the new, if not the new will be deleted
        old_groups = self.client.iam.groups.list(all=True)
        old_group_by_names = {g.name: g for g in old_groups.as_write()}
        changed = []
        for item in items:
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
            if data_set.get("writeProtected") is None:
                # Todo: Setting missing default value, bug in SDK.
                data_set["writeProtected"] = False
            if data_set.get("metadata") is None:
                # Todo: Wrongly set to empty dict, bug in SDK.
                data_set["metadata"] = {}

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
        return self.client.data_sets.retrieve_multiple(
            external_ids=cast(SequenceNotStr[str], ids), ignore_unknown_ids=True
        )

    def delete(self, ids: SequenceNotStr[str]) -> int:
        raise NotImplementedError("CDF does not support deleting data sets.")


@final
class FunctionLoader(ResourceLoader[str, FunctionWrite, Function, FunctionWriteList, FunctionList]):
    support_drop = True
    api_name = "functions"
    folder_name = "functions"
    filename_pattern = (
        r"^(?:(?!schedule).)*$"  # Matches all yaml files except file names who's stem contain *.schedule.
    )
    resource_cls = Function
    resource_write_cls = FunctionWrite
    list_cls = FunctionList
    list_write_cls = FunctionWriteList
    dependencies = frozenset({DataSetsLoader})

    def __init__(self, client: CogniteClient):
        super().__init__(client)

    @classmethod
    def get_required_capability(cls, ToolGlobals: CDFToolConfig) -> list[Capability]:
        return [
            FunctionsAcl([FunctionsAcl.Action.Read, FunctionsAcl.Action.Write], FunctionsAcl.Scope.All()),
            FilesAcl(
                [FilesAcl.Action.Read, FilesAcl.Action.Write], FilesAcl.Scope.All()
            ),  # Needed for uploading function artifacts
        ]

    @classmethod
    def get_id(cls, item: Function | FunctionWrite) -> str:
        if item.external_id is None:
            raise ValueError("Function must have external_id set.")
        return item.external_id

    def load_resource(
        self, filepath: Path, ToolGlobals: CDFToolConfig, skip_validation: bool
    ) -> FunctionWrite | FunctionWriteList | None:
        functions = load_yaml_inject_variables(
            filepath, ToolGlobals.environment_variables(), required_return_type="list"
        )
        for func in functions:
            if self.extra_configs.get(func["externalId"]) is None:
                self.extra_configs[func["externalId"]] = {}
            if func.get("externalDataSetId") is not None:
                self.extra_configs[func["externalId"]]["dataSetId"] = ToolGlobals.verify_dataset(
                    func.get("externalDataSetId", ""), skip_validation=skip_validation
                )
        return FunctionWriteList.load(functions)

    def _is_equal_custom(self, local: FunctionWrite, cdf_resource: Function) -> bool:
        if self.build_path is None:
            raise ValueError("build_path must be set to compare functions as function code must be compared.")
        # If the function failed, we want to always trigger a redeploy.
        if cdf_resource.status == "Failed":
            return False
        function_rootdir = Path(self.build_path / f"{local.external_id}")
        if local.metadata is None:
            local.metadata = {}
        local.metadata["cdf-toolkit-function-hash"] = calculate_directory_hash(function_rootdir)

        # Is changed as part of deploy to the API
        local.file_id = cdf_resource.file_id
        cdf_resource.secrets = local.secrets
        # Set empty values for local
        attrs = [
            attr for attr in dir(cdf_resource) if not callable(getattr(cdf_resource, attr)) and not attr.startswith("_")
        ]
        # Remove server-side attributes
        attrs.remove("created_time")
        attrs.remove("error")
        attrs.remove("id")
        attrs.remove("runtime_version")
        attrs.remove("status")
        # Set empty values for local that have default values server-side
        for attribute in attrs:
            if getattr(local, attribute) is None:
                setattr(local, attribute, getattr(cdf_resource, attribute))
        return local.dump() == cdf_resource.as_write().dump()

    def retrieve(self, ids: SequenceNotStr[str]) -> FunctionList:
        status = self.client.functions.status()
        if status.status != "activated":
            if status.status == "requested":
                print(
                    "  [bold yellow]WARNING:[/] Function service activation is in progress, cannot retrieve functions."
                )
                return FunctionList([])
            else:
                print(
                    "  [bold yellow]WARNING:[/] Function service has not been activated, activating now, this may take up to 2 hours..."
                )
                self.client.functions.activate()
                return FunctionList([])
        ret = self.client.functions.retrieve_multiple(
            external_ids=cast(SequenceNotStr[str], ids), ignore_unknown_ids=True
        )
        if ret is None:
            return FunctionList([])
        if isinstance(ret, Function):
            return FunctionList([ret])
        else:
            return ret

    def update(self, items: FunctionWriteList) -> FunctionList:
        self.delete([item.external_id for item in items])
        return self.create(items)

    def _zip_and_upload_folder(
        self,
        root_dir: Path,
        external_id: str,
        data_set_id: int | None = None,
    ) -> int:
        zip_path = Path(root_dir.parent / f"{external_id}.zip")
        root_length = len(root_dir.parts)
        with ZipFile(zip_path, "w") as zipfile:
            for file in root_dir.rglob("*"):
                if file.is_file():
                    zipfile.write(file, "/".join(file.parts[root_length - 1 : -1]) + f"/{file.name}")
        file_info = self.client.files.upload_bytes(
            zip_path.read_bytes(),
            name=f"{external_id}.zip",
            external_id=external_id,
            overwrite=True,
            data_set_id=data_set_id,
        )
        zip_path.unlink()
        return cast(int, file_info.id)

    def create(self, items: Sequence[FunctionWrite]) -> FunctionList:
        items = list(items)
        created = FunctionList([], cognite_client=self.client)
        status = self.client.functions.status()
        if status.status != "activated":
            if status.status == "requested":
                print("  [bold yellow]WARNING:[/] Function service activation is in progress, skipping functions.")
                return FunctionList([])
            else:
                print(
                    "  [bold yellow]WARNING:[/] Function service is not activated, activating and skipping functions..."
                )
                self.client.functions.activate()
                return FunctionList([])
        if self.build_path is None:
            raise ValueError("build_path must be set to compare functions as function code must be compared.")
        for item in items:
            function_rootdir = Path(self.build_path / (item.external_id or ""))
            if item.metadata is None:
                item.metadata = {}
            item.metadata["cdf-toolkit-function-hash"] = calculate_directory_hash(function_rootdir)
            file_id = self._zip_and_upload_folder(
                root_dir=function_rootdir,
                external_id=item.external_id or item.name,
                data_set_id=self.extra_configs[item.external_id or item.name].get("dataSetId", None),
            )
            created.append(
                self.client.functions.create(
                    name=item.name,
                    external_id=item.external_id or item.name,
                    file_id=file_id,
                    function_path=item.function_path or "./handler.py",
                    description=item.description,
                    owner=item.owner,
                    secrets=item.secrets,
                    env_vars=item.env_vars,
                    cpu=cast(Number, item.cpu),
                    memory=cast(Number, item.memory),
                    runtime=item.runtime,
                    metadata=item.metadata,
                    index_url=item.index_url,
                    extra_index_urls=item.extra_index_urls,
                )
            )
        return created

    def delete(self, ids: SequenceNotStr[str]) -> int:
        self.client.functions.delete(external_id=cast(SequenceNotStr[str], ids))
        return len(ids)


@final
class FunctionScheduleLoader(
    ResourceLoader[str, FunctionScheduleWrite, FunctionSchedule, FunctionScheduleWriteList, FunctionSchedulesList]
):
    api_name = "functions.schedules"
    folder_name = "functions"
    filename_pattern = r"^.*schedule.*$"  # Matches all yaml files who's stem contain *.schedule.
    resource_cls = FunctionSchedule
    resource_write_cls = FunctionScheduleWrite
    list_cls = FunctionSchedulesList
    list_write_cls = FunctionScheduleWriteList
    dependencies = frozenset({FunctionLoader})

    def __init__(self, client: CogniteClient):
        super().__init__(client)

    @classmethod
    def get_required_capability(cls, ToolGlobals: CDFToolConfig) -> list[Capability]:
        return [
            FunctionsAcl([FunctionsAcl.Action.Read, FunctionsAcl.Action.Write], FunctionsAcl.Scope.All()),
            SessionsAcl(
                [SessionsAcl.Action.List, SessionsAcl.Action.Create, SessionsAcl.Action.Delete], SessionsAcl.Scope.All()
            ),
        ]

    @classmethod
    def get_id(cls, item: FunctionScheduleWrite | FunctionSchedule) -> str:
        if item.function_external_id is None or item.cron_expression is None:
            raise ValueError("FunctionSchedule must have functionExternalId and CronExpression set.")
        return f"{item.function_external_id}:{item.cron_expression}"

    def load_resource(
        self, filepath: Path, ToolGlobals: CDFToolConfig, skip_validation: bool
    ) -> FunctionScheduleWrite | FunctionScheduleWriteList | None:
        schedules = load_yaml_inject_variables(
            filepath, ToolGlobals.environment_variables(), required_return_type="list"
        )
        for sched in schedules:
            ext_id = f"{sched['functionExternalId']}:{sched['cronExpression']}"
            if self.extra_configs.get(ext_id) is None:
                self.extra_configs[ext_id] = {}
            self.extra_configs[ext_id]["authentication"] = sched.pop("authentication", {})
        return FunctionScheduleWriteList.load(schedules)

    def _is_equal_custom(self, local: FunctionScheduleWrite, cdf_resource: FunctionSchedule) -> bool:
        remote_dump = cdf_resource.as_write().dump()
        del remote_dump["functionId"]
        return remote_dump == local.dump()

    def _resolve_functions_ext_id(self, items: FunctionScheduleWriteList) -> FunctionScheduleWriteList:
        functions = FunctionLoader(self.client).retrieve(list(set([item.function_external_id for item in items])))
        for item in items:
            for func in functions:
                if func.external_id == item.function_external_id:
                    item.function_id = func.id
        return items

    def retrieve(self, ids: SequenceNotStr[str]) -> FunctionSchedulesList:
        functions = FunctionLoader(self.client).retrieve(list(set([id.split(":")[0] for id in ids])))
        schedules = FunctionSchedulesList([])
        for func in functions:
            ret = self.client.functions.schedules.list(function_id=func.id, limit=-1)
            for schedule in ret:
                schedule.function_external_id = func.external_id
            schedules.extend(ret)
        return schedules

    def create(self, items: FunctionScheduleWriteList) -> FunctionSchedulesList:
        items = self._resolve_functions_ext_id(items)
        (_, bearer) = self.client.config.credentials.authorization_header()
        created = FunctionSchedulesList([])
        for item in items:
            if (
                authentication := self.extra_configs.get(f"{item.function_external_id}:{item.cron_expression}", {}).get(
                    "authentication"
                )
            ) is not None and len(authentication) > 0:
                new_tool_config = CDFToolConfig()
                old_credentials = cast(OAuthClientCredentials, new_tool_config.client.config.credentials)
                new_tool_config.client.config.credentials = OAuthClientCredentials(
                    client_id=authentication.get("clientId"),
                    client_secret=authentication.get("clientSecret"),
                    scopes=old_credentials.scopes,
                    token_url=old_credentials.token_url,
                )
                session = get_oneshot_session(new_tool_config.client)
            else:
                session = get_oneshot_session(self.client)
            nonce = session.nonce if session is not None else ""
            try:
                ret = self.client.post(
                    url=f"/api/v1/projects/{self.client.config.project}/functions/schedules",
                    json={
                        "items": [
                            {
                                "name": item.name,
                                "description": item.description,
                                "cronExpression": item.cron_expression,
                                "functionId": item.function_id,
                                "data": item.data,
                                "nonce": nonce,
                            }
                        ],
                    },
                    headers={"Authorization": bearer},
                )
            except CogniteAPIError as e:
                if e.code == 400 and "Failed to bind session" in e.message:
                    print("  [bold yellow]WARNING:[/] Failed to bind session because function is not ready.")
                continue
            if ret.status_code == 201:
                created.append(FunctionSchedule.load(ret.json()["items"][0]))
        return created

    def delete(self, ids: SequenceNotStr[str]) -> int:
        schedules = self.retrieve(ids)
        count = 0
        for schedule in schedules:
            self.client.functions.schedules.delete(id=schedule.id)
            count += 1
        return count


@final
class RawDatabaseLoader(
    ResourceContainerLoader[RawDatabaseTable, RawDatabaseTable, RawDatabaseTable, RawTableList, RawTableList]
):
    item_name = "raw tables"
    api_name = "raw.databases"
    folder_name = "raw"
    resource_cls = RawDatabaseTable
    resource_write_cls = RawDatabaseTable
    list_cls = RawTableList
    list_write_cls = RawTableList
    identifier_key = "table_name"

    def __init__(self, client: CogniteClient):
        super().__init__(client)
        self._loaded_db_names: set[str] = set()

    @classmethod
    def get_required_capability(cls, ToolGlobals: CDFToolConfig) -> Capability:
        return RawAcl([RawAcl.Action.Read, RawAcl.Action.Write], RawAcl.Scope.All())

    @classmethod
    def get_id(cls, item: RawDatabaseTable) -> RawDatabaseTable:
        return item

    def load_resource(
        self, filepath: Path, ToolGlobals: CDFToolConfig, skip_validation: bool
    ) -> RawDatabaseTable | RawTableList | None:
        resource = super().load_resource(filepath, ToolGlobals, skip_validation)
        if resource is None:
            return None
        dbs = resource if isinstance(resource, RawTableList) else RawTableList([resource])
        # This loader is only used for the raw databases, so we need to remove the table names
        # such that the comparison will work correctly.
        db_names = set(dbs.as_db_names()) - self._loaded_db_names
        if not db_names:
            # All databases already loaded
            return None
        self._loaded_db_names.update(db_names)
        return RawTableList([RawDatabaseTable(db_name=db_name) for db_name in db_names])

    def create(self, items: RawTableList) -> RawTableList:
        database_list = self.client.raw.databases.create(items.as_db_names())
        return RawTableList([RawDatabaseTable(db_name=db.name) for db in database_list])

    def retrieve(self, ids: SequenceNotStr[RawDatabaseTable]) -> RawTableList:
        database_list = self.client.raw.databases.list(limit=-1)
        target_dbs = {db.db_name for db in ids}
        return RawTableList([RawDatabaseTable(db_name=db.name) for db in database_list if db.name in target_dbs])

    def update(self, items: Sequence[RawDatabaseTable]) -> RawTableList:
        raise NotImplementedError("Raw tables do not support update.")

    def delete(self, ids: SequenceNotStr[RawDatabaseTable]) -> int:
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

    def count(self, ids: SequenceNotStr[RawDatabaseTable]) -> int:
        nr_of_tables = 0
        for db_name, raw_tables in itertools.groupby(sorted(ids), key=lambda x: x.db_name):
            try:
                tables = self.client.raw.tables.list(db_name=db_name, limit=-1)
            except CogniteAPIError as e:
                if db_name in {item.get("name") for item in e.missing or []}:
                    continue
                raise e
            nr_of_tables += len(tables.data)
        return nr_of_tables

    def drop_data(self, ids: SequenceNotStr[RawDatabaseTable]) -> int:
        nr_of_tables = 0
        for db_name, raw_tables in itertools.groupby(sorted(ids), key=lambda x: x.db_name):
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
class RawTableLoader(
    ResourceContainerLoader[RawDatabaseTable, RawDatabaseTable, RawDatabaseTable, RawTableList, RawTableList]
):
    item_name = "raw rows"
    api_name = "raw.tables"
    folder_name = "raw"
    resource_cls = RawDatabaseTable
    resource_write_cls = RawDatabaseTable
    list_cls = RawTableList
    list_write_cls = RawTableList
    identifier_key = "table_name"
    dependencies = frozenset({RawDatabaseLoader})

    def __init__(self, client: CogniteClient):
        super().__init__(client)
        self._printed_warning = False

    @classmethod
    def get_required_capability(cls, ToolGlobals: CDFToolConfig) -> Capability:
        return RawAcl([RawAcl.Action.Read, RawAcl.Action.Write], RawAcl.Scope.All())

    @classmethod
    def get_id(cls, item: RawDatabaseTable) -> RawDatabaseTable:
        return item

    def load_resource(self, filepath: Path, ToolGlobals: CDFToolConfig, skip_validation: bool) -> RawTableList | None:
        resource = super().load_resource(filepath, ToolGlobals, skip_validation)
        if resource is None:
            return None
        raw_tables = resource if isinstance(resource, RawTableList) else RawTableList([resource])
        raw_tables = RawTableList([table for table in raw_tables if table.table_name])
        if not raw_tables:
            # These are configs for Raw Databases only
            return None
        return raw_tables

    def create(self, items: RawTableList) -> RawTableList:
        created = RawTableList([])
        for db_name, raw_tables in itertools.groupby(sorted(items), key=lambda x: x.db_name):
            tables = [table.table_name for table in raw_tables]
            new_tables = self.client.raw.tables.create(db_name=db_name, name=tables)
            created.extend([RawDatabaseTable(db_name=db_name, table_name=table.name) for table in new_tables])
        return created

    def retrieve(self, ids: SequenceNotStr[RawDatabaseTable]) -> RawTableList:
        retrieved = RawTableList([])
        for db_name, raw_tables in itertools.groupby(sorted(ids), key=lambda x: x.db_name):
            expected_tables = {table.table_name for table in raw_tables}
            try:
                tables = self.client.raw.tables.list(db_name=db_name, limit=-1)
            except CogniteAPIError as e:
                if db_name in {item.get("name") for item in e.missing or []}:
                    continue
                raise e
            retrieved.extend(
                [
                    RawDatabaseTable(db_name=db_name, table_name=table.name)
                    for table in tables
                    if table.name in expected_tables
                ]
            )
        return retrieved

    def update(self, items: Sequence[RawDatabaseTable]) -> RawTableList:
        raise NotImplementedError("Raw tables do not support update.")

    def delete(self, ids: SequenceNotStr[RawDatabaseTable]) -> int:
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

    def count(self, ids: SequenceNotStr[RawDatabaseTable]) -> int:
        if not self._printed_warning:
            print("  [bold green]INFO:[/] Raw rows do not support count (there is no aggregation method).")
            self._printed_warning = True
        return -1

    def drop_data(self, ids: SequenceNotStr[RawDatabaseTable]) -> int:
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


@final
class TimeSeriesLoader(ResourceContainerLoader[str, TimeSeriesWrite, TimeSeries, TimeSeriesWriteList, TimeSeriesList]):
    item_name = "datapoints"
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
            (
                TimeSeriesAcl.Scope.DataSet([ToolGlobals.data_set_id])
                if ToolGlobals.data_set_id
                else TimeSeriesAcl.Scope.All()
            ),
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
                resource["dataSetId"] = ToolGlobals.verify_dataset(ds_external_id, skip_validation)
            if resource.get("securityCategories") is None:
                # Bug in SDK, the read version sets security categories to an empty list.
                resource["securityCategories"] = []
        return TimeSeriesWriteList.load(resources)

    def retrieve(self, ids: SequenceNotStr[str]) -> TimeSeriesList:
        return self.client.time_series.retrieve_multiple(
            external_ids=cast(SequenceNotStr[str], ids), ignore_unknown_ids=True
        )

    def delete(self, ids: SequenceNotStr[str]) -> int:
        existing = self.retrieve(ids).as_external_ids()
        if existing:
            self.client.time_series.delete(external_id=existing, ignore_unknown_ids=True)
        return len(existing)

    def count(self, ids: str | dict[str, Any] | SequenceNotStr[str | dict[str, Any]] | None) -> int:
        datapoints = cast(
            DatapointsList,
            self.client.time_series.data.retrieve(
                external_id=cast(SequenceNotStr[str], ids),
                start=_MIN_TIMESTAMP_MS,
                end=_MAX_TIMESTAMP_MS + 1,
                aggregates="count",
                granularity="1000d",
                ignore_unknown_ids=True,
            ),
        )
        return sum(sum(data.count or []) for data in datapoints)

    def drop_data(self, ids: SequenceNotStr[str] | None) -> int:
        count = self.count(ids)
        existing = self.client.time_series.retrieve_multiple(
            external_ids=cast(SequenceNotStr[str], ids), ignore_unknown_ids=True
        ).as_external_ids()
        for external_id in existing:
            self.client.time_series.data.delete_range(
                external_id=external_id, start=_MIN_TIMESTAMP_MS, end=_MAX_TIMESTAMP_MS + 1
            )
        return count


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
    dependencies = frozenset({DataSetsLoader, RawDatabaseLoader})

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

    def _is_equal_custom(self, local: TransformationWrite, cdf_resource: Transformation) -> bool:
        local_dumped = local.dump()
        local_dumped.pop("destinationOidcCredentials", None)
        local_dumped.pop("sourceOidcCredentials", None)

        return local_dumped == cdf_resource.as_write().dump()

    def load_resource(self, filepath: Path, ToolGlobals: CDFToolConfig, skip_validation: bool) -> TransformationWrite:
        raw = load_yaml_inject_variables(filepath, ToolGlobals.environment_variables(), required_return_type="dict")
        # The `authentication` key is custom for this template:

        source_oidc_credentials = raw.get("authentication", {}).get("read") or raw.get("authentication") or None
        destination_oidc_credentials = raw.get("authentication", {}).get("write") or raw.get("authentication") or None
        if raw.get("dataSetExternalId") is not None:
            ds_external_id = raw.pop("dataSetExternalId")
            raw["dataSetId"] = ToolGlobals.verify_dataset(ds_external_id, skip_validation)
        if raw.get("conflictMode") is None:
            # Todo; Bug SDK missing default value
            raw["conflictMode"] = "upsert"

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
        existing = self.retrieve(ids).as_external_ids()
        if existing:
            self.client.transformations.delete(external_id=existing, ignore_unknown_ids=True)
        return len(existing)


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

    def delete(self, ids: str | SequenceNotStr[str] | None) -> int:
        try:
            self.client.transformations.schedules.delete(
                external_id=cast(SequenceNotStr[str], ids), ignore_unknown_ids=False
            )
            return len(cast(SequenceNotStr[str], ids))
        except CogniteNotFoundError as e:
            return len(cast(SequenceNotStr[str], ids)) - len(e.not_found)


@final
class ExtractionPipelineLoader(
    ResourceLoader[
        str, ExtractionPipelineWrite, ExtractionPipeline, ExtractionPipelineWriteList, ExtractionPipelineList
    ]
):
    api_name = "extraction_pipelines"
    folder_name = "extraction_pipelines"
    filename_pattern = r"^(?:(?!\.config).)*$"  # Matches all yaml files except file names who's stem contain *.config.
    resource_cls = ExtractionPipeline
    resource_write_cls = ExtractionPipelineWrite
    list_cls = ExtractionPipelineList
    list_write_cls = ExtractionPipelineWriteList
    dependencies = frozenset({DataSetsLoader, RawDatabaseLoader, RawTableLoader})

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
            resource["dataSetId"] = ToolGlobals.verify_dataset(ds_external_id, skip_validation)
        if resource.get("createdBy") is None:
            # Todo; Bug SDK missing default value (this will be set on the server-side if missing)
            resource["createdBy"] = "unknown"

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
        except CogniteNotFoundError as e:
            not_existing = {external_id for dup in e.not_found if (external_id := dup.get("externalId", None))}
            if id_list := [id_ for id_ in id_list if id_ not in not_existing]:
                self.client.extraction_pipelines.delete(external_id=id_list)
        except CogniteAPIError as e:
            if e.code == 403 and "not found" in e.message and "extraction pipeline" in e.message.lower():
                return 0
        return len(id_list)


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

    # configs cannot be updated, instead new revision is created
    def update(self, items: Sequence[ExtractionPipelineConfigWrite]) -> ExtractionPipelineConfigList:
        return self.create(items)

    def retrieve(self, ids: SequenceNotStr[str]) -> ExtractionPipelineConfigList:
        retrieved = ExtractionPipelineConfigList([])
        for external_id in ids:
            try:
                config_retrieved = self.client.extraction_pipelines.config.retrieve(external_id=external_id)
            except CogniteAPIError as e:
                if (
                    e.code == 404
                    and e.message.startswith("There is no config stored for the extraction pipeline with external id")
                    or e.message.startswith("Extraction pipeline not found")
                ):
                    continue
                raise e
            if config_retrieved:
                retrieved.append(config_retrieved)
        return retrieved

    def delete(self, ids: SequenceNotStr[str]) -> int:
        count = 0
        for id_ in ids:
            try:
                result = self.client.extraction_pipelines.config.list(external_id=id_)
            except CogniteAPIError as e:
                if e.code == 403 and "not found" in e.message and "extraction pipeline" in e.message.lower():
                    continue
            else:
                count += len(result)
        return count


@final
class FileMetadataLoader(
    ResourceContainerLoader[str, FileMetadataWrite, FileMetadata, FileMetadataWriteList, FileMetadataList]
):
    item_name = "file contents"
    api_name = "files"
    folder_name = "files"
    resource_cls = FileMetadata
    resource_write_cls = FileMetadataWrite
    list_cls = FileMetadataList
    list_write_cls = FileMetadataWriteList
    dependencies = frozenset({DataSetsLoader})

    @property
    def display_name(self) -> str:
        return "file_metadata"

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
                resource["dataSetId"] = ToolGlobals.verify_dataset(ds_external_id, skip_validation)
            files_metadata = FileMetadataWriteList([FileMetadataWrite.load(resource)])
        except Exception:
            files_metadata = FileMetadataWriteList.load(
                load_yaml_inject_variables(filepath, ToolGlobals.environment_variables(), required_return_type="list")
            )

        # If we have a file with exact one file config, check to see if this is a pattern to expand
        if len(files_metadata) == 1 and ("$FILENAME" in (files_metadata[0].external_id or "")):
            # It is, so replace this file with all files in this folder using the same data
            print(f"  [bold green]INFO:[/] File pattern detected in {filepath.name}, expanding to all files in folder.")
            file_data = files_metadata.data[0]
            ext_id_pattern = file_data.external_id
            files_metadata = FileMetadataWriteList([], cognite_client=self.client)
            for file in filepath.parent.glob("*"):
                if file.suffix in [".yaml", ".yml"]:
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
                meta.data_set_id = ToolGlobals.verify_dataset(meta.data_set_id, skip_validation)
        return files_metadata

    def create(self, items: FileMetadataWriteList) -> FileMetadataList:
        created = FileMetadataList([])
        for meta in items:
            try:
                created.append(self.client.files.create(meta))
            except CogniteAPIError as e:
                if e.code == 409:
                    print(f"  [bold yellow]WARNING:[/] File {meta.external_id} already exists, skipping upload.")
        return created

    def delete(self, ids: str | SequenceNotStr[str] | None) -> int:
        self.client.files.delete(external_id=cast(SequenceNotStr[str], ids))
        return len(cast(SequenceNotStr[str], ids))

    def count(self, ids: SequenceNotStr[str]) -> int:
        return sum(
            1
            for meta in self.client.files.retrieve_multiple(external_ids=list(ids), ignore_unknown_ids=True)
            if meta.uploaded
        )

    def drop_data(self, ids: SequenceNotStr[str]) -> int:
        existing = self.client.files.retrieve_multiple(external_ids=list(ids), ignore_unknown_ids=True)
        # File and FileMetadata is tightly coupled, so we need to delete the metadata and recreate it
        # without the source set to delete the file.
        deleted_files = self.delete(existing.as_external_ids())
        self.create(existing.as_write())
        return deleted_files


@final
class SpaceLoader(ResourceContainerLoader[str, SpaceApply, Space, SpaceApplyList, SpaceList]):
    item_name = "nodes and edges"
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
        existing = self.client.data_modeling.spaces.retrieve(ids)
        is_global = {space.space for space in existing if space.is_global}
        if is_global:
            print(
                f"  [bold yellow]WARNING:[/] Spaces {list(is_global)} are global and cannot be deleted, skipping delete, for these."
            )
        to_delete = [space for space in ids if space not in is_global]
        deleted = self.client.data_modeling.spaces.delete(to_delete)
        return len(deleted)

    def count(self, ids: SequenceNotStr[str]) -> int:
        # Bug in spec of aggregate requiring view_id to be passed in, so we cannot use it.
        # When this bug is fixed, it will be much faster to use aggregate.
        existing = self.client.data_modeling.spaces.retrieve(ids)

        return sum(len(batch) for batch in self._iterate_over_nodes(existing)) + sum(
            len(batch) for batch in self._iterate_over_edges(existing)
        )

    def drop_data(self, ids: SequenceNotStr[str]) -> int:
        existing = self.client.data_modeling.spaces.retrieve(ids)
        if not existing:
            return 0
        print(f"[bold]Deleting existing data in spaces {ids}...[/]")
        nr_of_deleted = 0
        for edge_ids in self._iterate_over_edges(existing):
            self.client.data_modeling.instances.delete(edges=edge_ids)
            nr_of_deleted += len(edge_ids)
        for node_ids in self._iterate_over_nodes(existing):
            self.client.data_modeling.instances.delete(nodes=node_ids)
            nr_of_deleted += len(node_ids)
        return nr_of_deleted

    def _iterate_over_nodes(self, spaces: SpaceList) -> Iterable[list[NodeId]]:
        is_space: filters.Filter
        if len(spaces) == 0:
            return
        elif len(spaces) == 1:
            is_space = filters.Equals(["node", "space"], spaces[0].as_id())
        else:
            is_space = filters.In(["node", "space"], spaces.as_ids())
        for instances in self.client.data_modeling.instances(
            chunk_size=1000, instance_type="node", filter=is_space, limit=-1
        ):
            yield instances.as_ids()

    def _iterate_over_edges(self, spaces: SpaceList) -> Iterable[list[EdgeId]]:
        is_space: filters.Filter
        if len(spaces) == 0:
            return
        elif len(spaces) == 1:
            is_space = filters.Equals(["edge", "space"], spaces[0].as_id())
        else:
            is_space = filters.In(["edge", "space"], spaces.as_ids())
        for instances in self.client.data_modeling.instances(
            chunk_size=1000, instance_type="edge", limit=-1, filter=is_space
        ):
            yield instances.as_ids()


class ContainerLoader(
    ResourceContainerLoader[ContainerId, ContainerApply, Container, ContainerApplyList, ContainerList]
):
    item_name = "nodes and edges"
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
    ) -> ContainerApply | ContainerApplyList | None:
        loaded = super().load_resource(filepath, ToolGlobals, skip_validation)
        if loaded is None:
            return None
        items = loaded if isinstance(loaded, ContainerApplyList) else [loaded]
        if not skip_validation:
            ToolGlobals.verify_spaces(list({item.space for item in items}))
        for item in items:
            # Todo Bug in SDK, not setting defaults on load
            for prop_name in item.properties.keys():
                prop_dumped = item.properties[prop_name].dump()
                if prop_dumped.get("nullable") is None:
                    prop_dumped["nullable"] = False
                if prop_dumped.get("autoIncrement") is None:
                    prop_dumped["autoIncrement"] = False
                item.properties[prop_name] = ContainerProperty.load(prop_dumped)
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
        existing_containers = self.client.data_modeling.containers.retrieve(cast(Sequence, ids))
        return sum(len(batch) for batch in self._iterate_over_nodes(existing_containers)) + sum(
            len(batch) for batch in self._iterate_over_edges(existing_containers)
        )

    def drop_data(self, ids: SequenceNotStr[ContainerId]) -> int:
        nr_of_deleted = 0
        existing_containers = self.client.data_modeling.containers.retrieve(cast(Sequence, ids))
        for node_ids in self._iterate_over_nodes(existing_containers):
            self.client.data_modeling.instances.delete(nodes=node_ids)
            nr_of_deleted += len(node_ids)
        for edge_ids in self._iterate_over_edges(existing_containers):
            self.client.data_modeling.instances.delete(edges=edge_ids)
            nr_of_deleted += len(edge_ids)
        return nr_of_deleted

    def _iterate_over_nodes(self, containers: ContainerList) -> Iterable[list[NodeId]]:
        container_ids = [container.as_id() for container in containers if container.used_for in ["node", "all"]]
        if not container_ids:
            return
        is_container = filters.HasData(containers=container_ids)
        for instances in self.client.data_modeling.instances(
            chunk_size=1000, instance_type="node", filter=is_container, limit=-1
        ):
            yield instances.as_ids()

    def _iterate_over_edges(self, containers: ContainerList) -> Iterable[list[EdgeId]]:
        container_ids = [container.as_id() for container in containers if container.used_for in ["edge", "all"]]
        if not container_ids:
            return
        is_container = filters.HasData(containers=container_ids)
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

    def __init__(self, client: CogniteClient):
        super().__init__(client)
        # Caching to avoid multiple lookups on the same interfaces.
        self._interfaces_by_id: dict[ViewId, View] = {}

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
    ) -> ViewApply | ViewApplyList | None:
        loaded = super().load_resource(filepath, ToolGlobals, skip_validation)
        if not skip_validation:
            items = loaded if isinstance(loaded, ViewApplyList) else [loaded]
            ToolGlobals.verify_spaces(list({item.space for item in items}))
        return loaded

    def _get_parents(self, implements: list[ViewId]) -> list[View]:
        parents = implements
        found = []
        while parents:
            to_lookup = []
            new_parents = []
            for parent in parents:
                if parent in self._interfaces_by_id:
                    found.append(self._interfaces_by_id[parent])
                    new_parents.extend(self._interfaces_by_id[parent].implements or [])
                else:
                    to_lookup.append(parent)

            if to_lookup:
                looked_up = self.client.data_modeling.views.retrieve(to_lookup)
                self._interfaces_by_id.update({view.as_id(): view for view in looked_up})
                found.extend(looked_up)
                for view in looked_up:
                    for grandparent in view.implements or []:
                        new_parents.append(grandparent)

            parents = new_parents
        return found

    def _is_equal_custom(self, local: ViewApply, cdf_resource: View) -> bool:
        local_dumped = local.dump()
        cdf_resource_dumped = cdf_resource.as_write().dump()
        if not cdf_resource.implements:
            return local_dumped == cdf_resource_dumped

        if cdf_resource.properties:
            # All read version of views have all the properties of their parent views.
            # We need to remove these properties to compare with the local view.
            parents = self._get_parents(cdf_resource.implements)
            for parent in parents:
                for prop_name in parent.properties.keys():
                    cdf_resource_dumped["properties"].pop(prop_name, None)

        if not cdf_resource_dumped["properties"]:
            # All properties were removed, so we remove the properties key.
            cdf_resource_dumped.pop("properties", None)
        if "properties" in local_dumped and not local_dumped["properties"]:
            # In case the local properties are set to an empty dict.
            local_dumped.pop("properties", None)

        return local_dumped == cdf_resource_dumped

    def create(self, items: Sequence[ViewApply]) -> ViewList:
        return self.client.data_modeling.views.apply(items)

    def update(self, items: Sequence[ViewApply]) -> ViewList:
        return self.create(items)

    def delete(self, ids: SequenceNotStr[ViewId]) -> int:
        to_delete = list(ids)
        nr_of_deleted = 0
        attempt_count = 5
        for attempt_no in range(attempt_count):
            deleted = self.client.data_modeling.views.delete(to_delete)
            nr_of_deleted += len(deleted)
            existing = self.client.data_modeling.views.retrieve(to_delete).as_ids()
            if not existing:
                return nr_of_deleted
            sleep(2)
            to_delete = existing
        else:
            print(f"  [bold yellow]WARNING:[/] Could not delete views {to_delete} after {attempt_count} attempts.")
        return nr_of_deleted


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
    ) -> DataModelApply | DataModelApplyList | None:
        loaded = super().load_resource(filepath, ToolGlobals, skip_validation)
        if not skip_validation:
            items = loaded if isinstance(loaded, DataModelApplyList) else [loaded]
            ToolGlobals.verify_spaces(list({item.space for item in items}))
        return loaded

    def _is_equal_custom(self, local: DataModelApply, cdf_resource: DataModel) -> bool:
        local_dumped = local.dump()
        cdf_resource_dumped = cdf_resource.as_write().dump()

        # Data models that have the same views, but in different order, are considered equal.
        # We also account for whether views are given as IDs or View objects.
        local_dumped["views"] = sorted(
            (v if isinstance(v, ViewId) else v.as_id()).as_tuple() for v in local.views or []
        )
        cdf_resource_dumped["views"] = sorted(
            (v if isinstance(v, ViewId) else v.as_id()).as_tuple() for v in cdf_resource.views or []
        )

        return local_dumped == cdf_resource_dumped

    def create(self, items: DataModelApplyList) -> DataModelList:
        return self.client.data_modeling.data_models.apply(items)

    def update(self, items: DataModelApplyList) -> DataModelList:
        return self.create(items)


@final
class NodeLoader(ResourceContainerLoader[NodeId, NodeApply, Node, LoadableNodes, NodeList]):
    item_name = "nodes"
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

    def _is_equal_custom(self, local: NodeApply, cdf_resource: Node) -> bool:
        """Comparison for nodes to include properties in the comparison

        Note this is an expensive operation as we to an extra retrieve to fetch the properties.
        Thus, the cdf-tk should not be used to upload nodes that are data only nodes used for configuration.
        """
        # Note reading from a container is not supported.
        sources = [
            source_prop_pair.source
            for source_prop_pair in local.sources or []
            if isinstance(source_prop_pair.source, ViewId)
        ]
        try:
            cdf_resource_with_properties = self.client.data_modeling.instances.retrieve(
                nodes=cdf_resource.as_id(), sources=sources
            ).nodes[0]
        except CogniteAPIError:
            # View does not exist, so node does not exist.
            return False
        cdf_resource_dumped = cdf_resource_with_properties.as_write().dump()
        local_dumped = local.dump()
        if "existingVersion" not in local_dumped:
            # Existing version is typically not set when creating nodes, but we get it back
            # when we retrieve the node from the server.
            local_dumped["existingVersion"] = cdf_resource_dumped.get("existingVersion", None)

        return local_dumped == cdf_resource_dumped

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

    def retrieve(self, ids: SequenceNotStr[NodeId]) -> NodeList:
        return self.client.data_modeling.instances.retrieve(nodes=cast(Sequence, ids)).nodes

    def update(self, items: LoadableNodes) -> NodeApplyResultList:
        return self.create(items)

    def delete(self, ids: SequenceNotStr[NodeId]) -> int:
        try:
            deleted = self.client.data_modeling.instances.delete(nodes=cast(Sequence, ids))
        except CogniteAPIError as e:
            if "not exist" in e.message and "space" in e.message.lower():
                return 0
            raise e
        return len(deleted.nodes)

    def count(self, ids: SequenceNotStr[NodeId]) -> int:
        return len(ids)

    def drop_data(self, ids: SequenceNotStr[NodeId]) -> int:
        # Nodes will be deleted in .delete call.
        return 0


@final
class EdgeLoader(ResourceContainerLoader[EdgeId, EdgeApply, Edge, LoadableEdges, EdgeList]):
    item_name = "edges"
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

    def retrieve(self, ids: SequenceNotStr[EdgeId]) -> EdgeList:
        return self.client.data_modeling.instances.retrieve(edges=cast(Sequence, ids)).edges

    def update(self, items: LoadableEdges) -> EdgeApplyResultList:
        return self.create(items)

    def delete(self, ids: SequenceNotStr[EdgeId]) -> int:
        deleted = self.client.data_modeling.instances.delete(edges=cast(Sequence, ids))
        return len(deleted.edges)

    def count(self, ids: SequenceNotStr[EdgeId]) -> int:
        return len(ids)

    def drop_data(self, ids: SequenceNotStr[EdgeId]) -> int:
        # Edges will be deleted in .delete call.
        return 0
