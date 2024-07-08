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
from abc import ABC
from collections import defaultdict
from collections.abc import Callable, Hashable, Iterable, Sequence, Sized
from functools import lru_cache
from numbers import Number
from pathlib import Path
from time import sleep
from typing import Any, Literal, cast, final
from zipfile import ZipFile

import yaml
from cognite.client.data_classes import (
    ClientCredentials,
    DatapointsList,
    DatapointSubscription,
    DatapointSubscriptionList,
    DataPointSubscriptionUpdate,
    DataPointSubscriptionWrite,
    DatapointSubscriptionWriteList,
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
    LabelDefinition,
    LabelDefinitionList,
    LabelDefinitionWrite,
    OidcCredentials,
    TimeSeries,
    TimeSeriesList,
    TimeSeriesWrite,
    TimeSeriesWriteList,
    Transformation,
    TransformationList,
    TransformationNotification,
    TransformationNotificationList,
    TransformationSchedule,
    TransformationScheduleList,
    TransformationScheduleWrite,
    TransformationScheduleWriteList,
    TransformationWrite,
    TransformationWriteList,
    Workflow,
    WorkflowList,
    WorkflowUpsert,
    WorkflowUpsertList,
    WorkflowVersion,
    WorkflowVersionId,
    WorkflowVersionList,
    WorkflowVersionUpsert,
    WorkflowVersionUpsertList,
    capabilities,
    filters,
)
from cognite.client.data_classes._base import T_CogniteResourceList
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
    SecurityCategoriesAcl,
    SessionsAcl,
    TimeSeriesAcl,
    TimeSeriesSubscriptionsAcl,
    TransformationsAcl,
    WorkflowOrchestrationAcl,
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
from cognite.client.data_classes.iam import (
    Group,
    GroupList,
    GroupWrite,
    GroupWriteList,
    SecurityCategory,
    SecurityCategoryList,
    SecurityCategoryWrite,
    SecurityCategoryWriteList,
)
from cognite.client.data_classes.labels import LabelDefinitionWriteList
from cognite.client.data_classes.transformations.notifications import (
    TransformationNotificationWrite,
    TransformationNotificationWriteList,
)
from cognite.client.exceptions import CogniteAPIError, CogniteDuplicatedError, CogniteNotFoundError
from cognite.client.utils.useful_types import SequenceNotStr
from rich import print

from cognite_toolkit._cdf_tk._parameters import ANY_INT, ANY_STR, ANYTHING, ParameterSpec, ParameterSpecSet
from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.constants import INDEX_PATTERN
from cognite_toolkit._cdf_tk.exceptions import (
    ToolkitFileNotFoundError,
    ToolkitInvalidParameterNameError,
    ToolkitRequiredValueError,
    ToolkitYAMLFormatError,
)
from cognite_toolkit._cdf_tk.tk_warnings import (
    HighSeverityWarning,
    NamespacingConventionWarning,
    PrefixConventionWarning,
    WarningList,
    YAMLFileWarning,
)
from cognite_toolkit._cdf_tk.utils import (
    CDFToolConfig,
    calculate_directory_hash,
    load_yaml_inject_variables,
    retrieve_view_ancestors,
)

from ._base_loaders import ResourceContainerLoader, ResourceLoader
from .data_classes import NodeApplyListWithCall, RawDatabaseTable, RawTableList

_MIN_TIMESTAMP_MS = -2208988800000  # 1900-01-01 00:00:00.000
_MAX_TIMESTAMP_MS = 4102444799999  # 2099-12-31 23:59:59.999
_HAS_DATA_FILTER_LIMIT = 10


class GroupLoader(ResourceLoader[str, GroupWrite, Group, GroupWriteList, GroupList], ABC):
    folder_name = "auth"
    filename_pattern = r"^(?!.*SecurityCategory$).*"
    kind = "Group"
    resource_cls = Group
    resource_write_cls = GroupWrite
    list_cls = GroupList
    list_write_cls = GroupWriteList
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
    _doc_url = "Groups/operation/createGroups"

    def __init__(
        self,
        client: ToolkitClient,
        build_dir: Path | None,
        target_scopes: Literal[
            "all_scoped_only",
            "resource_scoped_only",
        ] = "all_scoped_only",
    ):
        super().__init__(client, build_dir)
        self.target_scopes = target_scopes

    @property
    def display_name(self) -> str:
        return f"iam.groups({self.target_scopes.removesuffix('_only')})"

    @classmethod
    def create_loader(
        cls,
        ToolGlobals: CDFToolConfig,
        build_dir: Path | None,
    ) -> GroupLoader:
        return cls(ToolGlobals.toolkit_client, build_dir)

    @classmethod
    def get_required_capability(cls, items: GroupWriteList) -> Capability | list[Capability]:
        if not items:
            return []
        return GroupsAcl(
            [GroupsAcl.Action.Read, GroupsAcl.Action.List, GroupsAcl.Action.Create, GroupsAcl.Action.Delete],
            GroupsAcl.Scope.All(),
        )

    @classmethod
    def get_id(cls, item: GroupWrite | Group | dict) -> str:
        if isinstance(item, dict):
            return item["name"]
        return item.name

    @classmethod
    def get_dependent_items(cls, item: dict) -> Iterable[tuple[type[ResourceLoader], Hashable]]:
        for capability in item.get("capabilities", []):
            for acl, content in capability.items():
                if scope := content.get("scope", {}):
                    if space_ids := scope.get(capabilities.SpaceIDScope._scope_name, []):
                        if isinstance(space_ids, dict) and "spaceIds" in space_ids:
                            for space_id in space_ids["spaceIds"]:
                                yield SpaceLoader, space_id
                    if data_set_ids := scope.get(capabilities.DataSetScope._scope_name, []):
                        if isinstance(data_set_ids, dict) and "ids" in data_set_ids:
                            for data_set_id in data_set_ids["ids"]:
                                yield DataSetsLoader, data_set_id
                    if table_ids := scope.get(capabilities.TableScope._scope_name, []):
                        for db_name, tables in table_ids.get("dbsToTables", {}).items():
                            yield RawDatabaseLoader, RawDatabaseTable(db_name)
                            for table in tables:
                                yield RawTableLoader, RawDatabaseTable(db_name, table)
                    if extraction_pipeline_ids := scope.get(capabilities.ExtractionPipelineScope._scope_name, []):
                        if isinstance(extraction_pipeline_ids, dict) and "ids" in extraction_pipeline_ids:
                            for extraction_pipeline_id in extraction_pipeline_ids["ids"]:
                                yield ExtractionPipelineLoader, extraction_pipeline_id
                    if (ids := scope.get(capabilities.IDScope._scope_name, [])) or (
                        ids := scope.get(capabilities.IDScopeLowerCase._scope_name, [])
                    ):
                        loader: type[ResourceLoader] | None = None
                        if acl == capabilities.DataSetsAcl._capability_name:
                            loader = DataSetsLoader
                        elif acl == capabilities.ExtractionPipelinesAcl._capability_name:
                            loader = ExtractionPipelineLoader
                        elif acl == capabilities.TimeSeriesAcl._capability_name:
                            loader = TimeSeriesLoader
                        if loader is not None and isinstance(ids, dict) and "ids" in ids:
                            for id_ in ids["ids"]:
                                yield loader, id_

    @classmethod
    def check_identifier_semantics(cls, identifier: str, filepath: Path, verbose: bool) -> WarningList[YAMLFileWarning]:
        warning_list = WarningList[YAMLFileWarning]()
        parts = identifier.split("_")
        if len(parts) < 2:
            if identifier == "applications-configuration":
                if verbose:
                    print(
                        "      [bold green]INFO:[/] the group applications-configuration does not follow the "
                        "recommended '_' based namespacing because Infield expects this specific name."
                    )
            else:
                warning_list.append(NamespacingConventionWarning(filepath, cls.folder_name, "name", identifier, "_"))
        elif not identifier.startswith("gp"):
            warning_list.append(PrefixConventionWarning(filepath, cls.folder_name, "name", identifier, "gp_"))
        return warning_list

    @staticmethod
    def _substitute_scope_ids(group: dict, ToolGlobals: CDFToolConfig, skip_validation: bool) -> dict:
        for capability in group.get("capabilities", []):
            for acl, values in capability.items():
                scope = values.get("scope", {})

                verify_method: Callable[[str, bool, str], int]
                for scope_name, verify_method, action in [
                    ("datasetScope", ToolGlobals.verify_dataset, "replace datasetExternalId with dataSetId in group"),
                    (
                        "idScope",
                        (
                            ToolGlobals.verify_extraction_pipeline
                            if acl == "extractionPipelinesAcl"
                            else ToolGlobals.verify_dataset
                        ),
                        "replace extractionPipelineExternalId with extractionPipelineId in group",
                    ),
                    (
                        "extractionPipelineScope",
                        ToolGlobals.verify_extraction_pipeline,
                        "replace extractionPipelineExternalId with extractionPipelineId in group",
                    ),
                ]:
                    if ids := scope.get(scope_name, {}).get("ids", []):
                        values["scope"][scope_name]["ids"] = [
                            verify_method(ext_id, skip_validation, action) if isinstance(ext_id, str) else ext_id
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

            group_write_list.append(GroupWrite.load(self._substitute_scope_ids(group, ToolGlobals, skip_validation)))

        if len(group_write_list) == 0:
            return None
        if len(group_write_list) == 1:
            return group_write_list[0]
        return group_write_list

    def _are_equal(
        self, local: GroupWrite, cdf_resource: Group, return_dumped: bool = False
    ) -> bool | tuple[bool, dict[str, Any], dict[str, Any]]:
        local_dumped = local.dump()
        cdf_dumped = cdf_resource.as_write().dump()

        scope_names = ["datasetScope", "idScope", "extractionPipelineScope"]

        ids_by_acl_by_actions_by_scope: dict[str, dict[frozenset[str], dict[str, list[str]]]] = {}
        for capability in cdf_dumped.get("capabilities", []):
            for acl, values in capability.items():
                ids_by_actions_by_scope = ids_by_acl_by_actions_by_scope.setdefault(acl, {})
                actions = values.get("actions", [])
                ids_by_scope = ids_by_actions_by_scope.setdefault(frozenset(actions), {})
                scope = values.get("scope", {})
                for scope_name in scope_names:
                    if ids := scope.get(scope_name, {}).get("ids", []):
                        if scope_name in ids_by_scope:
                            # Duplicated
                            ids_by_scope[scope_name].extend(ids)
                        else:
                            ids_by_scope[scope_name] = ids

        for capability in local_dumped.get("capabilities", []):
            for acl, values in capability.items():
                if acl not in ids_by_acl_by_actions_by_scope:
                    continue
                ids_by_actions_by_scope = ids_by_acl_by_actions_by_scope[acl]
                actions = frozenset(values.get("actions", []))
                if actions not in ids_by_actions_by_scope:
                    continue
                ids_by_scope = ids_by_actions_by_scope[actions]
                scope = values.get("scope", {})
                for scope_name in scope_names:
                    if ids := scope.get(scope_name, {}).get("ids", []):
                        is_dry_run = all(id_ == -1 for id_ in ids)
                        cdf_ids = ids_by_scope.get(scope_name, [])
                        are_equal_length = len(ids) == len(cdf_ids)
                        if is_dry_run and are_equal_length:
                            values["scope"][scope_name]["ids"] = list(cdf_ids)

        return self._return_are_equal(local_dumped, cdf_dumped, return_dumped)

    def _upsert(self, items: Sequence[GroupWrite]) -> GroupList:
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
        return self._upsert(items)

    def create(self, items: Sequence[GroupWrite]) -> GroupList:
        return self._upsert(items)

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

    def iterate(self) -> Iterable[Group]:
        return self.client.iam.groups.list(all=True)

    @classmethod
    @lru_cache(maxsize=1)
    def get_write_cls_parameter_spec(cls) -> ParameterSpecSet:
        spec = super().get_write_cls_parameter_spec()
        # The Capability class in the SDK class Group implementation is deviating from the API.
        # So we need to modify the spec to match the API.
        for item in spec:
            if item.path[0] == "capabilities" and len(item.path) > 2:
                # Add extra ANY_STR layer
                # The spec class is immutable, so we use this trick to modify it.
                object.__setattr__(item, "path", item.path[:2] + (ANY_STR,) + item.path[2:])
        spec.add(
            ParameterSpec(
                ("capabilities", ANY_INT, ANY_STR), frozenset({"dict"}), is_required=False, _is_nullable=False
            )
        )
        spec.add(
            ParameterSpec(
                ("capabilities", ANY_INT, ANY_STR, "scope", ANYTHING),
                frozenset({"dict"}),
                is_required=True,
                _is_nullable=False,
            )
        )
        return spec


@final
class GroupAllScopedLoader(GroupLoader):
    def __init__(self, client: ToolkitClient, build_dir: Path | None):
        super().__init__(client, build_dir, "all_scoped_only")


@final
class SecurityCategoryLoader(
    ResourceLoader[str, SecurityCategoryWrite, SecurityCategory, SecurityCategoryWriteList, SecurityCategoryList]
):
    filename_pattern = r"^.*SecurityCategory$"  # Matches all yaml files who's stem ends with *SecurityCategory.
    resource_cls = SecurityCategory
    resource_write_cls = SecurityCategoryWrite
    list_cls = SecurityCategoryList
    list_write_cls = SecurityCategoryWriteList
    kind = "SecurityCategory"
    folder_name = "auth"
    dependencies = frozenset({GroupAllScopedLoader})
    _doc_url = "Security-categories/operation/createSecurityCategories"

    @property
    def display_name(self) -> str:
        return "security.categories"

    @classmethod
    def get_id(cls, item: SecurityCategoryWrite | SecurityCategory | dict) -> str:
        if isinstance(item, dict):
            return item["name"]
        return cast(str, item.name)

    @classmethod
    def check_identifier_semantics(cls, identifier: str, filepath: Path, verbose: bool) -> WarningList[YAMLFileWarning]:
        warning_list = WarningList[YAMLFileWarning]()
        parts = identifier.split("_")
        if len(parts) < 2:
            warning_list.append(
                NamespacingConventionWarning(
                    filepath,
                    cls.folder_name,
                    "name",
                    identifier,
                    "_",
                )
            )
        elif not identifier.startswith("sc_"):
            warning_list.append(PrefixConventionWarning(filepath, cls.folder_name, "name", identifier, "sc_"))
        return warning_list

    @classmethod
    def get_required_capability(cls, items: SecurityCategoryWriteList) -> Capability | list[Capability]:
        if not items:
            return []
        return SecurityCategoriesAcl(
            actions=[
                SecurityCategoriesAcl.Action.Create,
                SecurityCategoriesAcl.Action.Update,
                SecurityCategoriesAcl.Action.MemberOf,
                SecurityCategoriesAcl.Action.List,
                SecurityCategoriesAcl.Action.Delete,
            ],
            scope=SecurityCategoriesAcl.Scope.All(),
        )

    def create(self, items: SecurityCategoryWriteList) -> SecurityCategoryList:
        return self.client.iam.security_categories.create(items)

    def retrieve(self, ids: SequenceNotStr[str]) -> SecurityCategoryList:
        names = set(ids)
        categories = self.client.iam.security_categories.list(limit=-1)
        return SecurityCategoryList([c for c in categories if c.name in names])

    def update(self, items: SecurityCategoryWriteList) -> SecurityCategoryList:
        items_by_name = {item.name: item for item in items}
        retrieved = self.retrieve(list(items_by_name.keys()))
        retrieved_by_name = {item.name: item for item in retrieved}
        new_items_by_name = {item.name: item for item in items if item.name not in retrieved_by_name}
        if new_items_by_name:
            created = self.client.iam.security_categories.create(list(new_items_by_name.values()))
            retrieved_by_name.update({item.name: item for item in created})
        return SecurityCategoryList([retrieved_by_name[name] for name in items_by_name])

    def delete(self, ids: SequenceNotStr[str]) -> int:
        retrieved = self.retrieve(ids)
        if retrieved:
            self.client.iam.security_categories.delete([item.id for item in retrieved if item.id])
        return len(retrieved)

    def iterate(self) -> Iterable[SecurityCategory]:
        return self.client.iam.security_categories.list(limit=-1)


@final
class DataSetsLoader(ResourceLoader[str, DataSetWrite, DataSet, DataSetWriteList, DataSetList]):
    support_drop = False
    folder_name = "data_sets"
    resource_cls = DataSet
    resource_write_cls = DataSetWrite
    list_cls = DataSetList
    list_write_cls = DataSetWriteList
    kind = "DataSet"
    dependencies = frozenset({GroupAllScopedLoader})
    _doc_url = "Data-sets/operation/createDataSets"

    @classmethod
    def get_required_capability(cls, items: DataSetWriteList) -> Capability | list[Capability]:
        if not items:
            return []
        return DataSetsAcl(
            [DataSetsAcl.Action.Read, DataSetsAcl.Action.Write],
            DataSetsAcl.Scope.All(),
        )

    @classmethod
    def get_id(cls, item: DataSet | DataSetWrite | dict) -> str:
        if isinstance(item, dict):
            return item["externalId"]
        if item.external_id is None:
            raise ToolkitRequiredValueError("DataSet must have external_id set.")
        return item.external_id

    @classmethod
    def check_identifier_semantics(cls, identifier: str, filepath: Path, verbose: bool) -> WarningList[YAMLFileWarning]:
        warning_list = WarningList[YAMLFileWarning]()
        parts = identifier.split("_")
        if len(parts) < 2:
            warning_list.append(NamespacingConventionWarning(filepath, cls.folder_name, "externalId", identifier, "_"))
        if not identifier.startswith("ds_"):
            warning_list.append(PrefixConventionWarning(filepath, cls.folder_name, "externalId", identifier, "ds_"))
        return warning_list

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

    def update(self, items: DataSetWriteList) -> DataSetList:
        return self.client.data_sets.update(items)

    def delete(self, ids: SequenceNotStr[str]) -> int:
        raise NotImplementedError("CDF does not support deleting data sets.")

    def iterate(self) -> Iterable[DataSet]:
        return iter(self.client.data_sets)

    @classmethod
    @lru_cache(maxsize=1)
    def get_write_cls_parameter_spec(cls) -> ParameterSpecSet:
        spec = super().get_write_cls_parameter_spec()
        # Added by toolkit, toolkit will automatically convert metadata to json
        spec.add(
            ParameterSpec(
                ("metadata", ANY_STR, ANYTHING), frozenset({"unknown"}), is_required=False, _is_nullable=False
            )
        )
        return spec


@final
class LabelLoader(
    ResourceLoader[str, LabelDefinitionWrite, LabelDefinition, LabelDefinitionWriteList, LabelDefinitionList]
):
    folder_name = "labels"
    filename_pattern = r"^.*Label$"  # Matches all yaml files whose stem ends with *Label.
    resource_cls = LabelDefinition
    resource_write_cls = LabelDefinitionWrite
    list_cls = LabelDefinitionList
    list_write_cls = LabelDefinitionWriteList
    kind = "Label"
    dependencies = frozenset({DataSetsLoader, GroupAllScopedLoader})
    _doc_url = "Labels/operation/createLabelDefinitions"

    @classmethod
    def get_id(cls, item: LabelDefinition | LabelDefinitionWrite | dict) -> str:
        if isinstance(item, dict):
            return item["externalId"]
        if not item.external_id:
            raise ToolkitRequiredValueError("LabelDefinition must have external_id set.")
        return item.external_id

    @classmethod
    def get_required_capability(cls, items: LabelDefinitionWriteList) -> Capability | list[Capability]:
        if not items:
            return []
        data_set_ids = {item.data_set_id for item in items if item.data_set_id}
        scope = (
            capabilities.LabelsAcl.Scope.DataSet(list(data_set_ids))
            if data_set_ids
            else capabilities.LabelsAcl.Scope.All()
        )

        return capabilities.LabelsAcl(
            [capabilities.LabelsAcl.Action.Read, capabilities.LabelsAcl.Action.Write],
            scope,  # type: ignore[arg-type]
        )

    def create(self, items: LabelDefinitionWriteList) -> LabelDefinitionList:
        return self.client.labels.create(items)

    def retrieve(self, ids: SequenceNotStr[str]) -> LabelDefinitionList:
        return self.client.labels.retrieve(ids, ignore_unknown_ids=True)

    def update(self, items: T_CogniteResourceList) -> LabelDefinitionList:
        existing = self.client.labels.retrieve([item.external_id for item in items])
        if existing:
            self.delete([item.external_id for item in items])
        return self.client.labels.create(items)

    def delete(self, ids: SequenceNotStr[str]) -> int:
        try:
            self.client.labels.delete(ids)
        except (CogniteAPIError, CogniteNotFoundError) as e:
            non_existing = set(e.failed or [])
            if existing := [id_ for id_ in ids if id_ not in non_existing]:
                self.client.labels.delete(existing)
            return len(existing)
        else:
            # All deleted successfully
            return len(ids)

    def iterate(self) -> Iterable[LabelDefinition]:
        return iter(self.client.labels)

    @classmethod
    @lru_cache(maxsize=1)
    def get_write_cls_parameter_spec(cls) -> ParameterSpecSet:
        spec = super().get_write_cls_parameter_spec()
        # Added by toolkit
        spec.add(ParameterSpec(("dataSetExternalId",), frozenset({"str"}), is_required=False, _is_nullable=False))
        return spec

    @classmethod
    def get_dependent_items(cls, item: dict) -> Iterable[tuple[type[ResourceLoader], Hashable]]:
        """Returns all items that this item requires.

        For example, a TimeSeries requires a DataSet, so this method would return the
        DatasetLoader and identifier of that dataset.
        """
        if "dataSetExternalId" in item:
            yield DataSetsLoader, item["dataSetExternalId"]

    def load_resource(
        self, filepath: Path, ToolGlobals: CDFToolConfig, skip_validation: bool
    ) -> LabelDefinitionWrite | LabelDefinitionWriteList | None:
        raw_yaml = load_yaml_inject_variables(filepath, ToolGlobals.environment_variables())
        items: list[dict[str, Any]] = [raw_yaml] if isinstance(raw_yaml, dict) else raw_yaml
        for item in items:
            if "dataSetExternalId" in item:
                ds_external_id = item.pop("dataSetExternalId")
                item["dataSetId"] = ToolGlobals.verify_dataset(
                    ds_external_id,
                    skip_validation=skip_validation,
                    action="replace dataSetExternalId with dataSetId in label",
                )
        loaded = LabelDefinitionWriteList.load(items)
        return loaded[0] if isinstance(raw_yaml, dict) else loaded

    def _are_equal(
        self, local: LabelDefinitionWrite, cdf_resource: LabelDefinition, return_dumped: bool = False
    ) -> bool | tuple[bool, dict[str, Any], dict[str, Any]]:
        local_dumped = local.dump()
        cdf_dumped = cdf_resource.as_write().dump()
        if local_dumped.get("dataSetId") == -1 and "dataSetId" in cdf_dumped:
            # Dry run
            local_dumped["dataSetId"] = cdf_dumped["dataSetId"]
        return self._return_are_equal(local_dumped, cdf_dumped, return_dumped)


@final
class FunctionLoader(ResourceLoader[str, FunctionWrite, Function, FunctionWriteList, FunctionList]):
    support_drop = True
    folder_name = "functions"
    filename_pattern = (
        r"^(?:(?!schedule).)*$"  # Matches all yaml files except file names who's stem contain *.schedule.
    )
    resource_cls = Function
    resource_write_cls = FunctionWrite
    list_cls = FunctionList
    list_write_cls = FunctionWriteList
    kind = "Function"
    dependencies = frozenset({DataSetsLoader, GroupAllScopedLoader})
    _doc_url = "Functions/operation/postFunctions"

    @classmethod
    def get_required_capability(cls, items: FunctionWriteList) -> list[Capability] | list[Capability]:
        if not items:
            return []
        return [
            FunctionsAcl([FunctionsAcl.Action.Read, FunctionsAcl.Action.Write], FunctionsAcl.Scope.All()),
            FilesAcl(
                [FilesAcl.Action.Read, FilesAcl.Action.Write], FilesAcl.Scope.All()
            ),  # Needed for uploading function artifacts
        ]

    @classmethod
    def get_id(cls, item: Function | FunctionWrite | dict) -> str:
        if isinstance(item, dict):
            return item["externalId"]
        if item.external_id is None:
            raise ToolkitRequiredValueError("Function must have external_id set.")
        return item.external_id

    @classmethod
    def get_dependent_items(cls, item: dict) -> Iterable[tuple[type[ResourceLoader], Hashable]]:
        if "dataSetExternalId" in item:
            yield DataSetsLoader, item["dataSetExternalId"]

    def load_resource(
        self, filepath: Path, ToolGlobals: CDFToolConfig, skip_validation: bool
    ) -> FunctionWrite | FunctionWriteList | None:
        if filepath.parent.name != self.folder_name:
            # Functions configs needs to be in the root function folder.
            # Thi is to allow arbitrary YAML files inside the function code folder.
            return None

        functions = load_yaml_inject_variables(filepath, ToolGlobals.environment_variables())

        if isinstance(functions, dict):
            functions = [functions]

        for func in functions:
            if self.extra_configs.get(func["externalId"]) is None:
                self.extra_configs[func["externalId"]] = {}
            if func.get("dataSetExternalId") is not None:
                self.extra_configs[func["externalId"]]["dataSetId"] = ToolGlobals.verify_dataset(
                    func.get("dataSetExternalId", ""),
                    skip_validation=skip_validation,
                    action="replace datasetExternalId with dataSetId in function",
                )
            if "fileId" not in func:
                # The fileID is required for the function to be created, but in the `.create` method
                # we first create that file and then set the fileID.
                func["fileId"] = "<will_be_generated>"

        if len(functions) == 1:
            return FunctionWrite.load(functions[0])
        else:
            return FunctionWriteList.load(functions)

    def _are_equal(
        self, local: FunctionWrite, cdf_resource: Function, return_dumped: bool = False
    ) -> bool | tuple[bool, dict[str, Any], dict[str, Any]]:
        if self.resource_build_path is None:
            raise ValueError("build_path must be set to compare functions as function code must be compared.")
        # If the function failed, we want to always trigger a redeploy.
        if cdf_resource.status == "Failed":
            if return_dumped:
                return False, local.dump(), {}
            else:
                return False
        function_rootdir = Path(self.resource_build_path / f"{local.external_id}")
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
        local_dumped = local.dump()
        cdf_dumped = cdf_resource.as_write().dump()
        if local_dumped.get("dataSetId") == -1 and "dataSetId" in cdf_dumped:
            # Dry run
            local_dumped["dataSetId"] = cdf_dumped["dataSetId"]
        return self._return_are_equal(local_dumped, cdf_dumped, return_dumped)

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
        if self.resource_build_path is None:
            raise ValueError("build_path must be set to compare functions as function code must be compared.")
        for item in items:
            function_rootdir = Path(self.resource_build_path / (item.external_id or ""))
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

    def iterate(self) -> Iterable[Function]:
        return iter(self.client.functions)

    @classmethod
    @lru_cache(maxsize=1)
    def get_write_cls_parameter_spec(cls) -> ParameterSpecSet:
        spec = super().get_write_cls_parameter_spec()
        # Added by toolkit
        spec.add(ParameterSpec(("dataSetExternalId",), frozenset({"str"}), is_required=False, _is_nullable=False))
        # Replaced by the toolkit
        spec.discard(ParameterSpec(("fileId",), frozenset({"int"}), is_required=True, _is_nullable=False))
        return spec


@final
class FunctionScheduleLoader(
    ResourceLoader[str, FunctionScheduleWrite, FunctionSchedule, FunctionScheduleWriteList, FunctionSchedulesList]
):
    folder_name = "functions"
    filename_pattern = r"^.*schedule.*$"  # Matches all yaml files who's stem contain *.schedule
    resource_cls = FunctionSchedule
    resource_write_cls = FunctionScheduleWrite
    list_cls = FunctionSchedulesList
    list_write_cls = FunctionScheduleWriteList
    kind = "Schedule"
    dependencies = frozenset({FunctionLoader})
    _doc_url = "Function-schedules/operation/postFunctionSchedules"
    _split_character = ":"

    @property
    def display_name(self) -> str:
        return "function.schedules"

    @classmethod
    def get_required_capability(cls, items: FunctionScheduleWriteList) -> list[Capability]:
        if not items:
            return []
        return [
            FunctionsAcl([FunctionsAcl.Action.Read, FunctionsAcl.Action.Write], FunctionsAcl.Scope.All()),
            SessionsAcl(
                [SessionsAcl.Action.List, SessionsAcl.Action.Create, SessionsAcl.Action.Delete], SessionsAcl.Scope.All()
            ),
        ]

    @classmethod
    def get_id(cls, item: FunctionScheduleWrite | FunctionSchedule | dict) -> str:
        if isinstance(item, dict):
            if missing := tuple(k for k in {"functionExternalId", "cronExpression"} if k not in item):
                # We need to raise a KeyError with all missing keys to get the correct error message.
                raise KeyError(*missing)
            return f"{item['functionExternalId']}{cls._split_character}{item['cronExpression']}"

        if item.function_external_id is None or item.cron_expression is None:
            raise ToolkitRequiredValueError("FunctionSchedule must have functionExternalId and CronExpression set.")
        return f"{item.function_external_id}{cls._split_character}{item.cron_expression}"

    @classmethod
    def get_dependent_items(cls, item: dict) -> Iterable[tuple[type[ResourceLoader], Hashable]]:
        if "functionExternalId" in item:
            yield FunctionLoader, item["functionExternalId"]

    def load_resource(
        self, filepath: Path, ToolGlobals: CDFToolConfig, skip_validation: bool
    ) -> FunctionScheduleWrite | FunctionScheduleWriteList | None:
        schedules = load_yaml_inject_variables(filepath, ToolGlobals.environment_variables())
        if isinstance(schedules, dict):
            schedules = [schedules]

        for sched in schedules:
            ext_id = f"{sched['functionExternalId']}{self._split_character}{sched['cronExpression']}"
            if self.extra_configs.get(ext_id) is None:
                self.extra_configs[ext_id] = {}
            self.extra_configs[ext_id]["authentication"] = sched.pop("authentication", {})
        return FunctionScheduleWriteList.load(schedules)

    def _are_equal(
        self, local: FunctionScheduleWrite, cdf_resource: FunctionSchedule, return_dumped: bool = False
    ) -> bool | tuple[bool, dict[str, Any], dict[str, Any]]:
        cdf_dumped = cdf_resource.as_write().dump()
        del cdf_dumped["functionId"]
        local_dumped = local.dump()
        return self._return_are_equal(local_dumped, cdf_dumped, return_dumped)

    def _resolve_functions_ext_id(self, items: FunctionScheduleWriteList) -> FunctionScheduleWriteList:
        functions = FunctionLoader(self.client, None).retrieve(list(set([item.function_external_id for item in items])))
        for item in items:
            for func in functions:
                if func.external_id == item.function_external_id:
                    item.function_id = func.id  # type: ignore[assignment]
        return items

    def retrieve(self, ids: SequenceNotStr[str]) -> FunctionSchedulesList:
        crons_by_function: dict[str, set[str]] = defaultdict(set)
        for id_ in ids:
            function_external_id, cron = id_.rsplit(self._split_character, 1)
            crons_by_function[function_external_id].add(cron)
        functions = FunctionLoader(self.client, None).retrieve(list(crons_by_function))
        schedules = FunctionSchedulesList([])
        for func in functions:
            ret = self.client.functions.schedules.list(function_id=func.id, limit=-1)
            for schedule in ret:
                schedule.function_external_id = func.external_id
            schedules.extend(
                [
                    schedule
                    for schedule in ret
                    if schedule.cron_expression in crons_by_function[cast(str, func.external_id)]
                ]
            )
        return schedules

    def create(self, items: FunctionScheduleWriteList) -> FunctionSchedulesList:
        items = self._resolve_functions_ext_id(items)
        created = []
        for item in items:
            key = f"{item.function_external_id}:{item.cron_expression}"
            auth_config = self.extra_configs.get(key, {}).get("authentication", {})
            if "clientId" in auth_config and "clientSecret" in auth_config:
                client_credentials = ClientCredentials(auth_config["clientId"], auth_config["clientSecret"])
            else:
                client_credentials = None

            created.append(
                self.client.functions.schedules.create(
                    name=item.name or "",
                    description=item.description or "",
                    cron_expression=cast(str, item.cron_expression),
                    function_id=cast(int, item.function_id),
                    data=item.data,
                    client_credentials=client_credentials,
                )
            )
        return FunctionSchedulesList(created)

    def update(self, items: FunctionScheduleWriteList) -> Sized:
        # Function schedule does not have an update, so we delete and recreate
        self.delete(self.get_ids(items))
        return self.create(items)

    def delete(self, ids: SequenceNotStr[str]) -> int:
        schedules = self.retrieve(ids)
        count = 0
        for schedule in schedules:
            if schedule.id:
                self.client.functions.schedules.delete(id=schedule.id)
            count += 1
        return count

    def iterate(self) -> Iterable[FunctionSchedule]:
        return iter(self.client.functions.schedules)

    @classmethod
    @lru_cache(maxsize=1)
    def get_write_cls_parameter_spec(cls) -> ParameterSpecSet:
        spec = super().get_write_cls_parameter_spec()
        # Added by toolkit
        spec.add(ParameterSpec(("authentication",), frozenset({"dict"}), is_required=False, _is_nullable=False))
        spec.add(
            ParameterSpec(("authentication", "clientId"), frozenset({"str"}), is_required=False, _is_nullable=False)
        )
        spec.add(
            ParameterSpec(("authentication", "clientSecret"), frozenset({"str"}), is_required=False, _is_nullable=False)
        )
        return spec


@final
class RawDatabaseLoader(
    ResourceContainerLoader[RawDatabaseTable, RawDatabaseTable, RawDatabaseTable, RawTableList, RawTableList]
):
    item_name = "raw tables"
    folder_name = "raw"
    resource_cls = RawDatabaseTable
    resource_write_cls = RawDatabaseTable
    list_cls = RawTableList
    list_write_cls = RawTableList
    kind = "Database"
    dependencies = frozenset({GroupAllScopedLoader})
    _doc_url = "Raw/operation/createDBs"

    def __init__(self, client: ToolkitClient, build_dir: Path):
        super().__init__(client, build_dir)
        self._loaded_db_names: set[str] = set()

    @property
    def display_name(self) -> str:
        return "raw.databases"

    @classmethod
    def get_required_capability(cls, items: RawTableList) -> Capability | list[Capability]:
        if not items:
            return []
        tables_by_database = defaultdict(list)
        for item in items:
            tables_by_database[item.db_name].append(item.table_name)

        scope = RawAcl.Scope.Table(dict(tables_by_database)) if tables_by_database else RawAcl.Scope.All()  # type: ignore[arg-type]

        return RawAcl([RawAcl.Action.Read, RawAcl.Action.Write], scope)  # type: ignore[arg-type]

    @classmethod
    def get_id(cls, item: RawDatabaseTable | dict) -> RawDatabaseTable:
        if isinstance(item, dict):
            return RawDatabaseTable(item["dbName"])
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
        return RawTableList([RawDatabaseTable(db_name=db.name) for db in database_list if db.name])

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

    def iterate(self) -> Iterable[RawDatabaseTable]:
        return (RawDatabaseTable(db_name=cast(str, db.name)) for db in self.client.raw.databases)

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
    folder_name = "raw"
    resource_cls = RawDatabaseTable
    resource_write_cls = RawDatabaseTable
    list_cls = RawTableList
    list_write_cls = RawTableList
    kind = "Table"
    dependencies = frozenset({RawDatabaseLoader, GroupAllScopedLoader})
    _doc_url = "Raw/operation/createTables"

    def __init__(self, client: ToolkitClient, build_dir: Path):
        super().__init__(client, build_dir)
        self._printed_warning = False

    @property
    def display_name(self) -> str:
        return "raw.tables"

    @classmethod
    def get_required_capability(cls, items: RawTableList) -> Capability | list[Capability]:
        if not items:
            return []
        tables_by_database = defaultdict(list)
        for item in items:
            tables_by_database[item.db_name].append(item.table_name)

        scope = RawAcl.Scope.Table(dict(tables_by_database)) if tables_by_database else RawAcl.Scope.All()  # type: ignore[arg-type]

        return RawAcl([RawAcl.Action.Read, RawAcl.Action.Write], scope)  # type: ignore[arg-type]

    @classmethod
    def get_id(cls, item: RawDatabaseTable | dict) -> RawDatabaseTable:
        if isinstance(item, dict):
            if missing := tuple(k for k in {"dbName", "tableName"} if k not in item):
                # We need to raise a KeyError with all missing keys to get the correct error message.
                raise KeyError(*missing)
            return RawDatabaseTable(item["dbName"], item["tableName"])
        return item

    @classmethod
    def get_dependent_items(cls, item: dict) -> Iterable[tuple[type[ResourceLoader], Hashable]]:
        if "dbName" in item:
            yield RawDatabaseLoader, RawDatabaseTable(item["dbName"])

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

    def iterate(self) -> Iterable[RawDatabaseTable]:
        return (
            RawDatabaseTable(db_name=cast(str, db.name), table_name=cast(str, table.name))
            for db in self.client.raw.databases
            for table in self.client.raw.tables(cast(str, db.name))
        )

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
    folder_name = "timeseries"
    filename_pattern = r"^(?!.*DatapointSubscription$).*"
    resource_cls = TimeSeries
    resource_write_cls = TimeSeriesWrite
    list_cls = TimeSeriesList
    list_write_cls = TimeSeriesWriteList
    kind = "TimeSeries"
    dependencies = frozenset({DataSetsLoader, GroupAllScopedLoader})
    _doc_url = "Time-series/operation/postTimeSeries"

    @classmethod
    def get_required_capability(cls, items: TimeSeriesWriteList) -> Capability | list[Capability]:
        if not items:
            return []

        dataset_ids = {item.data_set_id for item in items if item.data_set_id}

        scope = TimeSeriesAcl.Scope.DataSet(list(dataset_ids)) if dataset_ids else TimeSeriesAcl.Scope.All()

        return TimeSeriesAcl(
            [TimeSeriesAcl.Action.Read, TimeSeriesAcl.Action.Write],
            scope,  # type: ignore[arg-type]
        )

    @classmethod
    def get_id(cls, item: TimeSeries | TimeSeriesWrite | dict) -> str:
        if isinstance(item, dict):
            return item["externalId"]
        if item.external_id is None:
            raise ToolkitRequiredValueError("TimeSeries must have external_id set.")
        return item.external_id

    @classmethod
    def get_dependent_items(cls, item: dict) -> Iterable[tuple[type[ResourceLoader], Hashable]]:
        if "dataSetExternalId" in item:
            yield DataSetsLoader, item["dataSetExternalId"]
        if "securityCategoryNames" in item:
            for security_category in item["securityCategoryNames"]:
                yield SecurityCategoryLoader, security_category

    def load_resource(self, filepath: Path, ToolGlobals: CDFToolConfig, skip_validation: bool) -> TimeSeriesWriteList:
        resources = load_yaml_inject_variables(filepath, {})
        if not isinstance(resources, list):
            resources = [resources]
        for resource in resources:
            if resource.get("dataSetExternalId") is not None:
                ds_external_id = resource.pop("dataSetExternalId")
                resource["dataSetId"] = ToolGlobals.verify_dataset(
                    ds_external_id, skip_validation, action="replace dataSetExternalId with dataSetId in time series"
                )
            if "securityCategoryNames" in resource:
                if security_categories_names := resource.pop("securityCategoryNames", []):
                    security_categories = ToolGlobals.verify_security_categories(
                        security_categories_names,
                        skip_validation,
                        action="replace securityCategoryNames with securityCategoryIDs in time series",
                    )
                    resource["securityCategories"] = security_categories
            if resource.get("securityCategories") is None:
                # Bug in SDK, the read version sets security categories to an empty list.
                resource["securityCategories"] = []
        return TimeSeriesWriteList.load(resources)

    def _are_equal(
        self, local: TimeSeriesWrite, cdf_resource: TimeSeries, return_dumped: bool = False
    ) -> bool | tuple[bool, dict[str, Any], dict[str, Any]]:
        local_dumped = local.dump()
        cdf_dumped = cdf_resource.as_write().dump()

        # If dataSetId or SecurityCategories are not set in the local, but are set in the CDF, it is a dry run
        # and we assume they are the same.
        if local_dumped.get("dataSetId") == -1 and "dataSetId" in cdf_dumped:
            local_dumped["dataSetId"] = cdf_dumped["dataSetId"]
        if (
            all(s == -1 for s in local_dumped.get("securityCategories", []))
            and "securityCategories" in cdf_dumped
            and len(cdf_dumped["securityCategories"]) == len(local_dumped.get("securityCategories", []))
        ):
            local_dumped["securityCategories"] = cdf_dumped["securityCategories"]
        if local_dumped.get("assetId") == -1 and "assetId" in cdf_dumped:
            local_dumped["assetId"] = cdf_dumped["assetId"]
        return self._return_are_equal(local_dumped, cdf_dumped, return_dumped)

    def create(self, items: TimeSeriesWriteList) -> TimeSeriesList:
        return self.client.time_series.create(items)

    def retrieve(self, ids: SequenceNotStr[str]) -> TimeSeriesList:
        return self.client.time_series.retrieve_multiple(
            external_ids=cast(SequenceNotStr[str], ids), ignore_unknown_ids=True
        )

    def update(self, items: TimeSeriesWriteList) -> TimeSeriesList:
        return self.client.time_series.update(items)

    def delete(self, ids: SequenceNotStr[str]) -> int:
        existing = self.retrieve(ids).as_external_ids()
        if existing:
            self.client.time_series.delete(external_id=existing, ignore_unknown_ids=True)
        return len(existing)

    def iterate(self) -> Iterable[TimeSeries]:
        return iter(self.client.time_series)

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

    @classmethod
    @lru_cache(maxsize=1)
    def get_write_cls_parameter_spec(cls) -> ParameterSpecSet:
        spec = super().get_write_cls_parameter_spec()
        # Added by toolkit
        spec.add(ParameterSpec(("dataSetExternalId",), frozenset({"str"}), is_required=False, _is_nullable=False))

        spec.add(ParameterSpec(("securityCategoryNames",), frozenset({"list"}), is_required=False, _is_nullable=False))

        spec.add(
            ParameterSpec(("securityCategoryNames", ANY_STR), frozenset({"str"}), is_required=False, _is_nullable=False)
        )
        return spec


@final
class DatapointSubscriptionLoader(
    ResourceLoader[
        str,
        DataPointSubscriptionWrite,
        DatapointSubscription,
        DatapointSubscriptionWriteList,
        DatapointSubscriptionList,
    ]
):
    folder_name = "timeseries"
    filename_pattern = r"^.*DatapointSubscription$"  # Matches all yaml files who end with *DatapointSubscription.
    resource_cls = DatapointSubscription
    resource_write_cls = DataPointSubscriptionWrite
    list_cls = DatapointSubscriptionList
    list_write_cls = DatapointSubscriptionWriteList
    kind = "DatapointSubscription"
    _doc_url = "Data-point-subscriptions/operation/postSubscriptions"
    dependencies = frozenset(
        {
            TimeSeriesLoader,
            GroupAllScopedLoader,
        }
    )

    @property
    def display_name(self) -> str:
        return "timeseries.subscription"

    @classmethod
    def get_id(cls, item: DataPointSubscriptionWrite | DatapointSubscription | dict) -> str:
        if isinstance(item, dict):
            return item["externalId"]
        return item.external_id

    @classmethod
    @lru_cache(maxsize=1)
    def get_write_cls_parameter_spec(cls) -> ParameterSpecSet:
        spec = super().get_write_cls_parameter_spec()
        # Added by toolkit
        spec.add(ParameterSpec(("dataSetExternalId",), frozenset({"str"}), is_required=False, _is_nullable=False))
        # The Filter class in the SDK class View implementation is deviating from the API.
        # So we need to modify the spec to match the API.
        parameter_path = ("filter",)
        length = len(parameter_path)
        for item in spec:
            if len(item.path) >= length + 1 and item.path[:length] == parameter_path[:length]:
                # Add extra ANY_STR layer
                # The spec class is immutable, so we use this trick to modify it.
                object.__setattr__(item, "path", item.path[:length] + (ANY_STR,) + item.path[length:])
        spec.add(ParameterSpec(("filter", ANY_STR), frozenset({"dict"}), is_required=False, _is_nullable=False))
        return spec

    @classmethod
    def get_dependent_items(cls, item: dict) -> Iterable[tuple[type[ResourceLoader], Hashable]]:
        if "dataSetExternalId" in item:
            yield DataSetsLoader, item["dataSetExternalId"]
        for timeseries_id in item.get("timeSeriesIds", []):
            yield TimeSeriesLoader, timeseries_id

    @classmethod
    def get_required_capability(cls, items: DatapointSubscriptionWriteList) -> Capability | list[Capability]:
        if not items:
            return []
        data_set_ids = {item.data_set_id for item in items if item.data_set_id}
        scope = (
            TimeSeriesSubscriptionsAcl.Scope.DataSet(list(data_set_ids))
            if data_set_ids
            else TimeSeriesSubscriptionsAcl.Scope.All()
        )
        return TimeSeriesSubscriptionsAcl(
            [TimeSeriesSubscriptionsAcl.Action.Read, TimeSeriesSubscriptionsAcl.Action.Write],
            scope,  # type: ignore[arg-type]
        )

    def create(self, items: DatapointSubscriptionWriteList) -> DatapointSubscriptionList:
        created = DatapointSubscriptionList([])
        for item in items:
            created.append(self.client.time_series.subscriptions.create(item))
        return created

    def retrieve(self, ids: SequenceNotStr[str]) -> DatapointSubscriptionList:
        items = DatapointSubscriptionList([])
        for id_ in ids:
            retrieved = self.client.time_series.subscriptions.retrieve(id_)
            if retrieved:
                items.append(retrieved)
        return items

    def update(self, items: DatapointSubscriptionWriteList) -> DatapointSubscriptionList:
        updated = DatapointSubscriptionList([])
        for item in items:
            # Todo update SDK to support taking in Write object
            update = self.client.time_series.subscriptions._update_multiple(
                item,
                list_cls=DatapointSubscriptionWriteList,
                resource_cls=DataPointSubscriptionWrite,
                update_cls=DataPointSubscriptionUpdate,
            )
            updated.append(update)

        return updated

    def delete(self, ids: SequenceNotStr[str]) -> int:
        try:
            self.client.time_series.subscriptions.delete(ids)
        except (CogniteAPIError, CogniteNotFoundError) as e:
            non_existing = set(e.failed or [])
            if existing := [id_ for id_ in ids if id_ not in non_existing]:
                self.client.time_series.subscriptions.delete(existing)
            return len(existing)
        else:
            # All deleted successfully
            return len(ids)

    def iterate(self) -> Iterable[DatapointSubscription]:
        return iter(self.client.time_series.subscriptions)

    def _are_equal(
        self, local: DataPointSubscriptionWrite, cdf_resource: DatapointSubscription, return_dumped: bool = False
    ) -> bool | tuple[bool, dict[str, Any], dict[str, Any]]:
        local_dumped = local.dump()
        cdf_dumped = cdf_resource.as_write().dump()
        # Two subscription objects are equal if they have the same timeSeriesIds
        if "timeSeriesIds" in local_dumped:
            local_dumped["timeSeriesIds"] = set(local_dumped["timeSeriesIds"])
        if "timeSeriesIds" in cdf_dumped:
            local_dumped["timeSeriesIds"] = set(cdf_dumped["timeSeriesIds"])

        return self._return_are_equal(local_dumped, cdf_dumped, return_dumped)


@final
class TransformationLoader(
    ResourceLoader[str, TransformationWrite, Transformation, TransformationWriteList, TransformationList]
):
    folder_name = "transformations"
    filename_pattern = (
        # Matches all yaml files except file names whose stem contain *.schedule. or .Notification
        r"^(?!.*schedule.*|.*\.notification$).*$"
    )
    resource_cls = Transformation
    resource_write_cls = TransformationWrite
    list_cls = TransformationList
    list_write_cls = TransformationWriteList
    kind = "Transformation"
    dependencies = frozenset({DataSetsLoader, RawDatabaseLoader, GroupAllScopedLoader})
    _doc_url = "Transformations/operation/createTransformations"

    @classmethod
    def get_required_capability(cls, items: TransformationWriteList) -> Capability | list[Capability]:
        if not items:
            return []
        data_set_ids = {item.data_set_id for item in items if item.data_set_id}

        scope = TransformationsAcl.Scope.DataSet(list(data_set_ids)) if data_set_ids else TransformationsAcl.Scope.All()

        return TransformationsAcl(
            [TransformationsAcl.Action.Read, TransformationsAcl.Action.Write],
            scope,  # type: ignore[arg-type]
        )

    @classmethod
    def get_id(cls, item: Transformation | TransformationWrite | dict) -> str:
        if isinstance(item, dict):
            return item["externalId"]
        if item.external_id is None:
            raise ToolkitRequiredValueError("Transformation must have external_id set.")
        return item.external_id

    @classmethod
    def get_dependent_items(cls, item: dict) -> Iterable[tuple[type[ResourceLoader], Hashable]]:
        if "dataSetExternalId" in item:
            yield DataSetsLoader, item["dataSetExternalId"]
        if destination := item.get("destination", {}):
            if destination.get("type") == "raw" and _in_dict(("database", "table"), destination):
                yield RawDatabaseLoader, RawDatabaseTable(destination["database"])
                yield RawTableLoader, RawDatabaseTable(destination["database"], destination["table"])
            elif destination.get("type") in ("nodes", "edges") and (view := destination.get("view", {})):
                if space := destination.get("instanceSpace"):
                    yield SpaceLoader, space
                if _in_dict(("space", "externalId", "version"), view):
                    yield ViewLoader, ViewId.load(view)
            elif destination.get("type") == "instances":
                if space := destination.get("instanceSpace"):
                    yield SpaceLoader, space
                if data_model := destination.get("dataModel"):
                    if _in_dict(("space", "externalId", "version"), data_model):
                        yield DataModelLoader, DataModelId.load(data_model)

    @classmethod
    def check_identifier_semantics(cls, identifier: str, filepath: Path, verbose: bool) -> WarningList[YAMLFileWarning]:
        warning_list = WarningList[YAMLFileWarning]()
        parts = identifier.split("_")
        if len(parts) < 2:
            warning_list.append(
                NamespacingConventionWarning(
                    filepath,
                    cls.folder_name,
                    "externalId",
                    identifier,
                    "_",
                )
            )
        elif not identifier.startswith("tr"):
            warning_list.append(PrefixConventionWarning(filepath, cls.folder_name, "externalId", identifier, "tr_"))
        return warning_list

    def _are_equal(
        self, local: TransformationWrite, cdf_resource: Transformation, return_dumped: bool = False
    ) -> bool | tuple[bool, dict[str, Any], dict[str, Any]]:
        local_dumped = local.dump()
        local_dumped.pop("destinationOidcCredentials", None)
        local_dumped.pop("sourceOidcCredentials", None)
        cdf_dumped = cdf_resource.as_write().dump()
        if local_dumped.get("dataSetId") == -1 and "dataSetId" in cdf_dumped:
            # Dry run
            local_dumped["dataSetId"] = cdf_dumped["dataSetId"]

        return self._return_are_equal(local_dumped, cdf_dumped, return_dumped)

    @staticmethod
    def _get_query_file(filepath: Path, transformation_external_id: str | None) -> Path | None:
        query_file = filepath.parent / f"{filepath.stem}.sql"
        if not query_file.exists() and transformation_external_id:
            found_query_file = next(
                (
                    f
                    for f in filepath.parent.iterdir()
                    if f.is_file() and f.name.endswith(f"{transformation_external_id}.sql")
                ),
                None,
            )
            if found_query_file is None:
                return None
            query_file = found_query_file
        return query_file

    def load_resource(
        self, filepath: Path, ToolGlobals: CDFToolConfig, skip_validation: bool
    ) -> TransformationWrite | TransformationWriteList:
        resources = load_yaml_inject_variables(filepath, ToolGlobals.environment_variables())
        # The `authentication` key is custom for this template:

        if isinstance(resources, dict):
            resources = [resources]

        transformations = TransformationWriteList([])

        for resource in resources:
            invalid_parameters: dict[str, str] = {}
            if "action" in resource and "conflictMode" not in resource:
                invalid_parameters["action"] = "conflictMode"
            if "shared" in resource and "isPublic" not in resource:
                invalid_parameters["shared"] = "isPublic"
            if invalid_parameters:
                raise ToolkitInvalidParameterNameError(
                    "Parameters invalid. These are specific for the "
                    "'transformation-cli' and not supported by cognite-toolkit",
                    resource.get("externalId", "<Missing>"),
                    invalid_parameters,
                )

            if resource.get("dataSetExternalId") is not None:
                ds_external_id = resource.pop("dataSetExternalId")
                resource["dataSetId"] = ToolGlobals.verify_dataset(
                    ds_external_id, skip_validation, action="replace dataSetExternalId with dataSetId in transformation"
                )
            if resource.get("conflictMode") is None:
                # Todo; Bug SDK missing default value
                resource["conflictMode"] = "upsert"

            source_oidc_credentials = (
                resource.get("authentication", {}).get("read") or resource.get("authentication") or None
            )
            destination_oidc_credentials = (
                resource.get("authentication", {}).get("write") or resource.get("authentication") or None
            )
            transformation = TransformationWrite.load(resource)
            try:
                transformation.source_oidc_credentials = source_oidc_credentials and OidcCredentials.load(
                    source_oidc_credentials
                )

                transformation.destination_oidc_credentials = destination_oidc_credentials and OidcCredentials.load(
                    destination_oidc_credentials
                )
            except KeyError as e:
                raise ToolkitYAMLFormatError("authentication property is missing required fields", filepath, e)

            query_file = self._get_query_file(filepath, transformation.external_id)

            if transformation.query is None:
                if query_file is None:
                    raise ToolkitYAMLFormatError(
                        f"query property or is missing. It can be inline or a separate file named {filepath.stem}.sql or {transformation.external_id}.sql",
                        filepath,
                    )
                transformation.query = query_file.read_text()
            elif transformation.query is not None and query_file is not None:
                raise ToolkitYAMLFormatError(
                    f"query property is ambiguously defined in both the yaml file and a separate file named {query_file}\n"
                    f"Please remove one of the definitions, either the query property in {filepath} or the file {query_file}",
                )

            transformations.append(transformation)

        if len(transformations) == 1:
            return transformations[0]
        else:
            return transformations

    def dump_resource(
        self, resource: TransformationWrite, source_file: Path, local_resource: TransformationWrite
    ) -> tuple[dict[str, Any], dict[Path, str]]:
        dumped = resource.dump()
        query = dumped.pop("query")
        dumped.pop("dataSetId", None)
        dumped.pop("sourceOidcCredentials", None)
        dumped.pop("destinationOidcCredentials", None)
        return dumped, {source_file.parent / f"{source_file.stem}.sql": query}

    def create(self, items: Sequence[TransformationWrite]) -> TransformationList:
        return self.client.transformations.create(items)

    def retrieve(self, ids: SequenceNotStr[str]) -> TransformationList:
        return self.client.transformations.retrieve_multiple(external_ids=ids, ignore_unknown_ids=True)

    def update(self, items: TransformationWriteList) -> TransformationList:
        return self.client.transformations.update(items)

    def delete(self, ids: SequenceNotStr[str]) -> int:
        existing = self.retrieve(ids).as_external_ids()
        if existing:
            self.client.transformations.delete(external_id=existing, ignore_unknown_ids=True)
        return len(existing)

    def iterate(self) -> Iterable[Transformation]:
        return iter(self.client.transformations)

    @classmethod
    @lru_cache(maxsize=1)
    def get_write_cls_parameter_spec(cls) -> ParameterSpecSet:
        spec = super().get_write_cls_parameter_spec()
        spec.update(
            ParameterSpecSet(
                {
                    # Added by toolkit
                    ParameterSpec(("dataSetExternalId",), frozenset({"str"}), is_required=False, _is_nullable=False),
                    ParameterSpec(("authentication",), frozenset({"dict"}), is_required=False, _is_nullable=False),
                    ParameterSpec(
                        ("authentication", "clientId"), frozenset({"str"}), is_required=True, _is_nullable=False
                    ),
                    ParameterSpec(
                        ("authentication", "clientSecret"), frozenset({"str"}), is_required=True, _is_nullable=False
                    ),
                    ParameterSpec(
                        ("authentication", "scopes"), frozenset({"str"}), is_required=False, _is_nullable=False
                    ),
                    ParameterSpec(
                        ("authentication", "scopes", ANY_INT), frozenset({"str"}), is_required=False, _is_nullable=False
                    ),
                    ParameterSpec(
                        ("authentication", "tokenUri"), frozenset({"str"}), is_required=True, _is_nullable=False
                    ),
                    ParameterSpec(
                        ("authentication", "cdfProjectName"), frozenset({"str"}), is_required=True, _is_nullable=False
                    ),
                    ParameterSpec(
                        ("authentication", "audience"), frozenset({"str"}), is_required=False, _is_nullable=False
                    ),
                }
            )
        )
        return spec


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
    folder_name = "transformations"
    # Matches all yaml files whose stem contains *schedule or *TransformationSchedule.
    filename_pattern = r"^.*schedule$"
    resource_cls = TransformationSchedule
    resource_write_cls = TransformationScheduleWrite
    list_cls = TransformationScheduleList
    list_write_cls = TransformationScheduleWriteList
    kind = "Schedule"
    dependencies = frozenset({TransformationLoader})
    _doc_url = "Transformation-Schedules/operation/createTransformationSchedules"

    @property
    def display_name(self) -> str:
        return "transformation.schedules"

    @classmethod
    def get_required_capability(cls, items: TransformationScheduleWriteList) -> list[Capability]:
        # Access for transformations schedules is checked by the transformation that is deployed
        # first, so we don't need to check for any capabilities here.
        return []

    @classmethod
    def get_id(cls, item: TransformationSchedule | TransformationScheduleWrite | dict) -> str:
        if isinstance(item, dict):
            return item["externalId"]
        if item.external_id is None:
            raise ToolkitRequiredValueError("TransformationSchedule must have external_id set.")
        return item.external_id

    @classmethod
    def get_dependent_items(cls, item: dict) -> Iterable[tuple[type[ResourceLoader], Hashable]]:
        if "externalId" in item:
            yield TransformationLoader, item["externalId"]

    def load_resource(
        self, filepath: Path, ToolGlobals: CDFToolConfig, skip_validation: bool
    ) -> TransformationScheduleWrite | TransformationScheduleWriteList | None:
        raw = load_yaml_inject_variables(filepath, ToolGlobals.environment_variables())
        if isinstance(raw, dict):
            return TransformationScheduleWrite.load(raw)
        else:
            return TransformationScheduleWriteList.load(raw)

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

    def retrieve(self, ids: SequenceNotStr[str]) -> TransformationScheduleList:
        return self.client.transformations.schedules.retrieve_multiple(external_ids=ids, ignore_unknown_ids=True)

    def update(self, items: TransformationScheduleWriteList) -> TransformationScheduleList:
        return self.client.transformations.schedules.update(items)

    def delete(self, ids: str | SequenceNotStr[str] | None) -> int:
        try:
            self.client.transformations.schedules.delete(
                external_id=cast(SequenceNotStr[str], ids), ignore_unknown_ids=False
            )
            return len(cast(SequenceNotStr[str], ids))
        except CogniteNotFoundError as e:
            return len(cast(SequenceNotStr[str], ids)) - len(e.not_found)

    def iterate(self) -> Iterable[TransformationSchedule]:
        return iter(self.client.transformations.schedules)


@final
class TransformationNotificationLoader(
    ResourceLoader[
        str,
        TransformationNotificationWrite,
        TransformationNotification,
        TransformationNotificationWriteList,
        TransformationNotificationList,
    ]
):
    folder_name = "transformations"
    # Matches all yaml files whose stem ends with *Notification.
    filename_pattern = r"^.*Notification$"
    resource_cls = TransformationNotification
    resource_write_cls = TransformationNotificationWrite
    list_cls = TransformationNotificationList
    list_write_cls = TransformationNotificationWriteList
    kind = "Notification"
    dependencies = frozenset({TransformationLoader})
    _doc_url = "Transformation-Notifications/operation/createTransformationNotifications"
    _split_character = "@@@"

    @property
    def display_name(self) -> str:
        return "transformation.notifications"

    @classmethod
    def get_id(cls, item: TransformationNotification | TransformationNotificationWrite | dict) -> str:
        if isinstance(item, dict):
            if missing := tuple(k for k in {"transformationExternalId", "destination"} if k not in item):
                # We need to raise a KeyError with all missing keys to get the correct error message.
                raise KeyError(*missing)
            return f"{item['transformationExternalId']}{cls._split_character}{item['destination']}"

        return f"{item.transformation_external_id}{cls._split_character}{item.destination}"

    @classmethod
    def get_required_capability(cls, items: TransformationNotificationWriteList) -> Capability | list[Capability]:
        # Access for transformation notification is checked by the transformation that is deployed
        # first, so we don't need to check for any capabilities here.
        return []

    def create(self, items: TransformationNotificationWriteList) -> TransformationNotificationList:
        return self.client.transformations.notifications.create(items)  # type: ignore[return-value]

    def retrieve(self, ids: SequenceNotStr[str]) -> TransformationNotificationList:
        retrieved = TransformationNotificationList([])
        for id_ in ids:
            try:
                transformation_external_id, destination = id_.rsplit(self._split_character, maxsplit=1)
            except ValueError:
                # This should never happen, and is a bug in the toolkit if it occurs. Creating a nice error message
                # here so that if it does happen, it will be easier to debug.
                raise ValueError(
                    f"Invalid externalId: {id_}. Must be in the format 'transformationExternalId{self._split_character}destination'"
                )
            try:
                result = self.client.transformations.notifications.list(
                    transformation_external_id=transformation_external_id, destination=destination, limit=-1
                )
            except CogniteAPIError:
                # The notification endpoint gives a 500 if the notification does not exist.
                # The issue has been reported to the service team.
                continue
            retrieved.extend(result)
        return retrieved

    def update(self, items: TransformationNotificationWriteList) -> TransformationNotificationList:
        # Note that since a notification is identified by the combination of transformationExternalId and destination,
        # which is the entire object, an update should never happen. However, implementing just in case.
        item_by_id = {self.get_id(item): item for item in items}
        existing = self.retrieve(list(item_by_id.keys()))
        exiting_by_id = {self.get_id(item): item for item in existing}
        create: list[TransformationNotificationWrite] = []
        unchanged: list[str] = []
        delete: list[int] = []
        for id_, item in item_by_id.items():
            existing_item = exiting_by_id.get(id_)
            if existing_item and self._are_equal(item, existing_item):
                unchanged.append(self.get_id(existing_item))
            else:
                create.append(item)
            if existing_item:
                delete.append(cast(int, existing_item.id))
        if delete:
            self.client.transformations.notifications.delete(delete)
        updated_by_id: dict[str, TransformationNotification] = {}
        if create:
            # Bug in SDK
            created = self.client.transformations.notifications.create(create)
            updated_by_id.update({self.get_id(item): item for item in created})
        if unchanged:
            updated_by_id.update({id_: exiting_by_id[id_] for id_ in unchanged})
        return TransformationNotificationList([updated_by_id[id_] for id_ in item_by_id.keys()])

    def delete(self, ids: SequenceNotStr[str]) -> int:
        # Note that it is theoretically possible that more items will be deleted than
        # input ids. This is because TransformationNotifications are identified by an internal id,
        # while the toolkit uses the transformationExternalId + destination as the id. Thus, there could
        # be multiple notifications for the same transformationExternalId + destination.
        if existing := self.retrieve(ids):
            self.client.transformations.notifications.delete([item.id for item in existing])  # type: ignore[misc]
        return len(existing)

    def iterate(self) -> Iterable[TransformationNotification]:
        return iter(self.client.transformations.notifications)

    @classmethod
    def get_dependent_items(cls, item: dict) -> Iterable[tuple[type[ResourceLoader], Hashable]]:
        """Returns all items that this item requires.

        For example, a TimeSeries requires a DataSet, so this method would return the
        DatasetLoader and identifier of that dataset.
        """
        if "transformationExternalId" in item:
            yield TransformationLoader, item["transformationExternalId"]


@final
class ExtractionPipelineLoader(
    ResourceLoader[
        str, ExtractionPipelineWrite, ExtractionPipeline, ExtractionPipelineWriteList, ExtractionPipelineList
    ]
):
    folder_name = "extraction_pipelines"
    filename_pattern = r"^(?:(?!\.config).)*$"  # Matches all yaml files except file names who's stem contain *.config.
    resource_cls = ExtractionPipeline
    resource_write_cls = ExtractionPipelineWrite
    list_cls = ExtractionPipelineList
    list_write_cls = ExtractionPipelineWriteList
    kind = "ExtractionPipeline"
    dependencies = frozenset({DataSetsLoader, RawDatabaseLoader, RawTableLoader, GroupAllScopedLoader})
    _doc_url = "Extraction-Pipelines/operation/createExtPipes"

    @classmethod
    def get_required_capability(cls, items: ExtractionPipelineWriteList) -> Capability | list[Capability]:
        if not items:
            return []
        data_set_id = {item.data_set_id for item in items if item.data_set_id}

        scope = (
            ExtractionPipelinesAcl.Scope.DataSet(list(data_set_id))
            if data_set_id
            else ExtractionPipelinesAcl.Scope.All()
        )

        return ExtractionPipelinesAcl(
            [ExtractionPipelinesAcl.Action.Read, ExtractionPipelinesAcl.Action.Write],
            scope,  # type: ignore[arg-type]
        )

    @classmethod
    def get_id(cls, item: ExtractionPipeline | ExtractionPipelineWrite | dict) -> str:
        if isinstance(item, dict):
            return item["externalId"]
        if item.external_id is None:
            raise ToolkitRequiredValueError("ExtractionPipeline must have external_id set.")
        return item.external_id

    @classmethod
    def get_dependent_items(cls, item: dict) -> Iterable[tuple[type[ResourceLoader], Hashable]]:
        seen_databases: set[str] = set()
        if "dataSetExternalId" in item:
            yield DataSetsLoader, item["dataSetExternalId"]
        if "rawTables" in item:
            for entry in item["rawTables"]:
                if db := entry.get("dbName"):
                    if db not in seen_databases:
                        seen_databases.add(db)
                        yield RawDatabaseLoader, RawDatabaseTable(db_name=db)
                    if "tableName" in entry:
                        yield RawTableLoader, RawDatabaseTable._load(entry)

    @classmethod
    def check_identifier_semantics(cls, identifier: str, filepath: Path, verbose: bool) -> WarningList[YAMLFileWarning]:
        warning_list = WarningList[YAMLFileWarning]()
        parts = identifier.split("_")
        if len(parts) < 2:
            warning_list.append(
                NamespacingConventionWarning(
                    filepath,
                    "extraction pipeline",
                    "externalId",
                    identifier,
                    "_",
                )
            )
        elif not identifier.startswith("ep_"):
            warning_list.append(
                PrefixConventionWarning(
                    filepath,
                    "extraction pipeline",
                    "externalId",
                    identifier,
                    "ep_",
                )
            )
        return warning_list

    def load_resource(
        self, filepath: Path, ToolGlobals: CDFToolConfig, skip_validation: bool
    ) -> ExtractionPipelineWrite | ExtractionPipelineWriteList:
        resources = load_yaml_inject_variables(filepath, {})
        if isinstance(resources, dict):
            resources = [resources]

        for resource in resources:
            if "dataSetExternalId" in resource:
                ds_external_id = resource.pop("dataSetExternalId")
                resource["dataSetId"] = ToolGlobals.verify_dataset(
                    ds_external_id,
                    skip_validation,
                    action="replace datasetExternalId with dataSetId in extraction pipeline",
                )
            if "createdBy" not in resource:
                # Todo; Bug SDK missing default value (this will be set on the server-side if missing)
                resource["createdBy"] = "unknown"

        if len(resources) == 1:
            return ExtractionPipelineWrite.load(resources[0])
        else:
            return ExtractionPipelineWriteList.load(resources)

    def _are_equal(
        self, local: ExtractionPipelineWrite, cdf_resource: ExtractionPipeline, return_dumped: bool = False
    ) -> bool | tuple[bool, dict[str, Any], dict[str, Any]]:
        local_dumped = local.dump()
        cdf_dumped = cdf_resource.as_write().dump()
        if local_dumped.get("dataSetId") == -1 and "dataSetId" in cdf_dumped:
            # Dry run
            local_dumped["dataSetId"] = cdf_dumped["dataSetId"]
        return self._return_are_equal(local_dumped, cdf_dumped, return_dumped)

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

    def retrieve(self, ids: SequenceNotStr[str]) -> ExtractionPipelineList:
        return self.client.extraction_pipelines.retrieve_multiple(external_ids=ids, ignore_unknown_ids=True)

    def update(self, items: ExtractionPipelineWriteList) -> ExtractionPipelineList:
        return self.client.extraction_pipelines.update(items)

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

    def iterate(self) -> Iterable[ExtractionPipeline]:
        return iter(self.client.extraction_pipelines)

    @classmethod
    @lru_cache(maxsize=1)
    def get_write_cls_parameter_spec(cls) -> ParameterSpecSet:
        spec = super().get_write_cls_parameter_spec()
        # Added by toolkit
        spec.add(ParameterSpec(("dataSetExternalId",), frozenset({"str"}), is_required=False, _is_nullable=False))
        # Set on deploy time by toolkit
        spec.discard(ParameterSpec(("dataSetId",), frozenset({"int"}), is_required=True, _is_nullable=False))
        return spec


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
    folder_name = "extraction_pipelines"
    filename_pattern = r"^.*config$"
    resource_cls = ExtractionPipelineConfig
    resource_write_cls = ExtractionPipelineConfigWrite
    list_cls = ExtractionPipelineConfigList
    list_write_cls = ExtractionPipelineConfigWriteList
    kind = "Config"
    dependencies = frozenset({ExtractionPipelineLoader})
    _doc_url = "Extraction-Pipelines-Config/operation/createExtPipeConfig"

    @property
    def display_name(self) -> str:
        return "extraction_pipeline.config"

    @classmethod
    def get_required_capability(cls, items: ExtractionPipelineConfigWriteList) -> list[Capability]:
        # Access for extraction pipeline configs is checked by the extraction pipeline that is deployed
        # first, so we don't need to check for any capabilities here.
        return []

    @classmethod
    def get_id(cls, item: ExtractionPipelineConfig | ExtractionPipelineConfigWrite | dict) -> str:
        if isinstance(item, dict):
            return item["externalId"]
        if item.external_id is None:
            raise ToolkitRequiredValueError("ExtractionPipelineConfig must have external_id set.")
        return item.external_id

    @classmethod
    def get_dependent_items(cls, item: dict) -> Iterable[tuple[type[ResourceLoader], Hashable]]:
        if "externalId" in item:
            yield ExtractionPipelineLoader, item["externalId"]

    def load_resource(
        self, filepath: Path, ToolGlobals: CDFToolConfig, skip_validation: bool
    ) -> ExtractionPipelineConfigWrite | ExtractionPipelineConfigWriteList:
        resources = load_yaml_inject_variables(filepath, {})
        if isinstance(resources, dict):
            resources = [resources]

        for resource in resources:
            config_raw = resource.get("config")
            if isinstance(config_raw, (dict, list)):
                try:
                    resource["config"] = yaml.safe_dump(config_raw, indent=4)
                except yaml.YAMLError as e:
                    print(
                        HighSeverityWarning(
                            f"Configuration for {resource.get('external_id', 'missing')} could not be parsed "
                            f"as valid YAML, which is the recommended format. Error: {e}"
                        ).get_message()
                    )
        if len(resources) == 1:
            return ExtractionPipelineConfigWrite.load(resources[0])
        else:
            return ExtractionPipelineConfigWriteList.load(resources)

    def _upsert(self, items: ExtractionPipelineConfigWriteList) -> ExtractionPipelineConfigList:
        updated = ExtractionPipelineConfigList([])
        for item in items:
            if not item.external_id:
                raise ToolkitRequiredValueError("ExtractionPipelineConfig must have external_id set.")
            try:
                latest = self.client.extraction_pipelines.config.retrieve(item.external_id)
            except CogniteAPIError:
                latest = None
            if latest and self._are_equal(item, latest):
                updated.append(latest)
                continue
            else:
                created = self.client.extraction_pipelines.config.create(item)
                updated.append(created)
        return updated

    def create(self, items: ExtractionPipelineConfigWriteList) -> ExtractionPipelineConfigList:
        return self._upsert(items)

    # configs cannot be updated, instead new revision is created
    def update(self, items: ExtractionPipelineConfigWriteList) -> ExtractionPipelineConfigList:
        return self._upsert(items)

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
                if result:
                    count += 1
        return count

    def iterate(self) -> Iterable[ExtractionPipelineConfig]:
        return (
            self.client.extraction_pipelines.config.retrieve(external_id=cast(str, pipeline.external_id))
            for pipeline in self.client.extraction_pipelines
        )

    @classmethod
    @lru_cache(maxsize=1)
    def get_write_cls_parameter_spec(cls) -> ParameterSpecSet:
        spec = super().get_write_cls_parameter_spec()
        # Added by toolkit
        spec.add(ParameterSpec(("config", ANYTHING), frozenset({"dict"}), is_required=True, _is_nullable=False))
        return spec


@final
class FileMetadataLoader(
    ResourceContainerLoader[str, FileMetadataWrite, FileMetadata, FileMetadataWriteList, FileMetadataList]
):
    template_pattern = "$FILENAME"
    item_name = "file contents"
    folder_name = "files"
    resource_cls = FileMetadata
    resource_write_cls = FileMetadataWrite
    list_cls = FileMetadataList
    list_write_cls = FileMetadataWriteList
    kind = "FileMetadata"
    dependencies = frozenset({DataSetsLoader, GroupAllScopedLoader, LabelLoader})

    _doc_url = "Files/operation/initFileUpload"

    @property
    def display_name(self) -> str:
        return "file_metadata"

    @classmethod
    def get_required_capability(cls, items: FileMetadataWriteList) -> Capability | list[Capability]:
        if not items:
            return []
        data_set_ids = {item.data_set_id for item in items if item.data_set_id}

        scope = FilesAcl.Scope.DataSet(list(data_set_ids)) if data_set_ids else FilesAcl.Scope.All()

        return FilesAcl([FilesAcl.Action.Read, FilesAcl.Action.Write], scope)  # type: ignore[arg-type]

    @classmethod
    def get_id(cls, item: FileMetadata | FileMetadataWrite | dict) -> str:
        if isinstance(item, dict):
            return item["externalId"]
        if item.external_id is None:
            raise ToolkitRequiredValueError("FileMetadata must have external_id set.")
        return item.external_id

    @classmethod
    def get_dependent_items(cls, item: dict) -> Iterable[tuple[type[ResourceLoader], Hashable]]:
        if "dataSetExternalId" in item:
            yield DataSetsLoader, item["dataSetExternalId"]
        if "securityCategoryNames" in item:
            for security_category in item["securityCategoryNames"]:
                yield SecurityCategoryLoader, security_category
        if "labels" in item:
            for label in item["labels"]:
                if isinstance(label, dict):
                    yield LabelLoader, label["externalId"]
                elif isinstance(label, str):
                    yield LabelLoader, label

    def load_resource(
        self, filepath: Path, ToolGlobals: CDFToolConfig, skip_validation: bool
    ) -> FileMetadataWrite | FileMetadataWriteList:
        loaded = load_yaml_inject_variables(filepath, ToolGlobals.environment_variables())

        file_to_upload_by_source_name: dict[str, Path] = {
            INDEX_PATTERN.sub("", file.name): file
            for file in filepath.parent.glob("*")
            if file.suffix not in {".yaml", ".yml"}
        }

        is_file_template = (
            isinstance(loaded, list) and len(loaded) == 1 and "$FILENAME" in loaded[0].get("externalId", "")
        )
        if isinstance(loaded, list) and is_file_template:
            print(f"  [bold green]INFO:[/] File pattern detected in {filepath.name}, expanding to all files in folder.")
            template = loaded[0]
            template_prefix, template_suffix = "", ""
            if "name" in template and "$FILENAME" in template["name"]:
                template_prefix, template_suffix = template["name"].split("$FILENAME", maxsplit=1)
            loaded_list: list[dict[str, Any]] = []
            for source_name, file in file_to_upload_by_source_name.items():
                # Deep Copy
                new_file = json.loads(json.dumps(template))

                # We modify the filename in the build command, we clean the name here to get the original filename
                filename_in_module = source_name.removeprefix(template_prefix).removesuffix(template_suffix)
                new_file["name"] = source_name
                new_file["externalId"] = new_file["externalId"].replace("$FILENAME", filename_in_module)
                loaded_list.append(new_file)

        elif isinstance(loaded, dict):
            loaded_list = [loaded]
        else:
            # Is List
            loaded_list = loaded

        for resource in loaded_list:
            if resource.get("dataSetExternalId") is not None:
                ds_external_id = resource.pop("dataSetExternalId")
                resource["dataSetId"] = ToolGlobals.verify_dataset(
                    ds_external_id, skip_validation, action="replace dataSetExternalId with dataSetId in file metadata"
                )
            if security_categories_names := resource.pop("securityCategoryNames", []):
                security_categories = ToolGlobals.verify_security_categories(
                    security_categories_names,
                    skip_validation,
                    action="replace securityCategoryNames with securityCategoriesIDs in file metadata",
                )
                resource["securityCategories"] = security_categories

        files_metadata: FileMetadataWriteList = FileMetadataWriteList.load(loaded_list)
        for meta in files_metadata:
            if meta.name and meta.name not in file_to_upload_by_source_name:
                raise ToolkitFileNotFoundError(f"Could not find file {meta.name} referenced in filepath {filepath}")
        return files_metadata

    def _are_equal(
        self, local: FileMetadataWrite, cdf_resource: FileMetadata, return_dumped: bool = False
    ) -> bool | tuple[bool, dict[str, Any], dict[str, Any]]:
        local_dumped = local.dump()
        cdf_dumped = cdf_resource.as_write().dump()
        # Dry run mode
        if local_dumped.get("dataSetId") == -1 and "dataSetId" in cdf_dumped:
            local_dumped["dataSetId"] = cdf_dumped["dataSetId"]
        if (
            all(s == -1 for s in local_dumped.get("securityCategories", []))
            and "securityCategories" in cdf_dumped
            and len(cdf_dumped["securityCategories"]) == len(local_dumped.get("securityCategories", []))
        ):
            local_dumped["securityCategories"] = cdf_dumped["securityCategories"]
        if (
            all(a == -1 for a in local_dumped.get("assetIds", []))
            and "assetIds" in cdf_dumped
            and len(cdf_dumped["assetIds"]) == len(local_dumped.get("assetIds", []))
        ):
            local_dumped["assetIds"] = cdf_dumped["assetIds"]

        return self._return_are_equal(local_dumped, cdf_dumped, return_dumped)

    def create(self, items: FileMetadataWriteList) -> FileMetadataList:
        created = FileMetadataList([])
        for meta in items:
            try:
                created.append(self.client.files.create(meta))
            except CogniteAPIError as e:
                if e.code == 409:
                    print(f"  [bold yellow]WARNING:[/] File {meta.external_id} already exists, skipping upload.")
        return created

    def retrieve(self, ids: SequenceNotStr[str]) -> FileMetadataList:
        return self.client.files.retrieve_multiple(external_ids=ids, ignore_unknown_ids=True)

    def update(self, items: FileMetadataWriteList) -> FileMetadataList:
        return self.client.files.update(items)

    def delete(self, ids: str | SequenceNotStr[str] | None) -> int:
        self.client.files.delete(external_id=cast(SequenceNotStr[str], ids))
        return len(cast(SequenceNotStr[str], ids))

    def iterate(self) -> Iterable[FileMetadata]:
        return iter(self.client.files)

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

    @classmethod
    @lru_cache(maxsize=1)
    def get_write_cls_parameter_spec(cls) -> ParameterSpecSet:
        spec = super().get_write_cls_parameter_spec()
        # Added by toolkit
        spec.add(ParameterSpec(("dataSetExternalId",), frozenset({"str"}), is_required=False, _is_nullable=False))
        spec.add(ParameterSpec(("securityCategoryNames",), frozenset({"list"}), is_required=False, _is_nullable=False))

        spec.add(
            ParameterSpec(("securityCategoryNames", ANY_STR), frozenset({"str"}), is_required=False, _is_nullable=False)
        )

        return spec


@final
class SpaceLoader(ResourceContainerLoader[str, SpaceApply, Space, SpaceApplyList, SpaceList]):
    item_name = "nodes and edges"
    folder_name = "data_models"
    filename_pattern = r"^.*space$"
    resource_cls = Space
    resource_write_cls = SpaceApply
    list_write_cls = SpaceApplyList
    list_cls = SpaceList
    kind = "Space"
    dependencies = frozenset({GroupAllScopedLoader})
    _doc_url = "Spaces/operation/ApplySpaces"

    @property
    def display_name(self) -> str:
        return "spaces"

    @classmethod
    def get_required_capability(cls, items: SpaceApplyList) -> list[Capability] | list[Capability]:
        if not items:
            return []
        return [
            DataModelsAcl(
                [DataModelsAcl.Action.Read, DataModelsAcl.Action.Write],
                DataModelsAcl.Scope.All(),
            ),
        ]

    @classmethod
    def get_id(cls, item: SpaceApply | Space | dict) -> str:
        if isinstance(item, dict):
            return item["space"]
        return item.space

    @classmethod
    def check_identifier_semantics(cls, identifier: str, filepath: Path, verbose: bool) -> WarningList[YAMLFileWarning]:
        warning_list = WarningList[YAMLFileWarning]()

        parts = identifier.split("_")
        if len(parts) < 2:
            warning_list.append(
                NamespacingConventionWarning(
                    filepath,
                    "space",
                    "space",
                    identifier,
                    "_",
                )
            )
        elif not identifier.startswith("sp_"):
            if identifier in {"cognite_app_data", "APM_SourceData", "APM_Config"}:
                if verbose:
                    print(
                        f"      [bold green]INFO:[/] the space {identifier} does not follow the recommended '_' based "
                        "namespacing because Infield expects this specific name."
                    )
            else:
                warning_list.append(PrefixConventionWarning(filepath, "space", "space", identifier, "sp_"))
        return warning_list

    def create(self, items: Sequence[SpaceApply]) -> SpaceList:
        return self.client.data_modeling.spaces.apply(items)

    def retrieve(self, ids: SequenceNotStr[str]) -> SpaceList:
        return self.client.data_modeling.spaces.retrieve(ids)

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

    def iterate(self) -> Iterable[Space]:
        return iter(self.client.data_modeling.spaces)

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
    folder_name = "data_models"
    filename_pattern = r"^.*container$"
    resource_cls = Container
    resource_write_cls = ContainerApply
    list_cls = ContainerList
    list_write_cls = ContainerApplyList
    kind = "Container"
    dependencies = frozenset({SpaceLoader})
    _doc_url = "Containers/operation/ApplyContainers"

    @property
    def display_name(self) -> str:
        return "containers"

    @classmethod
    def get_required_capability(cls, items: ContainerApplyList) -> Capability | list[Capability]:
        if not items:
            return []
        return DataModelsAcl(
            [DataModelsAcl.Action.Read, DataModelsAcl.Action.Write],
            DataModelsAcl.Scope.SpaceID(list({item.space for item in items})),
        )

    @classmethod
    def get_id(cls, item: ContainerApply | Container | dict) -> ContainerId:
        if isinstance(item, dict):
            if missing := tuple(k for k in {"space", "externalId"} if k not in item):
                # We need to raise a KeyError with all missing keys to get the correct error message.
                raise KeyError(*missing)
            return ContainerId(space=item["space"], external_id=item["externalId"])
        return item.as_id()

    @classmethod
    def get_dependent_items(cls, item: dict) -> Iterable[tuple[type[ResourceLoader], Hashable]]:
        if "space" in item:
            yield SpaceLoader, item["space"]

    def load_resource(
        self, filepath: Path, ToolGlobals: CDFToolConfig, skip_validation: bool
    ) -> ContainerApply | ContainerApplyList | None:
        raw_yaml = load_yaml_inject_variables(filepath, ToolGlobals.environment_variables())
        dict_items = raw_yaml if isinstance(raw_yaml, list) else [raw_yaml]
        for raw_instance in dict_items:
            for prop in raw_instance.get("properties", {}).values():
                type_ = prop.get("type", {})
                if "list" not in type_:
                    # In the Python-SDK, list property of a container.properties.<property>.type.list is required.
                    # This is not the case in the API, so we need to set it here. (This is due to the PropertyType class
                    # is used as read and write in the SDK, and the read class has it required while the write class does not)
                    type_["list"] = False
                # Todo Bug in SDK, not setting defaults on load
                if "nullable" not in prop:
                    prop["nullable"] = False
                if "autoIncrement" not in prop:
                    prop["autoIncrement"] = False

        return ContainerApplyList.load(dict_items)

    def create(self, items: Sequence[ContainerApply]) -> ContainerList:
        return self.client.data_modeling.containers.apply(items)

    def retrieve(self, ids: SequenceNotStr[ContainerId]) -> ContainerList:
        return self.client.data_modeling.containers.retrieve(cast(Sequence, ids))

    def update(self, items: Sequence[ContainerApply]) -> ContainerList:
        return self.create(items)

    def delete(self, ids: SequenceNotStr[ContainerId]) -> int:
        deleted = self.client.data_modeling.containers.delete(cast(Sequence, ids))
        return len(deleted)

    def iterate(self) -> Iterable[Container]:
        return iter(self.client.data_modeling.containers)

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
        for container_id_chunk in self._chunker(container_ids, _HAS_DATA_FILTER_LIMIT):
            is_container = filters.HasData(containers=container_id_chunk)
            for instances in self.client.data_modeling.instances(
                chunk_size=1000, instance_type="node", filter=is_container, limit=-1
            ):
                yield instances.as_ids()

    def _iterate_over_edges(self, containers: ContainerList) -> Iterable[list[EdgeId]]:
        container_ids = [container.as_id() for container in containers if container.used_for in ["edge", "all"]]
        if not container_ids:
            return

        for container_id_chunk in self._chunker(container_ids, _HAS_DATA_FILTER_LIMIT):
            is_container = filters.HasData(containers=container_id_chunk)
            for instances in self.client.data_modeling.instances(
                chunk_size=1000, instance_type="edge", limit=-1, filter=is_container
            ):
                yield instances.as_ids()

    @staticmethod
    def _chunker(seq: Sequence, size: int) -> Iterable[Sequence]:
        return (seq[pos : pos + size] for pos in range(0, len(seq), size))

    def _are_equal(
        self, local: ContainerApply, remote: Container, return_dumped: bool = False
    ) -> bool | tuple[bool, dict[str, Any], dict[str, Any]]:
        local_dumped = local.dump(camel_case=True)
        # 'usedFor' and 'cursorable' have default values set on the server side,
        # but not when loading the container using the SDK. Thus, we set the default
        # values here if they are not present.
        if "usedFor" not in local_dumped:
            local_dumped["usedFor"] = "node"
        for index in local_dumped.get("indexes", {}).values():
            if "cursorable" not in index:
                index["cursorable"] = False

        cdf_dumped = remote.as_write().dump(camel_case=True)

        return self._return_are_equal(local_dumped, cdf_dumped, return_dumped)

    @classmethod
    @lru_cache(maxsize=1)
    def get_write_cls_parameter_spec(cls) -> ParameterSpecSet:
        output = super().get_write_cls_parameter_spec()
        # In the SDK this is called isList, while in the API it is called list.
        output.discard(
            ParameterSpec(
                ("properties", ANY_STR, "type", "isList"), frozenset({"bool"}), is_required=True, _is_nullable=False
            )
        )
        output.add(
            ParameterSpec(
                ("properties", ANY_STR, "type", "list"), frozenset({"bool"}), is_required=True, _is_nullable=False
            )
        )
        # The parameters below are used by the SDK to load the correct class, and ase thus not part of the init
        # that the spec is created from, so we need to add them manually.
        output.update(
            ParameterSpecSet(
                {
                    ParameterSpec(
                        ("properties", ANY_STR, "type", "type"),
                        frozenset({"str"}),
                        is_required=True,
                        _is_nullable=False,
                    ),
                    ParameterSpec(
                        # direct relations with constraint
                        ("properties", ANY_STR, "type", "container", "type"),
                        frozenset({"str"}),
                        is_required=True,
                        _is_nullable=False,
                    ),
                    ParameterSpec(
                        ("constraints", ANY_STR, "constraintType"),
                        frozenset({"str"}),
                        is_required=True,
                        _is_nullable=False,
                    ),
                    ParameterSpec(
                        ("constraints", ANY_STR, "require", "type"),
                        frozenset({"str"}),
                        is_required=True,
                        _is_nullable=False,
                    ),
                    ParameterSpec(
                        ("indexes", ANY_STR, "indexType"), frozenset({"str"}), is_required=True, _is_nullable=False
                    ),
                }
            )
        )
        return output


class ViewLoader(ResourceLoader[ViewId, ViewApply, View, ViewApplyList, ViewList]):
    folder_name = "data_models"
    filename_pattern = r"^.*view$"
    resource_cls = View
    resource_write_cls = ViewApply
    list_cls = ViewList
    list_write_cls = ViewApplyList
    kind = "View"
    dependencies = frozenset({SpaceLoader, ContainerLoader})
    _doc_url = "Views/operation/ApplyViews"

    def __init__(self, client: ToolkitClient, build_dir: Path) -> None:
        super().__init__(client, build_dir)
        # Caching to avoid multiple lookups on the same interfaces.
        self._interfaces_by_id: dict[ViewId, View] = {}

    @property
    def display_name(self) -> str:
        return "views"

    @classmethod
    def get_required_capability(cls, items: ViewApplyList) -> Capability | list[Capability]:
        if not items:
            return []
        return DataModelsAcl(
            [DataModelsAcl.Action.Read, DataModelsAcl.Action.Write],
            DataModelsAcl.Scope.SpaceID(list({item.space for item in items})),
        )

    @classmethod
    def get_id(cls, item: ViewApply | View | dict) -> ViewId:
        if isinstance(item, dict):
            if missing := tuple(k for k in {"space", "externalId", "version"} if k not in item):
                # We need to raise a KeyError with all missing keys to get the correct error message.
                raise KeyError(*missing)
            return ViewId(space=item["space"], external_id=item["externalId"], version=item["version"])
        return item.as_id()

    @classmethod
    def get_dependent_items(cls, item: dict) -> Iterable[tuple[type[ResourceLoader], Hashable]]:
        if "space" in item:
            yield SpaceLoader, item["space"]
        for prop in item.get("properties", {}).values():
            if (container := prop.get("container", {})) and container.get("type") == "container":
                if _in_dict(("space", "externalId"), container):
                    yield ContainerLoader, ContainerId(container["space"], container["externalId"])
            for key, dct_ in [("source", prop), ("edgeSource", prop), ("source", prop.get("through", {}))]:
                if source := dct_.get(key, {}):
                    if source.get("type") == "view" and _in_dict(("space", "externalId", "version"), source):
                        yield ViewLoader, ViewId(source["space"], source["externalId"], source["version"])
                    elif source.get("type") == "container" and _in_dict(("space", "externalId"), source):
                        yield ContainerLoader, ContainerId(source["space"], source["externalId"])

    def _are_equal(
        self, local: ViewApply, cdf_resource: View, return_dumped: bool = False
    ) -> bool | tuple[bool, dict[str, Any], dict[str, Any]]:
        local_dumped = local.dump()
        cdf_dumped = cdf_resource.as_write().dump()
        if not cdf_resource.implements:
            return self._return_are_equal(local_dumped, cdf_dumped, return_dumped)

        if cdf_resource.properties:
            # All read version of views have all the properties of their parent views.
            # We need to remove these properties to compare with the local view.
            # Unless the local view has overridden the properties.
            parents = retrieve_view_ancestors(self.client, cdf_resource.implements or [], self._interfaces_by_id)
            cdf_properties = cdf_dumped["properties"]
            for parent in parents:
                for prop_name, parent_prop in (parent.as_write().properties or {}).items():
                    is_overidden = prop_name in cdf_properties and cdf_properties[prop_name] != parent_prop.dump()
                    if is_overidden:
                        continue
                    cdf_properties.pop(prop_name, None)

        if not cdf_dumped["properties"]:
            # All properties were removed, so we remove the properties key.
            cdf_dumped.pop("properties", None)
        if "properties" in local_dumped and not local_dumped["properties"]:
            # In case the local properties are set to an empty dict.
            local_dumped.pop("properties", None)

        return self._return_are_equal(local_dumped, cdf_dumped, return_dumped)

    def create(self, items: Sequence[ViewApply]) -> ViewList:
        return self.client.data_modeling.views.apply(items)

    def retrieve(self, ids: SequenceNotStr[ViewId]) -> ViewList:
        return self.client.data_modeling.views.retrieve(cast(Sequence, ids))

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

    def iterate(self) -> Iterable[View]:
        return iter(self.client.data_modeling.views)

    @classmethod
    @lru_cache(maxsize=1)
    def get_write_cls_parameter_spec(cls) -> ParameterSpecSet:
        spec = super().get_write_cls_parameter_spec()
        # The Filter class in the SDK class View implementation is deviating from the API.
        # So we need to modify the spec to match the API.
        parameter_path = ("filter",)
        length = len(parameter_path)
        for item in spec:
            if len(item.path) >= length + 1 and item.path[:length] == parameter_path[:length]:
                # Add extra ANY_STR layer
                # The spec class is immutable, so we use this trick to modify it.
                is_has_data_filter = item.path[1] in ["containers", "views"]
                if is_has_data_filter:
                    # Special handling of the HasData filter that deviates in SDK implementation from API Spec.
                    object.__setattr__(item, "path", item.path[:length] + (ANY_STR,) + item.path[length + 1 :])
                else:
                    object.__setattr__(item, "path", item.path[:length] + (ANY_STR,) + item.path[length:])

        spec.add(ParameterSpec(("filter", ANY_STR), frozenset({"dict"}), is_required=False, _is_nullable=False))
        # The following types are used by the SDK to load the correct class. They are not part of the init,
        # so we need to add it manually.
        spec.update(
            ParameterSpecSet(
                {
                    ParameterSpec(
                        ("implements", ANY_INT, "type"), frozenset({"str"}), is_required=True, _is_nullable=False
                    ),
                    ParameterSpec(
                        ("properties", ANY_STR, "connectionType"),
                        frozenset({"str"}),
                        is_required=True,
                        _is_nullable=False,
                    ),
                    ParameterSpec(
                        ("properties", ANY_STR, "source", "type"),
                        frozenset({"str"}),
                        is_required=True,
                        _is_nullable=False,
                    ),
                    ParameterSpec(
                        ("properties", ANY_STR, "container", "type"),
                        frozenset({"str"}),
                        is_required=True,
                        _is_nullable=False,
                    ),
                    ParameterSpec(
                        ("properties", ANY_STR, "edgeSource", "type"),
                        frozenset({"str"}),
                        is_required=True,
                        _is_nullable=False,
                    ),
                    ParameterSpec(
                        ("properties", ANY_STR, "through", "source", "type"),
                        frozenset({"str"}),
                        is_required=True,
                        _is_nullable=False,
                    ),
                    ParameterSpec(
                        # In the SDK, this is called "property"
                        ("properties", ANY_STR, "through", "identifier"),
                        frozenset({"str"}),
                        is_required=True,
                        _is_nullable=False,
                    ),
                    # Filters are complex, so we do not attempt to give any more specific spec.
                    ParameterSpec(
                        ("filter", ANYTHING),
                        frozenset({"str"}),
                        is_required=True,
                        _is_nullable=False,
                    ),
                }
            )
        )
        spec.discard(
            ParameterSpec(
                # The API spec calls this "identifier", while the SDK calls it "property".
                ("properties", ANY_STR, "through", "property"),
                frozenset({"str"}),
                is_required=True,
                _is_nullable=False,
            )
        )
        return spec


@final
class DataModelLoader(ResourceLoader[DataModelId, DataModelApply, DataModel, DataModelApplyList, DataModelList]):
    folder_name = "data_models"
    filename_pattern = r"^.*datamodel$"
    resource_cls = DataModel
    resource_write_cls = DataModelApply
    list_cls = DataModelList
    list_write_cls = DataModelApplyList
    kind = "DataModel"
    dependencies = frozenset({SpaceLoader, ViewLoader})
    _doc_url = "Data-models/operation/createDataModels"

    @property
    def display_name(self) -> str:
        return "data models"

    @classmethod
    def get_required_capability(cls, items: DataModelApplyList) -> Capability | list[Capability]:
        if not items:
            return []
        return DataModelsAcl(
            [DataModelsAcl.Action.Read, DataModelsAcl.Action.Write],
            DataModelsAcl.Scope.SpaceID(list({item.space for item in items})),
        )

    @classmethod
    def get_id(cls, item: DataModelApply | DataModel | dict) -> DataModelId:
        if isinstance(item, dict):
            if missing := tuple(k for k in {"space", "externalId", "version"} if k not in item):
                # We need to raise a KeyError with all missing keys to get the correct error message.
                raise KeyError(*missing)
            return DataModelId(space=item["space"], external_id=item["externalId"], version=item["version"])
        return item.as_id()

    @classmethod
    def get_dependent_items(cls, item: dict) -> Iterable[tuple[type[ResourceLoader], Hashable]]:
        if "space" in item:
            yield SpaceLoader, item["space"]
        for view in item.get("views", []):
            if _in_dict(("space", "externalId"), view):
                yield ViewLoader, ViewId(view["space"], view["externalId"], view.get("version"))

    def _are_equal(
        self, local: DataModelApply, cdf_resource: DataModel, return_dumped: bool = False
    ) -> bool | tuple[bool, dict[str, Any], dict[str, Any]]:
        local_dumped = local.dump()
        cdf_dumped = cdf_resource.as_write().dump()

        # Data models that have the same views, but in different order, are considered equal.
        # We also account for whether views are given as IDs or View objects.
        local_dumped["views"] = sorted(
            (v if isinstance(v, ViewId) else v.as_id()).as_tuple() for v in local.views or []
        )
        cdf_dumped["views"] = sorted(
            (v if isinstance(v, ViewId) else v.as_id()).as_tuple() for v in cdf_resource.views or []
        )

        return self._return_are_equal(local_dumped, cdf_dumped, return_dumped)

    def create(self, items: DataModelApplyList) -> DataModelList:
        return self.client.data_modeling.data_models.apply(items)

    def retrieve(self, ids: SequenceNotStr[DataModelId]) -> DataModelList:
        return self.client.data_modeling.data_models.retrieve(cast(Sequence, ids))

    def update(self, items: DataModelApplyList) -> DataModelList:
        return self.create(items)

    def delete(self, ids: SequenceNotStr[DataModelId]) -> int:
        return len(self.client.data_modeling.data_models.delete(cast(Sequence, ids)))

    def iterate(self) -> Iterable[DataModel]:
        return iter(self.client.data_modeling.data_models)

    @classmethod
    @lru_cache(maxsize=1)
    def get_write_cls_parameter_spec(cls) -> ParameterSpecSet:
        spec = super().get_write_cls_parameter_spec()
        # ViewIds have the type set in the API Spec, but this is hidden in the SDK classes,
        # so we need to add it manually.
        spec.add(ParameterSpec(("views", ANY_INT, "type"), frozenset({"str"}), is_required=True, _is_nullable=False))
        return spec


@final
class NodeLoader(ResourceContainerLoader[NodeId, NodeApply, Node, NodeApplyListWithCall, NodeList]):
    item_name = "nodes"
    folder_name = "data_models"
    filename_pattern = r"^.*node$"
    resource_cls = Node
    resource_write_cls = NodeApply
    list_cls = NodeList
    list_write_cls = NodeApplyListWithCall
    kind = "Node"
    dependencies = frozenset({SpaceLoader, ViewLoader, ContainerLoader})
    _doc_url = "Instances/operation/applyNodeAndEdges"

    @property
    def display_name(self) -> str:
        return "nodes"

    @classmethod
    def get_required_capability(cls, items: NodeApplyListWithCall) -> Capability | list[Capability]:
        if not items:
            return []
        return DataModelInstancesAcl(
            [DataModelInstancesAcl.Action.Read, DataModelInstancesAcl.Action.Write],
            DataModelInstancesAcl.Scope.SpaceID(list({item.space for item in items})),
        )

    @classmethod
    def get_id(cls, item: NodeApply | Node | dict) -> NodeId:
        if isinstance(item, dict):
            if missing := tuple(k for k in {"space", "externalId"} if k not in item):
                # We need to raise a KeyError with all missing keys to get the correct error message.
                raise KeyError(*missing)
            return NodeId(space=item["space"], external_id=item["externalId"])
        return item.as_id()

    @classmethod
    def get_dependent_items(cls, item: dict) -> Iterable[tuple[type[ResourceLoader], Hashable]]:
        if "space" in item:
            yield SpaceLoader, item["space"]
        for source in item.get("sources", []):
            if (identifier := source.get("source")) and isinstance(identifier, dict):
                if identifier.get("type") == "view" and _in_dict(("space", "externalId", "version"), identifier):
                    yield ViewLoader, ViewId(identifier["space"], identifier["externalId"], identifier["version"])
                elif identifier.get("type") == "container" and _in_dict(("space", "externalId"), identifier):
                    yield ContainerLoader, ContainerId(identifier["space"], identifier["externalId"])

    @classmethod
    def create_empty_of(cls, items: NodeApplyListWithCall) -> NodeApplyListWithCall:
        return NodeApplyListWithCall([], items.api_call)

    def _are_equal(
        self, local: NodeApply, cdf_resource: Node, return_dumped: bool = False
    ) -> bool | tuple[bool, dict[str, Any], dict[str, Any]]:
        """Comparison for nodes to include properties in the comparison

        Note this is an expensive operation as we to an extra retrieve to fetch the properties.
        Thus, the cdf-tk should not be used to upload nodes that are data only nodes used for configuration.
        """
        local_dumped = local.dump()
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
        except Exception:
            # View does not exist, so node does not exist.
            return self._return_are_equal(local_dumped, {}, return_dumped)
        cdf_dumped = cdf_resource_with_properties.as_write().dump()

        if "existingVersion" not in local_dumped:
            # Existing version is typically not set when creating nodes, but we get it back
            # when we retrieve the node from the server.
            local_dumped["existingVersion"] = cdf_dumped.get("existingVersion", None)

        return self._return_are_equal(local_dumped, cdf_dumped, return_dumped)

    def load_resource(self, filepath: Path, ToolGlobals: CDFToolConfig, skip_validation: bool) -> NodeApplyListWithCall:
        raw = load_yaml_inject_variables(filepath, ToolGlobals.environment_variables())
        return NodeApplyListWithCall._load(raw, cognite_client=self.client)

    def dump_resource(
        self, resource: NodeApply, source_file: Path, local_resource: NodeApply
    ) -> tuple[dict[str, Any], dict[Path, str]]:
        resource_node = resource
        local_node = local_resource
        # Retrieve node again to get properties.
        view_ids = {source.source for source in local_node.sources or [] if isinstance(source.source, ViewId)}
        nodes = self.client.data_modeling.instances.retrieve(nodes=local_node.as_id(), sources=list(view_ids)).nodes
        if not nodes:
            print(
                f"  [bold yellow]WARNING:[/] Node {local_resource.as_id()} does not exist. Failed to fetch properties."
            )
            return resource_node.dump(), {}
        node = nodes[0]
        node_dumped = node.as_write().dump()
        node_dumped.pop("existingVersion", None)

        # Node files have configuration in the first 3 lines, we need to include this in the dumped file.
        dumped = yaml.safe_load("\n".join(source_file.read_text().splitlines()[:3]))

        dumped["nodes"] = [node_dumped]

        return dumped, {}

    def create(self, items: NodeApplyListWithCall) -> NodeApplyResultList:
        if not isinstance(items, NodeApplyListWithCall):
            raise ValueError("Unexpected node format file format")

        api_call_args = items.api_call.dump(camel_case=False) if items.api_call else {}
        result = self.client.data_modeling.instances.apply(nodes=items, **api_call_args)
        return result.nodes

    def retrieve(self, ids: SequenceNotStr[NodeId]) -> NodeList:
        return self.client.data_modeling.instances.retrieve(nodes=cast(Sequence, ids)).nodes

    def update(self, items: NodeApplyListWithCall) -> NodeApplyResultList:
        return self.create(items)

    def delete(self, ids: SequenceNotStr[NodeId]) -> int:
        try:
            deleted = self.client.data_modeling.instances.delete(nodes=cast(Sequence, ids))
        except CogniteAPIError as e:
            if "not exist" in e.message and "space" in e.message.lower():
                return 0
            raise e
        return len(deleted.nodes)

    def iterate(self) -> Iterable[Node]:
        return iter(self.client.data_modeling.instances)

    def count(self, ids: SequenceNotStr[NodeId]) -> int:
        return len(ids)

    def drop_data(self, ids: SequenceNotStr[NodeId]) -> int:
        # Nodes will be deleted in .delete call.
        return 0

    @classmethod
    @lru_cache(maxsize=1)
    def get_write_cls_parameter_spec(cls) -> ParameterSpecSet:
        node_spec = super().get_write_cls_parameter_spec()
        # This is a deviation between the SDK and the API
        node_spec.add(ParameterSpec(("instanceType",), frozenset({"str"}), is_required=False, _is_nullable=False))
        node_spec.add(
            ParameterSpec(
                ("sources", ANY_INT, "source", "type"),
                frozenset({"str"}),
                is_required=True,
                _is_nullable=False,
            )
        )
        return ParameterSpecSet(node_spec, spec_name=cls.__name__)


@final
class WorkflowLoader(ResourceLoader[str, WorkflowUpsert, Workflow, WorkflowUpsertList, WorkflowList]):
    folder_name = "workflows"
    filename_pattern = r"^.*Workflow$"
    resource_cls = Workflow
    resource_write_cls = WorkflowUpsert
    list_cls = WorkflowList
    list_write_cls = WorkflowUpsertList
    kind = "Workflow"
    dependencies = frozenset(
        {
            GroupAllScopedLoader,
            TransformationLoader,
            FunctionLoader,
        }
    )
    _doc_base_url = "https://api-docs.cognite.com/20230101-beta/tag/"
    _doc_url = "Workflows/operation/CreateOrUpdateWorkflow"

    @classmethod
    def get_required_capability(cls, items: WorkflowUpsertList) -> Capability | list[Capability]:
        if not items:
            return []
        return WorkflowOrchestrationAcl(
            [WorkflowOrchestrationAcl.Action.Read, WorkflowOrchestrationAcl.Action.Write],
            WorkflowOrchestrationAcl.Scope.All(),
        )

    @classmethod
    def get_id(cls, item: Workflow | WorkflowUpsert | dict) -> str:
        if isinstance(item, dict):
            return item["externalId"]
        if item.external_id is None:
            raise ToolkitRequiredValueError("Workflow must have external_id set.")
        return item.external_id

    def load_resource(self, filepath: Path, ToolGlobals: CDFToolConfig, skip_validation: bool) -> WorkflowUpsertList:
        resource = load_yaml_inject_variables(filepath, {})

        workflows = [resource] if isinstance(resource, dict) else resource
        return WorkflowUpsertList.load(workflows)

    def retrieve(self, ids: SequenceNotStr[str]) -> WorkflowList:
        workflows = []
        for ext_id in ids:
            workflow = self.client.workflows.retrieve(external_id=ext_id)
            if workflow:
                workflows.append(workflow)
        return WorkflowList(workflows)

    def _upsert(self, items: WorkflowUpsert | WorkflowUpsertList) -> WorkflowList:
        upserts = [items] if isinstance(items, WorkflowUpsert) else items
        return WorkflowList([self.client.workflows.upsert(upsert) for upsert in upserts])

    def create(self, items: WorkflowUpsert | WorkflowUpsertList) -> WorkflowList:
        return self._upsert(items)

    def update(self, items: WorkflowUpsertList) -> WorkflowList:
        return self._upsert(items)

    def delete(self, ids: SequenceNotStr[str]) -> int:
        successes = 0
        for id_ in ids:
            try:
                self.client.workflows.delete(external_id=id_)
            except CogniteNotFoundError:
                print(f"  [bold yellow]WARNING:[/] Workflow {id_} does not exist, skipping delete.")
            else:
                successes += 1
        return successes

    def iterate(self) -> Iterable[Workflow]:
        return self.client.workflows.list(limit=-1)


@final
class WorkflowVersionLoader(
    ResourceLoader[
        WorkflowVersionId, WorkflowVersionUpsert, WorkflowVersion, WorkflowVersionUpsertList, WorkflowVersionList
    ]
):
    folder_name = "workflows"
    filename_pattern = r"^.*WorkflowVersion$"
    resource_cls = WorkflowVersion
    resource_write_cls = WorkflowVersionUpsert
    list_cls = WorkflowVersionList
    list_write_cls = WorkflowVersionUpsertList
    kind = "WorkflowVersion"
    dependencies = frozenset({WorkflowLoader})

    _doc_base_url = "https://api-docs.cognite.com/20230101-beta/tag/"
    _doc_url = "Workflow-versions/operation/CreateOrUpdateWorkflowVersion"

    @property
    def display_name(self) -> str:
        return "workflow.versions"

    @classmethod
    def get_required_capability(cls, items: WorkflowVersionUpsertList) -> Capability | list[Capability]:
        if not items:
            return []
        return WorkflowOrchestrationAcl(
            [WorkflowOrchestrationAcl.Action.Read, WorkflowOrchestrationAcl.Action.Write],
            WorkflowOrchestrationAcl.Scope.All(),
        )

    @classmethod
    def get_id(cls, item: WorkflowVersion | WorkflowVersionUpsert | dict) -> WorkflowVersionId:
        if isinstance(item, dict):
            if missing := tuple(k for k in {"workflowExternalId", "version"} if k not in item):
                # We need to raise a KeyError with all missing keys to get the correct error message.
                raise KeyError(*missing)
            return WorkflowVersionId(item["workflowExternalId"], item["version"])
        return item.as_id()

    @classmethod
    def get_dependent_items(cls, item: dict) -> Iterable[tuple[type[ResourceLoader], Hashable]]:
        if "workflowExternalId" in item:
            yield WorkflowLoader, item["workflowExternalId"]

    def load_resource(
        self, filepath: Path, ToolGlobals: CDFToolConfig, skip_validation: bool
    ) -> WorkflowVersionUpsertList:
        resource = load_yaml_inject_variables(filepath, {})

        workflowversions = [resource] if isinstance(resource, dict) else resource
        return WorkflowVersionUpsertList.load(workflowversions)

    def retrieve(self, ids: SequenceNotStr[WorkflowVersionId]) -> WorkflowVersionList:
        return self.client.workflows.versions.list(list(ids))

    def _upsert(self, items: WorkflowVersionUpsertList) -> WorkflowVersionList:
        return WorkflowVersionList([self.client.workflows.versions.upsert(item) for item in items])

    def create(self, items: WorkflowVersionUpsertList) -> WorkflowVersionList:
        upserted = []
        for item in items:
            upserted.append(self.client.workflows.versions.upsert(item))
        return WorkflowVersionList(upserted)

    def update(self, items: WorkflowVersionUpsertList) -> WorkflowVersionList:
        updated = []
        for item in items:
            updated.append(self.client.workflows.versions.upsert(item))
        return WorkflowVersionList(updated)

    def delete(self, ids: SequenceNotStr[WorkflowVersionId]) -> int:
        successes = 0
        for id in ids:
            try:
                self.client.workflows.versions.delete(id)
            except CogniteNotFoundError:
                print(f"  [bold yellow]WARNING:[/] WorkflowVersion {id} does not exist, skipping delete.")
            else:
                successes += 1
        return successes

    def iterate(self) -> Iterable[WorkflowVersion]:
        return self.client.workflows.versions.list(limit=-1)

    @classmethod
    @lru_cache(maxsize=1)
    def get_write_cls_parameter_spec(cls) -> ParameterSpecSet:
        spec = super().get_write_cls_parameter_spec()
        # The Parameter class in the SDK class WorkflowVersion implementation is deviating from the API.
        # So we need to modify the spec to match the API.
        parameter_path = ("workflowDefinition", "tasks", ANY_INT, "parameters")
        length = len(parameter_path)
        for item in spec:
            if len(item.path) >= length + 1 and item.path[:length] == parameter_path[:length]:
                # Add extra ANY_STR layer
                # The spec class is immutable, so we use this trick to modify it.
                object.__setattr__(item, "path", item.path[:length] + (ANY_STR,) + item.path[length:])
        spec.add(ParameterSpec((*parameter_path, ANY_STR), frozenset({"dict"}), is_required=True, _is_nullable=False))
        # The depends on is implemented as a list of string in the SDK, but in the API spec it
        # is a list of objects with one 'externalId' field.
        spec.add(
            ParameterSpec(
                ("workflowDefinition", "tasks", ANY_INT, "dependsOn", ANY_INT, "externalId"),
                frozenset({"str"}),
                is_required=False,
                _is_nullable=False,
            )
        )
        spec.add(
            ParameterSpec(
                ("workflowDefinition", "tasks", ANY_INT, "type"),
                frozenset({"str"}),
                is_required=True,
                _is_nullable=False,
            )
        )
        return spec


@final
class GroupResourceScopedLoader(GroupLoader):
    dependencies = frozenset(
        {
            SpaceLoader,
            DataSetsLoader,
            ExtractionPipelineLoader,
            TimeSeriesLoader,
        }
    )

    def __init__(self, client: ToolkitClient, build_dir: Path | None):
        super().__init__(client, build_dir, "resource_scoped_only")


def _in_dict(keys: Iterable[str], dictionary: dict) -> bool:
    return all(key in dictionary for key in keys)
