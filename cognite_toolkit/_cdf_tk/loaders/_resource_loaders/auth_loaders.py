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

import difflib
from abc import ABC
from collections.abc import Callable, Hashable, Iterable, Sequence
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any, Literal, cast, final

from cognite.client.data_classes import capabilities as cap
from cognite.client.data_classes.capabilities import (
    Capability,
    GroupsAcl,
    SecurityCategoriesAcl,
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
from cognite.client.exceptions import CogniteAPIError
from cognite.client.utils.useful_types import SequenceNotStr
from rich import print

from cognite_toolkit._cdf_tk._parameters import ANY_INT, ANY_STR, ANYTHING, ParameterSpec, ParameterSpecSet
from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.data_classes.raw import RawDatabase, RawTable
from cognite_toolkit._cdf_tk.loaders._base_loaders import ResourceLoader
from cognite_toolkit._cdf_tk.tk_warnings import (
    MediumSeverityWarning,
)
from cognite_toolkit._cdf_tk.utils import (
    CDFToolConfig,
    load_yaml_inject_variables,
)


@dataclass
class _ReplaceMethod:
    """This is a small helper class used in the
    lookup and replace in the ACL scoped ids"""

    verify_method: Callable[[str, bool, str], int]
    operation: str
    id_name: str


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
            cap.IDScope,
            cap.SpaceIDScope,
            cap.DataSetScope,
            cap.TableScope,
            cap.AssetRootIDScope,
            cap.ExtractionPipelineScope,
            cap.IDScopeLowerCase,
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
    def get_required_capability(cls, items: GroupWriteList | None, read_only: bool) -> Capability | list[Capability]:
        if not items and items is not None:
            return []

        actions = (
            [
                GroupsAcl.Action.Read,
                GroupsAcl.Action.List,
            ]
            if read_only
            else [
                GroupsAcl.Action.Read,
                GroupsAcl.Action.List,
                GroupsAcl.Action.Create,
                GroupsAcl.Action.Delete,
                GroupsAcl.Action.Update,
            ]
        )

        return GroupsAcl(
            actions,
            GroupsAcl.Scope.All(),
        )

    @classmethod
    def get_id(cls, item: GroupWrite | Group | dict) -> str:
        if isinstance(item, dict):
            return item["name"]
        return item.name

    @classmethod
    def dump_id(cls, id: str) -> dict[str, Any]:
        return {"name": id}

    @classmethod
    def get_dependent_items(cls, item: dict) -> Iterable[tuple[type[ResourceLoader], Hashable]]:
        from .classic_loaders import AssetLoader
        from .data_organization_loaders import DataSetsLoader
        from .datamodel_loaders import SpaceLoader
        from .extraction_pipeline_loaders import ExtractionPipelineLoader
        from .location_loaders import LocationFilterLoader
        from .raw_loaders import RawDatabaseLoader, RawTableLoader
        from .timeseries_loaders import TimeSeriesLoader

        for capability in item.get("capabilities", []):
            for acl, content in capability.items():
                if scope := content.get("scope", {}):
                    if space_ids := scope.get(cap.SpaceIDScope._scope_name, []):
                        if isinstance(space_ids, dict) and "spaceIds" in space_ids:
                            for space_id in space_ids["spaceIds"]:
                                yield SpaceLoader, space_id
                    if data_set_ids := scope.get(cap.DataSetScope._scope_name, []):
                        if isinstance(data_set_ids, dict) and "ids" in data_set_ids:
                            for data_set_id in data_set_ids["ids"]:
                                yield DataSetsLoader, data_set_id
                    if table_ids := scope.get(cap.TableScope._scope_name, []):
                        for db_name, tables in table_ids.get("dbsToTables", {}).items():
                            yield RawDatabaseLoader, RawDatabase(db_name)
                            if isinstance(tables, Iterable):
                                for table in tables:
                                    yield RawTableLoader, RawTable(db_name, table)
                    if extraction_pipeline_ids := scope.get(cap.ExtractionPipelineScope._scope_name, []):
                        if isinstance(extraction_pipeline_ids, dict) and "ids" in extraction_pipeline_ids:
                            for extraction_pipeline_id in extraction_pipeline_ids["ids"]:
                                yield ExtractionPipelineLoader, extraction_pipeline_id
                    if asset_root_ids := scope.get(cap.AssetRootIDScope._scope_name, []):
                        if isinstance(asset_root_ids, dict) and "rootIds" in asset_root_ids:
                            for asset_root_id in asset_root_ids["rootIds"]:
                                yield AssetLoader, asset_root_id
                    if (ids := scope.get(cap.IDScope._scope_name, [])) or (
                        ids := scope.get(cap.IDScopeLowerCase._scope_name, [])
                    ):
                        loader: type[ResourceLoader] | None = None
                        if acl == cap.DataSetsAcl._capability_name:
                            loader = DataSetsLoader
                        elif acl == cap.ExtractionPipelinesAcl._capability_name:
                            loader = ExtractionPipelineLoader
                        elif acl == cap.TimeSeriesAcl._capability_name:
                            loader = TimeSeriesLoader
                        elif acl == cap.SecurityCategoriesAcl._capability_name:
                            loader = SecurityCategoryLoader
                        elif acl == cap.LocationFiltersAcl._capability_name:
                            loader = LocationFilterLoader
                        if loader is not None and isinstance(ids, dict) and "ids" in ids:
                            for id_ in ids["ids"]:
                                yield loader, id_

    @classmethod
    def _substitute_scope_ids(cls, group: dict, ToolGlobals: CDFToolConfig, skip_validation: bool) -> dict:
        replace_method_by_acl = cls._create_replace_method_by_acl_and_scope(ToolGlobals)

        for capability in group.get("capabilities", []):
            for acl, values in capability.items():
                scope = values.get("scope", {})
                if len(scope) != 1:
                    # This will raise an error when the group is loaded.
                    continue
                scope_name, scope_content = next(iter(scope.items()))

                if (acl, scope_name) in replace_method_by_acl:
                    replace_method = replace_method_by_acl[(acl, scope_name)]
                elif scope_name in replace_method_by_acl:
                    replace_method = replace_method_by_acl[scope_name]
                else:
                    continue
                if ids := scope.get(scope_name, {}).get(replace_method.id_name, []):
                    values["scope"][scope_name][replace_method.id_name] = [
                        replace_method.verify_method(ext_id, skip_validation, replace_method.operation)
                        if isinstance(ext_id, str)
                        else ext_id
                        for ext_id in ids
                    ]
        return group

    @classmethod
    def _create_replace_method_by_acl_and_scope(
        cls, ToolGlobals: CDFToolConfig
    ) -> dict[tuple[str, str] | str, _ReplaceMethod]:
        source = {
            (cap.DataSetsAcl, cap.DataSetsAcl.Scope.ID): _ReplaceMethod(
                ToolGlobals.verify_dataset,
                operation="replace datasetExternalId with dataSetId in group",
                id_name="ids",
            ),
            (cap.ExtractionPipelinesAcl, cap.ExtractionPipelinesAcl.Scope.ID): _ReplaceMethod(
                ToolGlobals.verify_extraction_pipeline,
                operation="replace extractionPipelineExternalId with extractionPipelineId in group",
                id_name="ids",
            ),
            (cap.LocationFiltersAcl, cap.LocationFiltersAcl.Scope.ID): _ReplaceMethod(
                ToolGlobals.verify_locationfilter,
                operation="replace locationFilterExternalId with locationFilterId in group",
                id_name="ids",
            ),
            (cap.SecurityCategoriesAcl, cap.SecurityCategoriesAcl.Scope.ID): _ReplaceMethod(
                ToolGlobals.verify_security_categories,
                operation="replace securityCategoryExternalId with securityCategoryId in group",
                id_name="ids",
            ),
            (cap.TimeSeriesAcl, cap.TimeSeriesAcl.Scope.ID): _ReplaceMethod(
                ToolGlobals.verify_timeseries,
                operation="replace timeSeriesExternalId with timeSeriesId in group",
                id_name="ids",
            ),
            cap.DataSetScope: _ReplaceMethod(
                ToolGlobals.verify_dataset,
                operation="replace datasetExternalId with dataSetId in group",
                id_name="ids",
            ),
            cap.ExtractionPipelineScope: _ReplaceMethod(
                ToolGlobals.verify_extraction_pipeline,
                operation="replace extractionPipelineExternalId with extractionPipelineId in group",
                id_name="ids",
            ),
            cap.AssetRootIDScope: _ReplaceMethod(
                ToolGlobals.verify_dataset,
                operation="replace rootAssetExternalId with rootAssetId in group",
                id_name="rootIds",
            ),
        }
        # Trick to avoid writing _capability_name and _scope_name for each entry.
        return {
            (key[0]._capability_name, key[1]._scope_name) if isinstance(key, tuple) else key._scope_name: method  # type: ignore[attr-defined]
            for key, method in source.items()
        }

    def load_resource(
        self, filepath: Path, ToolGlobals: CDFToolConfig, skip_validation: bool
    ) -> GroupWrite | GroupWriteList | None:
        use_environment_variables = (
            ToolGlobals.environment_variables() if self.do_environment_variable_injection else {}
        )
        raw_yaml = load_yaml_inject_variables(filepath, use_environment_variables)

        group_write_list = GroupWriteList([])

        if isinstance(raw_yaml, dict):
            raw_yaml = [raw_yaml]

        for raw_group in raw_yaml:
            is_resource_scoped = any(
                any(scope_name in capability.get(acl, {}).get("scope", {}) for scope_name in self.resource_scope_names)
                for capability in raw_group.get("capabilities", [])
                for acl in capability
            )

            if self.target_scopes == "all_scoped_only" and is_resource_scoped:
                continue

            if self.target_scopes == "resource_scoped_only" and not is_resource_scoped:
                continue

            substituted = self._substitute_scope_ids(raw_group, ToolGlobals, skip_validation)
            try:
                loaded = GroupWrite.load(substituted)
            except ValueError:
                # The GroupWrite class in the SDK will raise a ValueError if the ACI or scope is not valid or unknown.
                loaded = GroupWrite._load(substituted, allow_unknown=True)
                for capability in loaded.capabilities or []:
                    if isinstance(capability, cap.UnknownAcl):
                        msg = (
                            f"In group {loaded.name!r}, unknown capability found: {capability.capability_name!r}.\n"
                            "Will proceed with group creation and let the API validate the capability."
                        )
                        if matches := difflib.get_close_matches(capability.capability_name, cap.ALL_CAPABILITIES):
                            msg += f"\nIf the API rejects the capability, could it be that you meant on of: {matches}?"
                        prefix, warning_msg = MediumSeverityWarning(msg).print_prepare()
                        print(prefix, warning_msg)

            group_write_list.append(loaded)

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

        # Remove metadata if it is empty to avoid false negatives
        # as a result of cdf_resource.metadata = {} != local.metadata = None
        if not local_dumped.get("metadata"):
            local_dumped.pop("metadata", None)
        if not cdf_dumped.get("metadata"):
            cdf_dumped.pop("metadata", None)

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
        except CogniteAPIError as e:
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
    def dump_id(cls, id: str) -> dict[str, Any]:
        return {"name": id}

    @classmethod
    def get_required_capability(
        cls, items: SecurityCategoryWriteList | None, read_only: bool
    ) -> Capability | list[Capability]:
        if not items and items is not None:
            return []

        actions = (
            [
                SecurityCategoriesAcl.Action.List,
                SecurityCategoriesAcl.Action.MemberOf,
            ]
            if read_only
            else [
                SecurityCategoriesAcl.Action.Create,
                SecurityCategoriesAcl.Action.Update,
                SecurityCategoriesAcl.Action.MemberOf,
                SecurityCategoriesAcl.Action.List,
                SecurityCategoriesAcl.Action.Delete,
            ]
        )

        return SecurityCategoriesAcl(
            actions,
            SecurityCategoriesAcl.Scope.All(),
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
