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
from collections import defaultdict
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
from cognite.client.exceptions import CogniteAPIError, CogniteNotFoundError
from cognite.client.utils.useful_types import SequenceNotStr
from rich import print
from rich.console import Console
from rich.markup import escape

from cognite_toolkit._cdf_tk._parameters import ANY_INT, ANY_STR, ANYTHING, ParameterSpec, ParameterSpecSet
from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.data_classes.raw import RawDatabase, RawTable
from cognite_toolkit._cdf_tk.exceptions import ToolkitWrongResourceError
from cognite_toolkit._cdf_tk.loaders._base_loaders import ResourceLoader
from cognite_toolkit._cdf_tk.tk_warnings import (
    HighSeverityWarning,
    MediumSeverityWarning,
)
from cognite_toolkit._cdf_tk.utils import humanize_collection
from cognite_toolkit._cdf_tk.utils.diff_list import diff_list_hashable, diff_list_identifiable, hash_dict


@dataclass
class _ReplaceMethod:
    """This is a small helper class used in the
    lookup and replace in the ACL scoped ids"""

    lookup_method: Callable[[str, bool], int]
    reverse_lookup_method: Callable[[int], str | None]
    id_name: str


class GroupLoader(ResourceLoader[str, GroupWrite, Group, GroupWriteList, GroupList]):
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
        console: Console | None,
        target_scopes: Literal[
            "all_scoped_only",
            "resource_scoped_only",
        ] = "all_scoped_only",
    ):
        super().__init__(client, build_dir, console)
        self.target_scopes = target_scopes

    @property
    def display_name(self) -> str:
        return f"groups({self.target_scopes.removesuffix('_only')})"

    @classmethod
    def get_required_capability(
        cls, items: Sequence[GroupWrite] | None, read_only: bool
    ) -> Capability | list[Capability]:
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
                            if isinstance(tables, list):
                                yield from ((RawTableLoader, RawTable(db_name, table)) for table in tables)
                            elif isinstance(tables, dict) and "tables" in tables:
                                for table in tables["tables"]:
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

    def _substitute_scope_ids(self, group: dict[str, Any], is_dry_run: bool, reverse: bool = False) -> dict[str, Any]:
        replace_method_by_acl = self._create_replace_method_by_acl_and_scope()

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
                    if reverse:
                        cdf_ids = (
                            replace_method.reverse_lookup_method(int_id) if isinstance(int_id, int) else int_id
                            for int_id in ids
                        )
                        values["scope"][scope_name][replace_method.id_name] = [
                            id_ for id_ in cdf_ids if id_ is not None
                        ]
                    else:
                        values["scope"][scope_name][replace_method.id_name] = [
                            replace_method.lookup_method(ext_id, is_dry_run) if isinstance(ext_id, str) else ext_id
                            for ext_id in ids
                        ]
        return group

    def _create_replace_method_by_acl_and_scope(self) -> dict[tuple[str, str] | str, _ReplaceMethod]:
        source = {
            (cap.DataSetsAcl, cap.DataSetsAcl.Scope.ID): _ReplaceMethod(
                self.client.lookup.data_sets.id,
                self.client.lookup.data_sets.external_id,
                id_name="ids",
            ),
            (cap.ExtractionPipelinesAcl, cap.ExtractionPipelinesAcl.Scope.ID): _ReplaceMethod(
                self.client.lookup.extraction_pipelines.id,
                self.client.lookup.extraction_pipelines.external_id,
                id_name="ids",
            ),
            (cap.LocationFiltersAcl, cap.LocationFiltersAcl.Scope.ID): _ReplaceMethod(
                self.client.lookup.location_filters.id,
                self.client.lookup.location_filters.external_id,
                id_name="ids",
            ),
            (cap.SecurityCategoriesAcl, cap.SecurityCategoriesAcl.Scope.ID): _ReplaceMethod(
                self.client.lookup.security_categories.id,
                self.client.lookup.security_categories.external_id,
                id_name="ids",
            ),
            (cap.TimeSeriesAcl, cap.TimeSeriesAcl.Scope.ID): _ReplaceMethod(
                self.client.lookup.time_series.id,
                self.client.lookup.time_series.external_id,
                id_name="ids",
            ),
            cap.DataSetScope: _ReplaceMethod(
                self.client.lookup.data_sets.id,
                self.client.lookup.data_sets.external_id,
                id_name="ids",
            ),
            cap.ExtractionPipelineScope: _ReplaceMethod(
                self.client.lookup.extraction_pipelines.id,
                self.client.lookup.extraction_pipelines.external_id,
                id_name="ids",
            ),
            cap.AssetRootIDScope: _ReplaceMethod(
                self.client.lookup.assets.id,
                self.client.lookup.assets.external_id,
                id_name="rootIds",
            ),
        }
        # Trick to avoid writing _capability_name and _scope_name for each entry.
        return {
            (key[0]._capability_name, key[1]._scope_name) if isinstance(key, tuple) else key._scope_name: method  # type: ignore[attr-defined]
            for key, method in source.items()
        }

    def load_resource(self, resource: dict[str, Any], is_dry_run: bool = False) -> GroupWrite:
        is_resource_scoped = any(
            any(scope_name in capability.get(acl, {}).get("scope", {}) for scope_name in self.resource_scope_names)
            for capability in resource.get("capabilities", [])
            for acl in capability
        )

        if self.target_scopes == "all_scoped_only" and is_resource_scoped:
            raise ToolkitWrongResourceError()

        if self.target_scopes == "resource_scoped_only" and not is_resource_scoped:
            raise ToolkitWrongResourceError()

        substituted = self._substitute_scope_ids(resource, is_dry_run)
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

        return loaded

    def dump_resource(self, resource: Group, local: dict[str, Any] | None = None) -> dict[str, Any]:
        dumped = resource.as_write().dump()
        local = local or {}
        if not dumped.get("metadata") and "metadata" not in local:
            dumped.pop("metadata", None)
        if not dumped.get("sourceId") and "sourceId" not in local:
            dumped.pop("sourceId", None)
        # RAWAcls are not returned by the API following the spec.
        # If you have a table scoped RAW ACL the spec, and thus user will input
        # tableScope:
        #   dbsToTables
        #    db1:
        #    - tables1
        #    - tables2
        # While the API will return
        # tableScope:
        #   dbsToTables:
        #   db1:
        #     tables: [tables1, tables2]
        # Note the extra keyword 'tables' in the API response.
        for capability in dumped.get("capabilities", []):
            for acl, content in capability.items():
                if acl != cap.RawAcl._capability_name:
                    continue
                if scope := content.get("scope", {}):
                    if table_scope := scope.get(cap.TableScope._scope_name, {}):
                        db_to_tables = table_scope.get("dbsToTables", {})
                        if not db_to_tables:
                            continue
                        for db_name in list(db_to_tables.keys()):
                            tables = db_to_tables[db_name]
                            if isinstance(tables, dict) and "tables" in tables:
                                db_to_tables[db_name] = tables["tables"]
        # When you dump a CDF Group, all the referenced resources should be available in CDF.
        return self._substitute_scope_ids(dumped, is_dry_run=False, reverse=True)

    def create(self, items: Sequence[GroupWrite]) -> GroupList:
        if len(items) == 0:
            return GroupList([])
        return self._create_with_fallback(items, action="create")

    def update(self, items: Sequence[GroupWrite]) -> GroupList:
        # We MUST retrieve all the old groups BEFORE we add the new, if not the new will be deleted
        old_groups = self.client.iam.groups.list(all=True)
        created = self._create_with_fallback(items, action="update")
        created_names = {g.name for g in created}
        to_delete = GroupList([group for group in old_groups if group.name in created_names])
        if to_delete:
            self._delete(to_delete, check_own_principal=False)
        return created

    def _create_with_fallback(self, items: Sequence[GroupWrite], action: Literal["create", "update"]) -> GroupList:
        try:
            return self.client.iam.groups.create(items)
        except CogniteAPIError as e:
            if not (e.code == 400 and "buffer" in e.message.lower() and len(items) > 1):
                raise e
            # Fallback to create one by one
            created_list = GroupList([])
            for item in items:
                try:
                    created = self.client.iam.groups.create(item)
                except CogniteAPIError as e:
                    HighSeverityWarning(f"Failed to {action} group {item.name}. Error: {escape(str(e))}").print_warning(
                        include_timestamp=True, console=self.console
                    )
                else:
                    created_list.append(created)
            return created_list

    def retrieve(self, ids: SequenceNotStr[str]) -> GroupList:
        id_set = set(ids)
        remote = self.client.iam.groups.list(all=True)
        found = [g for g in remote if g.name in id_set]
        return GroupList(found)

    def delete(self, ids: SequenceNotStr[str]) -> int:
        return self._delete(self.retrieve(ids), check_own_principal=True)

    def _delete(self, delete_candidates: GroupList, check_own_principal: bool = True) -> int:
        if check_own_principal:
            print_fun = self.console.print if self.console else print
            try:
                # Let's prevent that we delete groups we belong to
                my_groups = self.client.iam.groups.list()
            except CogniteAPIError as e:
                print_fun(
                    f"[bold red]ERROR:[/] Failed to retrieve the current service principal's groups. Aborting group deletion.\n{e}"
                )
                return 0
            my_source_ids = {g.source_id for g in my_groups if g.source_id}
        else:
            my_source_ids = set()

        to_delete: list[int] = []
        counts_by_name: dict[str, int] = defaultdict(int)
        for group in delete_candidates:
            if group.source_id in my_source_ids:
                HighSeverityWarning(
                    f"Not deleting group {group.name} with sourceId {group.source_id} as it is used by"
                    f"the current service principal. If you want to delete this group, you must do it manually."
                ).print_warning(console=self.console)
            else:
                to_delete.append(group.id)
                counts_by_name[group.name] += 1
        if duplicates := {name for name, count in counts_by_name.items() if count > 1}:
            MediumSeverityWarning(
                f"The following names are used by multiple groups (all will be deleted): {duplicates}"
            ).print_warning(console=self.console)

        failed_deletes = []
        error_str = ""
        try:
            self.client.iam.groups.delete(to_delete)
        except CogniteNotFoundError:
            # Fallback to delete one by one
            for delete_item_id in to_delete:
                try:
                    self.client.iam.groups.delete(delete_item_id)
                except CogniteNotFoundError:
                    # If the group is already deleted, we can ignore the error
                    ...
                except CogniteAPIError as e:
                    error_str = str(e)
                    failed_deletes.append(delete_item_id)
        except CogniteAPIError as e:
            error_str = str(e)
            failed_deletes.extend(to_delete)
        if failed_deletes:
            MediumSeverityWarning(
                f"Failed to delete groups: {humanize_collection(to_delete)}. "
                "These must be deleted manually in the Fusion UI."
                f"Error: {escape(error_str)}"
            ).print_warning(include_timestamp=True, console=self.console)
        return len(to_delete) - len(failed_deletes)

    def _iterate(
        self,
        data_set_external_id: str | None = None,
        space: str | None = None,
        parent_ids: list[Hashable] | None = None,
    ) -> Iterable[Group]:
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

    def diff_list(
        self, local: list[Any], cdf: list[Any], json_path: tuple[str | int, ...]
    ) -> tuple[dict[int, int], list[int]]:
        if json_path == ("capabilities",):
            return diff_list_identifiable(local, cdf, get_identifier=hash_dict)
        elif json_path[0] == "capabilities":
            # All sublist inside capabilities are hashable
            return diff_list_hashable(local, cdf)
        elif json_path == ("members",):
            return diff_list_hashable(local, cdf)
        return super().diff_list(local, cdf, json_path)


@final
class GroupAllScopedLoader(GroupLoader):
    def __init__(self, client: ToolkitClient, build_dir: Path | None, console: Console | None):
        super().__init__(client, build_dir, console, "all_scoped_only")

    @property
    def display_name(self) -> str:
        return "all-scoped groups"


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
        return "security categories"

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
        cls, items: Sequence[SecurityCategoryWrite] | None, read_only: bool
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

    def _iterate(
        self,
        data_set_external_id: str | None = None,
        space: str | None = None,
        parent_ids: list[Hashable] | None = None,
    ) -> Iterable[SecurityCategory]:
        return self.client.iam.security_categories.list(limit=-1)
