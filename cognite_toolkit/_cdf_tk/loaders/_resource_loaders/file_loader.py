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

from collections.abc import Hashable, Iterable, Sequence
from datetime import date, datetime
from functools import lru_cache
from typing import Any, cast, final

from cognite.client.data_classes import (
    FileMetadata,
    FileMetadataList,
    FileMetadataWrite,
    FileMetadataWriteList,
)
from cognite.client.data_classes.capabilities import (
    Capability,
    DataModelInstancesAcl,
    FilesAcl,
)
from cognite.client.data_classes.data_modeling import NodeApplyResultList, NodeId, ViewId
from cognite.client.exceptions import CogniteAPIError
from cognite.client.utils._time import convert_data_modelling_timestamp
from cognite.client.utils.useful_types import SequenceNotStr
from rich import print

from cognite_toolkit._cdf_tk._parameters import ANY_INT, ANY_STR, ANYTHING, ParameterSpec, ParameterSpecSet
from cognite_toolkit._cdf_tk.client.data_classes.extendable_cognite_file import (
    ExtendableCogniteFile,
    ExtendableCogniteFileApply,
    ExtendableCogniteFileApplyList,
    ExtendableCogniteFileList,
)
from cognite_toolkit._cdf_tk.exceptions import (
    ToolkitRequiredValueError,
)
from cognite_toolkit._cdf_tk.loaders._base_loaders import ResourceContainerLoader, ResourceLoader
from cognite_toolkit._cdf_tk.utils import (
    in_dict,
)
from cognite_toolkit._cdf_tk.utils.diff_list import diff_list_hashable, diff_list_identifiable, dm_identifier

from .auth_loaders import GroupAllScopedLoader, SecurityCategoryLoader
from .classic_loaders import AssetLoader
from .data_organization_loaders import DataSetsLoader, LabelLoader
from .datamodel_loaders import SpaceLoader, ViewLoader


@final
class FileMetadataLoader(
    ResourceContainerLoader[str, FileMetadataWrite, FileMetadata, FileMetadataWriteList, FileMetadataList]
):
    item_name = "file contents"
    folder_name = "files"
    filename_pattern = (
        # Matches all yaml files except file names whose stem ends with `.CogniteFile` or `File`.
        r"(?i)^(?!.*(?:File|CogniteFile)$).*$"
    )
    resource_cls = FileMetadata
    resource_write_cls = FileMetadataWrite
    list_cls = FileMetadataList
    list_write_cls = FileMetadataWriteList
    kind = "FileMetadata"
    dependencies = frozenset({DataSetsLoader, GroupAllScopedLoader, LabelLoader, AssetLoader})

    _doc_url = "Files/operation/initFileUpload"

    @property
    def display_name(self) -> str:
        return "file metadata"

    @classmethod
    def get_required_capability(
        cls, items: Sequence[FileMetadataWrite] | None, read_only: bool
    ) -> Capability | list[Capability]:
        if not items and items is not None:
            return []

        actions = [FilesAcl.Action.Read] if read_only else [FilesAcl.Action.Read, FilesAcl.Action.Write]

        scope: FilesAcl.Scope.All | FilesAcl.Scope.DataSet = FilesAcl.Scope.All()  # type: ignore[valid-type]
        if items:
            if data_set_ids := {item.data_set_id for item in items if item.data_set_id}:
                scope = FilesAcl.Scope.DataSet(list(data_set_ids))

        return FilesAcl(actions, scope)  # type: ignore[arg-type]

    @classmethod
    def get_id(cls, item: FileMetadata | FileMetadataWrite | dict) -> str:
        if isinstance(item, dict):
            return item["externalId"]
        if item.external_id is None:
            raise ToolkitRequiredValueError("FileMetadata must have external_id set.")
        return item.external_id

    @classmethod
    def get_internal_id(cls, item: FileMetadata | dict) -> int:
        if isinstance(item, dict):
            return item["id"]
        return item.id

    @classmethod
    def dump_id(cls, id: str) -> dict[str, Any]:
        return {"externalId": id}

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
        for asset_external_id in item.get("assetExternalIds", []):
            yield AssetLoader, asset_external_id

    def load_resource(self, resource: dict[str, Any], is_dry_run: bool = False) -> FileMetadataWrite:
        if resource.get("dataSetExternalId") is not None:
            ds_external_id = resource.pop("dataSetExternalId")
            resource["dataSetId"] = self.client.lookup.data_sets.id(ds_external_id, is_dry_run)
        if security_categories_names := resource.pop("securityCategoryNames", []):
            security_categories = self.client.lookup.security_categories.id(security_categories_names, is_dry_run)
            resource["securityCategories"] = security_categories
        if "assetExternalIds" in resource:
            resource["assetIds"] = self.client.lookup.assets.id(resource["assetExternalIds"], is_dry_run)
        return FileMetadataWrite._load(resource)

    def dump_resource(self, resource: FileMetadata, local: dict[str, Any] | None = None) -> dict[str, Any]:
        dumped = resource.as_write().dump()
        if ds_id := dumped.pop("dataSetId", None):
            dumped["dataSetExternalId"] = self.client.lookup.data_sets.external_id(ds_id)
        if security_categories := dumped.pop("securityCategories", []):
            dumped["securityCategoryNames"] = self.client.lookup.security_categories.external_id(security_categories)
        if asset_ids := dumped.pop("assetIds", []):
            dumped["assetExternalIds"] = self.client.lookup.assets.external_id(asset_ids)
        return dumped

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
        return self.client.files.update(items, mode="replace")

    def delete(self, ids: str | int | SequenceNotStr[str | int] | None) -> int:
        internal_ids, external_ids = self._split_ids(ids)
        self.client.files.delete(id=internal_ids, external_id=external_ids)
        return len(cast(SequenceNotStr[str], ids))

    def _iterate(
        self,
        data_set_external_id: str | None = None,
        space: str | None = None,
        parent_ids: list[Hashable] | None = None,
    ) -> Iterable[FileMetadata]:
        return iter(self.client.files(data_set_external_ids=[data_set_external_id] if data_set_external_id else None))

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
        spec.discard(ParameterSpec(("dataSetId",), frozenset({"int"}), is_required=False, _is_nullable=False))
        spec.add(ParameterSpec(("securityCategoryNames",), frozenset({"list"}), is_required=False, _is_nullable=False))

        spec.add(
            ParameterSpec(("securityCategoryNames", ANY_STR), frozenset({"str"}), is_required=False, _is_nullable=False)
        )
        spec.add(ParameterSpec(("assetExternalIds",), frozenset({"list"}), is_required=False, _is_nullable=False))
        spec.add(
            ParameterSpec(("assetExternalIds", ANY_INT), frozenset({"int"}), is_required=False, _is_nullable=False)
        )
        spec.discard(ParameterSpec(("assetIds",), frozenset({"list"}), is_required=False, _is_nullable=False))
        spec.discard(ParameterSpec(("assetIds", ANY_INT), frozenset({"str"}), is_required=False, _is_nullable=False))

        return spec


@final
class CogniteFileLoader(
    ResourceContainerLoader[
        NodeId,
        ExtendableCogniteFileApply,
        ExtendableCogniteFile,
        ExtendableCogniteFileApplyList,
        ExtendableCogniteFileList,
    ]
):
    template_pattern = "$FILENAME"
    item_name = "file contents"
    folder_name = "files"
    filename_pattern = r"^.*CogniteFile"  # Matches all yaml files whose stem ends with 'CogniteFile'.
    kind = "CogniteFile"
    resource_cls = ExtendableCogniteFile
    resource_write_cls = ExtendableCogniteFileApply
    list_cls = ExtendableCogniteFileList
    list_write_cls = ExtendableCogniteFileApplyList
    dependencies = frozenset({GroupAllScopedLoader, SpaceLoader, ViewLoader})

    _doc_url = "Files/operation/initFileUpload"

    @property
    def display_name(self) -> str:
        return "cognite files"

    @classmethod
    def get_id(cls, item: ExtendableCogniteFile | ExtendableCogniteFileApply | dict) -> NodeId:
        if isinstance(item, dict):
            if missing := tuple(k for k in {"space", "externalId"} if k not in item):
                # We need to raise a KeyError with all missing keys to get the correct error message.
                raise KeyError(*missing)
            return NodeId(space=item["space"], external_id=item["externalId"])
        return item.as_id()

    @classmethod
    def dump_id(cls, id: NodeId) -> dict[str, Any]:
        return id.dump(include_instance_type=False)

    @classmethod
    def get_required_capability(
        cls, items: Sequence[ExtendableCogniteFileApply] | None, read_only: bool
    ) -> list[Capability]:
        if not items and items is not None:
            return []

        file_actions = [FilesAcl.Action.Read] if read_only else [FilesAcl.Action.Read, FilesAcl.Action.Write]
        instance_actions = (
            [DataModelInstancesAcl.Action.Read]
            if read_only
            else [DataModelInstancesAcl.Action.Read, DataModelInstancesAcl.Action.Write]
        )

        scope: DataModelInstancesAcl.Scope.All | DataModelInstancesAcl.Scope.SpaceID = DataModelInstancesAcl.Scope.All()  # type: ignore[valid-type]
        if items:
            if spaces := {item.space for item in items}:
                scope = DataModelInstancesAcl.Scope.SpaceID(list(spaces))
        return [
            FilesAcl(file_actions, FilesAcl.Scope.All()),
            DataModelInstancesAcl(
                instance_actions,  # type: ignore[valid-type]
                scope,  # type: ignore[arg-type]
            ),
        ]

    def dump_resource(self, resource: ExtendableCogniteFile, local: dict[str, Any] | None = None) -> dict[str, Any]:
        dumped = resource.as_write().dump(context="local")
        local = local or {}
        if "existingVersion" not in local:
            # Existing version is typically not set when creating nodes, but we get it back
            # when we retrieve the node from the server.
            dumped.pop("existingVersion", None)
        for key in list(dumped.keys()):
            value = dumped[key]
            if key not in local:
                if value is None:
                    dumped.pop(key)
                continue
            local_value = local[key]
            if isinstance(local_value, datetime) and isinstance(value, str):
                dumped[key] = convert_data_modelling_timestamp(value)
            elif isinstance(local_value, date) and isinstance(value, str):
                dumped[key] = date.fromisoformat(value)

        if "nodeSource" in local:
            dumped["nodeSource"] = local["nodeSource"]
        if dumped.get("instanceType") == "node" and "instanceType" not in local:
            dumped.pop("instanceType")
        return dumped

    def diff_list(
        self, local: list[Any], cdf: list[Any], json_path: tuple[str | int, ...]
    ) -> tuple[dict[int, int], list[int]]:
        if json_path == ("tags",):
            return diff_list_hashable(local, cdf)
        elif json_path[0] in ("assets", "category"):
            return diff_list_identifiable(local, cdf, get_identifier=dm_identifier)
        return super().diff_list(local, cdf, json_path)

    def create(self, items: ExtendableCogniteFileApplyList) -> NodeApplyResultList:
        created = self.client.data_modeling.instances.apply(
            nodes=items, replace=False, skip_on_version_conflict=True, auto_create_direct_relations=True
        )
        return created.nodes

    def retrieve(self, ids: SequenceNotStr[NodeId]) -> ExtendableCogniteFileList:
        # Todo: Problem, if you extend the CogniteFiles with a custom view, we need to know
        #   the ID of the custom view to retrieve data from it. This is not possible with the current
        #   structure. Need to reconsider how to handle this.
        items = self.client.data_modeling.instances.retrieve_nodes(  # type: ignore[call-overload]
            nodes=ids,
            node_cls=ExtendableCogniteFile,
        )
        return ExtendableCogniteFileList(items)

    def update(self, items: ExtendableCogniteFileApplyList) -> NodeApplyResultList:
        updated = self.client.data_modeling.instances.apply(nodes=items, replace=True)
        return updated.nodes

    def delete(self, ids: SequenceNotStr[NodeId]) -> int:
        try:
            deleted = self.client.data_modeling.instances.delete(nodes=list(ids))
        except CogniteAPIError as e:
            if "not exist" in e.message and "space" in e.message.lower():
                return 0
            raise e
        return len(deleted.nodes)

    def _iterate(
        self,
        data_set_external_id: str | None = None,
        space: str | None = None,
        parent_ids: list[Hashable] | None = None,
    ) -> Iterable[ExtendableCogniteFile]:
        # We do not have a way to know the source of the file, so we cannot filter on that.
        return []

    def count(self, ids: SequenceNotStr[NodeId]) -> int:
        return sum(
            [
                bool(n.is_uploaded or False)
                for n in self.client.data_modeling.instances.retrieve_nodes(nodes=ids, node_cls=ExtendableCogniteFile)  # type: ignore[call-overload]
            ]
        )

    def drop_data(self, ids: SequenceNotStr[NodeId]) -> int:
        existing_meta = self.client.files.retrieve_multiple(instance_ids=list(ids), ignore_unknown_ids=True)
        existing_node = self.retrieve(ids)

        # File and FileMetadata is tightly coupled, so we need to delete the metadata and recreate it
        # without the source set to delete the file.
        self.client.files.delete(id=existing_meta.as_ids())
        self.create(existing_node.as_write())
        return len(existing_meta)

    @classmethod
    @lru_cache(maxsize=1)
    def get_write_cls_parameter_spec(cls) -> ParameterSpecSet:
        spec = super().get_write_cls_parameter_spec()
        # Removed by the SDK
        spec.add(ParameterSpec(("instanceType",), frozenset({"str"}), is_required=False, _is_nullable=False))
        # Sources are used when writing to the API.
        spec.add(ParameterSpec(("sources",), frozenset({"list"}), is_required=False, _is_nullable=False))
        spec.add(ParameterSpec(("sources", ANYTHING), frozenset({"list"}), is_required=False, _is_nullable=False))
        return spec

    @classmethod
    def get_dependent_items(cls, item: dict) -> Iterable[tuple[type[ResourceLoader], Hashable]]:
        """Returns all items that this item requires.

        For example, a TimeSeries requires a DataSet, so this method would return the
        DatasetLoader and identifier of that dataset.
        """
        if "space" in item:
            yield SpaceLoader, item["space"]
        if "nodeSource" in item:
            if in_dict(("space", "externalId", "type"), item["nodeSource"]):
                yield ViewLoader, ViewId.load(item["nodeSource"])
