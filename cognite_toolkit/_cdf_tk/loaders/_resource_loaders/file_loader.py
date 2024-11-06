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

from collections.abc import Hashable, Iterable
from functools import lru_cache
from pathlib import Path
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
    CDFToolConfig,
    in_dict,
    load_yaml_inject_variables,
)

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
        # Matches all yaml files except file names whose stem ends with `.CogniteFile`.
        r"^(?!.*CogniteFile$).*"
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
        return "file_metadata"

    @classmethod
    def get_required_capability(
        cls, items: FileMetadataWriteList | None, read_only: bool
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

    def load_resource(self, filepath: Path, ToolGlobals: CDFToolConfig, skip_validation: bool) -> FileMetadataWriteList:
        use_environment_variables = (
            ToolGlobals.environment_variables() if self.do_environment_variable_injection else {}
        )
        loaded = load_yaml_inject_variables(filepath, use_environment_variables)

        loaded_list = [loaded] if isinstance(loaded, dict) else loaded

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
            if "assetExternalIds" in resource:
                resource["assetIds"] = ToolGlobals.verify_asset(
                    resource["assetExternalIds"],
                    skip_validation,
                    action="replace assetExternalIds with assetIds in file metadata",
                )

        return FileMetadataWriteList._load(loaded_list)

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
        return self.client.files.update(items, mode="replace")

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
        return "cognite_file"

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
    def get_required_capability(cls, items: ExtendableCogniteFileApplyList | None, read_only: bool) -> list[Capability]:
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

    def iterate(self) -> Iterable[ExtendableCogniteFile]:
        raise NotImplementedError("")
        # return iter(self.client.data_modeling.instances)

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
