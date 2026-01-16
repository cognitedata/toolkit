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


from collections.abc import Hashable, Iterable, Sequence
from datetime import date, datetime
from typing import Any, final

from cognite.client.data_classes.capabilities import (
    Capability,
    DataModelInstancesAcl,
    FilesAcl,
)
from cognite.client.data_classes.data_modeling import NodeApplyResultList, NodeId, ViewId
from cognite.client.exceptions import CogniteAPIError
from cognite.client.utils._time import convert_data_modelling_timestamp
from cognite.client.utils.useful_types import SequenceNotStr

from cognite_toolkit._cdf_tk.client.request_classes.filters import ClassicFilter
from cognite_toolkit._cdf_tk.client.resource_classes.filemetadata import FileMetadataRequest, FileMetadataResponse
from cognite_toolkit._cdf_tk.client.resource_classes.identifiers import ExternalId, InternalOrExternalId
from cognite_toolkit._cdf_tk.client.resource_classes.legacy.extendable_cognite_file import (
    ExtendableCogniteFile,
    ExtendableCogniteFileApply,
    ExtendableCogniteFileList,
)
from cognite_toolkit._cdf_tk.cruds._base_cruds import ResourceContainerCRUD, ResourceCRUD
from cognite_toolkit._cdf_tk.exceptions import (
    ToolkitRequiredValueError,
)
from cognite_toolkit._cdf_tk.resource_classes import CogniteFileYAML, FileMetadataYAML
from cognite_toolkit._cdf_tk.utils import (
    in_dict,
)
from cognite_toolkit._cdf_tk.utils.diff_list import diff_list_hashable, diff_list_identifiable, dm_identifier

from .auth import GroupAllScopedCRUD, SecurityCategoryCRUD
from .classic import AssetCRUD
from .data_organization import DataSetsCRUD, LabelCRUD
from .datamodel import SpaceCRUD, ViewCRUD


@final
class FileMetadataCRUD(ResourceContainerCRUD[ExternalId, FileMetadataRequest, FileMetadataResponse]):
    item_name = "file contents"
    folder_name = "files"
    resource_cls = FileMetadataResponse
    resource_write_cls = FileMetadataRequest
    yaml_cls = FileMetadataYAML
    kind = "FileMetadata"
    dependencies = frozenset({DataSetsCRUD, GroupAllScopedCRUD, LabelCRUD, AssetCRUD})

    _doc_url = "Files/operation/initFileUpload"

    @property
    def display_name(self) -> str:
        return "file metadata"

    @classmethod
    def get_required_capability(
        cls, items: Sequence[FileMetadataRequest] | None, read_only: bool
    ) -> Capability | list[Capability]:
        if not items and items is not None:
            return []

        actions = [FilesAcl.Action.Read] if read_only else [FilesAcl.Action.Read, FilesAcl.Action.Write]

        scope: FilesAcl.Scope.All | FilesAcl.Scope.DataSet = FilesAcl.Scope.All()  # type: ignore[valid-type]
        if items:
            if data_set_ids := {item.data_set_id for item in items if item.data_set_id}:
                scope = FilesAcl.Scope.DataSet(list(data_set_ids))

        return FilesAcl(actions, scope)

    @classmethod
    def get_id(cls, item: FileMetadataRequest | FileMetadataResponse | dict) -> ExternalId:
        if isinstance(item, dict):
            return ExternalId(external_id=item["externalId"])
        if item.external_id is None:
            raise ToolkitRequiredValueError("FileMetadata must have external_id set.")
        return ExternalId(external_id=item.external_id)

    @classmethod
    def get_internal_id(cls, item: FileMetadataResponse | dict) -> int:
        if isinstance(item, dict):
            return item["id"]
        return item.id

    @classmethod
    def dump_id(cls, id: ExternalId) -> dict[str, Any]:
        return id.dump()

    @classmethod
    def get_dependent_items(cls, item: dict) -> Iterable[tuple[type[ResourceCRUD], Hashable]]:
        if "dataSetExternalId" in item:
            yield DataSetsCRUD, item["dataSetExternalId"]
        if "securityCategoryNames" in item:
            for security_category in item["securityCategoryNames"]:
                yield SecurityCategoryCRUD, security_category
        if "labels" in item:
            for label in item["labels"]:
                if isinstance(label, dict):
                    yield LabelCRUD, label["externalId"]
                elif isinstance(label, str):
                    yield LabelCRUD, label
        for asset_external_id in item.get("assetExternalIds", []):
            yield AssetCRUD, ExternalId(external_id=asset_external_id)

    def load_resource(self, resource: dict[str, Any], is_dry_run: bool = False) -> FileMetadataRequest:
        if ds_external_id := resource.pop("dataSetExternalId", None):
            resource["dataSetId"] = self.client.lookup.data_sets.id(ds_external_id, is_dry_run)
        if security_categories_names := resource.pop("securityCategoryNames", []):
            resource["securityCategories"] = self.client.lookup.security_categories.id(
                security_categories_names, is_dry_run
            )
        if asset_external_ids := resource.pop("assetExternalIds", None):
            resource["assetIds"] = self.client.lookup.assets.id(asset_external_ids, is_dry_run)
        return FileMetadataRequest.model_validate(resource)

    def dump_resource(self, resource: FileMetadataResponse, local: dict[str, Any] | None = None) -> dict[str, Any]:
        dumped = resource.as_request_resource().dump()
        if data_set_id := dumped.pop("dataSetId", None):
            dumped["dataSetExternalId"] = self.client.lookup.data_sets.external_id(data_set_id)
        if security_categories := dumped.pop("securityCategories", []):
            dumped["securityCategoryNames"] = self.client.lookup.security_categories.external_id(security_categories)
        if asset_ids := dumped.pop("assetIds", []):
            dumped["assetExternalIds"] = self.client.lookup.assets.external_id(asset_ids)
        return dumped

    def create(self, items: Sequence[FileMetadataRequest]) -> list[FileMetadataResponse]:
        return self.client.tool.filemetadata.create(items, overwrite=True)

    def retrieve(self, ids: SequenceNotStr[ExternalId]) -> list[FileMetadataResponse]:
        return self.client.tool.filemetadata.retrieve(list(ids), ignore_unknown_ids=True)

    def update(self, items: Sequence[FileMetadataRequest]) -> list[FileMetadataResponse]:
        return self.client.tool.filemetadata.update(items, mode="replace")

    def delete(self, ids: SequenceNotStr[InternalOrExternalId]) -> int:
        if not ids:
            return 0
        self.client.tool.filemetadata.delete(list(ids), ignore_unknown_ids=True)
        return len(ids)

    def _iterate(
        self,
        data_set_external_id: str | None = None,
        space: str | None = None,
        parent_ids: list[Hashable] | None = None,
    ) -> Iterable[FileMetadataResponse]:
        filter_ = ClassicFilter.from_asset_subtree_and_data_sets(data_set_id=data_set_external_id)
        for files in self.client.tool.filemetadata.iterate(filter=filter_, limit=None):
            yield from files

    def count(self, ids: SequenceNotStr[ExternalId]) -> int:
        return sum(
            1 for meta in self.client.tool.filemetadata.retrieve(list(ids), ignore_unknown_ids=True) if meta.uploaded
        )

    def drop_data(self, ids: SequenceNotStr[ExternalId]) -> int:
        existing = self.client.tool.filemetadata.retrieve(list(ids), ignore_unknown_ids=True)
        # File and FileMetadata is tightly coupled, so we need to delete the metadata and recreate it
        # without the source set to delete the file.
        deleted_files = self.delete([meta.as_id() for meta in existing])
        self.create([meta.as_request_resource() for meta in existing])
        return deleted_files


@final
class CogniteFileCRUD(ResourceContainerCRUD[NodeId, ExtendableCogniteFileApply, ExtendableCogniteFile]):
    template_pattern = "$FILENAME"
    item_name = "file contents"
    folder_name = "files"
    kind = "CogniteFile"
    resource_cls = ExtendableCogniteFile
    resource_write_cls = ExtendableCogniteFileApply
    yaml_cls = CogniteFileYAML
    dependencies = frozenset({GroupAllScopedCRUD, SpaceCRUD, ViewCRUD})

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
            DataModelInstancesAcl(instance_actions, scope),
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

    def create(self, items: Sequence[ExtendableCogniteFileApply]) -> NodeApplyResultList:
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

    def update(self, items: Sequence[ExtendableCogniteFileApply]) -> NodeApplyResultList:
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
    def get_dependent_items(cls, item: dict) -> Iterable[tuple[type[ResourceCRUD], Hashable]]:
        """Returns all items that this item requires.

        For example, a TimeSeries requires a DataSet, so this method would return the
        DatasetLoader and identifier of that dataset.
        """
        if "space" in item:
            yield SpaceCRUD, item["space"]
        if "nodeSource" in item:
            if in_dict(("space", "externalId", "type"), item["nodeSource"]):
                yield ViewCRUD, ViewId.load(item["nodeSource"])
