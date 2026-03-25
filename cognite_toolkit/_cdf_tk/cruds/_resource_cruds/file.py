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
from pathlib import Path
from typing import Any, Literal, final

from cognite_toolkit._cdf_tk.client._resource_base import Identifier
from cognite_toolkit._cdf_tk.client.identifiers import (
    ExternalId,
    InternalOrExternalId,
    NameId,
    NodeId,
    SpaceId,
)
from cognite_toolkit._cdf_tk.client.request_classes.filters import ClassicFilter
from cognite_toolkit._cdf_tk.client.resource_classes.cognite_file import CogniteFileRequest, CogniteFileResponse
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import InstanceSlimDefinition
from cognite_toolkit._cdf_tk.client.resource_classes.filemetadata import FileMetadataRequest, FileMetadataResponse
from cognite_toolkit._cdf_tk.client.resource_classes.group import (
    AclType,
    AllScope,
    DataModelInstancesAcl,
    DataSetScope,
    FilesAcl,
    ScopeDefinition,
    SpaceIDScope,
)
from cognite_toolkit._cdf_tk.cruds._base_cruds import ResourceContainerCRUD, ResourceCRUD
from cognite_toolkit._cdf_tk.exceptions import (
    ToolkitRequiredValueError,
)
from cognite_toolkit._cdf_tk.feature_flags import Flags
from cognite_toolkit._cdf_tk.utils import (
    calculate_hash,
    in_dict,
)
from cognite_toolkit._cdf_tk.utils.acl_helper import (
    as_instance_acl_actions,
    dataset_scoped_resource,
    space_scoped_resource,
)
from cognite_toolkit._cdf_tk.utils.diff_list import diff_list_hashable, diff_list_identifiable, dm_identifier
from cognite_toolkit._cdf_tk.utils.text import suffix_description
from cognite_toolkit._cdf_tk.utils.time import convert_data_modelling_timestamp
from cognite_toolkit._cdf_tk.yaml_classes import CogniteFileYAML, FileMetadataYAML

from .auth import GroupAllScopedCRUD, SecurityCategoryCRUD
from .classic import AssetCRUD
from .data_organization import DataSetsCRUD, LabelCRUD
from .datamodel import NodeCRUD, SpaceCRUD, ViewCRUD


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

    class _MetadataKey:
        filecontent_hash = "cognite-toolkit-hash"

    @property
    def display_name(self) -> str:
        return "file metadata"

    @classmethod
    def get_minimum_scope(cls, items: Sequence[FileMetadataRequest]) -> ScopeDefinition:
        return dataset_scoped_resource(items)

    @classmethod
    def create_acl(cls, actions: set[Literal["READ", "WRITE"]], scope: ScopeDefinition) -> Iterable[AclType]:
        if isinstance(scope, AllScope | DataSetScope):
            yield FilesAcl(actions=sorted(actions), scope=scope)

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
            yield DataSetsCRUD, ExternalId(external_id=item["dataSetExternalId"])
        if "securityCategoryNames" in item:
            for security_category in item["securityCategoryNames"]:
                yield SecurityCategoryCRUD, NameId(name=security_category)
        if "labels" in item:
            for label in item["labels"]:
                if isinstance(label, dict):
                    yield LabelCRUD, ExternalId(external_id=label["externalId"])
                elif isinstance(label, str):
                    yield LabelCRUD, ExternalId(external_id=label)
        for asset_external_id in item.get("assetExternalIds", []):
            yield AssetCRUD, ExternalId(external_id=asset_external_id)

    @classmethod
    def get_dependencies(cls, resource: FileMetadataYAML) -> Iterable[tuple[type[ResourceCRUD], Identifier]]:
        if resource.data_set_external_id:
            yield DataSetsCRUD, ExternalId(external_id=resource.data_set_external_id)
        for security_category in resource.security_categories or []:
            yield SecurityCategoryCRUD, NameId(name=security_category)
        for label in resource.labels or []:
            if isinstance(label, dict):
                yield LabelCRUD, ExternalId(external_id=label["externalId"])
        for asset_external_id in resource.asset_external_ids or []:
            yield AssetCRUD, ExternalId(external_id=asset_external_id)

    def load_resource_file(
        self, filepath: Path, environment_variables: dict[str, str | None] | None = None
    ) -> list[dict[str, Any]]:
        raw_files = super().load_resource_file(filepath, environment_variables)
        stem = filepath.stem
        if stem.lower().endswith(self.kind.lower()):
            stem = stem[: -len(self.kind)]
        for item in raw_files:
            source_file = item.get("$FILEPATH")
            if source_file is None:
                if candidate := next((filepath.parent.rglob(f"{stem}*")), None):
                    source_file = candidate
                elif isinstance(name := item.get("name"), str) and (filepath.parent / name).exists():
                    source_file = filepath.parent / name
                else:
                    # No filepath found
                    continue
            if Flags.v08.is_enabled():
                file_hash = calculate_hash(source_file, shorten=True)
                if "metadata" not in item:
                    item["metadata"] = {}
                # Store hash for efficient diffing
                item["metadata"][self._MetadataKey.filecontent_hash] = file_hash
            item["$FILEPATH"] = source_file
        return raw_files

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
        responses = self.client.tool.filemetadata.create(items, overwrite=True)
        if Flags.v08.is_enabled():
            for response in responses:
                self._try_upload_file_content(response)
        return responses

    def _try_upload_file_content(self, response: FileMetadataResponse) -> None:
        if response.filepath and response.upload_url:
            self.client.tool.filemetadata.upload_file(response.filepath, response.upload_url, response.mime_type)

    def retrieve(self, ids: Sequence[ExternalId]) -> list[FileMetadataResponse]:
        return self.client.tool.filemetadata.retrieve(list(ids), ignore_unknown_ids=True)

    def update(self, items: Sequence[FileMetadataRequest]) -> list[FileMetadataResponse]:
        responses = self.client.tool.filemetadata.update(items, mode="replace")
        if Flags.v08.is_enabled():
            response_by_external_id = {
                response.external_id: response for response in responses if response.external_id is not None
            }
            for item in items:
                if not (item.external_id and item.external_id in response_by_external_id):
                    continue
                response = response_by_external_id[item.external_id]
                if (
                    item.filepath
                    and (response.metadata or {}).get(self._MetadataKey.filecontent_hash)
                    != (item.metadata or {})[self._MetadataKey.filecontent_hash]
                ):
                    # Need to reupload the file content
                    responses_with_url = self.client.tool.filemetadata.get_upload_url([item.as_id()])
                    if len(responses_with_url) != 0:
                        raise RuntimeError(
                            f"Expected to get one upload url for file with external id {item.external_id}, but got {len(responses_with_url)}"
                        )
                    self._try_upload_file_content(responses_with_url[0])
        return responses

    def delete(self, ids: Sequence[InternalOrExternalId]) -> int:
        if not ids:
            return 0
        self.client.tool.filemetadata.delete(list(ids), ignore_unknown_ids=True)
        return len(ids)

    def _iterate(
        self,
        data_set_external_id: str | None = None,
        space: str | None = None,
        parent_ids: Sequence[Hashable] | None = None,
    ) -> Iterable[FileMetadataResponse]:
        filter_ = ClassicFilter.from_asset_subtree_and_data_sets(data_set_id=data_set_external_id)
        for files in self.client.tool.filemetadata.iterate(filter=filter_, limit=None):
            yield from files

    def count(self, ids: Sequence[ExternalId]) -> int:
        return sum(
            1 for meta in self.client.tool.filemetadata.retrieve(list(ids), ignore_unknown_ids=True) if meta.uploaded
        )

    def drop_data(self, ids: Sequence[ExternalId]) -> int:
        existing = self.client.tool.filemetadata.retrieve(list(ids), ignore_unknown_ids=True)
        # File and FileMetadata is tightly coupled, so we need to delete the metadata and recreate it
        # without the source set to delete the file.
        deleted_files = self.delete([meta.as_id() for meta in existing])
        self.create([meta.as_request_resource() for meta in existing])
        return deleted_files


@final
class CogniteFileCRUD(ResourceContainerCRUD[NodeId, CogniteFileRequest, CogniteFileResponse]):
    template_pattern = "$FILENAME"
    item_name = "file contents"
    folder_name = "files"
    kind = "CogniteFile"
    resource_cls = CogniteFileResponse
    resource_write_cls = CogniteFileRequest
    yaml_cls = CogniteFileYAML
    dependencies = frozenset({GroupAllScopedCRUD, SpaceCRUD, ViewCRUD})

    _doc_url = "Files/operation/initFileUpload"
    # 128,000 bytes (UTF-8) is the maximum, worse case utf-8 will use 4 bytes per character
    TEXT_FIELD_MAX_LENGTH = 32_000

    class _SourceContextKey:
        filecontent_hash = "cognite-toolkit-hash"

    @property
    def display_name(self) -> str:
        return "cognite files"

    @classmethod
    def get_id(cls, item: CogniteFileResponse | CogniteFileRequest | dict) -> NodeId:
        if isinstance(item, dict):
            if missing := tuple(k for k in {"space", "externalId"} if k not in item):
                # We need to raise a KeyError with all missing keys to get the correct error message.
                raise KeyError(*missing)
            return NodeId(space=item["space"], external_id=item["externalId"])
        return NodeId(space=item.space, external_id=item.external_id)

    @classmethod
    def dump_id(cls, id: NodeId) -> dict[str, Any]:
        return id.dump()

    @classmethod
    def get_minimum_scope(cls, items: Sequence[CogniteFileRequest]) -> ScopeDefinition:
        return space_scoped_resource(items)

    @classmethod
    def create_acl(cls, actions: set[Literal["READ", "WRITE"]], scope: ScopeDefinition) -> Iterable[AclType]:
        yield FilesAcl(actions=sorted(actions), scope=AllScope())
        if isinstance(scope, AllScope | SpaceIDScope):
            yield DataModelInstancesAcl(actions=as_instance_acl_actions(actions), scope=scope)

    def load_resource_file(
        self, filepath: Path, environment_variables: dict[str, str | None] | None = None
    ) -> list[dict[str, Any]]:
        raw_files = super().load_resource_file(filepath, environment_variables)
        stem = filepath.stem
        if stem.lower().endswith(self.kind.lower()):
            stem = stem[: -len(self.kind)]
        for item in raw_files:
            source_file = item.get("$FILEPATH")
            if source_file is None:
                if candidate := next((filepath.parent.rglob(f"{stem}*")), None):
                    source_file = candidate
                elif isinstance(name := item.get("name"), str) and (filepath.parent / name).exists():
                    source_file = filepath.parent / name
                else:
                    # No filepath found
                    continue
            if Flags.v08.is_enabled():
                file_hash = calculate_hash(source_file, shorten=True)
                extra_str = f"{self._SourceContextKey.filecontent_hash}: {file_hash}"
                # Store hash on source_context for efficient diffing
                item["sourceContext"] = suffix_description(
                    extra_str,
                    item.get("sourceContext"),
                    self.TEXT_FIELD_MAX_LENGTH,
                    self.get_id(item),
                    self.display_name,
                    self.client.console,
                )
            item["$FILEPATH"] = source_file
        return raw_files

    def dump_resource(self, resource: CogniteFileResponse, local: dict[str, Any] | None = None) -> dict[str, Any]:
        dumped = resource.as_request_resource().dump(context="toolkit")
        local = local or {}
        if "existingVersion" not in local:
            # Existing version is typically not set when creating nodes, but we get it back
            # when we retrieve the node from the server.
            dumped.pop("existingVersion", None)
        for key in list(dumped.keys()):
            value = dumped[key]
            if key not in local:
                if value is None or value in ([], {}):
                    dumped.pop(key)
                continue
            local_value = local[key]
            if isinstance(local_value, datetime) and isinstance(value, str):
                dumped[key] = convert_data_modelling_timestamp(value)
            elif isinstance(local_value, date) and isinstance(value, str):
                dumped[key] = date.fromisoformat(value)

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

    def create(self, items: Sequence[CogniteFileRequest]) -> list[InstanceSlimDefinition]:
        responses = self.client.tool.cognite_files.create(items)
        if Flags.v08.is_enabled():
            for item in items:
                self._try_upload_file_content(item)
        return responses

    def _try_upload_file_content(self, item: CogniteFileRequest) -> None:
        if item.filepath:
            upload_urls = self.client.tool.filemetadata.get_upload_url([item.as_instance_id()])
            if upload_urls and upload_urls[0].upload_url:
                self.client.tool.filemetadata.upload_file(item.filepath, upload_urls[0].upload_url, item.mime_type)

    def retrieve(self, ids: Sequence[NodeId]) -> list[CogniteFileResponse]:
        return self.client.tool.cognite_files.retrieve(
            [NodeId(space=id_.space, external_id=id_.external_id) for id_ in ids]
        )

    def update(self, items: Sequence[CogniteFileRequest]) -> list[InstanceSlimDefinition]:
        responses = self.client.tool.cognite_files.create(items)
        if Flags.v08.is_enabled():
            items_by_id = {
                response.as_id(): response
                # We know that file responses will always be NodeIds.
                for response in self.client.tool.cognite_files.retrieve([item.as_id() for item in responses])  # type:ignore[misc]
            }
            for item in items:
                if not item.filepath:
                    continue
                existing_file = items_by_id.get(item.as_id())
                # Check if content hash has changed
                if existing_file and existing_file.source_context == item.source_context:
                    # Content hasn't changed, skip upload
                    continue
                # Need to upload the file content
                self._try_upload_file_content(item)
        return responses

    def delete(self, ids: Sequence[NodeId]) -> int:
        return len(
            self.client.tool.cognite_files.delete([NodeId(space=id_.space, external_id=id_.external_id) for id_ in ids])
        )

    def _iterate(
        self,
        data_set_external_id: str | None = None,
        space: str | None = None,
        parent_ids: Sequence[Hashable] | None = None,
    ) -> Iterable[CogniteFileResponse]:
        # We do not have a way to know the source of the file, so we cannot filter on that.
        return []

    def count(self, ids: Sequence[NodeId]) -> int:
        return sum(bool(file.is_uploaded or False) for file in self.retrieve(ids))

    def drop_data(self, ids: Sequence[NodeId]) -> int:
        # Deleting and recreating the file, will remove the file contents but keep the metadata.
        retrieved = self.retrieve(ids)
        self.delete(ids)
        return len(self.create([file.as_request_resource() for file in retrieved]))

    @classmethod
    def get_dependent_items(cls, item: dict) -> Iterable[tuple[type[ResourceCRUD], Hashable]]:
        """Returns all items that this item requires.

        For example, a TimeSeries requires a DataSet, so this method would return the
        DatasetLoader and identifier of that dataset.
        """
        if "space" in item:
            yield SpaceCRUD, SpaceId(space=item["space"])
        for key in ["source", "category", "type"]:
            if key in item and in_dict(("space", "externalId"), item[key]):
                yield NodeCRUD, NodeId(space=item[key]["space"], external_id=item[key]["externalId"])
        if "assets" in item:
            for asset in item["assets"]:
                if isinstance(asset, dict) and in_dict(("space", "externalId"), asset):
                    yield NodeCRUD, NodeId(space=asset["space"], external_id=asset["externalId"])

    @classmethod
    def get_dependencies(cls, resource: CogniteFileYAML) -> Iterable[tuple[type[ResourceCRUD], Identifier]]:
        yield SpaceCRUD, SpaceId(space=resource.space)
        for ref in [resource.source, resource.category, resource.type]:
            if ref:
                yield NodeCRUD, NodeId(space=ref.space, external_id=ref.external_id)
        for asset in resource.assets or []:
            yield NodeCRUD, NodeId(space=asset.space, external_id=asset.external_id)
