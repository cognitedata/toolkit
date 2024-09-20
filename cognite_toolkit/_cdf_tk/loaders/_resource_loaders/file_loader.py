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

import json
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
    FilesAcl,
)
from cognite.client.exceptions import CogniteAPIError
from cognite.client.utils.useful_types import SequenceNotStr
from rich import print

from cognite_toolkit._cdf_tk._parameters import ANY_INT, ANY_STR, ParameterSpec, ParameterSpecSet
from cognite_toolkit._cdf_tk.constants import INDEX_PATTERN
from cognite_toolkit._cdf_tk.exceptions import (
    ToolkitFileNotFoundError,
    ToolkitRequiredValueError,
)
from cognite_toolkit._cdf_tk.loaders._base_loaders import ResourceContainerLoader, ResourceLoader
from cognite_toolkit._cdf_tk.utils import (
    CDFToolConfig,
    load_yaml_inject_variables,
)

from .auth_loaders import GroupAllScopedLoader, SecurityCategoryLoader
from .classic_loaders import AssetLoader
from .data_organization_loaders import DataSetsLoader, LabelLoader


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
    dependencies = frozenset({DataSetsLoader, GroupAllScopedLoader, LabelLoader, AssetLoader})

    _doc_url = "Files/operation/initFileUpload"

    @property
    def display_name(self) -> str:
        return "file_metadata"

    @classmethod
    def get_required_capability(cls, items: FileMetadataWriteList | None) -> Capability | list[Capability]:
        if not items and items is not None:
            return []
        scope: FilesAcl.Scope.All | FilesAcl.Scope.DataSet = FilesAcl.Scope.All()  # type: ignore[valid-type]
        if items:
            if data_set_ids := {item.data_set_id for item in items if item.data_set_id}:
                scope = FilesAcl.Scope.DataSet(list(data_set_ids))

        return FilesAcl([FilesAcl.Action.Read, FilesAcl.Action.Write], scope)  # type: ignore[arg-type]

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
            if "assetExternalIds" in resource:
                resource["assetIds"] = ToolGlobals.verify_asset(
                    resource["assetExternalIds"],
                    skip_validation,
                    action="replace assetExternalIds with assetIds in file metadata",
                )

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
