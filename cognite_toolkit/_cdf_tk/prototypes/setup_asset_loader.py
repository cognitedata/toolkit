import json
import re
import sys
from collections.abc import Hashable, Iterable
from functools import lru_cache
from pathlib import Path
from typing import Any, Literal

from cognite.client.data_classes import FileMetadataWriteList, TimeSeriesWriteList

import cognite_toolkit._cdf_tk.loaders as loaders
from cognite_toolkit._cdf_tk._parameters import ANY_INT, ParameterSpec, ParameterSpecSet
from cognite_toolkit._cdf_tk.exceptions import ToolkitFileNotFoundError
from cognite_toolkit._cdf_tk.loaders import (
    LOADER_BY_FOLDER_NAME,
    LOADER_LIST,
    RESOURCE_LOADER_LIST,
    FileMetadataLoader,
    ResourceLoader,
    TimeSeriesLoader,
)
from cognite_toolkit._cdf_tk.prototypes.resource_loaders import AssetLoader
from cognite_toolkit._cdf_tk.utils import CDFToolConfig, load_yaml_inject_variables

if sys.version_info >= (3, 10):
    from typing import TypeAlias
else:
    from typing_extensions import TypeAlias

ResourceTypes: TypeAlias = Literal[
    "assets",
    "auth",
    "data_models",
    "data_sets",
    "labels",
    "transformations",
    "files",
    "timeseries",
    "timeseries_datapoints",
    "extraction_pipelines",
    "functions",
    "raw",
    "robotics",
    "workflows",
]

_HAS_SETUP = False


def setup_asset_loader() -> None:
    """Set up the asset loader to be used by the Cognite Toolkit."""
    global _HAS_SETUP
    if _HAS_SETUP:
        return
    LOADER_BY_FOLDER_NAME["assets"] = [AssetLoader]
    LOADER_LIST.append(AssetLoader)
    RESOURCE_LOADER_LIST.append(AssetLoader)
    setattr(loaders, "ResourceTypes", ResourceTypes)
    _modify_timeseries_loader()
    _modify_file_metadata_loader()
    _HAS_SETUP = True


def _modify_timeseries_loader() -> None:
    TimeSeriesLoader.dependencies = frozenset({AssetLoader} | TimeSeriesLoader.dependencies)
    timeseries_spec_method_original = TimeSeriesLoader.get_write_cls_parameter_spec

    @lru_cache(maxsize=1)
    def get_write_cls_parameter_spec(cls: type[TimeSeriesLoader]) -> ParameterSpecSet:
        nonlocal timeseries_spec_method_original
        spec = timeseries_spec_method_original()
        spec.add(ParameterSpec(("assetExternalId",), frozenset({"str"}), is_required=False, _is_nullable=False))
        spec.discard(ParameterSpec(("assetId",), frozenset({"int"}), is_required=False, _is_nullable=False))
        return spec

    TimeSeriesLoader.get_write_cls_parameter_spec = classmethod(get_write_cls_parameter_spec)  # type: ignore[method-assign, assignment, arg-type]

    timeseries_get_dependent_items_original = TimeSeriesLoader.get_dependent_items

    def get_dependent_items(cls: type[TimeSeriesLoader], item: dict) -> Iterable[tuple[type[ResourceLoader], Hashable]]:
        nonlocal timeseries_get_dependent_items_original
        yield from timeseries_get_dependent_items_original(item)

        if "assetExternalId" in item:
            yield AssetLoader, item["assetExternalId"]

    TimeSeriesLoader.get_dependent_items = classmethod(get_dependent_items)  # type: ignore[method-assign, assignment, arg-type]

    def load_resource(
        self: TimeSeriesLoader, filepath: Path, ToolGlobals: CDFToolConfig, skip_validation: bool
    ) -> TimeSeriesWriteList:
        resources = load_yaml_inject_variables(filepath, ToolGlobals.environment_variables())
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
            if "assetExternalId" in resource:
                asset_external_id = resource.pop("assetExternalId")
                resource["assetId"] = ToolGlobals.verify_asset(
                    asset_external_id, skip_validation, action="replace assetExternalId with assetId in time series"
                )
        return TimeSeriesWriteList.load(resources)

    TimeSeriesLoader.load_resource = load_resource  # type: ignore[method-assign]


def _modify_file_metadata_loader() -> None:
    FileMetadataLoader.dependencies = frozenset({AssetLoader} | FileMetadataLoader.dependencies)
    file_metadata_spec_method_original = FileMetadataLoader.get_write_cls_parameter_spec

    @lru_cache(maxsize=1)
    def get_write_cls_parameter_spec_file_metadata(cls: type[FileMetadataLoader]) -> ParameterSpecSet:
        nonlocal file_metadata_spec_method_original
        spec = file_metadata_spec_method_original()
        spec.add(ParameterSpec(("assetExternalIds",), frozenset({"list"}), is_required=False, _is_nullable=False))
        spec.add(
            ParameterSpec(("assetExternalIds", ANY_INT), frozenset({"int"}), is_required=False, _is_nullable=False)
        )
        spec.discard(ParameterSpec(("assetIds",), frozenset({"list"}), is_required=False, _is_nullable=False))
        spec.discard(ParameterSpec(("assetIds", ANY_INT), frozenset({"str"}), is_required=False, _is_nullable=False))
        return spec

    FileMetadataLoader.get_write_cls_parameter_spec = classmethod(get_write_cls_parameter_spec_file_metadata)  # type: ignore[method-assign, assignment, arg-type]

    file_metadata_get_dependent_items_original = FileMetadataLoader.get_dependent_items

    def get_dependent_items(
        cls: type[FileMetadataLoader], item: dict
    ) -> Iterable[tuple[type[ResourceLoader], Hashable]]:
        nonlocal file_metadata_get_dependent_items_original
        yield from file_metadata_get_dependent_items_original(item)

        for asset_external_id in item.get("assetExternalIds", []):
            yield AssetLoader, asset_external_id

    FileMetadataLoader.get_dependent_items = classmethod(get_dependent_items)  # type: ignore[method-assign, assignment, arg-type]

    def load_resource(
        self: FileMetadataLoader, filepath: Path, ToolGlobals: CDFToolConfig, skip_validation: bool
    ) -> FileMetadataWriteList:
        loaded = load_yaml_inject_variables(filepath, ToolGlobals.environment_variables())
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
            for file in filepath.parent.glob("*"):
                if file.suffix in [".yaml", ".yml"]:
                    continue
                # Deep Copy
                new_file = json.loads(json.dumps(template))
                # We modify the filename in the build command, we clean the name here to get the original filename
                filename_in_module = (
                    re.sub("^[0-9]+\\.", "", file.name).removeprefix(template_prefix).removesuffix(template_suffix)
                )
                new_file["name"] = file.name
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
            if meta.name and not Path(filepath.parent / meta.name).exists():
                raise ToolkitFileNotFoundError(
                    f"Could not find file {meta.name} referenced in filepath {filepath.name}"
                )
        return files_metadata

    FileMetadataLoader.load_resource = load_resource  # type: ignore[method-assign]
