import copy
from collections.abc import Iterable
from typing import Any

from cognite_toolkit._cdf_tk.builders import Builder
from cognite_toolkit._cdf_tk.data_classes import (
    BuildDestinationFile,
    BuildSourceFile,
    ModuleLocation,
)
from cognite_toolkit._cdf_tk.exceptions import ToolkitYAMLFormatError
from cognite_toolkit._cdf_tk.loaders import FileLoader, FileMetadataLoader


class FileBuilder(Builder):
    _resource_folder = FileMetadataLoader.folder_name

    def build(self, source_files: list[BuildSourceFile], module: ModuleLocation) -> Iterable[BuildDestinationFile]:
        for source_file in source_files:
            if source_file.loaded is None:
                continue
            loaded = self._expand_file_metadata(source_file.loaded, module)
            destination_path = self._create_destination_path(source_file.source.path, module.dir)

            yield BuildDestinationFile(
                path=destination_path,
                loaded=loaded,
                loader=FileMetadataLoader,
                source=source_file.source,
                extra_sources=None,
            )

    def _expand_file_metadata(
        self, raw_list: list[dict[str, Any]] | dict[str, Any], module: ModuleLocation
    ) -> list[dict[str, Any]] | dict[str, Any]:
        is_file_template = (
            isinstance(raw_list, list)
            and len(raw_list) == 1
            and FileMetadataLoader.template_pattern in raw_list[0].get("externalId", "")
        )
        if not is_file_template:
            return raw_list
        if not (isinstance(raw_list, list) and raw_list and isinstance(raw_list[0], dict)):
            raise ToolkitYAMLFormatError(
                f"Expected a list with a single dictionary in the file metadata file {module.dir}, "
                f"but got {type(raw_list).__name__}"
            )
        template = raw_list[0]
        if self.verbose:
            self.console(
                f"Detected file template name {FileMetadataLoader.template_pattern!r} in {module.relative_path.as_posix()!r}"
                f"Expanding file metadata..."
            )
        expanded_metadata: list[dict[str, Any]] = []
        for filepath in module.source_paths_by_resource_folder[FileLoader.folder_name]:
            if not FileLoader.is_supported_file(filepath):
                continue
            new_entry = copy.deepcopy(template)
            new_entry["externalId"] = new_entry["externalId"].replace(
                FileMetadataLoader.template_pattern, filepath.name
            )
            new_entry["name"] = filepath.name
            expanded_metadata.append(new_entry)
        return expanded_metadata
