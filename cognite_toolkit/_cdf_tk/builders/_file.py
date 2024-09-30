import copy
from pathlib import Path
from typing import Any

import yaml

from cognite_toolkit._cdf_tk.builders import Builder
from cognite_toolkit._cdf_tk.data_classes import (
    BuildVariables,
    BuiltResource,
    BuiltResourceList,
    ModuleLocation,
    SourceLocationEager,
)
from cognite_toolkit._cdf_tk.exceptions import ToolkitYAMLFormatError
from cognite_toolkit._cdf_tk.loaders import FileLoader, FileMetadataLoader
from cognite_toolkit._cdf_tk.utils import calculate_str_or_file_hash, read_yaml_content, safe_read, safe_write


class FileBuilder(Builder):
    _resource_folder = FileLoader.folder_name

    def _build_resources(
        self,
        source_path: Path,
        destination_path: Path,
        variables: BuildVariables,
        module: ModuleLocation,
        verbose: bool,
    ) -> BuiltResourceList:
        if verbose:
            self.console(f"Processing {source_path.name}")

        destination_path.parent.mkdir(parents=True, exist_ok=True)

        content = safe_read(source_path)

        location = SourceLocationEager(source_path, calculate_str_or_file_hash(content, shorten=True))

        content = variables.replace(content, source_path.suffix)

        if source_path.suffix.lower() in {".yaml", ".yml"}:
            content = self._expand_file_metadata(content, module)

        safe_write(destination_path, content)

        file_warnings, identifiers_kind_pairs = self.validate(content, source_path, destination_path)

        if file_warnings:
            self.warning_list.extend(file_warnings)
            # Here we do not use the self.warn method as we want to print the warnings as a group.
            if self.print_warning:
                print(str(file_warnings))

        return BuiltResourceList(
            [BuiltResource(identifier, location, kind, destination_path) for identifier, kind in identifiers_kind_pairs]
        )

    def _expand_file_metadata(self, raw_content: str, module: ModuleLocation) -> str:
        try:
            raw_list = read_yaml_content(raw_content)
        except yaml.YAMLError as e:
            raise ToolkitYAMLFormatError(f"Failed to load file definitions file {module.dir} due to: {e}")

        is_file_template = (
            isinstance(raw_list, list)
            and len(raw_list) == 1
            and FileMetadataLoader.template_pattern in raw_list[0].get("externalId", "")
        )
        if not is_file_template:
            return raw_content
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
        return yaml.safe_dump(expanded_metadata)
