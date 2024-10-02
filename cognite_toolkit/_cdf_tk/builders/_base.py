import difflib
import re
from collections import defaultdict
from collections.abc import Hashable, Sequence
from pathlib import Path
from typing import Any, ClassVar

import yaml

from cognite_toolkit._cdf_tk.constants import INDEX_PATTERN, TEMPLATE_VARS_FILE_SUFFIXES
from cognite_toolkit._cdf_tk.data_classes import (
    BuildVariables,
    BuiltResource,
    BuiltResourceList,
    ModuleLocation,
    SourceLocationEager,
)
from cognite_toolkit._cdf_tk.exceptions import (
    AmbiguousResourceFileError,
    ToolkitYAMLFormatError,
)
from cognite_toolkit._cdf_tk.loaders import (
    LOADER_BY_FOLDER_NAME,
    GroupLoader,
    Loader,
    RawDatabaseLoader,
    RawTableLoader,
    ResourceLoader,
)
from cognite_toolkit._cdf_tk.tk_warnings import (
    ToolkitNotSupportedWarning,
    ToolkitWarning,
    WarningList,
)
from cognite_toolkit._cdf_tk.tk_warnings.fileread import (
    DuplicatedItemWarning,
    FileReadWarning,
    MissingRequiredIdentifierWarning,
    UnknownResourceTypeWarning,
    UnresolvedVariableWarning,
)
from cognite_toolkit._cdf_tk.utils import (
    calculate_str_or_file_hash,
    humanize_collection,
    module_from_path,
    quote_int_value_by_key_in_yaml,
    safe_read,
    safe_write,
)
from cognite_toolkit._cdf_tk.validation import validate_data_set_is_set, validate_resource_yaml


class Builder:
    _resource_folder: ClassVar[str | None] = None

    def __init__(
        self,
        build_dir: Path,
        module_names_by_variable_key: dict[str, list[str]],
        silent: bool,
        verbose: bool,
        resource_folder: str | None = None,
    ):
        self.build_dir = build_dir
        self.silent = silent
        self.warning_list = WarningList[ToolkitWarning]()
        self.verbose = verbose

        self.resource_counter = 0
        self._warning_index = 0
        self.index_by_filepath_stem: dict[Path, int] = {}
        self.ids_by_resource_type: dict[type[ResourceLoader], dict[Hashable, Path]] = defaultdict(dict)
        self.dependencies_by_required: dict[tuple[type[ResourceLoader], Hashable], list[tuple[Hashable, Path]]] = (
            defaultdict(list)
        )
        self._module_names_by_variable_key = module_names_by_variable_key
        if self._resource_folder is not None:
            self.resource_folder = self._resource_folder
        elif resource_folder is not None:
            self.resource_folder = resource_folder
        else:
            raise ValueError("Either _resource_folder or resource_folder must be set.")

    @property
    def print_warning(self) -> bool:
        return not self.silent

    def warn(self, warning: ToolkitWarning) -> None:
        self.warning_list.append(warning)
        if not self.silent:
            warning.print_warning()

    def console(self, message: str, prefix: str = "[bold green]INFO:[/] ") -> None:
        if not self.silent:
            print(f"{prefix}{message}")

    def build_resource_folder(
        self, resource_files: Sequence[Path], module_variables: BuildVariables, module: ModuleLocation
    ) -> BuiltResourceList[Hashable]:
        self._warning_index = len(self.warning_list)
        built_resource_list = BuiltResourceList[Hashable]()

        for source_path in resource_files:
            if source_path.suffix.lower() not in TEMPLATE_VARS_FILE_SUFFIXES:
                continue

            built_resources = self._build_resources(source_path, module_variables, module, self.verbose)

            built_resource_list.extend(built_resources)

        return built_resource_list

    def last_build_warnings(self) -> WarningList[ToolkitWarning]:
        return self.warning_list[self._warning_index :]

    def _build_resources(
        self,
        source_path: Path,
        variables: BuildVariables,
        module: ModuleLocation,
        verbose: bool,
    ) -> BuiltResourceList:
        if verbose:
            self.console(f"Processing {source_path.name}")

        content = safe_read(source_path)
        location = SourceLocationEager(source_path, calculate_str_or_file_hash(content, shorten=True))

        content = variables.replace(content, source_path.suffix)

        destination_path = self._create_destination_path(source_path, module.dir)

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

    def _create_destination_path(self, source_path: Path, module_dir: Path) -> Path:
        """Creates the filepath in the build directory for the given source path.

        Note that this is a complex operation as the modules in the source are nested while the build directory is flat.
        This means that we lose information and risk having duplicate filenames. To avoid this, we prefix the filename
        with a number to ensure uniqueness.
        """
        filename = source_path.name
        # Get rid of the local index
        filename = INDEX_PATTERN.sub("", filename)

        relative_stem = module_dir.name / source_path.relative_to(module_dir).parent / source_path.stem
        if relative_stem in self.index_by_filepath_stem:
            # Ensure extra files (.sql, .pdf) with the same stem gets the same index as the
            # main YAML file. The Transformation Loader expects this.
            index = self.index_by_filepath_stem[relative_stem]
        else:
            # Increment to ensure we do not get duplicate filenames when we flatten the file
            # structure from the module to the build directory.
            self.resource_counter += 1
            index = self.resource_counter
            self.index_by_filepath_stem[relative_stem] = index

        filename = f"{index}.{filename}"
        destination_path = self.build_dir / self.resource_folder / filename
        destination_path.parent.mkdir(parents=True, exist_ok=True)
        return destination_path

    def validate(
        self,
        content: str,
        source_path: Path,
        destination: Path,
    ) -> tuple[WarningList[FileReadWarning], list[tuple[Hashable, str]]]:
        warning_list = WarningList[FileReadWarning]()

        module = module_from_path(source_path)

        warnings = self._all_variables_replaced(content, module, source_path)
        warning_list.extend(warnings)

        if destination.suffix not in {".yaml", ".yml"}:
            return warning_list, []

        # Ensure that all keys that are version gets read as strings.
        # This is required by DataModels, Views, and Transformations that reference DataModels and Views.
        content = quote_int_value_by_key_in_yaml(content, key="version")
        try:
            parsed = yaml.safe_load(content)
        except yaml.YAMLError as e:
            if self.print_warning:
                print(str(warning_list))
            raise ToolkitYAMLFormatError(
                f"YAML validation error for {destination.name} after substituting config variables: {e}"
            )

        loader = self._get_loader(self.resource_folder, destination, source_path)
        if loader is None or not issubclass(loader, ResourceLoader):
            return warning_list, []

        is_dict_item = isinstance(parsed, dict)
        items = [parsed] if is_dict_item else parsed

        identifier_kind_pairs: list[tuple[Hashable, str]] = []
        for no, item in enumerate(items, 1):
            element_no = None if is_dict_item else no

            identifier: Any | None = None
            # Raw Tables and Raw Databases can have different loaders in the same file.
            item_loader = loader
            try:
                identifier = item_loader.get_id(item)
            except KeyError as error:
                if loader is RawTableLoader:
                    try:
                        identifier = RawDatabaseLoader.get_id(item)
                        item_loader = RawDatabaseLoader
                    except KeyError:
                        warning_list.append(
                            MissingRequiredIdentifierWarning(source_path, element_no, tuple(), error.args)
                        )
                else:
                    warning_list.append(MissingRequiredIdentifierWarning(source_path, element_no, tuple(), error.args))

            if identifier:
                identifier_kind_pairs.append((identifier, item_loader.kind))
                if first_seen := self.ids_by_resource_type[item_loader].get(identifier):
                    warning_list.append(DuplicatedItemWarning(source_path, identifier, first_seen))
                else:
                    self.ids_by_resource_type[item_loader][identifier] = source_path

                for dependency in item_loader.get_dependent_items(item):
                    self.dependencies_by_required[dependency].append((identifier, source_path))

            api_spec = item_loader.safe_get_write_cls_parameter_spec()
            if api_spec is not None:
                resource_warnings = validate_resource_yaml(parsed, api_spec, source_path, element_no)
                warning_list.extend(resource_warnings)

            data_set_warnings = validate_data_set_is_set(items, loader.resource_cls, source_path)
            warning_list.extend(data_set_warnings)

        return warning_list, identifier_kind_pairs

    def _all_variables_replaced(self, content: str, module: str, source_path: Path) -> WarningList[FileReadWarning]:
        all_unmatched = re.findall(pattern=r"\{\{.*?\}\}", string=content)
        warning_list = WarningList[FileReadWarning]()
        for unmatched in all_unmatched:
            warning_list.append(UnresolvedVariableWarning(source_path, unmatched))
            variable = unmatched[2:-2]
            if module_names := self._module_names_by_variable_key.get(variable):
                module_str = (
                    f"{module_names[0]!r}"
                    if len(module_names) == 1
                    else (", ".join(module_names[:-1]) + f" or {module_names[-1]}")
                )
                self.console(
                    f"The variables in 'config.[ENV].yaml' need to be organised in a tree structure following"
                    f"\n    the folder structure of the modules, but can also be moved up the config hierarchy to be shared between modules."
                    f"\n    The variable {variable!r} is defined in the variable section{'s' if len(module_names) > 1 else ''} {module_str}."
                    f"\n    Check that {'these paths reflect' if len(module_names) > 1 else 'this path reflects'} the location of {module}.",
                    prefix="    [bold green]Hint:[/] ",
                )
        return warning_list

    def _get_loader(self, resource_folder: str, destination: Path, source_path: Path) -> type[Loader] | None:
        folder_loaders = LOADER_BY_FOLDER_NAME.get(resource_folder, [])
        if not folder_loaders:
            self.warn(
                ToolkitNotSupportedWarning(
                    f"resource of type {resource_folder!r} in {source_path.name}.",
                    details=f"Available resources are: {', '.join(LOADER_BY_FOLDER_NAME.keys())}",
                )
            )
            return None

        loaders = [loader for loader in folder_loaders if loader.is_supported_file(destination)]
        if len(loaders) == 0:
            suggestion: str | None = None
            if "." in source_path.stem:
                core, kind = source_path.stem.rsplit(".", 1)
                match = difflib.get_close_matches(kind, [loader.kind for loader in folder_loaders])
                if match:
                    suggested_name = f"{core}.{match[0]}{source_path.suffix}"
                    suggestion = f"Did you mean to call the file {suggested_name!r}?"
            else:
                kinds = [loader.kind for loader in folder_loaders]
                if len(kinds) == 1:
                    suggestion = f"Did you mean to call the file '{source_path.stem}.{kinds[0]}{source_path.suffix}'?"
                else:
                    suggestion = (
                        f"All files in the {resource_folder!r} folder must have a file extension that matches "
                        f"the resource type. Supported types are: {humanize_collection(kinds)}."
                    )
            self.warn(UnknownResourceTypeWarning(source_path, suggestion))
            return None
        elif len(loaders) > 1 and all(loader.folder_name == "raw" for loader in loaders):
            # Multiple raw loaders load from the same file.
            return RawTableLoader
        elif len(loaders) > 1 and all(issubclass(loader, GroupLoader) for loader in loaders):
            # There are two group loaders, one for resource scoped and one for all scoped.
            return GroupLoader
        elif len(loaders) > 1:
            names = " or ".join(f"{destination.stem}.{loader.kind}{destination.suffix}" for loader in loaders)
            raise AmbiguousResourceFileError(
                f"Ambiguous resource file {destination.name} in {destination.parent.name} folder. "
                f"Unclear whether it is {' or '.join(loader.kind for loader in loaders)}."
                f"\nPlease name the file {names}."
            )

        return loaders[0]
