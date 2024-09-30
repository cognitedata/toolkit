import copy
import difflib
import re
import shutil
import traceback
from collections import defaultdict
from collections.abc import Callable, Hashable, Sequence
from functools import partial
from pathlib import Path
from typing import Any, ClassVar

import yaml

from cognite_toolkit._cdf_tk._parameters import ParameterSpecSet
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
    ToolkitFileExistsError,
    ToolkitNotADirectoryError,
    ToolkitYAMLFormatError,
)
from cognite_toolkit._cdf_tk.loaders import (
    LOADER_BY_FOLDER_NAME,
    FileLoader,
    FileMetadataLoader,
    FunctionLoader,
    GroupLoader,
    Loader,
    RawDatabaseLoader,
    RawTableLoader,
    ResourceLoader,
)
from cognite_toolkit._cdf_tk.loaders.data_classes import RawDatabaseTable
from cognite_toolkit._cdf_tk.tk_warnings import (
    HighSeverityWarning,
    LowSeverityWarning,
    MediumSeverityWarning,
    ToolkitBugWarning,
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
    read_yaml_content,
    resource_folder_from_path,
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
        build_plugin = {
            FileMetadataLoader.folder_name: partial(self._expand_file_metadata, module=module, verbose=self.verbose),
        }.get(self.resource_folder)

        built_resource_list = BuiltResourceList[Hashable]()
        for source_path in resource_files:
            if source_path.suffix.lower() not in TEMPLATE_VARS_FILE_SUFFIXES or self._is_exception_file(
                source_path, self.resource_folder
            ):
                continue

            destination = self._create_destination_path(source_path, self.resource_folder, module.dir, self.build_dir)

            built_resources = self._build_resources(
                source_path, destination, module_variables, build_plugin, self.verbose
            )

            built_resource_list.extend(built_resources)

        return built_resource_list

    def last_build_warnings(self) -> WarningList[ToolkitWarning]:
        return self.warning_list[self._warning_index :]

    def _build_resources(
        self,
        source_path: Path,
        destination_path: Path,
        variables: BuildVariables,
        build_plugin: Callable[[str], str] | None,
        verbose: bool,
    ) -> BuiltResourceList:
        if verbose:
            self.console(f"Processing {source_path.name}")

        destination_path.parent.mkdir(parents=True, exist_ok=True)

        content = safe_read(source_path)

        location = SourceLocationEager(source_path, calculate_str_or_file_hash(content, shorten=True))

        content = variables.replace(content, source_path.suffix)
        if build_plugin is not None and source_path.suffix.lower() in {".yaml", ".yml"}:
            content = build_plugin(content)

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

    def _create_destination_path(
        self, source_path: Path, resource_folder_name: str, module_dir: Path, build_dir: Path
    ) -> Path:
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
        destination_path = build_dir / resource_folder_name / filename
        destination_path.parent.mkdir(parents=True, exist_ok=True)
        return destination_path

    @staticmethod
    def _is_exception_file(filepath: Path, resource_directory: str) -> bool:
        # In the 'functions' resource directories, all `.yaml` files must be in the root of the directory
        # This is to allow for function code to include arbitrary yaml files.
        # In addition, all files in not int the 'functions' directory are considered other files.
        return resource_directory == FunctionLoader.folder_name and filepath.parent.name != FunctionLoader.folder_name

    def _expand_file_metadata(self, raw_content: str, module: ModuleLocation, verbose: bool) -> str:
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
        if verbose:
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

    def validate(
        self,
        content: str,
        source_path: Path,
        destination: Path,
    ) -> tuple[WarningList[FileReadWarning], list[tuple[Hashable, str]]]:
        warning_list = WarningList[FileReadWarning]()
        module = module_from_path(source_path)
        resource_folder = resource_folder_from_path(source_path)

        all_unmatched = re.findall(pattern=r"\{\{.*?\}\}", string=content)
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

        loader = self._get_loader(resource_folder, destination, source_path)
        if loader is None or not issubclass(loader, ResourceLoader):
            return warning_list, []

        api_spec = self._get_api_spec(loader, destination)
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
                if item_loader is RawTableLoader:
                    database = RawDatabaseTable(identifier.db_name)
                    if database not in self.ids_by_resource_type[RawDatabaseLoader]:
                        self.ids_by_resource_type[RawDatabaseLoader][database] = source_path

                identifier_kind_pairs.append((identifier, item_loader.kind))
                if first_seen := self.ids_by_resource_type[item_loader].get(identifier):
                    warning_list.append(DuplicatedItemWarning(source_path, identifier, first_seen))
                else:
                    self.ids_by_resource_type[item_loader][identifier] = source_path

                for dependency in loader.get_dependent_items(item):
                    self.dependencies_by_required[dependency].append((identifier, source_path))

            if api_spec is not None:
                resource_warnings = validate_resource_yaml(parsed, api_spec, source_path, element_no)
                warning_list.extend(resource_warnings)

            data_set_warnings = validate_data_set_is_set(items, loader.resource_cls, source_path)
            warning_list.extend(data_set_warnings)

        return warning_list, identifier_kind_pairs

    def _get_api_spec(self, loader: type[ResourceLoader], destination: Path) -> ParameterSpecSet | None:
        api_spec: ParameterSpecSet | None = None
        try:
            api_spec = loader.get_write_cls_parameter_spec()
        except Exception as e:
            # Todo Replace with an automatic message to sentry.
            self.warn(
                ToolkitBugWarning(
                    header=f"Failed to validate {destination.name} due to: {e}", traceback=traceback.format_exc()
                )
            )
        return api_spec

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


class FunctionBuilder(Builder):
    _resource_folder = FunctionLoader.folder_name

    def build_resource_folder(
        self, resource_files: Sequence[Path], module_variables: BuildVariables, module: ModuleLocation
    ) -> BuiltResourceList[Hashable]:
        built_resources = super().build_resource_folder(resource_files, module_variables, module)

        self._validate_function_directory(built_resources, module)
        self.copy_function_directory_to_build(built_resources, module.dir)

        return built_resources

    def _validate_function_directory(self, built_resources: BuiltResourceList, module: ModuleLocation) -> None:
        has_config_files = any(resource.kind == FunctionLoader.kind for resource in built_resources)
        if has_config_files:
            return
        config_files_misplaced = [
            file
            for file in module.source_paths_by_resource_folder[FunctionLoader.folder_name]
            if FunctionLoader.is_supported_file(file)
        ]
        if config_files_misplaced:
            for yaml_source_path in config_files_misplaced:
                required_location = module.dir / FunctionLoader.folder_name / yaml_source_path.name
                self.warn(
                    LowSeverityWarning(
                        f"The required Function resource configuration file "
                        f"was not found in {required_location.as_posix()!r}. "
                        f"The file {yaml_source_path.as_posix()!r} is currently "
                        f"considered part of the Function's artifacts and "
                        f"will not be processed by the Toolkit."
                    )
                )

    def copy_function_directory_to_build(
        self,
        built_resources: BuiltResourceList,
        module_dir: Path,
    ) -> None:
        function_directory_by_name = {
            dir_.name: dir_ for dir_ in (module_dir / FunctionLoader.folder_name).iterdir() if dir_.is_dir()
        }
        external_id_by_function_path = self._read_function_path_by_external_id(
            built_resources, function_directory_by_name
        )

        for external_id, function_path in external_id_by_function_path.items():
            # Function directory already validated to exist in read function
            function_dir = function_directory_by_name[external_id]
            destination = self.build_dir / FunctionLoader.folder_name / external_id
            if destination.exists():
                raise ToolkitFileExistsError(
                    f"Function {external_id!r} is duplicated. If this is unexpected, ensure you have a clean build directory."
                )
            shutil.copytree(function_dir, destination)

            # Clean up cache files
            for subdir in destination.iterdir():
                if subdir.is_dir():
                    shutil.rmtree(subdir / "__pycache__", ignore_errors=True)
            shutil.rmtree(destination / "__pycache__", ignore_errors=True)

    def _read_function_path_by_external_id(
        self, built_resources: BuiltResourceList, function_directory_by_name: dict[str, Path]
    ) -> dict[str, str | None]:
        function_path_by_external_id: dict[str, str | None] = {}
        for built_resource in built_resources:
            if built_resource.kind != FunctionLoader.kind or built_resource.destination is None:
                continue
            source_file = built_resource.destination
            try:
                raw_content = read_yaml_content(safe_read(source_file))
            except yaml.YAMLError as e:
                raise ToolkitYAMLFormatError(f"Failed to load function files {source_file.as_posix()!r} due to: {e}")
            raw_functions = raw_content if isinstance(raw_content, list) else [raw_content]
            for raw_function in raw_functions:
                external_id = raw_function.get("externalId")
                function_path = raw_function.get("functionPath")
                if not external_id:
                    self.warn(
                        HighSeverityWarning(
                            f"Function in {source_file.as_posix()!r} has no externalId defined. "
                            f"This is used to match the function to the function directory."
                        )
                    )
                    continue
                elif external_id not in function_directory_by_name:
                    raise ToolkitNotADirectoryError(
                        f"Function directory not found for externalId {external_id} defined in {source_file.as_posix()!r}."
                    )
                if not function_path:
                    self.warn(
                        MediumSeverityWarning(
                            f"Function {external_id} in {source_file.as_posix()!r} has no function_path defined."
                        )
                    )
                function_path_by_external_id[external_id] = function_path

        return function_path_by_external_id


class FileBuilder(Builder):
    _resource_folder = FileLoader.folder_name
