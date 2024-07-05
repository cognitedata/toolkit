from __future__ import annotations

import contextlib
import datetime
import difflib
import io
import re
import shutil
import sys
import traceback
from collections import ChainMap, Counter, defaultdict
from collections.abc import Hashable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pandas as pd
import yaml
from cognite.client._api.functions import validate_function_folder
from rich import print
from rich.panel import Panel

from cognite_toolkit._cdf_tk._parameters import ParameterSpecSet
from cognite_toolkit._cdf_tk.commands._base import ToolkitCommand
from cognite_toolkit._cdf_tk.constants import (
    _RUNNING_IN_BROWSER,
    INDEX_PATTERN,
    ROOT_MODULES,
    TEMPLATE_VARS_FILE_SUFFIXES,
)
from cognite_toolkit._cdf_tk.data_classes import (
    BuildConfigYAML,
    SystemYAML,
)
from cognite_toolkit._cdf_tk.exceptions import (
    AmbiguousResourceFileError,
    ToolkitDuplicatedModuleError,
    ToolkitFileExistsError,
    ToolkitNotADirectoryError,
    ToolkitValidationError,
    ToolkitYAMLFormatError,
)
from cognite_toolkit._cdf_tk.hints import ModuleDefinition
from cognite_toolkit._cdf_tk.loaders import (
    LOADER_BY_FOLDER_NAME,
    ContainerLoader,
    DataModelLoader,
    DatapointsLoader,
    FileLoader,
    FileMetadataLoader,
    FunctionLoader,
    GroupLoader,
    Loader,
    NodeLoader,
    RawDatabaseLoader,
    RawTableLoader,
    ResourceLoader,
    SpaceLoader,
    ViewLoader,
)
from cognite_toolkit._cdf_tk.tk_warnings import (
    FileReadWarning,
    HighSeverityWarning,
    LowSeverityWarning,
    MediumSeverityWarning,
    MissingDependencyWarning,
    ToolkitBugWarning,
    ToolkitNotSupportedWarning,
    UnresolvedVariableWarning,
    WarningList,
)
from cognite_toolkit._cdf_tk.tk_warnings.fileread import (
    DuplicatedItemWarning,
    MissingRequiredIdentifierWarning,
    UnknownResourceTypeWarning,
)
from cognite_toolkit._cdf_tk.utils import (
    calculate_str_or_file_hash,
    iterate_modules,
    module_from_path,
    read_yaml_content,
    resource_folder_from_path,
)
from cognite_toolkit._cdf_tk.validation import (
    validate_data_set_is_set,
    validate_modules_variables,
    validate_resource_yaml,
)
from cognite_toolkit._version import __version__

from .featureflag import FeatureFlag, Flags


class BuildCommand(ToolkitCommand):
    def execute(self, verbose: bool, source_path: Path, build_dir: Path, build_env_name: str, no_clean: bool) -> None:
        if not source_path.is_dir():
            raise ToolkitNotADirectoryError(str(source_path))

        system_config = SystemYAML.load_from_directory(source_path, build_env_name, self.warn, self.user_command)
        sources = SystemYAML.validate_module_dir(source_path)
        config = BuildConfigYAML.load_from_directory(source_path, build_env_name, self.warn)

        directory_name = "current directory" if source_path == Path(".") else f"project '{source_path!s}'"
        module_locations = "\n".join(f"  - Module directory '{source!s}'" for source in sources)
        print(
            Panel(
                f"Building {directory_name}:\n  - Toolkit Version '{__version__!s}'\n"
                f"  - Environment name {build_env_name!r}, type {config.environment.build_type!r}.\n"
                f"  - Config '{config.filepath!s}'"
                f"\n{module_locations}",
                expand=False,
            )
        )

        config.set_environment_variables()

        self.build_config(
            build_dir=build_dir,
            source_dir=source_path,
            config=config,
            system_config=system_config,
            clean=not no_clean,
            verbose=verbose,
        )

    def build_config(
        self,
        build_dir: Path,
        source_dir: Path,
        config: BuildConfigYAML,
        system_config: SystemYAML,
        clean: bool = False,
        verbose: bool = False,
    ) -> dict[Path, Path]:
        is_populated = build_dir.exists() and any(build_dir.iterdir())
        if is_populated and clean:
            shutil.rmtree(build_dir)
            build_dir.mkdir()
            if not _RUNNING_IN_BROWSER:
                print(f"[bold green]INFO:[/] Cleaned existing build directory {build_dir!s}.")
        elif is_populated and not _RUNNING_IN_BROWSER:
            self.warn(
                LowSeverityWarning("Build directory is not empty. Run without --no-clean to remove existing files.")
            )
        elif build_dir.exists() and not _RUNNING_IN_BROWSER:
            print("[bold green]INFO:[/] Build directory does already exist and is empty. No need to create it.")
        else:
            build_dir.mkdir(exist_ok=True)

        if issue := config.validate_environment():
            self.warn(issue)

        if FeatureFlag.is_enabled(Flags.NO_NAMING):
            print(
                "[bold green]INFO:[/] Naming convention warnings have been disabled. "
                "To enable them, run 'cdf-tk features set no-naming --disable'."
            )

        module_parts_by_name: dict[str, list[tuple[str, ...]]] = defaultdict(list)
        available_modules: set[str | tuple[str, ...]] = set()
        for module, _ in iterate_modules(source_dir):
            available_modules.add(module.name)
            module_parts = module.relative_to(source_dir).parts
            for i in range(1, len(module_parts) + 1):
                available_modules.add(module_parts[:i])

            module_parts_by_name[module.name].append(module.relative_to(source_dir).parts)

        if duplicate_modules := {
            module_name: paths
            for module_name, paths in module_parts_by_name.items()
            if len(paths) > 1 and module_name in config.environment.selected
        }:
            raise ToolkitDuplicatedModuleError(
                f"Ambiguous module selected in config.{config.environment.name}.yaml:", duplicate_modules
            )
        system_config.validate_modules(available_modules, config.environment.selected)

        selected_modules = config.get_selected_modules(system_config.packages, available_modules, source_dir, verbose)

        module_directories = [
            (module_dir, source_paths)
            for module_dir, source_paths in iterate_modules(source_dir)
            if self._is_selected_module(module_dir.relative_to(source_dir), selected_modules)
        ]
        selected_variables = self._get_selected_variables(config.variables, module_directories)

        warnings = validate_modules_variables(selected_variables, config.filepath)
        if warnings:
            self.warn(LowSeverityWarning(f"Found the following warnings in config.{config.environment.name}.yaml:"))
            for warning in warnings:
                print(f"    {warning.get_message()}")

        state = self.process_config_files(source_dir, module_directories, build_dir, config, verbose)

        build_environment = config.create_build_environment(state.hash_by_source_path)
        build_environment.dump_to_file(build_dir)
        if not _RUNNING_IN_BROWSER:
            print(f"  [bold green]INFO:[/] Build complete. Files are located in {build_dir!s}/")
        return state.source_by_build_path

    def process_config_files(
        self,
        project_config_dir: Path,
        module_directories: Sequence[tuple[Path, list[Path]]],
        build_dir: Path,
        config: BuildConfigYAML,
        verbose: bool = False,
    ) -> _BuildState:
        state = _BuildState.create(config)
        for module_dir, source_paths in module_directories:
            if verbose:
                print(f"  [bold green]INFO:[/] Processing module {module_dir.name}")

            state.update_local_variables(module_dir)

            files_by_resource_directory = self._to_files_by_resource_directory(source_paths, module_dir)

            for resource_directory_name, directory_files in files_by_resource_directory.items():
                build_folder: list[Path] = []
                for source_path in directory_files.resource_files:
                    destination = state.create_destination_path(
                        source_path, resource_directory_name, module_dir, build_dir
                    )

                    self._replace_variables_validate_to_build_directory(source_path, destination, state, verbose)
                    build_folder.append(destination)

                if resource_directory_name == FunctionLoader.folder_name:
                    self._validate_function_directory(state, directory_files, module_dir)
                    self.validate_and_copy_function_directory_to_build(
                        resource_files_build_folder=build_folder,
                        state=state,
                        module_dir=module_dir,
                        build_dir=build_dir,
                    )
                elif resource_directory_name == FileLoader.folder_name:
                    self.copy_files_to_upload_to_build_directory(
                        file_to_upload=directory_files.other_files,
                        resource_files_build_folder=build_folder,
                        state=state,
                        module_dir=module_dir,
                        build_dir=build_dir,
                        verbose=verbose,
                    )
                else:
                    for source_path in directory_files.other_files:
                        destination = state.create_destination_path(
                            source_path, resource_directory_name, module_dir, build_dir
                        )
                        destination.parent.mkdir(parents=True, exist_ok=True)
                        if (
                            resource_directory_name == DatapointsLoader.folder_name
                            and source_path.suffix.lower() == ".csv"
                        ):
                            self._copy_and_timeshift_csv_files(source_path, destination)
                        else:
                            if verbose:
                                print(
                                    f"    [bold green]INFO:[/] Found unrecognized file {source_path}. Copying in untouched..."
                                )
                            # Copy the file as is, not variable replacement
                            shutil.copyfile(source_path, destination)

        self._check_missing_dependencies(state, project_config_dir)
        return state

    def _validate_function_directory(
        self, state: _BuildState, directory_files: ResourceDirectory, module_dir: Path
    ) -> None:
        if not state.printed_function_warning and sys.version_info >= (3, 12):
            self.warn(
                HighSeverityWarning(
                    "The functions API does not support Python 3.12. "
                    "It is recommended that you use Python 3.11 or 3.10 to develop functions locally."
                )
            )
            state.printed_function_warning = True

        has_config_files = any(FunctionLoader.is_supported_file(file) for file in directory_files.resource_files)
        config_files_misplaced = [
            file for file in directory_files.other_files if FunctionLoader.is_supported_file(file)
        ]
        if not has_config_files and config_files_misplaced:
            for yaml_source_path in config_files_misplaced:
                required_location = module_dir / FunctionLoader.folder_name / yaml_source_path.name
                self.warn(
                    LowSeverityWarning(
                        f"The required Function resource configuration file "
                        f"was not found in {required_location.as_posix()!r}. "
                        f"The file {yaml_source_path.as_posix()!r} is currently "
                        f"considered part of the Function's artifacts and "
                        f"will not be processed by the Toolkit."
                    )
                )

    def _replace_variables_validate_to_build_directory(
        self, source_path: Path, destination_path: Path, state: _BuildState, verbose: bool
    ) -> None:
        if verbose:
            print(f"    [bold green]INFO:[/] Processing {source_path.name}")

        destination_path.parent.mkdir(parents=True, exist_ok=True)

        content = source_path.read_text()
        state.hash_by_source_path[source_path] = calculate_str_or_file_hash(content)

        content = state.replace_variables(content, source_path.suffix)
        destination_path.write_text(content)
        state.source_by_build_path[destination_path] = source_path

        file_warnings = self.validate(content, source_path, destination_path, state, verbose)
        if file_warnings:
            self.warning_list.extend(file_warnings)
            # Here we do not use the self.warn method as we want to print the warnings as a group.
            if self.print_warning:
                print(str(file_warnings))

    def _check_missing_dependencies(self, state: _BuildState, project_config_dir: Path) -> None:
        existing = {(resource_cls, id_) for resource_cls, ids in state.ids_by_resource_type.items() for id_ in ids}
        missing_dependencies = set(state.dependencies_by_required.keys()) - existing
        for resource_cls, id_ in missing_dependencies:
            if self._is_system_resource(resource_cls, id_):
                continue
            required_by = {
                (required, path.relative_to(project_config_dir))
                for required, path in state.dependencies_by_required[(resource_cls, id_)]
            }
            self.warn(MissingDependencyWarning(resource_cls.resource_cls.__name__, id_, required_by))

    @staticmethod
    def _is_system_resource(resource_cls: type[ResourceLoader], id_: Hashable) -> bool:
        """System resources are deployed to all CDF project and should not be checked for dependencies."""
        if resource_cls is SpaceLoader and isinstance(id_, str) and id_.startswith("cdf_"):
            return True
        elif (
            resource_cls in {ContainerLoader, ViewLoader, DataModelLoader, NodeLoader}
            and hasattr(id_, "space")
            and id_.space.startswith("cdf_")
        ):
            return True
        return False

    @staticmethod
    def _get_selected_variables(
        config_variables: dict[str, Any], module_directories: list[tuple[Path, list[Path]]]
    ) -> dict[str, Any]:
        selected_paths = {
            dir_.parts[1:i]
            for dir_, _ in module_directories
            if len(dir_.parts) > 1
            for i in range(2, len(dir_.parts) + 1)
        }
        selected_variables: dict[str, Any] = {}
        to_check: list[tuple[tuple[str, ...], dict[str, Any]]] = [(tuple(), config_variables)]
        while to_check:
            path, current = to_check.pop()
            for key, value in current.items():
                if isinstance(value, dict):
                    to_check.append(((*path, key), value))
                elif path in selected_paths:
                    selected = selected_variables
                    for part in path:
                        selected = selected.setdefault(part, {})
                    selected[key] = value
        return selected_variables

    @staticmethod
    def _is_selected_module(relative_module_dir: Path, selected_modules: list[str | tuple[str, ...]]) -> bool:
        module_parts = relative_module_dir.parts
        is_in_selected_modules = relative_module_dir.name in selected_modules or module_parts in selected_modules
        is_parent_in_selected_modules = any(
            parent in selected_modules for parent in (module_parts[:i] for i in range(1, len(module_parts)))
        )
        return is_parent_in_selected_modules or is_in_selected_modules

    def _to_files_by_resource_directory(self, filepaths: list[Path], module_dir: Path) -> dict[str, ResourceDirectory]:
        # Sort to support 1., 2. etc prefixes
        def sort_key(p: Path) -> tuple[int, int, str]:
            first = {
                ".yaml": 0,
                ".yml": 0,
            }.get(p.suffix.lower(), 1)
            # We ensure that the YAML files are sorted before other files.
            # This is when we add indexes to files. We want to ensure that, for example, a .sql file
            # with the same name as a .yaml file gets the same index as the .yaml file.
            if result := INDEX_PATTERN.search(p.stem):
                return first, int(result.group()[:-1]), p.name
            else:
                return first, len(filepaths) + 1, p.name

        # The builder of a module can control the order that resources are deployed by prefixing a number
        # The custom key 'sort_key' is to get the sort on integer and not the string.
        filepaths = sorted(filepaths, key=sort_key)

        files_by_resource_directory: dict[str, ResourceDirectory] = defaultdict(ResourceDirectory)
        not_resource_directory: set[str] = set()
        for filepath in filepaths:
            try:
                resource_directory = resource_folder_from_path(filepath)
            except ValueError:
                relative_to_module = filepath.relative_to(module_dir)
                is_file_in_resource_folder = relative_to_module.parts[0] == filepath.name
                if not is_file_in_resource_folder:
                    not_resource_directory.add(relative_to_module.parts[0])
                continue

            if filepath.suffix.lower() in TEMPLATE_VARS_FILE_SUFFIXES and not self._is_exception_file(
                filepath, resource_directory
            ):
                files_by_resource_directory[resource_directory].resource_files.append(filepath)
            else:
                files_by_resource_directory[resource_directory].other_files.append(filepath)

        if not_resource_directory:
            self.warn(
                LowSeverityWarning(
                    f"Module {module_dir.name!r} has non-resource directories: {sorted(not_resource_directory)}. {ModuleDefinition.short()}"
                )
            )
        return files_by_resource_directory

    @staticmethod
    def _is_exception_file(filepath: Path, resource_directory: str) -> bool:
        # In the 'functions' resource directories, all `.yaml` files must be in the root of the directory
        # This is to allow for function code to include arbitrary yaml files.
        return (
            resource_directory == FunctionLoader.folder_name
            and filepath.suffix.lower() in {".yaml", ".yml"}
            and filepath.parent.name != FunctionLoader.folder_name
        )

    @staticmethod
    def _copy_and_timeshift_csv_files(csv_file: Path, destination: Path) -> None:
        """Copies and time-shifts CSV files to today if the index name contains 'timeshift_'."""
        # Process all csv files
        if csv_file.suffix.lower() != ".csv":
            return
        # Special case for timeseries datapoints, we want to timeshift datapoints
        # if the file is a csv file, and we have been instructed to.
        # The replacement is used to ensure that we read exactly the same file on Windows and Linux
        file_content = csv_file.read_bytes().replace(b"\r\n", b"\n").decode("utf-8")
        data = pd.read_csv(io.StringIO(file_content), parse_dates=True, index_col=0)
        if "timeshift_" in data.index.name:
            print("      [bold green]INFO:[/] Found 'timeshift_' in index name, timeshifting datapoints up to today...")
            data.index.name = str(data.index.name).replace("timeshift_", "")
            data.index = pd.DatetimeIndex(data.index)
            periods = datetime.datetime.today() - data.index[-1]
            data.index = pd.DatetimeIndex.shift(data.index, periods=periods.days, freq="D")
        destination.write_text(data.to_csv())

    def validate_and_copy_function_directory_to_build(
        self,
        resource_files_build_folder: list[Path],
        state: _BuildState,
        module_dir: Path,
        build_dir: Path,
    ) -> None:
        function_directory_by_name = {
            dir_.name: dir_ for dir_ in (module_dir / FunctionLoader.folder_name).iterdir() if dir_.is_dir()
        }
        external_id_by_function_path = self._read_function_path_by_external_id(
            resource_files_build_folder, function_directory_by_name, state
        )

        for external_id, function_path in external_id_by_function_path.items():
            # Function directory already validated to exist in read function
            function_dir = function_directory_by_name[external_id]
            destination = build_dir / FunctionLoader.folder_name / external_id
            if destination.exists():
                raise ToolkitFileExistsError(
                    f"Function {external_id!r} is duplicated. If this is unexpected, ensure you have a clean build directory."
                )
            shutil.copytree(function_dir, destination)
            if function_path:
                try:
                    # Run validations on the function using the SDK's validation function
                    validate_function_folder(
                        root_path=destination.as_posix(),
                        function_path=function_path,
                        skip_folder_validation=False,
                    )
                except Exception as e:
                    raise ToolkitValidationError(
                        f"Failed to package function {external_id} at {function_dir.as_posix()!r}, python module is not loadable "
                        f"due to: {type(e)}({e}). Note that you need to have any requirements your function uses "
                        "installed in your current, local python environment."
                    ) from e

            # Clean up cache files
            for subdir in destination.iterdir():
                if subdir.is_dir():
                    shutil.rmtree(subdir / "__pycache__", ignore_errors=True)
            shutil.rmtree(destination / "__pycache__", ignore_errors=True)

    def _read_function_path_by_external_id(
        self, resource_files_build_folder: list[Path], function_directory_by_name: dict[str, Path], state: _BuildState
    ) -> dict[str, str | None]:
        function_path_by_external_id: dict[str, str | None] = {}
        configuration_files = [file for file in resource_files_build_folder if FunctionLoader.is_supported_file(file)]
        for config_file in configuration_files:
            source_file = state.source_by_build_path[config_file]

            try:
                raw_content = read_yaml_content(config_file.read_text())
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

    @staticmethod
    def copy_files_to_upload_to_build_directory(
        file_to_upload: list[Path],
        resource_files_build_folder: list[Path],
        state: _BuildState,
        module_dir: Path,
        build_dir: Path,
        verbose: bool = False,
    ) -> None:
        """This function copies the file to upload to the build directory.

        It also checks the resource configuration files for a file template definition and renames the
        file to upload if a template is found.
        """
        if len(file_to_upload) == 0:
            return

        template_name = BuildCommand._get_file_template_name(resource_files_build_folder, module_dir, verbose)

        for filepath in file_to_upload:
            destination_stem = filepath.stem
            if template_name:
                destination_stem = template_name.replace(FileMetadataLoader.template_pattern, filepath.stem)
            new_source = filepath.parent / f"{destination_stem}{filepath.suffix}"
            destination = state.create_destination_path(new_source, FileLoader.folder_name, module_dir, build_dir)
            shutil.copyfile(filepath, destination)

    @staticmethod
    def _get_file_template_name(resource_files_build_folder: list[Path], module_dir: Path, verbose: bool) -> str | None:
        # We only support one file template definition per module.
        configuration_files = [
            file for file in resource_files_build_folder if FileMetadataLoader.is_supported_file(file)
        ]
        if len(configuration_files) != 1:
            # Multiple configuration files, then there is no template
            return None

        config_content = configuration_files[0].read_text()
        try:
            raw_files = read_yaml_content(config_content)
        except yaml.YAMLError as e:
            raise ToolkitYAMLFormatError(f"Failed to load file definitions file {module_dir} due to: {e}")

        is_file_template = (
            isinstance(raw_files, list)
            and len(raw_files) == 1
            and FileMetadataLoader.template_pattern in raw_files[0].get("externalId", "")
        )
        if not is_file_template:
            return None

        # MyPy is not able to infer the type of raw_files here, so we need to use ignore
        template = raw_files[0]  # type: ignore[index]
        has_template_name = "name" in template and FileMetadataLoader.template_pattern in template["name"]
        if not has_template_name:
            return None
        template_name = template["name"]
        if verbose:
            print(
                f"      [bold green]INFO:[/] Detected file template name {template_name!r} in "
                f"{module_dir}/{FileMetadataLoader.folder_name}, renaming files..."
            )
        return template_name

    def validate(
        self,
        content: str,
        source_path: Path,
        destination: Path,
        state: _BuildState,
        verbose: bool,
    ) -> WarningList[FileReadWarning]:
        warning_list = WarningList[FileReadWarning]()
        module = module_from_path(source_path)
        resource_folder = resource_folder_from_path(source_path)

        all_unmatched = re.findall(pattern=r"\{\{.*?\}\}", string=content)
        for unmatched in all_unmatched:
            warning_list.append(UnresolvedVariableWarning(source_path, unmatched))
            variable = unmatched[2:-2]
            if modules := state.modules_by_variable.get(variable):
                module_str = (
                    f"{modules[0]!r}" if len(modules) == 1 else (", ".join(modules[:-1]) + f" or {modules[-1]}")
                )
                print(
                    f"    [bold green]Hint:[/] The variables in 'config.[ENV].yaml' need to be organised in a tree structure following"
                    f"\n    the folder structure of the template modules, but can also be moved up the config hierarchy to be shared between modules."
                    f"\n    The variable {variable!r} is defined in the variable section{'s' if len(modules) > 1 else ''} {module_str}."
                    f"\n    Check that {'these paths reflect' if len(modules) > 1 else 'this path reflects'} the location of {module}."
                )

        if destination.suffix not in {".yaml", ".yml"}:
            return warning_list

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
            return warning_list

        api_spec = self._get_api_spec(loader, destination)
        is_dict_item = isinstance(parsed, dict)
        items = [parsed] if is_dict_item else parsed

        for no, item in enumerate(items, 1):
            element_no = None if is_dict_item else no

            identifier: Any | None = None
            try:
                identifier = loader.get_id(item)
            except KeyError as error:
                warning_list.append(MissingRequiredIdentifierWarning(source_path, element_no, tuple(), error.args))

            if identifier:
                if first_seen := state.ids_by_resource_type[loader].get(identifier):
                    if loader is not RawDatabaseLoader:
                        # RAW Database will pick up all Raw Tables, so we don't want to warn about duplicates.
                        warning_list.append(DuplicatedItemWarning(source_path, identifier, first_seen))
                else:
                    state.ids_by_resource_type[loader][identifier] = source_path

                if loader is RawDatabaseLoader:
                    # We might also have Raw Tables that is in the same file.
                    with contextlib.suppress(KeyError):
                        table_id = RawTableLoader.get_id(item)
                        if table_id not in state.ids_by_resource_type[RawTableLoader]:
                            state.ids_by_resource_type[RawTableLoader][table_id] = source_path

                warnings = loader.check_identifier_semantics(identifier, source_path, verbose)
                warning_list.extend(warnings)

                for dependency in loader.get_dependent_items(item):
                    state.dependencies_by_required[dependency].append((identifier, source_path))

            if api_spec is not None:
                resource_warnings = validate_resource_yaml(parsed, api_spec, source_path, element_no)
                warning_list.extend(resource_warnings)

            data_set_warnings = validate_data_set_is_set(items, loader.resource_cls, source_path)
            warning_list.extend(data_set_warnings)

        return warning_list

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
                    suggestion = f"{core}.{match[0]}{source_path.suffix}"
            self.warn(UnknownResourceTypeWarning(source_path, suggestion))
            return None
        elif len(loaders) > 1 and all(loader.folder_name == "raw" for loader in loaders):
            # Multiple raw loaders load from the same file.
            return RawDatabaseLoader
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


@dataclass
class _BuildState:
    """This is used in the build process to keep track of variables and help with variable replacement.

    It contains some counters and convenience dictionaries for easy lookup of variables and modules.
    """

    modules_by_variable: dict[str, list[str]] = field(default_factory=dict)
    variables_by_module_path: dict[str, dict[str, str]] = field(default_factory=dict)
    source_by_build_path: dict[Path, Path] = field(default_factory=dict)
    hash_by_source_path: dict[Path, str] = field(default_factory=dict)
    index_by_resource_type_counter: Counter[str] = field(default_factory=Counter)
    index_by_filepath_stem: dict[Path, int] = field(default_factory=dict)
    printed_function_warning: bool = False
    ids_by_resource_type: dict[type[ResourceLoader], dict[Hashable, Path]] = field(
        default_factory=lambda: defaultdict(dict)
    )
    dependencies_by_required: dict[tuple[type[ResourceLoader], Hashable], list[tuple[Hashable, Path]]] = field(
        default_factory=lambda: defaultdict(list)
    )

    _local_variables: Mapping[str, str] = field(default_factory=dict)

    @property
    def local_variables(self) -> Mapping[str, str]:
        return self._local_variables

    def update_local_variables(self, module_dir: Path) -> None:
        self._local_variables = _Helpers.create_local_config(self.variables_by_module_path, module_dir)

    def create_destination_path(
        self, source_path: Path, resource_directory: str, module_dir: Path, build_dir: Path
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
            self.index_by_resource_type_counter[resource_directory] += 1
            index = self.index_by_resource_type_counter[resource_directory]
            self.index_by_filepath_stem[relative_stem] = index

        filename = f"{index}.{filename}"
        destination_path = build_dir / resource_directory / filename
        destination_path.parent.mkdir(parents=True, exist_ok=True)
        return destination_path

    def replace_variables(self, content: str, file_suffix: str = ".yaml") -> str:
        for name, variable in self.local_variables.items():
            replace = variable
            _core_patter = rf"{{{{\s*{name}\s*}}}}"
            if file_suffix in {".yaml", ".yml", ".json"}:
                # Preserve data types
                if isinstance(replace, str) and (replace.isdigit() or replace.endswith(":")):
                    replace = f'"{replace}"'
                elif replace is None:
                    replace = "null"
                content = re.sub(rf"'{_core_patter}'|{_core_patter}|" + rf'"{_core_patter}"', str(replace), content)
            else:
                content = re.sub(_core_patter, str(replace), content)

        return content

    @classmethod
    def create(cls, config: BuildConfigYAML) -> _BuildState:
        variables_by_module_path = _Helpers.to_variables_by_module_path(config.variables)
        modules_by_variables = defaultdict(list)
        for module_path, variables in variables_by_module_path.items():
            for variable in variables:
                modules_by_variables[variable].append(module_path)
        return cls(modules_by_variable=modules_by_variables, variables_by_module_path=variables_by_module_path)


@dataclass
class ResourceDirectory:
    """The files in a Resource Directory.

    Args:
        resource_files: The files that are considered resources, which will be preformed variable replacement on
        other_files: Files that are not considered resources, and will be copied as is.
    """

    resource_files: list[Path] = field(default_factory=list)
    other_files: list[Path] = field(default_factory=list)


class _Helpers:
    @staticmethod
    def create_local_config(config: dict[str, Any], module_dir: Path) -> Mapping[str, str]:
        maps = []
        parts = module_dir.parts
        for root_module in ROOT_MODULES:
            if parts[0] != root_module and root_module in parts:
                parts = parts[parts.index(root_module) :]
        for no in range(len(parts), -1, -1):
            if c := config.get(".".join(parts[:no])):
                maps.append(c)
        return ChainMap(*maps)

    @classmethod
    def to_variables_by_module_path(cls, config: dict[str, Any]) -> dict[str, dict[str, str]]:
        configs: dict[str, dict[str, str]] = {}
        cls._split_config(config, configs, prefix="")
        return configs

    @classmethod
    def _split_config(cls, config: dict[str, Any], configs: dict[str, dict[str, str]], prefix: str = "") -> None:
        for key, value in config.items():
            if isinstance(value, dict):
                if prefix and not prefix.endswith("."):
                    prefix = f"{prefix}."
                cls._split_config(value, configs, prefix=f"{prefix}{key}")
            else:
                configs.setdefault(prefix.removesuffix("."), {})[key] = value
