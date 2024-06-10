from __future__ import annotations

import datetime
import io
import re
import shutil
import sys
import traceback
from collections import ChainMap, defaultdict
from collections.abc import Hashable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pandas as pd
import yaml
from cognite.client._api.functions import validate_function_folder
from cognite.client.data_classes import FileMetadataList, FunctionList
from rich import print
from rich.panel import Panel

from cognite_toolkit._cdf_tk._parameters import ParameterSpecSet
from cognite_toolkit._cdf_tk.commands._base import ToolkitCommand
from cognite_toolkit._cdf_tk.constants import (
    _RUNNING_IN_BROWSER,
    EXCL_INDEX_SUFFIX,
    PROC_TMPL_VARS_SUFFIX,
    ROOT_MODULES,
)
from cognite_toolkit._cdf_tk.data_classes import (
    BuildConfigYAML,
    SystemYAML,
)
from cognite_toolkit._cdf_tk.exceptions import (
    AmbiguousResourceFileError,
    ToolkitDuplicatedModuleError,
    ToolkitFileExistsError,
    ToolkitMissingModulesError,
    ToolkitNotADirectoryError,
    ToolkitValidationError,
    ToolkitYAMLFormatError,
)
from cognite_toolkit._cdf_tk.loaders import (
    LOADER_BY_FOLDER_NAME,
    DatapointsLoader,
    FileLoader,
    FunctionLoader,
    GroupLoader,
    Loader,
    RawDatabaseLoader,
    ResourceLoader,
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
from cognite_toolkit._cdf_tk.tk_warnings.fileread import DuplicatedItemWarning, MissingRequiredIdentifierWarning
from cognite_toolkit._cdf_tk.utils import (
    calculate_str_or_file_hash,
    iterate_modules,
    module_from_path,
    resource_folder_from_path,
)
from cognite_toolkit._cdf_tk.validation import (
    validate_data_set_is_set,
    validate_modules_variables,
    validate_resource_yaml,
)


class BuildCommand(ToolkitCommand):
    def execute(self, verbose: bool, source_path: Path, build_dir: Path, build_env_name: str, no_clean: bool) -> None:
        if not source_path.is_dir():
            raise ToolkitNotADirectoryError(str(source_path))

        system_config = SystemYAML.load_from_directory(source_path, build_env_name, self.warn, self.user_command)
        config = BuildConfigYAML.load_from_directory(source_path, build_env_name, self.warn)
        sources = [module_dir for root_module in ROOT_MODULES if (module_dir := source_path / root_module).exists()]
        if not sources:
            directories = "\n".join(f"   ┣ {name}" for name in ROOT_MODULES[:-1])
            raise ToolkitMissingModulesError(
                f"Could not find the source modules directory.\nExpected to find one of the following directories\n"
                f"{source_path.name}\n{directories}\n   ┗  {ROOT_MODULES[-1]}"
            )
        directory_name = "current directory" if source_path == Path(".") else f"project '{source_path!s}'"
        module_locations = "\n".join(f"  - Module directory '{source!s}'" for source in sources)
        print(
            Panel(
                f"Building {directory_name}:\n  - Environment {build_env_name!r}\n  - Config '{config.filepath!s}'"
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
                print(f"  [bold green]INFO:[/] Cleaned existing build directory {build_dir!s}.")
        elif is_populated and not _RUNNING_IN_BROWSER:
            self.warn(
                LowSeverityWarning("Build directory is not empty. Run without --no-clean to remove existing files.")
            )
        elif build_dir.exists() and not _RUNNING_IN_BROWSER:
            print("  [bold green]INFO:[/] Build directory does already exist and is empty. No need to create it.")
        else:
            build_dir.mkdir(exist_ok=True)

        config.validate_environment()

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

        selected_modules = config.get_selected_modules(system_config.packages, available_modules, verbose)

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

            files_by_resource_folder = self._to_files_by_resource_folder(source_paths, verbose)

            for resource_folder in files_by_resource_folder:
                for source_path in files_by_resource_folder[resource_folder].resource_files:
                    if verbose:
                        print(f"    [bold green]INFO:[/] Processing {source_path.name}")
                    destination = build_dir / resource_folder / state.create_file_name(source_path)
                    destination.parent.mkdir(parents=True, exist_ok=True)

                    is_function_non_yaml = resource_folder == FunctionLoader.folder_name and (
                        source_path.suffix.lower() != ".yaml" or source_path.parent.name != FunctionLoader.folder_name
                    )
                    # We only want to process the yaml files for functions as the function code is handled separately.
                    # Note that yaml files that are NOT in the root function folder are considered function code.
                    if not is_function_non_yaml:
                        content = source_path.read_text()
                        state.hash_by_source_path[source_path] = calculate_str_or_file_hash(content)
                        content = state.replace_variables(content)
                        destination.write_text(content)
                        state.source_by_build_path[destination] = source_path
                        file_warnings = self.validate(content, source_path, destination, state, verbose)
                        if file_warnings:
                            self.warning_list.extend(file_warnings)
                            # Here we do not use the self.warn method as we want to print the warnings as a group.
                            if self.print_warning:
                                print(str(file_warnings))

                    is_function_yaml = (
                        resource_folder == FunctionLoader.folder_name
                        and source_path.suffix.lower() == ".yaml"
                        and re.match(FunctionLoader.filename_pattern, source_path.stem)
                    )
                    if is_function_yaml:
                        if not state.printed_function_warning and sys.version_info >= (3, 12):
                            self.warn(
                                HighSeverityWarning(
                                    "The functions API does not support Python 3.12. "
                                    "It is recommended that you use Python 3.11 or 3.10 to develop functions locally."
                                )
                            )
                            state.printed_function_warning = True
                        self.process_function_directory(
                            yaml_source_path=source_path,
                            yaml_dest_path=destination,
                            module_dir=module_dir,
                            build_dir=build_dir,
                            verbose=verbose,
                        )
                        files_by_resource_folder[resource_folder].other_files = []

                    if resource_folder == FileLoader.folder_name:
                        self.process_files_directory(
                            files=files_by_resource_folder[resource_folder].other_files,
                            yaml_dest_path=destination,
                            module_dir=module_dir,
                            build_dir=build_dir,
                            verbose=verbose,
                        )
                        files_by_resource_folder[resource_folder].other_files = []

                for source_path in files_by_resource_folder[resource_folder].other_files:
                    destination = build_dir / DatapointsLoader.folder_name / source_path.name
                    destination.parent.mkdir(parents=True, exist_ok=True)
                    if resource_folder == DatapointsLoader.folder_name and source_path.suffix.lower() == ".csv":
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

    def _check_missing_dependencies(self, state: _BuildState, project_config_dir: Path) -> None:
        existing = {(resource_cls, id_) for resource_cls, ids in state.ids_by_resource_type.items() for id_ in ids}
        missing_dependencies = set(state.dependencies_by_required.keys()) - existing
        for resource_cls, id_ in missing_dependencies:
            required_by = {
                (required, path.relative_to(project_config_dir))
                for required, path in state.dependencies_by_required[(resource_cls, id_)]
            }
            self.warn(MissingDependencyWarning(resource_cls.resource_cls.__name__, id_, required_by))

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

    @staticmethod
    def _to_files_by_resource_folder(filepaths: list[Path], verbose: bool) -> dict[str, _ResourceFiles]:
        # Sort to support 1., 2. etc prefixes
        def sort_key(p: Path) -> int:
            if result := re.findall(r"^(\d+)", p.stem):
                return int(result[0])
            else:
                return len(filepaths)

        # The builder of a module can control the order that resources are deployed by prefixing a number
        # The custom key 'sort_key' is to get the sort on integer and not the string.
        filepaths = sorted(filepaths, key=sort_key)

        files_by_resource_folder: dict[str, _ResourceFiles] = defaultdict(_ResourceFiles)
        for filepath in filepaths:
            try:
                resource_folder = resource_folder_from_path(filepath)
            except ValueError:
                if verbose:
                    print(
                        f"      [bold green]INFO:[/] The file {filepath.name} is not in a resource directory, skipping it..."
                    )
                continue
            if filepath.suffix.lower() in PROC_TMPL_VARS_SUFFIX:
                files_by_resource_folder[resource_folder].resource_files.append(filepath)
            else:
                files_by_resource_folder[resource_folder].other_files.append(filepath)
        return files_by_resource_folder

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

    def process_function_directory(
        self,
        yaml_source_path: Path,
        yaml_dest_path: Path,
        module_dir: Path,
        build_dir: Path,
        verbose: bool = False,
    ) -> None:
        if yaml_source_path.parent.name != FunctionLoader.folder_name:
            self.warn(
                LowSeverityWarning(
                    f"The file {yaml_source_path} is considered part of the Function's code and will not be processed as a CDF resource. If this is a "
                    f"function config please move it to {FunctionLoader.folder_name} folder."
                )
            )
            return None
        try:
            functions: FunctionList = FunctionList.load(yaml.safe_load(yaml_dest_path.read_text()))
        except (KeyError, yaml.YAMLError) as e:
            raise ToolkitYAMLFormatError(f"Failed to load function file {yaml_source_path} due to: {e}")

        for func in functions:
            found = False
            for function_subdirs in self.iterate_functions(module_dir):
                for function_dir in function_subdirs:
                    if (fn_xid := func.external_id) == function_dir.name:
                        found = True
                        if verbose:
                            print(f"      [bold green]INFO:[/] Found function {fn_xid}")
                        if func.file_id != "<will_be_generated>":
                            self.warn(
                                LowSeverityWarning(
                                    f"Function {fn_xid} in {yaml_source_path} has set a file_id. Expects '<will_be_generated>' and this will be ignored."
                                )
                            )
                        destination = build_dir / "functions" / fn_xid
                        if destination.exists():
                            raise ToolkitFileExistsError(
                                f"Function {fn_xid} is duplicated. If this is unexpected, you may want to use '--clean'."
                            )
                        shutil.copytree(function_dir, destination)

                        # Run validations on the function using the SDK's validation function
                        try:
                            if func.function_path:
                                validate_function_folder(
                                    root_path=destination.as_posix(),
                                    function_path=func.function_path,
                                    skip_folder_validation=False,
                                )
                            else:
                                self.warn(
                                    MediumSeverityWarning(
                                        f"Function {fn_xid} in {yaml_source_path} has no function_path defined."
                                    )
                                )
                        except Exception as e:
                            raise ToolkitValidationError(
                                f"Failed to package function {fn_xid} at {function_dir}, python module is not loadable "
                                f"due to: {type(e)}({e}). Note that you need to have any requirements your function uses "
                                "installed in your current, local python environment."
                            ) from e
                        # Clean up cache files
                        for subdir in destination.iterdir():
                            if subdir.is_dir():
                                shutil.rmtree(subdir / "__pycache__", ignore_errors=True)
                        shutil.rmtree(destination / "__pycache__", ignore_errors=True)
            if not found:
                raise ToolkitNotADirectoryError(
                    f"Function directory not found for externalId {func.external_id} defined in {yaml_source_path}."
                )

    def process_files_directory(
        self,
        files: list[Path],
        yaml_dest_path: Path,
        module_dir: Path,
        build_dir: Path,
        verbose: bool = False,
    ) -> None:
        if len(files) == 0:
            return
        try:
            file_def = FileMetadataList.load(yaml_dest_path.read_text())
        except KeyError as e:
            raise ToolkitValidationError(f"Failed to load file definitions file {yaml_dest_path}, error in key: {e}")
        # We only support one file template definition per module.
        if len(file_def) == 1:
            if file_def[0].name and "$FILENAME" in file_def[0].name and file_def[0].name != "$FILENAME":
                if verbose:
                    print(
                        f"      [bold green]INFO:[/] Found file template {file_def[0].name} in {module_dir}, renaming files..."
                    )
                for filepath in files:
                    if file_def[0].name:
                        destination = (
                            build_dir / filepath.parent.name / re.sub(r"\$FILENAME", filepath.name, file_def[0].name)
                        )
                        destination.parent.mkdir(parents=True, exist_ok=True)
                        shutil.copyfile(filepath, destination)
                return
        for filepath in files:
            destination = build_dir / filepath.parent.name / filepath.name
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(filepath, destination)

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

        for unmatched in re.findall(pattern=r"\{\{.*?\}\}", string=content):
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
            raise ToolkitYAMLFormatError(
                f"YAML validation error for {destination.name} after substituting config variables: {e}"
            )

        loader = self._get_loader(resource_folder, destination)
        if loader is None:
            return warning_list
        if not issubclass(loader, ResourceLoader):
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

            if first_seen := state.ids_by_resource_type[loader].get(identifier):
                warning_list.append(DuplicatedItemWarning(source_path, identifier, first_seen))
            else:
                state.ids_by_resource_type[loader][identifier] = source_path

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

    def _get_loader(self, resource_folder: str, destination: Path) -> type[Loader] | None:
        loaders = LOADER_BY_FOLDER_NAME.get(resource_folder, [])
        loaders = [loader for loader in loaders if loader.is_supported_file(destination)]
        if len(loaders) == 0:
            self.warn(
                ToolkitNotSupportedWarning(
                    f"the resource {resource_folder!r}",
                    details=f"Available resources are: {', '.join(LOADER_BY_FOLDER_NAME.keys())}",
                )
            )
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

    @staticmethod
    def iterate_functions(module_dir: Path) -> Iterator[list[Path]]:
        for function_dir in module_dir.glob(f"**/{FunctionLoader.folder_name}"):
            if not function_dir.is_dir():
                continue
            function_directories = [path for path in function_dir.iterdir() if path.is_dir()]
            if function_directories:
                yield function_directories


@dataclass
class _BuildState:
    """This is used in the build process to keep track of variables and help with variable replacement.

    It contains some counters and convenience dictionaries for easy lookup of variables and modules.
    """

    modules_by_variable: dict[str, list[str]] = field(default_factory=dict)
    variables_by_module_path: dict[str, dict[str, str]] = field(default_factory=dict)
    source_by_build_path: dict[Path, Path] = field(default_factory=dict)
    hash_by_source_path: dict[Path, str] = field(default_factory=dict)
    number_by_resource_type: dict[str, int] = field(default_factory=lambda: defaultdict(int))
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

    def create_file_name(self, filepath: Path) -> str:
        return _Helpers.create_file_name(filepath, self.number_by_resource_type)

    def replace_variables(self, content: str) -> str:
        return _Helpers.replace_variables(content, self.local_variables)

    @classmethod
    def create(cls, config: BuildConfigYAML) -> _BuildState:
        variables_by_module_path = _Helpers.to_variables_by_module_path(config.variables)
        modules_by_variables = defaultdict(list)
        for module_path, variables in variables_by_module_path.items():
            for variable in variables:
                modules_by_variables[variable].append(module_path)
        return cls(modules_by_variable=modules_by_variables, variables_by_module_path=variables_by_module_path)


@dataclass
class _ResourceFiles:
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

    @staticmethod
    def create_file_name(filepath: Path, number_by_resource_type: dict[str, int]) -> str:
        filename = filepath.name
        if filepath.suffix in EXCL_INDEX_SUFFIX:
            return filename
        # Get rid of the local index
        filename = re.sub("^[0-9]+\\.", "", filename)
        number_by_resource_type[filepath.parent.name] += 1
        filename = f"{number_by_resource_type[filepath.parent.name]}.{filename}"
        return filename

    @staticmethod
    def replace_variables(content: str, local_config: Mapping[str, str]) -> str:
        for name, variable in local_config.items():
            content = re.sub(rf"{{{{\s*{name}\s*}}}}", str(variable), content)
        return content
