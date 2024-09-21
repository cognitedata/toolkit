from __future__ import annotations

import contextlib
import copy
import difflib
import re
import shutil
import traceback
from collections import Counter, defaultdict
from collections.abc import Callable, Hashable, Iterable
from dataclasses import dataclass, field
from functools import partial
from pathlib import Path
from typing import Any, cast

import yaml
from rich import print
from rich.panel import Panel
from rich.progress import track

from cognite_toolkit._cdf_tk._parameters import ParameterSpecSet
from cognite_toolkit._cdf_tk.cdf_toml import CDFToml
from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.commands._base import ToolkitCommand
from cognite_toolkit._cdf_tk.constants import (
    _RUNNING_IN_BROWSER,
    INDEX_PATTERN,
    ROOT_MODULES,
    TEMPLATE_VARS_FILE_SUFFIXES,
)
from cognite_toolkit._cdf_tk.data_classes import (
    BuildConfigYAML,
    BuildVariables,
    BuiltModule,
    BuiltModuleList,
    BuiltResource,
    BuiltResourceList,
    ModuleDirectories,
    ModuleLocation,
    SourceLocationEager,
    SourceLocationLazy,
)
from cognite_toolkit._cdf_tk.exceptions import (
    AmbiguousResourceFileError,
    ToolkitDuplicatedModuleError,
    ToolkitEnvError,
    ToolkitError,
    ToolkitFileExistsError,
    ToolkitMissingModuleError,
    ToolkitNotADirectoryError,
    ToolkitYAMLFormatError,
)
from cognite_toolkit._cdf_tk.hints import ModuleDefinition, verify_module_directory
from cognite_toolkit._cdf_tk.loaders import (
    LOADER_BY_FOLDER_NAME,
    ContainerLoader,
    DataModelLoader,
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
    CDFToolConfig,
    calculate_str_or_file_hash,
    humanize_collection,
    module_from_path,
    read_yaml_content,
    resource_folder_from_path,
    safe_read,
    safe_write,
)
from cognite_toolkit._cdf_tk.validation import (
    validate_data_set_is_set,
    validate_modules_variables,
    validate_resource_yaml,
)
from cognite_toolkit._version import __version__


class BuildCommand(ToolkitCommand):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.existing_resources_by_loader: dict[type[ResourceLoader], set[Hashable]] = defaultdict(set)
        self.instantiated_loaders: dict[type[ResourceLoader], ResourceLoader] = {}

        # Built State
        self._module_names_by_variable_key: dict[str, list[str]] = defaultdict(list)
        self._state = _BuildState()
        self._has_built = False

    def execute(
        self,
        verbose: bool,
        organization_dir: Path,
        build_dir: Path,
        selected: list[str] | None,
        build_env_name: str | None,
        no_clean: bool,
        ToolGlobals: CDFToolConfig | None = None,
    ) -> None:
        if organization_dir in {Path("."), Path("./")}:
            organization_dir = Path.cwd()
        verify_module_directory(organization_dir, build_env_name)

        cdf_toml = CDFToml.load()

        if build_env_name:
            config = BuildConfigYAML.load_from_directory(organization_dir, build_env_name)
        else:
            # Loads the default environment
            config = BuildConfigYAML.load_default(organization_dir)

        if selected:
            config.environment.selected = config.environment.load_selected(selected, organization_dir)

        directory_name = "current directory" if organization_dir == Path(".") else f"project '{organization_dir!s}'"
        root_modules = [
            module_dir for root_module in ROOT_MODULES if (module_dir := organization_dir / root_module).exists()
        ]
        module_locations = "\n".join(f"  - Module directory '{root_module!s}'" for root_module in root_modules)
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
            organization_dir=organization_dir,
            config=config,
            packages=cdf_toml.modules.packages,
            clean=not no_clean,
            verbose=verbose,
            ToolGlobals=ToolGlobals,
        )

    def build_config(
        self,
        build_dir: Path,
        organization_dir: Path,
        config: BuildConfigYAML,
        packages: dict[str, list[str]],
        clean: bool = False,
        verbose: bool = False,
        ToolGlobals: CDFToolConfig | None = None,
        progress_bar: bool = False,
    ) -> tuple[BuiltModuleList, dict[Path, Path]]:
        is_populated = build_dir.exists() and any(build_dir.iterdir())
        if is_populated and clean:
            shutil.rmtree(build_dir)
            build_dir.mkdir()
            if not _RUNNING_IN_BROWSER:
                self.console(f"Cleaned existing build directory {build_dir!s}.")
        elif is_populated and not _RUNNING_IN_BROWSER:
            self.warn(
                LowSeverityWarning("Build directory is not empty. Run without --no-clean to remove existing files.")
            )
        elif build_dir.exists() and not _RUNNING_IN_BROWSER:
            self.console("Build directory does already exist and is empty. No need to create it.")
        else:
            build_dir.mkdir(exist_ok=True)

        if issue := config.validate_environment():
            self.warn(issue)

        user_selected_modules = config.environment.get_selected_modules(packages)
        modules = ModuleDirectories.load(organization_dir, user_selected_modules)
        self._validate_modules(modules, config, packages, user_selected_modules, organization_dir)

        if verbose:
            self.console("Selected packages:")
            selected_packages = [package for package in packages if package in config.environment.selected]
            if len(selected_packages) == 0:
                self.console("    None", prefix="")
            for package in selected_packages:
                self.console(f"    {package}", prefix="")
            self.console("Selected modules:")
            for module in [module.name for module in modules.selected]:
                self.console(f"    {module}", prefix="")

        variables = BuildVariables.load_raw(config.variables, modules.available_paths, modules.selected.available_paths)
        warnings = validate_modules_variables(variables.selected, config.filepath)
        if warnings:
            self.warn(LowSeverityWarning(f"Found the following warnings in config.{config.environment.name}.yaml:"))
            for warning in warnings:
                if self.print_warning:
                    print(f"    {warning.get_message()}")

        # Setup state before building modules
        self._module_names_by_variable_key.clear()
        for variable in variables:
            for module_location in modules:
                if variable.location in module_location.relative_path.parts:
                    self._module_names_by_variable_key[variable.key].append(module_location.name)
        if self._has_built:
            # Todo: Reset of state??
            raise RuntimeError("In the build command, the `build_config` method should only be called once.")
        else:
            self._has_built = True

        built_modules = self.build_modules(modules.selected, build_dir, variables, verbose, progress_bar)

        self._check_missing_dependencies(organization_dir, ToolGlobals)

        build_environment = config.create_build_environment(built_modules)
        build_environment.dump_to_file(build_dir)
        if not _RUNNING_IN_BROWSER:
            self.console(f"Build complete. Files are located in {build_dir!s}/")
        return built_modules, self._state.source_by_build_path

    def build_modules(
        self,
        modules: ModuleDirectories,
        build_dir: Path,
        variables: BuildVariables,
        verbose: bool = False,
        progress_bar: bool = False,
    ) -> BuiltModuleList:
        build = BuiltModuleList()
        warning_count = len(self.warning_list)
        if progress_bar:
            modules_iter = cast(
                Iterable[ModuleLocation], track(modules, description="Building modules", transient=True)
            )
        else:
            modules_iter = modules
        for module in modules_iter:
            if verbose:
                self.console(f"Processing module {module.name}")
            module_variables = variables.get_module_variables(module)
            try:
                built_module_resources = self._build_module_resources(module, build_dir, module_variables, verbose)
            except ToolkitError as err:
                print(f"  [bold red]Failed Building:([/][red]: {module.name}")
                print(f"  [bold red]ERROR ([/][red]{type(err).__name__}[/][bold red]):[/] {err}")
                built_status = type(err).__name__
                built_module_resources = {}
            else:
                built_status = "Success"

            module_warnings = len(self.warning_list) - warning_count
            warning_count = len(self.warning_list)

            built_module = BuiltModule(
                name=module.name,
                location=SourceLocationLazy(
                    path=module.relative_path,
                    absolute_path=module.dir,
                ),
                build_variables=module_variables,
                resources=built_module_resources,
                warning_count=module_warnings,
                status=built_status,
            )
            build.append(built_module)
            self.tracker.track_module_build(built_module)
        return build

    def _build_module_resources(
        self,
        module: ModuleLocation,
        build_dir: Path,
        module_variables: BuildVariables,
        verbose: bool,
    ) -> dict[str, BuiltResourceList]:
        build_resources_by_folder: dict[str, BuiltResourceList] = {}
        if not_resource_directory := module.not_resource_directories:
            self.warn(
                LowSeverityWarning(
                    f"Module {module.dir.name!r} has non-resource directories: {sorted(not_resource_directory)}. {ModuleDefinition.short()}"
                )
            )

        for resource_name, resource_files in module.source_paths_by_resource_folder.items():
            build_plugin = {
                FileMetadataLoader.folder_name: partial(self._expand_file_metadata, module=module, verbose=verbose),
            }.get(resource_name)

            built_resource_list = BuiltResourceList[Hashable]()
            for source_path in resource_files:
                if source_path.suffix.lower() not in TEMPLATE_VARS_FILE_SUFFIXES or self._is_exception_file(
                    source_path, resource_name
                ):
                    continue

                destination = self._state.create_destination_path(source_path, resource_name, module.dir, build_dir)

                built_resources = self._build_resources(
                    source_path, destination, module_variables, build_plugin, verbose
                )

                built_resource_list.extend(built_resources)

            if resource_name == FunctionLoader.folder_name:
                self._validate_function_directory(built_resource_list, module=module)
                self.copy_function_directory_to_build(built_resource_list, module.dir, build_dir)

            build_resources_by_folder[resource_name] = built_resource_list
        return build_resources_by_folder

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
        # Todo: Remove when state is removed
        self._state.hash_by_source_path[source_path] = location.hash
        self._state.source_by_build_path[destination_path] = source_path

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

    @staticmethod
    def _validate_modules(
        modules: ModuleDirectories,
        config: BuildConfigYAML,
        packages: dict[str, list[str]],
        selected_modules: set[str | Path],
        organization_dir: Path,
    ) -> None:
        # Validations: Ambiguous selection.
        selected_names = {s for s in config.environment.selected if isinstance(s, str)}
        if duplicate_modules := {
            module_name: paths
            for module_name, paths in modules.as_path_by_name().items()
            if len(paths) > 1 and module_name in selected_names
        }:
            # If the user has selected a module by name, and there are multiple modules with that name, raise an error.
            # Note, if the user uses a path to select a module, this error will not be raised.
            raise ToolkitDuplicatedModuleError(
                f"Ambiguous module selected in config.{config.environment.name}.yaml:", duplicate_modules
            )
        # Package Referenced Modules Exists
        for package, package_modules in packages.items():
            if package not in selected_names:
                # We do not check packages that are not selected.
                # Typically, the user will delete the modules that are irrelevant for them;
                # thus we only check the selected packages.
                continue
            if missing_packages := set(package_modules) - modules.available_names:
                ToolkitMissingModuleError(
                    f"Package {package} defined in {CDFToml.file_name!s} is referring "
                    f"the following missing modules {missing_packages}."
                )

        # Selected modules does not exists
        if missing_modules := set(selected_modules) - modules.available:
            hint = ModuleDefinition.long(missing_modules, organization_dir)
            raise ToolkitMissingModuleError(
                f"The following selected modules are missing, please check path: {missing_modules}.\n{hint}"
            )

        # Nothing is Selected
        if not modules.selected:
            raise ToolkitEnvError(
                f"No selected modules specified in {config.filepath!s}, have you configured "
                f"the environment ({config.environment.name})?"
            )

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

    def _check_missing_dependencies(self, project_config_dir: Path, ToolGlobals: CDFToolConfig | None = None) -> None:
        existing = {
            (resource_cls, id_) for resource_cls, ids in self._state.ids_by_resource_type.items() for id_ in ids
        }
        missing_dependencies = set(self._state.dependencies_by_required.keys()) - existing
        for resource_cls, id_ in missing_dependencies:
            if self._is_system_resource(resource_cls, id_):
                continue
            if ToolGlobals and self._resource_exists_in_cdf(ToolGlobals.toolkit_client, resource_cls, id_):
                continue
            required_by = {
                (required, path.relative_to(project_config_dir))
                for required, path in self._state.dependencies_by_required[(resource_cls, id_)]
            }
            self.warn(MissingDependencyWarning(resource_cls.resource_cls.__name__, id_, required_by))

    def _resource_exists_in_cdf(self, client: ToolkitClient, loader_cls: type[ResourceLoader], id_: Hashable) -> bool:
        """Check is the resource exists in the CDF project. If there are any issues assume it does not exist."""
        if id_ in self.existing_resources_by_loader[loader_cls]:
            return True
        with contextlib.suppress(Exception):
            if loader_cls not in self.instantiated_loaders:
                self.instantiated_loaders[loader_cls] = loader_cls(client, None)
            loader = self.instantiated_loaders[loader_cls]
            retrieved = loader.retrieve([id_])
            if retrieved:
                self.existing_resources_by_loader[loader_cls].add(id_)
                return True
        return False

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
    def _is_exception_file(filepath: Path, resource_directory: str) -> bool:
        # In the 'functions' resource directories, all `.yaml` files must be in the root of the directory
        # This is to allow for function code to include arbitrary yaml files.
        # In addition, all files in not int the 'functions' directory are considered other files.
        return resource_directory == FunctionLoader.folder_name and filepath.parent.name != FunctionLoader.folder_name

    def copy_function_directory_to_build(
        self,
        built_resources: BuiltResourceList,
        module_dir: Path,
        build_dir: Path,
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
            destination = build_dir / FunctionLoader.folder_name / external_id
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
        if loader is NodeLoader and is_dict_item and "node" in parsed:
            items = [parsed["node"]]
        elif loader is NodeLoader and is_dict_item and "nodes" in parsed:
            items = parsed["nodes"]
            is_dict_item = False
        else:
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
                if first_seen := self._state.ids_by_resource_type[item_loader].get(identifier):
                    warning_list.append(DuplicatedItemWarning(source_path, identifier, first_seen))
                else:
                    self._state.ids_by_resource_type[item_loader][identifier] = source_path

                for dependency in loader.get_dependent_items(item):
                    self._state.dependencies_by_required[dependency].append((identifier, source_path))

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
                    suggestion = f"All files in the {resource_folder!r} folder must have a file extension that matches the resource type. Supported types are: {humanize_collection(kinds)}."
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


@dataclass
class _BuildState:
    """This is used in the build process to keep track of source of build files and hashes

    It contains some counters and convenience dictionaries for easy lookup of variables and modules.
    """

    source_by_build_path: dict[Path, Path] = field(default_factory=dict)
    hash_by_source_path: dict[Path, str] = field(default_factory=dict)
    index_by_resource_type_counter: Counter[str] = field(default_factory=Counter)
    index_by_filepath_stem: dict[Path, int] = field(default_factory=dict)
    ids_by_resource_type: dict[type[ResourceLoader], dict[Hashable, Path]] = field(
        default_factory=lambda: defaultdict(dict)
    )
    dependencies_by_required: dict[tuple[type[ResourceLoader], Hashable], list[tuple[Hashable, Path]]] = field(
        default_factory=lambda: defaultdict(list)
    )

    def create_destination_path(
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
            self.index_by_resource_type_counter[resource_folder_name] += 1
            index = self.index_by_resource_type_counter[resource_folder_name]
            self.index_by_filepath_stem[relative_stem] = index

        filename = f"{index}.{filename}"
        destination_path = build_dir / resource_folder_name / filename
        destination_path.parent.mkdir(parents=True, exist_ok=True)
        return destination_path
