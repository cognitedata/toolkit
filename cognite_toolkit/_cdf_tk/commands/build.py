from __future__ import annotations

import contextlib
import shutil
from collections import Counter, defaultdict
from collections.abc import Hashable, Iterable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, cast

from rich import print
from rich.panel import Panel
from rich.progress import track

from cognite_toolkit._cdf_tk.builders import Builder, create_builder
from cognite_toolkit._cdf_tk.cdf_toml import CDFToml
from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.commands._base import ToolkitCommand
from cognite_toolkit._cdf_tk.constants import (
    _RUNNING_IN_BROWSER,
    INDEX_PATTERN,
    ROOT_MODULES,
)
from cognite_toolkit._cdf_tk.data_classes import (
    BuildConfigYAML,
    BuildVariables,
    BuiltModule,
    BuiltModuleList,
    BuiltResourceList,
    ModuleDirectories,
    ModuleLocation,
    SourceLocationLazy,
)
from cognite_toolkit._cdf_tk.exceptions import (
    ToolkitDuplicatedModuleError,
    ToolkitEnvError,
    ToolkitError,
    ToolkitMissingModuleError,
)
from cognite_toolkit._cdf_tk.hints import ModuleDefinition, verify_module_directory
from cognite_toolkit._cdf_tk.loaders import (
    ContainerLoader,
    DataModelLoader,
    NodeLoader,
    ResourceLoader,
    SpaceLoader,
    ViewLoader,
)
from cognite_toolkit._cdf_tk.tk_warnings import (
    LowSeverityWarning,
    MissingDependencyWarning,
)
from cognite_toolkit._cdf_tk.utils import (
    CDFToolConfig,
)
from cognite_toolkit._cdf_tk.validation import (
    validate_modules_variables,
)
from cognite_toolkit._version import __version__


class BuildCommand(ToolkitCommand):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.existing_resources_by_loader: dict[type[ResourceLoader], set[Hashable]] = defaultdict(set)
        self.instantiated_loaders: dict[type[ResourceLoader], ResourceLoader] = {}

        # Built State
        self._module_names_by_variable_key: dict[str, list[str]] = defaultdict(list)
        self._builder_by_resource_folder: dict[str, Builder] = {}
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
    ) -> BuiltModuleList:
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
        self._builder_by_resource_folder.clear()
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
        return built_modules

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
            if resource_name not in self._builder_by_resource_folder:
                self._builder_by_resource_folder[resource_name] = create_builder(
                    resource_name, build_dir, self._module_names_by_variable_key, silent=self.silent, verbose=verbose
                )
            builder = self._builder_by_resource_folder[resource_name]

            built_resource_list = builder.build_resource_folder(resource_files, module_variables, module)

            build_resources_by_folder[resource_name] = built_resource_list

            self.warning_list.extend(builder.last_build_warnings())

        return build_resources_by_folder

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


@dataclass
class _BuildState:
    """This is used in the build process to keep track of source of build files and hashes

    It contains some counters and convenience dictionaries for easy lookup of variables and modules.
    """

    source_by_build_path: dict[Path, Path] = field(default_factory=dict)
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
