import os
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from functools import cached_property
from pathlib import Path
from typing import Literal, Self, TypedDict

from rich import print
from rich.panel import Panel

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.commands.build_cmd import ToolkitCommand
from cognite_toolkit._cdf_tk.constants import DEFAULT_ENV
from cognite_toolkit._cdf_tk.data_classes import (
    BuildConfigYAML,
    BuildVariables,
    BuiltModuleList,
    ModuleDirectories,
    ModuleLocation,
)
from cognite_toolkit._cdf_tk.data_classes._built_modules import BuiltModule
from cognite_toolkit._cdf_tk.exceptions import ToolkitError
from cognite_toolkit._cdf_tk.hints import verify_module_directory
from cognite_toolkit._cdf_tk.tk_warnings import ToolkitWarning, WarningList
from cognite_toolkit._cdf_tk.utils.file import safe_rmtree
from cognite_toolkit._cdf_tk.utils.modules import parse_user_selected_modules
from cognite_toolkit._cdf_tk.validation import validate_module_selection, validate_modules_variables
from cognite_toolkit._version import __version__


@dataclass
class Recommendation:
    """Recommendation for the user to improve the build. Can have auto-fix functionality."""

    def fix(self) -> None:
        pass


class BuildWarnings(TypedDict):
    warning: ToolkitWarning
    location: list[Path]


@dataclass(frozen=True)
class BuildInput:
    """Input to the build process."""

    organization_dir: Path
    build_dir: Path
    build_env_name: str
    config: BuildConfigYAML
    client: ToolkitClient | None = None
    selected: list[str | Path] | None = None
    warnings: WarningList[ToolkitWarning] | None = None

    @classmethod
    def load(
        cls,
        organization_dir: Path,
        build_dir: Path,
        build_env_name: str | None,
        client: ToolkitClient | None,
        selected: list[str | Path] | None = None,
    ) -> Self:
        resolved_org_dir = Path.cwd() if organization_dir in {Path("."), Path("./")} else organization_dir
        resolved_env = build_env_name or DEFAULT_ENV
        config, warnings = cls._load_config(resolved_org_dir, resolved_env, selected)
        return cls(resolved_org_dir, build_dir, resolved_env, config, client, selected, warnings)

    @classmethod
    def _load_config(
        cls, organization_dir: Path, build_env_name: str, selected: list[str | Path] | None
    ) -> tuple[BuildConfigYAML, WarningList[ToolkitWarning]]:
        warnings: WarningList[ToolkitWarning] = WarningList[ToolkitWarning]()
        if (organization_dir / BuildConfigYAML.get_filename(build_env_name or DEFAULT_ENV)).exists():
            config = BuildConfigYAML.load_from_directory(organization_dir, build_env_name or DEFAULT_ENV)
        else:
            # Loads the default environment
            config = BuildConfigYAML.load_default(organization_dir)
        if selected:
            config.environment.selected = parse_user_selected_modules(selected, organization_dir)
        config.set_environment_variables()
        if environment_warning := config.validate_environment():
            warnings.append(environment_warning)
        return config, warnings

    @cached_property
    def modules(self) -> ModuleDirectories:
        user_selected_modules = self.config.environment.get_selected_modules({})
        return ModuleDirectories.load(self.organization_dir, user_selected_modules)

    @cached_property
    def variables(self) -> BuildVariables:
        return BuildVariables.load_raw(
            self.config.variables, self.modules.available_paths, self.modules.selected.available_paths
        )

    @cached_property
    def recommendations(self) -> list[Recommendation]:
        raise NotImplementedError("Recommendations are not implemented yet")


@dataclass(frozen=True)
class BuildCache:
    def __init__(self, variables: BuildVariables, modules: ModuleDirectories):
        self._module_names_by_variable_key: dict[str, list[str]] = defaultdict(list)

        for variable in variables:
            for module_location in modules:
                if variable.location in module_location.relative_path.parts:
                    self._module_names_by_variable_key[variable.key].append(module_location.name)


class BuildCommandV2(ToolkitCommand):
    verbose: bool = False
    on_error: Literal["continue", "raise"] = "continue"

    def execute(
        self,
        verbose: bool,
        organization_dir: Path,
        build_dir: Path,
        selected: list[str | Path] | None,
        build_env_name: str | None,
        no_clean: bool,
        client: ToolkitClient | None = None,
        on_error: Literal["continue", "raise"] = "continue",
    ) -> BuiltModuleList:
        """
        Build the resources into deployable artifacts in the build directory.
        """

        self.verbose = verbose
        self.on_error = on_error

        # Tracking the project and cluster for the build.
        if client:
            self._additional_tracking_info.project = client.config.project
            self._additional_tracking_info.cluster = client.config.cdf_cluster

        # Setting the parameters for the build.
        input = BuildInput.load(organization_dir, build_dir, build_env_name, client, selected)

        # Print the build input.
        self._print_build_input(input)

        # Save warnings from the config
        if input.warnings:
            self.warning_list.extend(input.warnings)

        # Logistics: clean and create build directory
        self._prepare(input, no_clean)

        # Verify the build input and validate the module selection, variables, etc.
        self._verify(input)

        # Compile the configuration and variables,
        # check syntax on module and resource level
        # for any "compilation errors and warnings"
        # Consider running on multiple threads.
        built_modules = self._compile(input)

        # Optimize the build, "ruff and mypy".
        # Consistency checks and recommendations.
        # Fix where we can.
        self._optimize(input)

        # Finally, print warnings grouped by category/code and location.
        # Print warnings grouped by category/code and location.
        self._print_or_log_warnings_by_category(self.warning_list)

        return built_modules

    def _print_build_input(self, input: BuildInput) -> None:
        print(
            Panel(
                f"Building {input.organization_dir!s}:\n  - Toolkit Version '{__version__!s}'\n"
                f"  - Environment name {input.build_env_name!r}, validation-type {input.config.environment.validation_type!r}.\n"
                f"  - Config '{input.config.filepath!s}'",
                expand=False,
            )
        )

    def _prepare(self, input: BuildInput, no_clean: bool = False) -> None:
        """
        Directory logistics
        """

        if input.build_dir.exists() and any(input.build_dir.iterdir()):
            if not no_clean:
                raise ToolkitError("Build directory is not empty. Run without --no-clean to remove existing files.")
            safe_rmtree(input.build_dir)

        input.build_dir.mkdir(parents=True, exist_ok=True)

    def _verify(self, input: BuildInput) -> None:
        # Verify that the modules exists, are not duplicates,
        # and at least one is selected
        verify_module_directory(input.organization_dir, input.build_env_name)

        # Validate module selection
        user_selected_modules = input.config.environment.get_selected_modules({})
        module_warnings = validate_module_selection(
            input.modules,
            input.config,
            {},
            user_selected_modules,
            input.organization_dir,
        )
        if module_warnings:
            self.warning_list.extend(module_warnings)

        # Validate variables. Note: this looks for non-replaced template
        # variables <.*?> and can be improved in the future.
        # Keeping for reference.
        validate_modules_variables(input.variables, input.config.filepath)

        # Track LOC of managed configuration
        self._track(input)

    def _compile(self, input: BuildInput) -> BuiltModuleList:
        modules = list(input.modules.selected)
        if not modules:
            return BuiltModuleList()

        # first collect variables into practical lookup
        # then split the work of building modules into parallel tasks
        # TODO: Tasks not implemented yet. I'm sure there are optimizations to be had here.
        # Also need to watch sequencing and dependencies internally and between modules.

        cache = BuildCache(input.variables, input.modules)
        built_modules = BuiltModuleList()

        max_workers = min(len(modules), 8, (os.cpu_count() or 1))
        if max_workers < 1:
            max_workers = 1

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(
                    self._compile_module,
                    module_location,
                    cache,
                    input,
                ): module_location
                for module_location in modules
            }

            for future in as_completed(futures):
                module = futures[future]
                try:
                    built_module, warnings = future.result()
                    built_modules.append(built_module)
                    if warnings:
                        self.warning_list.extend(warnings)
                except Exception as e:  # pragma: no cover - defensive
                    if self.on_error == "raise":
                        raise
                    # For now, just log the failure; detailed warning handling can be added later.
                    self.console(f"Failed to compile module {module.name}: {e}", prefix="[bold red]ERROR:[/] ")

        return built_modules

    def _compile_module(
        self,
        module_location: ModuleLocation,
        cache: BuildCache,
        input: BuildInput,
    ) -> tuple[BuiltModule, WarningList[ToolkitWarning]]:
        raise NotImplementedError("Module compilation is not implemented yet")

    def _optimize(self, input: BuildInput) -> None:
        # I'm thinking build errors and warnings should have been exposed by now.
        # This is about parsing a successful build in light of the recommendations.
        # and trying to apply fixes where we can.

        if input.recommendations:
            for recommendation in input.recommendations:
                recommendation.fix()

        pass

    def _write(self, input: BuildInput) -> None:
        # Write the build to the build directory.
        # Track lines of code built.
        raise NotImplementedError()

    def _track(self, input: BuildInput) -> None:
        raise NotImplementedError()

    def _print_or_log_warnings_by_category(self, warnings: WarningList[ToolkitWarning]) -> None:
        raise NotImplementedError()
