from pathlib import Path
from typing import Any, Literal

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.commands.build_cmd import ToolkitCommand
from cognite_toolkit._cdf_tk.data_classes import BuildConfigYAML, BuildVariables, BuiltModuleList, ModuleDirectories
from cognite_toolkit._cdf_tk.exceptions import ToolkitError
from cognite_toolkit._cdf_tk.utils.file import safe_rmtree


class BuildCommandV2(ToolkitCommand):
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
        # Logistics, establish the conditions for the build
        config = self._prepare(organization_dir, build_dir, no_clean, build_env_name, selected, client)

        # Collect the resources to build
        self._collect(config, organization_dir, client)

        # the step that does the buildin'
        pass

        # Validate the configuration, checking for syntax, consistency, and best practices.
        self._validate(config)

        # Build the resources into deployable artifacts in the build directory
        self._build()

        return BuiltModuleList()

    def _prepare(
        self,
        organization_dir: Path,
        build_dir: Path,
        no_clean: bool,
        build_env_name: str | None,
        selected: list[str | Path] | None,
        client: ToolkitClient | None,
    ) -> Any:
        """
        Logistics, establish the conditions for the build
        """

        organization_dir = self._prepare_organization_directory(organization_dir)
        build_dir = self._prepare_build_directory(build_dir, clean=not no_clean)

        return organization_dir

    def _collect(self, config: BuildConfigYAML, organization_dir: Path, client: ToolkitClient | None) -> Any:
        """
        Collect the resources for the build.
        """

        # todo: the packages complicates things. Need to revisit this.
        user_selected_modules = config.environment.get_selected_modules({})
        modules = ModuleDirectories.load(organization_dir, user_selected_modules)
        variables = BuildVariables.load_raw(config.variables, modules.available_paths, modules.selected.available_paths)
        return modules, variables

    def _validate(self, config: BuildConfigYAML) -> None:
        """
        Validate the configuration, checking for syntax, consistency, and best practices.
        """
        if issue := config.validate_environment():
            self.warn(issue)

        pass

    def _build(self) -> None:
        """
        Build the resources into deployable artifacts in the build directory.
        """
        pass

    def _prepare_organization_directory(self, organization_dir: Path) -> Path:
        return Path.cwd() if organization_dir in {Path("."), Path("./")} else organization_dir

    def _prepare_build_directory(self, build_dir: Path, clean: bool = False) -> Path:
        """
        Prepare the build directory by creating it if it does not exist.
        """
        if build_dir.exists() and any(build_dir.iterdir()):
            if not clean:
                raise ToolkitError("Build directory is not empty. Run without --no-clean to remove existing files.")
            safe_rmtree(build_dir)

        build_dir.mkdir(parents=True, exist_ok=True)
        return build_dir
