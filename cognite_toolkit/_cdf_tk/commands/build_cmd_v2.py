from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from _cdf_tk.constants import DEFAULT_ENV

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.commands.build_cmd import ToolkitCommand
from cognite_toolkit._cdf_tk.data_classes import BuildConfigYAML, BuildVariables, BuiltModuleList, ModuleDirectories
from cognite_toolkit._cdf_tk.exceptions import ToolkitError
from cognite_toolkit._cdf_tk.utils.file import safe_rmtree


@dataclass
class Recommendation:
    """Recommendation for the user to improve the build. Can have auto-fix functionality."""

    def fix(self) -> None:
        pass


@dataclass(frozen=True)
class BuildInput:
    """Input to the build process."""

    # Todo: add validation of the input.
    # Like that the organization_dir exists, the build_dir is empty, the config is valid, the modules are valid, the variables are valid, the recommendations are valid, etc.
    # This includes the currenyt validate_modules_variables, for example

    organization_dir: Path
    build_dir: Path
    config: BuildConfigYAML
    client: ToolkitClient | None = None
    modules: ModuleDirectories | None = None
    variables: BuildVariables | None = None
    recommendations: list[Recommendation] | None = None

    @classmethod
    def load(
        cls,
        organization_dir: Path,
        build_dir: Path,
        build_env_name: str | None,
        client: ToolkitClient | None,
        selected: list[str | Path] | None = None,
    ) -> "BuildInput":
        config = cls._load_config(organization_dir, build_env_name or DEFAULT_ENV)
        modules = cls._load_modules(organization_dir, {Path("")})
        variables = cls._load_variables(config, modules)
        recommendations = cls._load_recommendations(config)

        return cls(organization_dir, build_dir, config, client, modules, variables, recommendations)

    @classmethod
    def _load_config(self, organization_dir: Path, build_env_name: str) -> BuildConfigYAML:
        if (organization_dir / BuildConfigYAML.get_filename(build_env_name or DEFAULT_ENV)).exists():
            config = BuildConfigYAML.load_from_directory(organization_dir, build_env_name or DEFAULT_ENV)
        else:
            # Loads the default environment
            config = BuildConfigYAML.load_default(organization_dir)
        return config

    @classmethod
    def _load_modules(cls, organization_dir: Path, user_selected_modules: set[str | Path]) -> ModuleDirectories:
        modules = ModuleDirectories.load(organization_dir, user_selected_modules)
        return modules

    @classmethod
    def _load_variables(cls, config: BuildConfigYAML, modules: ModuleDirectories) -> BuildVariables:
        variables = BuildVariables.load_raw(config.variables, modules.available_paths, modules.selected.available_paths)
        # if warnings := validate_modules_variables(variables, config.filepath):
        #     # Todo: Handle the warnings
        #     pass
        return variables

    @classmethod
    def _load_recommendations(cls, config: BuildConfigYAML) -> list[Recommendation]:
        recommendations: list[Recommendation] = []
        return recommendations


class BuildCommandV2(ToolkitCommand):
    verbose: bool = False

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

        # Setting the parameters for the build.
        input = BuildInput.load(organization_dir, build_dir, build_env_name, client, selected)

        # Logistics: clean and create build directory
        self._prepare_directories(input, no_clean)

        # Verify that the modules exists, are not duplicates,
        # and at least one is selected
        # TODO: Consider if this should be done here or in the BuildInput class.
        self._verify_modules(input)

        # Compile the configuration and variables,
        # check syntax on module and resource level
        # for any "compilation errors and warnings"
        # Consider running on multiple threads.
        self._compile(input)

        # Optimize the build, "ruff and mypy".
        # Consistency checks and recommendations.
        # Fix where we can.
        self._optimize(input)

        return BuiltModuleList()

    def _prepare_directories(self, input: BuildInput, no_clean: bool = False) -> None:
        """
        Directory logistics
        """

        if input.build_dir.exists() and any(input.build_dir.iterdir()):
            if not no_clean:
                raise ToolkitError("Build directory is not empty. Run without --no-clean to remove existing files.")
            safe_rmtree(input.build_dir)

        input.build_dir.mkdir(parents=True, exist_ok=True)

    def _verify_modules(self, input: BuildInput) -> None:
        """
        Validate the modules, checking for duplicates and at least one is selected.
        TODO: Consider if this should be done here or in the BuildInput class.
        """
        pass

    def _compile(self, input: BuildInput) -> None:
        """
        Merge configuration and variables, check syntax on module and resource level.
        """
        pass

    def _optimize(self, input: BuildInput) -> None:
        """
        Optimize the build, "ruff and mypy".
        """
        pass

    def _write(self, input: BuildInput) -> None:
        """
        Write the build to the build directory.
        """
        pass

    def _track(self, input: BuildInput) -> None:
        """
        Track the build.
        """
        pass
