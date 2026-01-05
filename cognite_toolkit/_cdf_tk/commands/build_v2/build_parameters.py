import sys
from functools import cached_property
from pathlib import Path

from cognite_toolkit._cdf_tk.data_classes.modules import ModuleRootDirectory

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self

from pydantic import BaseModel, ConfigDict

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.constants import DEFAULT_ENV
from cognite_toolkit._cdf_tk.data_classes import (
    BuildConfigYAML,
    BuildVariables,
)
from cognite_toolkit._cdf_tk.tk_warnings import ToolkitWarning, WarningList
from cognite_toolkit._cdf_tk.utils.modules import parse_user_selected_modules


class BuildParameters(BaseModel):
    """Input to the build process."""

    # need this until we turn BuildConfigYaml and ToolkitClient into Pydantic models
    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    organization_dir: Path
    build_dir: Path
    build_env_name: str
    config: BuildConfigYAML
    client: ToolkitClient | None = None
    warnings: WarningList[ToolkitWarning] | None = None
    user_selected: list[str | Path] | None = None

    @classmethod
    def load(
        cls,
        organization_dir: Path,
        build_dir: Path,
        build_env_name: str | None,
        client: ToolkitClient | None,
        user_selected: list[str | Path] | None = None,
    ) -> Self:
        resolved_org_dir = Path.cwd() if organization_dir in {Path("."), Path("./")} else organization_dir
        resolved_env = build_env_name or DEFAULT_ENV
        config, warnings = cls._load_config(resolved_org_dir, resolved_env, user_selected)
        return cls(
            organization_dir=resolved_org_dir,
            build_dir=build_dir,
            build_env_name=resolved_env,
            config=config,
            client=client,
            warnings=warnings,
            user_selected=user_selected,
        )

    @classmethod
    def _load_config(
        cls, organization_dir: Path, build_env_name: str, user_selected: list[str | Path] | None
    ) -> tuple[BuildConfigYAML, WarningList[ToolkitWarning]]:
        warnings: WarningList[ToolkitWarning] = WarningList[ToolkitWarning]()
        if (organization_dir / BuildConfigYAML.get_filename(build_env_name or DEFAULT_ENV)).exists():
            config = BuildConfigYAML.load_from_directory(organization_dir, build_env_name or DEFAULT_ENV)
        else:
            # Loads the default environment
            config = BuildConfigYAML.load_default(organization_dir)
        if user_selected:
            config.environment.selected = list(set(parse_user_selected_modules(list(user_selected), organization_dir)))
        config.set_environment_variables()
        if environment_warning := config.validate_environment():
            warnings.append(environment_warning)
        return config, warnings

    @cached_property
    def modules(self) -> ModuleRootDirectory:
        selection = self.user_selected or self.config.environment.selected
        return ModuleRootDirectory.load(self.organization_dir, selection)

    @cached_property
    def variables(self) -> BuildVariables:
        return BuildVariables.load_raw(
            self.config.variables,
            self.modules.available_paths,
            set(Path(sel) for sel in self.config.environment.selected),
        )
