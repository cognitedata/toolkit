import copy
import os
from collections.abc import Iterable
from datetime import date, datetime
from pathlib import Path
from typing import Any, ClassVar, Literal, Self, TypeAlias

from pydantic import BaseModel, Field, JsonValue, PrivateAttr, ValidationError, field_validator
from pydantic_core.core_schema import ValidationInfo

from cognite_toolkit._cdf_tk.exceptions import ToolkitValueError
from cognite_toolkit._cdf_tk.feature_flags import Flags
from cognite_toolkit._cdf_tk.utils import humanize_collection
from cognite_toolkit._cdf_tk.utils.file import relative_to_if_possible
from cognite_toolkit._cdf_tk.validation import humanize_validation_error

from ._base import YAMLFile
from ._types import ValidationType

CONFIG_FILE_SUFFIXES = frozenset({".yaml", ".yml"})


def _normalize_config_variables(value: Any) -> Any:
    """Coerce YAML-parsed date/datetime values to strings for JSON-compatible config variables."""
    if isinstance(value, datetime | date):
        return str(value)
    if isinstance(value, dict):
        return {_normalize_config_variables(key): _normalize_config_variables(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_normalize_config_variables(item) for item in value]
    return value


DeployProtection: TypeAlias = Literal["none", "protected"]


class Environment(BaseModel):
    name: str = "dev"
    project: str = Field(default_factory=lambda: os.environ.get("CDF_PROJECT", "UNKNOWN"))
    validation_type: ValidationType = Field("dev", alias="validation-type")
    selection_type: Literal["path", "git-diff"] = Field("path", alias="selection-type")
    deploy_branch: str | None = Field(None, alias="deploy-branch")
    deploy_protection: DeployProtection | None = Field(None, alias="deploy-protection")
    selected: list[str] | None = None

    @field_validator("selection_type", mode="after")
    @classmethod
    def _require_validation_type_dev(
        cls, value: Literal["path", "git-diff"], info: ValidationInfo
    ) -> Literal["path", "git-diff"]:
        if value == "git-diff" and info.data.get("validation_type") != "dev":
            raise ValueError("`selection-type` can only be set to 'git-diff' when `validation-type` is 'dev'")
        return value


class ConfigYAML(YAMLFile):
    filename: ClassVar[str] = "config.{name}.yaml"

    environment: Environment = Field(default_factory=Environment)
    dependencies: list[Path] | None = Field(
        None,
        description="Config YAML files this config depends on. Relative paths are resolved from the directory "
        "of the config file that lists them.",
    )
    variables: dict[str, JsonValue] | None = None

    _variable_source_files: dict[Path, Path] = PrivateAttr(default_factory=dict)

    @field_validator("dependencies", mode="after")
    @classmethod
    def _dependencies_are_yaml_files(cls, value: list[Path] | None) -> list[Path] | None:
        if value is None:
            return value
        if invalid := [path.as_posix() for path in value if path.suffix not in CONFIG_FILE_SUFFIXES]:
            raise ValueError(f"Dependencies must be YAML files (.yaml or .yml), got {humanize_collection(invalid)}")
        return value

    @field_validator("variables", mode="before")
    @classmethod
    def _normalize_variables(cls, value: Any) -> Any:
        if value is None:
            return value
        return _normalize_config_variables(value)

    @property
    def variable_source_files(self) -> dict[Path, Path]:
        """The config file each variable path, for example `modules/my_module/my_var`, is defined in.

        This is only populated when the config has dependencies.
        """
        return self._variable_source_files

    @classmethod
    def get_filename(cls, name: str) -> str:
        return cls.filename.format(name=name)

    @classmethod
    def get_filepath(cls, organization_dir: Path, name: str) -> Path:
        return organization_dir / cls.get_filename(name)

    @classmethod
    def from_yaml_file(cls, file_path: Path) -> Self:
        """Create an instance from a YAML file, with all its dependencies merged into it."""
        config = super().from_yaml_file(file_path)
        if not config.dependencies:
            return config
        if not Flags.CONFIG_DEPENDENCIES.is_enabled():
            raise ToolkitValueError(
                f"Config YAML file '{_display_path(file_path)}' has dependencies, which is "
                "an alpha feature. Enable it by setting 'config-dependencies = true' under [alpha_flags] in cdf.toml."
            )
        file_path = file_path.resolve()
        return config._merge(file_path, cls._load_dependencies(file_path, config))

    @classmethod
    def _load_dependencies(cls, file_path: Path, config: "ConfigYAML") -> list[tuple[Path, "ConfigYAML"]]:
        """Loads all direct and indirect dependencies. Each file is loaded once, even if it is depended on many times."""
        visited: set[Path] = {file_path}
        dependencies: list[tuple[Path, ConfigYAML]] = []
        to_check: list[tuple[Path, ConfigYAML]] = [(file_path, config)]
        while to_check:
            parent_path, parent = to_check.pop()
            missing: list[str] = []
            for dependency in parent.dependencies or []:
                dependency_path = (parent_path.parent / dependency).resolve()
                if dependency_path in visited:
                    continue
                if not dependency_path.is_file():
                    missing.append(dependency.as_posix())
                    continue
                visited.add(dependency_path)
                dependency_config = cls._read_dependency(dependency_path)
                dependencies.append((dependency_path, dependency_config))
                to_check.append((dependency_path, dependency_config))
            if missing:
                raise ToolkitValueError(
                    f"Config YAML file '{_display_path(parent_path)}' has dependencies that "
                    f"do not exist: {humanize_collection(missing)}"
                )
        return dependencies

    @classmethod
    def _read_dependency(cls, file_path: Path) -> "ConfigYAML":
        try:
            return super().from_yaml_file(file_path)
        except ValidationError as e:
            errors = humanize_validation_error(e)
            raise ToolkitValueError(
                f"Config YAML dependency '{_display_path(file_path)}' is invalid:\n- " + "\n- ".join(errors)
            ) from e

    def _merge(self, file_path: Path, dependencies: list[tuple[Path, "ConfigYAML"]]) -> Self:
        conflicts: list[str] = []
        selected = self.environment.selected
        variables: dict[str, Any] = copy.deepcopy(self.variables or {})
        source_files = dict.fromkeys(_variable_paths(variables, Path("")), file_path)
        for dependency_path, dependency in dependencies:
            conflicts.extend(self._environment_conflicts(file_path, dependency_path, dependency.environment))
            selected = _union(selected, dependency.environment.selected)
            conflicts.extend(
                _merge_variables(variables, dependency.variables or {}, dependency_path, source_files, Path(""))
            )
        if conflicts:
            raise ToolkitValueError(
                f"Cannot merge the dependencies of config YAML file '{_display_path(file_path)}':\n"
                + "\n".join(f"- {conflict}" for conflict in conflicts)
            )
        merged = self.model_copy(
            update={
                "environment": self.environment.model_copy(update={"selected": selected}),
                "variables": variables or self.variables,
            }
        )
        merged._variable_source_files = source_files
        return merged

    def _environment_conflicts(self, file_path: Path, dependency_path: Path, dependency: Environment) -> list[str]:
        conflicts: list[str] = []
        for field_name in sorted(dependency.model_fields_set - {"selected"}):
            value = getattr(self.environment, field_name)
            dependency_value = getattr(dependency, field_name)
            if value == dependency_value:
                continue
            key = Environment.model_fields[field_name].alias or field_name
            conflicts.append(
                f"environment.{key} is {dependency_value!r} in '{_display_path(dependency_path)}', "
                f"but {value!r} in '{_display_path(file_path)}'"
            )
        return conflicts


def _display_path(path: Path) -> str:
    return relative_to_if_possible(path, Path.cwd()).as_posix()


def _union(existing: list[str] | None, new: list[str] | None) -> list[str] | None:
    if new is None:
        return existing
    merged = list(existing or [])
    merged.extend(item for item in new if item not in merged)
    return merged


def _is_section(value: Any) -> bool:
    # An empty key, e.g. `modules:` without any children, is treated as an empty section.
    return value is None or isinstance(value, dict)


def _variable_paths(variables: dict[str, Any], parent: Path) -> Iterable[Path]:
    for key, value in variables.items():
        path = parent / key
        yield path
        if isinstance(value, dict):
            yield from _variable_paths(value, path)


def _merge_variables(
    target: dict[str, Any],
    source: dict[str, Any],
    source_file: Path,
    source_files: dict[Path, Path],
    parent: Path,
) -> list[str]:
    """Merges source into target in place and returns a description of every variable defined in both."""
    conflicts: list[str] = []
    for key, value in source.items():
        path = parent / key
        if key not in target:
            target[key] = copy.deepcopy(value)
            source_files[path] = source_file
            if isinstance(value, dict):
                source_files.update(dict.fromkeys(_variable_paths(value, path), source_file))
            continue
        existing = target[key]
        if _is_section(existing) and _is_section(value):
            if existing is None:
                target[key] = existing = {}
            conflicts.extend(_merge_variables(existing, value or {}, source_file, source_files, path))
            continue
        defined_in = {file for var_path, file in source_files.items() if var_path == path or path in var_path.parents}
        defined_in.add(source_file)
        conflicts.append(
            f"variables.{'.'.join(path.parts)} is defined in "
            f"{humanize_collection({_display_path(file) for file in defined_in})}"
        )
    return conflicts
