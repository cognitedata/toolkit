import copy
import os
from collections.abc import Iterable
from datetime import date, datetime
from pathlib import Path
from typing import Any, ClassVar, Literal, Self, TypeAlias

import yaml
from pydantic import BaseModel, Field, JsonValue, PrivateAttr, ValidationError, field_validator
from pydantic_core.core_schema import ValidationInfo

from cognite_toolkit._cdf_tk.exceptions import ToolkitValueError
from cognite_toolkit._cdf_tk.feature_flags import Flags
from cognite_toolkit._cdf_tk.utils import humanize_collection
from cognite_toolkit._cdf_tk.utils.file import relative_to_if_possible, safe_read, yaml_positions
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
# The keys and list indices leading to a value in the variables section, for example ("modules", "my_module", 0, "var").
VariableLocation: TypeAlias = tuple[str | int, ...]


class VariableSource(BaseModel):
    """Where a variable is defined. The line and column, 1-based, are of the variable's key."""

    file: Path
    line: int | None = None
    column: int | None = None

    @property
    def display_location(self) -> str:
        location = _display_path(self.file)
        if self.line is None or self.column is None:
            return location
        return f"{location}:{self.line}:{self.column}"


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

    _variable_sources: dict[VariableLocation, VariableSource] = PrivateAttr(default_factory=dict)

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
    def variable_sources(self) -> dict[VariableLocation, VariableSource]:
        """Where each location in the variables section is defined.

        This is only populated when the config is loaded from a file.
        """
        return self._variable_sources

    @classmethod
    def get_filename(cls, name: str) -> str:
        return cls.filename.format(name=name)

    @classmethod
    def get_filepath(cls, organization_dir: Path, name: str) -> Path:
        return organization_dir / cls.get_filename(name)

    @classmethod
    def from_yaml_file(cls, file_path: Path) -> Self:
        """Create an instance from a YAML file, with all its dependencies merged into it."""
        file_path = file_path.resolve()
        config = cls._read(file_path)
        if not config.dependencies:
            return config
        if not Flags.CONFIG_DEPENDENCIES.is_enabled():
            raise ToolkitValueError(
                f"Config YAML file '{_display_path(file_path)}' has dependencies, which is "
                "an alpha feature. Enable it by setting 'config-dependencies = true' under [alpha_flags] in cdf.toml."
            )
        return config._merge(file_path, cls._load_dependencies(file_path, config))

    @classmethod
    def _read(cls, file_path: Path) -> Self:
        config = super().from_yaml_file(file_path)
        config._variable_sources = _variable_sources(config.variables or {}, file_path)
        return config

    @classmethod
    def _load_dependencies(cls, file_path: Path, config: Self) -> list[tuple[Path, Self]]:
        """Loads all direct and indirect dependencies. Each file is loaded once, even if it is depended on many times."""
        visited: set[Path] = {file_path}
        dependencies: list[tuple[Path, Self]] = []
        to_check: list[tuple[Path, Self]] = [(file_path, config)]
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
    def _read_dependency(cls, file_path: Path) -> Self:
        try:
            return cls._read(file_path)
        except ValidationError as e:
            errors = humanize_validation_error(e)
            raise ToolkitValueError(
                f"Config YAML dependency '{_display_path(file_path)}' is invalid:\n- " + "\n- ".join(errors)
            ) from e

    def _merge(self, file_path: Path, dependencies: list[tuple[Path, Self]]) -> Self:
        conflicts: list[str] = []
        selected = self.environment.selected
        variables: dict[str, Any] = copy.deepcopy(self.variables or {})
        sources = dict(self._variable_sources)
        for dependency_path, dependency in dependencies:
            conflicts.extend(self._environment_conflicts(file_path, dependency_path, dependency.environment))
            selected = _union(selected, dependency.environment.selected)
            conflicts.extend(
                _merge_variables(variables, dependency.variables or {}, dependency.variable_sources, sources, ())
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
        merged._variable_sources = sources
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


def _variable_locations(value: Any, parent: VariableLocation) -> Iterable[VariableLocation]:
    if isinstance(value, dict):
        items: Iterable[tuple[str | int, Any]] = value.items()
    elif isinstance(value, list):
        items = enumerate(value)
    else:
        return
    for key, item in items:
        location = (*parent, key)
        yield location
        yield from _variable_locations(item, location)


def _variable_sources(variables: dict[str, Any], file_path: Path) -> dict[VariableLocation, VariableSource]:
    try:
        positions = yaml_positions(safe_read(file_path))
    except (OSError, yaml.YAMLError):
        positions = {}
    sources: dict[VariableLocation, VariableSource] = {}
    for location in _variable_locations(variables, ()):
        # Missing if normalization changed the key, for example, a datetime key written in a non-standard format.
        position = positions.get(("variables", *location))
        sources[location] = VariableSource(
            file=file_path,
            line=position.line if position else None,
            column=position.column if position else None,
        )
    return sources


def _merge_variables(
    target: dict[str, Any],
    source: dict[str, Any],
    source_sources: dict[VariableLocation, VariableSource],
    target_sources: dict[VariableLocation, VariableSource],
    parent: VariableLocation,
) -> list[str]:
    """Merges source into target in place and returns a description of every variable defined in both."""
    conflicts: list[str] = []
    for key, value in source.items():
        location = (*parent, key)
        if key not in target:
            target[key] = copy.deepcopy(value)
            for sub_location in [location, *_variable_locations(value, location)]:
                if sub_location in source_sources:
                    target_sources[sub_location] = source_sources[sub_location]
            continue
        existing = target[key]
        if _is_section(existing) and _is_section(value):
            if existing is None:
                target[key] = existing = {}
            conflicts.extend(_merge_variables(existing, value or {}, source_sources, target_sources, location))
            continue
        defined_in = [
            variable_source.display_location
            for variable_source in (target_sources.get(location), source_sources.get(location))
            if variable_source is not None
        ]
        conflicts.append(
            f"variables.{'.'.join(map(str, location))} is defined in {humanize_collection(defined_in, sort=False)}"
        )
    return conflicts
