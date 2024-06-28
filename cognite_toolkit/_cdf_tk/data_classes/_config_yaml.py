from __future__ import annotations

import itertools
import os
import re
from abc import ABC
from collections import UserDict, defaultdict
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal, get_args

import yaml
from rich import print
from typing_extensions import TypeAlias

from cognite_toolkit._cdf_tk.constants import (
    _RUNNING_IN_BROWSER,
    BUILD_ENVIRONMENT_FILE,
    DEFAULT_CONFIG_FILE,
    MODULE_PATH_SEP,
    ROOT_MODULES,
    SEARCH_VARIABLES_SUFFIX,
)
from cognite_toolkit._cdf_tk.exceptions import ToolkitEnvError, ToolkitMissingModuleError
from cognite_toolkit._cdf_tk.hints import ModuleDefinition
from cognite_toolkit._cdf_tk.loaders import LOADER_BY_FOLDER_NAME
from cognite_toolkit._cdf_tk.tk_warnings import (
    FileReadWarning,
    MediumSeverityWarning,
    MissingFileWarning,
    SourceFileModifiedWarning,
    ToolkitWarning,
    WarningList,
)
from cognite_toolkit._cdf_tk.utils import YAMLComment, YAMLWithComments, calculate_str_or_file_hash, flatten_dict
from cognite_toolkit._version import __version__

from ._base import ConfigCore, _load_version_variable

EnvType: TypeAlias = Literal["dev", "test", "staging", "qa", "prod"]

_AVAILABLE_ENV_TYPES = tuple(get_args(EnvType))


@dataclass
class Environment:
    name: str
    project: str
    build_type: EnvType
    selected: list[str | tuple[str, ...]]

    def __post_init__(self) -> None:
        if self.build_type not in _AVAILABLE_ENV_TYPES:
            raise ToolkitEnvError(
                f"Invalid type {self.build_type} in {self.name!s}. " f"Must be one of {_AVAILABLE_ENV_TYPES}"
            )

    @classmethod
    def load(cls, data: dict[str, Any], build_name: str) -> Environment:
        _deprecation_selected(data)

        if missing := {"name", "project", "type", "selected"} - set(data.keys()):
            raise ToolkitEnvError(
                f"Environment section is missing one or more required fields: {missing} in {BuildConfigYAML._file_name(build_name)!s}"
            )
        build_type = data["type"]
        if build_type not in _AVAILABLE_ENV_TYPES:
            raise ToolkitEnvError(
                f"Invalid type {build_type} in {BuildConfigYAML._file_name(build_name)!s}. "
                f"Must be one of {_AVAILABLE_ENV_TYPES}"
            )

        return Environment(
            name=build_name,
            project=data["project"],
            build_type=build_type,
            selected=[
                tuple([part for part in selected.split(MODULE_PATH_SEP) if part])
                if MODULE_PATH_SEP in selected
                else selected
                for selected in data["selected"] or []
            ],
        )

    def dump(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "project": self.project,
            "type": self.build_type,
            "selected": [
                MODULE_PATH_SEP.join(selected) if isinstance(selected, tuple) else selected
                for selected in self.selected
            ],
        }


@dataclass
class ConfigYAMLCore(ABC):
    environment: Environment


@dataclass
class BuildConfigYAML(ConfigCore, ConfigYAMLCore):
    """This is the config.[env].yaml file used in the cdf-tk build command."""

    variables: dict[str, Any] = field(default_factory=dict)

    @property
    def available_modules(self) -> list[str | tuple[str, ...]]:
        available_modules: list[str | tuple[str, ...]] = []
        to_check = [self.variables]
        while to_check:
            current = to_check.pop()
            for key, value in current.items():
                if isinstance(value, dict) and not value:
                    available_modules.append(key)
                elif isinstance(value, dict) and any(isinstance(v, dict) for v in value.values()):
                    to_check.append(value)
                elif isinstance(value, dict):
                    available_modules.append(key)
        return available_modules

    @classmethod
    def _file_name(cls, build_env_name: str) -> str:
        return f"config.{build_env_name}.yaml"

    def set_environment_variables(self) -> None:
        os.environ["CDF_ENVIRON"] = self.environment.name
        os.environ["CDF_BUILD_TYPE"] = self.environment.build_type

    def validate_environment(self) -> ToolkitWarning | None:
        if _RUNNING_IN_BROWSER:
            return None
        project = self.environment.project
        project_env = os.environ.get("CDF_PROJECT")
        if project_env == project:
            return None

        build_type = self.environment.build_type
        env_name = self.environment.name
        file_name = self._file_name(env_name)
        missing_message = (
            "No 'CDF_PROJECT' environment variable set. This is expected to match the project "
            f"set in environment section of {file_name!r}.\nThis is required for "
            "building configurations for staging and prod environments to ensure that you do "
            "not accidentally deploy to the wrong project."
        )
        mismatch_message = (
            f"Project name mismatch between project set in the environment section of {file_name!r} and the "
            f"environment variable 'CDF_PROJECT', {project} ≠ {project_env}.\nThis is required for "
            "building configurations for staging and prod environments to ensure that you do not "
            "accidentally deploy to the wrong project."
        )
        if build_type != "dev" and project_env is None:
            raise ToolkitEnvError(missing_message)
        elif build_type != "dev":
            raise ToolkitEnvError(mismatch_message)
        elif build_type == "dev" and project_env is None:
            return MediumSeverityWarning(missing_message)
        else:
            return MediumSeverityWarning(mismatch_message)

    @classmethod
    def load(cls, data: dict[str, Any], build_env_name: str, filepath: Path) -> BuildConfigYAML:
        if "environment" not in data:
            err_msg = f"Expected 'environment' section in {filepath!s}."
            raise ToolkitEnvError(err_msg)
        environment = Environment.load(data["environment"], build_env_name)

        if "modules" in data and "variables" not in data:
            err_msg = (
                f"The 'modules' section is deprecated and has been renamed to 'variables' instead.\n"
                f"Please rename 'modules' to 'variables' in your config file., f{filepath.name}"
            )
            raise ToolkitEnvError(err_msg)
        variables = data.get("variables", {})
        return cls(environment=environment, variables=variables, filepath=filepath)

    def create_build_environment(self, hash_by_source_file: dict[Path, str] | None = None) -> BuildEnvironment:
        return BuildEnvironment(
            name=self.environment.name,  # type: ignore[arg-type]
            project=self.environment.project,
            build_type=self.environment.build_type,
            selected=self.environment.selected,
            cdf_toolkit_version=__version__,
            hash_by_source_file=hash_by_source_file or {},
        )

    def get_selected_modules(
        self,
        modules_by_package: dict[str, list[str | tuple[str, ...]]],
        available_modules: set[str | tuple[str, ...]],
        source_dir: Path,
        verbose: bool,
    ) -> list[str | tuple[str, ...]]:
        selected_packages = [
            package
            for package in self.environment.selected
            if package in modules_by_package and isinstance(package, str)
        ]
        if verbose:
            print("  [bold green]INFO:[/] Selected packages:")
            if len(selected_packages) == 0:
                print("    None")
            for package in selected_packages:
                print(f"    {package}")

        selected_modules = [module for module in self.environment.selected if module not in modules_by_package]
        if missing := set(selected_modules) - available_modules:
            hint = ModuleDefinition.long(missing, source_dir)
            raise ToolkitMissingModuleError(
                f"The following selected modules are missing, please check path: {missing}.\n{hint}"
            )

        selected_modules.extend(
            itertools.chain.from_iterable(modules_by_package[package] for package in selected_packages)
        )
        if not selected_modules:
            raise ToolkitEnvError(
                f"No selected modules specified in {self.filepath!s}, have you configured "
                f"the environment ({self.environment.name})?"
            )
        if verbose:
            print("  [bold green]INFO:[/] Selected modules:")
            for module in selected_modules:
                if isinstance(module, str):
                    print(f"    {module}")
                else:
                    print(f"    {MODULE_PATH_SEP.join(module)!s}")
        return selected_modules


@dataclass
class BuildEnvironment(Environment):
    cdf_toolkit_version: str
    hash_by_source_file: dict[Path, str] = field(default_factory=dict)

    @classmethod
    def load(
        cls, data: dict[str, Any], build_name: str, action: Literal["build", "deploy", "clean"] = "build"
    ) -> BuildEnvironment:
        if build_name is None:
            raise ValueError("build_name must be specified")
        version = _load_version_variable(data, BUILD_ENVIRONMENT_FILE)
        _deprecation_selected(data)
        try:
            return BuildEnvironment(
                name=build_name,
                project=data["project"],
                build_type=data["type"],
                selected=data["selected"],
                cdf_toolkit_version=version,
                hash_by_source_file={Path(file): hash_ for file, hash_ in data.get("source_files", {}).items()},
            )
        except KeyError:
            raise ToolkitEnvError(
                f"  [bold red]ERROR:[/] Environment {build_name} is missing required fields 'project', 'type', "
                f"or 'selected_modules_and_packages' in {BUILD_ENVIRONMENT_FILE!s}"
            )

    def dump(self) -> dict[str, Any]:
        output = super().dump()
        output["cdf_toolkit_version"] = self.cdf_toolkit_version
        if self.hash_by_source_file:
            output["source_files"] = {str(file): hash_ for file, hash_ in self.hash_by_source_file.items()}
        return output

    def dump_to_file(self, build_dir: Path) -> None:
        (build_dir / BUILD_ENVIRONMENT_FILE).write_text(
            "# DO NOT EDIT THIS FILE!\n" + yaml.dump(self.dump(), sort_keys=False, indent=2)
        )

    def set_environment_variables(self) -> None:
        os.environ["CDF_ENVIRON"] = self.name
        os.environ["CDF_BUILD_TYPE"] = self.build_type

    def check_source_files_changed(self) -> WarningList[FileReadWarning]:
        warning_list = WarningList[FileReadWarning]()
        for file, hash_ in self.hash_by_source_file.items():
            if not file.exists():
                warning_list.append(MissingFileWarning(file, attempted_check="source file has changed."))
            elif hash_ != calculate_str_or_file_hash(file):
                warning_list.append(SourceFileModifiedWarning(file))
        return warning_list


def _deprecation_selected(data: dict[str, Any]) -> None:
    if "selected_modules_and_packages" in data and "selected" not in data:
        print(
            "  [bold yellow]Warning:[/] In environment section: 'selected_modules_and_packages' "
            "is deprecated, use 'selected' instead."
        )
        data["selected"] = data.pop("selected_modules_and_packages")


@dataclass
class ConfigEntry:
    """This represents a single entry in a config.yaml file.

    Note that an entry is

    Args:
        key_path: The path to the variable, e.g. ('cognite_modules', 'another_module', 'source_asset')
        current_value: The current value of the variable
        default_value: The default value of the variable
        current_comment: The comment attached to the variable in the current config.yaml file
        default_comment: The comment attached to the variable in the default.config.yaml files in the module directories.
    """

    key_path: tuple[str, ...]
    current_value: float | int | str | bool | None = None
    default_value: float | int | str | bool | None = None
    current_comment: YAMLComment | None = None
    default_comment: YAMLComment | None = None

    @property
    def value(self) -> float | int | str | bool:
        if self.current_value is not None:
            return self.current_value
        elif self.default_value is not None:
            return self.default_value
        else:
            raise ValueError("config.yaml has not been loaded correctly, both default and current values are None")

    @property
    def comment(self) -> YAMLComment | None:
        return self.current_comment or self.default_comment

    @property
    def is_added(self) -> bool:
        return self.current_value is None and self.default_value is not None

    @property
    def is_removed(self) -> bool:
        return self.current_value is not None and self.default_value is None

    @property
    def is_unchanged(self) -> bool:
        # Default and current value can be different. Changed means
        # that the variable is either added or removed.
        return self.current_value is not None and self.default_value is not None

    def __str__(self) -> str:
        if self.is_removed:
            return f"{self.key} was removed"
        elif self.is_added:
            if len(self.key_path) < 2:
                return f"{self.key!r} was added with value {self.value!r}"
            variable = self.key_path[-1]
            module = ".".join(self.key_path[:-1])
            return f"{variable!r} was added to {module!r} with value {self.value!r}"
        else:
            return f"{self.key} is unchanged"

    def __repr__(self) -> str:
        return f"{self.key}={self.current_value!r}"

    @property
    def key(self) -> str:
        return ".".join(self.key_path)


@dataclass
class InitConfigYAML(YAMLWithComments[tuple[str, ...], ConfigEntry], ConfigYAMLCore):
    """This represents the 'config.[env].yaml' file in the root of the project.
    It is used in the init command.

    The motivation for having a specialist data structure and not just a dictionary:

    1. We want to be able to dump the config.yaml file with comments.
    2. We want to track which variables are added, removed, or unchanged.
    """

    # Top level keys
    _environment = "environment"
    _variables = "variables"

    def __init__(self, environment: Environment, entries: dict[tuple[str, ...], ConfigEntry] | None = None):
        self.environment = environment
        super().__init__(entries or {})

    def as_build_config(self) -> BuildConfigYAML:
        return BuildConfigYAML(environment=self.environment, variables=self.dump()[self._variables], filepath=Path(""))

    def load_selected_defaults(self, cognite_root_module: Path) -> InitConfigYAML:
        if not self.environment.selected or len(self.environment.selected) == 0:
            return self.load_defaults(cognite_root_module)

        relevant_defaults: list[Path] = []
        for selected in self.environment.selected:
            relevant_defaults.extend(cognite_root_module.glob(f"**/{selected}/**/{DEFAULT_CONFIG_FILE}"))

        return self._load_defaults(cognite_root_module, relevant_defaults)

    def load_defaults(self, cognite_root_module: Path) -> InitConfigYAML:
        """Loads all default.config.yaml files in the cognite root module."""

        default_files_iterable: Iterable[Path]
        if cognite_root_module.name in ROOT_MODULES:
            default_files_iterable = cognite_root_module.glob(f"**/{DEFAULT_CONFIG_FILE}")
        else:
            default_files_iterable = itertools.chain(
                *[
                    (cognite_root_module / root_module).glob(f"**/{DEFAULT_CONFIG_FILE}")
                    for root_module in ROOT_MODULES
                    if (cognite_root_module / root_module).exists()
                ]
            )

        default_files = sorted(default_files_iterable, key=lambda f: f.relative_to(cognite_root_module))
        return self._load_defaults(cognite_root_module, default_files)

    def _load_defaults(self, cognite_root_module: Path, defaults_files: list[Path]) -> InitConfigYAML:
        """Loads all default.config.yaml files in the cognite root module.

        This extracts the default values from the default.config.yaml files and
        adds them to the config.yaml file.

        Args:
            cognite_root_module: The root module for all cognite modules.

        Returns:
            self
        """

        for default_config in defaults_files:
            parts = default_config.parent.relative_to(cognite_root_module).parts
            raw_file = default_config.read_text()
            file_comments = self._extract_comments(raw_file, key_prefix=tuple(parts))
            file_data = yaml.safe_load(raw_file)
            for key, value in file_data.items():
                key_path = (self._variables, *parts, key)
                local_file_path = (*parts, key)
                if key_path in self:
                    self[key_path].default_value = value
                    self[key_path].default_comment = file_comments.get(local_file_path)
                else:
                    self[key_path] = ConfigEntry(
                        key_path=key_path,
                        default_value=value,
                        default_comment=file_comments.get(local_file_path),
                    )

        return self

    @classmethod
    def load_existing(cls, existing_config_yaml: str, build_env_name: str = "dev") -> InitConfigYAML:
        """Loads an existing config.yaml file.

        This does a yaml.safe_load, in addition to extracting comments from the file.

        Args:
            existing_config_yaml: The existing config.yaml file.
            build_env_name: The build environment.

        Returns:
            self

        """
        raw_file = existing_config_yaml
        comments = cls._extract_comments(raw_file)
        config = yaml.safe_load(raw_file)
        if cls._environment in config:
            environment = Environment.load(config[cls._environment], build_env_name)
        else:
            raise ToolkitEnvError(f"Missing environment in {existing_config_yaml!s}")

        modules = config[cls._variables] if cls._variables in config else config
        entries: dict[tuple[str, ...], ConfigEntry] = {}
        for key_path, value in flatten_dict(modules).items():
            full_key_path = (cls._variables, *key_path)
            if full_key_path in entries:
                entries[full_key_path].current_value = value
                entries[full_key_path].current_comment = comments.get(full_key_path)
            else:
                entries[full_key_path] = ConfigEntry(
                    key_path=full_key_path,
                    current_value=value,
                    current_comment=comments.get(full_key_path),
                )

        return cls(
            environment=environment,
            entries=entries,
        )

    def load_variables(self, project_dir: Path, propagate_reused_variables: bool = False) -> InitConfigYAML:
        """This scans the content the files in the given directories and finds the variables.
        The motivation is to find the variables that are used in the templates, as well
        as picking up variables that are used in custom modules.

        Variables are marked with a {{ variable }} syntax.

        Args:
            project_dir: The directory with all project configurations.
            propagate_reused_variables: Whether to move variables with the same name to a shared parent.

        Returns:
            self
        """
        variable_by_parent_key: dict[str, set[tuple[str, ...]]] = defaultdict(set)
        for filepath in project_dir.glob("**/*"):
            if filepath.suffix.lower() not in SEARCH_VARIABLES_SUFFIX:
                continue
            if filepath.name.startswith("default"):
                continue
            content = filepath.read_text()
            key_parent = filepath.parent.relative_to(project_dir).parts
            if key_parent and key_parent[-1] in LOADER_BY_FOLDER_NAME:
                key_parent = key_parent[:-1]

            for match in re.findall(r"{{\s*([a-zA-Z0-9_]+)\s*}}", content):
                variable_by_parent_key[match].add(key_parent)

        return self._load_variables(variable_by_parent_key, propagate_reused_variables)

    def _load_variables(
        self, variable_by_parent_key: dict[str, set[tuple[str, ...]]], propagate_reused_variables: bool = False
    ) -> InitConfigYAML:
        for variable, key_parents in variable_by_parent_key.items():
            if len(key_parents) > 1 and propagate_reused_variables:
                key_parents = {self._find_common_parent(list(key_parents))}

            for key_parent in key_parents:
                # Remove module subfolders.
                key_parent_list = list(key_parent)
                for i in range(len(key_parent_list)):
                    if key_parent_list[i] in LOADER_BY_FOLDER_NAME:
                        key_parent_list = key_parent_list[:i]
                        break
                key_parent = tuple(key_parent_list)
                key_path = (self._variables, *key_parent, variable)
                if key_path in self:
                    continue
                # Search for the first parent that match.
                for i in range(1, len(key_parent)):
                    alt_key_path = (self._variables, *key_parent[:i], variable)
                    if alt_key_path in self:
                        break
                else:
                    self[key_path] = ConfigEntry(key_path=key_path, current_value="<Not Set>")
        return self

    @property
    def removed(self) -> list[ConfigEntry]:
        return [entry for entry in self.values() if entry.is_removed]

    @property
    def added(self) -> list[ConfigEntry]:
        return [entry for entry in self.values() if entry.is_added]

    @property
    def unchanged(self) -> list[ConfigEntry]:
        return [entry for entry in self.values() if entry.is_unchanged]

    def dump(self) -> dict[str, Any]:
        config: dict[str, Any] = {}
        for entry in self.values():
            local_config = config
            for key in entry.key_path[:-1]:
                if key not in local_config:
                    local_config[key] = {}
                local_config = local_config[key]
            local_config[entry.key_path[-1]] = entry.value
        config = self._reorder_config_yaml(config)
        return {
            self._environment: self.environment.dump(),
            **config,
        }

    def dump_yaml_with_comments(self, indent_size: int = 2) -> str:
        """Dump a config dictionary to a yaml string"""
        return self._dump_yaml_with_comments(indent_size, True)

    def _get_comment(self, key: tuple[str, ...]) -> YAMLComment | None:
        if (entry := self.get(key)) and entry.comment:
            return entry.comment
        return None

    def __str__(self) -> str:
        total_variables = len(self)
        lines = []
        if removed := self.removed:
            lines.append(f"Untracked {len(removed)} variables in {self.environment.name}.config.yaml.")
        if added := self.added:
            lines.append(f"Added {len(added)} variables to {self.environment.name}.config.yaml.")
        if total_variables == len(self.unchanged):
            lines.append(f"No variables in {self.environment.name}.config.yaml were changed.")
        return "\n".join(lines)

    @classmethod
    def _reorder_config_yaml(cls, config: dict[str, Any]) -> dict[str, Any]:
        """Reorder the config.yaml the variables before the modules."""
        new_config = {}
        for key in [k for k in config.keys() if not isinstance(config[k], dict)]:
            new_config[key] = config[key]
        for key in [k for k in config.keys() if isinstance(config[k], dict)]:
            new_config[key] = cls._reorder_config_yaml(config[key])
        return new_config

    @staticmethod
    def _find_common_parent(key_parents: list[tuple[str, ...]]) -> tuple[str, ...]:
        """Find the common parent for a list of key parents."""
        if len(key_parents) == 1:
            return key_parents[0]
        common_parent = []
        for i in range(len(key_parents[0])):
            if len({key_parent[i] if i < len(key_parent) else None for key_parent in key_parents}) == 1:
                common_parent.append(key_parents[0][i])
            else:
                break
        return tuple(common_parent)


class ConfigYAMLs(UserDict[str, InitConfigYAML]):
    def __init__(self, entries: dict[str, InitConfigYAML] | None = None):
        super().__init__(entries or {})

    @classmethod
    def load_default_environments(cls, default: dict[str, Any]) -> ConfigYAMLs:
        instance = cls()
        for environment_name, environment_config in default.items():
            environment = Environment.load(environment_config, environment_name)
            instance[environment.name] = InitConfigYAML(environment)
        return instance

    @classmethod
    def load_existing_environments(cls, existing_config_yamls: Sequence[Path]) -> ConfigYAMLs:
        instance = cls()
        for config_yaml in existing_config_yamls:
            config = InitConfigYAML.load_existing(config_yaml.read_text(), config_yaml.name.split(".")[0])
            instance[config.environment.name] = config
        return instance

    def load_default_variables(self, cognite_module: Path) -> None:
        # Can be optimized, but not a priority
        for config_yaml in self.values():
            config_yaml.load_defaults(cognite_module)

    def load_variables(self, project_dir: Path) -> None:
        # Can be optimized, but not a priority
        for config_yaml in self.values():
            config_yaml.load_variables(project_dir)
