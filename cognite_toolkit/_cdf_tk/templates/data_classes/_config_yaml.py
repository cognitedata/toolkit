from __future__ import annotations

import itertools
import os
import re
from abc import ABC
from collections import UserDict, defaultdict
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, cast

import yaml
from rich import print

from cognite_toolkit._cdf_tk.constants import _RUNNING_IN_BROWSER
from cognite_toolkit._cdf_tk.load import LOADER_BY_FOLDER_NAME
from cognite_toolkit._cdf_tk.templates._constants import (
    BUILD_ENVIRONMENT_FILE,
    DEFAULT_CONFIG_FILE,
    SEARCH_VARIABLES_SUFFIX,
)
from cognite_toolkit._cdf_tk.templates._utils import flatten_dict
from cognite_toolkit._cdf_tk.utils import YAMLComment, YAMLWithComments
from cognite_toolkit._version import __version__

from ._base import ConfigCore, _load_version_variable


@dataclass
class Environment:
    name: str
    project: str
    build_type: str
    selected_modules_and_packages: list[str]
    common_function_code: str

    @classmethod
    def load(cls, data: dict[str, Any], build_env: str) -> Environment:
        try:
            return Environment(
                name=data["name"],
                project=data["project"],
                build_type=data["type"],
                selected_modules_and_packages=data["selected_modules_and_packages"],
                common_function_code=data.get("common_function_code", "./common_function_code"),
            )
        except KeyError:
            print(
                f"  [bold red]ERROR:[/] Environment is missing "
                f"required fields 'name', 'project', 'type', or 'selected_modules_and_packages' in {BuildConfigYAML._file_name(build_env)!s}"
            )
            exit(1)

    def dump(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "project": self.project,
            "type": self.build_type,
            "selected_modules_and_packages": self.selected_modules_and_packages,
            "common_function_code": self.common_function_code,
        }


@dataclass
class ConfigYAMLCore(ABC):
    environment: Environment


@dataclass
class BuildConfigYAML(ConfigCore, ConfigYAMLCore):
    """This is the config.[env].yaml file used in the cdf-tk build command."""

    modules: dict[str, Any]

    @property
    def available_modules(self) -> list[str]:
        available_modules: list[str] = []
        to_check = [self.modules]
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
    def _file_name(cls, build_env: str) -> str:
        return f"config.{build_env}.yaml"

    def set_environment_variables(self) -> None:
        os.environ["CDF_ENVIRON"] = self.environment.name
        os.environ["CDF_BUILD_TYPE"] = self.environment.build_type

    def validate_environment(self) -> None:
        if _RUNNING_IN_BROWSER:
            return None
        file_name = self._file_name(self.environment.name)
        if (project_env := os.environ.get("CDF_PROJECT", "<not set>")) != self.environment.project:
            if self.environment.name in {"dev", "local", "demo"}:
                print(
                    f"  [bold yellow]WARNING:[/] Project name mismatch (CDF_PROJECT) between {file_name!s} ({self.environment.project}) and what is defined in environment ({project_env})."
                )
                print(
                    f"  Environment is {self.environment.name}, continuing (would have stopped for staging and prod)..."
                )
            else:
                print(
                    f"  [bold red]ERROR:[/] Project name mismatch (CDF_PROJECT) between {file_name!s} ({self.environment.project}) and what is defined in environment ({project_env=} != {self.environment.project=})."
                )
                exit(1)
        return None

    @classmethod
    def load(cls, data: dict[str, Any], build_env: str, filepath: Path) -> BuildConfigYAML:
        try:
            environment = Environment.load(data["environment"], build_env)
            modules = data["modules"]
        except KeyError:
            print(f"  [bold red]ERROR:[/] Missing 'environment' or 'modules' in {filepath!s}")
            exit(1)
        return cls(environment=environment, modules=modules, filepath=filepath)

    def create_build_environment(self) -> BuildEnvironment:
        return BuildEnvironment(
            name=self.environment.name,  # type: ignore[arg-type]
            project=self.environment.project,
            build_type=self.environment.build_type,
            selected_modules_and_packages=self.environment.selected_modules_and_packages,
            common_function_code=self.environment.common_function_code,
            cdf_toolkit_version=__version__,
        )

    def get_selected_modules(
        self, modules_by_package: dict[str, list[str]], available_modules: set[str], verbose: bool
    ) -> list[str]:
        selected_packages = [
            package for package in self.environment.selected_modules_and_packages if package in modules_by_package
        ]
        if verbose:
            print("  [bold green]INFO:[/] Selected packages:")
            for package in selected_packages:
                print(f"    {package}")

        selected_modules = [
            module for module in self.environment.selected_modules_and_packages if module not in modules_by_package
        ]
        missing = set(selected_modules) - available_modules
        if missing:
            print(f"  [bold red]ERROR:[/] The following selected modules are missing: {missing}")
            exit(1)
        selected_modules.extend(
            itertools.chain.from_iterable(modules_by_package[package] for package in selected_packages)
        )

        if verbose:
            print("  [bold green]INFO:[/] Selected modules:")
            for module in selected_modules:
                print(f"    {module}")
        if not selected_modules:
            print(
                f"  [bold yellow]WARNING:[/] Found no defined modules in {self.filepath!s}, have you configured the environment ({self.environment.name})?"
            )
            exit(1)

        return selected_modules


@dataclass
class BuildEnvironment(Environment):
    cdf_toolkit_version: str

    @classmethod
    def load(
        cls, data: dict[str, Any], build_env: str, action: Literal["build", "deploy", "clean"] = "build"
    ) -> BuildEnvironment:
        if build_env is None:
            raise ValueError("build_env must be specified")
        environment = data.get("name")
        if environment is None:
            environment = build_env
            load_data = cast(dict[str, Any], data.get(build_env))
        else:
            load_data = data
        if environment is None:
            print(f"  [bold red]ERROR:[/] Environment {build_env} not found in {BUILD_ENVIRONMENT_FILE!s}")
            exit(1)
        version = _load_version_variable(load_data, BUILD_ENVIRONMENT_FILE)
        try:
            return BuildEnvironment(
                name=cast(Literal["dev", "local", "demo", "staging", "prod"], build_env),
                project=load_data["project"],
                build_type=load_data["type"],
                selected_modules_and_packages=load_data["selected_modules_and_packages"],
                cdf_toolkit_version=version,
                common_function_code=load_data.get("common_function_code", "./common_function_code"),
            )
        except KeyError:
            print(
                f"  [bold red]ERROR:[/] Environment {build_env} is missing required fields 'project', 'type', or 'selected_modules_and_packages' in {BUILD_ENVIRONMENT_FILE!s}"
            )
            exit(1)

    def dump(self) -> dict[str, Any]:
        output = super().dump()
        output["cdf_toolkit_version"] = self.cdf_toolkit_version
        return output

    def dump_to_file(self, build_dir: Path) -> None:
        (build_dir / BUILD_ENVIRONMENT_FILE).write_text(
            "# DO NOT EDIT THIS FILE!\n" + yaml.dump(self.dump(), sort_keys=False, indent=2)
        )

    def set_environment_variables(self) -> None:
        os.environ["CDF_ENVIRON"] = self.name
        os.environ["CDF_BUILD_TYPE"] = self.build_type


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
    _modules = "modules"

    def __init__(self, environment: Environment, entries: dict[tuple[str, ...], ConfigEntry] | None = None):
        self.environment = environment
        super().__init__(entries or {})

    def load_defaults(self, cognite_root_module: Path) -> InitConfigYAML:
        """Loads all default.config.yaml files in the cognite root module.

        This extracts the default values from the default.config.yaml files and
        adds them to the config.yaml file.

        Args:
            cognite_root_module: The root module for all cognite modules.

        Returns:
            self
        """
        defaults = sorted(
            cognite_root_module.glob(f"**/{DEFAULT_CONFIG_FILE}"), key=lambda f: f.relative_to(cognite_root_module)
        )
        for default_config in defaults:
            parts = default_config.parent.relative_to(cognite_root_module).parts
            raw_file = default_config.read_text()
            file_comments = self._extract_comments(raw_file, key_prefix=tuple(parts))
            file_data = yaml.safe_load(raw_file)
            for key, value in file_data.items():
                key_path = (self._modules, *parts, key)
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
    def load_existing(cls, existing_config_yaml: str, build_env: str = "dev") -> InitConfigYAML:
        """Loads an existing config.yaml file.

        This does a yaml.safe_load, in addition to extracting comments from the file.

        Args:
            existing_config_yaml: The existing config.yaml file.
            build_env: The build environment.

        Returns:
            self

        """
        raw_file = existing_config_yaml
        comments = cls._extract_comments(raw_file)
        config = yaml.safe_load(raw_file)
        if cls._environment in config:
            environment = Environment.load(config[cls._environment], build_env)
        else:
            raise ValueError(f"Missing environment in {existing_config_yaml!s}")

        modules = config[cls._modules] if cls._modules in config else config
        entries: dict[tuple[str, ...], ConfigEntry] = {}
        for key_path, value in flatten_dict(modules).items():
            full_key_path = (cls._modules, *key_path)
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
                key_path = (self._modules, *key_parent, variable)
                if key_path in self:
                    continue
                # Search for the first parent that match.
                for i in range(1, len(key_parent)):
                    alt_key_path = (self._modules, *key_parent[:i], variable)
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
