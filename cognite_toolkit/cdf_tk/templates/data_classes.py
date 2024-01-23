from __future__ import annotations

import itertools
import os
import re
from abc import ABC, abstractmethod
from collections import UserDict, defaultdict
from collections.abc import Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, ClassVar, Literal, TypeVar, cast

import yaml
from rich import print

from cognite_toolkit import _version
from cognite_toolkit.cdf_tk.load import LOADER_BY_FOLDER_NAME
from cognite_toolkit.cdf_tk.utils import read_yaml_file

from ._constants import BUILD_ENVIRONMENT_FILE, DEFAULT_CONFIG_FILE, SEARCH_VARIABLES_SUFFIX
from ._utils import flatten_dict, iterate_modules

__all__ = ["GlobalConfig", "EnvironmentConfig", "ConfigYAML", "ConfigYAMLs", "BuildEnvironment"]


@dataclass
class BuildConfig(ABC):
    """Base class for the two build config files (global.yaml and [env].config.yaml)"""

    filepath: Path

    @classmethod
    @abstractmethod
    def _file_name(cls, build_env: str) -> str:
        raise NotImplementedError

    @classmethod
    def load_from_directory(cls: type[T_BuildConfig], source_path: Path, build_env: str) -> T_BuildConfig:
        file_name = cls._file_name(build_env)
        filepath = source_path / file_name
        filepath = filepath if filepath.is_file() else Path.cwd() / file_name
        if not filepath.is_file():
            print(f"  [bold red]ERROR:[/] {filepath.name!r} does not exist")
            exit(1)
        return cls.load(read_yaml_file(filepath), build_env, filepath)

    @classmethod
    @abstractmethod
    def load(cls: type[T_BuildConfig], data: dict[str, Any], build_env: str, filepath: Path) -> T_BuildConfig:
        raise NotImplementedError


T_BuildConfig = TypeVar("T_BuildConfig", bound=BuildConfig)


@dataclass
class SystemVariables:
    cdf_toolkit_version: str

    @classmethod
    def load(cls, data: dict[str, Any], action: Literal["build", "deploy", "clean"]) -> SystemVariables:
        file_name = BUILD_ENVIRONMENT_FILE if action in {"deploy", "clean"} else GlobalConfig.file_name
        try:
            system = SystemVariables(cdf_toolkit_version=data["__system"]["cdf_toolkit_version"])
        except KeyError:
            print(
                f"  [bold red]ERROR:[/] System variables are missing required field 'cdf_toolkit_version' in {file_name!s}"
            )
            if action in {"deploy", "clean"}:
                print(f"  rerun `cdf-tk build` to build the templates again and create `{file_name!s}` correctly.")
            elif action == "build":
                print(
                    f"  run `cdf-tk init --upgrade` to initialize the templates again and create a correct `{file_name!s}` file."
                )
            exit(1)
        if system.cdf_toolkit_version != _version.__version__:
            print(
                f"  [bold red]Error:[/] The version of the templates ({system.cdf_toolkit_version}) does not match the version of the installed package ({_version.__version__})."
            )
            print("  Please either run `cdf-tk init --upgrade` to upgrade the templates OR")
            print(f"  run `pip install cognite-toolkit==={system.cdf_toolkit_version}` to downgrade cdf-tk.")
            exit(1)
        return system


@dataclass
class GlobalConfig(BuildConfig):
    file_name: ClassVar[str] = "global.yaml"
    system: SystemVariables
    packages: dict[str, list[str]] = field(default_factory=dict)

    @classmethod
    def _file_name(cls, build_env: str) -> str:
        return cls.file_name

    @classmethod
    def load(cls, data: dict[str, Any], build_env: str, filepath: Path) -> GlobalConfig:
        system = SystemVariables.load(data, "build")
        packages = data.get("packages", {})
        if not packages:
            print(f"  [bold yellow]Warning:[/] No packages defined in {cls.file_name}.")
        return cls(
            filepath=filepath,
            system=system,
            packages=packages,
        )

    def validate_modules(self, available_modules: set[str]) -> None:
        for package, modules in self.packages.items():
            if missing := set(modules) - available_modules:
                print(
                    f"  [bold red]ERROR:[/] Package {package} defined in {self.filepath.name!s} is referring "
                    f"the following missing modules {missing}."
                )
                exit(1)


@dataclass
class Environment:
    name: str = "dev"
    project: str = "<customer-dev>"
    build_type: str = "dev"
    deploy: list[str] = field(default_factory=list)

    @classmethod
    def load(cls, data: dict[str, Any], build_env: str) -> Environment:
        try:
            return Environment(
                name=data["name"],
                project=data["project"],
                build_type=data["type"],
                deploy=data["deploy"],
            )
        except KeyError:
            print(
                f"  [bold red]ERROR:[/] Environment is missing "
                f"required fields 'name', 'project', 'type', or 'deploy' in {EnvironmentConfig._file_name(build_env)!s}"
            )
            exit(1)

    def dump(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "project": self.project,
            "type": self.build_type,
            "deploy": self.deploy,
        }


@dataclass
class EnvironmentConfig(BuildConfig):
    environment: Environment
    modules: dict[str, Any]

    @classmethod
    def _file_name(cls, build_env: str) -> str:
        return f"{build_env}.config.yaml"

    def set_environment_variables(self) -> None:
        os.environ["CDF_ENVIRON"] = self.environment.name
        os.environ["CDF_BUILD_TYPE"] = self.environment.build_type

    def validate_environment(self) -> None:
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
    def load(cls, data: dict[str, Any], build_env: str, filepath: Path) -> EnvironmentConfig:
        try:
            environment = Environment.load(data["environment"], build_env)
            modules = data["modules"]
        except KeyError:
            print(f"  [bold red]ERROR:[/] Missing 'environment' or 'modules' in {filepath!s}")
            exit(1)
        return cls(environment=environment, modules=modules, filepath=filepath)

    def create_build_environment(self, system_variables: SystemVariables) -> BuildEnvironment:
        return BuildEnvironment(
            name=self.environment.name,  # type: ignore[arg-type]
            project=self.environment.project,
            build_type=self.environment.build_type,
            deploy=self.environment.deploy,
            system=system_variables,
        )

    def get_selected_modules(
        self, modules_by_package: dict[str, list[str]], available_modules: set[str], verbose: bool
    ) -> list[str]:
        selected_packages = [package for package in self.environment.deploy if package in modules_by_package]
        if verbose:
            print("  [bold green]INFO:[/] Selected packages:")
            for package in selected_packages:
                print(f"    {package}")

        selected_modules = [module for module in self.environment.deploy if module not in modules_by_package]
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
class BuildEnvironment:
    name: Literal["dev", "local", "demo", "staging", "prod"]
    project: str
    build_type: str
    deploy: list[str]
    system: SystemVariables

    @classmethod
    def load(
        cls, data: dict[str, Any], build_env: str, action: Literal["build", "deploy", "clean"]
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
        system = SystemVariables.load(data, action)
        try:
            return BuildEnvironment(
                name=cast(Literal["dev", "local", "demo", "staging", "prod"], build_env),
                project=load_data["project"],
                build_type=load_data["type"],
                deploy=load_data["deploy"],
                system=system,
            )
        except KeyError:
            print(
                f"  [bold red]ERROR:[/] Environment {build_env} is missing required fields 'project', 'type', or 'deploy' in {BUILD_ENVIRONMENT_FILE!s}"
            )
            exit(1)

    def dump(self) -> dict[str, Any]:
        return {
            self.name: {
                "project": self.project,
                "type": self.build_type,
                "deploy": self.deploy,
            },
            "__system": {
                "cdf_toolkit_version": self.system.cdf_toolkit_version,
            },
        }

    def dump_to_file(self, build_dir: Path) -> None:
        (build_dir / BUILD_ENVIRONMENT_FILE).write_text(yaml.dump(self.dump(), sort_keys=False, indent=2))

    def set_environment_variables(self) -> None:
        os.environ["CDF_ENVIRON"] = self.name
        os.environ["CDF_BUILD_TYPE"] = self.build_type


@dataclass(frozen=True)
class YAMLComment:
    """This represents a comment in a YAML file. It can be either above or after a variable."""

    above: list[str] = field(default_factory=list)
    after: list[str] = field(default_factory=list)


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
        is_active: Whether the variable is in the project config. If False, the variable is not in the project config.
    """

    key_path: tuple[str, ...]
    current_value: float | int | str | bool | None = None
    default_value: float | int | str | bool | None = None
    current_comment: YAMLComment | None = None
    default_comment: YAMLComment | None = None
    is_active: bool = False

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
        return not self.is_active

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


class ConfigYAML(UserDict[tuple[str, ...], ConfigEntry]):
    """This represents the 'config.yaml' file in the root of the project.

    The motivation for having a specialist data structure and not just a dictionary:

    1. We want to be able to dump the config.yaml file with comments.
    2. We want to track which variables are added, removed, or unchanged.
    """

    # Top level keys
    _environment = "environment"
    _modules = "modules"

    def __init__(
        self, entries: dict[tuple[str, ...], ConfigEntry] | None = None, environment: Environment | None = None
    ):
        super().__init__(entries or {})
        self.environment = environment or Environment()

    def load_defaults(self, cognite_root_module: Path) -> ConfigYAML:
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
        module_names = {module_path.name for module_path, _ in iterate_modules(cognite_root_module)}
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
                        # All variables set at the root level are active by default.
                        is_active=not parts or parts[-1] not in module_names,
                    )

        return self

    def load_existing(self, existing_config_yaml: str, build_env: str = "dev") -> ConfigYAML:
        """Loads an existing config.yaml file.

        This does a yaml.safe_load, in addition to extracting comments from the file.

        Args:
            existing_config_yaml: The existing config.yaml file.
            build_env: The build environment.

        Returns:
            self

        """
        raw_file = existing_config_yaml
        comments = self._extract_comments(raw_file)
        config = yaml.safe_load(raw_file)
        if self._environment in config:
            self.environment = Environment.load(config[self._environment], build_env)

        modules = config[self._modules] if self._modules in config else config
        for key_path, value in flatten_dict(modules).items():
            full_key_path = (self._modules, *key_path)
            if full_key_path in self:
                self[full_key_path].current_value = value
                self[full_key_path].current_comment = comments.get(full_key_path)
            else:
                self[full_key_path] = ConfigEntry(
                    key_path=full_key_path,
                    current_value=value,
                    current_comment=comments.get(full_key_path),
                )
        # Activate all top level variables
        for key_path in self:
            if len(key_path) <= 3:
                self[key_path].is_active = True

        return self

    def load_variables(self, project_dir: Path, propagate_reused_variables: bool = False) -> ConfigYAML:
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
        variable_by_paren_key: dict[str, set[tuple[str, ...]]] = defaultdict(set)
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
                variable_by_paren_key[match].add(key_parent)

        for variable, key_parents in variable_by_paren_key.items():
            if len(key_parents) > 1 and propagate_reused_variables:
                key_parents = {self._find_common_parent(list(key_parents))}

            for key_parent in key_parents:
                key_path = (self._modules, *key_parent, variable)
                if key_path in self:
                    self[key_path].is_active = True
                else:
                    # Search for the first parent that match.
                    for i in range(len(key_parents) - 1, -1, -1):
                        alt_key_path = (self._modules, key_parent[0], *key_parent[i - 1 : len(key_parents)], variable)
                        if alt_key_path in self:
                            self[alt_key_path].is_active = True
                            break
                    else:
                        self[key_path] = ConfigEntry(key_path=key_path, is_active=True, current_value="<Not Set>")
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

    def dump(self, active: tuple[bool, ...] = (True,)) -> dict[str, Any]:
        config: dict[str, Any] = {}
        for entry in self.values():
            if entry.is_active not in active:
                continue
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

    def dump_yaml_with_comments(self, indent_size: int = 2, active: tuple[bool, ...] = (True,)) -> str:
        """Dump a config dictionary to a yaml string"""
        config = self.dump(active)
        dumped = yaml.dump(config, sort_keys=False, indent=indent_size)
        out_lines = []
        if (entry := self.get(tuple())) and entry.comment:
            for comment in entry.comment.above:
                out_lines.append(f"# {comment}")
        last_indent = 0
        last_variable: str | None = None
        path: tuple[str, ...] = tuple()
        for line in dumped.splitlines():
            indent = len(line) - len(line.lstrip())
            if last_indent < indent:
                if last_variable is None:
                    raise ValueError("Unexpected state of last_variable being None")
                path = (*path, last_variable)
            elif last_indent > indent:
                # Adding some extra space between modules
                out_lines.append("")
                indent_reduction_steps = (last_indent - indent) // indent_size
                path = path[:-indent_reduction_steps]

            variable = line.split(":", maxsplit=1)[0].strip()
            if (entry := self.get((*path, variable))) and entry.comment:
                for line_comment in entry.comment.above:
                    out_lines.append(f"{' ' * indent}# {line_comment}")
                if after := entry.comment.after:
                    line = f"{line} # {after[0]}"

            out_lines.append(line)
            last_indent = indent
            last_variable = variable
        out_lines.append("")
        return "\n".join(out_lines)

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

    @staticmethod
    def _extract_comments(raw_file: str, key_prefix: tuple[str, ...] = tuple()) -> dict[tuple[str, ...], YAMLComment]:
        """Extract comments from a raw file and return a dictionary with the comments."""
        comments: dict[tuple[str, ...], YAMLComment] = defaultdict(YAMLComment)
        position: Literal["above", "after"]
        init_value: object = object()
        variable: str | None | object = init_value
        last_comments: list[str] = []
        last_variable: str | None = None
        last_leading_spaces = 0
        parent_variables: list[str] = []
        indent: int | None = None
        for line in raw_file.splitlines():
            if ":" in line:
                # Is variable definition
                leading_spaces = len(line) - len(line.lstrip())
                variable = str(line.split(":", maxsplit=1)[0].strip())
                if leading_spaces > last_leading_spaces and last_variable:
                    parent_variables.append(last_variable)
                    if indent is None:
                        # Automatically indent based on the first variable
                        indent = leading_spaces
                elif leading_spaces < last_leading_spaces and parent_variables:
                    parent_variables = parent_variables[: -((last_leading_spaces - leading_spaces) // (indent or 2))]

                if last_comments:
                    comments[(*key_prefix, *parent_variables, variable)].above.extend(last_comments)
                    last_comments.clear()

                last_variable = variable
                last_leading_spaces = leading_spaces

            if "#" in line:
                # Potentially has comment.
                before, comment = str(line).rsplit("#", maxsplit=1)
                position = "after" if ":" in before else "above"
                if position == "after" and (before.count('"') % 2 == 1 or before.count("'") % 2 == 1):
                    # The comment is inside a string
                    continue
                # This is a new comment.
                if (position == "after" or variable is None) and variable is not init_value:
                    key = (*key_prefix, *parent_variables, *((variable and [variable]) or []))  # type: ignore[misc]
                    if position == "after":
                        comments[key].after.append(comment.strip())
                    else:
                        comments[key].above.append(comment.strip())
                else:
                    last_comments.append(comment.strip())

        return dict(comments)

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


class ConfigYAMLs(UserDict[str, ConfigYAML]):
    def __init__(self, entries: dict[str, ConfigYAML] | None = None):
        super().__init__(entries or {})

    @classmethod
    def load_default_environments(cls, default: dict[str, Any]) -> ConfigYAMLs:
        instance = cls()
        for environment_name, environment_config in default.items():
            environment = Environment.load(environment_config, environment_name)
            instance[environment.name] = ConfigYAML(environment=environment)
        return instance

    @classmethod
    def load_existing_environments(cls, existing_config_yamls: Sequence[Path]) -> ConfigYAMLs:
        instance = cls()
        for config_yaml in existing_config_yamls:
            config = ConfigYAML().load_existing(config_yaml.read_text(), config_yaml.name.split(".")[0])
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
