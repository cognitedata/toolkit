from __future__ import annotations

import itertools
import os
import re
import shutil
import tempfile
import urllib
import zipfile
from abc import ABC, abstractmethod
from collections import UserDict, defaultdict
from collections.abc import Sequence
from dataclasses import dataclass, field
from importlib import resources
from pathlib import Path
from typing import Any, ClassVar, Literal, TypeVar, cast

import yaml
from rich import print

from cognite_toolkit import _version
from cognite_toolkit.cdf_tk.load import LOADER_BY_FOLDER_NAME
from cognite_toolkit.cdf_tk.utils import read_yaml_file

from ._constants import (
    BUILD_ENVIRONMENT_FILE,
    COGNITE_MODULES,
    CUSTOM_MODULES,
    DEFAULT_CONFIG_FILE,
    SEARCH_VARIABLES_SUFFIX,
)
from ._utils import flatten_dict, iterate_modules

__all__ = ["SystemConfig", "BuildConfigYAML", "InitConfigYAML", "ConfigYAMLs", "BuildEnvironment"]


@dataclass
class ConfigCore(ABC):
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


T_BuildConfig = TypeVar("T_BuildConfig", bound=ConfigCore)


@dataclass
class SystemConfig(ConfigCore):
    file_name: ClassVar[str] = "_system.yaml"
    cdf_toolkit_version: str
    packages: dict[str, list[str]] = field(default_factory=dict)

    @classmethod
    def _file_name(cls, build_env: str) -> str:
        return cls.file_name

    @classmethod
    def load(cls, data: dict[str, Any], build_env: str, filepath: Path) -> SystemConfig:
        version = _load_version_variable(data, filepath.name)
        packages = data.get("packages", {})
        if not packages:
            print(f"  [bold yellow]Warning:[/] No packages defined in {cls.file_name}.")
        return cls(
            filepath=filepath,
            cdf_toolkit_version=version,
            packages=packages,
        )

    def validate_modules(self, available_modules: set[str], selected_modules_and_packages: list[str]) -> None:
        selected_packages = {package for package in selected_modules_and_packages if package in self.packages}
        for package, modules in self.packages.items():
            if package not in selected_packages:
                # We do not check packages that are not selected.
                # Typically, the user will delete the modules that are irrelevant for them,
                # thus we only check the selected packages.
                continue
            if missing := set(modules) - available_modules:
                print(
                    f"  [bold red]ERROR:[/] Package {package} defined in {self.filepath.name!s} is referring "
                    f"the following missing modules {missing}."
                )
                exit(1)


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

    @classmethod
    def _file_name(cls, build_env: str) -> str:
        return f"config.{build_env}.yaml"

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
    def load(cls, data: dict[str, Any], build_env: str, filepath: Path) -> BuildConfigYAML:
        try:
            environment = Environment.load(data["environment"], build_env)
            modules = data["modules"]
        except KeyError:
            print(f"  [bold red]ERROR:[/] Missing 'environment' or 'modules' in {filepath!s}")
            exit(1)
        return cls(environment=environment, modules=modules, filepath=filepath)

    def create_build_environment(self, system_config: SystemConfig) -> BuildEnvironment:
        return BuildEnvironment(
            name=self.environment.name,  # type: ignore[arg-type]
            project=self.environment.project,
            build_type=self.environment.build_type,
            selected_modules_and_packages=self.environment.selected_modules_and_packages,
            common_function_code=self.environment.common_function_code,
            cdf_toolkit_version=system_config.cdf_toolkit_version,
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
            "# # DO NOT EDIT THIS FILE!\n" + yaml.dump(self.dump(), sort_keys=False, indent=2)
        )

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
class InitConfigYAML(UserDict[tuple[str, ...], ConfigEntry], ConfigYAMLCore):
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
                    continue
                # Search for the first parent that match.
                for i in range(len(key_parents) - 1, -1, -1):
                    alt_key_path = (self._modules, key_parent[0], *key_parent[i - 1 : len(key_parents)], variable)
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
        config = self.dump()
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


def _load_version_variable(data: dict[str, Any], file_name: str) -> str:
    try:
        cdf_tk_version: str = data["cdf_toolkit_version"]
    except KeyError:
        print(
            f"  [bold red]ERROR:[/] System variables are missing required field 'cdf_toolkit_version' in {file_name!s}"
        )
        if file_name == BUILD_ENVIRONMENT_FILE:
            print(f"  rerun `cdf-tk build` to build the templates again and create `{file_name!s}` correctly.")
        else:
            print(
                f"  run `cdf-tk init --upgrade` to initialize the templates again and create a correct `{file_name!s}` file."
            )
        exit(1)
    if cdf_tk_version != _version.__version__:
        print(
            f"  [bold red]Error:[/] The version of the templates ({cdf_tk_version}) does not match the version of the installed package ({_version.__version__})."
        )
        print("  Please either run `cdf-tk init --upgrade` to upgrade the templates OR")
        print(f"  run `pip install cognite-toolkit==={cdf_tk_version}` to downgrade cdf-tk.")
        exit(1)
    return cdf_tk_version


class ProjectDirectory:
    """This represents the project directory, and is used in the init command.

    It is responsible for copying the files from the templates to the project directory.

    Args:
        project_dir: The project directory.
        dry_run: Whether to do a dry run or not.
    """

    def __init__(self, project_dir: Path, dry_run: bool):
        self.project_dir = project_dir
        self._dry_run = dry_run
        self._files_to_copy: list[str] = [
            "README.md",
            ".gitignore",
            ".env.tmpl",
        ]
        self._root_modules: list[str] = [
            COGNITE_MODULES,
            CUSTOM_MODULES,
        ]
        self._source = Path(resources.files("cognite_toolkit"))  # type: ignore[arg-type]
        self.modules_by_root: dict[str, list[str]] = {}
        for root_module in self._root_modules:
            self.modules_by_root[root_module] = [
                f"{module.relative_to(self._source)!s}" for module, _ in iterate_modules(self._source / root_module)
            ]

    def set_source(self, git_branch: str | None) -> None:
        ...

    @property
    def target_dir_display(self) -> str:
        return f"'{self.project_dir.relative_to(Path.cwd())!s}'"

    @abstractmethod
    def create_project_directory(self, clean: bool) -> None:
        ...

    def print_what_to_copy(self) -> None:
        copy_prefix = "Would" if self._dry_run else "Will"
        print(f"{copy_prefix} copy these files to {self.target_dir_display}:")
        print(self._files_to_copy)

        for root_module, modules in self.modules_by_root.items():
            print(f"{copy_prefix} copy these modules to {self.target_dir_display} from {root_module}:")
            print(modules)

    def copy(self, verbose: bool) -> None:
        dry_run = self._dry_run
        copy_prefix = "Would copy" if dry_run else "Copying"
        for filename in self._files_to_copy:
            if verbose:
                print(f"{copy_prefix} file {filename} to {self.target_dir_display}")
            if not dry_run:
                if filename == "README.md":
                    content = (self._source / filename).read_text().replace("<MY_PROJECT>", self._source.name)
                    (self.project_dir / filename).write_text(content)
                else:
                    shutil.copyfile(self._source / filename, self.project_dir / filename)

        for root_module in self._root_modules:
            if verbose:
                print(f"{copy_prefix} the following modules from  {root_module} to {self.target_dir_display}")
                print(self.modules_by_root[root_module])
            if not dry_run:
                (Path(self.project_dir) / root_module).mkdir(exist_ok=True)
                # Default files are not copied, as they are only used to setup the config.yaml.
                shutil.copytree(
                    self._source / root_module,
                    self.project_dir / root_module,
                    dirs_exist_ok=True,
                    ignore=shutil.ignore_patterns("default.*"),
                )

    @abstractmethod
    def done_message(self) -> str:
        raise NotImplementedError()


class ProjectDirectoryInit(ProjectDirectory):
    """This represents the project directory, and is used in the init command.
    It is used when creating a new project (or overwriting an existing one).
    """

    def create_project_directory(self, clean: bool) -> None:
        if self.project_dir.exists() and not clean:
            print(f"Directory {self.target_dir_display} already exists.")
            exit(1)
        elif self.project_dir.exists() and clean and self._dry_run:
            print(f"Would clean out directory {self.target_dir_display}...")
        elif self.project_dir.exists() and clean:
            print(f"Cleaning out directory {self.target_dir_display}...")
            shutil.rmtree(self.project_dir)
        else:
            print(f"Found no directory {self.target_dir_display} to upgrade.")
            exit(1)

        if not self._dry_run:
            self.project_dir.mkdir(exist_ok=True)

    def create_config_yamls(self) -> None:
        # Creating the config.[environment].yaml files
        environment_default = self._source / COGNITE_MODULES / "default.environments.yaml"
        if not environment_default.is_file():
            print(
                f"  [bold red]ERROR:[/] Could not find default.environments.yaml in {environment_default.parent.relative_to(Path.cwd())!s}. "
                f"There is something wrong with your installation, try to reinstall `cognite-tk`, and if the problem persists, please contact support."
            )
            exit(1)

        config_yamls = ConfigYAMLs.load_default_environments(read_yaml_file(environment_default))

        config_yamls.load_default_variables(self._source)
        config_yamls.load_variables(self._source)

        for environment, config_yaml in config_yamls.items():
            config_filepath = self.project_dir / f"config.{environment}.yaml"

            print(f"Created config for {environment!r} environment.")
            if self._dry_run:
                print(f"Would write {config_filepath.name!r} to {self.target_dir_display}")
            else:
                config_filepath.write_text(config_yaml.dump_yaml_with_comments(indent_size=2))
                print(f"Wrote {config_filepath.name!r} file to {self.target_dir_display}")

    def done_message(self) -> str:
        return f"A new project was created in {self.target_dir_display}."


class ProjectDirectoryUpgrade(ProjectDirectory):
    """This represents the project directory, and is used in the init command.

    It is used when upgrading an existing project.

    """

    def create_project_directory(self, clean: bool) -> None:
        if self.project_dir.exists():
            print(f"[bold]Upgrading directory {self.target_dir_display}...[/b]")
        else:
            print(f"Found no directory {self.target_dir_display} to upgrade.")
            exit(1)

    def do_backup(self, no_backup: bool, verbose: bool) -> None:
        if not no_backup:
            prefix = "Would have backed up" if self._dry_run else "Backing up"
            if verbose:
                print(f"{prefix} {self.target_dir_display}")
            if not self._dry_run:
                backup_dir = tempfile.mkdtemp(prefix=f"{self.project_dir.name}.", suffix=".bck", dir=Path.cwd())
                shutil.copytree(self.project_dir, Path(backup_dir), dirs_exist_ok=True)
        else:
            print(
                f"[bold yellow]WARNING:[/] --no-backup is specified, no backup {'would have been' if self._dry_run else 'will be'} be."
            )

    def print_what_to_copy(self) -> None:
        print("  Will upgrade modules and files in place.")
        super().print_what_to_copy()

    def set_source(self, git_branch: str | None) -> None:
        if git_branch is None:
            return

        self._source = self._download_templates(git_branch, self._dry_run)

    def done_message(self) -> str:
        return f"You project in {self.target_dir_display} was upgraded."

    @staticmethod
    def _download_templates(git_branch: str, dry_run: bool) -> Path:
        toolkit_github_url = f"https://github.com/cognitedata/cdf-project-templates/archive/refs/heads/{git_branch}.zip"
        extract_dir = tempfile.mkdtemp(prefix="git.", suffix=".tmp", dir=Path.cwd())
        prefix = "Would download" if dry_run else "Downloading"
        print(f"{prefix} templates from https://github.com/cognitedata/cdf-project-templates, branch {git_branch}...")
        print(
            "  [bold yellow]WARNING:[/] You are only upgrading templates, not the cdf-tk tool. "
            "Your current version may not support the new templates."
        )
        if not dry_run:
            try:
                zip_path, _ = urllib.request.urlretrieve(toolkit_github_url)
                with zipfile.ZipFile(zip_path, "r") as f:
                    f.extractall(extract_dir)
            except Exception:
                print(
                    f"Failed to download templates. Are you sure that the branch {git_branch} exists in"
                    + "the https://github.com/cognitedata/cdf-project-templatesrepository?\n{e}"
                )
                exit(1)
        return Path(extract_dir) / f"cdf-project-templates-{git_branch}" / "cognite_toolkit"
