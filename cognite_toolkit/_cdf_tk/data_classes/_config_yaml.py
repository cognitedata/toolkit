from __future__ import annotations

import itertools
import os
import re
from abc import ABC
from collections import UserDict, defaultdict
from collections.abc import Hashable, Iterable, Sequence, Set
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, ClassVar, Literal, cast, get_args

import yaml
from rich import print

from cognite_toolkit._cdf_tk.constants import (
    _RUNNING_IN_BROWSER,
    BUILD_ENVIRONMENT_FILE,
    BUILTIN_MODULES,
    DEFAULT_CONFIG_FILE,
    DEFAULT_ENV,
    MODULES,
    ROOT_MODULES,
    SEARCH_VARIABLES_SUFFIX,
    EnvType,
)
from cognite_toolkit._cdf_tk.exceptions import ToolkitEnvError, ToolkitMissingModuleError
from cognite_toolkit._cdf_tk.hints import ModuleDefinition
from cognite_toolkit._cdf_tk.loaders import LOADER_BY_FOLDER_NAME, RawDatabaseLoader
from cognite_toolkit._cdf_tk.tk_warnings import (
    FileReadWarning,
    MediumSeverityWarning,
    MissingFileWarning,
    SourceFileModifiedWarning,
    ToolkitWarning,
    WarningList,
)
from cognite_toolkit._cdf_tk.utils import (
    YAMLComment,
    YAMLWithComments,
    calculate_str_or_file_hash,
    flatten_dict,
    read_yaml_content,
    safe_read,
)
from cognite_toolkit._cdf_tk.utils.modules import parse_user_selected_modules
from cognite_toolkit._version import __version__

from . import BuiltModuleList
from ._base import ConfigCore, _load_version_variable
from ._built_resources import BuiltResourceList
from ._module_directories import ModuleDirectories, ReadModule

_AVAILABLE_ENV_TYPES = tuple(get_args(EnvType))


@dataclass
class Environment:
    name: str = "dev"
    project: str = field(default_factory=lambda: os.environ.get("CDF_PROJECT", "UNKNOWN"))
    validation_type: str = "dev"
    selected: list[str | Path] = field(default_factory=lambda: [Path(MODULES)])

    @property
    def is_strict_validation(self) -> bool:
        return self.validation_type.casefold() != "dev"

    @classmethod
    def load(cls, data: dict[str, Any], build_name: str) -> Environment:
        _deprecation_selected(data)
        if "name" not in data:
            data["name"] = build_name
        _deprecate_type(data, build_name)
        if missing := {"name", "project", "validation-type", "selected"} - set(data.keys()):
            raise ToolkitEnvError(
                f"Environment section is missing one or more required fields: {missing} in {BuildConfigYAML.get_filename(build_name)!s}"
            )
        return Environment(
            name=build_name,
            project=data["project"],
            validation_type=data["validation-type"],
            selected=parse_user_selected_modules(data.get("selected")),
        )

    def dump(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "project": self.project,
            "validation-type": self.validation_type,
            "selected": [
                selected.as_posix() + "/" if isinstance(selected, Path) else selected for selected in self.selected
            ],
        }

    def get_selected_modules(self, modules_by_package: dict[str, list[str]]) -> set[str | Path]:
        selected_modules: set[str | Path] = set()
        for selected in self.selected:
            if selected in modules_by_package and isinstance(selected, str):
                selected_modules.update(modules_by_package[selected])
            else:
                selected_modules.add(selected)
        return selected_modules


@dataclass
class ConfigYAMLCore(ABC):
    environment: Environment = field(default_factory=Environment)


@dataclass
class BuildConfigYAML(ConfigYAMLCore, ConfigCore):
    """This is the config.[env].yaml file used in the cdf-tk build command."""

    filename: ClassVar[str] = "config.{build_env}.yaml"
    variables: dict[str, Any] = field(default_factory=dict)

    def set_environment_variables(self) -> None:
        os.environ["CDF_ENVIRON"] = self.environment.name
        os.environ["CDF_BUILD_TYPE"] = self.environment.validation_type

    def validate_environment(self) -> ToolkitWarning | None:
        if _RUNNING_IN_BROWSER:
            return None
        project = self.environment.project
        project_env = os.environ.get("CDF_PROJECT")
        if project_env == project:
            return None

        is_strict_validation = self.environment.is_strict_validation
        env_name = self.environment.name
        file_name = self.get_filename(env_name)
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
        if is_strict_validation and project_env is None:
            raise ToolkitEnvError(missing_message)
        elif is_strict_validation:
            raise ToolkitEnvError(mismatch_message)
        elif not is_strict_validation and project_env is None:
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

    def create_build_environment(
        self, built_modules: BuiltModuleList, selected_modules: ModuleDirectories
    ) -> BuildEnvironment:
        return BuildEnvironment(
            name=self.environment.name,  # type: ignore[arg-type]
            project=self.environment.project,
            validation_type=self.environment.validation_type,
            selected=self.environment.selected,
            cdf_toolkit_version=__version__,
            built_resources=built_modules.as_resources_by_folder(),
            read_modules=[module.as_read_module() for module in selected_modules],
        )

    def get_selected_modules(
        self,
        modules_by_package: dict[str, list[str | Path]],
        available_modules: set[str | Path],
        organization_dir: Path,
        verbose: bool,
    ) -> list[str | Path]:
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
            hint = ModuleDefinition.long(missing, organization_dir)
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
                if isinstance(module, Path):
                    print(f"    {module.as_posix()}")
                else:
                    print(f"    {module}")
        return selected_modules

    @classmethod
    def load_default(cls, organization_dir: Path) -> BuildConfigYAML:
        return cls(filepath=organization_dir / BuildConfigYAML.get_filename(DEFAULT_ENV))

    def dump(self) -> dict[str, Any]:
        return {"environment": self.environment.dump(), "variables": self.variables}


@dataclass
class BuildEnvironment(Environment):
    cdf_toolkit_version: str = __version__
    built_resources: dict[str, BuiltResourceList] = field(default_factory=dict)
    read_modules: list[ReadModule] = field(default_factory=list)

    @property
    def read_resource_folders(self) -> set[str]:
        return {resource_folder for module in self.read_modules for resource_folder in module.resource_directories}

    def dump(self) -> dict[str, Any]:
        output = super().dump()
        output["cdf_toolkit_version"] = self.cdf_toolkit_version
        if self.built_resources:
            output["built_resources"] = {
                resource_folder: resources.dump(resource_folder, include_destination=True)
                for resource_folder, resources in self.built_resources.items()
            }
        if self.read_modules:
            output["read_modules"] = [module.dump() for module in self.read_modules]
        return output

    def dump_to_file(self, build_dir: Path) -> None:
        (build_dir / BUILD_ENVIRONMENT_FILE).write_text(
            "# DO NOT EDIT THIS FILE!\n" + yaml.dump(self.dump(), sort_keys=False, indent=2)
        )

    @classmethod
    def load(
        cls, data: dict[str, Any], build_name: str | None, action: Literal["build", "deploy", "clean", "pull"] = "build"
    ) -> BuildEnvironment:
        if "name" in data and build_name is not None and data["name"] != build_name:
            raise ToolkitEnvError(
                f"Expected to {action} for {build_name!r} environment, but the last "
                f"build was created for the {data['name']!r} environment."
            )
        build_name = build_name or data.get("name")

        version = _load_version_variable(data, BUILD_ENVIRONMENT_FILE)
        _deprecation_selected(data)
        built_resources: dict[str, BuiltResourceList] = {}
        if "built_resources" in data:
            # We expect to dump BuildEnvironment, and load DeployEnvironment
            built_resources = {
                resource_folder: BuiltResourceList.load(resources, resource_folder)
                for resource_folder, resources in data["built_resources"].items()
            }
        read_modules: list[ReadModule] = []
        if "read_modules" in data:
            read_modules = [ReadModule.load(module_data) for module_data in data["read_modules"]]
        _deprecate_type(data, build_name or "dev")
        try:
            return cls(
                name=data["name"],
                project=data["project"],
                validation_type=data["validation-type"],
                selected=data["selected"],
                cdf_toolkit_version=version,
                built_resources=built_resources,
                read_modules=read_modules,
            )
        except KeyError:
            raise ToolkitEnvError(
                f"  [bold red]ERROR:[/] Environment {build_name} is missing required fields 'name', 'project', 'validation-type', "
                f"or 'selected' in {BUILD_ENVIRONMENT_FILE!s}"
            )

    def set_environment_variables(self) -> None:
        os.environ["CDF_ENVIRON"] = self.name
        os.environ["CDF_BUILD_TYPE"] = self.validation_type

    def check_source_files_changed(self) -> WarningList[FileReadWarning]:
        warning_list = WarningList[FileReadWarning]()
        for resource_folder, resources in self.built_resources.items():
            if resource_folder == RawDatabaseLoader.folder_name:
                # We modify the hash for RawDatabaseLoader, so we skip checking the hash for this folder.
                continue
            for resource in resources:
                to_check = [resource.source, *(resource.extra_sources or [])]
                for source in to_check:
                    source_filepath = source.path
                    if source_filepath.suffix in {".csv", ".parquet"}:
                        # When we copy over the source files we use utf-8 encoding, which can change the file hash.
                        # Thus, we skip checking the hash for these file types.
                        continue

                    if not source_filepath.exists():
                        warning_list.append(
                            MissingFileWarning(source_filepath, attempted_check="source file has changed")
                        )
                    elif source.hash != calculate_str_or_file_hash(source_filepath, shorten=True):
                        warning_list.append(SourceFileModifiedWarning(source_filepath))
        return warning_list


def _deprecation_selected(data: dict[str, Any]) -> None:
    if "selected_modules_and_packages" in data and "selected" not in data:
        print(
            "  [bold yellow]Warning:[/] In environment section: 'selected_modules_and_packages' "
            "is deprecated, use 'selected' instead."
        )
        data["selected"] = data.pop("selected_modules_and_packages")


def _deprecate_type(data: dict[str, Any], build_name: str) -> None:
    if "type" in data and "validation-type" not in data:
        MediumSeverityWarning(
            f"In environment section of {BuildConfigYAML.get_filename(build_name)!s}: 'type' is deprecated, use 'validation-type' instead."
        ).print_warning()
        data["validation-type"] = data.pop("type")


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


class _WildcardSequence(tuple, Sequence[str]):
    def __eq__(self, other: Any) -> bool:
        return (
            isinstance(other, Sequence)
            and len(other) == len(self)
            and all(a == b or a == "*" for a, b in zip(self, other))
        )

    def __hash__(self) -> int:
        return hash(tuple(self))


class WildcardSet(set, Set[_WildcardSequence]):
    """Sets that support wildcard matching."""

    @classmethod
    def load(cls, items: Sequence[Sequence[str]]) -> WildcardSet:
        return cls(_WildcardSequence(item) for item in items)

    def __contains__(self, key: Any) -> bool:
        if not isinstance(key, Sequence):
            return False
        return any(pattern == key for pattern in self)


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

    def load_defaults(
        self,
        cognite_root_module: Path,
        selected_paths: set[Path] | None = None,
        ignore_patterns: list[tuple[str, ...]] | None = None,
    ) -> InitConfigYAML:
        """Loads all default.config.yaml files in the cognite root module."""

        default_files_iterable: Iterable[Path]
        if cognite_root_module.name in ROOT_MODULES or cognite_root_module.name == BUILTIN_MODULES:
            default_files_iterable = cognite_root_module.glob(f"**/{DEFAULT_CONFIG_FILE}")
        else:
            default_files_iterable = itertools.chain(
                *[
                    (cognite_root_module / root_module).glob(f"**/{DEFAULT_CONFIG_FILE}")
                    for root_module in ROOT_MODULES
                    if (cognite_root_module / root_module).exists()
                ]
            )
        if selected_paths:
            default_files_iterable = (
                file
                for file in default_files_iterable
                if file.relative_to(cognite_root_module).parent in selected_paths
            )

        default_files = sorted(default_files_iterable, key=lambda f: f.relative_to(cognite_root_module))
        return self._load_defaults(cognite_root_module, default_files, ignore_patterns)

    def _load_defaults(
        self, cognite_root_module: Path, defaults_files: list[Path], ignore_patterns: list[tuple[str, ...]] | None
    ) -> InitConfigYAML:
        """Loads all default.config.yaml files in the cognite root module.

        This extracts the default values from the default.config.yaml files and
        adds them to the config.yaml file.

        Args:
            cognite_root_module: The root module for all cognite modules.

        Returns:
            self
        """

        ignore_set = WildcardSet.load(ignore_patterns) if ignore_patterns else None

        for default_config in defaults_files:
            parts = default_config.parent.relative_to(cognite_root_module).parts
            raw_file = safe_read(default_config)
            file_comments = self._extract_comments(raw_file, key_prefix=tuple(parts))
            file_data = cast(dict, read_yaml_content(raw_file))
            for key, value in file_data.items():
                if len(parts) >= 1 and parts[0] in ROOT_MODULES:
                    key_path = (self._variables, *parts, key)
                else:
                    key_path = (self._variables, MODULES, *parts, key)
                local_file_path = (*parts, key)
                if ignore_set and key_path[1:] in ignore_set:
                    continue
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
        config = cast(dict, read_yaml_content(raw_file))
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

    def load_variables(self, organization_dir: Path, propagate_reused_variables: bool = False) -> InitConfigYAML:
        """This scans the content the files in the given directories and finds the variables.
        The motivation is to find the variables that are used in the templates, as well
        as picking up variables that are used in custom modules.

        Variables are marked with a {{ variable }} syntax.

        Args:
            organization_dir: The directory with all project configurations.
            propagate_reused_variables: Whether to move variables with the same name to a shared parent.

        Returns:
            self
        """
        variable_by_parent_key: dict[str, set[tuple[str, ...]]] = defaultdict(set)
        for filepath in organization_dir.glob("**/*"):
            if filepath.suffix.lower() not in SEARCH_VARIABLES_SUFFIX:
                continue
            if filepath.name.startswith("default"):
                continue
            content = safe_read(filepath)
            key_parent = filepath.parent.relative_to(organization_dir).parts
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

    def lift(self) -> None:
        """Lift variables that are used in multiple modules to the highest shared level"""
        variables_by_key_value: dict[
            tuple[str, float | int | str | bool | tuple[Hashable] | None], list[ConfigEntry]
        ] = defaultdict(list)
        count_by_variable_keys: dict[str, set[float | int | str | bool | tuple[Hashable] | None]] = defaultdict(set)
        for key, entry in self.items():
            value = tuple(entry.value) if isinstance(entry.value, list) else entry.value  # type: ignore[arg-type]
            variables_by_key_value[(key[-1], value)].append(entry)
            count_by_variable_keys[key[-1]].add(value)

        for (variable_name, _), entries in variables_by_key_value.items():
            if len(entries) == 1:
                continue
            if len(count_by_variable_keys[variable_name]) > 1:
                continue
            shared_parent = self._find_common_parent([entry.key_path for entry in entries])
            new_key = (*shared_parent, entries[0].key_path[-1])
            self[new_key] = ConfigEntry(
                key_path=new_key, current_value=entries[0].current_value, default_value=entries[0].default_value
            )
            for entry in entries:
                del self[entry.key_path]

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
            config = InitConfigYAML.load_existing(safe_read(config_yaml), config_yaml.name.split(".")[0])
            instance[config.environment.name] = config
        return instance

    def load_default_variables(self, cognite_module: Path) -> None:
        # Can be optimized, but not a priority
        for config_yaml in self.values():
            config_yaml.load_defaults(cognite_module)

    def load_variables(self, organization_dir: Path) -> None:
        # Can be optimized, but not a priority
        for config_yaml in self.values():
            config_yaml.load_variables(organization_dir)
