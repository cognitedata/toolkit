from __future__ import annotations

import datetime
import io
import itertools
import os
import re
import shutil
from collections import ChainMap, UserDict, defaultdict
from collections.abc import Iterator, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal, cast, overload

import pandas as pd
import yaml
from rich import print

from cognite_toolkit import _version
from cognite_toolkit.cdf_tk.load import LOADER_BY_FOLDER_NAME, Loader, ResourceLoader
from cognite_toolkit.cdf_tk.utils import validate_case_raw, validate_config_yaml, validate_data_set_is_set

# This is the default config located locally in each module.
DEFAULT_CONFIG_FILE = "default.config.yaml"
# The environment file:
ENVIRONMENTS_FILE = "environments.yaml"
BUILD_ENVIRONMENT_FILE = "build_environment.yaml"
# The local config file:
CONFIG_FILE = "config.yaml"
# The default package files
DEFAULT_PACKAGES_FILE = "default.packages.yaml"
# The package files:
PACKAGES_FILE = "packages.yaml"
COGNITE_MODULES = "cognite_modules"
CUSTOM_MODULES = "custom_modules"

# Add any other files below that should be included in a build
EXCL_FILES = ["README.md", DEFAULT_CONFIG_FILE]
# Which suffixes to exclude when we create indexed files (i.e., they are bundled with their main config file)
EXCL_INDEX_SUFFIX = frozenset([".sql", ".csv", ".parquet"])
# Which suffixes to process for template variable replacement
PROC_TMPL_VARS_SUFFIX = frozenset([".yaml", ".yml", ".sql", ".csv", ".parquet", ".json", ".txt", ".md", ".html", ".py"])


@dataclass
class BuildEnvironment:
    name: Literal["dev", "local", "demo", "staging", "prod"]
    project: str
    build_type: str
    deploy: list[str]
    system: SystemVariables

    @classmethod
    def load(
        cls, environment_config: dict[str, Any], build_env: str, action: Literal["build", "deploy", "clean"]
    ) -> BuildEnvironment:
        if build_env is None:
            raise ValueError("build_env must be specified")
        environment = environment_config.get(build_env)
        if environment is None:
            print(f"  [bold red]ERROR:[/] Environment {build_env} not found in {ENVIRONMENTS_FILE!s}")
            exit(1)
        system = SystemVariables.load(environment_config, action)
        try:
            return BuildEnvironment(
                name=cast(Literal["dev", "local", "demo", "staging", "prod"], build_env),
                project=environment["project"],
                build_type=environment["type"],
                deploy=environment["deploy"],
                system=system,
            )
        except KeyError:
            print(
                f"  [bold red]ERROR:[/] Environment {build_env} is missing required fields 'project', 'type', or 'deploy' in {ENVIRONMENTS_FILE!s}"
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

    def validate_environment(self) -> None:
        if (project_env := os.environ.get("CDF_PROJECT", "<not set>")) != self.project:
            if self.name in {"dev", "local", "demo"}:
                print(
                    f"  [bold yellow]WARNING:[/] Project name mismatch (CDF_PROJECT) between {ENVIRONMENTS_FILE!s} ({self.project}) and what is defined in environment ({project_env})."
                )
                print(f"  Environment is {self.name}, continuing (would have stopped for staging and prod)...")
            else:
                print(
                    f"  [bold red]ERROR:[/] Project name mismatch (CDF_PROJECT) between {ENVIRONMENTS_FILE!s} ({self.project}) and what is defined in environment ({project_env=} != {self.project=})."
                )
                exit(1)
        return None

    def set_environment_variables(self) -> None:
        os.environ["CDF_ENVIRON"] = self.name
        os.environ["CDF_BUILD_TYPE"] = self.build_type


@dataclass
class SystemVariables:
    cdf_toolkit_version: str

    @classmethod
    def load(cls, data: dict[str, Any], action: Literal["build", "deploy", "clean"]) -> SystemVariables:
        file_name = BUILD_ENVIRONMENT_FILE if action in {"deploy", "clean"} else ENVIRONMENTS_FILE
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


def get_selected_modules(
    source_module: Path,
    selected_module_and_packages: list[str],
    build_env: str,
    verbose: bool = False,
) -> list[str]:
    modules_by_package = _read_packages(source_module, verbose)
    selected_packages = [package for package in selected_module_and_packages if package in modules_by_package]
    if verbose:
        print("  [bold green]INFO:[/] Selected packages:")
        for package in selected_packages:
            print(f"    {package}")

    selected_modules = [module for module in selected_module_and_packages if module not in modules_by_package]
    selected_modules.extend(itertools.chain.from_iterable(modules_by_package[package] for package in selected_packages))

    if verbose:
        print("  [bold green]INFO:[/] Selected modules:")
        for module in selected_modules:
            print(f"    {module}")
    if not selected_modules:
        print(
            f"  [bold yellow]WARNING:[/] Found no defined modules in {ENVIRONMENTS_FILE!s}, have you configured the environment ({build_env})?"
        )
        exit(1)

    available_modules = {module.name for module, _ in iterate_modules(source_module)}
    if not (missing_modules := set(selected_modules) - available_modules):
        return selected_modules

    print(f"  [bold red]ERROR:[/] Modules {missing_modules} not found in {source_module}.")
    exit(1)


def _read_packages(source_module: Path, verbose: bool) -> dict[str, Any]:
    cdf_modules_by_packages = read_yaml_file(source_module / COGNITE_MODULES / DEFAULT_PACKAGES_FILE).get(
        "packages", {}
    )
    if (package_path := source_module / COGNITE_MODULES / PACKAGES_FILE).exists():
        local_modules_by_packages = read_yaml_file(package_path).get("packages", {})
        if overwrites := set(cdf_modules_by_packages.keys()) & set(local_modules_by_packages.keys()):
            print(
                f"  [bold yellow]WARNING:[/] Found modules in {PACKAGES_FILE} that are also defined in {DEFAULT_PACKAGES_FILE}:"
            )
            for module in overwrites:
                print(f"    {module}")
            print(f"  Using the modules defined in {PACKAGES_FILE}.")
        modules_by_package = {**cdf_modules_by_packages, **local_modules_by_packages}
    else:
        modules_by_package = cdf_modules_by_packages
    if verbose:
        print("  [bold green]INFO:[/] Found defined packages:")
        for name, content in modules_by_package.items():
            print(f"    {name}: {content}")
    return modules_by_package


@overload
def read_yaml_file(filepath: Path, expected_output: Literal["dict"] = "dict") -> dict[str, Any]:
    ...


@overload
def read_yaml_file(filepath: Path, expected_output: Literal["list"]) -> list[dict[str, Any]]:
    ...


def read_yaml_file(
    filepath: Path, expected_output: Literal["list", "dict"] = "dict"
) -> dict[str, Any] | list[dict[str, Any]]:
    """Read a YAML file and return a dictionary

    filepath: path to the YAML file
    """
    try:
        config_data = yaml.safe_load(filepath.read_text())
    except yaml.YAMLError as e:
        print(f"  [bold red]ERROR:[/] reading {filepath}: {e}")
        return {}
    if expected_output == "list" and isinstance(config_data, dict):
        print(f"  [bold red]ERROR:[/] {filepath} is not a list")
        exit(1)
    elif expected_output == "dict" and isinstance(config_data, list):
        print(f"  [bold red]ERROR:[/] {filepath} is not a dict")
        exit(1)
    return config_data


def check_yaml_semantics(parsed: dict | list, filepath_src: Path, filepath_build: Path, verbose: bool = False) -> bool:
    """Check the yaml file for semantic errors

    parsed: the parsed yaml file
    filepath: the path to the yaml file
    yields: True if the yaml file is semantically acceptable, False if build should fail.
    """
    if parsed is None or filepath_src is None or filepath_build is None:
        return False
    resource_type = filepath_src.parent.name
    ext_id = None
    if resource_type == "data_models" and ".space." in filepath_src.name:
        if isinstance(parsed, list):
            print(f"      [bold red]:[/] Multiple spaces in one file {filepath_src} is not supported .")
            exit(1)
        elif isinstance(parsed, dict):
            ext_id = parsed.get("space")
        else:
            print(f"      [bold red]:[/] Space file {filepath_src} has invalid dataformat.")
            exit(1)
        ext_id_type = "space"
    elif resource_type == "data_models" and ".node." in filepath_src.name:
        if isinstance(parsed, list):
            print(f"      [bold red]:[/] Nodes YAML must be an object file {filepath_src} is not supported .")
            exit(1)
        try:
            ext_ids = {source["source"]["externalId"] for node in parsed["nodes"] for source in node["sources"]}
        except KeyError:
            print(f"      [bold red]:[/] Node file {filepath_src} has invalid dataformat.")
            exit(1)
        if len(ext_ids) != 1:
            print(f"      [bold red]:[/] All nodes in {filepath_src} must have the same view.")
            exit(1)
        ext_id = ext_ids.pop()
        ext_id_type = "view.externalId"
    elif resource_type == "auth":
        if isinstance(parsed, list):
            print(f"      [bold red]:[/] Multiple Groups in one file {filepath_src} is not supported .")
            exit(1)
        ext_id = parsed.get("name")
        ext_id_type = "name"
    elif resource_type in ["data_sets", "timeseries", "files"] and isinstance(parsed, list):
        ext_id = ""
        ext_id_type = "multiple"
    elif resource_type == "raw":
        if isinstance(parsed, list):
            ext_id = ""
            ext_id_type = "multiple"
        elif isinstance(parsed, dict):
            ext_id = parsed.get("dbName")
            ext_id_type = "dbName"
            if "tableName" in parsed:
                ext_id = f"{ext_id}.{parsed.get('tableName')}"
                ext_id_type = "dbName and tableName"
        else:
            print(f"      [bold red]:[/] Raw file {filepath_src} has invalid dataformat.")
            exit(1)
    else:
        if isinstance(parsed, list):
            print(f"      [bold red]:[/] Multiple {resource_type} in one file {filepath_src} is not supported .")
            exit(1)
        ext_id = parsed.get("externalId") or parsed.get("external_id")
        ext_id_type = "externalId"

    if ext_id is None:
        print(
            f"      [bold yellow]WARNING:[/] the {resource_type} {filepath_src} is missing the {ext_id_type} field(s)."
        )
        return False

    if resource_type == "auth":
        parts = ext_id.split("_")
        if len(parts) < 2:
            if ext_id == "applications-configuration":
                if verbose:
                    print(
                        "      [bold green]INFO:[/] the group applications-configuration does not follow the recommended '_' based namespacing because Infield expects this specific name."
                    )
            else:
                print(
                    f"      [bold yellow]WARNING:[/] the group {filepath_src} has a name [bold]{ext_id}[/] without the recommended '_' based namespacing."
                )
        elif parts[0] != "gp":
            print(
                f"      [bold yellow]WARNING:[/] the group {filepath_src} has a name [bold]{ext_id}[/] without the recommended `gp_` based prefix."
            )
    elif resource_type == "transformations" and not filepath_src.stem.endswith("schedule"):
        # First try to find the sql file next to the yaml file with the same name
        sql_file1 = filepath_src.parent / f"{filepath_src.stem}.sql"
        if not sql_file1.exists():
            # Next try to find the sql file next to the yaml file with the external_id as filename
            sql_file2 = filepath_src.parent / f"{ext_id}.sql"
            if not sql_file2.exists():
                print("      [bold yellow]WARNING:[/] could not find sql file:")
                print(f"                 [bold]{sql_file1.name}[/] or ")
                print(f"                 [bold]{sql_file2.name}[/]")
                print(f"               Expected to find it next to the yaml file at {sql_file1.parent}.")
                return False
        parts = ext_id.split("_")
        if len(parts) < 2:
            print(
                f"      [bold yellow]WARNING:[/] the transformation {filepath_src} has an externalId [bold]{ext_id}[/] without the recommended '_' based namespacing."
            )
        elif parts[0] != "tr":
            print(
                f"      [bold yellow]WARNING:[/] the transformation {filepath_src} has an externalId [bold]{ext_id}[/] without the recommended 'tr_' based prefix."
            )
    elif resource_type == "data_models" and ext_id_type == "space":
        parts = ext_id.split("_")
        if len(parts) < 2:
            print(
                f"      [bold yellow]WARNING:[/] the space {filepath_src} has an externalId [bold]{ext_id}[/] without the recommended '_' based namespacing."
            )
        elif parts[0] != "sp":
            if ext_id == "cognite_app_data" or ext_id == "APM_SourceData" or ext_id == "APM_Config":
                if verbose:
                    print(
                        f"      [bold green]INFO:[/] the space {ext_id} does not follow the recommended '_' based namespacing because Infield expects this specific name."
                    )
            else:
                print(
                    f"      [bold yellow]WARNING:[/] the space {filepath_src} has an externalId [bold]{ext_id}[/] without the recommended 'sp_' based prefix."
                )
    elif resource_type == "extraction_pipelines":
        parts = ext_id.split("_")
        if len(parts) < 2:
            print(
                f"      [bold yellow]WARNING:[/] the extraction pipeline {filepath_src} has an externalId [bold]{ext_id}[/] without the recommended '_' based namespacing."
            )
        elif parts[0] != "ep":
            print(
                f"      [bold yellow]WARNING:[/] the extraction pipeline {filepath_src} has an externalId [bold]{ext_id}[/] without the recommended 'ep_' based prefix."
            )
    elif resource_type == "data_sets" or resource_type == "timeseries" or resource_type == "files":
        if not isinstance(parsed, list):
            parsed = [parsed]
        for ds in parsed:
            ext_id = ds.get("externalId") or ds.get("external_id")
            if ext_id is None:
                print(
                    f"      [bold yellow]WARNING:[/] the {resource_type} {filepath_src} is missing the {ext_id_type} field."
                )
                return False
            parts = ext_id.split("_")
            # We don't want to throw a warning on entities that should not be governed by the tool
            # in production (i.e. fileseries, files, and other "real" data)
            if resource_type == "data_sets" and len(parts) < 2:
                print(
                    f"      [bold yellow]WARNING:[/] the {resource_type} {filepath_src} has an externalId [bold]{ext_id}[/] without the recommended '_' based namespacing."
                )
    return True


def process_config_files(
    source_module_dir: Path,
    selected_modules: list[str],
    build_dir: Path,
    config: dict[str, Any],
    build_env: str = "dev",
    verbose: bool = False,
) -> None:
    configs = split_config(config)
    modules_by_variables = defaultdict(list)
    for module_path, variables in configs.items():
        for variable in variables:
            modules_by_variables[variable].append(module_path)
    number_by_resource_type: dict[str, int] = defaultdict(int)

    for module_dir, filepaths in iterate_modules(source_module_dir):
        if module_dir.name not in selected_modules:
            continue
        if verbose:
            print(f"  [bold green]INFO:[/] Processing module {module_dir.name}")
        local_config = create_local_config(configs, module_dir)
        # Sort to support 1., 2. etc prefixes
        filepaths.sort()
        for filepath in filepaths:
            if verbose:
                print(f"    [bold green]INFO:[/] Processing {filepath.name}")

            if filepath.suffix.lower() not in PROC_TMPL_VARS_SUFFIX:
                # Copy the file as is, not variable replacement
                destination = build_dir / filepath.parent.name / filepath.name
                destination.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(filepath, destination)
                continue

            content = filepath.read_text()
            content = replace_variables(content, local_config, build_env)
            filename = create_file_name(filepath, number_by_resource_type)

            destination = build_dir / filepath.parent.name / filename
            destination.parent.mkdir(parents=True, exist_ok=True)
            if "timeseries_datapoints" in filepath.parent.name and filepath.suffix.lower() == ".csv":
                # Special case for timeseries datapoints, we want to timeshit datapoints
                # if the file is a csv file and we have been instructed to.
                # The replacement is used to ensure that we read exactly the same file on Windows and Linux
                file_content = filepath.read_bytes().replace(b"\r\n", b"\n").decode("utf-8")
                data = pd.read_csv(io.StringIO(file_content), parse_dates=True, index_col=0)
                if "timeshift_" in data.index.name:
                    print(
                        "      [bold green]INFO:[/] Found 'timeshift_' in index name, timeshifting datapoints up to today..."
                    )
                    data.index.name = data.index.name.replace("timeshift_", "")
                    data.index = pd.DatetimeIndex(data.index)
                    periods = datetime.datetime.today() - data.index[-1]
                    data.index = pd.DatetimeIndex.shift(data.index, periods=periods.days, freq="D")
                    destination.write_text(data.to_csv())
                else:
                    destination.write_text(content)
            else:
                destination.write_text(content)

            validate(content, destination, filepath, modules_by_variables)


def build_config(
    build_dir: Path,
    source_dir: Path,
    config_file: Path,
    build: BuildEnvironment,
    clean: bool = False,
    verbose: bool = False,
) -> None:
    is_populated = build_dir.exists() and any(build_dir.iterdir())
    if is_populated and clean:
        shutil.rmtree(build_dir)
        build_dir.mkdir()
        print(f"  [bold green]INFO:[/] Cleaned existing build directory {build_dir!s}.")
    elif is_populated:
        print("  [bold yellow]WARNING:[/] Build directory is not empty. Use --clean to remove existing files.")
    elif build_dir.exists():
        print("  [bold green]INFO:[/] Build directory does already exist and is empty. No need to create it.")
    else:
        build_dir.mkdir(exist_ok=True)

    build.validate_environment()

    selected_modules = get_selected_modules(source_dir, build.deploy, build.name, verbose)

    config = read_yaml_file(config_file)
    warnings = validate_config_yaml(config, config_file)
    if warnings:
        print("  [bold yellow]WARNING:[/] Found the following warnings in config.yaml:")
        for warning in warnings:
            print(f"    {warning}")
    process_config_files(source_dir, selected_modules, build_dir, config, build.name, verbose)
    build.dump_to_file(build_dir)
    print(f"  [bold green]INFO:[/] Build complete. Files are located in {build_dir!s}/")
    return None


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
        return self.current_value is None

    @property
    def is_removed(self) -> bool:
        return self.default_value is None

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
class ConfigYAML(UserDict[tuple[str, ...], ConfigEntry]):
    """This represents the 'config.yaml' file in the root of the project.

    The motivation for having a specialist data structure and not just a dictionary:

    1. We want to be able to dump the config.yaml file with comments.
    2. We want to track which variables are added, removed, or unchanged.
    """

    def __init__(self, entries: dict[tuple[str, ...], ConfigEntry] | None = None):
        super().__init__(entries or [])

    @classmethod
    def load(cls, cognite_modules: Path, existing_config_yaml: str | None = None) -> ConfigYAML:
        """Loads the config.yaml file 'cognite_modules' directory and optionally and existing config.yaml file.

        Args:
            cognite_modules: The directory with all the cognite modules
            existing_config_yaml: The existing config.yaml file to compare against.

        Returns:
            A ConfigYAML object with all the entries in the config.yaml file.
        """
        directories = [cognite_modules]
        if files := [dir_ for dir_ in directories if dir_.is_file()]:
            raise ValueError(f"Expected directories, found files: {files}")

        entries: dict[tuple[str, ...], ConfigEntry] = {}
        if isinstance(existing_config_yaml, str):
            raw_file = existing_config_yaml
            comments = cls._extract_comments(raw_file)
            config = yaml.safe_load(raw_file)
            for key_path, value in flatten_dict(config).items():
                entries[key_path] = ConfigEntry(
                    key_path=key_path,
                    current_value=value,
                    current_comment=comments.get(key_path),
                )

        for dir_ in directories:
            defaults = sorted(dir_.glob(f"**/{DEFAULT_CONFIG_FILE}"), key=lambda f: f.relative_to(dir_))
            for default_config in defaults:
                parts = default_config.parent.relative_to(dir_).parts
                raw_file = default_config.read_text()
                file_comments = cls._extract_comments(raw_file, key_prefix=tuple(parts))
                file_data = yaml.safe_load(raw_file)
                for key, value in file_data.items():
                    key_path = (*parts, key)
                    if key_path in entries:
                        entries[key_path].default_value = value
                        entries[key_path].default_comment = file_comments.get(key_path)
                    else:
                        entries[key_path] = ConfigEntry(
                            key_path=key_path,
                            default_value=value,
                            default_comment=file_comments.get(key_path),
                        )

        return cls(entries)

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
        return config

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
            lines.append(f"Untracked {len(removed)} variables in config.yaml.")
        if added := self.added:
            lines.append(f"Added {len(added)} variables to config.yaml.")
        if total_variables == len(self.unchanged):
            lines.append("No variables in config.yaml were changed.")
        return "\n".join(lines)

    @staticmethod
    def _extract_comments(raw_file: str, key_prefix: tuple[str, ...] = tuple()) -> dict[tuple[str, ...], YAMLComment]:
        """Extract comments from a raw file and return a dictionary with the comments."""
        comments: dict[tuple[str, ...], YAMLComment] = defaultdict(YAMLComment)
        position: Literal["above", "after"]
        variable: str | None = None
        last_comment: str | None = None
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

                if last_comment:
                    comments[(*key_prefix, *parent_variables, variable)].above.append(last_comment)
                    last_comment = None

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
                if position == "after" or variable is None:
                    key = (*key_prefix, *parent_variables, *((variable and [variable]) or []))
                    if position == "after":
                        comments[key].after.append(comment.strip())
                    else:
                        comments[key].above.append(comment.strip())
                else:
                    last_comment = comment.strip()

        return dict(comments)

    @classmethod
    def _reorder_config_yaml(cls, config: dict[str, Any]) -> dict[str, Any]:
        """Reorder the config.yaml file to have the keys in alphabetical order
        and the variables before the modules.
        """
        new_config = {}
        for key in sorted([k for k in config.keys() if not isinstance(config[k], dict)]):
            new_config[key] = config[key]
        for key in sorted([k for k in config.keys() if isinstance(config[k], dict)]):
            new_config[key] = cls._reorder_config_yaml(config[key])
        return new_config


def flatten_dict(dct: dict[str, Any]) -> dict[tuple[str, ...], Any]:
    """Flatten a dictionary to a list of tuples with the key path and value."""
    items: dict[tuple[str, ...], Any] = {}
    for key, value in dct.items():
        if isinstance(value, dict):
            for sub_key, sub_value in flatten_dict(value).items():
                items[(key, *sub_key)] = sub_value
        else:
            items[(key,)] = value
    return items


def iterate_modules(root_dir: Path) -> Iterator[tuple[Path, list[Path]]]:
    for module_dir in root_dir.rglob("*"):
        if not module_dir.is_dir():
            continue
        module_directories = [path for path in module_dir.iterdir() if path.is_dir()]
        is_any_resource_directories = any(dir.name in LOADER_BY_FOLDER_NAME for dir in module_directories)
        if module_directories and is_any_resource_directories:
            yield module_dir, [
                path
                for path in module_dir.rglob("*")
                if path.is_file() and path.name not in EXCL_FILES and path.parent != module_dir
            ]


def create_local_config(config: dict[str, Any], module_dir: Path) -> Mapping[str, str]:
    maps = []
    parts = module_dir.parts
    if parts[0] != COGNITE_MODULES and COGNITE_MODULES in parts:
        parts = parts[parts.index(COGNITE_MODULES) :]
    if parts[0] != CUSTOM_MODULES and CUSTOM_MODULES in parts:
        parts = parts[parts.index(CUSTOM_MODULES) :]
    for no in range(len(parts), -1, -1):
        if c := config.get(".".join(parts[:no])):
            maps.append(c)
    return ChainMap(*maps)


def split_config(config: dict[str, Any]) -> dict[str, dict[str, str]]:
    configs: dict[str, dict[str, str]] = {}
    _split_config(config, configs, prefix="")
    return configs


def _split_config(config: dict[str, Any], configs: dict[str, dict[str, str]], prefix: str = "") -> None:
    for key, value in config.items():
        if isinstance(value, dict):
            if prefix and not prefix.endswith("."):
                prefix = f"{prefix}."
            _split_config(value, configs, prefix=f"{prefix}{key}")
        else:
            configs.setdefault(prefix.removesuffix("."), {})[key] = value


def create_file_name(filepath: Path, number_by_resource_type: dict[str, int]) -> str:
    filename = filepath.name
    if filepath.suffix in EXCL_INDEX_SUFFIX:
        return filename
    # Get rid of the local index
    filename = re.sub("^[0-9]+\\.", "", filename)
    number_by_resource_type[filepath.parent.name] += 1
    filename = f"{number_by_resource_type[filepath.parent.name]}.{filename}"
    return filename


def replace_variables(content: str, local_config: Mapping[str, str], build_env: str) -> str:
    for name, variable in local_config.items():
        if "." in name:
            # If the key has a dot, it is a build_env specific variable.
            # Skip if it's the wrong environment.
            env, name = name.split(".", 1)
            if env != build_env:
                continue
        content = content.replace(f"{{{{{name}}}}}", str(variable))
    return content


def validate(content: str, destination: Path, source_path: Path, modules_by_variable: dict[str, list[str]]) -> None:
    this_module = ".".join(source_path.parts[1:-2])

    for unmatched in re.findall(pattern=r"\{\{.*?\}\}", string=content):
        print(f"  [bold yellow]WARNING:[/] Unresolved template variable {unmatched} in {destination!s}")
        variable = unmatched[2:-2]
        if modules := modules_by_variable.get(variable):
            module_str = f"{modules[0]!r}" if len(modules) == 1 else (", ".join(modules[:-1]) + f" or {modules[-1]}")
            print(
                f"    [bold green]Hint:[/] The variables in 'config.yaml' are defined in a tree structure, i.e., "
                "variables defined at a higher level can be used in lower levels."
                f"\n    The variable {variable!r} is defined in the following module{'s' if len(modules) > 1 else ''}: {module_str} "
                f"\n    need{'' if len(modules) > 1 else 's'} to be moved up in the config structure to be used "
                f"in {this_module!r}."
            )

    if destination.suffix in {".yaml", ".yml"}:
        try:
            parsed = yaml.safe_load(content)
        except yaml.YAMLError as e:
            print(
                f"  [bold red]ERROR:[/] YAML validation error for {destination.name} after substituting config variables: \n{e}"
            )
            exit(1)

        if isinstance(parsed, dict):
            parsed = [parsed]
        for item in parsed:
            if not check_yaml_semantics(
                parsed=item,
                filepath_src=source_path,
                filepath_build=destination,
            ):
                exit(1)
        loaders = LOADER_BY_FOLDER_NAME.get(destination.parent.name, [])
        loader: type[Loader] | None = None
        if len(loaders) == 1:
            loader = loaders[0]
        else:
            loader = next((loader for loader in loaders if re.match(loader.filename_pattern, destination.stem)), None)

        if loader is None:
            print(
                f"  [bold yellow]WARNING:[/] In module {source_path.parent.parent.name!r}, the resource {destination.parent.name!r} is not supported by the toolkit."
            )
            print(f"    Available resources are: {', '.join(LOADER_BY_FOLDER_NAME.keys())}")
            return

        if isinstance(loader, ResourceLoader):
            load_warnings = validate_case_raw(
                parsed, loader.resource_cls, destination, identifier_key=loader.identifier_key
            )
            if load_warnings:
                print(f"  [bold yellow]WARNING:[/] Found potential snake_case issues: {load_warnings!s}")

            data_set_warnings = validate_data_set_is_set(parsed, loader.resource_cls, source_path)
            if data_set_warnings:
                print(f"  [bold yellow]WARNING:[/] Found missing data_sets: {data_set_warnings!s}")


if __name__ == "__main__":
    target_dir = Path(__file__).resolve().parent.parent
    config_str = ConfigYAML.load(target_dir, existing_config_yaml=(target_dir / CONFIG_FILE).read_text())
    (target_dir / CONFIG_FILE).write_text(config_str.dump_yaml_with_comments())
    print(str(config_str))
