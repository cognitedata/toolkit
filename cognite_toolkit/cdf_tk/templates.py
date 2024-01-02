from __future__ import annotations

import itertools
import os
import re
import shutil
from collections import ChainMap, UserList, defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, overload

import yaml
from rich import print

from cognite_toolkit import _version
from cognite_toolkit.cdf_tk.load import LOADER_BY_FOLDER_NAME
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
            raise ValueError(f"Environment {build_env} not found in {ENVIRONMENTS_FILE!s}")
        system = SystemVariables.load(environment_config, action)
        try:
            return BuildEnvironment(
                name=build_env,
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

    def validate_environment(self):
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

    def set_environment_variables(self):
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


def _read_packages(source_module, verbose):
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


def check_yaml_semantics(parsed: Any, filepath_src: Path, filepath_build: Path, verbose: bool = False) -> bool:
    """Check the yaml file for semantic errors

    parsed: the parsed yaml file
    filepath: the path to the yaml file
    yields: True if the yaml file is semantically acceptable, False if build should fail.
    """
    if parsed is None or filepath_src is None or filepath_build is None:
        return False
    resource_type = filepath_src.parts[-2]
    ext_id = None
    if resource_type == "data_models" and ".space." in filepath_src.name:
        ext_id = parsed.get("space")
        ext_id_type = "space"
    elif resource_type == "data_models" and ".node." in filepath_src.name:
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
        ext_id = parsed.get("name")
        ext_id_type = "name"
    elif resource_type in ["data_sets", "timeseries", "files"] and isinstance(parsed, list):
        ext_id = ""
        ext_id_type = "multiple"
    elif resource_type == "raw":
        ext_id = f"{parsed.get('dbName')}.{parsed.get('tableName')}"
        if "None" in ext_id:
            ext_id = None
        ext_id_type = "dbName and/or tableName"
    else:
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
    number_by_resource_type = defaultdict(int)

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
            destination.write_text(content)

            validate(content, destination, filepath)


def build_config(
    build_dir: Path,
    source_dir: Path,
    config_file: Path,
    build: BuildEnvironment,
    clean: bool = False,
    verbose: bool = False,
):
    is_populated = build_dir.exists() and any(build_dir.iterdir())
    if is_populated and clean:
        shutil.rmtree(build_dir)
        build_dir.mkdir()
        print(f"  [bold green]INFO:[/] Cleaned existing build directory {build_dir!s}.")
    elif is_populated:
        print("  [bold yellow]WARNING:[/] Build directory is not empty. Use --clean to remove existing files.")
    else:
        build_dir.mkdir()

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


def generate_config(
    directory: Path | Sequence[Path], include_modules: set[str] | None = None, existing_config: str | None = None
) -> tuple[str, ConfigEntries]:
    """Generate a config dictionary from the default.config.yaml files in the given directories.

    You can specify a set of modules to include in the config. If you do not specify any modules, all modules will be included.

    Args:
        directory: A root directory to search for default.config.yaml files.
        include_modules: A set of modules to include in the config. If None, all modules will be included.
        existing_config: An existing config dictionary to

    Returns:
        A config dictionary.
    """
    if not directory.exists():
        raise ValueError(f"Directory {directory} does not exist")
    entries = ConfigEntries((existing_config and yaml.safe_load(existing_config)) or None)
    if isinstance(directory, Path):
        directories = [directory]
    else:
        directories = directory
    config = {}
    comments: dict[str, dict[Literal["above", "after"], list[str]]] = {}
    for dir_ in directories:
        defaults = sorted(directory.glob(f"**/{DEFAULT_CONFIG_FILE}"), key=lambda f: f.relative_to(dir_))

        for default_config in defaults:
            if include_modules is not None and default_config.parent.name not in include_modules:
                continue
            raw_file = default_config.read_text()

            comments.update(
                _extract_comments(raw_file, key_prefix=tuple(default_config.parent.relative_to(directory).parts))
            )

            file_data = yaml.safe_load(raw_file)
            parts = default_config.relative_to(directory).parent.parts
            if len(parts) == 0:
                # This is a root config file
                config.update(file_data)
                entries.extend(
                    [
                        ConfigEntry(
                            key=key,
                            module="",
                            path="",
                            last_value=None,
                            current_value=value,
                        )
                        for key, value in file_data.items()
                    ]
                )
                continue
            local_config = config
            for key in parts:
                if key not in local_config:
                    local_config[key] = {}
                local_config = local_config[key]

            local_config.update(file_data)

            entries.extend(
                [
                    ConfigEntry(
                        key=key,
                        module=default_config.parent.name,
                        path=".".join(parts[:-1]),
                        last_value=None,
                        current_value=value,
                    )
                    for key, value in file_data.items()
                ]
            )

    config = _reorder_config_yaml(config)
    output_yaml = _dump_yaml_with_comments(config, comments)
    return output_yaml, entries


def _reorder_config_yaml(config: dict[str, Any]) -> dict[str, Any]:
    """Reorder the config.yaml file to have the keys in alphabetical order
    and the variables before the modules.
    """
    new_config = {}
    for key in sorted([k for k in config.keys() if not isinstance(config[k], dict)]):
        new_config[key] = config[key]
    for key in sorted([k for k in config.keys() if isinstance(config[k], dict)]):
        new_config[key] = _reorder_config_yaml(config[key])
    return new_config


def _extract_comments(
    raw_file: str, key_prefix: tuple[str, ...] = tuple()
) -> dict[tuple[str, ...], dict[Literal["above", "after"], list[str]]]:
    """Extract comments from a raw file and return a dictionary with the comments."""
    comments: dict[tuple[str, ...], dict[Literal["above", "after"], list[str]]] = defaultdict(
        lambda: {"above": [], "after": []}
    )
    position: Literal["above", "after"]
    variable: str | None = None
    last_comment: str | None = None
    for line in raw_file.splitlines():
        if ":" in line:
            variable = str(line.split(":", maxsplit=1)[0].strip())
            if last_comment:
                comments[(*key_prefix, variable)]["above"].append(last_comment)
                last_comment = None
        if "#" in line:
            before, comment = str(line).rsplit("#", maxsplit=1)
            position = "after" if ":" in before else "above"
            if position == "after" and (before.count('"') % 2 == 1 or before.count("'") % 2 == 1):
                # The comment is inside a string
                continue
            if position == "after" or variable is None:
                key = (*key_prefix, *((variable and [variable]) or []))
                comments[key][position].append(comment.strip())
            else:
                last_comment = comment.strip()
    return dict(comments)


def _dump_yaml_with_comments(
    config: dict[str, Any],
    comments: dict[tuple[str, ...], dict[Literal["above", "after"], list[str]]],
    indent_size: int = 2,
) -> str:
    """Dump a config dictionary to a yaml string"""
    dumped = yaml.dump(config, sort_keys=False, indent=indent_size)
    out_lines = []
    if module_comment := comments.get(tuple()):
        for comment in module_comment["above"]:
            out_lines.append(f"# {comment}")
    last_indent = 0
    last_variable: str | None = None
    path: tuple[str, ...] = tuple()
    for line in dumped.splitlines():
        indent = len(line) - len(line.lstrip())
        if last_indent < indent:
            path = (*path, last_variable)
        elif last_indent > indent:
            # Adding some extra space between modules
            out_lines.append("")
            indent_reduction_steps = (last_indent - indent) // indent_size
            path = path[:-indent_reduction_steps]

        variable = line.split(":", maxsplit=1)[0].strip()
        if comment := comments.get((*path, variable)):
            for line_comment in comment["above"]:
                out_lines.append(f"{' ' * indent}# {line_comment}")
            if after := comment["after"]:
                line = f"{line} # {after[0]}"

        out_lines.append(line)
        last_indent = indent
        last_variable = variable
    out_lines.append("")
    return "\n".join(out_lines)


@dataclass
class ConfigEntries(UserList):
    def __init__(self, entries: list[ConfigEntry] | dict | None = None):
        if isinstance(entries, dict):
            entries = self._initialize(entries)
        super().__init__(entries or [])
        self._lookup = {}
        for entry in self:
            self._lookup.setdefault(entry.module, {})[entry.key] = entry

    @staticmethod
    def _initialize(entries: dict, path: str = "") -> list[ConfigEntry]:
        results = []
        if "." in path:
            path_to, module = path.rsplit(".", maxsplit=1)
        else:
            module = path
            path_to = ""
        for key, value in entries.items():
            if isinstance(value, dict):
                results.extend(ConfigEntries._initialize(value, f"{path}.{key}" if path else key))
            else:
                results.append(
                    ConfigEntry(
                        key=key,
                        module=module,
                        path=path_to,
                        last_value=value,
                        current_value=None,
                    )
                )
        return results

    def append(self, item: ConfigEntry) -> None:
        if item.module not in self._lookup:
            self._lookup[item.module] = {}
        if item.key not in self._lookup[item.module]:
            self._lookup[item.module][item.key] = item
            super().append(item)
        else:
            self._lookup[item.module][item.key].current_value = item.current_value

    def extend(self, items: list[ConfigEntry]) -> None:
        for item in items:
            self.append(item)

    @property
    def changed(self) -> list[ConfigEntry]:
        return [entry for entry in self if entry.changed]

    @property
    def removed(self) -> list[ConfigEntry]:
        return [entry for entry in self if entry.removed]

    @property
    def added(self) -> list[ConfigEntry]:
        return [entry for entry in self if entry.added]

    @property
    def unchanged(self) -> list[ConfigEntry]:
        return [entry for entry in self if entry.unchanged]

    def __str__(self) -> str:
        total_variables = len(self)
        lines = []
        if removed := self.removed:
            lines.append(f"Removed {len(removed)} variables from config.yaml: {[str(r) for r in removed]}")
        if added := self.added:
            lines.append(f"Added {len(added)} variables to config.yaml: {[str(a) for a in added]}")
        if changed := self.changed:
            lines.append(f"Changed {len(changed)} variables in config.yaml: {[str(c) for c in changed]}")
        if total_variables == len(self.unchanged):
            lines.append("No variables in config.yaml was changed.")
        return "\n".join(lines)


@dataclass
class ConfigEntry:
    key: str
    module: str
    path: str
    last_value: Any | None
    current_value: Any | None

    @property
    def changed(self) -> bool:
        return self.last_value is not None and self.current_value is not None and self.last_value != self.current_value

    @property
    def removed(self) -> bool:
        return self.last_value is not None and self.current_value is None

    @property
    def added(self) -> bool:
        return self.last_value is None and self.current_value is not None

    @property
    def unchanged(self) -> bool:
        return self.last_value is not None and self.current_value is not None and self.last_value == self.current_value

    def __str__(self):
        prefix = self._prefix()
        if self.removed:
            return f"{prefix}{self.key} was removed"
        elif self.added:
            return f"{prefix}{self.key} was added"
        elif self.changed:
            return f"{prefix}{self.key} changed from {self.last_value!r} to {self.current_value!r}"
        else:
            return f"{prefix}{self.key} is unchanged"

    def __repr__(self):
        prefix = self._prefix()
        return f"{prefix}{self.key}={self.current_value!r}"

    def _prefix(self):
        parts = []
        if self.path:
            parts.append(self.path)
        if self.module:
            parts.append(self.module)
        prefix = ""
        if parts:
            prefix = ".".join(parts) + "."
        return prefix


def iterate_modules(root_dir: Path) -> tuple[Path, list[Path]]:
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
    configs = {}
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


def validate(content: str, destination: Path, source_path: Path) -> None:
    for unmatched in re.findall(pattern=r"\{\{.*?\}\}", string=content):
        print(f"  [bold yellow]WARNING:[/] Unresolved template variable {unmatched} in {destination!s}")

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
        loader = LOADER_BY_FOLDER_NAME.get(destination.parent.name, [])
        if len(loader) == 1:
            loader = loader[0]
        else:
            loader = next((loader for loader in loader if re.match(loader.filename_pattern, destination.stem)), None)

        if loader is None:
            print(
                f"  [bold yellow]WARNING:[/] In module {source_path.parent.parent.name!r}, the resource {destination.parent.name!r} is not supported by the toolkit."
            )
            print(f"    Available resources are: {', '.join(LOADER_BY_FOLDER_NAME.keys())}")
            return

        if loader:
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
    config_str, differences = generate_config(target_dir, existing_config=(target_dir / CONFIG_FILE).read_text())
    (target_dir / CONFIG_FILE).write_text(config_str)
    print(str(differences))
