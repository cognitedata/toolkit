from __future__ import annotations

import datetime
import io
import re
import shutil
from collections import ChainMap, defaultdict
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import pandas as pd
import yaml
from rich import print

from cognite_toolkit.cdf_tk.load import LOADER_BY_FOLDER_NAME, Loader, ResourceLoader
from cognite_toolkit.cdf_tk.utils import validate_case_raw, validate_data_set_is_set, validate_modules_variables

from ._constants import COGNITE_MODULES, CUSTOM_MODULES, EXCL_INDEX_SUFFIX, PROC_TMPL_VARS_SUFFIX
from ._utils import iterate_modules
from .data_classes import EnvironmentConfig, GlobalConfig


def build_config(
    build_dir: Path,
    source_dir: Path,
    config: EnvironmentConfig,
    global_config: GlobalConfig,
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

    config.validate_environment()

    available_modules = {module.name for module, _ in iterate_modules(source_dir)}
    global_config.validate_modules(available_modules)

    selected_modules = config.get_selected_modules(global_config.packages, available_modules, verbose)

    warnings = validate_modules_variables(config.modules, config.filepath)
    if warnings:
        print("  [bold yellow]WARNING:[/] Found the following warnings in config.yaml:")
        for warning in warnings:
            print(f"    {warning}")

    process_config_files(source_dir, selected_modules, build_dir, config.modules, verbose)

    build_environment = config.create_build_environment(global_config.system)
    build_environment.dump_to_file(build_dir)
    print(f"  [bold green]INFO:[/] Build complete. Files are located in {build_dir!s}/")
    return None


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
    project_config_dir: Path,
    selected_modules: list[str],
    build_dir: Path,
    config: dict[str, Any],
    verbose: bool = False,
) -> None:
    configs = split_config(config)
    number_by_resource_type: dict[str, int] = defaultdict(int)

    for module_dir, filepaths in iterate_modules(project_config_dir):
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
            content = replace_variables(content, local_config)
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

            validate(content, destination, filepath)


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


def replace_variables(content: str, local_config: Mapping[str, str]) -> str:
    for name, variable in local_config.items():
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
