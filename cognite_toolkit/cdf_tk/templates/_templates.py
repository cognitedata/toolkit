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
from cognite.client._api.functions import validate_function_folder
from cognite.client.data_classes.functions import FunctionList
from rich import print

from cognite_toolkit.cdf_tk.load import LOADER_BY_FOLDER_NAME, FunctionLoader, Loader, ResourceLoader
from cognite_toolkit.cdf_tk.utils import validate_case_raw, validate_data_set_is_set, validate_modules_variables

from ._constants import COGNITE_MODULES, CUSTOM_MODULES, EXCL_INDEX_SUFFIX, PROC_TMPL_VARS_SUFFIX
from ._utils import iterate_functions, iterate_modules, module_from_path, resource_folder_from_path
from .data_classes import BuildConfigYAML, SystemYAML


def build_config(
    build_dir: Path,
    source_dir: Path,
    config: BuildConfigYAML,
    system_config: SystemYAML,
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
    system_config.validate_modules(available_modules, config.environment.selected_modules_and_packages)

    selected_modules = config.get_selected_modules(system_config.packages, available_modules, verbose)

    warnings = validate_modules_variables(config.modules, config.filepath)
    if warnings:
        print(f"  [bold yellow]WARNING:[/] Found the following warnings in config.{config.environment.name}.yaml:")
        for warning in warnings:
            print(f"    {warning}")

    process_config_files(source_dir, selected_modules, build_dir, config, verbose)

    build_environment = config.create_build_environment()
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
    elif resource_type in ["functions"] and "schedule" in filepath_src.stem:
        if isinstance(parsed, list):
            ext_id = ""
            ext_id_type = "multiple"
        elif isinstance(parsed, dict):
            ext_id = parsed.get("functionExternalId") or parsed.get("function_external_id")
            ext_id_type = "functionExternalId"
    elif resource_type in ["functions"]:
        if isinstance(parsed, list):
            ext_id = ""
            ext_id_type = "multiple"
        elif isinstance(parsed, dict):
            ext_id = parsed.get("externalId") or parsed.get("external_id")
            ext_id_type = "externalId"
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


def copy_common_code(
    function_source_dir: Path,
    build_dest_dir: Path,
    common_code_dir: Path,
) -> None:
    # Copies in code from the common folder if it exists
    if not common_code_dir.exists():
        return

    common_destination = Path(build_dest_dir / "common")
    print(
        f"        [bold green]INFO:[/] Copying common function code from " f"{common_code_dir} to {common_destination}"
    )

    if Path(function_source_dir / "common").is_symlink():
        # If we had a symlink in the source dir, we can safely delete the copied in common dir
        shutil.rmtree(common_destination)

    try:
        shutil.copytree(
            common_code_dir,
            common_destination,
        )
    except FileExistsError:
        # The common dir was NOT a symlink, it was actual code, so we risk overwriting code.
        print(
            f"        [bold yellow]ERROR:[/] 'common' dir already exists in function code dir:"
            f" {dir}. This is not supported when common_function_code is set in your config.<env>.yaml file."
        )
        exit(1)

    if Path(common_destination / "requirements.txt").exists():
        reqs = (
            Path(build_dest_dir / "requirements.txt").read_text()
            if Path(build_dest_dir / "requirements.txt").exists()
            else ""
        )
        reqs += "\n" + Path(common_destination / "requirements.txt").read_text()
        Path(build_dest_dir / "requirements.txt").write_text(reqs)
        Path(common_destination / "requirements.txt").unlink()


def process_function_directory(
    yaml_source_path: Path,
    yaml_dest_path: Path,
    module_dir: Path,
    build_dir: Path,
    common_code_dir: Path,
) -> None:
    try:
        functions = FunctionList.load(yaml.safe_load(yaml_dest_path.read_text()))
    except KeyError as e:
        print(f"      [bold red]ERROR:[/] Failed to load function file {yaml_source_path}, error in key: {e}")
        exit(1)
    except Exception as e:
        print(f"      [bold red]ERROR:[/] Failed to load function file {yaml_source_path}, error:\n{e}")
        exit(1)
    for func in functions:
        found = False
        for function_subdirs in iterate_functions(module_dir):
            for function_dir in function_subdirs:
                if func.external_id == function_dir.name:
                    found = True
                    print(f"      [bold green]INFO:[/] Found function {func.external_id}")
                    if func.file_id != "<will_be_generated>":
                        print(
                            f"        [bold yellow]WARNING:[/] Function {func.external_id} in {yaml_source_path} has set a file_id. Expects '<will_be_generated>' and this will be ignored."
                        )
                    destination = build_dir / "functions" / f"{func.external_id}"
                    if destination.exists():
                        print(
                            f"        [bold red]ERROR:[/] Function {func.external_id} is duplicated. If this is unexpected, you want want to use '--clean'."
                        )
                        exit(1)
                    shutil.copytree(function_dir, destination)

                    # Copy in common code if it exists
                    if common_code_dir.is_dir():
                        copy_common_code(
                            function_source_dir=function_dir,
                            build_dest_dir=destination,
                            common_code_dir=common_code_dir,
                        )
                    # Run validations on the function using the SDK's validation function
                    try:
                        validate_function_folder(
                            root_path=destination.as_posix(),
                            function_path=func.function_path,
                            skip_folder_validation=False,
                        )
                    except Exception as e:
                        print(
                            f"      [bold red]ERROR:[/] Failed to package function {func.external_id} at {function_dir}, python module is not loadable:\n{e}"
                        )
                        exit(1)
                    # Clean up cache files
                    for subdir in destination.iterdir():
                        if subdir.is_dir():
                            shutil.rmtree(subdir / "__pycache__", ignore_errors=True)
                    shutil.rmtree(destination / "__pycache__", ignore_errors=True)
        if not found:
            print(
                f"        [bold red]ERROR:[/] Function directory not found for externalId {func.external_id} defined in {yaml_source_path}."
            )
            exit(1)


def process_config_files(
    project_config_dir: Path,
    selected_modules: list[str],
    build_dir: Path,
    config: BuildConfigYAML,
    verbose: bool = False,
) -> None:
    environment = config.environment
    configs = split_config(config.modules)
    modules_by_variables = defaultdict(list)
    for module_path, variables in configs.items():
        for variable in variables:
            modules_by_variables[variable].append(module_path)
    number_by_resource_type: dict[str, int] = defaultdict(int)

    for module_dir, filepaths in iterate_modules(project_config_dir):
        if module_dir.name not in selected_modules:
            continue
        if verbose:
            print(f"  [bold green]INFO:[/] Processing module {module_dir.name}")
        local_config = create_local_config(configs, module_dir)

        # Sort to support 1., 2. etc prefixes
        def sort_key(p: Path) -> int:
            if result := re.findall(r"^(\d+)", p.stem):
                return int(result[0])
            else:
                return len(filepaths)

        # The builder of a module can control the order that resources are deployed by prefixing a number
        # The custom key 'sort_key' is to get the sort on integer and not the string.
        filepaths = sorted(filepaths, key=sort_key)

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

            try:
                resource_folder = resource_folder_from_path(filepath)
            except ValueError:
                print(f"      [bold green]INFO:[/] The file {filepath.name} is not a resource file, skipping it...")
                continue

            destination = build_dir / resource_folder / filename
            destination.parent.mkdir(parents=True, exist_ok=True)
            if resource_folder == "timeseries_datapoints" and filepath.suffix.lower() == ".csv":
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

            # If we have a function definition, we want to process the directory.
            if (
                resource_folder == "functions"
                and filepath.suffix.lower() == ".yaml"
                and re.match(FunctionLoader.filename_pattern, filepath.stem)
            ):
                process_function_directory(
                    yaml_source_path=filepath,
                    yaml_dest_path=destination,
                    module_dir=module_dir,
                    build_dir=build_dir,
                    common_code_dir=Path(project_config_dir / environment.common_function_code),
                )


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


def validate(content: str, destination: Path, source_path: Path, modules_by_variable: dict[str, list[str]]) -> None:
    module = module_from_path(source_path)
    resource_folder = resource_folder_from_path(source_path)

    for unmatched in re.findall(pattern=r"\{\{.*?\}\}", string=content):
        print(f"  [bold yellow]WARNING:[/] Unresolved template variable {unmatched} in {destination!s}")
        variable = unmatched[2:-2]
        if modules := modules_by_variable.get(variable):
            module_str = f"{modules[0]!r}" if len(modules) == 1 else (", ".join(modules[:-1]) + f" or {modules[-1]}")
            print(
                f"    [bold green]Hint:[/] The variables in 'config.[ENV].yaml' are defined in a tree structure, i.e., "
                "variables defined at a higher level can be used in lower levels."
                f"\n    The variable {variable!r} is defined in the following module{'s' if len(modules) > 1 else ''}: {module_str} "
                f"\n    need{'' if len(modules) > 1 else 's'} to be moved up in the config structure to be used "
                f"in {module!r}."
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
                print(
                    f"  [bold yellow]WARNING:[/] In module {source_path.parent.parent.name!r}, the resource {destination.parent.name!r} is not semantically correct."
                )
        loaders = LOADER_BY_FOLDER_NAME.get(resource_folder, [])
        loader: type[Loader] | None
        if len(loaders) == 1:
            loader = loaders[0]
        else:
            try:
                loader = next(
                    (loader for loader in loaders if re.match(loader.filename_pattern, destination.stem)), None
                )
            except Exception as e:
                raise NotImplementedError(f"Loader not found for {source_path}\n{e}")

        if loader is None:
            print(
                f"  [bold yellow]WARNING:[/] In module {module!r}, the resource {resource_folder!r} is not supported by the toolkit."
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
