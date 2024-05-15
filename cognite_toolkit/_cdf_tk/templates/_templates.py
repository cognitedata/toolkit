from __future__ import annotations

import re
import shutil
from collections import ChainMap
from collections.abc import Mapping
from enum import Enum
from pathlib import Path
from typing import Any

import yaml
from cognite.client._api.functions import validate_function_folder
from cognite.client.data_classes.files import FileMetadataList
from cognite.client.data_classes.functions import FunctionList
from rich import print

from cognite_toolkit._cdf_tk.exceptions import (
    ToolkitFileExistsError,
    ToolkitNotADirectoryError,
    ToolkitValidationError,
    ToolkitYAMLFormatError,
)

from ._constants import EXCL_INDEX_SUFFIX, ROOT_MODULES
from ._utils import iterate_functions

WARN_YELLOW = "[bold yellow]WARNING:[/]"


class Resource(Enum):
    AUTH = "auth"
    DATA_SETS = "data_sets"
    DATA_MODELS = "data_models"
    RAW = "raw"
    WORKFLOWS = "workflows"
    TRANSFORMATIONS = "transformations"
    EXTRACTION_PIPELINES = "extraction_pipelines"
    TIMESERIES = "timeseries"
    FILES = "files"
    FUNCTIONS = "functions"
    OTHER = "other"

    @classmethod
    def _missing_(cls, value: object) -> Resource:
        """Returns Resource.OTHER for any other value."""
        assert isinstance(value, str)
        return cls.OTHER


def _extract_ext_id_dm_spaces(parsed: dict | list, filepath_src: Path) -> tuple[str | None, str]:
    if isinstance(parsed, list):
        raise ToolkitYAMLFormatError(f"Multiple spaces in one file {filepath_src} is not supported.")
    elif isinstance(parsed, dict):
        ext_id = parsed.get("space")
    else:
        raise ToolkitYAMLFormatError(f"Space file {filepath_src} has invalid dataformat.")
    return ext_id, "space"


def _extract_ext_id_dm_nodes(parsed: dict | list, filepath_src: Path) -> tuple[str | None, str]:
    if isinstance(parsed, list):
        raise ToolkitYAMLFormatError(f"Nodes YAML must be an object file {filepath_src} is not supported.")
    try:
        ext_ids = {source["source"]["externalId"] for node in parsed["nodes"] for source in node["sources"]}
    except KeyError:
        raise ToolkitYAMLFormatError(f"Node file {filepath_src} has invalid dataformat.")
    if len(ext_ids) != 1:
        raise ToolkitYAMLFormatError(f"All nodes in {filepath_src} must have the same view.")
    return ext_ids.pop(), "view.externalId"


def _extract_ext_id_auth(parsed: dict | list, filepath_src: Path) -> tuple[str | None, str]:
    if isinstance(parsed, list):
        raise ToolkitYAMLFormatError(f"Multiple Groups in one file {filepath_src} is not supported.")
    return parsed.get("name"), "name"


def _extract_ext_id_function_schedules(parsed: dict | list, filepath_src: Path) -> tuple[str | None, str]:
    if isinstance(parsed, list):
        return "", "multiple"
    elif isinstance(parsed, dict):
        ext_id = parsed.get("functionExternalId") or parsed.get("function_external_id")
        return ext_id, "functionExternalId"


def _extract_ext_id_functions(parsed: dict | list, filepath_src: Path) -> tuple[str | None, str]:
    if isinstance(parsed, list):
        return "", "multiple"
    elif isinstance(parsed, dict):
        ext_id = parsed.get("externalId") or parsed.get("external_id")
        return ext_id, "externalId"


def _extract_ext_id_raw(parsed: dict | list, filepath_src: Path) -> tuple[str | None, str]:
    if isinstance(parsed, list):
        return "", "multiple"
    elif isinstance(parsed, dict):
        ext_id, ext_id_type = parsed.get("dbName"), "dbName"
        if "tableName" in parsed:
            ext_id = f"{ext_id}.{parsed.get('tableName')}"
            ext_id_type = "dbName and tableName"
        return ext_id, ext_id_type
    else:
        raise ToolkitYAMLFormatError(f"Raw file {filepath_src} has invalid dataformat.")


def _extract_ext_id_workflows(parsed: dict | list, filepath_src: Path) -> tuple[str | None, str]:
    if isinstance(parsed, dict):
        if "version" in filepath_src.stem.lower():
            ext_id = parsed.get("workflowExternalId")
            ext_id_type = "workflowExternalId"
        else:
            ext_id = parsed.get("externalId") or parsed.get("external_id")
            ext_id_type = "externalId"
        return ext_id, ext_id_type
    else:
        raise ToolkitYAMLFormatError(f"Multiple Workflows in one file ({filepath_src}) is not supported.")


def _extract_ext_id_other(resource: Resource, parsed: dict | list, filepath_src: Path) -> tuple[str | None, str]:
    if isinstance(parsed, list):
        raise ToolkitYAMLFormatError(f"Multiple {resource} in one file {filepath_src} is not supported.")

    ext_id = parsed.get("externalId") or parsed.get("external_id")
    return ext_id, "externalId"


def _get_ext_id_and_type_from_parsed_yaml(
    resource: Resource, parsed: dict | list, filepath_src: Path
) -> tuple[str | None, str]:
    args: tuple[dict | list, Path] = (parsed, filepath_src)
    if resource is Resource.DATA_MODELS and ".space." in filepath_src.name:
        return _extract_ext_id_dm_spaces(*args)

    elif resource is Resource.DATA_MODELS and ".node." in filepath_src.name:
        return _extract_ext_id_dm_nodes(*args)

    elif resource is Resource.AUTH:
        return _extract_ext_id_auth(*args)

    elif resource in (Resource.DATA_SETS, Resource.TIMESERIES, Resource.FILES) and isinstance(parsed, list):
        return "", "multiple"

    elif resource is Resource.FUNCTIONS and "schedule" in filepath_src.stem:
        return _extract_ext_id_function_schedules(*args)

    elif resource is Resource.FUNCTIONS:
        return _extract_ext_id_functions(*args)

    elif resource is Resource.RAW:
        return _extract_ext_id_raw(*args)

    elif resource is Resource.WORKFLOWS:
        return _extract_ext_id_workflows(*args)

    else:
        return _extract_ext_id_other(resource, *args)


def _check_yaml_semantics_auth(ext_id: str, filepath_src: Path, verbose: bool) -> None:
    parts = ext_id.split("_")
    if len(parts) < 2:
        if ext_id == "applications-configuration":
            if verbose:
                print(
                    "      [bold green]INFO:[/] the group applications-configuration does not follow the "
                    "recommended '_' based namespacing because Infield expects this specific name."
                )
        else:
            print(
                f"      {WARN_YELLOW} the group {filepath_src} has a name [bold]{ext_id}[/] without the "
                "recommended '_' based namespacing."
            )
    elif parts[0] != "gp":
        print(
            f"      {WARN_YELLOW} the group {filepath_src} has a name [bold]{ext_id}[/] without the "
            "recommended `gp_` based prefix."
        )


def _check_yaml_semantics_transformations_schedules(ext_id: str, filepath_src: Path, verbose: bool) -> None:
    # First try to find the sql file next to the yaml file with the same name
    sql_file1 = filepath_src.parent / f"{filepath_src.stem}.sql"
    if not sql_file1.exists():
        # Next try to find the sql file next to the yaml file with the external_id as filename
        sql_file2 = filepath_src.parent / f"{ext_id}.sql"
        if not sql_file2.exists():
            print(f"      {WARN_YELLOW} could not find sql file:")
            print(f"                 [bold]{sql_file1.name}[/] or ")
            print(f"                 [bold]{sql_file2.name}[/]")
            print(f"               Expected to find it next to the yaml file at {sql_file1.parent}.")
            raise ToolkitYAMLFormatError
    parts = ext_id.split("_")
    if len(parts) < 2:
        print(
            f"      {WARN_YELLOW} the transformation {filepath_src} has an externalId [bold]{ext_id}[/] without the "
            "recommended '_' based namespacing."
        )
    elif parts[0] != "tr":
        print(
            f"      {WARN_YELLOW} the transformation {filepath_src} has an externalId [bold]{ext_id}[/] without the "
            "recommended 'tr_' based prefix."
        )


def _check_yaml_semantics_dm_spaces(ext_id: str, filepath_src: Path, verbose: bool) -> None:
    parts = ext_id.split("_")
    if len(parts) < 2:
        print(
            f"      {WARN_YELLOW} the space {filepath_src} has an externalId [bold]{ext_id}[/] without the "
            "recommended '_' based namespacing."
        )
    elif parts[0] != "sp":
        if ext_id == "cognite_app_data" or ext_id == "APM_SourceData" or ext_id == "APM_Config":
            if verbose:
                print(
                    f"      [bold green]INFO:[/] the space {ext_id} does not follow the recommended '_' based "
                    "namespacing because Infield expects this specific name."
                )
        else:
            print(
                f"      {WARN_YELLOW} the space {filepath_src} has an externalId [bold]{ext_id}[/] without the "
                "recommended 'sp_' based prefix."
            )


def _check_yaml_semantics_extpipes(ext_id: str, filepath_src: Path, verbose: bool) -> None:
    parts = ext_id.split("_")
    if len(parts) < 2:
        print(
            f"      {WARN_YELLOW} the extraction pipeline {filepath_src} has an externalId [bold]{ext_id}[/] without "
            "the recommended '_' based namespacing."
        )
    elif parts[0] != "ep":
        print(
            f"      {WARN_YELLOW} the extraction pipeline {filepath_src} has an externalId [bold]{ext_id}[/] without "
            "the recommended 'ep_' based prefix."
        )


def _check_yaml_semantics_basic_resources(
    parsed: list | dict, resource: Resource, ext_id_type: str, ext_id: str, filepath_src: Path, verbose: bool
) -> None:
    if not isinstance(parsed, list):
        parsed = [parsed]
    for ds in parsed:
        ext_id = ds.get("externalId") or ds.get("external_id")
        if ext_id is None:
            print(f"      {WARN_YELLOW} the {resource} {filepath_src} is missing the {ext_id_type} field.")
            raise ToolkitYAMLFormatError
        parts = ext_id.split("_")
        # We don't want to throw a warning on entities that should not be governed by the tool
        # in production (i.e. fileseries, files, and other "real" data)
        if resource is Resource.DATA_SETS and len(parts) < 2:
            print(
                f"      {WARN_YELLOW} the {resource} {filepath_src} has an externalId [bold]{ext_id}[/] without "
                "the recommended '_' based namespacing."
            )


def _check_yaml_semantics(
    parsed: list | dict, resource: Resource, filepath_src: Path, ext_id: str, ext_id_type: str, verbose: bool
) -> None:
    args: tuple[str, Path, bool] = ext_id, filepath_src, verbose
    if resource is Resource.AUTH:
        _check_yaml_semantics_auth(*args)

    elif resource is Resource.TRANSFORMATIONS and not filepath_src.stem.endswith("schedule"):
        _check_yaml_semantics_transformations_schedules(*args)

    elif resource is Resource.DATA_MODELS and ext_id_type == "space":
        _check_yaml_semantics_dm_spaces(*args)

    elif resource is Resource.EXTRACTION_PIPELINES:
        _check_yaml_semantics_extpipes(*args)

    elif resource in (Resource.DATA_SETS, Resource.TIMESERIES, Resource.FILES):
        _check_yaml_semantics_basic_resources(parsed, resource, ext_id_type, *args)


def check_yaml_semantics(parsed: dict | list, filepath_src: Path, filepath_build: Path, verbose: bool = False) -> None:
    """Check the yaml file for semantic errors

    Args:
        parsed (dict | list): the loaded yaml file
        filepath_src (Path): the path to the yaml file
        filepath_build: (Path): No description
        verbose: (bool): Turn on verbose mode

    Returns:
        None: File is semantically acceptable if no exceptions are raised.
    """
    if parsed is None or filepath_src is None or filepath_build is None:
        raise ToolkitYAMLFormatError

    resource = Resource(filepath_src.parent.name)
    ext_id, ext_id_type = _get_ext_id_and_type_from_parsed_yaml(resource, parsed, filepath_src)

    if ext_id is None:
        print(f"      {WARN_YELLOW} the {resource} {filepath_src} is missing the {ext_id_type} field(s).")
        raise ToolkitYAMLFormatError

    _check_yaml_semantics(parsed, resource, filepath_src, ext_id, ext_id_type, verbose)


def process_function_directory(
    yaml_source_path: Path,
    yaml_dest_path: Path,
    module_dir: Path,
    build_dir: Path,
    verbose: bool = False,
) -> None:
    try:
        functions: FunctionList = FunctionList.load(yaml.safe_load(yaml_dest_path.read_text()))
    except (KeyError, yaml.YAMLError) as e:
        raise ToolkitYAMLFormatError(f"Failed to load function file {yaml_source_path} due to: {e}")

    for func in functions:
        found = False
        for function_subdirs in iterate_functions(module_dir):
            for function_dir in function_subdirs:
                if (fn_xid := func.external_id) == function_dir.name:
                    found = True
                    if verbose:
                        print(f"      [bold green]INFO:[/] Found function {fn_xid}")
                    if func.file_id != "<will_be_generated>":
                        print(
                            f"        {WARN_YELLOW} Function {fn_xid} in {yaml_source_path} has set a file_id. Expects '<will_be_generated>' and this will be ignored."
                        )
                    destination = build_dir / "functions" / fn_xid
                    if destination.exists():
                        raise ToolkitFileExistsError(
                            f"Function {fn_xid} is duplicated. If this is unexpected, you may want to use '--clean'."
                        )
                    shutil.copytree(function_dir, destination)

                    # Run validations on the function using the SDK's validation function
                    try:
                        if func.function_path:
                            validate_function_folder(
                                root_path=destination.as_posix(),
                                function_path=func.function_path,
                                skip_folder_validation=False,
                            )
                        else:
                            print(
                                f"        {WARN_YELLOW} Function {fn_xid} in {yaml_source_path} has no function_path defined."
                            )
                    except Exception as e:
                        raise ToolkitValidationError(
                            f"Failed to package function {fn_xid} at {function_dir}, python module is not loadable "
                            f"due to: {type(e)}({e}). Note that you need to have any requirements your function uses "
                            "installed in your current, local python environment."
                        ) from e
                    # Clean up cache files
                    for subdir in destination.iterdir():
                        if subdir.is_dir():
                            shutil.rmtree(subdir / "__pycache__", ignore_errors=True)
                    shutil.rmtree(destination / "__pycache__", ignore_errors=True)
        if not found:
            raise ToolkitNotADirectoryError(
                f"Function directory not found for externalId {func.external_id} defined in {yaml_source_path}."
            )


def process_files_directory(
    files: list[Path],
    yaml_dest_path: Path,
    module_dir: Path,
    build_dir: Path,
    verbose: bool = False,
) -> None:
    if len(files) == 0:
        return
    try:
        file_def = FileMetadataList.load(yaml_dest_path.read_text())
    except KeyError as e:
        raise ToolkitValidationError(f"Failed to load file definitions file {yaml_dest_path}, error in key: {e}")
    # We only support one file template definition per module.
    if len(file_def) == 1:
        if file_def[0].name and "$FILENAME" in file_def[0].name and file_def[0].name != "$FILENAME":
            if verbose:
                print(
                    f"      [bold green]INFO:[/] Found file template {file_def[0].name} in {module_dir}, renaming files..."
                )
            for filepath in files:
                if file_def[0].name:
                    destination = (
                        build_dir / filepath.parent.name / re.sub(r"\$FILENAME", filepath.name, file_def[0].name)
                    )
                    destination.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copyfile(filepath, destination)
            return
    for filepath in files:
        destination = build_dir / filepath.parent.name / filepath.name
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(filepath, destination)


def create_local_config(config: dict[str, Any], module_dir: Path) -> Mapping[str, str]:
    maps = []
    parts = module_dir.parts
    for root_module in ROOT_MODULES:
        if parts[0] != root_module and root_module in parts:
            parts = parts[parts.index(root_module) :]
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
        content = re.sub(rf"{{{{\s*{name}\s*}}}}", str(variable), content)
    return content
