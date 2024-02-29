"""Local utils for the templates module"""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from typing import Any, Literal, overload

from rich import print

from cognite_toolkit._cdf_tk.load import LOADER_BY_FOLDER_NAME
from cognite_toolkit._cdf_tk.utils import read_yaml_file

from ._constants import EXCL_FILES


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
    """Iterate over all modules in the project and yield the module directory and all files in the module.

    Args:
        root_dir (Path): The root directory of the project

    Yields:
        Iterator[tuple[Path, list[Path]]]: A tuple containing the module directory and a list of all files in the module

    """
    for module_dir in root_dir.iterdir():
        if not module_dir.is_dir():
            continue
        sub_directories = [path for path in module_dir.iterdir() if path.is_dir()]
        is_any_resource_directories = any(dir.name in LOADER_BY_FOLDER_NAME for dir in sub_directories)
        if sub_directories and is_any_resource_directories:
            # Module found
            yield module_dir, [path for path in module_dir.rglob("*") if path.is_file() and path.name not in EXCL_FILES]
            # Stop searching for modules in subdirectories
            continue
        yield from iterate_modules(module_dir)


@overload
def module_from_path(path: Path, return_resource_folder: Literal[True]) -> tuple[str, str]: ...


@overload
def module_from_path(path: Path, return_resource_folder: Literal[False] = False) -> str: ...


def module_from_path(path: Path, return_resource_folder: bool = False) -> str | tuple[str, str]:
    """Get the module name from a path"""
    if len(path.parts) == 1:
        raise ValueError("Path is not a module")
    last_folder = path.parts[1]
    for part in path.parts[1:]:
        if part in LOADER_BY_FOLDER_NAME:
            if return_resource_folder:
                return last_folder, part
            return last_folder
        last_folder = part
    raise ValueError("Path is not part of a module")


def resource_folder_from_path(path: Path) -> str:
    """Get the resource_folder from a path"""
    for part in path.parts:
        if part in LOADER_BY_FOLDER_NAME:
            return part
    raise ValueError("Path does not contain a resource folder")


def iterate_functions(module_dir: Path) -> Iterator[list[Path]]:
    for function_dir in module_dir.glob("**/functions"):
        if not function_dir.is_dir():
            continue
        function_directories = [path for path in function_dir.iterdir() if path.is_dir()]
        if function_directories:
            yield function_directories


def _get_cognite_module_version(project_dir: Path) -> str:
    cognite_modules = project_dir / "cognite_modules"
    if (cognite_modules / "_system.yaml").exists():
        system_yaml = read_yaml_file(cognite_modules / "_system.yaml")
        try:
            previous_version = system_yaml["cdf_toolkit_version"]
        except KeyError:
            previous_version = None
    elif (project_dir / "environments.yaml").exists():
        environments_yaml = read_yaml_file(project_dir / "environments.yaml")
        try:
            previous_version = environments_yaml["__system"]["cdf_toolkit_version"]
        except KeyError:
            previous_version = None
    else:
        previous_version = None
    if previous_version is None:
        print(
            "Failed to load previous version, have you changed the "
            "'_system.yaml' or 'environments.yaml' (before 0.1.0b6) file?"
        )
        exit(1)
    return previous_version
