from collections.abc import Iterator
from pathlib import Path
from typing import Literal, overload

from cognite_toolkit._cdf_tk.constants import (
    BUILTIN_MODULES,
    ROOT_MODULES,
)


def iterate_modules(root_dir: Path) -> Iterator[tuple[Path, list[Path]]]:
    """Iterate over all modules in the project and yield the module directory and all files in the module.

    Args:
        root_dir (Path): The root directory of the project

    Yields:
        Iterator[tuple[Path, list[Path]]]: A tuple containing the module directory and a list of all files in the module

    """
    if root_dir.name in ROOT_MODULES:
        yield from _iterate_modules(root_dir)
        return
    elif root_dir.name == BUILTIN_MODULES:
        yield from _iterate_modules(root_dir)
        return
    for root_module in ROOT_MODULES:
        module_dir = root_dir / root_module
        if module_dir.exists():
            yield from _iterate_modules(module_dir)


def _iterate_modules(root_dir: Path) -> Iterator[tuple[Path, list[Path]]]:
    # local import to avoid circular import
    from cognite_toolkit._cdf_tk.constants import EXCL_FILES
    from cognite_toolkit._cdf_tk.loaders import LOADER_BY_FOLDER_NAME

    if not root_dir.exists():
        return
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
        yield from _iterate_modules(module_dir)


@overload
def module_from_path(path: Path, return_resource_folder: Literal[True]) -> tuple[str, str]: ...


@overload
def module_from_path(path: Path, return_resource_folder: Literal[False] = False) -> str: ...


def module_from_path(path: Path, return_resource_folder: bool = False) -> str | tuple[str, str]:
    """Get the module name from a path"""
    # local import to avoid circular import
    from cognite_toolkit._cdf_tk.loaders import LOADER_BY_FOLDER_NAME

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
    # local import to avoid circular import
    from cognite_toolkit._cdf_tk.loaders import LOADER_BY_FOLDER_NAME

    for part in path.parts:
        if part in LOADER_BY_FOLDER_NAME:
            return part
    raise ValueError("Path does not contain a resource folder")


def find_directory_with_subdirectories(
    directory_name: str | None, root_directory: Path
) -> tuple[Path | None, list[str]]:
    """Search for a directory with a specific name in the root_directory
    and return the directory and all subdirectories."""
    if directory_name is None:
        return None, []
    search = [root_directory]
    while search:
        current = search.pop()
        for root in current.iterdir():
            if not root.is_dir():
                continue
            if root.name == directory_name:
                return root, [d.name for d in root.iterdir() if d.is_dir()]
            search.append(root)
    return None, []
