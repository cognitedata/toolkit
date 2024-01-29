"""Local utils for the templates module"""
from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from typing import Any

from cognite_toolkit.cdf_tk.load import LOADER_BY_FOLDER_NAME

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
                # Exclude files that are found in subdirs of functions dir (i.e. function code)
                and "functions" not in path.parent.parent.parts
            ]


def iterate_functions(module_dir: Path) -> Iterator[list[Path]]:
    for function_dir in module_dir.glob("**/functions"):
        if not function_dir.is_dir():
            continue
        function_directories = [path for path in function_dir.iterdir() if path.is_dir()]
        if function_directories:
            yield function_directories
