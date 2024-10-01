from pathlib import Path

from ._base import Builder


def create_builder(
    resource_folder: str,
    build_dir: Path,
    module_names_by_variable_key: dict[str, list[str]],
    silent: bool,
    verbose: bool,
) -> Builder:
    return Builder(build_dir, module_names_by_variable_key, silent, resource_folder, verbose)


__all__ = ["Builder"]
