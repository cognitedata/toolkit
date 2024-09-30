from pathlib import Path

from ._base import Builder, FileBuilder, FunctionBuilder


def create_builder(
    resource_folder: str,
    build_dir: Path,
    module_names_by_variable_key: dict[str, list[str]],
    silent: bool,
    verbose: bool,
) -> Builder:
    if builder_cls := _BUILDER_BY_RESOURCE_FOLDER.get(resource_folder):
        return builder_cls(build_dir, module_names_by_variable_key, silent, verbose)

    return Builder(build_dir, module_names_by_variable_key, silent, verbose, resource_folder)


_BUILDER_BY_RESOURCE_FOLDER = {_builder._resource_folder: _builder for _builder in Builder.__subclasses__()}
__all__ = ["Builder", "FileBuilder", "FunctionBuilder", "create_builder"]
