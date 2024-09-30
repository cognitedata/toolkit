from ._base import Builder


def create_builder(
    resource_folder: str, module_names_by_variable_key: dict[str, list[str]], silent: bool, verbose: bool
) -> Builder:
    return Builder(module_names_by_variable_key, silent, resource_folder, verbose)


__all__ = ["Builder"]
