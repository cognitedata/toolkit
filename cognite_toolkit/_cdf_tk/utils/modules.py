from pathlib import Path


def resource_folder_from_path(path: Path) -> str:
    """Get the resource_folder from a path"""
    # local import to avoid circular import
    from cognite_toolkit._cdf_tk.resource_ios import CRUDS_BY_FOLDER_NAME

    for part in path.parts:
        if part in CRUDS_BY_FOLDER_NAME:
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
