from collections import defaultdict
from collections.abc import Hashable, Sequence
from pathlib import Path
from typing import ClassVar

from cognite_toolkit._cdf_tk.constants import INDEX_PATTERN
from cognite_toolkit._cdf_tk.data_classes import BuiltResourceList
from cognite_toolkit._cdf_tk.loaders import ResourceLoader
from cognite_toolkit._cdf_tk.tk_warnings import ToolkitWarning, WarningList


class Builder:
    _resource_folder: ClassVar[str]

    def __init__(self, silent: bool, resource_folder: str):
        self.silent = silent
        self.warning_list = WarningList[ToolkitWarning]()

        self.index = 0
        self.index_by_filepath_stem: dict[Path, int] = {}
        self.ids_by_resource_type: dict[type[ResourceLoader], dict[Hashable, Path]] = defaultdict(dict)
        self.dependencies_by_required: dict[tuple[type[ResourceLoader], Hashable], list[tuple[Hashable, Path]]] = (
            defaultdict(list)
        )
        self.__resource_folder = resource_folder

    def warn(self, warning: ToolkitWarning) -> None:
        self.warning_list.append(warning)
        if not self.silent:
            warning.print_warning()

    def console(self, message: str, prefix: str = "[bold green]INFO:[/] ") -> None:
        if not self.silent:
            print(f"{prefix}{message}")

    @property
    def resource_folder(self) -> str:
        if hasattr(self, "_resource_folder"):
            return self._resource_folder
        return self.__resource_folder

    def build_resource_folder(self, resource_files: Sequence[Path]) -> BuiltResourceList[Hashable]:
        raise NotImplementedError

    def create_destination_path(
        self, source_path: Path, resource_folder_name: str, module_dir: Path, build_dir: Path
    ) -> Path:
        """Creates the filepath in the build directory for the given source path.

        Note that this is a complex operation as the modules in the source are nested while the build directory is flat.
        This means that we lose information and risk having duplicate filenames. To avoid this, we prefix the filename
        with a number to ensure uniqueness.
        """
        filename = source_path.name
        # Get rid of the local index
        filename = INDEX_PATTERN.sub("", filename)

        relative_stem = module_dir.name / source_path.relative_to(module_dir).parent / source_path.stem
        if relative_stem in self.index_by_filepath_stem:
            # Ensure extra files (.sql, .pdf) with the same stem gets the same index as the
            # main YAML file. The Transformation Loader expects this.
            index = self.index_by_filepath_stem[relative_stem]
        else:
            # Increment to ensure we do not get duplicate filenames when we flatten the file
            # structure from the module to the build directory.
            self.index += 1
            index = self.index
            self.index_by_filepath_stem[relative_stem] = index

        filename = f"{index}.{filename}"
        destination_path = build_dir / resource_folder_name / filename
        destination_path.parent.mkdir(parents=True, exist_ok=True)
        return destination_path
