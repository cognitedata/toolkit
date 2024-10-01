import difflib
from abc import abstractmethod, ABC
from collections.abc import Iterable
from pathlib import Path
from typing import ClassVar

from cognite_toolkit._cdf_tk.constants import INDEX_PATTERN, YAML_SUFFIX
from cognite_toolkit._cdf_tk.data_classes import (
    ModuleLocation,
    BuildDestinationFile, BuildSourceFile, BuiltResourceList,
)
from cognite_toolkit._cdf_tk.exceptions import (
    AmbiguousResourceFileError,
)
from cognite_toolkit._cdf_tk.loaders import (
    LOADER_BY_FOLDER_NAME,
    GroupLoader,
    Loader,
    RawTableLoader, ResourceLoader,
)
from cognite_toolkit._cdf_tk.tk_warnings import (
    ToolkitNotSupportedWarning, WarningList, ToolkitWarning,
)
from cognite_toolkit._cdf_tk.tk_warnings.fileread import (
    UnknownResourceTypeWarning,
)
from cognite_toolkit._cdf_tk.utils import (
    humanize_collection,
)


class Builder(ABC):
    _resource_folder: ClassVar[str | None] = None

    def __init__(
        self,
        build_dir: Path,
        resource_folder: str | None = None,
    ):
        self.build_dir = build_dir
        self.resource_counter = 0
        self.index_by_filepath_stem: dict[Path, int] = {}
        if self._resource_folder is not None:
            self.resource_folder = self._resource_folder
        elif resource_folder is not None:
            self.resource_folder = resource_folder
        else:
            raise ValueError("Either _resource_folder or resource_folder must be set.")

    @abstractmethod
    def build(self, source_files: list[BuildSourceFile], module: ModuleLocation) -> Iterable[BuildDestinationFile]:
        raise NotImplementedError()

    def validate_folder(self, built_resources: BuiltResourceList, module: ModuleLocation) -> WarningList[ToolkitWarning]:
        """This can be overridden to add additional validation for the built resources."""
        return WarningList[ToolkitWarning]()

    # Helper methods
    def _create_destination_path(self, source_path: Path, module_dir: Path) -> Path:
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
            self.resource_counter += 1
            index = self.resource_counter
            self.index_by_filepath_stem[relative_stem] = index

        filename = f"{index}.{filename}"
        destination_path = self.build_dir / self.resource_folder / filename
        destination_path.parent.mkdir(parents=True, exist_ok=True)
        return destination_path


    def _get_loader(self, resource_folder: str, destination: Path, source_path: Path) -> type[ResourceLoader] | None:
        folder_loaders = LOADER_BY_FOLDER_NAME.get(resource_folder, [])
        if not folder_loaders:
            self.warn(
                ToolkitNotSupportedWarning(
                    f"resource of type {resource_folder!r} in {source_path.name}.",
                    details=f"Available resources are: {', '.join(LOADER_BY_FOLDER_NAME.keys())}",
                )
            )
            return None

        loaders = [loader for loader in folder_loaders if loader.is_supported_file(destination)]
        if len(loaders) == 0:
            suggestion: str | None = None
            if "." in source_path.stem:
                core, kind = source_path.stem.rsplit(".", 1)
                match = difflib.get_close_matches(kind, [loader.kind for loader in folder_loaders])
                if match:
                    suggested_name = f"{core}.{match[0]}{source_path.suffix}"
                    suggestion = f"Did you mean to call the file {suggested_name!r}?"
            else:
                kinds = [loader.kind for loader in folder_loaders]
                if len(kinds) == 1:
                    suggestion = f"Did you mean to call the file '{source_path.stem}.{kinds[0]}{source_path.suffix}'?"
                else:
                    suggestion = (
                        f"All files in the {resource_folder!r} folder must have a file extension that matches "
                        f"the resource type. Supported types are: {humanize_collection(kinds)}."
                    )
            self.warn(UnknownResourceTypeWarning(source_path, suggestion))
            return None
        elif len(loaders) > 1 and all(loader.folder_name == "raw" for loader in loaders):
            # Multiple raw loaders load from the same file.
            return RawTableLoader
        elif len(loaders) > 1 and all(issubclass(loader, GroupLoader) for loader in loaders):
            # There are two group loaders, one for resource scoped and one for all scoped.
            return GroupLoader
        elif len(loaders) > 1:
            names = " or ".join(f"{destination.stem}.{loader.kind}{destination.suffix}" for loader in loaders)
            raise AmbiguousResourceFileError(
                f"Ambiguous resource file {destination.name} in {destination.parent.name} folder. "
                f"Unclear whether it is {' or '.join(loader.kind for loader in loaders)}."
                f"\nPlease name the file {names}."
            )

        return loaders[0]


class DefaultBuilder(Builder):
    def build(self, source_files: list[BuildSourceFile], module: ModuleLocation) -> Iterable[BuildDestinationFile]:
        for source_file in source_files:
            if source_file.source.path.suffix.lower() not in YAML_SUFFIX:
                continue
            destination_path = self._create_destination_path(source_file.source.path, module.dir)
            loader = self._get_loader(self.resource_folder, destination_path, source_file.source.path)
            if loader is None:
                continue

            destination = BuildDestinationFile(
                path=destination_path,
                loaded=source_file.loaded,
                loader=loader,
                source=source_file.source,
                extra_sources=None,
            )
            yield destination
