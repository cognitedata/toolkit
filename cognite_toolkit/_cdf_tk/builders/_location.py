from collections.abc import Callable, Iterable, Sequence
from graphlib import CycleError, TopologicalSorter
from pathlib import Path
from typing import Any

from cognite_toolkit._cdf_tk.builders._base import Builder
from cognite_toolkit._cdf_tk.constants import INDEX_PATTERN
from cognite_toolkit._cdf_tk.data_classes._build_files import BuildDestinationFile, BuildSourceFile
from cognite_toolkit._cdf_tk.data_classes._module_directories import ModuleLocation
from cognite_toolkit._cdf_tk.exceptions import ToolkitError
from cognite_toolkit._cdf_tk.loaders._resource_loaders.location_loaders import LocationFilterLoader
from cognite_toolkit._cdf_tk.tk_warnings.base import ToolkitWarning, WarningList
from cognite_toolkit._cdf_tk.tk_warnings.fileread import FileReadWarning


class LocationBuilder(Builder):
    _resource_folder = LocationFilterLoader.folder_name

    def build(
        self, source_files: list[BuildSourceFile], module: ModuleLocation, console: Callable[[str], None] | None = None
    ) -> Iterable[BuildDestinationFile | Sequence[ToolkitWarning]]:
        location_by_external_id: dict[str, tuple[dict[str, Any], BuildSourceFile]] = {}
        location_hierarchy_graph: dict[str, list[Any]] = {}

        # Ordering all location filters in to ensure correct hierarchy dependency
        # within the module. This is required by the Location API.
        # Doing this in three stages:
        # 1. collect all locations across source files,
        # 2. sort them in a topological order,
        # 3. create a new file for each location where the prefix index ensures deployment order
        # ... while also maintaining reference to source file

        for source_file in source_files:
            loader, warning = self._get_loader(source_file.source.path)
            if isinstance(loader, LocationFilterLoader):
                if warning is not None:
                    yield [warning]
                continue

            loaded_locations = (
                source_file.loaded
                if isinstance(source_file.loaded, list)
                else [source_file.loaded]
                if source_file.loaded
                else []
            )
            for loaded_location in loaded_locations:
                ext_id = loaded_location.get("externalId")
                parent_external_id = loaded_location.get("parentExternalId")

                if ext_id:
                    location_by_external_id[ext_id] = (loaded_location, source_file)
                    location_hierarchy_graph.setdefault(ext_id, [])

                    if parent_external_id:
                        location_hierarchy_graph.setdefault(parent_external_id, [])
                        location_hierarchy_graph[ext_id].append(parent_external_id)

            warnings = WarningList[FileReadWarning]()

        ordered_locations = []
        try:
            for external_id in TopologicalSorter(location_hierarchy_graph).static_order():
                (location, _) = location_by_external_id[external_id]
                ordered_locations.append(location)
        except CycleError:
            raise ToolkitError(
                "Circular dependency found in Locations. Locations must be hierarchical. Please check the externalId and parentExternalId fields."
            )

        for i, item in enumerate(ordered_locations):
            external_id = item["externalId"]
            (location, build_source_file) = location_by_external_id[external_id]
            destination_path = self._create_file_path(build_source_file.source.path, i, loader.kind)  # type: ignore[union-attr]

            yield BuildDestinationFile(
                path=destination_path,
                loaded=location,
                loader=loader,  # type: ignore[arg-type]
                source=build_source_file.source,
                extra_sources=None,
                warnings=warnings,
            )

    def _create_file_path(self, source_path: Path, index: int, kind: str) -> Path:
        """Helper function to prefix the filename with an index that ensures correct deployment order.
        It also adds the kind as a suffix.

        """
        filestem = source_path.stem
        # Get rid of the local index
        filestem = INDEX_PATTERN.sub("", filestem)

        # Increment to ensure we do not get duplicate filenames when we flatten the file
        # structure from the module to the build directory.
        self.resource_counter = index

        filename = f"{self.resource_counter}.{filestem}"
        if not filename.casefold().endswith(kind.casefold()):
            filename = f"{filename}.{kind}"
        filename = f"{filename}{source_path.suffix}"
        destination_path = self.build_dir / self.resource_folder / filename
        destination_path.parent.mkdir(parents=True, exist_ok=True)
        return destination_path
