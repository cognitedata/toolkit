from collections.abc import Callable, Iterable, Sequence
from graphlib import TopologicalSorter
from typing import Any

from cognite_toolkit._cdf_tk.builders._base import Builder
from cognite_toolkit._cdf_tk.data_classes._build_files import BuildDestinationFile, BuildSourceFile
from cognite_toolkit._cdf_tk.data_classes._module_directories import ModuleLocation
from cognite_toolkit._cdf_tk.loaders._resource_loaders.location_loaders import LocationFilterLoader
from cognite_toolkit._cdf_tk.tk_warnings.base import ToolkitWarning, WarningList
from cognite_toolkit._cdf_tk.tk_warnings.fileread import FileReadWarning


class LocationBuilder(Builder):
    _resource_folder = LocationFilterLoader.folder_name

    def build(
        self, source_files: list[BuildSourceFile], module: ModuleLocation, console: Callable[[str], None] | None = None
    ) -> Iterable[BuildDestinationFile | Sequence[ToolkitWarning]]:
        location_by_external_id: dict[str, dict[str, Any]] = {}
        location_hierarchy_graph: dict[str, list[Any]] = {}

        # combining all location filters in one file to ensure correct sequence and
        # dependencies within the module
        destination_path = self.build_dir / self.resource_folder / "ordered.LocationFilter.yaml"
        destination_path.parent.mkdir(parents=True, exist_ok=True)

        loaded_locations = []
        for source_file in source_files:
            if isinstance(source_file.loaded, list):
                loaded_locations.extend(source_file.loaded)
            elif isinstance(source_file.loaded, dict):
                loaded_locations.append(source_file.loaded)
            else:
                continue

            loader, warning = self._get_loader(source_file.source.path)
            if loader is None:
                if warning is not None:
                    yield [warning]
                continue

        for loaded_location in loaded_locations:
            ext_id = loaded_location.get("externalId")
            parent_id = loaded_location.get("parentExternalId")

            if ext_id:
                location_by_external_id[ext_id] = loaded_location
                location_hierarchy_graph.setdefault(ext_id, [])  # Initialize if not present
                if parent_id:
                    location_hierarchy_graph.setdefault(parent_id, [])  # Initialize parent too
                    location_hierarchy_graph[ext_id].append(parent_id)

        warnings = WarningList[FileReadWarning]()

        ordered_locations = []
        for external_id in TopologicalSorter(location_hierarchy_graph).static_order():
            target_dict = location_by_external_id[external_id]
            ordered_locations.append(target_dict)

        if loader:
            yield BuildDestinationFile(
                path=destination_path,
                loaded=ordered_locations,
                loader=loader,
                source=source_file.source,
                extra_sources=None,
                warnings=warnings,
            )
