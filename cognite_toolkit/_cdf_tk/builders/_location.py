from collections.abc import Callable, Iterable, Sequence
from graphlib import TopologicalSorter
from pathlib import Path
from typing import Any

from cognite_toolkit._cdf_tk.builders._base import Builder
from cognite_toolkit._cdf_tk.constants import INDEX_PATTERN
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

        for source_file in source_files:
            if source_file.loaded is None:
                continue

            loader, warning = self._get_loader(source_file.source.path)
            if loader is None:
                if warning is not None:
                    yield [warning]
                continue

            loaded_locations = source_file.loaded if isinstance(source_file.loaded, list) else [source_file.loaded]

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

            for i, external_id in enumerate(TopologicalSorter(location_hierarchy_graph).static_order()):
                destination_path = self._create_file_path(source_file.source.path, i, loader.kind)

                target_dict = location_by_external_id[external_id]

                yield BuildDestinationFile(
                    path=destination_path,
                    loaded=target_dict,
                    loader=loader,
                    source=source_file.source,
                    extra_sources=None,
                    warnings=warnings,
                )

    def _create_file_path(self, source_path: Path, index: int, kind: str) -> Path:
        """Creates the filepath in the build directory for the given source path.

        Note that we are splitting location filter into single files to ensure deployment dependencies.
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
