from collections import defaultdict
from collections.abc import Callable, Iterable, Sequence
from typing import Any

from cognite_toolkit._cdf_tk.builders import Builder
from cognite_toolkit._cdf_tk.data_classes import (
    BuildDestinationFile,
    BuildSourceFile,
    ModuleLocation,
)
from cognite_toolkit._cdf_tk.loaders import RawDatabaseLoader, RawTableLoader, ResourceLoader
from cognite_toolkit._cdf_tk.tk_warnings import ToolkitWarning


class RawBuilder(Builder):
    _resource_folder = RawDatabaseLoader.folder_name

    def build(
        self, source_files: list[BuildSourceFile], module: ModuleLocation, console: Callable[[str], None] | None = None
    ) -> Iterable[BuildDestinationFile | Sequence[ToolkitWarning]]:
        for source_file in source_files:
            loaded = source_file.loaded
            if loaded is None:
                continue
            loaded_list = loaded if isinstance(loaded, list) else [loaded]
            entry_by_loader: dict[type[ResourceLoader], list[dict[str, Any]]] = defaultdict(list)
            for item in loaded_list:
                try:
                    RawTableLoader.get_id(item)
                except KeyError:
                    entry_by_loader[RawDatabaseLoader].append(item)
                else:
                    entry_by_loader[RawTableLoader].append(item)
            for loader, entries in entry_by_loader.items():
                if not entries:
                    continue
                destination_path = self._create_destination_path(source_file.source.path, module.dir, loader.kind)

                yield BuildDestinationFile(
                    path=destination_path,
                    loaded=entries,
                    loader=loader,
                    source=source_file.source,
                    extra_sources=None,
                )
