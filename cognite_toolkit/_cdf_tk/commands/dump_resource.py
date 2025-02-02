import itertools
from abc import ABC
from collections.abc import Hashable
from pathlib import Path

import questionary
from cognite.client import data_modeling as dm
from cognite.client.data_classes.capabilities import DataModelsAcl
from cognite.client.data_classes.data_modeling import DataModelId
from cognite.client.exceptions import CogniteAPIError
from questionary import Choice
from rich import print
from rich.panel import Panel

from cognite_toolkit._cdf_tk.exceptions import ResourceRetrievalError, ToolkitResourceMissingError
from cognite_toolkit._cdf_tk.loaders import ResourceLoader
from cognite_toolkit._cdf_tk.tk_warnings import MediumSeverityWarning
from cognite_toolkit._cdf_tk.utils import CDFToolConfig
from cognite_toolkit._cdf_tk.utils.file import safe_rmtree, safe_write, yaml_safe_dump
from cognite.client.data_classes._base import (
    T_CogniteResourceList,
    T_WritableCogniteResource,
    T_WriteClass, CogniteResourceList,
)
from ._base import ToolkitCommand
from cognite_toolkit._cdf_tk.loaders._base_loaders import T_ID, T_WritableCogniteResourceList
from collections.abc import Iterator, Iterable
from dataclasses import dataclass


class ResourceSelector(Iterable, ABC):
    def __init__(self, identifier: Hashable | None = None):
        self.identifier = identifier

    def _interactive_select(self, loader: ResourceLoader) -> Iterator[Hashable]:
        raise NotImplementedError

    def _selected(self) -> Hashable:
        return self.identifier or self.interactive_select()

    def update(self, resources: CogniteResourceList) -> None:
        raise NotImplementedError

    def __iter__(self) -> Iterator[tuple[list[Hashable], ResourceLoader, str | None]]:
        return self

    def __next__(self) -> tuple[list[Hashable], ResourceLoader, str | None]:
        raise NotImplementedError


class DumpResource(ToolkitCommand):
    def dump_to_yamls(
         self,
         selector: ResourceSelector,
         output_dir: Path,
         clean: bool,
         verbose: bool,
    ) -> None:
        is_populated = output_dir.exists() and any(output_dir.iterdir())
        if is_populated and clean:
            safe_rmtree(output_dir)
            output_dir.mkdir()
            self.console(f"Cleaned existing output directory {output_dir!s}.")
        elif is_populated:
            self.warn(MediumSeverityWarning("Output directory is not empty. Use --clean to remove existing files."))
        elif not output_dir.exists():
            output_dir.mkdir(exist_ok=True)

        for identifiers, loader, subfolder in selector:
            try:
                resources = loader.retrieve(identifiers)
            except CogniteAPIError as e:
                raise ResourceRetrievalError(f"Failed to retrieve {identifiers}: {e!s}") from e
            if len(resources) == 0:
                raise ToolkitResourceMissingError(f"Resource {identifiers} not found", str(identifiers))
            selector.update(resources)
            resource_folder = output_dir / loader.folder_name
            if subfolder:
                resource_folder = resource_folder / subfolder
            resource_folder.mkdir(exist_ok=True, parents=True)
            for resource in resources:
                dumped = loader.dump_resource(resource)
                filepath = resource_folder / f"{loader.as_str(resource)}.{loader.kind}.yaml"
                if filepath.exists():
                    # Todo warning and skip instead.
                    raise FileExistsError(f"File {filepath!s} already exists")
                safe_write(filepath, yaml_safe_dump(dumped), encoding="utf-8")
                if verbose:
                    self.console(f"Dumped {loader.kind} {loader.as_str(resource)} to {filepath!s}")
