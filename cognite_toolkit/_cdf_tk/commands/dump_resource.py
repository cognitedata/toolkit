import itertools
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
    T_WriteClass,
)
from ._base import ToolkitCommand
from cognite_toolkit._cdf_tk.loaders._base_loaders import T_ID, T_WritableCogniteResourceList


class DumpResource(ToolkitCommand):
    def execute(self,
                identifier: Hashable | None,
                loader: ResourceLoader,
                dependency_loaders: list[ResourceLoader] | None,
                output_dir: Path,
                clean: bool,
                verbose: bool,
                ) -> None:
        is_populated = output_dir.exists() and any(output_dir.iterdir())
        if is_populated and clean:
            safe_rmtree(output_dir)
            output_dir.mkdir()
            print(f"  [bold green]INFO:[/] Cleaned existing output directory {output_dir!s}.")
        elif is_populated:
            self.warn(MediumSeverityWarning("Output directory is not empty. Use --clean to remove existing files."))
        elif not output_dir.exists():
            output_dir.mkdir(exist_ok=True)

        selected_identifier = identifier or self._interactive_select_identifier(loader)
        try:
            resources = loader.retrieve([selected_identifier])
        except CogniteAPIError as e:
            raise ResourceRetrievalError(f"Failed to retrieve {identifier}: {e!s}") from e

        if len(resources) == 0:
            raise ToolkitResourceMissingError(f"Resource {selected_identifier} not found", str(selected_identifier))
        elif len(resources) > 1:
            raise ResourceRetrievalError(f"Expected 1 resource, got {len(resources)}: {loader.get_ids(resources)}")
        resource = loader.dump_resource(resources[0], {})

        resource_folder = output_dir / loader.folder_name
        resource_folder.mkdir(exist_ok=True)
        filepath = resource_folder / f"{loader.as_str(resource)}.{loader.kind}.yaml"
        safe_write(filepath, yaml_safe_dump(resource), encoding="utf-8")

    def _interactive_select_identifier(self, loader: ResourceLoader) -> Hashable:
        raise NotImplementedError