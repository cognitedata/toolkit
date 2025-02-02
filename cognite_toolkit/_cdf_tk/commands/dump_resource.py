import sys
from abc import ABC, abstractmethod
from collections.abc import Generator, Hashable, Iterator
from pathlib import Path
from typing import Any

import questionary
from cognite.client import data_modeling as dm
from cognite.client.data_classes._base import (
    CogniteResourceList,
)
from cognite.client.data_classes.data_modeling import DataModelId
from cognite.client.exceptions import CogniteAPIError
from questionary import Choice

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.exceptions import (
    ResourceRetrievalError,
    ToolkitMissingResourceError,
    ToolkitResourceMissingError,
)
from cognite_toolkit._cdf_tk.loaders import ContainerLoader, DataModelLoader, ResourceLoader, SpaceLoader, ViewLoader
from cognite_toolkit._cdf_tk.tk_warnings import FileExistsWarning, MediumSeverityWarning
from cognite_toolkit._cdf_tk.utils import humanize_collection
from cognite_toolkit._cdf_tk.utils.file import safe_rmtree, safe_write, yaml_safe_dump

from ._base import ToolkitCommand

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self


class ResourceFinder(Iterator, ABC):
    def __init__(self, client: ToolkitClient, identifier: Hashable | None = None):
        self.client = client
        self.identifier = identifier

    def _selected(self) -> Hashable:
        return self.identifier or self._interactive_select()

    def __iter__(self) -> Self:
        return self

    @abstractmethod
    def _interactive_select(self) -> Hashable:
        raise NotImplementedError

    @abstractmethod
    def update(self, resources: CogniteResourceList) -> None:
        raise NotImplementedError

    @abstractmethod
    def __next__(self) -> Generator[tuple[list[Hashable], ResourceLoader, None | str], Any, None]:
        raise NotImplementedError


class DataModelFinder(ResourceFinder):
    def __init__(self, client: ToolkitClient, identifier: Hashable | None = None):
        super().__init__(client, identifier)
        self.view_ids: set[dm.ViewId] = set()
        self.container_ids: set[dm.ContainerId] = set()
        self.space_ids: set[str] = set()

    def _interactive_select(self) -> Hashable:
        include_global = False
        spaces = self.client.data_modeling.spaces.list(limit=-1, include_global=include_global)
        selected_space: str = questionary.select(
            "In which space is your data model located?", [space.space for space in spaces]
        ).ask()

        data_models = self.client.data_modeling.data_models.list(
            space=selected_space, all_versions=False, limit=-1, include_global=include_global
        ).as_ids()

        if not data_models:
            raise ToolkitMissingResourceError(f"No data models found in space {selected_space}")

        selected_data_model: DataModelId = questionary.select(
            "Which data model would you like to dump?", [Choice(f"{model!r}", value=model) for model in data_models]
        ).ask()

        data_models = self.client.data_modeling.data_models.list(
            space=selected_space,
            all_versions=True,
            limit=-1,
            include_global=include_global,
        ).as_ids()
        data_model_versions = [
            model.version
            for model in data_models
            if (model.space, model.external_id) == (selected_data_model.space, selected_data_model.external_id)
            and model.version is not None
        ]
        if (
            len(data_model_versions) == 1
            or not questionary.confirm(
                f"Would you like to select a different version than {selected_data_model.version} of the data model",
                default=False,
            ).ask()
        ):
            return selected_data_model

        selected_version = questionary.select("Which version would you like to dump?", data_model_versions).ask()
        return DataModelId(selected_space, selected_data_model.external_id, selected_version)

    def update(self, resources: CogniteResourceList) -> None:
        if isinstance(resources, dm.DataModelList):
            self.view_ids |= {
                view.as_id() if isinstance(view, dm.View) else view for item in resources for view in item.views
            }
        elif isinstance(resources, dm.ViewList):
            self.container_ids |= resources.referenced_containers()
        elif isinstance(resources, dm.SpaceList):
            return
        self.space_ids |= {item.space for item in resources}

    def __next__(self) -> Generator[tuple[list[Hashable], ResourceLoader, None | str], Any, None]:
        yield [self._selected()], DataModelLoader.create_loader(self.client), None
        yield list(self.view_ids), ViewLoader.create_loader(self.client), "views"
        yield list(self.container_ids), ContainerLoader.create_loader(self.client), "containers"
        yield list(self.space_ids), SpaceLoader.create_loader(self.client), "spaces"


class DumpResource(ToolkitCommand):
    def dump_to_yamls(
        self,
        finder: ResourceFinder,
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

        for identifiers, loader, subfolder in finder:
            if not identifiers:
                # No resources to dump
                continue
            try:
                resources = loader.retrieve(identifiers)
            except CogniteAPIError as e:
                raise ResourceRetrievalError(f"Failed to retrieve {humanize_collection(identifiers)}: {e!s}") from e
            if len(resources) == 0:
                raise ToolkitResourceMissingError(
                    f"Resource(s) {humanize_collection(identifiers)} not found", str(identifiers)
                )
            finder.update(resources)
            resource_folder = output_dir / loader.folder_name
            if subfolder:
                resource_folder = resource_folder / subfolder
            resource_folder.mkdir(exist_ok=True, parents=True)
            for resource in resources:
                filepath = resource_folder / f"{loader.as_str(resource)}.{loader.kind}.yaml"
                if filepath.exists():
                    self.warn(FileExistsWarning(filepath, "Skipping... Use --clean to remove existing files."))
                    continue
                dumped = loader.dump_resource(resource)
                safe_write(filepath, yaml_safe_dump(dumped), encoding="utf-8")
                if verbose:
                    self.console(f"Dumped {loader.kind} {loader.as_str(resource)} to {filepath!s}")
