from __future__ import annotations

from collections.abc import Hashable
from graphlib import TopologicalSorter

import questionary
from rich import print

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.data_classes import DeployResults, ResourceDeployResult
from cognite_toolkit._cdf_tk.exceptions import ToolkitMissingResourceError, ToolkitValueError
from cognite_toolkit._cdf_tk.loaders import RESOURCE_LOADER_LIST, ResourceLoader, SpaceLoader
from cognite_toolkit._cdf_tk.utils import CDFToolConfig

from ._base import ToolkitCommand


class PurgeCommand(ToolkitCommand):
    def space(
        self,
        ToolGlobals: CDFToolConfig,
        space: str | None = None,
        include_space: bool = False,
        dry_run: bool = False,
        verbose: bool = False,
    ) -> None:
        """Purge a space and all its content"""
        selected_space = self._get_selected_space(space, ToolGlobals.toolkit_client)
        loaders = {
            loader_cls: loader_cls.dependencies
            for loader_cls in RESOURCE_LOADER_LIST
            if SpaceLoader in loader_cls.dependencies
        }
        self._purge(ToolGlobals, loaders, selected_space, dry_run=dry_run, verbose=verbose)
        if include_space:
            space_loader = SpaceLoader.create_loader(ToolGlobals, None)
            if dry_run:
                print(f"Would delete space {selected_space}")
            else:
                space_loader.delete([selected_space])
                print(f"Space {selected_space} deleted")
        prefix = "Would purge" if dry_run else "Purged"
        print(f"{prefix} space: {selected_space!r}.")

    @staticmethod
    def _get_selected_space(space: str | None, client: ToolkitClient) -> str:
        if space is None:
            spaces = client.data_modeling.spaces.list(limit=-1, include_global=False)
            selected_space = questionary.select(
                "Which space are you going to purge"
                " (delete all data models, views, containers, nodes and edges in space)?",
                [space.space for space in spaces],
            ).ask()
        else:
            retrieved = client.data_modeling.spaces.retrieve(space)
            if retrieved is None:
                raise ToolkitMissingResourceError(f"Space {space} does not exist")
            selected_space = space

        if selected_space is None:
            raise ToolkitValueError("No space selected")
        return selected_space

    def dataset(
        self,
        ToolGlobals: CDFToolConfig,
        external_id: str | None = None,
        include_dataset: bool = False,
        dry_run: bool = False,
        verbose: bool = False,
    ) -> None:
        """Purge a dataset and all its content"""
        raise NotImplementedError("Purging datasets is not yet supported")

    def _purge(
        self,
        ToolGlobals: CDFToolConfig,
        loaders: dict[type[ResourceLoader], frozenset[type[ResourceLoader]]],
        selected_space: str | None = None,
        selected_data_set: str | None = None,
        dry_run: bool = False,
        verbose: bool = False,
        batch_size: int = 1000,
    ) -> None:
        results = DeployResults([], "purge", dry_run=dry_run)
        loader_cls: type[ResourceLoader]
        for loader_cls in reversed(list(TopologicalSorter(loaders).static_order())):
            loader = loader_cls.create_loader(ToolGlobals, None)
            batch_ids: list[Hashable] = []
            count = 0
            for resource in loader.iterate(data_set_external_id=selected_data_set, space=selected_space):
                batch_ids.append(loader.get_id(resource))
                if len(batch_ids) >= batch_size:
                    count += self._delete_batch(batch_ids, dry_run, loader, verbose)
                    batch_ids = []
            if batch_ids:
                count += self._delete_batch(batch_ids, dry_run, loader, verbose)
            results[loader.display_name] = ResourceDeployResult(
                name=loader.display_name,
                deleted=count,
                total=count,
            )
        print(results.counts_table())

    @staticmethod
    def _delete_batch(batch_ids: list[Hashable], dry_run: bool, loader: ResourceLoader, verbose: bool) -> int:
        if dry_run:
            deleted = len(batch_ids)
        else:
            deleted = loader.delete(batch_ids)

        if verbose:
            prefix = "Would delete" if dry_run else "Deleted"
            print(f"{prefix} {deleted:,} resources")
        return deleted
