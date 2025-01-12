from __future__ import annotations

import uuid
from collections.abc import Hashable
from graphlib import TopologicalSorter

import questionary
from cognite.client.data_classes import DataSetUpdate
from cognite.client.data_classes.data_modeling import NodeId
from cognite.client.exceptions import CogniteAPIError
from rich import print
from rich.panel import Panel

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.data_classes import DeployResults, ResourceDeployResult
from cognite_toolkit._cdf_tk.exceptions import ToolkitMissingResourceError, ToolkitRequiredValueError, ToolkitValueError
from cognite_toolkit._cdf_tk.loaders import (
    RESOURCE_LOADER_LIST,
    CogniteFileLoader,
    DataSetsLoader,
    FunctionLoader,
    GraphQLLoader,
    GroupAllScopedLoader,
    GroupLoader,
    GroupResourceScopedLoader,
    HostedExtractorDestinationLoader,
    LocationFilterLoader,
    NodeLoader,
    ResourceLoader,
    SpaceLoader,
    StreamlitLoader,
    TransformationLoader,
)
from cognite_toolkit._cdf_tk.tk_warnings import HighSeverityWarning
from cognite_toolkit._cdf_tk.utils import CDFToolConfig

from ._base import ToolkitCommand


class PurgeCommand(ToolkitCommand):
    def space(
        self,
        ToolGlobals: CDFToolConfig,
        space: str | None = None,
        include_space: bool = False,
        dry_run: bool = False,
        auto_yes: bool = False,
        verbose: bool = False,
    ) -> None:
        """Purge a space and all its content"""
        selected_space = self._get_selected_space(space, ToolGlobals.toolkit_client)
        if space is None:
            # Interactive mode
            include_space = questionary.confirm("Do you also want to delete the space itself?", default=False).ask()
            dry_run = questionary.confirm("Dry run?", default=True).ask()
        if not dry_run:
            self._print_panel("space", selected_space)
            if not auto_yes:
                confirm = questionary.confirm(
                    f"Are you really sure you want to purge the {selected_space!r} space?", default=False
                ).ask()
                if not confirm:
                    return

        loaders = self._get_dependencies(
            SpaceLoader,
            exclude={
                GraphQLLoader,
                GroupResourceScopedLoader,
                LocationFilterLoader,
                TransformationLoader,
                CogniteFileLoader,
            },
        )
        is_purged = self._purge(ToolGlobals, loaders, selected_space, dry_run=dry_run, verbose=verbose)
        if include_space and is_purged:
            space_loader = SpaceLoader.create_loader(ToolGlobals, None)
            if dry_run:
                print(f"Would delete space {selected_space}")
            else:
                try:
                    space_loader.delete([selected_space])
                    print(f"Space {selected_space} deleted")
                except CogniteAPIError as e:
                    self.warn(HighSeverityWarning(f"Failed to delete space {selected_space!r}: {e}"))
        elif include_space:
            self.warn(HighSeverityWarning(f"Space {selected_space!r} was not deleted due to errors during the purge"))

        if not dry_run and is_purged:
            print(f"Purge space {selected_space!r} completed.")
        elif not dry_run:
            print(f"Purge space {selected_space!r} partly completed. See warnings for details.")

    @staticmethod
    def _get_dependencies(
        loader_cls: type[ResourceLoader], exclude: set[type[ResourceLoader]] | None = None
    ) -> dict[type[ResourceLoader], frozenset[type[ResourceLoader]]]:
        return {
            dep_cls: dep_cls.dependencies
            for dep_cls in RESOURCE_LOADER_LIST
            if loader_cls in dep_cls.dependencies and (exclude is None or dep_cls not in exclude)
        }

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
        auto_yes: bool = False,
        verbose: bool = False,
    ) -> None:
        """Purge a dataset and all its content"""
        selected_dataset = self._get_selected_dataset(external_id, ToolGlobals.toolkit_client)
        if external_id is None:
            # Interactive mode
            include_dataset = questionary.confirm(
                "Do you want to archive the dataset itself after the purge?", default=False
            ).ask()
            dry_run = questionary.confirm("Dry run?", default=True).ask()
        if not dry_run:
            self._print_panel("dataset", selected_dataset)
            if not auto_yes:
                confirm = questionary.confirm(
                    f"Are you really sure you want to purge the {selected_dataset!r} dataset?", default=False
                ).ask()
                if not confirm:
                    return

        loaders = self._get_dependencies(
            DataSetsLoader,
            exclude={
                GroupLoader,
                GroupResourceScopedLoader,
                GroupAllScopedLoader,
                StreamlitLoader,
                HostedExtractorDestinationLoader,
                FunctionLoader,
                LocationFilterLoader,
            },
        )
        is_purged = self._purge(
            ToolGlobals, loaders, selected_data_set=selected_dataset, dry_run=dry_run, verbose=verbose
        )
        if include_dataset and is_purged:
            if dry_run:
                print(f"Would have archived {selected_dataset}")
            else:
                archived = (
                    DataSetUpdate(external_id=selected_dataset)
                    .external_id.set(str(uuid.uuid4()))
                    .metadata.add({"archived": "true"})
                    .write_protected.set(True)
                )
                ToolGlobals.toolkit_client.data_sets.update(archived)
                print(f"DataSet {selected_dataset} archived")
        elif include_dataset:
            self.warn(
                HighSeverityWarning(f"DataSet {selected_dataset} was not archived due to errors during the purge")
            )

        if not dry_run and is_purged:
            print(f"Purged dataset {selected_dataset!r} completed")
        elif not dry_run:
            print(f"Purged dataset {selected_dataset!r} partly completed. See warnings for details.")

    def _print_panel(self, resource_type: str, resource: str) -> None:
        print(
            Panel(
                f"[red]WARNING:[/red] This operation [bold]cannot be undone[/bold]! "
                f"Resources in {resource!r} are permanently deleted",
                style="bold",
                title=f"Purge {resource_type}",
                title_align="left",
                border_style="red",
            )
        )

    @staticmethod
    def _get_selected_dataset(external_id: str | None, client: ToolkitClient) -> str:
        if external_id is None:
            datasets = client.data_sets.list(limit=-1)
            selected_dataset: str = questionary.select(
                "Which space are you going to purge (delete all resources in dataset)?",
                [dataset.external_id for dataset in datasets if dataset.external_id],
            ).ask()
        else:
            retrieved = client.data_sets.retrieve(external_id=external_id)
            if retrieved is None:
                raise ToolkitMissingResourceError(f"DataSet {external_id!r} does not exist")
            selected_dataset = external_id

        if selected_dataset is None:
            raise ToolkitValueError("No space selected")
        return selected_dataset

    def _purge(
        self,
        ToolGlobals: CDFToolConfig,
        loaders: dict[type[ResourceLoader], frozenset[type[ResourceLoader]]],
        selected_space: str | None = None,
        selected_data_set: str | None = None,
        dry_run: bool = False,
        verbose: bool = False,
        batch_size: int = 1000,
    ) -> bool:
        is_purged = True
        results = DeployResults([], "purge", dry_run=dry_run)
        loader_cls: type[ResourceLoader]
        for loader_cls in reversed(list(TopologicalSorter(loaders).static_order())):
            if loader_cls not in loaders:
                # Dependency that is included
                continue
            loader = loader_cls.create_loader(ToolGlobals, None)

            if isinstance(loader, NodeLoader) and not dry_run:
                # Special handling of nodes as node type must be deleted after regular nodes
                # In dry-run mode, we are not deleting the nodes, so we can skip this.
                warnings_before = len(self.warning_list)
                deleted_nodes = self._purge_nodes(loader, selected_space, verbose)
                results[loader.display_name] = ResourceDeployResult(
                    name=loader.display_name,
                    deleted=deleted_nodes,
                    total=deleted_nodes,
                )
                if len(self.warning_list) > warnings_before:
                    is_purged = False
                continue

            # Child loaders are, for example, WorkflowTriggerLoader, WorkflowVersionLoader for WorkflowLoader
            # These must delete all resources that are connected to the resource that the loader is deleting
            # Exclude loaders that we are already iterating over
            child_loader_classes = self._get_dependencies(loader_cls, exclude=set(loaders))
            child_loaders = [
                child_loader.create_loader(ToolGlobals, None)
                for child_loader in reversed(list(TopologicalSorter(child_loader_classes).static_order()))
                # Necessary as the topological sort includes dependencies that are not in the loaders
                if child_loader in child_loader_classes
            ]
            count = 0
            batch_ids: list[Hashable] = []
            for resource in loader.iterate(data_set_external_id=selected_data_set, space=selected_space):
                try:
                    batch_ids.append(loader.get_id(resource))
                except ToolkitRequiredValueError as e:
                    try:
                        batch_ids.append(loader.get_internal_id(resource))
                    except (AttributeError, NotImplementedError):
                        self.warn(
                            HighSeverityWarning(f"Cannot delete {type(resource).__name__}. Failed to obtain ID: {e}")
                        )
                        is_purged = False
                        continue

                if len(batch_ids) >= batch_size:
                    child_deletion = self._delete_children(batch_ids, child_loaders, dry_run, verbose)
                    count += self._delete_batch(batch_ids, dry_run, loader, verbose)
                    batch_ids = []
                    # The DeployResults is overloaded such that the below accumulates the counts
                    for name, child_count in child_deletion.items():
                        results[name] = ResourceDeployResult(name, deleted=child_count, total=child_count)

            if batch_ids:
                child_deletion = self._delete_children(batch_ids, child_loaders, dry_run, verbose)
                count += self._delete_batch(batch_ids, dry_run, loader, verbose)
                for name, child_count in child_deletion.items():
                    results[name] = ResourceDeployResult(name, deleted=child_count, total=child_count)

            results[loader.display_name] = ResourceDeployResult(
                name=loader.display_name,
                deleted=count,
                total=count,
            )
        print(results.counts_table(exclude_columns={"Created", "Changed", "Untouched", "Total"}))
        return is_purged

    @staticmethod
    def _delete_batch(batch_ids: list[Hashable], dry_run: bool, loader: ResourceLoader, verbose: bool) -> int:
        if dry_run:
            deleted = len(batch_ids)
        else:
            deleted = loader.delete(batch_ids)

        if verbose:
            prefix = "Would delete" if dry_run else "Deleted"
            print(f"{prefix} {deleted:,} {loader.display_name}")
        return deleted

    @staticmethod
    def _delete_children(
        parent_ids: list[Hashable], child_loaders: list[ResourceLoader], dry_run: bool, verbose: bool
    ) -> dict[str, int]:
        child_deletion: dict[str, int] = {}
        for child_loader in child_loaders:
            child_ids = set()
            for child in child_loader.iterate(parent_ids=parent_ids):
                child_ids.add(child_loader.get_id(child))
            count = 0
            if child_ids:
                if dry_run:
                    count = len(child_ids)
                else:
                    count = child_loader.delete(list(child_ids))

                if verbose:
                    prefix = "Would delete" if dry_run else "Deleted"
                    print(f"{prefix} {count:,} {child_loader.display_name}")
            child_deletion[child_loader.display_name] = count
        return child_deletion

    def _purge_nodes(
        self, loader: NodeLoader, selected_space: str | None = None, verbose: bool = False, batch_size: int = 1000
    ) -> int:
        """Special handling of nodes as we must ensure all node types are deleted last."""
        # First find all Node Types
        node_types: set[NodeId] = set()
        for node in loader.iterate(space=selected_space):
            if node.type:
                node_types.add(NodeId(node.type.space, node.type.external_id))
        count = 0
        batch_ids: list[NodeId] = []
        for node in loader.iterate(space=selected_space):
            node_id = node.as_id()
            if node_id in node_types:
                # Skip if it is a node type
                continue
            batch_ids.append(node_id)
            if len(batch_ids) >= batch_size:
                count += self._delete_node_batch(batch_ids, loader, verbose)
                batch_ids = []

        if batch_ids:
            count += self._delete_node_batch(batch_ids, loader, verbose)

        # Finally delete all node types
        count += self._delete_node_batch(list(node_types), loader, verbose)
        return count

    def _delete_node_batch(self, batch_ids: list[NodeId], loader: NodeLoader, verbose: bool) -> int:
        try:
            deleted = loader.delete(batch_ids)
        except CogniteAPIError as e:
            if e.code == 400 and "Attempted to delete a node which is used as a type" in e.message:
                # Fallback to delete one by one
                deleted = 0
                for node_id in batch_ids:
                    try:
                        loader.delete([node_id])
                        deleted += 1
                    except CogniteAPIError as e:
                        self.warn(
                            HighSeverityWarning(
                                f"Failed to delete {node_id!r}. This is because it is used as a node type in a different space: {e!s}"
                            )
                        )
            else:
                raise

        if verbose:
            print(f"Deleted {deleted:,} {loader.display_name}")
        return deleted
