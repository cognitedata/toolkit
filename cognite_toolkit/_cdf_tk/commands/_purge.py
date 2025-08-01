import uuid
from collections.abc import Callable, Hashable, Iterable
from functools import partial
from graphlib import CycleError, TopologicalSorter
from typing import Literal, cast

import questionary
from cognite.client.data_classes import AggregateResultItem, DataSetUpdate, filters
from cognite.client.data_classes.aggregations import Count
from cognite.client.data_classes.data_modeling import NodeId, View, ViewId
from cognite.client.exceptions import CogniteAPIError, CogniteException
from cognite.client.utils._identifier import InstanceId
from rich import print
from rich.console import Console
from rich.panel import Panel
from rich.status import Status

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.constants import COGNITE_FILE_CONTAINER, COGNITE_TIME_SERIES_CONTAINER
from cognite_toolkit._cdf_tk.data_classes import DeployResults, ResourceDeployResult
from cognite_toolkit._cdf_tk.exceptions import (
    CDFAPIError,
    ResourceDeleteError,
    ToolkitMissingResourceError,
    ToolkitNotImplementedError,
    ToolkitRequiredValueError,
    ToolkitValueError,
)
from cognite_toolkit._cdf_tk.loaders import (
    RESOURCE_LOADER_LIST,
    AssetLoader,
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
    ViewLoader,
)
from cognite_toolkit._cdf_tk.tk_warnings import (
    HighSeverityWarning,
    LimitedAccessWarning,
    MediumSeverityWarning,
)
from cognite_toolkit._cdf_tk.utils import humanize_collection
from cognite_toolkit._cdf_tk.utils.cdf import iterate_instances
from cognite_toolkit._cdf_tk.utils.interactive_select import DataModelingSelect
from cognite_toolkit._cdf_tk.utils.producer_worker import ProducerWorkerExecutor
from cognite_toolkit._cdf_tk.utils.validate_access import ValidateAccess

from ._base import ToolkitCommand


class PurgeCommand(ToolkitCommand):
    def space(
        self,
        client: ToolkitClient,
        space: str | None = None,
        include_space: bool = False,
        dry_run: bool = False,
        auto_yes: bool = False,
        verbose: bool = False,
    ) -> None:
        """Purge a space and all its content"""
        selected_space = self._get_selected_space(space, client)
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
        is_purged = self._purge(client, loaders, selected_space, dry_run=dry_run, verbose=verbose)
        if include_space and is_purged:
            space_loader = SpaceLoader.create_loader(client)
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
                "Which space do you want to purge"
                " (including all data models, views, containers, nodes and edges within that space)?",
                sorted([space.space for space in spaces]),
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
        client: ToolkitClient,
        external_id: str | None = None,
        include_dataset: bool = False,
        dry_run: bool = False,
        auto_yes: bool = False,
        verbose: bool = False,
    ) -> None:
        """Purge a dataset and all its content"""
        selected_dataset = self._get_selected_dataset(external_id, client)
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
        is_purged = self._purge(client, loaders, selected_data_set=selected_dataset, dry_run=dry_run, verbose=verbose)
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
                client.data_sets.update(archived)
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
                expand=False,
            )
        )

    @staticmethod
    def _get_selected_dataset(external_id: str | None, client: ToolkitClient) -> str:
        if external_id is None:
            datasets = client.data_sets.list(limit=-1)
            selected_dataset: str = questionary.select(
                "Which space are you going to purge (delete all resources in dataset)?",
                sorted([dataset.external_id for dataset in datasets if dataset.external_id]),
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
        client: ToolkitClient,
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
        has_purged_views = False
        with Console().status("...", spinner="aesthetic", speed=0.4) as status:
            for loader_cls in reversed(list(TopologicalSorter(loaders).static_order())):
                if loader_cls not in loaders:
                    # Dependency that is included
                    continue
                loader = loader_cls.create_loader(client, console=status.console)
                status_prefix = "Would have deleted" if dry_run else "Deleted"
                if isinstance(loader, ViewLoader) and not dry_run:
                    status_prefix = "Expected deleted"  # Views are not always deleted immediately
                    has_purged_views = True

                if not dry_run and isinstance(loader, NodeLoader):
                    # Special handling of nodes as node type must be deleted after regular nodes
                    # In dry-run mode, we are not deleting the nodes, so we can skip this.
                    warnings_before = len(self.warning_list)
                    deleted_nodes = self._purge_nodes(loader, status, selected_space, verbose)
                    results[loader.display_name] = ResourceDeployResult(
                        name=loader.display_name,
                        deleted=deleted_nodes,
                        total=deleted_nodes,
                    )
                    if len(self.warning_list) > warnings_before and any(
                        isinstance(warn, HighSeverityWarning) for warn in self.warning_list[warnings_before:]
                    ):
                        is_purged = False
                    continue
                elif not dry_run and isinstance(loader, AssetLoader):
                    # Special handling of assets as we must ensure all children are deleted before the parent.
                    # In dry-run mode, we are not deleting the assets, so we can skip this.
                    deleted_assets = self._purge_assets(loader, status, selected_data_set)
                    results[loader.display_name] = ResourceDeployResult(
                        loader.display_name, deleted=deleted_assets, total=deleted_assets
                    )
                    continue

                # Child loaders are, for example, WorkflowTriggerLoader, WorkflowVersionLoader for WorkflowLoader
                # These must delete all resources that are connected to the resource that the loader is deleting
                # Exclude loaders that we are already iterating over
                child_loader_classes = self._get_dependencies(loader_cls, exclude=set(loaders))
                child_loaders = [
                    child_loader.create_loader(client)
                    for child_loader in reversed(list(TopologicalSorter(child_loader_classes).static_order()))
                    # Necessary as the topological sort includes dependencies that are not in the loaders
                    if child_loader in child_loader_classes
                ]
                count = 0
                status.update(f"{status_prefix} {count:,} {loader.display_name}...")
                batch_ids: list[Hashable] = []
                for resource in loader.iterate(data_set_external_id=selected_data_set, space=selected_space):
                    try:
                        batch_ids.append(loader.get_id(resource))
                    except (ToolkitRequiredValueError, KeyError) as e:
                        try:
                            batch_ids.append(loader.get_internal_id(resource))
                        except (AttributeError, NotImplementedError):
                            self.warn(
                                HighSeverityWarning(
                                    f"Cannot delete {type(resource).__name__}. Failed to obtain ID: {e}"
                                ),
                                console=status.console,
                            )
                            is_purged = False
                            continue

                    if len(batch_ids) >= batch_size:
                        child_deletion = self._delete_children(
                            batch_ids, child_loaders, dry_run, status.console, verbose
                        )
                        batch_delete, batch_size = self._delete_batch(
                            batch_ids, dry_run, loader, batch_size, status.console, verbose
                        )
                        count += batch_delete
                        status.update(f"{status_prefix} {count:,} {loader.display_name}...")
                        batch_ids = []
                        # The DeployResults is overloaded such that the below accumulates the counts
                        for name, child_count in child_deletion.items():
                            results[name] = ResourceDeployResult(name, deleted=child_count, total=child_count)

                if batch_ids:
                    child_deletion = self._delete_children(batch_ids, child_loaders, dry_run, status.console, verbose)
                    batch_delete, batch_size = self._delete_batch(
                        batch_ids, dry_run, loader, batch_size, status.console, verbose
                    )
                    count += batch_delete
                    status.update(f"{status_prefix} {count:,} {loader.display_name}...")
                    for name, child_count in child_deletion.items():
                        results[name] = ResourceDeployResult(name, deleted=child_count, total=child_count)
                if count > 0:
                    status.console.print(f"{status_prefix} {count:,} {loader.display_name}.")
                results[loader.display_name] = ResourceDeployResult(
                    name=loader.display_name,
                    deleted=count,
                    total=count,
                )
        print(results.counts_table(exclude_columns={"Created", "Changed", "Untouched", "Total"}))
        if has_purged_views:
            print("You might need to run the purge command multiple times to delete all views.")
        return is_purged

    def _delete_batch(
        self,
        batch_ids: list[Hashable],
        dry_run: bool,
        loader: ResourceLoader,
        batch_size: int,
        console: Console,
        verbose: bool,
    ) -> tuple[int, int]:
        if dry_run:
            deleted = len(batch_ids)
        else:
            try:
                deleted = loader.delete(batch_ids)
            except CogniteAPIError as delete_error:
                if (
                    delete_error.code == 408
                    and "timed out" in delete_error.message.casefold()
                    and batch_size > 1
                    and (len(batch_ids) > 1)
                ):
                    self.warn(
                        MediumSeverityWarning(
                            f"Timed out deleting {loader.display_name}. Trying again with a smaller batch size."
                        ),
                        include_timestamp=True,
                        console=console,
                    )
                    new_batch_size = len(batch_ids) // 2
                    first = batch_ids[:new_batch_size]
                    second = batch_ids[new_batch_size:]
                    first_deleted, first_batch_size = self._delete_batch(
                        first, dry_run, loader, new_batch_size, console, verbose
                    )
                    second_deleted, second_batch_size = self._delete_batch(
                        second, dry_run, loader, new_batch_size, console, verbose
                    )
                    return first_deleted + second_deleted, min(first_batch_size, second_batch_size)
                else:
                    raise delete_error

        if verbose:
            prefix = "Would delete" if dry_run else "Finished purging"
            console.print(f"{prefix} {deleted:,} {loader.display_name}")
        return deleted, batch_size

    @staticmethod
    def _delete_children(
        parent_ids: list[Hashable], child_loaders: list[ResourceLoader], dry_run: bool, console: Console, verbose: bool
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
                    console.print(f"{prefix} {count:,} {child_loader.display_name}")
            child_deletion[child_loader.display_name] = count
        return child_deletion

    def _purge_nodes(
        self,
        loader: NodeLoader,
        status: Status,
        selected_space: str | None = None,
        verbose: bool = False,
        batch_size: int = 1000,
    ) -> int:
        """Special handling of nodes as we must ensure all node types are deleted last."""
        # First find all Node Types
        node_types: dict[NodeId, set[NodeId]] = {}
        total_count = 0
        for node in loader.iterate(space=selected_space):
            if node.type and (selected_space is None or node.space == selected_space):
                node_id = NodeId(node.type.space, node.type.external_id)
                node_types[node_id] = set()
            total_count += 1
            status.update(f"Looking up node.type {total_count:,}...")

        count = 0
        batch_ids: list[NodeId] = []
        for node in loader.iterate(space=selected_space):
            node_id = node.as_id()
            if node_id in node_types:
                # Skip if it is a node type
                continue
            batch_ids.append(node_id)
            if len(batch_ids) >= batch_size:
                deleted, batch_size = self._delete_node_batch(batch_ids, loader, batch_size, status.console, verbose)
                count += deleted
                status.update(f"Deleted {count:,}/{total_count} {loader.display_name}...")
                batch_ids = []

        if batch_ids:
            deleted, batch_size = self._delete_node_batch(batch_ids, loader, batch_size, status.console, verbose)
            count += deleted

        # Finally delete all node types, first do a lookup and topological sort to ensure deleting in the right order.
        # Note this is an edge case, and is a result of some strange usage. But we need to handle it.
        for node_type in loader.retrieve(list(node_types.keys())):
            if node_type.type:
                required = NodeId(node_type.type.space, node_type.type.external_id)
                if required in node_types:
                    node_types[required].add(node_type.as_id())

        try:
            node_type_ids = list(TopologicalSorter(node_types).static_order())
        except CycleError as e:
            self.warn(
                HighSeverityWarning(f"Failed to delete node-types: Cycle detected. {e.args}"), console=status.console
            )
            return count

        deleted, batch_size = self._delete_node_batch(node_type_ids, loader, batch_size, status.console, verbose)
        count += deleted
        if count > 0:
            status.console.print(f"Finished purging {loader.display_name}.")
        return count

    def _delete_node_batch(
        self, batch_ids: list[NodeId], loader: NodeLoader, batch_size: int, console: Console, verbose: bool
    ) -> tuple[int, int]:
        try:
            deleted = loader.delete(batch_ids)
        except CogniteAPIError as delete_error:
            if (
                delete_error.code == 400
                and "Attempted to delete a node which is used as a type" in delete_error.message
            ):
                # Fallback to delete one by one
                deleted = 0
                for node_id in batch_ids:
                    try:
                        loader.delete([node_id])
                        deleted += 1
                    except CogniteAPIError:
                        is_type = filters.Equals(["node", "type"], node_id.dump(include_instance_type=False))
                        instance_spaces = {node.space for node in loader.client.data_modeling.instances(filter=is_type)}
                        if instance_spaces:
                            suffix = (
                                "It is used as a node type in the following spaces, "
                                f"which must be purged first: {humanize_collection(instance_spaces)}"
                            )
                        else:
                            suffix = "It is used as a node type, but failed to find the spaces where it is used."
                        self.warn(HighSeverityWarning(f"Failed to delete {node_id!r}. {suffix}"), console=console)
            elif (
                delete_error.code == 408
                and "timed out" in delete_error.message.casefold()
                and batch_size > 1
                and (len(batch_ids) > 1)
            ):
                self.warn(
                    MediumSeverityWarning(
                        f"Timed out deleting {loader.display_name}. Trying again with a smaller batch size."
                    ),
                    include_timestamp=True,
                    console=console,
                )
                new_batch_size = len(batch_ids) // 2
                first = batch_ids[:new_batch_size]
                second = batch_ids[new_batch_size:]
                first_deleted, first_batch_size = self._delete_node_batch(
                    first, loader, new_batch_size, console, verbose
                )
                second_deleted, second_batch_size = self._delete_node_batch(
                    second, loader, new_batch_size, console, verbose
                )
                return first_deleted + second_deleted, min(first_batch_size, second_batch_size)
            else:
                raise delete_error

        if verbose:
            console.print(f"Deleted {deleted:,} {loader.display_name}")
        return deleted, batch_size

    def _purge_assets(
        self,
        loader: AssetLoader,
        status: Status,
        selected_data_set: str | None = None,
        batch_size: int = 1000,
    ) -> int:
        # Using sets to avoid duplicates
        children_ids: set[int] = set()
        parent_ids: set[int] = set()
        is_first = True
        last_failed = False
        count = level = depth = last_parent_count = 0
        total_asset_count: int | None = None
        # Iterate through the asset hierarchy once per depth level. This is to delete all children before the parent.
        while is_first or level < depth:
            for asset in loader.iterate(data_set_external_id=selected_data_set):
                aggregates = cast(AggregateResultItem, asset.aggregates)
                if is_first and aggregates.depth is not None:
                    depth = max(depth, aggregates.depth)

                if aggregates.child_count == 0:
                    children_ids.add(asset.id)
                else:
                    parent_ids.add(asset.id)

                if len(children_ids) >= batch_size:
                    count += loader.delete(list(children_ids))
                    if total_asset_count:
                        status.update(f"Deleted {count:,}/{total_asset_count:,} {loader.display_name}...")
                    else:
                        status.update(f"Deleted {count:,} {loader.display_name}...")
                    children_ids = set()

            if is_first:
                total_asset_count = count + len(parent_ids) + len(children_ids)
            if children_ids:
                count += loader.delete(list(children_ids))
                status.update(f"Deleted {count:,}/{total_asset_count:,} {loader.display_name}...")
                children_ids = set()

            if len(parent_ids) == last_parent_count and last_failed:
                try:
                    # Just try to delete them all at once
                    count += loader.delete(list(parent_ids))
                except CogniteAPIError as e:
                    raise CDFAPIError(
                        f"Failed to delete {len(parent_ids)} assets. This could be due to a parent-child cycle or an "
                        "eventual consistency issue. Wait a few seconds and try again. An alternative is to use the "
                        "Python-SDK to delete the asset hierarchy "
                        "`client.assets.delete(external_id='my_root_asset', recursive=True)`"
                    ) from e
                else:
                    status.update(f"Deleted {count:,}/{total_asset_count:,} {loader.display_name}...")
                    break
            elif len(parent_ids) == last_parent_count:
                last_failed = True
            else:
                last_failed = False
            level += 1
            last_parent_count = len(parent_ids)
            parent_ids.clear()
            is_first = False
        status.console.print(f"Finished purging {loader.display_name}.")
        return count

    def instances(
        self,
        client: ToolkitClient,
        view: list[str] | None = None,
        instance_space: list[str] | None = None,
        instance_type: str = "node",
        dry_run: bool = False,
        auto_yes: bool = False,
        unlink: bool = True,
        verbose: bool = False,
    ) -> None:
        is_interactive = view is None
        validator = ValidateAccess(client, default_operation="purge")
        self.validate_model_access(validator, view)
        self.validate_instance_access(validator)
        selector = DataModelingSelect(client, operation="purge")
        selected_view = (
            selector.select_view(include_global=True) if view is None else self._get_selected_view(view, client)
        )
        self.validate_timeseries_access(validator, selected_view, unlink)

        if is_interactive:
            selected_instance_type = selector.select_instance_type(selected_view.used_for)
            instance_space = selector.select_instance_spaces(selected_view.as_id(), selected_instance_type)
            dry_run = questionary.confirm("Dry run?", default=True).ask()
            if self._is_timeseries(selected_view):
                unlink = questionary.confirm(
                    "Do you want to unlink timeseries from the datapoints before deleting the instances?", default=True
                ).ask()
        else:
            selected_instance_type = self._validate_instance_type(instance_type, selected_view.used_for)

        if not dry_run:
            self._print_panel("view", str(selected_view.as_id()))
            if not auto_yes:
                confirm = questionary.confirm(
                    f"Are you really sure you want to purge all {selected_instance_type}s with properties in the {selected_view.as_id()!r} view?",
                    default=False,
                ).ask()
                if not confirm:
                    return

        process: Callable[[list[InstanceId]], list[InstanceId]] = self._no_op
        if self._is_timeseries(selected_view) and unlink:
            process = partial(self._unlink_timeseries, client=client, dry_run=dry_run, verbose=verbose)

        if self._is_files(selected_view) and unlink:
            raise ToolkitNotImplementedError("Purging files and unlinking them is not yet implemented.")

        total = client.data_modeling.instances.aggregate(
            view=selected_view.as_id(),
            aggregates=Count("externalId"),
            instance_type=selected_instance_type,
            filter=filters.SpaceFilter(space=instance_space, instance_type=selected_instance_type)
            if instance_space
            else None,
            limit=-1,
        ).value
        if total is None or total == 0:
            print(f"No {selected_instance_type}s found with properties in the {selected_view.as_id()!r} view.")
            return

        iteration_count = int(total // 1000 + (1 if total % 1000 > 0 else 0))
        console = Console()

        executor = ProducerWorkerExecutor[list[InstanceId], list[InstanceId]](
            download_iterable=self._iterate_instances(
                client=client,
                view_id=selected_view.as_id(),
                instance_space=instance_space,
                instance_type=selected_instance_type,
                console=console,
            ),
            process=process,
            write=self._no_delete if dry_run else client.data_modeling.instances.delete_fast,  # type: ignore[arg-type]
            iteration_count=iteration_count,
            max_queue_size=10,
            download_description=f"Retrieving {instance_type}s",
            process_description=f"Preparing {instance_type}s for deletion",
            write_description=f"Deleting {instance_type}s",
            console=console,
        )

        executor.run()
        if executor.error_occurred:
            raise ResourceDeleteError(executor.error_message)

        prefix = "Would have purged" if dry_run else "Purged"
        console.print(
            f"{prefix} {executor.total_items:,} {selected_instance_type} with properties in the {selected_view.as_id()!r} view."
        )

    def validate_model_access(self, validator: ValidateAccess, view: list[str] | None) -> None:
        space = view[0] if isinstance(view, list) and view and isinstance(view[0], str) else None
        if space_ids := validator.data_model(["read"], space=space):
            self.warn(
                LimitedAccessWarning(
                    f"You can only select views in the {len(space_ids)} spaces you have access to: {humanize_collection(space_ids)}."
                )
            )

    def validate_instance_access(self, validator: ValidateAccess) -> None:
        if space_ids := validator.instances(["read", "write"]):
            self.warn(
                LimitedAccessWarning(
                    f"You can only purge instances in the {len(space_ids)} spaces you have access to: {humanize_collection(space_ids)}."
                )
            )

    def validate_timeseries_access(self, validator: ValidateAccess, view: View, unlink: bool) -> None:
        if unlink is False or not self._is_timeseries(view):
            return
        if ids_by_scope := validator.timeseries(["read", "write"], operation="unlink"):
            scope_str = humanize_collection(
                [f"{scope_name} ({humanize_collection(ids)})" for scope_name, ids in ids_by_scope.items()],
                bind_word="and",
            )
            self.warn(LimitedAccessWarning(f"You can only unlink time series in the following scopes: {scope_str}."))

    @staticmethod
    def _validate_instance_type(
        instance_type: str, view_used_for: Literal["node", "edge", "all"]
    ) -> Literal["node", "edge"]:
        if instance_type not in ["node", "edge"]:
            raise ToolkitValueError(f"Invalid instance type: {instance_type!r}. Must be 'node' or 'edge'.")
        if view_used_for == "all":
            return instance_type  # type: ignore[return-value]
        if view_used_for != instance_type:
            raise ToolkitValueError(
                f"View {view_used_for!r} does not support instance type {instance_type!r}. "
                f"You must purge {view_used_for!r} instances or select another view."
            )
        return instance_type  # type: ignore[return-value]

    @staticmethod
    def _is_timeseries(view: View) -> bool:
        return COGNITE_TIME_SERIES_CONTAINER in view.referenced_containers()

    @staticmethod
    def _is_files(view: View) -> bool:
        return COGNITE_FILE_CONTAINER in view.referenced_containers()

    @staticmethod
    def _iterate_instances(
        client: ToolkitClient,
        view_id: ViewId,
        instance_space: list[str] | None,
        instance_type: Literal["node", "edge"],
        console: Console,
    ) -> Iterable[list[InstanceId]]:
        chunk: list[InstanceId] = []
        for instance in iterate_instances(
            client=client,
            instance_type=instance_type,
            source=view_id,
            space=instance_space,
            console=console,
        ):
            chunk.append(instance.as_id())
            if len(chunk) >= 1000:
                yield chunk
                chunk = []
        if chunk:
            yield chunk

    @staticmethod
    def _no_op(instances: list[InstanceId]) -> list[InstanceId]:
        """No operation function that returns the input instances unchanged."""
        return instances

    @staticmethod
    def _no_delete(instances: list[InstanceId]) -> None:
        """No operation function that does not delete the instances."""
        # This is used in dry-run mode to avoid actual deletion
        pass

    def _unlink_timeseries(
        self, instances: list[InstanceId], client: ToolkitClient, dry_run: bool, verbose: bool
    ) -> list[InstanceId]:
        node_ids = [instance for instance in instances if isinstance(instance, NodeId)]
        if node_ids:
            timeseries = client.time_series.retrieve_multiple(instance_ids=node_ids, ignore_unknown_ids=True)
            if not dry_run and timeseries:
                migrated_timeseries_ids = [ts.id for ts in timeseries if ts.instance_id and ts.pending_instance_id]  # type: ignore[attr-defined]
                client.time_series.unlink_instance_ids(id=migrated_timeseries_ids)
                if verbose:
                    self.console(f"Unlinked {len(migrated_timeseries_ids)} timeseries from datapoints.")
        return instances

    @staticmethod
    def _get_selected_view(view: list[str], client: ToolkitClient) -> View:
        if not (isinstance(view, list) and len(view) == 3):
            raise ToolkitValueError(f"Invalid view format: {view}. Expected format is 'space externalId version'.")

        try:
            retrieve_views = client.data_modeling.views.retrieve(
                ViewId.load(tuple(view)),  # type: ignore[arg-type]
                include_inherited_properties=True,
            )
        except CogniteAPIError as e:
            raise CDFAPIError(f"Failed to retrieve view {view!r}. Status {e.code} Error: {e.message}") from e
        except CogniteException as e:
            raise CDFAPIError(f"Failed to retrieve view {view!r}. Error: {e}") from e
        if len(retrieve_views) == 0:
            raise ToolkitMissingResourceError(f"View {view!r} does not exist")
        return retrieve_views[0]
