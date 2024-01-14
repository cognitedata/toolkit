from __future__ import annotations

from collections import UserList
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from functools import total_ordering
from pathlib import Path
from typing import Literal

from cognite.client.data_classes._base import T_CogniteResourceList, T_WritableCogniteResource, T_WriteClass
from cognite.client.exceptions import CogniteAPIError, CogniteNotFoundError
from rich import print
from rich.table import Table

from cognite_toolkit.cdf_tk.load._base_loaders import T_ID, ResourceLoader, T_WritableCogniteResourceList
from cognite_toolkit.cdf_tk.load._resource_loaders import EdgeLoader, NodeLoader
from cognite_toolkit.cdf_tk.utils import CDFToolConfig


@total_ordering
@dataclass
class DeployResult:
    name: str
    created: int = 0
    deleted: int = 0
    changed: int = 0
    unchanged: int = 0
    skipped: int = 0
    total: int = 0

    @property
    def calculated_total(self) -> int:
        return self.created + self.deleted + self.changed + self.unchanged + self.skipped

    def __lt__(self, other: object) -> bool:
        if isinstance(other, DeployResult):
            return self.name < other.name
        else:
            return NotImplemented

    def __eq__(self, other: object) -> bool:
        if isinstance(other, DeployResult):
            return self.name == other.name
        else:
            return NotImplemented


class DeployResults(UserList):
    def __init__(self, collection: Iterable[DeployResult], action: Literal["deploy", "clean"], dry_run: bool = False):
        super().__init__(collection)
        self.action = action
        self.dry_run = dry_run

    def create_rich_table(self) -> Table:
        table = Table(title=f"Summary of {self.action} command:")
        prefix = ""
        if self.dry_run:
            prefix = "Would have "
        table.add_column("Resource", justify="right")
        table.add_column(f"{prefix}Created", justify="right", style="green")
        table.add_column(f"{prefix}Deleted", justify="right", style="red")
        table.add_column(f"{prefix}Changed", justify="right", style="magenta")
        table.add_column("Unchanged", justify="right", style="cyan")
        table.add_column(f"{prefix}Skipped", justify="right", style="yellow")
        table.add_column("Total", justify="right")
        for item in sorted(entry for entry in self.data if entry is not None):
            table.add_row(
                item.name,
                str(item.created),
                str(item.deleted),
                str(item.changed),
                str(item.unchanged),
                str(item.skipped),
                str(item.total),
            )

        return table


def clean_resources(
    loader: ResourceLoader[
        T_ID, T_WriteClass, T_WritableCogniteResource, T_CogniteResourceList, T_WritableCogniteResourceList
    ],
    path: Path,
    ToolGlobals: CDFToolConfig,
    dry_run: bool = False,
    drop_data: bool = False,
    verbose: bool = False,
) -> DeployResult | None:
    filepaths = loader.find_files(path)

    # Since we do a clean, we do not want to verify that everything exists wrt data sets, spaces etc.
    batches: list[T_CogniteResourceList] | None = _load_batches(loader, filepaths, skip_validation=dry_run)
    if batches is None:
        ToolGlobals.failed = True
        return None

    nr_of_batches = len(batches)
    nr_of_items = sum(len(batch) for batch in batches)
    if nr_of_items == 0:
        return DeployResult(name=loader.display_name)

    action_word = "Loading" if dry_run else "Cleaning"
    print(f"[bold]{action_word} {nr_of_items} {loader.display_name} in {nr_of_batches} batches to CDF...[/]")

    # Deleting resources.
    nr_of_deleted = _delete_resources(loader, batches, drop_data, dry_run, verbose)

    return DeployResult(name=loader.display_name, deleted=nr_of_deleted, total=nr_of_items)


def deploy_resources(
    loader: ResourceLoader[
        T_ID, T_WriteClass, T_WritableCogniteResource, T_CogniteResourceList, T_WritableCogniteResourceList
    ],
    path: Path,
    ToolGlobals: CDFToolConfig,
    drop: bool = False,
    clean: bool = False,
    dry_run: bool = False,
    drop_data: bool = False,
    verbose: bool = False,
) -> DeployResult | None:
    filepaths = loader.find_files(path)

    batches: list[T_CogniteResourceList] | None = _load_batches(loader, filepaths, skip_validation=dry_run)
    if batches is None:
        ToolGlobals.failed = True
        return None

    nr_of_batches = len(batches)
    nr_of_items = sum(len(batch) for batch in batches)
    if nr_of_items == 0:
        return DeployResult(name=loader.display_name)

    action_word = "Loading" if dry_run else "Uploading"
    print(f"[bold]{action_word} {nr_of_items} {loader.display_name} in {nr_of_batches} batches to CDF...[/]")

    if drop and loader.support_drop:
        if drop_data and (loader.api_name in ["data_modeling.spaces", "data_modeling.containers"]):
            print(
                f"  --drop-data is specified, will delete existing nodes and edges before before deleting {loader.display_name}."
            )
        else:
            print(f"  --drop is specified, will delete existing {loader.display_name} before uploading.")

    # Deleting resources.
    nr_of_deleted = 0
    if (drop and loader.support_drop) or clean:
        nr_of_deleted = _delete_resources(loader, batches, drop_data, dry_run, verbose)

    nr_of_created = 0
    nr_of_changed = 0
    nr_of_unchanged = 0
    nr_of_skipped = 0
    for batch_no, (batch, filepath) in enumerate(zip(batches, filepaths), 1):
        batch_ids = loader.get_ids(batch)
        cdf_resources = loader.retrieve(batch_ids).as_write()
        cdf_resource_by_id = {loader.get_id(resource): resource for resource in cdf_resources}

        to_create: T_CogniteResourceList
        to_update: T_CogniteResourceList
        if isinstance(loader, (NodeLoader, EdgeLoader)):
            # Special case for nodes and edges
            to_create = loader.list_write_cls.create_empty_from(batch)  # type: ignore[attr-defined]
            to_update = loader.list_write_cls.create_empty_from(batch)  # type: ignore[attr-defined]
        else:
            to_create = loader.list_write_cls([])
            to_update = loader.list_write_cls([])

        for item in batch:
            cdf_resource = cdf_resource_by_id.get(loader.get_id(item))
            if cdf_resource and item == cdf_resource:
                nr_of_unchanged += 1
            elif cdf_resource:
                to_update.append(item)
            else:
                to_create.append(item)

        if dry_run:
            nr_of_created += len(to_create)
            nr_of_changed += len(to_update)
            if verbose:
                print(
                    f" {batch_no}/{len(batch)} {loader.display_name} would have: Changed {nr_of_changed},"
                    f" Created {nr_of_created}, and left {nr_of_unchanged} unchanged"
                )
            continue

        if to_create:
            try:
                created = loader.create(to_create, drop, filepath)
            except Exception as e:
                print(f"  [bold yellow]WARNING:[/] Failed to upload {loader.display_name}. Error {e}.")
                ToolGlobals.failed = True
                return None
            else:
                newly_created = len(created) if created is not None else 0
                nr_of_created += newly_created
                nr_of_skipped += len(batch) - newly_created
                # For timeseries.datapoints, we can load multiple timeseries in one file,
                # so the number of created items can be larger than the number of items in the batch.
                if nr_of_skipped < 0:
                    nr_of_items += -nr_of_skipped
                    nr_of_skipped = 0

        if to_update:
            try:
                updated = loader.update(to_update, filepath)
            except Exception as e:
                print(f"  [bold yellow]WARNING:[/] Failed to update {loader.display_name}. Error {e}.")
                ToolGlobals.failed = True
                return None
            else:
                nr_of_changed += len(updated)

    if verbose:
        print(
            f"  Created {nr_of_created}, Deleted {nr_of_deleted}, Changed {nr_of_changed}, Unchanged {nr_of_unchanged}, Skipped {nr_of_skipped}, Total {nr_of_items}."
        )
    return DeployResult(
        name=loader.display_name,
        created=nr_of_created,
        deleted=nr_of_deleted,
        changed=nr_of_changed,
        unchanged=nr_of_unchanged,
        skipped=nr_of_skipped,
        total=nr_of_items,
    )


def _load_batches(
    loader: ResourceLoader, filepaths: list[Path], skip_validation: bool
) -> list[T_CogniteResourceList] | None:
    batches: list[T_CogniteResourceList] = []
    for filepath in filepaths:
        try:
            resource = loader.load_resource(filepath, skip_validation=skip_validation)
        except KeyError as e:
            # KeyError means that we are missing a required field in the yaml file.
            print(
                f"[bold red]ERROR:[/] Failed to load {filepath.name} with {loader.display_name}. Missing required field: {e}."
            )
            return None
        if resource is None:
            print(f"[bold yellow]WARNING:[/] Skipping {filepath.name}. No data to load.")
            continue
        batches.append(resource if isinstance(resource, Sequence) else loader.list_write_cls([resource]))
    return batches


def _delete_resources(
    loader: ResourceLoader, batches: list[T_CogniteResourceList], drop_data: bool, dry_run: bool, verbose: bool
) -> int:
    nr_of_deleted = 0
    for batch in batches:
        batch_ids = loader.get_ids(batch)
        if dry_run:
            nr_of_deleted += len(batch_ids)
            if verbose:
                print(f"  Would have deleted {len(batch_ids)} {loader.display_name}.")
            continue

        try:
            nr_of_deleted += loader.delete(batch_ids, drop_data)
        except CogniteAPIError as e:
            if e.code == 404:
                print(f"  [bold yellow]WARNING:[/] {len(batch_ids)} {loader.display_name} do(es) not exist.")
        except CogniteNotFoundError:
            print(f"  [bold yellow]WARNING:[/] {len(batch_ids)} {loader.display_name} do(es) not exist.")
        except Exception as e:
            print(f"  [bold yellow]WARNING:[/] Failed to delete {len(batch_ids)} {loader.display_name}. Error {e}.")
        else:  # Delete succeeded
            if verbose:
                print(f"  Deleted {len(batch_ids)} {loader.display_name}.")
    return nr_of_deleted
