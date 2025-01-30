from __future__ import annotations

from abc import ABC
from collections import UserDict
from collections.abc import Iterable
from dataclasses import dataclass
from functools import total_ordering
from typing import Literal

from rich.table import Table


@total_ordering
@dataclass
class DeployResult(ABC):
    name: str

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


@dataclass
class ResourceDeployResult(DeployResult):
    created: int = 0
    deleted: int = 0
    changed: int = 0
    unchanged: int = 0
    total: int = 0

    @property
    def calculated_total(self) -> int:
        return self.created + self.deleted + self.changed + self.unchanged

    def __iadd__(self, other: ResourceDeployResult) -> ResourceDeployResult:
        if self.name != other.name:
            raise ValueError("Cannot add two DeployResult objects with different names")
        self.created += other.created
        self.deleted += other.deleted
        self.changed += other.changed
        self.unchanged += other.unchanged
        self.total += other.total

        if isinstance(other, ResourceContainerDeployResult):
            return ResourceContainerDeployResult(
                name=self.name,
                created=self.created,
                deleted=self.deleted,
                changed=self.changed,
                unchanged=self.unchanged,
                total=self.total,
                item_name=other.item_name,
                dropped_datapoints=other.dropped_datapoints,
            )
        else:
            return self


@dataclass
class ResourceContainerDeployResult(ResourceDeployResult):
    item_name: str = ""
    dropped_datapoints: int = 0

    def __iadd__(self, other: ResourceDeployResult) -> ResourceContainerDeployResult:
        if self.name != other.name:
            raise ValueError("Cannot add two ResourceContainerDeployResult objects with different names")
        super().__iadd__(other)
        if isinstance(other, ResourceContainerDeployResult):
            self.dropped_datapoints += other.dropped_datapoints
        return self


@dataclass
class UploadDeployResult(DeployResult):
    uploaded: int = 0
    item_name: str = ""

    def __iadd__(self, other: UploadDeployResult) -> UploadDeployResult:
        if self.name != other.name:
            raise ValueError("Cannot add two DeployResult objects with different names")
        self.uploaded += other.uploaded

        if isinstance(other, DatapointDeployResult):
            return DatapointDeployResult(
                name=self.name, uploaded=self.uploaded, item_name=other.item_name, points=other.points
            )
        else:
            return self


@dataclass
class DatapointDeployResult(UploadDeployResult):
    points: int = 0

    def __iadd__(self, other: UploadDeployResult) -> UploadDeployResult:
        if self.name != other.name:
            raise ValueError("Cannot add two DeployResult objects with different names")
        super().__iadd__(other)
        if isinstance(other, DatapointDeployResult):
            self.points += other.points
        return self


class DeployResults(UserDict):
    def __init__(
        self,
        collection: Iterable[DeployResult],
        action: Literal["deploy", "clean", "purge", "pull"],
        dry_run: bool = False,
    ):
        super().__init__({entry.name: entry for entry in collection})
        self.action = action
        self.dry_run = dry_run

    @property
    def has_counts(self) -> bool:
        return any(isinstance(entry, ResourceDeployResult) for entry in self.data.values())

    @property
    def has_uploads(self) -> bool:
        return any(
            isinstance(entry, (UploadDeployResult, ResourceContainerDeployResult)) for entry in self.data.values()
        )

    def counts_table(
        self, exclude_columns: set[Literal["Created", "Deleted", "Changed", "Untouched", "Total"]] | None = None
    ) -> Table:
        table = Table(title=f"Summary of Resources {self.action.title()} operation:")
        prefix = "Would have " if self.dry_run else ""
        table.add_column("Resource", justify="right", width=30)
        if exclude_columns is None or "Created" not in exclude_columns:
            table.add_column(f"{prefix}Created", justify="right", style="green")
        if exclude_columns is None or "Deleted" not in exclude_columns:
            table.add_column(f"{prefix}Deleted", justify="right", style="red")
        if exclude_columns is None or "Changed" not in exclude_columns:
            table.add_column(f"{prefix}Changed", justify="right", style="magenta")
        if exclude_columns is None or "Untouched" not in exclude_columns:
            table.add_column("Untouched" if self.dry_run else "Unchanged", justify="right", style="cyan")
        if exclude_columns is None or "Total" not in exclude_columns:
            table.add_column("Total", justify="right")
        is_deploy = self.action == "deploy"
        for item in sorted(
            entry for entry in self.data.values() if entry is not None and isinstance(entry, ResourceDeployResult)
        ):
            if (
                item.created == 0
                and item.deleted == 0
                and item.changed == 0
                and item.unchanged == 0
                and item.total == 0
            ):
                continue

            row = [item.name]
            if exclude_columns is None or "Created" not in exclude_columns:
                row.append(f"{item.created:,}")
            if exclude_columns is None or "Deleted" not in exclude_columns:
                row.append(f"{item.deleted:,}")
            if exclude_columns is None or "Changed" not in exclude_columns:
                row.append(f"{item.changed:,}")
            if exclude_columns is None or "Untouched" not in exclude_columns:
                row.append(f"{item.unchanged:,}" if is_deploy else "-")
            if exclude_columns is None or "Total" not in exclude_columns:
                row.append(f"{item.total:,}")
            table.add_row(*row)

        return table

    def uploads_table(self) -> Table:
        table = Table(title=f"Summary of Data {self.action.title()} operation (data is always uploaded):")
        prefix = "Would have " if self.dry_run else ""
        table.add_column("Resource", justify="right")
        table.add_column(f"{prefix}Uploaded Data", justify="right", style="cyan")
        table.add_column("Item Type", justify="right")
        table.add_column("From files", justify="right", style="green")
        table.add_column(f"{prefix}Deleted Data", justify="right", style="red")
        for item in sorted(
            entry
            for entry in self.data.values()
            if isinstance(entry, (UploadDeployResult, ResourceContainerDeployResult))
        ):
            if item.name == "raw.tables":
                # We skip this as we cannot count the number of datapoints in a raw table
                # and all we can do is to print a misleading 0 for deleted datapoints.
                continue

            if isinstance(item, UploadDeployResult):
                if isinstance(item, DatapointDeployResult):
                    datapoints = f"{item.points:,}"
                else:
                    datapoints = "-"
                table.add_row(item.name, datapoints, item.item_name, str(item.uploaded), "-")
            elif isinstance(item, ResourceContainerDeployResult):
                table.add_row(item.name, "-", item.item_name, "-", f"{item.dropped_datapoints:,}")

        return table

    def __getitem__(self, item: str) -> DeployResult:
        return self.data[item]

    def __setitem__(self, key: str, value: DeployResult) -> None:
        if key not in self.data:
            self.data[key] = value
        else:
            self.data[key] += value
