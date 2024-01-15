"""These are helper data classes for the load module."""

from __future__ import annotations

from abc import ABC
from collections import UserList
from collections.abc import Iterable
from dataclasses import dataclass
from functools import total_ordering
from typing import Any, Literal

from cognite.client import CogniteClient
from cognite.client.data_classes._base import (
    CogniteResourceList,
    WriteableCogniteResource,
    WriteableCogniteResourceList,
)
from cognite.client.data_classes.data_modeling import EdgeApplyList, NodeApplyList
from rich.table import Table
from typing_extensions import Self


@total_ordering
@dataclass(frozen=True)
class RawDatabaseTable(WriteableCogniteResource):
    db_name: str
    table_name: str | None = None

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> RawDatabaseTable:
        return cls(db_name=resource["dbName"], table_name=resource.get("tableName"))

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        dumped = {
            "dbName" if camel_case else "db_name": self.db_name,
        }
        if self.table_name is not None:
            dumped["tableName" if camel_case else "table_name"] = self.table_name
        return dumped

    def as_write(self) -> RawDatabaseTable:
        return self

    def __lt__(self, other: object) -> bool:
        if isinstance(other, RawDatabaseTable):
            if self.db_name == other.db_name:
                return (self.table_name or "") < (other.table_name or "")
            else:
                return self.db_name < other.db_name
        else:
            return NotImplemented

    def __eq__(self, other: object) -> bool:
        if isinstance(other, RawDatabaseTable):
            return self.db_name == other.db_name and self.table_name == other.table_name
        else:
            return NotImplemented


class RawTableList(WriteableCogniteResourceList[RawDatabaseTable, RawDatabaseTable]):
    _RESOURCE = RawDatabaseTable

    def as_write(self) -> CogniteResourceList[RawDatabaseTable]:
        return self

    def as_db_names(self) -> list[str]:
        return [table.db_name for table in self.data]


@dataclass
class LoadableNodes(NodeApplyList):
    """
    This is a helper class for nodes that contains arguments that are required for writing the
    nodes to CDF.
    """

    auto_create_direct_relations: bool
    skip_on_version_conflict: bool
    replace: bool
    nodes: NodeApplyList

    def __post_init__(self) -> None:
        self.data = self.nodes.data

    def __len__(self) -> int:
        return len(self.data)

    @classmethod
    def create_empty_from(cls, other: LoadableNodes) -> LoadableNodes:
        return cls(
            auto_create_direct_relations=other.auto_create_direct_relations,
            skip_on_version_conflict=other.skip_on_version_conflict,
            replace=other.replace,
            nodes=NodeApplyList([]),
        )

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> LoadableNodes:  # type: ignore[override]
        return cls(
            auto_create_direct_relations=resource["autoCreateDirectRelations"],
            skip_on_version_conflict=resource["skipOnVersionConflict"],
            replace=resource["replace"],
            nodes=NodeApplyList.load(resource["nodes"]),
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:  # type: ignore[override]
        return {
            "autoCreateDirectRelations"
            if camel_case
            else "auto_create_direct_relations": self.auto_create_direct_relations,
            "skipOnVersionConflict" if camel_case else "skip_on_version_conflict": self.skip_on_version_conflict,
            "replace": self.replace,
            "nodes": self.nodes.dump(camel_case),
        }


@dataclass
class LoadableEdges(EdgeApplyList):
    """
    This is a helper class for edges that contains arguments that are required for writing the
    edges to CDF.
    """

    auto_create_start_nodes: bool
    auto_create_end_nodes: bool
    skip_on_version_conflict: bool
    replace: bool
    edges: EdgeApplyList

    def __post_init__(self) -> None:
        self.data = self.edges.data

    def __len__(self) -> int:
        return len(self.data)

    @classmethod
    def create_empty_from(cls, other: LoadableEdges) -> LoadableEdges:
        return cls(
            auto_create_start_nodes=other.auto_create_start_nodes,
            auto_create_end_nodes=other.auto_create_end_nodes,
            skip_on_version_conflict=other.skip_on_version_conflict,
            replace=other.replace,
            edges=EdgeApplyList([]),
        )

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:  # type: ignore[override]
        return cls(
            auto_create_start_nodes=resource["autoCreateStartNodes"],
            auto_create_end_nodes=resource["autoCreateEndNodes"],
            skip_on_version_conflict=resource["skipOnVersionConflict"],
            replace=resource["replace"],
            edges=EdgeApplyList.load(resource["edges"]),
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:  # type: ignore[override]
        return {
            "autoCreateStartNodes" if camel_case else "auto_create_start_nodes": self.auto_create_start_nodes,
            "autoCreateEndNodes" if camel_case else "auto_create_end_nodes": self.auto_create_end_nodes,
            "skipOnVersionConflict" if camel_case else "skip_on_version_conflict": self.skip_on_version_conflict,
            "replace": self.replace,
            "edges": self.edges.dump(camel_case),
        }


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
    skipped: int = 0
    total: int = 0

    @property
    def calculated_total(self) -> int:
        return self.created + self.deleted + self.changed + self.unchanged + self.skipped


@dataclass
class UploadDeployResult(DeployResult):
    uploaded: int = 0


@dataclass
class DatapointDeployResult(UploadDeployResult):
    cells: int = 0


class DeployResults(UserList):
    def __init__(self, collection: Iterable[DeployResult], action: Literal["deploy", "clean"], dry_run: bool = False):
        super().__init__(collection)
        self.action = action
        self.dry_run = dry_run

    @property
    def has_counts(self) -> bool:
        return any(isinstance(entry, ResourceDeployResult) for entry in self.data)

    @property
    def has_uploads(self) -> bool:
        return any(isinstance(entry, UploadDeployResult) for entry in self.data)

    def counts_table(self) -> Table:
        table = Table(title=f"Summary of Resources {self.action.title()} operation:")
        prefix = "Would have " if self.dry_run else ""
        table.add_column("Resource", justify="right")
        table.add_column(f"{prefix}Created", justify="right", style="green")
        table.add_column(f"{prefix}Deleted", justify="right", style="red")
        table.add_column(f"{prefix}Changed", justify="right", style="magenta")
        table.add_column("Unchanged", justify="right", style="cyan")
        table.add_column("Total", justify="right")
        for item in sorted(
            entry for entry in self.data if entry is not None and isinstance(entry, ResourceDeployResult)
        ):
            table.add_row(
                item.name,
                str(item.created),
                str(item.deleted),
                str(item.changed),
                str(item.unchanged),
                str(item.total),
            )

        return table

    def uploads_table(self) -> Table:
        table = Table(title=f"Summary of Data {self.action.title()} operation:")
        prefix = "Would have " if self.dry_run else ""
        table.add_column("Resource", justify="right")
        table.add_column(f"{prefix}Uploaded", justify="right", style="green")
        table.add_column("Datapoints", justify="right", style="cyan")
        for item in sorted(entry for entry in self.data if entry is not None and isinstance(entry, UploadDeployResult)):
            if isinstance(item, DatapointDeployResult):
                datapoints = f"{item.cells:,}"
            else:
                datapoints = "-"
            table.add_row(
                item.name,
                f"{item.uploaded} files",
                datapoints,
            )

        return table
