from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from cognite.client import CogniteClient
from cognite.client.data_classes._base import (
    CogniteResourceList,
    WriteableCogniteResource,
    WriteableCogniteResourceList,
)
from cognite.client.data_classes.data_modeling import EdgeApplyList, NodeApplyList
from typing_extensions import Self


@dataclass(frozen=True)
class RawTable(WriteableCogniteResource):
    db_name: str
    table_name: str

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> RawTable:
        return cls(db_name=resource["dbName"], table_name=resource["tableName"])

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return {
            "dbName" if camel_case else "db_name": self.db_name,
            "tableName" if camel_case else "table_name": self.table_name,
        }

    def as_write(self) -> RawTable:
        return self


class RawTableList(WriteableCogniteResourceList[RawTable, RawTable]):
    _RESOURCE = RawTable

    def as_write(self) -> CogniteResourceList[RawTable]:
        return self


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
