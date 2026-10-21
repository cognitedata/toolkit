"""Light data flow models.

An edge reads "subject verb object": a transformation reads a RAW table, a transformation writes a view,
a workflow contains a transformation, a trigger triggers a workflow version and a view requires a container.
A contains edge may carry depends_on: the node ids of the contained resources that run before this one.
Viewers derive the display direction (source to consumption) from the kind.
"""

import hashlib
import json
from collections import Counter, deque
from enum import Enum
from typing import TYPE_CHECKING, Any, Literal, Self, TypeVar

from pydantic import BaseModel, ConfigDict, Field
from pydantic.alias_generators import to_camel

from cognite_toolkit._cdf_tk.dataflow._types import Confidence, FlowKind, ParseStatus

if TYPE_CHECKING:
    from cognite_toolkit._cdf_tk.client._resource_base import Identifier
    from cognite_toolkit._cdf_tk.resource_ios._base_ios import BaseResourceIO

SCHEMA_VERSION = 1
_LABEL_LIMIT = 80
_INCOMPLETE_STATUSES = (ParseStatus.PARTIAL, ParseStatus.FAILED)


class _CamelModel(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)


class TargetType(str, Enum):
    RESOURCE = "resource"
    COLLECTION = "collection"
    EXTERNAL = "external"
    UNRESOLVED = "unresolved"


def make_node_id(folder: str, kind: str, identifier: dict[str, Any]) -> str:
    # The type key of data modeling identifiers is not always set, so it is left out to keep the id stable.
    canonical = {key: value for key, value in identifier.items() if key != "type"}
    return f"{folder}/{kind}/{json.dumps(canonical, sort_keys=True, separators=(',', ':'))}"


def make_label(identifier: dict[str, Any]) -> str:
    if set(identifier) == {"dbName", "name"}:
        return f"{identifier['dbName']}.{identifier['name']}"
    version = identifier.get("version")
    parts = [str(value) for key, value in identifier.items() if key != "version"]
    label = ":".join(parts)
    if version is not None:
        label += f"(v{version})"
    return label


def resource_node_id(crud_cls: "type[BaseResourceIO]", identifier: "Identifier") -> str:
    return make_node_id(crud_cls.folder_name, crud_cls.kind, identifier.dump())


class FlowTarget(_CamelModel):
    """What an edge points at: a resource, a collection of resources, an external object or something unresolved."""

    model_config = ConfigDict(frozen=True)

    type: TargetType
    folder: str | None = None
    kind: str | None = None
    identifier: dict[str, Any] | None = None
    # Collection name (for example _cdf.assets) or external URI.
    name: str | None = None
    # Unresolved only. The owner keeps the stubs of different resources apart.
    fragment: str | None = None
    owner: str | None = None

    def __hash__(self) -> int:
        return hash(self.node_id)

    @classmethod
    def resource(cls, crud_cls: "type[BaseResourceIO]", identifier: "Identifier") -> "FlowTarget":
        return cls(
            type=TargetType.RESOURCE,
            folder=crud_cls.folder_name,
            kind=crud_cls.kind,
            identifier=identifier.dump(),
        )

    @classmethod
    def collection(cls, name: str) -> "FlowTarget":
        return cls(type=TargetType.COLLECTION, name=name)

    @classmethod
    def external(cls, uri: str) -> "FlowTarget":
        return cls(type=TargetType.EXTERNAL, name=uri)

    @classmethod
    def unresolved(cls, owner: str, fragment: str) -> "FlowTarget":
        return cls(type=TargetType.UNRESOLVED, owner=owner, fragment=fragment)

    @property
    def node_id(self) -> str:
        if self.type is TargetType.RESOURCE:
            return make_node_id(self.folder or "", self.kind or "", self.identifier or {})
        if self.type is TargetType.UNRESOLVED:
            digest = hashlib.sha256((self.fragment or "").encode()).hexdigest()[:12]
            return f"unresolved/{self.owner}/{digest}"
        return f"{self.type.value}/{self.name}"

    @property
    def label(self) -> str:
        if self.type is TargetType.RESOURCE:
            return make_label(self.identifier or {})
        if self.type is TargetType.UNRESOLVED:
            return _shorten(" ".join((self.fragment or "").split()))
        return self.name or ""

    def as_stub(self) -> "DataFlowNode":
        """A node for a target that was not collected as a resource itself."""
        return DataFlowNode(
            id=self.node_id,
            type=self.type.value if self.type is not TargetType.RESOURCE else f"{self.folder}/{self.kind}",
            label=self.label,
            folder=self.folder,
            kind=self.kind,
            identifier=self.identifier,
            stub=True,
        )


def _shorten(text: str) -> str:
    return text if len(text) <= _LABEL_LIMIT else text[:_LABEL_LIMIT] + "..."


class DataFlowEdge(_CamelModel):
    """An edge as yielded by ResourceIO.get_data_flow. The source is the node id of the yielding resource."""

    model_config = ConfigDict(frozen=True)

    source: str
    target: FlowTarget
    kind: FlowKind
    confidence: Confidence = Confidence.HIGH
    note: str | None = None
    depends_on: tuple[str, ...] | None = None


class SnapshotEdge(_CamelModel):
    model_config = ConfigDict(frozen=True)

    source: str
    target: str
    kind: FlowKind
    confidence: Confidence = Confidence.HIGH
    note: str | None = None
    depends_on: tuple[str, ...] | None = None

    @classmethod
    def from_edge(cls, edge: DataFlowEdge) -> "SnapshotEdge":
        return cls(
            source=edge.source,
            target=edge.target.node_id,
            kind=edge.kind,
            confidence=edge.confidence,
            note=edge.note,
            depends_on=edge.depends_on,
        )

    def sort_key(self) -> tuple[str, str, str]:
        return self.source, self.target, self.kind.value


class DataFlowNode(_CamelModel):
    id: str
    # "<folder>/<kind>" for resources, otherwise the target type.
    type: str
    label: str
    folder: str | None = None
    kind: str | None = None
    identifier: dict[str, Any] | None = None
    # The numeric id is only used for links to Fusion. Many resources do not have one.
    internal_id: int | None = None
    version_token: str | None = None
    parse_status: ParseStatus | None = None
    # A stub is referenced by an edge, but was not collected as a resource of its own.
    stub: bool = False
    # False for a stub whose type was listed and which was not found, that is a dangling edge.
    exists: bool = True
    status: Literal["ok", "failed"] = "ok"
    error: str | None = None


T_Snapshot = TypeVar("T_Snapshot", bound="Snapshot")


class Snapshot(_CamelModel):
    schema_version: int = SCHEMA_VERSION
    generated_at: str | None = None
    toolkit_version: str | None = None
    cdf_project: str | None = None
    scope: dict[str, Any] = Field(default_factory=dict)
    nodes: list[DataFlowNode] = Field(default_factory=list)
    edges: list[SnapshotEdge] = Field(default_factory=list)
    # Resource types that could not be listed. Their previous nodes are kept.
    errors: list[str] = Field(default_factory=list)

    @classmethod
    def create(
        cls,
        nodes: list[DataFlowNode],
        edges: list[SnapshotEdge],
        *,
        generated_at: str | None = None,
        toolkit_version: str | None = None,
        cdf_project: str | None = None,
        scope: dict[str, Any] | None = None,
        errors: list[str] | None = None,
    ) -> Self:
        """Sorts and removes duplicates, so equal content gives equal JSON."""
        unique_nodes = {node.id: node for node in nodes}
        unique_edges = {(edge.source, edge.target, edge.kind): edge for edge in edges}
        return cls(
            generated_at=generated_at,
            toolkit_version=toolkit_version,
            cdf_project=cdf_project,
            scope=scope or {},
            nodes=[unique_nodes[key] for key in sorted(unique_nodes)],
            edges=sorted(unique_edges.values(), key=SnapshotEdge.sort_key),
            errors=sorted(set(errors or [])),
        )

    def content_hash(self) -> str:
        """Hash of the content that excludes when and with which version the snapshot was generated."""
        content = self.model_dump(mode="json", by_alias=True, exclude={"generated_at", "toolkit_version"})
        return hashlib.sha256(_canonical_json(content).encode()).hexdigest()

    def node_by_id(self) -> dict[str, DataFlowNode]:
        return {node.id: node for node in self.nodes}

    def summary(self) -> dict[str, Any]:
        unresolved = sum(
            1 for node in self.nodes if node.parse_status in (ParseStatus.PARTIAL, ParseStatus.FAILED) and not node.stub
        )
        failed = sum(1 for node in self.nodes if node.status == "failed")
        stubs = sum(1 for node in self.nodes if node.stub)
        messages: list[str] = []
        if unresolved:
            messages.append(f"{unresolved} transformations have unresolved sources")
        if failed:
            messages.append(f"{failed} resources could not be read")
        if self.errors:
            messages.append(f"{len(self.errors)} resource types could not be listed")
        return {
            "nodes": len(self.nodes),
            "edges": len(self.edges),
            "stubs": stubs,
            "failed": failed,
            "unresolvedTransformations": unresolved,
            "messages": messages,
        }

    def search(
        self,
        q: str | None = None,
        kind: str | None = None,
        folder: str | None = None,
        node_type: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> "NodePage":
        """Nodes matching all filters. The counts per type are over all matches, before paging."""
        needle = q.lower() if q else None
        matches: list[tuple[int, str, str, DataFlowNode]] = []
        for node in self.nodes:
            if kind is not None and (node.kind or "").lower() != kind.lower():
                continue
            if folder is not None and node.folder != folder:
                continue
            if node_type is not None and node.type != node_type:
                continue
            label = node.label.lower()
            if needle is not None:
                haystack = " ".join(
                    [label, node.id.lower(), *(str(value).lower() for value in (node.identifier or {}).values())]
                )
                if needle not in haystack:
                    continue
            prefix_match = 0 if needle is None or label.startswith(needle) else 1
            matches.append((prefix_match, label, node.id, node))
        matches.sort(key=lambda match: match[:3])
        return NodePage(
            total=len(matches),
            offset=offset,
            limit=limit,
            counts=dict(Counter(match[3].type for match in matches)),
            nodes=[match[3] for match in matches[offset : offset + limit]],
        )

    def neighbors(
        self,
        node_id: str,
        direction: Literal["in", "out", "both"] = "both",
        depth: int = 1,
        kinds: set[FlowKind] | None = None,
        limit: int | None = None,
    ) -> "Subgraph":
        """The subgraph within depth edges of a node. Out follows edges from subject to object.

        With a limit, the nodes nearest to the root are kept (the root always is) and edges to dropped nodes are left out.
        """
        if depth < 1:
            raise ValueError("depth must be at least 1")
        nodes = self.node_by_id()
        if node_id not in nodes:
            raise KeyError(f"Unknown node {node_id!r}")
        edges = [edge for edge in self.edges if kinds is None or edge.kind in kinds]
        outgoing: dict[str, list[SnapshotEdge]] = {}
        incoming: dict[str, list[SnapshotEdge]] = {}
        for edge in edges:
            outgoing.setdefault(edge.source, []).append(edge)
            incoming.setdefault(edge.target, []).append(edge)

        order = [node_id]
        seen = {node_id}
        selected: list[SnapshotEdge] = []
        queue: deque[tuple[str, int]] = deque([(node_id, 0)])
        while queue:
            current, distance = queue.popleft()
            if distance >= depth:
                continue
            followed: list[tuple[SnapshotEdge, str]] = []
            if direction in ("out", "both"):
                followed.extend((edge, edge.target) for edge in outgoing.get(current, []))
            if direction in ("in", "both"):
                followed.extend((edge, edge.source) for edge in incoming.get(current, []))
            for edge, other in followed:
                selected.append(edge)
                if other not in seen:
                    seen.add(other)
                    order.append(other)
                    queue.append((other, distance + 1))
        reachable = [key for key in order if key in nodes]
        kept = _cut(reachable, limit)
        return self._subgraph(
            Subgraph, kept, selected, truncated=len(kept) < len(reachable), total_nodes=len(reachable)
        )

    def lineage(
        self,
        node_id: str,
        direction: Literal["upstream", "downstream", "both"] = "both",
        depth: int | None = None,
        limit: int | None = None,
        orchestration: bool = True,
        only_processes: bool = False,
    ) -> "Lineage":
        """The processes and data that feed a node (upstream) and that consume it (downstream).

        Only reads and writes edges are data flow. Data moves from the object of a reads edge to its subject, and
        from the subject of a writes edge to its object. The limit keeps the nodes nearest to the root.
        """
        if depth is not None and depth < 1:
            raise ValueError("depth must be at least 1")
        nodes = self.node_by_id()
        if node_id not in nodes:
            raise KeyError(f"Unknown node {node_id!r}")

        # The subject of a reads or writes edge is a process, for example a transformation.
        processes: set[str] = set()
        forward: dict[str, list[tuple[str, SnapshotEdge]]] = {}
        backward: dict[str, list[tuple[str, SnapshotEdge]]] = {}
        for edge in self.edges:
            if edge.kind is FlowKind.READS:
                origin, destination = edge.target, edge.source
            elif edge.kind is FlowKind.WRITES:
                origin, destination = edge.source, edge.target
            else:
                continue
            processes.add(edge.source)
            forward.setdefault(origin, []).append((destination, edge))
            backward.setdefault(destination, []).append((origin, edge))

        upstream, upstream_edges = _walk(node_id, backward, depth) if direction != "downstream" else ([], [])
        downstream, downstream_edges = _walk(node_id, forward, depth) if direction != "upstream" else ([], [])

        reachable = [node_id]
        for step_id, _ in sorted([*upstream, *downstream], key=lambda step: step[1]):
            if step_id in nodes and step_id not in reachable:
                reachable.append(step_id)
        kept = _cut(reachable, limit)
        kept_ids = set(kept)

        def steps(found: list[tuple[str, int]]) -> list[LineageStep]:
            result = [
                LineageStep(id=step_id, distance=distance, role="process" if step_id in processes else "data")
                for step_id, distance in found
                if step_id in kept_ids
            ]
            return [step for step in result if step.role == "process"] if only_processes else result

        upstream_steps, downstream_steps = steps(upstream), steps(downstream)
        visible = (
            [node_id, *dict.fromkeys(step.id for step in [*upstream_steps, *downstream_steps])]
            if only_processes
            else kept
        )
        edges = [*upstream_edges, *downstream_edges]

        runs: dict[str, list[str]] = {}
        if orchestration:
            incoming: dict[str, list[SnapshotEdge]] = {}
            for edge in self.edges:
                if edge.kind in (FlowKind.CONTAINS, FlowKind.TRIGGERS):
                    incoming.setdefault(edge.target, []).append(edge)
            visible_processes = [
                node_id,
                *(step.id for step in [*upstream_steps, *downstream_steps] if step.role == "process"),
            ]
            for process_id in dict.fromkeys(visible_processes):
                if process_id not in processes:
                    continue
                found, used = _walk_orchestrators(process_id, incoming)
                found = [found_id for found_id in found if found_id in nodes]
                if found:
                    runs[process_id] = found
                    edges.extend(used)
        all_ids = list(dict.fromkeys([*visible, *(found_id for found in runs.values() for found_id in found)]))

        return self._subgraph(
            Lineage,
            all_ids,
            edges,
            truncated=len(kept) < len(reachable),
            total_nodes=len(reachable),
            root=node_id,
            upstream=upstream_steps,
            downstream=downstream_steps,
            orchestration=runs,
            incomplete=any(
                nodes[key].type == TargetType.UNRESOLVED.value or nodes[key].parse_status in _INCOMPLETE_STATUSES
                for key in kept
            ),
        )

    def _subgraph(self, cls: type[T_Snapshot], ids: list[str], edges: list[SnapshotEdge], **fields: Any) -> T_Snapshot:
        nodes = self.node_by_id()
        keep = set(ids)
        subgraph = cls.create(
            [nodes[key] for key in ids],
            [edge for edge in edges if edge.source in keep and edge.target in keep],
            generated_at=self.generated_at,
            toolkit_version=self.toolkit_version,
            cdf_project=self.cdf_project,
            scope=self.scope,
            errors=self.errors,
        )
        return subgraph.model_copy(update=fields)

    def node_with_edges(self, node_id: str) -> dict[str, Any]:
        node = self.node_by_id().get(node_id)
        if node is None:
            raise KeyError(f"Unknown node {node_id!r}")
        return {
            "node": node.model_dump(mode="json", by_alias=True),
            "outgoing": [edge.model_dump(mode="json", by_alias=True) for edge in self.edges if edge.source == node_id],
            "incoming": [edge.model_dump(mode="json", by_alias=True) for edge in self.edges if edge.target == node_id],
        }


class Subgraph(Snapshot):
    """A part of a snapshot. Truncated is set when nodes were left out because of a limit."""

    truncated: bool = False
    total_nodes: int = 0


class LineageStep(_CamelModel):
    id: str
    distance: int
    role: Literal["process", "data"]


class Lineage(Subgraph):
    root: str = ""
    upstream: list[LineageStep] = Field(default_factory=list)
    downstream: list[LineageStep] = Field(default_factory=list)
    # Process id to the workflow versions and triggers that run it.
    orchestration: dict[str, list[str]] = Field(default_factory=dict)
    # An unresolved source or a partly parsed query is part of the lineage, so links may be missing.
    incomplete: bool = False


class NodePage(_CamelModel):
    total: int
    offset: int
    limit: int
    counts: dict[str, int]
    nodes: list[DataFlowNode]


def _cut(ordered_ids: list[str], limit: int | None) -> list[str]:
    return ordered_ids if limit is None else ordered_ids[: max(limit, 1)]


def _walk(
    start: str, adjacency: dict[str, list[tuple[str, SnapshotEdge]]], depth: int | None
) -> tuple[list[tuple[str, int]], list[SnapshotEdge]]:
    """Breadth first from start. Returns the nodes reached with their distance, in order, and the edges followed."""
    seen = {start}
    steps: list[tuple[str, int]] = []
    followed: list[SnapshotEdge] = []
    queue: deque[tuple[str, int]] = deque([(start, 0)])
    while queue:
        current, distance = queue.popleft()
        if depth is not None and distance >= depth:
            continue
        for other, edge in adjacency.get(current, []):
            followed.append(edge)
            if other not in seen:
                seen.add(other)
                steps.append((other, distance + 1))
                queue.append((other, distance + 1))
    return steps, followed


def _walk_orchestrators(
    process_id: str, incoming: dict[str, list[SnapshotEdge]]
) -> tuple[list[str], list[SnapshotEdge]]:
    """What runs a process, followed transitively: trigger to workflow version to transformation."""
    seen = {process_id}
    found: list[str] = []
    followed: list[SnapshotEdge] = []
    queue = deque([process_id])
    while queue:
        current = queue.popleft()
        for edge in incoming.get(current, []):
            followed.append(edge)
            if edge.source not in seen:
                seen.add(edge.source)
                found.append(edge.source)
                queue.append(edge.source)
    return found, followed


def _canonical_json(content: Any) -> str:
    return json.dumps(content, sort_keys=True, separators=(",", ":"))
