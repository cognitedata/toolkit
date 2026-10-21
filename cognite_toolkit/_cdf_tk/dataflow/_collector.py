"""Collects the data flow of the live resources in a CDF project into a Snapshot.

Resource classes are filtered by alpha flags when ``resource_ios`` is imported. A caller that wants the alpha
resource types (for example a Function writing the snapshot) must call
``cognite_toolkit._cdf_tk.sandbox._flags.enable_all_flags()`` before importing this module.
"""

import hashlib
import json
import re
from collections.abc import Iterable
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any

from pydantic import ValidationError

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.dataflow._models import (
    DataFlowEdge,
    DataFlowNode,
    FlowTarget,
    Snapshot,
    SnapshotEdge,
    TargetType,
    make_label,
    resource_node_id,
)
from cognite_toolkit._cdf_tk.dataflow._types import FlowKind, ParseStatus
from cognite_toolkit._cdf_tk.resource_ios import RESOURCE_IO_LIST, ResourceIO
from cognite_toolkit._cdf_tk.resource_ios._data_organization import DataSetsIO
from cognite_toolkit._cdf_tk.resource_ios._datamodel import ContainerIO, DataModelIO, SpaceIO, ViewIO
from cognite_toolkit._cdf_tk.resource_ios._function import FunctionIO
from cognite_toolkit._cdf_tk.resource_ios._raw import RawDatabaseIO, RawTableIO
from cognite_toolkit._cdf_tk.resource_ios._streams import StreamIO
from cognite_toolkit._cdf_tk.resource_ios._transformation import TransformationIO
from cognite_toolkit._cdf_tk.resource_ios._workflow import WorkflowIO
from cognite_toolkit._cdf_tk.utils.sql_sources import SQL_EXTRACTOR_VERSION
from cognite_toolkit._version import __version__

# Structure classes make nodes appear even when no flow edge points at them: unreferenced RAW tables, spaces and views.
DEFAULT_STRUCTURE_CLASSES: tuple[type[ResourceIO], ...] = (
    SpaceIO,
    ContainerIO,
    ViewIO,
    DataModelIO,
    RawDatabaseIO,
    RawTableIO,
    DataSetsIO,
    StreamIO,
    FunctionIO,
    WorkflowIO,
)

SYSTEM_SPACE_PREFIX = "cdf_"
_PLACEHOLDER_AUTHENTICATION = {"clientId": "not-collected", "clientSecret": "not-collected"}
_ERROR_LIMIT = 200
# The request id differs on every call, so it would change the snapshot hash and rewrite the cache each run.
_REQUEST_ID = re.compile(r"\s*\|\s*X-Request-ID:\s*\S+")


@dataclass(frozen=True)
class CollectScope:
    """Bounds what is listed. System spaces (cdf_ prefix) are not listed unless include_system_spaces is set."""

    data_set_external_id: str | None = None
    space: str | None = None
    include_system_spaces: bool = False

    @property
    def is_restricted(self) -> bool:
        return self.data_set_external_id is not None or self.space is not None


def get_flow_classes() -> list[type[ResourceIO]]:
    """The resource classes that override get_data_flow."""
    return [
        crud_cls
        for crud_cls in RESOURCE_IO_LIST
        if crud_cls.get_data_flow.__func__ is not ResourceIO.get_data_flow.__func__  # type: ignore[attr-defined]
    ]


def collect(
    client: ToolkitClient,
    previous: Snapshot | None = None,
    scope: CollectScope | None = None,
    structure_classes: Iterable[type[ResourceIO]] = DEFAULT_STRUCTURE_CLASSES,
) -> Snapshot:
    """Lists the resources of the flow and structure classes and builds the snapshot.

    A resource whose version token is unchanged since the previous snapshot reuses its previous node and edges, which
    skips the dump, the validation and the SQL parsing. Every class is still listed in full. A resource that is no
    longer listed is dropped.
    """
    scope = scope or CollectScope()
    flow_classes = get_flow_classes()
    classes = list(dict.fromkeys([*flow_classes, *structure_classes]))
    # The code that makes the edges changes with the version, so a new version re-reads everything.
    if previous is not None and previous.toolkit_version != __version__:
        previous = None
    collector = _Collector(client, previous, scope, set(flow_classes))
    for crud_cls in classes:
        collector.collect_class(crud_cls)
    return collector.build_snapshot()


class _Collector:
    def __init__(
        self,
        client: ToolkitClient,
        previous: Snapshot | None,
        scope: CollectScope,
        flow_classes: set[type[ResourceIO]],
    ) -> None:
        self.client = client
        self.previous = previous
        self.scope = scope
        self.flow_classes = flow_classes
        self._previous_nodes = previous.node_by_id() if previous else {}
        self._previous_edges: dict[str, list[SnapshotEdge]] = {}
        for edge in previous.edges if previous else []:
            self._previous_edges.setdefault(edge.source, []).append(edge)
        self.nodes: dict[str, DataFlowNode] = {}
        self.edges: list[SnapshotEdge] = []
        self.fresh_stubs: dict[str, DataFlowNode] = {}
        self.listed_types: set[str] = set()
        self.errors: list[str] = []

    def collect_class(self, crud_cls: type[ResourceIO]) -> None:
        node_type = f"{crud_cls.folder_name}/{crud_cls.kind}"
        try:
            crud = crud_cls.create_io(self.client)
            # The listing is materialized, so a failure half-way does not leave a partial class behind.
            responses = list(crud.iterate(data_set_external_id=self.scope.data_set_external_id, space=self.scope.space))
        except Exception as error:
            self.errors.append(f"{node_type}: {_short(error)}")
            self._keep_previous(node_type)
            return
        self.listed_types.add(node_type)
        for response in responses:
            self._collect_resource(crud_cls, crud, response, node_type)

    def _keep_previous(self, node_type: str) -> None:
        """A class that could not be listed keeps its previous nodes, instead of looking deleted."""
        for node in self._previous_nodes.values():
            if node.type == node_type and not node.stub:
                self.nodes[node.id] = node
                self.edges.extend(self._previous_edges.get(node.id, []))

    def _collect_resource(self, crud_cls: type[ResourceIO], crud: ResourceIO, response: Any, node_type: str) -> None:
        try:
            identifier = crud_cls.get_id(response)
        except Exception as error:
            self.errors.append(f"{node_type}: {_short(error)}")
            return
        identifier_dump = identifier.dump()
        if self._is_skipped_system_resource(identifier_dump):
            return
        node_id = resource_node_id(crud_cls, identifier)
        try:
            token = _version_token(crud_cls, response)
            previous_node = self._previous_nodes.get(node_id)
            if previous_node is not None and previous_node.version_token == token and previous_node.status == "ok":
                self.nodes[node_id] = previous_node
                self.edges.extend(self._previous_edges.get(node_id, []))
                return
            edges = self._read_edges(crud_cls, crud, response, node_id)
            parse_status = _parse_status(edges) if crud_cls is TransformationIO else None
            node = DataFlowNode(
                id=node_id,
                type=node_type,
                label=make_label(identifier_dump),
                folder=crud_cls.folder_name,
                kind=crud_cls.kind,
                identifier=identifier_dump,
                internal_id=_internal_id(response),
                version_token=token,
                parse_status=parse_status,
            )
        except Exception as error:
            # One bad resource must not stop the snapshot of the others. No token, so it is retried next time.
            self.nodes[node_id] = DataFlowNode(
                id=node_id,
                type=node_type,
                label=make_label(identifier_dump),
                folder=crud_cls.folder_name,
                kind=crud_cls.kind,
                identifier=identifier_dump,
                status="failed",
                error=_short(error),
            )
            return
        self.nodes[node_id] = node
        self.edges.extend(SnapshotEdge.from_edge(edge) for edge in edges)
        for edge in edges:
            self.fresh_stubs.setdefault(edge.target.node_id, edge.target.as_stub())

    def _read_edges(
        self, crud_cls: type[ResourceIO], crud: ResourceIO, response: Any, node_id: str
    ) -> list[DataFlowEdge]:
        dumped = crud.dump_resource(response)
        resource = _validate(crud_cls, dumped)
        edges: list[DataFlowEdge] = []
        if crud_cls in self.flow_classes:
            edges.extend(crud_cls.get_data_flow(resource))
        for dependency_cls, dependency_id in crud_cls.get_dependencies(resource):
            target = FlowTarget.resource(dependency_cls, dependency_id)
            if target.node_id != node_id:
                edges.append(DataFlowEdge(source=node_id, target=target, kind=FlowKind.REQUIRES))
        return edges

    def _is_skipped_system_resource(self, identifier: dict[str, Any]) -> bool:
        if self.scope.include_system_spaces:
            return False
        space = identifier.get("space")
        return isinstance(space, str) and space.startswith(SYSTEM_SPACE_PREFIX)

    def build_snapshot(self) -> Snapshot:
        nodes = dict(self.nodes)
        referenced = {edge.target for edge in self.edges}
        for target_id in referenced - set(nodes):
            nodes[target_id] = self._stub(target_id)
        return Snapshot.create(
            list(nodes.values()),
            self.edges,
            generated_at=datetime.now(timezone.utc).isoformat(timespec="seconds"),
            toolkit_version=__version__,
            cdf_project=self.client.config.project,
            scope=asdict(self.scope),
            errors=self.errors,
        )

    def _stub(self, target_id: str) -> DataFlowNode:
        candidate = self.fresh_stubs.get(target_id) or self._previous_nodes.get(target_id)
        if candidate is None:
            return DataFlowNode(id=target_id, type="unknown", label=target_id, stub=True)
        stub = DataFlowNode(
            id=candidate.id,
            type=candidate.type,
            label=candidate.label,
            folder=candidate.folder,
            kind=candidate.kind,
            identifier=candidate.identifier,
            stub=True,
        )
        # A dangling edge: the type was listed in full, so a missing resource does not exist. System spaces and
        # scoped runs do not list everything, so nothing is known about those.
        space = (candidate.identifier or {}).get("space")
        is_system = isinstance(space, str) and space.startswith(SYSTEM_SPACE_PREFIX)
        if candidate.type in self.listed_types and not self.scope.is_restricted and not is_system:
            stub.exists = False
        return stub


def _version_token(crud_cls: type[ResourceIO], response: Any) -> str:
    last_updated = getattr(response, "last_updated_time", None)
    if isinstance(last_updated, int) and not isinstance(last_updated, bool):
        token = f"t{last_updated}"
    else:
        content = json.dumps(response.dump(), sort_keys=True, separators=(",", ":"), default=str)
        token = f"h{hashlib.sha256(content.encode()).hexdigest()[:16]}"
    if crud_cls is TransformationIO:
        token += f"|sql{SQL_EXTRACTOR_VERSION}"
    return token


def _internal_id(response: Any) -> int | None:
    value = getattr(response, "id", None)
    return value if isinstance(value, int) and not isinstance(value, bool) else None


def _validate(crud_cls: type[ResourceIO], dumped: dict[str, Any]) -> Any:
    try:
        return crud_cls.validate_object(dumped, extra="ignore")
    except ValidationError as error:
        # The API never returns credentials, but the YAML format of triggers and schedules requires them.
        missing_authentication = "authentication" not in dumped and any(
            item["loc"][:1] == ("authentication",) for item in error.errors()
        )
        if not missing_authentication:
            raise
        return crud_cls.validate_object({**dumped, "authentication": _PLACEHOLDER_AUTHENTICATION}, extra="ignore")


def _parse_status(edges: list[DataFlowEdge]) -> ParseStatus:
    reads = [edge for edge in edges if edge.kind is FlowKind.READS]
    unresolved = [edge for edge in reads if edge.target.type is TargetType.UNRESOLVED]
    if not unresolved:
        return ParseStatus.OK
    return ParseStatus.PARTIAL if len(unresolved) < len(reads) else ParseStatus.FAILED


def _short(error: Exception) -> str:
    message = str(error).strip().splitlines()[0] if str(error).strip() else ""
    return f"{type(error).__name__}: {_REQUEST_ID.sub('', message)}"[:_ERROR_LIMIT]
