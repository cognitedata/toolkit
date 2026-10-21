"""Load several modules of an organization directory into one modeling session."""

import json
from collections import Counter
from pathlib import Path
from typing import Any, Literal

from pydantic import TypeAdapter

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.commands.build_v2.build_v2 import BuildV2Command
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._insights import Insight
from cognite_toolkit._cdf_tk.dataflow import FlowKind, Snapshot, resource_node_id
from cognite_toolkit._cdf_tk.dataflow._io import load_json, load_snapshot
from cognite_toolkit._cdf_tk.rules._base import InternalValidatorException
from cognite_toolkit._cdf_tk.sandbox._operations import (
    AddResource,
    Operation,
    RemovePath,
    RemoveResource,
    SetExtra,
    SetPath,
)
from cognite_toolkit._cdf_tk.sandbox._session import ApplyResult, ModelingSession
from cognite_toolkit._cdf_tk.sandbox._store import CdfScope, CdfStore, CommitResult, StorePlan, YamlStore

OPERATIONS: dict[str, type[Operation]] = {
    cls.__name__: cls for cls in (SetPath, RemovePath, SetExtra, AddResource, RemoveResource)
}
DEFAULT_NODES_LIMIT = 50
MAX_NODES_LIMIT = 500
DEFAULT_SUBGRAPH_LIMIT = 200
MAX_SUBGRAPH_LIMIT = 1000


def _subgraph_limit(limit: int) -> int:
    if limit < 1:
        raise ValueError("limit must be at least 1")
    return min(limit, MAX_SUBGRAPH_LIMIT)


def insight_to_json(insight: Insight | InternalValidatorException) -> dict[str, Any]:
    if isinstance(insight, InternalValidatorException):
        return {"insightType": type(insight).__name__, "message": str(insight)}
    return insight.model_dump(mode="json")


def parse_operation(payload: dict[str, Any]) -> Operation:
    payload = dict(payload)
    name = payload.pop("op", None)
    if name not in OPERATIONS:
        raise ValueError(f"Unknown op {name!r}. Available: {', '.join(sorted(OPERATIONS))}")
    return OPERATIONS[name].model_validate(payload)


class Workspace:
    """The session behind the server. Modules can be added by repeated loads."""

    def __init__(self, client: ToolkitClient | None = None) -> None:
        self.client = client
        self.session: ModelingSession | None = None
        self._loaded_modules: set[Path] = set()
        self.yaml_store = YamlStore()
        self._cdf_store: CdfStore | None = CdfStore(client) if client is not None else None
        self._snapshot: Snapshot | None = None
        self._snapshot_source: Literal["loaded", "collected"] | None = None

    @property
    def cdf_store(self) -> CdfStore:
        if self._cdf_store is None:
            raise ValueError("No CDF client. Start the sandbox with credentials (for example --env-file).")
        return self._cdf_store

    def get_session(self) -> ModelingSession:
        if self.session is None:
            raise ValueError("Nothing is loaded. POST /load first.")
        return self.session

    def load(self, organization_dir: Path, config_yaml: Path | None, modules: list[str]) -> dict[str, Any]:
        organization_dir = organization_dir.resolve()
        build = BuildV2Command.read_file_system(organization_dir, config_yaml, modules)
        if build.selected_modules is None:
            raise ValueError("No modules selected. Pass modules or set environment.selected in the config YAML.")
        scan = BuildV2Command.find_modules(build, "load")
        if scan.non_existing_module_names:
            raise ValueError(f"Modules do not exist: {[item.name for item in scan.non_existing_module_names]}")

        if self.session is None or self.session.root != organization_dir:
            self.session = ModelingSession(root=organization_dir, client=self.client)
            self._loaded_modules = set()

        loaded: list[str] = []
        for module in scan.modules:
            if module.path in self._loaded_modules:
                continue
            self.session.add_module(module)
            self._loaded_modules.add(module.path)
            loaded.append(module.id.as_posix())
        return {"modules": loaded, "counts": self.counts()}

    def counts(self) -> dict[str, int]:
        session = self.get_session()
        return dict(Counter(ref.source.read.source_type.kind.lower() for ref in session.resources()))

    def list_resources(self) -> list[dict[str, Any]]:
        return [
            {
                "kind": ref.source.read.source_type.kind,
                "folder": ref.source.read.source_type.resource_folder,
                "identifier": ref.resource.identifier.dump(),
                "nodeId": resource_node_id(ref.source.crud_cls, ref.resource.identifier),
                "file": str(ref.source.path),
            }
            for ref in self.get_session().resources()
        ]

    def get_resource(self, kind: str, identifier: dict[str, Any], folder: str | None = None) -> dict[str, Any]:
        session = self.get_session()
        ref = session.find(kind, identifier, folder)
        return {
            "resource": ref.source.resolved[ref.index],
            "extraFiles": [
                {"field": field, "file": str(text_file.path), "text": text_file.text}
                for field, text_file in session.extra_texts(ref).items()
            ],
        }

    def get_schema(self, kind: str, folder: str | None = None) -> dict[str, Any]:
        crud_cls = self.get_session().crud_class(kind, folder)
        return TypeAdapter(crud_cls.yaml_cls).json_schema(by_alias=True)

    def apply(self, payload: dict[str, Any]) -> ApplyResult:
        return self.get_session().apply(parse_operation(payload))

    def yaml_plan(self) -> StorePlan:
        return self.yaml_store.plan(self.get_session())

    def yaml_commit(self, *, dry_run: bool = False, confirm: bool = True) -> CommitResult:
        # /write keeps the old behaviour: write for real without an extra confirm flag.
        return self.yaml_store.commit(self.get_session(), dry_run=dry_run, confirm=confirm)

    def cdf_plan(self) -> StorePlan:
        return self.cdf_store.plan(self.get_session())

    def cdf_deploy(self, *, dry_run: bool = True, confirm: bool = False) -> CommitResult:
        return self.cdf_store.commit(self.get_session(), dry_run=dry_run, confirm=confirm)

    def cdf_load(self, body: dict[str, Any]) -> dict[str, Any]:
        """Load resources from CDF into the session as new files (dump)."""
        session = self.get_session()
        scope = CdfScope(
            kind=body["kind"],
            folder=body.get("folder"),
            data_set_external_id=body.get("dataSetExternalId"),
            space=body.get("space"),
            identifiers=body.get("identifiers"),
        )
        if not scope.identifiers and not scope.data_set_external_id and not scope.space:
            raise ValueError("Pass identifiers, dataSetExternalId, or space to bound the CDF load")
        module = body.get("module")
        added: list[dict[str, Any]] = []
        for crud_cls, resource, sidecars in self.cdf_store.read(scope):
            identifier = session.add_resource(
                crud_cls.kind,
                resource,
                folder=crud_cls.folder_name,
                module=module,
                extras=sidecars or None,
                from_cdf=True,
            )
            added.append({"kind": crud_cls.kind, "identifier": identifier.dump()})
        return {"added": added, "counts": self.counts()}

    def cdf_pull(self, body: dict[str, Any] | None = None) -> dict[str, Any]:
        """Merge CDF values into resources already loaded in the session."""
        body = body or {}
        return self.cdf_store.merge_into_session(
            self.get_session(),
            kind=body.get("kind"),
            identifier=body.get("identifier"),
            folder=body.get("folder"),
        )

    def get_snapshot(self) -> Snapshot:
        if self._snapshot is None:
            raise ValueError("No data flow snapshot. POST /dataflow/load, or GET /dataflow?refresh=true with a client.")
        return self._snapshot

    def dataflow_load(self, body: dict[str, Any]) -> dict[str, Any]:
        """Load a prebuilt snapshot. Only the models are needed, so this works without sqlglot."""
        if "path" in body:
            snapshot = load_snapshot(Path(body["path"]))
        elif "json" in body:
            payload = body["json"]
            snapshot = load_json(payload if isinstance(payload, str) else json.dumps(payload))
        else:
            raise ValueError("Pass path (a local file) or json (the snapshot, inline)")
        self._snapshot = snapshot
        self._snapshot_source = "loaded"
        return {"source": self._snapshot_source, "summary": snapshot.summary()}

    def dataflow(self, refresh: bool = False) -> dict[str, Any]:
        if refresh:
            # Collecting lists every resource type in CDF, so it is only done on request.
            from cognite_toolkit._cdf_tk.dataflow._collector import collect

            self._snapshot = collect(self.cdf_store.client, previous=self._snapshot)
            self._snapshot_source = "collected"
        snapshot = self.get_snapshot()
        return {
            "source": self._snapshot_source,
            "summary": snapshot.summary(),
            **snapshot.model_dump(mode="json", by_alias=True),
        }

    def dataflow_nodes(
        self,
        q: str | None = None,
        kind: str | None = None,
        folder: str | None = None,
        node_type: str | None = None,
        limit: int = DEFAULT_NODES_LIMIT,
        offset: int = 0,
    ) -> dict[str, Any]:
        if limit < 1 or offset < 0:
            raise ValueError("limit must be at least 1 and offset at least 0")
        page = self.get_snapshot().search(q, kind, folder, node_type, min(limit, MAX_NODES_LIMIT), offset)
        return page.model_dump(mode="json", by_alias=True)

    def dataflow_neighbors(
        self,
        node_id: str,
        direction: str = "both",
        depth: int = 1,
        kinds: str | None = None,
        limit: int = DEFAULT_SUBGRAPH_LIMIT,
    ) -> dict[str, Any]:
        if direction not in ("in", "out", "both"):
            raise ValueError("direction must be in, out or both")
        selected = {FlowKind(kind.strip()) for kind in kinds.split(",") if kind.strip()} if kinds else None
        subgraph = self.get_snapshot().neighbors(node_id, direction, depth, selected, _subgraph_limit(limit))
        return subgraph.model_dump(mode="json", by_alias=True)

    def dataflow_lineage(
        self,
        node_id: str,
        direction: str = "both",
        depth: int | None = None,
        limit: int = DEFAULT_SUBGRAPH_LIMIT,
        only: str | None = None,
        orchestration: bool = True,
    ) -> dict[str, Any]:
        if direction not in ("upstream", "downstream", "both"):
            raise ValueError("direction must be upstream, downstream or both")
        if only not in (None, "processes"):
            raise ValueError("only must be processes")
        lineage = self.get_snapshot().lineage(
            node_id,
            direction,  # type: ignore[arg-type]
            depth,
            _subgraph_limit(limit),
            orchestration,
            only_processes=only == "processes",
        )
        return lineage.model_dump(mode="json", by_alias=True)

    def dataflow_node_id(self, kind: str, folder: str | None, identifier: dict[str, Any]) -> dict[str, Any]:
        """The node id of a resource, also when it is not loaded in the session."""
        crud_cls = ModelingSession.crud_class(kind, folder)
        node_id = resource_node_id(crud_cls, crud_cls.get_id(identifier))
        return {
            "id": node_id,
            "kind": crud_cls.kind,
            "folder": crud_cls.folder_name,
            "identifier": identifier,
            "inSnapshot": self._snapshot is not None and node_id in self._snapshot.node_by_id(),
        }

    def dataflow_node(self, node_id: str) -> dict[str, Any]:
        return self.get_snapshot().node_with_edges(node_id)
