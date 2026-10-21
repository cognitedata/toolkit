"""Stdlib HTTP server around a Workspace. JSON in, JSON out."""

import json
import threading
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

from pydantic import ValidationError

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.exceptions import ToolkitError
from cognite_toolkit._cdf_tk.sandbox._workspace import (
    DEFAULT_NODES_LIMIT,
    DEFAULT_SUBGRAPH_LIMIT,
    OPERATIONS,
    Workspace,
    insight_to_json,
)

TOKEN_HINT = (
    "The value contains a {{ variable }} token. Values in SetPath and AddResource are validated unresolved, "
    "so tokens only work where the raw string is valid for the field (for example not in a space or externalId "
    "that must be short). Otherwise pass the resolved value and fix the token by hand afterwards."
)


def describe_api() -> dict[str, Any]:
    return {
        "routes": {
            "POST /load": (
                "Body: organizationDir (absolute), configYaml (absolute), modules "
                "(paths relative to organizationDir, for example modules/my_module). "
                "A module is a folder holding resource folders such as data_modeling or transformations. "
                "Can be called again to add modules."
            ),
            "GET /resources": "List loaded resources: kind, folder, identifier, nodeId (the data flow node id), file.",
            "GET /resources/<Kind>?<identifier fields>[&folder=]": (
                "One resource as resolved YAML, plus extraFiles such as the SQL query of a transformation."
            ),
            "GET /schema/<Kind>[?folder=]": "JSON schema of a resource kind.",
            "POST /apply": "Body: one operation, selected with the op field. Returns ok and insights. ok=false is rolled back.",
            "GET /insights": "Validate the whole session.",
            "GET /diff": "Unified diffs of the pending edits.",
            "GET /changes": (
                "Files with the text to compare them against: path, origin (cdf or sandbox), baseline, current, "
                "state (new, changed, unchanged). A cdf baseline is the file as dumped or pulled from CDF."
            ),
            "POST /write": "Write the pending edits to disk.",
            "GET /cdf/diff": "Compare the session to CDF (create / update / delete / unchanged). Requires a client.",
            "POST /cdf/deploy": (
                "Deploy the session to CDF. Body: dryRun (default true), confirm (required when dryRun is false)."
            ),
            "POST /cdf/load": (
                "Dump resources from CDF into the session as new files. Body: kind, and one of identifiers, "
                "dataSetExternalId, or space. Optional: folder, module."
            ),
            "POST /cdf/pull": (
                "Merge CDF values into loaded session resources, keeping variables and comments. "
                "Optional body: kind, identifier, folder."
            ),
            "POST /dataflow/load": (
                "Load a prebuilt data flow snapshot. Body: path (a local file) or json (the snapshot, inline). "
                "Needs no client and no sqlglot."
            ),
            "GET /dataflow[?refresh=true]": (
                "The loaded data flow snapshot (summary, nodes, edges) and its source (loaded or collected). "
                "refresh=true collects it from CDF, which lists every resource type. Requires a client."
            ),
            "GET /dataflow/nodes?q=&kind=&folder=&type=&limit=&offset=": (
                "Search or list nodes. q is a case-insensitive substring of the label, id or identifier values. "
                "kind is a resource kind such as View, folder a resource folder such as data_modeling, type is "
                "<folder>/<kind> or collection, external, unresolved. limit defaults to 50 (max 500). "
                "Returns total, offset, limit, counts per type over all matches, and nodes."
            ),
            "GET /dataflow/neighbors?id=&direction=&depth=&kinds=&limit=": (
                "The subgraph around a node. direction is in, out or both (default both), depth defaults to 1, "
                "kinds is a comma separated list of reads, writes, triggers, contains, requires. "
                "limit keeps the nearest nodes, default 200 and clamped to 1000. "
                "truncated and totalNodes tell if nodes were left out."
            ),
            "GET /dataflow/lineage?id=&direction=&depth=&limit=&only=&orchestration=": (
                "The data flow into (upstream) and out of (downstream) a node, following reads and writes edges. "
                "direction is upstream, downstream or both (default both), depth is unlimited by default, "
                "limit as for neighbors. Steps have a distance and a role (process or data). "
                "only=processes returns only process steps. orchestration (default true) adds the workflow "
                "versions and triggers that run each process. incomplete is true if an unresolved source "
                "or a partly parsed query is part of the flow."
            ),
            "GET /dataflow/node?id=": "One node with its incoming and outgoing edges.",
            "GET /dataflow/node-id?kind=&folder=&<identifier fields>": (
                "The node id of a resource, loaded in the session or not, and whether it is in the snapshot."
            ),
        },
        "operations": {
            name: {"description": (cls.__doc__ or "").strip(), "schema": cls.model_json_schema(by_alias=True)}
            for name, cls in OPERATIONS.items()
        },
        "notes": [TOKEN_HINT, "SetExtra text keeps {{ variable }} tokens. Edit the text returned by GET /resources."],
    }


def make_server(
    host: str, port: int, workspace: Workspace | None = None, client: ToolkitClient | None = None
) -> ThreadingHTTPServer:
    workspace = workspace or Workspace(client=client)
    lock = threading.Lock()

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            self._dispatch("GET")

        def do_POST(self) -> None:
            self._dispatch("POST")

        def _dispatch(self, method: str) -> None:
            url = urlparse(self.path)
            query = {key: values[0] for key, values in parse_qs(url.query).items()}
            try:
                body = self._read_body() if method == "POST" else {}
                with lock:
                    status, payload = self._route(method, url.path, query, body)
            except (ValueError, KeyError, TypeError, ValidationError, OSError, ToolkitError) as error:
                status, payload = HTTPStatus.BAD_REQUEST, {"error": str(error)}
            self._send(status, payload)

        def _route(self, method: str, path: str, query: dict[str, str], body: dict[str, Any]) -> tuple[HTTPStatus, Any]:
            if method == "POST" and path == "/load":
                config_yaml = body.get("configYaml")
                result = workspace.load(
                    Path(body["organizationDir"]),
                    Path(config_yaml) if config_yaml else None,
                    body.get("modules", []),
                )
                return HTTPStatus.OK, result
            if method == "GET" and path == "/":
                return HTTPStatus.OK, describe_api()
            if method == "GET" and path == "/resources":
                return HTTPStatus.OK, workspace.list_resources()
            if method == "GET" and path.startswith("/resources/"):
                kind = path.removeprefix("/resources/")
                identifier = {key: value for key, value in query.items() if key != "folder"}
                return HTTPStatus.OK, workspace.get_resource(kind, identifier, query.get("folder"))
            if method == "GET" and path.startswith("/schema/"):
                return HTTPStatus.OK, workspace.get_schema(path.removeprefix("/schema/"), query.get("folder"))
            if method == "POST" and path == "/apply":
                result = workspace.apply(body)
                payload: dict[str, Any] = {"ok": result.ok, "insights": [insight_to_json(i) for i in result.insights]}
                if not result.ok and "{{" in json.dumps(body.get("value")):
                    payload["hint"] = TOKEN_HINT
                return HTTPStatus.OK, payload
            if method == "GET" and path == "/insights":
                insights = workspace.get_session().validate()
                return HTTPStatus.OK, {"insights": [insight_to_json(i) for i in insights]}
            if method == "GET" and path == "/diff":
                return HTTPStatus.OK, {str(file): diff for file, diff in workspace.get_session().diff().items()}
            if method == "GET" and path == "/changes":
                return HTTPStatus.OK, {"files": workspace.get_session().changes()}
            if method == "POST" and path == "/write":
                return HTTPStatus.OK, {"written": [str(file) for file in workspace.get_session().write()]}
            if method == "GET" and path == "/cdf/diff":
                return HTTPStatus.OK, workspace.cdf_plan().to_json()
            if method == "POST" and path == "/cdf/deploy":
                return HTTPStatus.OK, workspace.cdf_deploy(
                    dry_run=body.get("dryRun", True),
                    confirm=body.get("confirm", False),
                ).to_json()
            if method == "POST" and path == "/cdf/load":
                return HTTPStatus.OK, workspace.cdf_load(body)
            if method == "POST" and path == "/cdf/pull":
                return HTTPStatus.OK, workspace.cdf_pull(body)
            if method == "POST" and path == "/dataflow/load":
                return HTTPStatus.OK, workspace.dataflow_load(body)
            if method == "GET" and path == "/dataflow":
                return HTTPStatus.OK, workspace.dataflow(refresh=query.get("refresh") == "true")
            if method == "GET" and path == "/dataflow/neighbors":
                return HTTPStatus.OK, workspace.dataflow_neighbors(
                    query["id"],
                    query.get("direction", "both"),
                    int(query.get("depth", 1)),
                    query.get("kinds"),
                    int(query.get("limit", DEFAULT_SUBGRAPH_LIMIT)),
                )
            if method == "GET" and path == "/dataflow/nodes":
                return HTTPStatus.OK, workspace.dataflow_nodes(
                    query.get("q"),
                    query.get("kind"),
                    query.get("folder"),
                    query.get("type"),
                    int(query.get("limit", DEFAULT_NODES_LIMIT)),
                    int(query.get("offset", 0)),
                )
            if method == "GET" and path == "/dataflow/node":
                return HTTPStatus.OK, workspace.dataflow_node(query["id"])
            if method == "GET" and path == "/dataflow/node-id":
                identifier = {key: value for key, value in query.items() if key not in ("kind", "folder")}
                return HTTPStatus.OK, workspace.dataflow_node_id(query["kind"], query.get("folder"), identifier)
            if method == "GET" and path == "/dataflow/lineage":
                return HTTPStatus.OK, workspace.dataflow_lineage(
                    query["id"],
                    query.get("direction", "both"),
                    int(query["depth"]) if "depth" in query else None,
                    int(query.get("limit", DEFAULT_SUBGRAPH_LIMIT)),
                    query.get("only"),
                    query.get("orchestration", "true") != "false",
                )
            return HTTPStatus.NOT_FOUND, {"error": f"No route for {method} {path}"}

        def _read_body(self) -> dict[str, Any]:
            length = int(self.headers.get("Content-Length") or 0)
            if length == 0:
                return {}
            body = json.loads(self.rfile.read(length))
            if not isinstance(body, dict):
                raise ValueError("Request body must be a JSON object")
            return body

        def _send(self, status: HTTPStatus, payload: Any) -> None:
            data = json.dumps(payload, default=str).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def log_message(self, format: str, *args: Any) -> None:
            pass

    return ThreadingHTTPServer((host, port), Handler)
