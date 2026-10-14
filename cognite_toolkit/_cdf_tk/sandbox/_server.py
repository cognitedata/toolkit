"""Stdlib HTTP server around a Workspace. JSON in, JSON out."""

import json
import threading
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

from pydantic import ValidationError

from cognite_toolkit._cdf_tk.exceptions import ToolkitError
from cognite_toolkit._cdf_tk.sandbox._workspace import OPERATIONS, Workspace, insight_to_json

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
            "GET /resources": "List loaded resources: kind, folder, identifier, file.",
            "GET /resources/<Kind>?<identifier fields>[&folder=]": (
                "One resource as resolved YAML, plus extraFiles such as the SQL query of a transformation."
            ),
            "GET /schema/<Kind>[?folder=]": "JSON schema of a resource kind.",
            "POST /apply": "Body: one operation, selected with the op field. Returns ok and insights. ok=false is rolled back.",
            "GET /insights": "Validate the whole session.",
            "GET /diff": "Unified diffs of the pending edits.",
            "POST /write": "Write the pending edits to disk.",
        },
        "operations": {
            name: {"description": (cls.__doc__ or "").strip(), "schema": cls.model_json_schema(by_alias=True)}
            for name, cls in OPERATIONS.items()
        },
        "notes": [TOKEN_HINT, "SetExtra text keeps {{ variable }} tokens. Edit the text returned by GET /resources."],
    }


def make_server(host: str, port: int, workspace: Workspace | None = None) -> ThreadingHTTPServer:
    workspace = workspace or Workspace()
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
            if method == "POST" and path == "/write":
                return HTTPStatus.OK, {"written": [str(file) for file in workspace.get_session().write()]}
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
