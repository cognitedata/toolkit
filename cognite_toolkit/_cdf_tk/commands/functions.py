import os
import re
from dataclasses import dataclass
from pathlib import Path

import questionary
from questionary import Choice
from rich import print

from cognite_toolkit._cdf_tk.commands._base import ToolkitCommand


@dataclass(slots=True)
class Route:
    path: str
    description: str
    has_body: bool


def _sanitize_route_path(path: str) -> str:
    """Normalise a user-supplied route path to lowercase-with-hyphens.

    - Lowercases the path
    - Replaces underscores, spaces, and dots with hyphens
    - Strips any remaining non-alphanumeric characters (keeping ``/`` and ``-``)
    - Collapses consecutive hyphens
    - Ensures a leading ``/``
    """
    path = path.lower()
    path = re.sub(r"[_\s.]+", "-", path)
    path = re.sub(r"[^a-z0-9/\-]", "", path)
    path = re.sub(r"-+", "-", path)
    path = re.sub(r"/+", "/", path)
    path = path.strip("-/")
    return f"/{path}"


def _path_to_func_name(path: str) -> str:
    """Convert /some-route/action → some_route_action."""
    name = re.sub(r"[^a-zA-Z0-9]", "_", path.lstrip("/"))
    return re.sub(r"_+", "_", name).strip("_")


def _path_to_model_name(path: str) -> str:
    """Convert /some-route/action → SomeRouteActionRequest."""
    parts = _path_to_func_name(path).split("_")
    return "".join(p.capitalize() for p in parts if p) + "Request"


_REQUIREMENTS_TXT = "cognite-function-apps[tracing]>=0.9.0\n"
_TRACING_ENV_VAR = "TRACING_API_KEY"
_DEFAULT_TRACING_BACKEND = "eu.honeycomb"

_TRACING_BACKENDS = [
    Choice(title="No tracing", value=""),
    Choice(title="eu.honeycomb  — Honeycomb EU (free tier available)", value="eu.honeycomb"),
    Choice(title="honeycomb     — Honeycomb US (free tier available)", value="honeycomb"),
    Choice(title="lightstep     — Lightstep / ServiceNow", value="lightstep"),
]

_SIGNUP_URLS: dict[str, str] = {
    "eu.honeycomb": "https://ui.eu1.honeycomb.io/signup",
    "honeycomb": "https://ui.honeycomb.io/signup",
    "lightstep": "https://docs.lightstep.com/docs/create-and-manage-access-tokens",
}


def _generate_handler_py(name: str, routes: list[Route], tracing_backend: str = _DEFAULT_TRACING_BACKEND) -> str:
    route_docs = "\n".join(f"  POST  {r.path:<20} {r.description}" for r in routes)

    imports: list[str] = []
    imports.append("from cognite_function_apps import (")
    imports.append("    FunctionApp,")
    imports.append("    create_function_service,")
    imports.append("    create_introspection_app,")
    if tracing_backend:
        imports.append("    create_tracing_app,")
    imports.append(")")

    lines: list[str] = []
    lines.append(f'"""{name} — Cognite Function App.')
    lines.append("")
    lines.append("Routes:")
    lines.append(route_docs)
    lines.append('"""')
    lines.append("")
    lines.append("import logging")
    lines.append("")
    lines.append("from cognite.client import CogniteClient")
    lines.extend(imports)
    lines.append("from pydantic import BaseModel")
    lines.append("")
    lines.append(f'app = FunctionApp(title="{name}", version="1.0.0")')
    if tracing_backend:
        lines.append(f'tracing = create_tracing_app(backend="{tracing_backend}")  # no-op if tracing-api-key is absent')
    lines.append("introspection = create_introspection_app()")
    lines.append("")
    lines.append("")

    body_routes = [r for r in routes if r.has_body]
    if body_routes:
        lines.append("# ── Models ────────────────────────────────────────────────────────────────────")
        lines.append("")
        for r in body_routes:
            model_name = _path_to_model_name(r.path)
            lines.append("")
            lines.append(f"class {model_name}(BaseModel):")
            lines.append("    # TODO: define your request fields")
            lines.append("    pass")
            lines.append("")

    lines.append("# ── Routes ────────────────────────────────────────────────────────────────────")
    lines.append("")
    for r in routes:
        func_name = _path_to_func_name(r.path)
        lines.append("")
        lines.append(f'@app.post("{r.path}")')
        if tracing_backend:
            lines.append("@tracing.trace()")
        if r.has_body:
            model_name = _path_to_model_name(r.path)
            lines.append(
                f"def {func_name}(client: CogniteClient, logger: logging.Logger, request: {model_name}) -> dict:"
            )
        else:
            lines.append(f"def {func_name}(client: CogniteClient, logger: logging.Logger) -> dict:")
        if r.description:
            lines.append(f'    """{r.description}"""')
        lines.append("    raise NotImplementedError")
        lines.append("")
        lines.append("")

    lines.append("# ── Entry point ───────────────────────────────────────────────────────────────")
    lines.append("")
    lines.append(f"handle = create_function_service({'tracing, ' if tracing_backend else ''}introspection, app)")
    lines.append("")
    lines.append('__all__ = ["handle"]')
    lines.append("")

    return "\n".join(lines)


class FunctionsCommand(ToolkitCommand):
    def init(
        self,
        module_path: Path,
        external_id: str,
        name: str | None = None,
        routes: list[Route] | None = None,
        prompt_tracing: bool = True,
        tracing_backend: str = _DEFAULT_TRACING_BACKEND,
    ) -> None:
        """Scaffold handler.py and requirements.txt for a cognite-function-apps Function App."""
        if not name:
            name = questionary.text(
                "Enter a display name for the function:",
                default=external_id,
            ).unsafe_ask()
        name = (name or external_id).strip("\"'")

        if routes is None:
            routes = self._collect_routes()

        if prompt_tracing:
            tracing_backend = self._guide_tracing()

        functions_dir = module_path / "functions"
        handler_dir = functions_dir / external_id

        handler_path = handler_dir / "handler.py"
        requirements_path = handler_dir / "requirements.txt"

        handler_dir.mkdir(parents=True, exist_ok=True)

        self._write_file(handler_path, _generate_handler_py(name, routes, tracing_backend))
        self._write_file(requirements_path, _REQUIREMENTS_TXT)

        print("\n[bold green]Function app scaffolded![/bold green]")

    def _collect_routes(self) -> list[Route]:
        routes: list[Route] = []
        while True:
            path = (
                questionary.text(
                    "Route path (e.g. /process):",
                    validate=lambda v: bool(v.strip()) or "Path cannot be empty",
                )
                .unsafe_ask()
                .strip()
            )
            desc = questionary.text("Brief description (becomes docstring):").unsafe_ask().strip().strip("\"'")
            has_body = questionary.confirm(
                "Does this route accept a structured request body?", default=True
            ).unsafe_ask()
            path = _sanitize_route_path(path)
            routes.append(Route(path=path, description=desc, has_body=has_body))
            if not questionary.confirm("Add another route?", default=False).unsafe_ask():
                break
        return routes

    def _guide_tracing(self) -> str:
        """Prompt for tracing backend and guide TRACING_API_KEY setup. Returns the selected backend."""
        backend: str = questionary.select(
            "Enable tracing? Select a provider:",
            choices=_TRACING_BACKENDS,
            default="",
        ).unsafe_ask()

        if not backend:
            return ""

        if os.environ.get(_TRACING_ENV_VAR):
            print(f"[green]{_TRACING_ENV_VAR} found in environment — tracing is ready.[/green]")
        elif signup_url := _SIGNUP_URLS.get(backend):
            print(
                f"\n[blue]1. Sign up at: {signup_url}[/blue]\n"
                f"2. Add your API key to your .env file:\n"
                f"   {_TRACING_ENV_VAR}=your-api-key\n"
            )

        return backend

    def _write_file(self, path: Path, content: str) -> None:
        if path.exists() and not questionary.confirm(f"{path.name} already exists. Overwrite?").unsafe_ask():
            print(f"[yellow]Skipping {path.as_posix()}[/yellow]")
            return

        path.write_text(content)
        print(f"[green]Created {path.as_posix()}[/green]")
