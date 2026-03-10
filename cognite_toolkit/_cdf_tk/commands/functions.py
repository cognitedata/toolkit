import os
import re
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

import questionary
from questionary import Choice
from rich import print

from cognite_toolkit._cdf_tk.commands._base import ToolkitCommand

# Constants

_BASIC_HANDLER_PY = """\
from cognite.client import CogniteClient


def handle(client: CogniteClient, data: dict) -> dict:
    print(f"Hello from {data.get('name', 'world')}!")
    return {"status": "ok"}
"""

_TRACING_BACKENDS = [
    Choice(title="No tracing", value=""),
    Choice(title="eu.honeycomb  — Honeycomb EU (free tier available)", value="eu.honeycomb"),
    Choice(title="honeycomb     — Honeycomb US (free tier available)", value="honeycomb"),
    Choice(title="lightstep     — Lightstep / ServiceNow", value="lightstep"),
]
_DEFAULT_TRACING_BACKEND = _TRACING_BACKENDS[1].value


# Dataclasses


@dataclass(slots=True)
class ScaffoldDef:
    """A scaffold variant for a resource type (e.g. basic Function vs Function App)."""

    label: str
    description: str
    run: Callable[[Path, str, ToolkitCommand], None]


@dataclass(slots=True)
class Route:
    path: str
    description: str
    has_body: bool


# Helper functions


def _sanitize_route_path(path: str) -> str:
    """Normalise a route path to lowercase-with-hyphens, ensuring a leading ``/``."""
    path = re.sub(r"[_\s.]+", "-", path.lower())
    path = re.sub(r"[^a-z0-9/\-]", "", path)
    path = re.sub(r"/+", "/", path)
    path = re.sub(r"-+", "-", path).strip("-/")
    return f"/{path}"


def _path_to_func_name(path: str) -> str:
    """Convert /some-route/action → some_route_action."""
    name = re.sub(r"[^a-zA-Z0-9]", "_", path.lstrip("/"))
    return re.sub(r"_+", "_", name).strip("_")


def _path_to_model_name(path: str) -> str:
    """Convert /some-route/action → SomeRouteActionRequest."""
    return "".join(p.capitalize() for p in _path_to_func_name(path).split("_") if p) + "Request"


def _generate_handler_py(name: str, routes: list[Route], tracing_backend: str = _DEFAULT_TRACING_BACKEND) -> str:
    route_docs = "\n".join(f"  POST  {r.path:<20} {r.description}" for r in routes)
    t_import = "\n    create_tracing_app," if tracing_backend else ""
    t_setup = (
        f'\ntracing = create_tracing_app(backend="{tracing_backend}")  # no-op without key' if tracing_backend else ""
    )
    t_arg = "tracing, " if tracing_backend else ""
    t_deco = "@tracing.trace()\n" if tracing_backend else ""

    def _model_block(r: Route) -> str:
        return f"class {_path_to_model_name(r.path)}(BaseModel):\n    # TODO: define your request fields\n    pass\n"

    def _route_block(r: Route) -> str:
        fn = _path_to_func_name(r.path)
        params = "client: CogniteClient, logger: logging.Logger"
        if r.has_body:
            params += f", request: {_path_to_model_name(r.path)}"
        doc = f'    """{r.description}"""\n' if r.description else ""
        return f'@app.post("{r.path}")\n{t_deco}def {fn}({params}) -> dict:\n{doc}    raise NotImplementedError\n'

    body_routes = [r for r in routes if r.has_body]
    models = (
        "\n# ── Models ────────────────────────────────────────────────────────────────────\n\n"
        + "\n\n".join(_model_block(r) for r in body_routes)
        if body_routes
        else ""
    )

    route_fns = "\n".join(_route_block(r) for r in routes)

    return f'''\
"""{name} — Cognite Function App.

Routes:
{route_docs}
"""

import logging

from cognite.client import CogniteClient
from cognite_function_apps import (
    FunctionApp,
    create_function_service,
    create_introspection_app,{t_import}
)
from pydantic import BaseModel

app = FunctionApp(title="{name}", version="1.0.0"){t_setup}
introspection = create_introspection_app()

{models}
# ── Routes ────────────────────────────────────────────────────────────────────

{route_fns}
# ── Entry point ───────────────────────────────────────────────────────────────

handle = create_function_service({t_arg}introspection, app)

__all__ = ["handle"]
'''


def _scaffold_basic_function(module_path: Path, external_id: str, cmd: ToolkitCommand) -> None:
    handler_dir = module_path / "functions" / external_id
    handler_dir.mkdir(parents=True, exist_ok=True)
    for name, content in [("handler.py", _BASIC_HANDLER_PY), ("requirements.txt", "")]:
        (handler_dir / name).write_text(content)
        print(f"[green]Created {(handler_dir / name).as_posix()}[/green]")


def _scaffold_function_app(module_path: Path, external_id: str, cmd: ToolkitCommand) -> None:
    FunctionsCommand(print_warning=cmd._print_warning, skip_tracking=True, silent=cmd.silent).init(
        module_path=module_path, external_id=external_id
    )


def get_scaffolds() -> dict[str, list[ScaffoldDef]]:
    """Return scaffold variants keyed by CRUD kind (casefold).

    Called at import time by ``resources.py``.  This is safe because
    ``commands/`` never imports from ``cruds/`` at module level here;
    only the scaffold *runners* reference CRUD classes, and those run
    later at call time.
    """
    return {
        "function": [
            ScaffoldDef("Function", "Single entry-point function", run=_scaffold_basic_function),
            ScaffoldDef("Function App", "Let a function perform multiple tasks", run=_scaffold_function_app),
        ],
    }


# Command class


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
            name = questionary.text("Enter a display name for the function:", default=external_id).unsafe_ask()
        name = (name or external_id).strip("\"'")
        if routes is None:
            routes = self._collect_routes()
        if prompt_tracing:
            tracing_backend = self._guide_tracing()

        handler_dir = module_path / "functions" / external_id
        handler_dir.mkdir(parents=True, exist_ok=True)

        self._write_file(handler_dir / "handler.py", _generate_handler_py(name, routes, tracing_backend))
        self._write_file(handler_dir / "requirements.txt", "cognite-function-apps[tracing]\n")
        print("\n[bold green]Function app scaffolded![/bold green]")

    def _collect_routes(self) -> list[Route]:
        ask = questionary.text
        routes: list[Route] = []
        while True:
            path = (
                ask("Route path (e.g. /process):", validate=lambda v: bool(v.strip()) or "Path cannot be empty")
                .unsafe_ask()
                .strip()
            )
            desc = ask("Brief description (becomes docstring):").unsafe_ask().strip().strip("\"'")
            has_body = questionary.confirm("Accept a structured request body?", default=True).unsafe_ask()
            routes.append(Route(path=_sanitize_route_path(path), description=desc, has_body=has_body))
            if not questionary.confirm("Add another route?", default=False).unsafe_ask():
                return routes

    def _guide_tracing(self) -> str:
        """Prompt for tracing backend. Returns the selected backend or empty string."""
        backend: str = questionary.select(
            "Enable tracing? Select a provider:", choices=_TRACING_BACKENDS, default=""
        ).unsafe_ask()
        if not backend:
            return ""
        if os.environ.get("TRACING_API_KEY"):
            print("[green]TRACING_API_KEY found in environment — tracing is ready.[/green]")
        else:
            print("\n[blue]Add your tracing API key to your .env file:[/blue]\n   TRACING_API_KEY=your-api-key\n")
        return backend

    def _write_file(self, path: Path, content: str) -> None:
        if path.exists() and not questionary.confirm(f"{path.name} already exists. Overwrite?").unsafe_ask():
            print(f"[yellow]Skipping {path.as_posix()}[/yellow]")
            return
        path.write_text(content)
        print(f"[green]Created {path.as_posix()}[/green]")
