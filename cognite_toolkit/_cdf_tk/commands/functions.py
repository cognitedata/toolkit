from dataclasses import dataclass
from pathlib import Path

import questionary
import typer
from rich import print

from cognite_toolkit._cdf_tk.commands._base import ToolkitCommand
from cognite_toolkit._cdf_tk.commands.resources import ResourcesCommand


@dataclass
class Route:
    path: str
    description: str
    has_body: bool


def _path_to_func_name(path: str) -> str:
    """Convert /some-route/action → some_route_action."""
    return path.lstrip("/").replace("-", "_").replace("/", "_")


def _path_to_model_name(path: str) -> str:
    """Convert /some-route/action → SomeRouteActionRequest."""
    parts = _path_to_func_name(path).split("_")
    return "".join(p.capitalize() for p in parts if p) + "Request"


_FUNCTION_YAML_TEMPLATE = """\
# API docs: https://api-docs.cognite.com/20230101/tag/Functions
# YAML reference: https://docs.cognite.com/cdf/deploy/cdf_toolkit/references/resource_library

externalId: {external_id}
name: {name}
runtime: py311
functionPath: ./handler.py
secrets:
  tracing-api-key: '{{{{ {secret_var} }}}}'
"""

_REQUIREMENTS_TXT = "cognite-function-apps[tracing]>=0.4.0\n"


def _generate_handler_py(name: str, routes: list[Route]) -> str:
    route_docs = "\n".join(f"  POST  {r.path:<20} {r.description}" for r in routes)

    lines: list[str] = [
        f'"""{name} — Cognite Function App.',
        "",
        "Routes:",
        route_docs,
        "",
        "Tracing is enabled when the 'tracing-api-key' CDF secret is present.",
        "If the secret is absent, tracing is silently disabled and all routes",
        "continue to work normally.",
        "",
        "Local dev server:",
        '  pip install "cognite-function-apps[cli,tracing]"',
        "  fun serve handler.py",
        '"""',
        "",
        "import logging",
        "from pydantic import BaseModel",
        "from cognite.client import CogniteClient",
        "from cognite_function_apps import (",
        "    FunctionApp,",
        "    create_function_service,",
        "    create_introspection_app,",
        "    create_tracing_app,",
        ")",
        "",
        f'app = FunctionApp(title="{name}", version="1.0.0")',
        'tracing = create_tracing_app(backend="honeycomb")   # no-op if tracing-api-key is absent',
        "introspection = create_introspection_app()",
        "",
        "",
    ]

    body_routes = [r for r in routes if r.has_body]
    if body_routes:
        lines.append("# ── Models ────────────────────────────────────────────────────────────────────")
        lines.append("")
        for r in body_routes:
            model_name = _path_to_model_name(r.path)
            lines.append(f"class {model_name}(BaseModel):")
            lines.append("    # TODO: define your request fields")
            lines.append("    pass")
            lines.append("")
        lines.append("")

    lines.append("# ── Routes ────────────────────────────────────────────────────────────────────")
    lines.append("")
    for r in routes:
        func_name = _path_to_func_name(r.path)
        lines.append(f'@app.post("{r.path}")')
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

    lines += [
        "# ── Entry point ───────────────────────────────────────────────────────────────",
        "",
        "handle = create_function_service(tracing, introspection, app)",
        "",
        '__all__ = ["handle"]',
        "",
    ]

    return "\n".join(lines)


class FunctionsCommand(ToolkitCommand):
    def init(
        self,
        organization_dir: Path,
        module_name: str | None,
        verbose: bool,
        external_id: str | None = None,
        name: str | None = None,
        routes: list[Route] | None = None,
    ) -> None:
        """Scaffold a cognite-function-apps Function App into an existing module."""
        resources_cmd = ResourcesCommand(
            print_warning=self._print_warning,
            skip_tracking=True,
            silent=self.silent,
        )
        module_path = resources_cmd._get_or_prompt_module_path(module_name, organization_dir, verbose)

        if not external_id:
            external_id = questionary.text(
                "Enter the function externalId:",
                validate=lambda v: bool(v.strip()) or "externalId cannot be empty",
            ).unsafe_ask()
            if not external_id:
                print("[red]No externalId provided. Aborting...[/red]")
                raise typer.Exit()

        if not name:
            name = questionary.text(
                "Enter a display name for the function:",
                default=external_id,
            ).unsafe_ask()

        if routes is None:
            routes = self._collect_routes()

        secret_var = external_id.replace("-", "_").replace(" ", "_") + "_tracing_api_key"

        functions_dir = module_path / "functions"
        handler_dir = functions_dir / external_id

        yaml_path = functions_dir / f"{external_id}.Function.yaml"
        handler_path = handler_dir / "handler.py"
        requirements_path = handler_dir / "requirements.txt"

        functions_dir.mkdir(parents=True, exist_ok=True)
        handler_dir.mkdir(parents=True, exist_ok=True)

        self._write_file(
            yaml_path,
            _FUNCTION_YAML_TEMPLATE.format(external_id=external_id, name=name, secret_var=secret_var),
        )
        self._write_file(handler_path, _generate_handler_py(name, routes))
        self._write_file(requirements_path, _REQUIREMENTS_TXT)

        print(
            f"\n[bold green]Function app scaffolded![/bold green]\n"
            f"  {yaml_path.as_posix()}\n"
            f"  {handler_path.as_posix()}\n"
            f"  {requirements_path.as_posix()}\n\n"
            f"Next steps:\n"
            f'  pip install "cognite-function-apps[cli,tracing]"\n'
            f"  fun serve {handler_path.as_posix()}"
        )

    def _collect_routes(self) -> list[Route]:
        routes: list[Route] = []
        while True:
            path = questionary.text(
                "Route path (e.g. /process):",
                validate=lambda v: bool(v.strip()) or "Path cannot be empty",
            ).unsafe_ask()
            desc = questionary.text("Brief description (becomes docstring):").unsafe_ask()
            has_body = questionary.confirm(
                "Does this route accept a structured request body?", default=True
            ).unsafe_ask()
            routes.append(Route(path=path, description=desc, has_body=has_body))
            if not questionary.confirm("Add another route?", default=False).unsafe_ask():
                break
        return routes

    def _write_file(self, path: Path, content: str) -> None:
        if path.exists() and not questionary.confirm(
            f"{path.name} already exists. Overwrite?"
        ).unsafe_ask():
            print(f"[yellow]Skipping {path.as_posix()}[/yellow]")
            return

        path.write_text(content)
        print(f"[green]Created {path.as_posix()}[/green]")
