from pathlib import Path

import questionary
import typer
from rich import print

from cognite_toolkit._cdf_tk.commands._base import ToolkitCommand
from cognite_toolkit._cdf_tk.commands.resources import ResourcesCommand


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

_HANDLER_PY_TEMPLATE = '''\
"""{name} — Cognite Function App.

Routes:
  GET  /hello          Simple health probe
  POST /process        Example POST with a typed request body

Tracing is enabled when the \'tracing-api-key\' CDF secret is present.
If the secret is absent, tracing is silently disabled and all routes
continue to work normally.

Local dev server:
  pip install "cognite-function-apps[cli,tracing]"
  fun serve handler.py
"""

import logging
from pydantic import BaseModel
from cognite.client import CogniteClient
from cognite_function_apps import (
    FunctionApp,
    create_function_service,
    create_introspection_app,
    create_tracing_app,
)

app = FunctionApp(title="{name}", version="1.0.0")
tracing = create_tracing_app(backend="honeycomb")   # no-op if tracing-api-key is absent
introspection = create_introspection_app()


# ── Models ────────────────────────────────────────────────────────────────────

class ProcessRequest(BaseModel):
    asset_external_id: str
    limit: int = 100


class ProcessResponse(BaseModel):
    asset_external_id: str
    count: int


# ── Routes ────────────────────────────────────────────────────────────────────

@app.get("/hello")
def hello(client: CogniteClient, logger: logging.Logger) -> dict:
    """Health probe — returns \'ok\' when the function is reachable."""
    logger.info("Health probe called")
    return {{"status": "ok"}}


@app.post("/process")
@tracing.trace()
def process(
    client: CogniteClient,
    logger: logging.Logger,
    request: ProcessRequest,
) -> ProcessResponse:
    """Example POST: count events linked to an asset."""
    logger.info(f"Processing asset {{request.asset_external_id!r}}")
    events = client.events.list(
        asset_external_ids=[request.asset_external_id],
        limit=request.limit,
    )
    return ProcessResponse(
        asset_external_id=request.asset_external_id,
        count=len(events),
    )


# ── Entry point ───────────────────────────────────────────────────────────────

handle = create_function_service(tracing, introspection, app)

__all__ = ["handle"]
'''

_REQUIREMENTS_TXT = "cognite-function-apps[tracing]>=0.4.0\n"


class FunctionsCommand(ToolkitCommand):
    def init(
        self,
        organization_dir: Path,
        module_name: str | None,
        external_id: str | None,
        name: str | None,
        verbose: bool,
    ) -> None:
        """Scaffold a cognite-function-apps Function App into an existing module."""
        # Reuse module resolution from ResourcesCommand
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

        # Derive the secret variable name from external_id (replace hyphens/spaces with underscores)
        secret_var = external_id.replace("-", "_").replace(" ", "_") + "_tracing_api_key"

        functions_dir = module_path / "functions"
        handler_dir = functions_dir / external_id

        yaml_path = functions_dir / f"{external_id}.Function.yaml"
        handler_path = handler_dir / "handler.py"
        requirements_path = handler_dir / "requirements.txt"

        # Create directories
        functions_dir.mkdir(parents=True, exist_ok=True)
        handler_dir.mkdir(parents=True, exist_ok=True)

        # Write each file, prompting on overwrite
        self._write_file(yaml_path, _FUNCTION_YAML_TEMPLATE.format(external_id=external_id, name=name, secret_var=secret_var))
        self._write_file(handler_path, _HANDLER_PY_TEMPLATE.format(name=name))
        self._write_file(requirements_path, _REQUIREMENTS_TXT)

        print(
            f"\n[bold green]Function app scaffolded![/bold green]\n"
            f"  {yaml_path.as_posix()}\n"
            f"  {handler_path.as_posix()}\n"
            f"  {requirements_path.as_posix()}\n\n"
            f"Next steps:\n"
            f"  pip install \"cognite-function-apps[cli,tracing]\"\n"
            f"  fun serve {handler_path.as_posix()}"
        )

    def _write_file(self, path: Path, content: str) -> None:
        if path.exists() and not questionary.confirm(
            f"{path.name} already exists. Overwrite?"
        ).unsafe_ask():
            print(f"[yellow]Skipping {path.as_posix()}[/yellow]")
            return

        path.write_text(content)
        print(f"[green]Created {path.as_posix()}[/green]")
