from pathlib import Path
from typing import Annotated, Any

import typer
from rich import print

from cognite_toolkit._cdf_tk.commands import ServeFunctionCommand


class DevFunctionApp(typer.Typer):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.callback(invoke_without_command=True)(self.main)
        self.command("serve")(self.serve)

    @staticmethod
    def main(ctx: typer.Context) -> None:
        """Commands for function app development."""
        if ctx.invoked_subcommand is None:
            print("Use [bold yellow]cdf dev function --help[/] for more information.")

    @staticmethod
    def serve(
        path: Annotated[
            Path | None,
            typer.Argument(help="Path to the directory containing handler.py. If omitted, discovers and prompts."),
        ] = None,
        host: Annotated[str, typer.Option("--host", help="Host to bind to")] = "127.0.0.1",
        port: Annotated[int, typer.Option("--port", help="Port to bind to")] = 8000,
        reload: Annotated[bool, typer.Option("--reload/--no-reload", help="Enable auto-reload on code changes")] = True,
        log_level: Annotated[
            str,
            typer.Option("--log-level", help="Log level for the server"),
        ] = "info",
    ) -> None:
        """Start a local development server for testing a function app handler.

        Loads your handler.py and starts a uvicorn dev server with Swagger UI.

        Example:
            cdf dev function serve my_function/
            cdf dev function serve . --port 3000 --no-reload
        """
        cmd = ServeFunctionCommand(client=None, skip_tracking=True)
        cmd.run(lambda: cmd.serve(path, host, port, reload, log_level))
