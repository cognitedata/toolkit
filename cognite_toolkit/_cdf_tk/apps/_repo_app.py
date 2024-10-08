from pathlib import Path
from typing import Annotated

import typer
from rich import print

from cognite_toolkit._cdf_tk.commands import RepoCommand
from cognite_toolkit._cdf_tk.commands.repo import REPOSITORY_HOSTING
from cognite_toolkit._cdf_tk.utils import humanize_collection


class RepoApp(typer.Typer):
    def __init__(self, *args, **kwargs) -> None:  # type: ignore
        super().__init__(*args, **kwargs)
        self.callback(invoke_without_command=True)(self.main)
        self.command()(self.init)

    def main(self, ctx: typer.Context) -> None:
        """Commands to repo management"""
        if ctx.invoked_subcommand is None:
            print("Use [bold yellow]cdf repo --help[/] for more information.")

    def init(
        self,
        cwd: Annotated[
            Path,
            typer.Argument(
                help="",
            ),
        ] = Path.cwd(),
        host: str | None = typer.Option(
            None,
            "--host",
            "-h",
            help=f"Hosting service for the repository. Supported {humanize_collection(REPOSITORY_HOSTING)}. If not provided, you will be prompted to choose.",
        ),
        verbose: bool = typer.Option(False, "-v", "--verbose", help="Verbose output"),
    ) -> None:
        """Initialize a new git repository with files like .gitignore, cdf.toml, and so on."""

        cmd = RepoCommand()
        cmd.run(lambda: cmd.init(cwd=cwd, host=host, verbose=verbose))
