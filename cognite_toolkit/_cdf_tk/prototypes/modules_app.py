from __future__ import annotations

from typing import Annotated, Optional

import typer
from rich import print

from cognite_toolkit._cdf import _get_user_command
from cognite_toolkit._cdf_tk.prototypes.commands.modules import ModulesCommand


class Modules(typer.Typer):
    def __init__(self, *args, **kwargs) -> None:  # type: ignore
        super().__init__(*args, **kwargs)
        self.callback(invoke_without_command=True)(self.main)
        self.command()(self.init)

    def main(self, ctx: typer.Context) -> None:
        """Commands to manage modules"""
        if ctx.invoked_subcommand is None:
            print("Use [bold yellow]cdf-tk modules --help[/] for more information.")

    def init(
        self,
        arg_init_dir: Annotated[
            Optional[str],
            typer.Option(
                "--init-dir",
                help="Directory path to project to initialize or upgrade with templates.",
            ),
        ] = None,
        arg_package: Annotated[
            Optional[str],
            typer.Option(
                "--package",
                help="Name of package to include",
            ),
        ] = None,
    ) -> None:
        """Initialize or upgrade a new CDF project with templates interactively."""

        cmd = ModulesCommand(user_command=_get_user_command())
        cmd.init(
            init_dir=arg_init_dir,
            arg_package=arg_package,
        )
