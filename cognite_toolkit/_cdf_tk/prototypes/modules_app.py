from __future__ import annotations

from typing import Annotated, Optional

import typer
from rich import print

from cognite_toolkit._cdf_tk.prototypes.commands.modules import ModulesCommand
from cognite_toolkit._version import __version__


class Modules(typer.Typer):
    def __init__(self, *args, **kwargs) -> None:  # type: ignore
        super().__init__(*args, **kwargs)
        self.callback(invoke_without_command=True)(self.main)
        self.command()(self.init)
        self.command()(self.upgrade)

    def main(self, ctx: typer.Context) -> None:
        """Commands to manage modules"""
        if ctx.invoked_subcommand is None:
            print("Use [bold yellow]cdf-tk modules --help[/] for more information.")

    def init(
        self,
        project_dir: Annotated[
            Optional[str],
            typer.Argument(
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

        cmd = ModulesCommand()
        cmd.init(
            init_dir=project_dir,
            arg_package=arg_package,
        )

    def upgrade(
        self,
        project_dir: Annotated[
            Optional[str],
            typer.Argument(
                help="Directory path to project to upgrade with templates. Defaults to current directory.",
            ),
        ] = None,
    ) -> None:
        cmd = ModulesCommand()
        cmd.upgrade(project_dir=project_dir)


# This is a trick to use an f-string for the docstring
Modules.upgrade.__doc__ = f"""Upgrade the existing CDF project modules to version {__version__}."""
